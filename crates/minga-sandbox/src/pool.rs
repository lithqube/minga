//! Warm pool for sandbox runtimes.
//!
//! A [`WarmPool`] wraps any [`SandboxRuntime`] and maintains a pool of
//! pre-warmed (pre-created) sandboxes to eliminate cold-start latency.
//!
//! # How it works
//!
//! 1. At startup, the pool pre-creates `min_size` sandboxes.
//! 2. [`WarmPool::acquire`] returns a ready sandbox from the pool.
//!    If the pool is empty, a new sandbox is created on demand.
//! 3. [`WarmPool::release`] returns a sandbox to the pool for reuse.
//! 4. A background task periodically replenishes the pool back to
//!    `min_size` and evicts idle sandboxes beyond `max_size`.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use minga_sandbox::{WarmPool, PoolConfig, WasmtimeRuntime, SandboxSpec};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let runtime = Arc::new(WasmtimeRuntime::new()?);
//! let config = PoolConfig::default();
//! let pool = WarmPool::new(runtime, config);
//! pool.start().await;
//!
//! let sandbox_id = pool.acquire().await?;
//! // ... use the sandbox ...
//! pool.release(&sandbox_id).await;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::runtime::{
    SandboxError, SandboxInfo, SandboxResult, SandboxRuntime, SandboxSpec, SandboxStatus,
};

/// Configuration for a [`WarmPool`].
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of pre-warmed sandboxes to maintain.
    pub min_size: usize,
    /// Maximum number of sandboxes in the pool (including in-use).
    pub max_size: usize,
    /// How long an idle sandbox can sit in the pool before eviction.
    pub idle_timeout: Duration,
    /// Spec used to create new sandboxes.
    pub spec: SandboxSpec,
    /// How often the background replenish/evict task runs.
    pub replenish_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 2,
            max_size: 10,
            idle_timeout: Duration::from_secs(300),
            spec: SandboxSpec::default(),
            replenish_interval: Duration::from_secs(10),
        }
    }
}

/// Statistics about the warm pool.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Sandboxes currently idle in the pool.
    pub idle: usize,
    /// Sandboxes currently checked out (in use).
    pub in_use: usize,
    /// Total sandboxes created since startup.
    pub total_created: u64,
    /// Total acquire calls.
    pub total_acquires: u64,
    /// Acquires that had to create a new sandbox (pool was empty).
    pub cache_misses: u64,
    /// Acquires served from the warm pool.
    pub cache_hits: u64,
    /// Sandboxes evicted due to idle timeout.
    pub evicted: u64,
}

struct PooledSandbox {
    id: String,
    idle_since: Instant,
}

struct PoolInner {
    idle: VecDeque<PooledSandbox>,
    in_use: std::collections::HashSet<String>,
    stats: PoolStats,
}

/// A warm pool of pre-created sandboxes backed by any [`SandboxRuntime`].
pub struct WarmPool {
    runtime: Arc<dyn SandboxRuntime>,
    config: PoolConfig,
    inner: Arc<Mutex<PoolInner>>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl std::fmt::Debug for WarmPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WarmPool")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl WarmPool {
    /// Creates a new warm pool. Call [`WarmPool::start`] to begin
    /// pre-warming and background maintenance.
    pub fn new(runtime: Arc<dyn SandboxRuntime>, config: PoolConfig) -> Self {
        Self {
            runtime,
            config,
            inner: Arc::new(Mutex::new(PoolInner {
                idle: VecDeque::new(),
                in_use: std::collections::HashSet::new(),
                stats: PoolStats::default(),
            })),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Starts the pool: pre-warms `min_size` sandboxes and spawns a
    /// background task for replenishment and eviction.
    pub async fn start(&self) {
        // Pre-warm.
        for _ in 0..self.config.min_size {
            match self.runtime.create(&self.config.spec).await {
                Ok(id) => {
                    let mut inner = self.inner.lock().await;
                    inner.idle.push_back(PooledSandbox {
                        id,
                        idle_since: Instant::now(),
                    });
                    inner.stats.total_created += 1;
                }
                Err(e) => {
                    tracing::warn!("warm pool: failed to pre-warm sandbox: {e}");
                }
            }
        }

        // Spawn background maintenance task.
        let inner_arc = Arc::clone(&self.inner);
        let runtime = Arc::clone(&self.runtime);
        let config = self.config.clone();
        let shutdown = Arc::clone(&self.shutdown);

        tokio::spawn(async move {
            loop {
                let interval = tokio::time::sleep(config.replenish_interval);
                tokio::select! {
                    _ = interval => {}
                    _ = shutdown.notified() => break
                }

                // Evict idle sandboxes that have been idle too long.
                let evicted_ids: Vec<String> = {
                    let mut guard = inner_arc.lock().await;
                    let now = Instant::now();
                    let mut to_remove: Vec<String> = Vec::new();
                    guard.idle.retain(|ps| {
                        let expired = now.duration_since(ps.idle_since) >= config.idle_timeout;
                        if expired {
                            to_remove.push(ps.id.clone());
                        }
                        !expired
                    });
                    guard.stats.evicted += to_remove.len() as u64;
                    to_remove
                };

                // Destroy evicted sandboxes.
                for id in evicted_ids {
                    let _ = runtime.destroy(&id).await;
                }

                // Replenish to min_size.
                let to_create = {
                    let guard = inner_arc.lock().await;
                    let total = guard.idle.len() + guard.in_use.len();
                    let need = config.min_size.saturating_sub(total);
                    let can_create = config.max_size.saturating_sub(total);
                    need.min(can_create)
                };

                for _ in 0..to_create {
                    match runtime.create(&config.spec).await {
                        Ok(id) => {
                            let mut guard = inner_arc.lock().await;
                            guard.idle.push_back(PooledSandbox {
                                id,
                                idle_since: Instant::now(),
                            });
                            guard.stats.total_created += 1;
                        }
                        Err(e) => {
                            tracing::warn!("warm pool: failed to replenish sandbox: {e}");
                            break;
                        }
                    }
                }
            }
            tracing::info!("warm pool: background task stopped");
        });

        tracing::info!(
            min_size = self.config.min_size,
            max_size = self.config.max_size,
            "warm pool: started"
        );
    }

    /// Acquires a sandbox from the pool. Returns the sandbox ID.
    ///
    /// If the pool has an idle sandbox, it is returned immediately.
    /// Otherwise, a new sandbox is created on demand (up to `max_size`).
    pub async fn acquire(&self) -> Result<String, SandboxError> {
        let mut inner = self.inner.lock().await;
        inner.stats.total_acquires += 1;

        // Try to pop an idle sandbox.
        if let Some(ps) = inner.idle.pop_front() {
            inner.stats.cache_hits += 1;
            inner.in_use.insert(ps.id.clone());
            tracing::debug!(sandbox = %ps.id, "warm pool: acquired from pool");
            return Ok(ps.id);
        }

        // Pool is empty — create on demand if under max_size.
        let total = inner.idle.len() + inner.in_use.len();
        if total >= self.config.max_size {
            return Err(SandboxError::Other(format!(
                "warm pool exhausted (max_size={})",
                self.config.max_size
            )));
        }

        inner.stats.cache_misses += 1;
        drop(inner);

        let id = self.runtime.create(&self.config.spec).await?;
        let mut inner = self.inner.lock().await;
        inner.in_use.insert(id.clone());
        inner.stats.total_created += 1;
        tracing::debug!(sandbox = %id, "warm pool: created on demand");
        Ok(id)
    }

    /// Releases a sandbox back to the pool for reuse.
    pub async fn release(&self, sandbox_id: &str) {
        let mut inner = self.inner.lock().await;
        if inner.in_use.remove(sandbox_id) {
            inner.idle.push_back(PooledSandbox {
                id: sandbox_id.to_string(),
                idle_since: Instant::now(),
            });
            tracing::debug!(sandbox = %sandbox_id, "warm pool: released to pool");
        }
    }

    /// Runs a sandbox acquired from the pool and releases it afterward.
    ///
    /// This is the primary entry point for executing work: it acquires
    /// a sandbox, runs it, and returns it to the pool.
    pub async fn run(&self, input: &[u8]) -> Result<SandboxResult, SandboxError> {
        let id = self.acquire().await?;
        let result = self.runtime.run(&id, input).await;
        self.release(&id).await;
        result
    }

    /// Returns current pool statistics.
    pub async fn stats(&self) -> PoolStats {
        self.inner.lock().await.stats.clone()
    }

    /// Lists all sandboxes (idle + in-use) via the underlying runtime.
    pub async fn list(&self) -> Vec<SandboxInfo> {
        self.runtime.list().await
    }

    /// Returns the status of a specific sandbox.
    pub async fn status(&self, sandbox_id: &str) -> Option<SandboxStatus> {
        self.runtime.status(sandbox_id).await
    }

    /// Returns the number of idle sandboxes in the pool.
    pub async fn idle_count(&self) -> usize {
        self.inner.lock().await.idle.len()
    }

    /// Returns the number of in-use sandboxes.
    pub async fn in_use_count(&self) -> usize {
        self.inner.lock().await.in_use.len()
    }

    /// Shuts down the pool: stops the background task and destroys
    /// all sandboxes.
    pub async fn shutdown(&self) {
        self.shutdown.notify_one();

        let mut inner = self.inner.lock().await;
        let idle_ids: Vec<String> = inner.idle.drain(..).map(|ps| ps.id).collect();
        let in_use_ids: Vec<String> = inner.in_use.drain().collect();
        drop(inner);

        for id in idle_ids.into_iter().chain(in_use_ids) {
            let _ = self.runtime.destroy(&id).await;
        }

        tracing::info!("warm pool: shut down");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MockSandboxRuntime;

    fn test_config(min: usize, max: usize) -> PoolConfig {
        PoolConfig {
            min_size: min,
            max_size: max,
            idle_timeout: Duration::from_secs(60),
            spec: SandboxSpec::default(),
            replenish_interval: Duration::from_secs(3600), // don't interfere with tests
        }
    }

    #[tokio::test]
    async fn pre_warm_creates_min_sandboxes() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(3, 10));
        pool.start().await;

        assert_eq!(pool.idle_count().await, 3);
        assert_eq!(rt.sandbox_count(), 3);

        let stats = pool.stats().await;
        assert_eq!(stats.total_created, 3);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn acquire_from_pool() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(2, 10));
        pool.start().await;

        let _id = pool.acquire().await.unwrap();
        assert_eq!(pool.idle_count().await, 1);
        assert_eq!(pool.in_use_count().await, 1);

        let stats = pool.stats().await;
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 0);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn release_returns_to_pool() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(2, 10));
        pool.start().await;

        let id = pool.acquire().await.unwrap();
        assert_eq!(pool.in_use_count().await, 1);

        pool.release(&id).await;
        assert_eq!(pool.in_use_count().await, 0);
        assert_eq!(pool.idle_count().await, 2);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn acquire_creates_on_demand_when_empty() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(0, 5));
        pool.start().await;

        assert_eq!(pool.idle_count().await, 0);

        let _id = pool.acquire().await.unwrap();
        assert_eq!(pool.in_use_count().await, 1);

        let stats = pool.stats().await;
        assert_eq!(stats.cache_misses, 1);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn acquire_fails_when_pool_exhausted() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(0, 2));
        pool.start().await;

        let _id1 = pool.acquire().await.unwrap();
        let _id2 = pool.acquire().await.unwrap();
        let result = pool.acquire().await;
        assert!(result.is_err());

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn run_acquires_and_releases() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(1, 5));
        pool.start().await;

        let result = pool.run(b"hello").await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("5 bytes"));

        // Sandbox should be back in the pool.
        assert_eq!(pool.in_use_count().await, 0);
        assert_eq!(pool.idle_count().await, 1);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn multiple_acquire_release_cycles() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(1, 5));
        pool.start().await;

        for i in 0..5 {
            let result = pool.run(format!("input-{i}").as_bytes()).await.unwrap();
            assert_eq!(result.exit_code, 0);
        }

        let stats = pool.stats().await;
        assert_eq!(stats.total_acquires, 5);
        // First acquire is a hit (pre-warmed), rest may be hits or misses
        // depending on timing, but all should succeed.
        assert_eq!(stats.cache_hits + stats.cache_misses, 5);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_destroys_all_sandboxes() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(3, 10));
        pool.start().await;

        let _id = pool.acquire().await.unwrap();
        assert_eq!(rt.sandbox_count(), 3);

        pool.shutdown().await;
        assert_eq!(rt.sandbox_count(), 0);
    }

    #[tokio::test]
    async fn stats_track_acquires() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(2, 10));
        pool.start().await;

        let id1 = pool.acquire().await.unwrap();
        let _id2 = pool.acquire().await.unwrap();
        pool.release(&id1).await;
        let _id3 = pool.acquire().await.unwrap();

        let stats = pool.stats().await;
        assert_eq!(stats.total_acquires, 3);
        assert_eq!(stats.cache_hits, 3); // all served from pool
        assert_eq!(stats.cache_misses, 0);

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn release_unknown_sandbox_is_noop() {
        let rt = Arc::new(MockSandboxRuntime::new());
        let pool = WarmPool::new(rt.clone(), test_config(1, 5));
        pool.start().await;

        // Releasing a sandbox that was never acquired should be a no-op.
        pool.release("nonexistent").await;
        assert_eq!(pool.idle_count().await, 1);
        assert_eq!(pool.in_use_count().await, 0);

        pool.shutdown().await;
    }
}
