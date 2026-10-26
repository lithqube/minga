//! Sandbox runtime abstraction.
//!
//! The [`SandboxRuntime`] trait abstracts over the sandbox execution
//! backend (wasmtime in production for WASM workloads, Firecracker for
//! microVM workloads, mock for testing). The agent supervisor talks to
//! the runtime exclusively through this trait.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox `{0}` not found")]
    NotFound(String),
    #[error("failed to create sandbox: {0}")]
    Create(String),
    #[error("failed to run sandbox: {0}")]
    Run(String),
    #[error("failed to destroy sandbox: {0}")]
    Destroy(String),
    #[error("sandbox timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("fuel limit exceeded (consumed {consumed}, limit {limit})")]
    FuelExhausted { consumed: u64, limit: u64 },
    #[error("runtime error: {0}")]
    Other(String),
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Specification for creating a sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxSpec {
    /// WASM module bytes (or path to `.wasm` file, resolved by the runtime).
    pub module: Vec<u8>,
    /// Maximum fuel (instruction budget). `None` = unlimited.
    pub fuel_limit: Option<u64>,
    /// Maximum memory in bytes. `None` = runtime default.
    pub memory_limit: Option<u64>,
    /// Maximum execution time. `None` = runtime default (30s).
    pub timeout: Option<std::time::Duration>,
    /// Environment variables passed to the sandbox.
    pub env: HashMap<String, String>,
    /// Maximum stdout/stderr capture size in bytes.
    pub output_limit: Option<usize>,
}

impl Default for SandboxSpec {
    fn default() -> Self {
        Self {
            module: Vec::new(),
            fuel_limit: Some(1_000_000_000),
            memory_limit: Some(128 * 1024 * 1024), // 128 MiB
            timeout: Some(std::time::Duration::from_secs(30)),
            env: HashMap::new(),
            output_limit: Some(1024 * 1024), // 1 MiB
        }
    }
}

/// Status of a sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxStatus {
    /// Sandbox is created and ready to accept work.
    Ready,
    /// Sandbox is currently executing.
    Running,
    /// Sandbox completed successfully.
    Completed,
    /// Sandbox failed.
    Failed,
    /// Sandbox was destroyed.
    Destroyed,
}

impl std::fmt::Display for SandboxStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready => write!(f, "ready"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Destroyed => write!(f, "destroyed"),
        }
    }
}

/// Information about a sandbox known to the runtime.
#[derive(Debug, Clone)]
pub struct SandboxInfo {
    pub id: String,
    pub status: SandboxStatus,
    /// Fuel consumed so far (if the backend tracks it).
    pub fuel_consumed: Option<u64>,
    /// Wall-clock time since creation.
    pub age: std::time::Duration,
}

/// Result of running a sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxResult {
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
    /// Exit code (0 = success).
    pub exit_code: i32,
    /// Fuel consumed during execution.
    pub fuel_consumed: u64,
    /// Wall-clock execution time.
    pub duration: std::time::Duration,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A sandbox runtime. Implementations must be thread-safe.
///
/// The lifecycle is:
/// 1. [`SandboxRuntime::create`] — instantiate a sandbox from a spec.
/// 2. [`SandboxRuntime::run`] — execute the sandbox with input data.
/// 3. [`SandboxRuntime::destroy`] — tear down the sandbox.
#[async_trait]
pub trait SandboxRuntime: Send + Sync {
    /// Creates a new sandbox from the given spec. Returns the sandbox ID.
    async fn create(&self, spec: &SandboxSpec) -> Result<String, SandboxError>;

    /// Runs the sandbox with the given input. Returns the result.
    ///
    /// The input is passed to the sandbox as stdin (or equivalent).
    async fn run(&self, sandbox_id: &str, input: &[u8]) -> Result<SandboxResult, SandboxError>;

    /// Destroys a sandbox and releases its resources.
    async fn destroy(&self, sandbox_id: &str) -> Result<(), SandboxError>;

    /// Lists all sandboxes known to this runtime.
    async fn list(&self) -> Vec<SandboxInfo>;

    /// Returns the status of a sandbox, or `None` if not found.
    async fn status(&self, sandbox_id: &str) -> Option<SandboxStatus>;
}

// ---------------------------------------------------------------------------
// MockSandboxRuntime
// ---------------------------------------------------------------------------

/// An in-memory mock sandbox runtime for testing. Simulates sandbox
/// lifecycle without actually executing anything.
#[derive(Debug, Default)]
pub struct MockSandboxRuntime {
    sandboxes: Mutex<HashMap<String, MockSandbox>>,
    counter: Mutex<u64>,
    /// If true, `create()` returns an error.
    pub fail_creates: Mutex<bool>,
    /// If true, `run()` returns an error.
    pub fail_runs: Mutex<bool>,
    /// If set, `run()` returns this exit code instead of 0.
    pub run_exit_code: Mutex<Option<i32>>,
}

#[derive(Debug)]
struct MockSandbox {
    id: String,
    status: SandboxStatus,
    created_at: std::time::Instant,
    fuel_consumed: u64,
}

impl MockSandboxRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of currently tracked sandboxes.
    pub fn sandbox_count(&self) -> usize {
        self.sandboxes.lock().unwrap().len()
    }
}

#[async_trait]
impl SandboxRuntime for MockSandboxRuntime {
    async fn create(&self, _spec: &SandboxSpec) -> Result<String, SandboxError> {
        if *self.fail_creates.lock().unwrap() {
            return Err(SandboxError::Create("mock: creates disabled".into()));
        }

        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        let id = format!("mock-sbx-{:04}", *counter);
        drop(counter);

        self.sandboxes.lock().unwrap().insert(
            id.clone(),
            MockSandbox {
                id: id.clone(),
                status: SandboxStatus::Ready,
                created_at: std::time::Instant::now(),
                fuel_consumed: 0,
            },
        );

        tracing::info!(sandbox = %id, "mock sandbox: created");
        Ok(id)
    }

    async fn run(&self, sandbox_id: &str, input: &[u8]) -> Result<SandboxResult, SandboxError> {
        if *self.fail_runs.lock().unwrap() {
            return Err(SandboxError::Run("mock: runs disabled".into()));
        }

        let mut sandboxes = self.sandboxes.lock().unwrap();
        let sbx = sandboxes
            .get_mut(sandbox_id)
            .ok_or_else(|| SandboxError::NotFound(sandbox_id.to_string()))?;

        sbx.status = SandboxStatus::Running;
        let fuel = input.len() as u64 * 100; // fake fuel: 100 per input byte
        sbx.fuel_consumed += fuel;
        sbx.status = SandboxStatus::Completed;

        let exit_code = self.run_exit_code.lock().unwrap().unwrap_or(0);
        let stdout = if exit_code == 0 {
            format!("mock output ({} bytes input)", input.len())
        } else {
            String::new()
        };

        Ok(SandboxResult {
            stdout,
            stderr: String::new(),
            exit_code,
            fuel_consumed: fuel,
            duration: std::time::Duration::from_millis(1),
        })
    }

    async fn destroy(&self, sandbox_id: &str) -> Result<(), SandboxError> {
        let mut sandboxes = self.sandboxes.lock().unwrap();
        match sandboxes.get_mut(sandbox_id) {
            Some(sbx) => {
                sbx.status = SandboxStatus::Destroyed;
                sandboxes.remove(sandbox_id);
                tracing::info!(sandbox = %sandbox_id, "mock sandbox: destroyed");
                Ok(())
            }
            None => Err(SandboxError::NotFound(sandbox_id.to_string())),
        }
    }

    async fn list(&self) -> Vec<SandboxInfo> {
        self.sandboxes
            .lock()
            .unwrap()
            .values()
            .map(|s| SandboxInfo {
                id: s.id.clone(),
                status: s.status,
                fuel_consumed: Some(s.fuel_consumed),
                age: s.created_at.elapsed(),
            })
            .collect()
    }

    async fn status(&self, sandbox_id: &str) -> Option<SandboxStatus> {
        self.sandboxes
            .lock()
            .unwrap()
            .get(sandbox_id)
            .map(|s| s.status)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SandboxSpec {
        SandboxSpec {
            module: vec![0x00, 0x61, 0x73, 0x6d], // WASM magic bytes
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn mock_create_and_destroy() {
        let rt = MockSandboxRuntime::new();
        let id = rt.create(&spec()).await.unwrap();
        assert_eq!(rt.sandbox_count(), 1);
        assert_eq!(rt.status(&id).await, Some(SandboxStatus::Ready));

        rt.destroy(&id).await.unwrap();
        assert_eq!(rt.sandbox_count(), 0);
        assert_eq!(rt.status(&id).await, None);
    }

    #[tokio::test]
    async fn mock_run() {
        let rt = MockSandboxRuntime::new();
        let id = rt.create(&spec()).await.unwrap();
        let result = rt.run(&id, b"hello").await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("5 bytes"));
        assert!(result.fuel_consumed > 0);
    }

    #[tokio::test]
    async fn mock_run_custom_exit_code() {
        let rt = MockSandboxRuntime::new();
        *rt.run_exit_code.lock().unwrap() = Some(1);
        let id = rt.create(&spec()).await.unwrap();
        let result = rt.run(&id, b"test").await.unwrap();
        assert_eq!(result.exit_code, 1);
        assert!(result.stdout.is_empty());
    }

    #[tokio::test]
    async fn mock_unique_ids() {
        let rt = MockSandboxRuntime::new();
        let id1 = rt.create(&spec()).await.unwrap();
        let id2 = rt.create(&spec()).await.unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn mock_destroy_missing() {
        let rt = MockSandboxRuntime::new();
        assert!(rt.destroy("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn mock_run_missing() {
        let rt = MockSandboxRuntime::new();
        assert!(rt.run("nonexistent", b"").await.is_err());
    }

    #[tokio::test]
    async fn mock_fail_creates() {
        let rt = MockSandboxRuntime::new();
        *rt.fail_creates.lock().unwrap() = true;
        assert!(rt.create(&spec()).await.is_err());
    }

    #[tokio::test]
    async fn mock_fail_runs() {
        let rt = MockSandboxRuntime::new();
        *rt.fail_runs.lock().unwrap() = true;
        let id = rt.create(&spec()).await.unwrap();
        assert!(rt.run(&id, b"").await.is_err());
    }

    #[tokio::test]
    async fn mock_list() {
        let rt = MockSandboxRuntime::new();
        rt.create(&spec()).await.unwrap();
        rt.create(&spec()).await.unwrap();
        let sandboxes = rt.list().await;
        assert_eq!(sandboxes.len(), 2);
        assert!(sandboxes.iter().all(|s| s.status == SandboxStatus::Ready));
    }

    #[tokio::test]
    async fn spec_default() {
        let s = SandboxSpec::default();
        assert_eq!(s.fuel_limit, Some(1_000_000_000));
        assert_eq!(s.memory_limit, Some(128 * 1024 * 1024));
        assert_eq!(s.timeout, Some(std::time::Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn status_display() {
        assert_eq!(SandboxStatus::Ready.to_string(), "ready");
        assert_eq!(SandboxStatus::Running.to_string(), "running");
        assert_eq!(SandboxStatus::Completed.to_string(), "completed");
        assert_eq!(SandboxStatus::Failed.to_string(), "failed");
        assert_eq!(SandboxStatus::Destroyed.to_string(), "destroyed");
    }
}
