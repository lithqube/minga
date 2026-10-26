//! Container runtime abstraction.
//!
//! The [`Runtime`] trait abstracts over the container runtime (containerd
//! in production, [`MockRuntime`] for testing). The supervisor talks to
//! the runtime exclusively through this trait, so the runtime can be
//! swapped without touching the supervisor logic.
//!
//! Phase 0 ships [`MockRuntime`], which simulates container lifecycle
//! in memory. A containerd-backed implementation comes in a later phase.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use minga_state::types::{TaskId, TaskSpec};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("container `{0}` not found")]
    NotFound(String),
    #[error("failed to start container: {0}")]
    Start(String),
    #[error("failed to stop container: {0}")]
    Stop(String),
    #[error("runtime error: {0}")]
    Other(String),
}

/// Status of a running (or stopped) container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerStatus {
    Running,
    Stopped,
    Failed,
}

impl ContainerStatus {
    /// Whether the container is no longer running.
    pub fn is_terminated(&self) -> bool {
        matches!(self, ContainerStatus::Stopped | ContainerStatus::Failed)
    }
}

/// A container's current state together with the exit code of its last run.
///
/// The exit code is `None` while the container is still running, and is
/// `Some(0)` for a container that exited successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContainerState {
    pub status: ContainerStatus,
    pub exit_code: Option<i32>,
}

/// Information about a container known to the runtime.
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub id: String,
    pub task_id: TaskId,
    pub image: String,
    pub status: ContainerStatus,
}

/// A chunk of log output from a container.
#[derive(Debug, Clone)]
pub struct ContainerLog {
    pub stream: LogStream,
    pub data: Vec<u8>,
}

/// Which output stream a log chunk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// A container runtime. Implementations must be thread-safe.
#[async_trait]
pub trait Runtime: Send + Sync {
    /// Starts a container for the given task. Returns the container ID.
    async fn start(&self, task_id: &TaskId, spec: &TaskSpec) -> Result<String, RuntimeError>;

    /// Stops and removes a container.
    async fn stop(&self, container_id: &str) -> Result<(), RuntimeError>;

    /// Lists all containers known to this runtime.
    async fn list(&self) -> Vec<ContainerInfo>;

    /// Returns the status of a container, or `None` if not found.
    async fn status(&self, container_id: &str) -> Option<ContainerStatus>;

    /// Returns the status of a container together with its exit code.
    ///
    /// The exit code is `Some(code)` once the container has exited. This is
    /// what lets restart policies distinguish `on-failure` from `always`.
    async fn state(&self, container_id: &str) -> Option<ContainerState>;

    /// Returns recent log output for a container. `tail` limits the number
    /// of lines returned (0 = all available). Returns an empty vec if the
    /// container is not found or has no logs.
    async fn logs(&self, container_id: &str, tail: usize) -> Vec<ContainerLog>;
}

// --- MockRuntime ---------------------------------------------------------------

/// An in-memory mock runtime for testing. Simulates container lifecycle
/// without actually running anything.
///
/// Each `start()` call creates a fake container with a unique ID and
/// marks it as running. `stop()` marks it as stopped and removes it.
#[derive(Debug, Default)]
pub struct MockRuntime {
    containers: Mutex<HashMap<String, MockContainer>>,
    counter: Mutex<u64>,
    /// If true, `start()` returns an error (to test failure handling).
    pub fail_starts: Mutex<bool>,
    /// Container IDs that should report as `Failed` status.
    pub failed_containers: Mutex<HashSet<String>>,
    /// Exit codes for containers marked as failed. Defaults to 1.
    pub exit_codes: Mutex<HashMap<String, i32>>,
}

#[derive(Debug, Clone)]
struct MockContainer {
    id: String,
    task_id: TaskId,
    image: String,
    status: ContainerStatus,
}

impl MockRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of currently tracked containers.
    pub fn container_count(&self) -> usize {
        self.containers.lock().unwrap().len()
    }

    /// Mark a container as failed (for testing restart policies).
    pub fn mark_failed(&self, container_id: &str) {
        self.failed_containers
            .lock()
            .unwrap()
            .insert(container_id.to_string());
    }

    /// Mark a container as failed with a specific exit code.
    pub fn mark_failed_with_code(&self, container_id: &str, exit_code: i32) {
        self.failed_containers
            .lock()
            .unwrap()
            .insert(container_id.to_string());
        self.exit_codes
            .lock()
            .unwrap()
            .insert(container_id.to_string(), exit_code);
    }

    /// The exit code recorded for a failed container (default 1).
    fn failure_exit_code(&self, container_id: &str) -> i32 {
        self.exit_codes
            .lock()
            .unwrap()
            .get(container_id)
            .copied()
            .unwrap_or(1)
    }
}

#[async_trait]
impl Runtime for MockRuntime {
    async fn start(&self, task_id: &TaskId, spec: &TaskSpec) -> Result<String, RuntimeError> {
        if *self.fail_starts.lock().unwrap() {
            return Err(RuntimeError::Start("mock runtime: starts disabled".into()));
        }

        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        let id = format!("mock-{:04}", *counter);
        drop(counter);

        let container = MockContainer {
            id: id.clone(),
            task_id: task_id.clone(),
            image: spec.image.clone(),
            status: ContainerStatus::Running,
        };

        self.containers
            .lock()
            .unwrap()
            .insert(id.clone(), container);

        tracing::info!(task_id = %task_id, container = %id, image = %spec.image, "mock runtime: started container");
        Ok(id)
    }

    async fn stop(&self, container_id: &str) -> Result<(), RuntimeError> {
        let mut containers = self.containers.lock().unwrap();
        match containers.get_mut(container_id) {
            Some(c) => {
                tracing::info!(container = %container_id, task_id = %c.task_id, "mock runtime: stopped container");
                c.status = ContainerStatus::Stopped;
                containers.remove(container_id);
                Ok(())
            }
            None => Err(RuntimeError::NotFound(container_id.to_string())),
        }
    }

    async fn list(&self) -> Vec<ContainerInfo> {
        self.containers
            .lock()
            .unwrap()
            .values()
            .map(|c| ContainerInfo {
                id: c.id.clone(),
                task_id: c.task_id.clone(),
                image: c.image.clone(),
                status: c.status,
            })
            .collect()
    }

    async fn status(&self, container_id: &str) -> Option<ContainerStatus> {
        // Check if this container has been marked as failed.
        if self
            .failed_containers
            .lock()
            .unwrap()
            .contains(container_id)
        {
            return Some(ContainerStatus::Failed);
        }
        self.containers
            .lock()
            .unwrap()
            .get(container_id)
            .map(|c| c.status)
    }

    async fn state(&self, container_id: &str) -> Option<ContainerState> {
        let status = self.status(container_id).await?;
        // A failed container exited non-zero; a stopped one exited zero.
        let exit_code = match status {
            ContainerStatus::Running => None,
            ContainerStatus::Failed => Some(self.failure_exit_code(container_id)),
            ContainerStatus::Stopped => Some(0),
        };
        Some(ContainerState { status, exit_code })
    }

    async fn logs(&self, container_id: &str, tail: usize) -> Vec<ContainerLog> {
        let containers = self.containers.lock().unwrap();
        let Some(c) = containers.get(container_id) else {
            return Vec::new();
        };
        // Generate synthetic log lines for the mock container.
        let lines = vec![
            ContainerLog {
                stream: LogStream::Stdout,
                data: format!("[{}] starting {} (mock)\n", c.task_id, c.image).into_bytes(),
            },
            ContainerLog {
                stream: LogStream::Stdout,
                data: format!("[{}] listening on 0.0.0.0:8080\n", c.task_id).into_bytes(),
            },
            ContainerLog {
                stream: LogStream::Stdout,
                data: format!("[{}] ready (mock)\n", c.task_id).into_bytes(),
            },
        ];
        if tail > 0 && tail < lines.len() {
            lines[lines.len() - tail..].to_vec()
        } else {
            lines
        }
    }
}

/// Convenience: wrap a runtime in `Arc` for sharing.
pub fn shared_runtime<R: Runtime + 'static>(runtime: R) -> Arc<dyn Runtime> {
    Arc::new(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(image: &str) -> TaskSpec {
        TaskSpec {
            image: image.to_string(),
            env: HashMap::new(),
            ports: Vec::new(),
            resources: Default::default(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn mock_start_and_stop() {
        let rt = MockRuntime::new();
        let id = rt
            .start(&"task-1".to_string(), &spec("nginx:latest"))
            .await
            .unwrap();
        assert_eq!(rt.container_count(), 1);
        assert_eq!(rt.status(&id).await, Some(ContainerStatus::Running));

        rt.stop(&id).await.unwrap();
        assert_eq!(rt.container_count(), 0);
        assert_eq!(rt.status(&id).await, None);
    }

    #[tokio::test]
    async fn mock_unique_ids() {
        let rt = MockRuntime::new();
        let id1 = rt.start(&"t1".to_string(), &spec("a")).await.unwrap();
        let id2 = rt.start(&"t2".to_string(), &spec("b")).await.unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn mock_stop_missing() {
        let rt = MockRuntime::new();
        assert!(rt.stop("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn mock_fail_starts() {
        let rt = MockRuntime::new();
        *rt.fail_starts.lock().unwrap() = true;
        assert!(rt.start(&"t1".to_string(), &spec("a")).await.is_err());
    }

    #[tokio::test]
    async fn mock_list() {
        let rt = MockRuntime::new();
        rt.start(&"t1".to_string(), &spec("a")).await.unwrap();
        rt.start(&"t2".to_string(), &spec("b")).await.unwrap();
        let containers = rt.list().await;
        assert_eq!(containers.len(), 2);
        assert!(containers
            .iter()
            .all(|c| c.status == ContainerStatus::Running));
    }

    #[tokio::test]
    async fn mock_logs_returns_synthetic_output() {
        let rt = MockRuntime::new();
        let id = rt
            .start(&"t1".to_string(), &spec("nginx:latest"))
            .await
            .unwrap();

        let logs = rt.logs(&id, 0).await;
        assert_eq!(logs.len(), 3);
        assert!(logs.iter().all(|l| l.stream == LogStream::Stdout));
        assert!(String::from_utf8_lossy(&logs[0].data).contains("starting"));
        assert!(String::from_utf8_lossy(&logs[2].data).contains("ready"));
    }

    #[tokio::test]
    async fn mock_logs_tail_limits_output() {
        let rt = MockRuntime::new();
        let id = rt.start(&"t1".to_string(), &spec("a")).await.unwrap();

        let logs = rt.logs(&id, 1).await;
        assert_eq!(logs.len(), 1);
        assert!(String::from_utf8_lossy(&logs[0].data).contains("ready"));
    }

    #[tokio::test]
    async fn mock_logs_missing_container() {
        let rt = MockRuntime::new();
        let logs = rt.logs("nonexistent", 0).await;
        assert!(logs.is_empty());
    }
}
