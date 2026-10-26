//! Container runtime abstraction.
//!
//! The [`Runtime`] trait abstracts over the container runtime (containerd
//! in production, [`MockRuntime`] for testing). The supervisor talks to
//! the runtime exclusively through this trait, so the runtime can be
//! swapped without touching the supervisor logic.
//!
//! Phase 0 ships [`MockRuntime`], which simulates container lifecycle
//! in memory. A containerd-backed implementation comes in a later phase.

use std::collections::HashMap;
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

/// Information about a container known to the runtime.
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub id: String,
    pub task_id: TaskId,
    pub image: String,
    pub status: ContainerStatus,
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
        self.containers
            .lock()
            .unwrap()
            .get(container_id)
            .map(|c| c.status)
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
}
