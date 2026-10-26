//! Node supervisor: the reconcile loop that runs tasks on this node.
//!
//! The supervisor runs on every node (manager and worker). It reads tasks
//! assigned to this node from the Raft state store, starts containers for
//! new tasks, and stops containers for removed tasks.
//!
//! Task status (running, failed, etc.) is tracked locally — it does NOT
//! go through Raft. Per the architecture principle: "Only desired state
//! and placement go through Raft. Heartbeats, task status and agent run
//! steps go through JetStream."
//!
//! # Reconcile loop
//!
//! ```text
//! loop {
//!     tasks = state.list_tasks()           // all tasks in the cluster
//!     my_tasks = tasks.filter(node_id == self.node_id)
//!     running = local tracking of started containers
//!
//!     // Start containers for new tasks
//!     for task in my_tasks where task.id not in running:
//!         container_id = runtime.start(task.id, task.spec)
//!         running[task.id] = container_id
//!
//!     // Stop containers for removed tasks
//!     for task_id in running where task_id not in my_tasks:
//!         runtime.stop(running[task_id])
//!         running.remove(task_id)
//!
//!     sleep(interval)
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_state::types::{NodeId, Task, TaskId};
use minga_state::{StateError, StateStore};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::runtime::{Runtime, RuntimeError};

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
}

/// Tracks which tasks have been started on this node and their container IDs.
type RunningTasks = Arc<Mutex<HashMap<TaskId, String>>>;

/// The node supervisor. Runs a reconcile loop that converges the local
/// container state with the tasks assigned to this node in Raft.
pub struct Supervisor {
    state: Arc<StateStore>,
    runtime: Arc<dyn Runtime>,
    node_id: NodeId,
    running: RunningTasks,
}

impl Supervisor {
    pub fn new(state: Arc<StateStore>, runtime: Arc<dyn Runtime>, node_id: NodeId) -> Self {
        Self {
            state,
            runtime,
            node_id,
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the set of task IDs currently running on this node.
    pub async fn running_tasks(&self) -> Vec<TaskId> {
        self.running.lock().await.keys().cloned().collect()
    }

    /// Returns the number of containers currently running.
    pub async fn running_count(&self) -> usize {
        self.running.lock().await.len()
    }

    /// Runs one reconciliation pass. Returns the number of actions taken
    /// (containers started + stopped).
    pub async fn reconcile_once(&self) -> Result<usize, SupervisorError> {
        let all_tasks = self.state.list_tasks().await?;
        let my_task_ids: HashSet<TaskId> = all_tasks
            .iter()
            .filter(|t| t.node_id == self.node_id)
            .map(|t| t.id.clone())
            .collect();
        let my_tasks: Vec<&Task> = all_tasks
            .iter()
            .filter(|t| t.node_id == self.node_id)
            .collect();

        let mut running = self.running.lock().await;
        let mut actions = 0;

        // --- start containers for new tasks -----------------------------------
        for task in &my_tasks {
            if !running.contains_key(&task.id) {
                match self.runtime.start(&task.id, &task.spec).await {
                    Ok(container_id) => {
                        tracing::info!(
                            task = %task.id,
                            container = %container_id,
                            node = self.node_id,
                            "started container for task"
                        );
                        running.insert(task.id.clone(), container_id);
                        actions += 1;
                    }
                    Err(e) => {
                        tracing::error!(
                            task = %task.id,
                            error = %e,
                            "failed to start container"
                        );
                    }
                }
            }
        }

        // --- stop containers for removed tasks --------------------------------
        let to_stop: Vec<(TaskId, String)> = running
            .iter()
            .filter(|(task_id, _)| !my_task_ids.contains(*task_id))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (task_id, container_id) in to_stop {
            match self.runtime.stop(&container_id).await {
                Ok(()) => {
                    tracing::info!(
                        task = %task_id,
                        container = %container_id,
                        node = self.node_id,
                        "stopped container for removed task"
                    );
                    running.remove(&task_id);
                    actions += 1;
                }
                Err(e) => {
                    tracing::error!(
                        task = %task_id,
                        container = %container_id,
                        error = %e,
                        "failed to stop container"
                    );
                }
            }
        }

        Ok(actions)
    }

    /// Runs the supervisor loop forever, sleeping `interval` between passes.
    pub async fn run(self, interval: Duration) -> ! {
        tracing::info!(node = self.node_id, ?interval, "supervisor loop starting");
        loop {
            match self.reconcile_once().await {
                Ok(0) => {
                    tracing::trace!(node = self.node_id, "supervisor: no changes");
                }
                Ok(n) => {
                    tracing::debug!(
                        node = self.node_id,
                        actions = n,
                        "supervisor: applied changes"
                    );
                }
                Err(e) => {
                    tracing::error!(node = self.node_id, error = %e, "supervisor pass failed");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MockRuntime;
    use minga_state::types::{PortMapping, ResourceSpec, TaskSpec, TaskState};
    use std::collections::BTreeMap;

    fn make_task(id: &str, node_id: u64) -> Task {
        Task {
            id: id.to_string(),
            service_id: "svc".to_string(),
            node_id,
            state: TaskState::Pending,
            spec: TaskSpec {
                image: "nginx:latest".to_string(),
                env: HashMap::new(),
                ports: vec![PortMapping {
                    target: 80,
                    published: None,
                    protocol: "tcp".to_string(),
                }],
                resources: ResourceSpec::default(),
            },
        }
    }

    async fn setup(node_id: u64) -> (Supervisor, Arc<MockRuntime>, Arc<StateStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(node_id, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        // Leak tempdir — store needs it for the test duration.
        std::mem::forget(dir);

        let runtime = Arc::new(MockRuntime::new());
        let supervisor = Supervisor::new(store.clone(), runtime.clone(), node_id);
        (supervisor, runtime, store)
    }

    #[tokio::test]
    async fn starts_container_for_assigned_task() {
        let (supervisor, runtime, store) = setup(1).await;

        store.upsert_task(make_task("t1", 1)).await.unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(runtime.container_count(), 1);
        assert_eq!(supervisor.running_count().await, 1);
    }

    #[tokio::test]
    async fn ignores_tasks_for_other_nodes() {
        let (supervisor, runtime, store) = setup(1).await;

        store.upsert_task(make_task("t1", 2)).await.unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(runtime.container_count(), 0);
    }

    #[tokio::test]
    async fn stops_container_when_task_removed() {
        let (supervisor, runtime, store) = setup(1).await;

        store.upsert_task(make_task("t1", 1)).await.unwrap();
        supervisor.reconcile_once().await.unwrap();
        assert_eq!(runtime.container_count(), 1);

        store.remove_task("t1".to_string()).await.unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(runtime.container_count(), 0);
        assert_eq!(supervisor.running_count().await, 0);
    }

    #[tokio::test]
    async fn idempotent_no_changes() {
        let (supervisor, runtime, store) = setup(1).await;

        store.upsert_task(make_task("t1", 1)).await.unwrap();
        supervisor.reconcile_once().await.unwrap();
        assert_eq!(runtime.container_count(), 1);

        // Second pass: no changes.
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(runtime.container_count(), 1);
    }

    #[tokio::test]
    async fn multiple_tasks() {
        let (supervisor, runtime, store) = setup(1).await;

        store.upsert_task(make_task("t1", 1)).await.unwrap();
        store.upsert_task(make_task("t2", 1)).await.unwrap();
        store.upsert_task(make_task("t3", 2)).await.unwrap(); // other node

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 2);
        assert_eq!(runtime.container_count(), 2);

        let running = supervisor.running_tasks().await;
        assert!(running.contains(&"t1".to_string()));
        assert!(running.contains(&"t2".to_string()));
        assert!(!running.contains(&"t3".to_string()));
    }

    #[tokio::test]
    async fn start_failure_does_not_crash() {
        let (supervisor, runtime, store) = setup(1).await;
        *runtime.fail_starts.lock().unwrap() = true;

        store.upsert_task(make_task("t1", 1)).await.unwrap();
        // Should not panic, just log the error.
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(supervisor.running_count().await, 0);

        // Re-enable starts and retry.
        *runtime.fail_starts.lock().unwrap() = false;
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
    }
}
