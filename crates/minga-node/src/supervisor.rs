//! Node supervisor: the reconcile loop that runs tasks on this node.
//!
//! The supervisor runs on every node (manager and worker). It receives
//! task assign/stop commands via the event bus, executes them through
//! the container [`Runtime`], and reports actual state back over the bus.
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
//!     // Bus path: pull commands, refresh lease, publish status
//!     bus.refresh_lease(node_id)
//!     while let Some(cmd) = bus.next_command(node_id) {
//!         match cmd {
//!             Assign { task_id, spec } => runtime.start(task_id, spec),
//!             Stop { task_id } => runtime.stop(task_id),
//!         }
//!     }
//!     for task in running_tasks {
//!         bus.put_status(task.{id}, TaskStatusReport::running(task))
//!     }
//!
//!     // Raft fallback: reconcile with desired state (catch-up after restart)
//!     tasks = state.list_tasks().filter(node_id == self.node_id)
//!     for task in tasks where task.id not in running {
//!         runtime.start(task.id, task.spec)
//!     }
//!     for task_id in running where task_id not in tasks {
//!         runtime.stop(task_id)
//!     }
//!
//!     sleep(interval)
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{subjects, Command, EventBus, TaskActualState, TaskStatusReport};
use minga_state::types::{NodeId, Task, TaskId, TaskSpec, WorkloadKind};
use minga_state::{StateError, StateStore};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::agent::{AgentError, AgentExecutor};
use crate::runtime::{Runtime, RuntimeError};

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("agent error: {0}")]
    Agent(#[from] AgentError),
    #[error("bus error: {0}")]
    Bus(String),
}

/// Tracks which tasks have been started on this node and their container IDs.
type RunningTasks = Arc<Mutex<HashMap<TaskId, String>>>;

/// Tracks task IDs that were assigned via the bus (vs Raft). The Raft
/// fallback reconcile only stops containers for Raft-assigned tasks;
/// bus-assigned tasks are stopped via bus Stop commands.
type BusAssignedTasks = Arc<Mutex<HashSet<TaskId>>>;

/// Tracks agent task IDs currently running on this node.
type RunningAgents = Arc<Mutex<HashSet<TaskId>>>;

/// The node supervisor. Runs a reconcile loop that converges the local
/// container/agent state with the tasks assigned to this node.
///
/// When constructed with [`Supervisor::with_bus`], the supervisor also
/// receives commands via the event bus and reports status back over it.
///
/// When constructed with [`Supervisor::with_agent_executor`], the
/// supervisor can also run agent (WASM sandbox) workloads.
pub struct Supervisor {
    state: Arc<StateStore>,
    runtime: Arc<dyn Runtime>,
    agent_executor: Option<Arc<dyn AgentExecutor>>,
    bus: Option<Arc<dyn EventBus>>,
    node_id: NodeId,
    running: RunningTasks,
    running_agents: RunningAgents,
    bus_assigned: BusAssignedTasks,
}

impl Supervisor {
    pub fn new(state: Arc<StateStore>, runtime: Arc<dyn Runtime>, node_id: NodeId) -> Self {
        Self {
            state,
            runtime,
            agent_executor: None,
            bus: None,
            node_id,
            running: Arc::new(Mutex::new(HashMap::new())),
            running_agents: Arc::new(Mutex::new(HashSet::new())),
            bus_assigned: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Create a supervisor that receives commands and reports status
    /// via the event bus.
    pub fn with_bus(
        state: Arc<StateStore>,
        runtime: Arc<dyn Runtime>,
        bus: Arc<dyn EventBus>,
        node_id: NodeId,
    ) -> Self {
        Self {
            state,
            runtime,
            agent_executor: None,
            bus: Some(bus),
            node_id,
            running: Arc::new(Mutex::new(HashMap::new())),
            running_agents: Arc::new(Mutex::new(HashSet::new())),
            bus_assigned: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Create a supervisor with bus and agent executor support.
    pub fn with_agent_executor(
        state: Arc<StateStore>,
        runtime: Arc<dyn Runtime>,
        bus: Arc<dyn EventBus>,
        agent_executor: Arc<dyn AgentExecutor>,
        node_id: NodeId,
    ) -> Self {
        Self {
            state,
            runtime,
            agent_executor: Some(agent_executor),
            bus: Some(bus),
            node_id,
            running: Arc::new(Mutex::new(HashMap::new())),
            running_agents: Arc::new(Mutex::new(HashSet::new())),
            bus_assigned: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Returns the set of task IDs currently running on this node (containers + agents).
    pub async fn running_tasks(&self) -> Vec<TaskId> {
        let mut tasks: Vec<TaskId> = self.running.lock().await.keys().cloned().collect();
        tasks.extend(self.running_agents.lock().await.iter().cloned());
        tasks
    }

    /// Returns the number of containers + agents currently running.
    pub async fn running_count(&self) -> usize {
        self.running.lock().await.len() + self.running_agents.lock().await.len()
    }

    /// Returns the number of agents currently running.
    pub async fn running_agent_count(&self) -> usize {
        self.running_agents.lock().await.len()
    }

    /// Runs one reconciliation pass. Returns the number of actions taken
    /// (containers started + stopped).
    pub async fn reconcile_once(&self) -> Result<usize, SupervisorError> {
        let mut actions = 0;

        // --- Bus path: commands, lease, status --------------------------------
        if let Some(bus) = &self.bus {
            actions += self.bus_reconcile(bus).await;
        }

        // --- Raft fallback: reconcile with desired state ----------------------
        actions += self.raft_reconcile().await?;

        Ok(actions)
    }

    /// Bus reconcile: refresh lease, pull commands, publish status.
    async fn bus_reconcile(&self, bus: &Arc<dyn EventBus>) -> usize {
        let mut actions = 0;

        // Refresh liveness lease.
        if let Err(e) = bus.refresh_lease(self.node_id).await {
            tracing::warn!(node = self.node_id, error = %e, "failed to refresh lease");
        }

        // Pull commands (non-blocking, 100ms timeout per pull).
        loop {
            match tokio::time::timeout(Duration::from_millis(100), bus.next_command(self.node_id))
                .await
            {
                Ok(Ok(cmd)) => {
                    actions += self.execute_command(cmd).await;
                }
                Ok(Err(e)) => {
                    tracing::error!(node = self.node_id, error = %e, "bus command error");
                    break;
                }
                Err(_) => break, // timeout — no more commands
            }
        }

        // Publish status reports for running tasks.
        self.publish_status(bus).await;

        actions
    }

    /// Execute a single command received from the bus.
    async fn execute_command(&self, cmd: Command) -> usize {
        match cmd {
            Command::Assign { task_id, payload } => {
                // Deserialize the task spec.
                let spec: TaskSpec = match bincode::serde::decode_from_slice(
                    &payload,
                    bincode::config::standard(),
                ) {
                    Ok((spec, _)) => spec,
                    Err(e) => {
                        tracing::error!(
                            task = %task_id,
                            error = %e,
                            "failed to deserialize task spec from command"
                        );
                        return 0;
                    }
                };

                // Skip if already running (container or agent).
                {
                    let running = self.running.lock().await;
                    let agents = self.running_agents.lock().await;
                    if running.contains_key(&task_id) || agents.contains(&task_id) {
                        return 0;
                    }
                }

                // Dispatch by workload kind.
                match &spec.kind {
                    WorkloadKind::Container => match self.runtime.start(&task_id, &spec).await {
                        Ok(container_id) => {
                            tracing::info!(
                                task = %task_id,
                                container = %container_id,
                                node = self.node_id,
                                "started container (bus command)"
                            );
                            self.running
                                .lock()
                                .await
                                .insert(task_id.clone(), container_id);
                            self.bus_assigned.lock().await.insert(task_id);
                            1
                        }
                        Err(e) => {
                            tracing::error!(
                                task = %task_id,
                                error = %e,
                                "failed to start container (bus command)"
                            );
                            0
                        }
                    },
                    WorkloadKind::Agent(agent_spec) => {
                        let Some(exec) = &self.agent_executor else {
                            tracing::error!(
                                task = %task_id,
                                "agent task assigned but no agent executor configured"
                            );
                            return 0;
                        };
                        match exec.start(&task_id, agent_spec).await {
                            Ok(agent_id) => {
                                tracing::info!(
                                    task = %task_id,
                                    agent = %agent_id,
                                    node = self.node_id,
                                    "started agent (bus command)"
                                );
                                self.running_agents.lock().await.insert(task_id.clone());
                                self.bus_assigned.lock().await.insert(task_id);
                                1
                            }
                            Err(e) => {
                                tracing::error!(
                                    task = %task_id,
                                    error = %e,
                                    "failed to start agent (bus command)"
                                );
                                0
                            }
                        }
                    }
                }
            }
            Command::Stop { task_id } => {
                self.bus_assigned.lock().await.remove(&task_id);

                // Check if it's a container or an agent.
                let container_id = self.running.lock().await.remove(&task_id);
                let was_agent = self.running_agents.lock().await.remove(&task_id);

                if let Some(id) = container_id {
                    match self.runtime.stop(&id).await {
                        Ok(()) => {
                            tracing::info!(
                                task = %task_id,
                                container = %id,
                                node = self.node_id,
                                "stopped container (bus command)"
                            );
                            1
                        }
                        Err(e) => {
                            tracing::error!(
                                task = %task_id,
                                container = %id,
                                error = %e,
                                "failed to stop container (bus command)"
                            );
                            0
                        }
                    }
                } else if was_agent {
                    if let Some(exec) = &self.agent_executor {
                        match exec.stop(&task_id).await {
                            Ok(()) => {
                                tracing::info!(
                                    task = %task_id,
                                    node = self.node_id,
                                    "stopped agent (bus command)"
                                );
                                1
                            }
                            Err(e) => {
                                tracing::error!(
                                    task = %task_id,
                                    error = %e,
                                    "failed to stop agent (bus command)"
                                );
                                0
                            }
                        }
                    } else {
                        1
                    }
                } else {
                    0
                }
            }
        }
    }

    /// Publish status reports for all running tasks (containers + agents) to the bus KV.
    async fn publish_status(&self, bus: &Arc<dyn EventBus>) {
        let running = self.running.lock().await;
        for (task_id, container_id) in running.iter() {
            let report = TaskStatusReport {
                task_id: task_id.clone(),
                node_id: self.node_id,
                state: TaskActualState::Running,
                container_id: Some(container_id.clone()),
                observed_at: now_millis(),
            };
            self.publish_report(bus, &report).await;
        }
        drop(running);

        // Publish agent status.
        let agents = self.running_agents.lock().await;
        for task_id in agents.iter() {
            // Check if the agent has completed.
            let state = if let Some(exec) = &self.agent_executor {
                match exec.status(task_id).await {
                    Some(crate::agent::AgentStatus::Completed) => TaskActualState::Stopped,
                    Some(crate::agent::AgentStatus::Failed) => TaskActualState::Failed,
                    _ => TaskActualState::Running,
                }
            } else {
                TaskActualState::Running
            };

            let report = TaskStatusReport {
                task_id: task_id.clone(),
                node_id: self.node_id,
                state,
                container_id: None,
                observed_at: now_millis(),
            };
            self.publish_report(bus, &report).await;

            // Remove completed/failed agents from the running set.
            if state != TaskActualState::Running {
                // Will be cleaned up on next reconcile pass.
            }
        }
    }

    async fn publish_report(&self, bus: &Arc<dyn EventBus>, report: &TaskStatusReport) {
        match report.to_bytes() {
            Ok(bytes) => {
                let key = subjects::kv_task_status(&report.task_id);
                if let Err(e) = bus.put_status(&key, &bytes).await {
                    tracing::warn!(
                        task = %report.task_id,
                        error = %e,
                        "failed to publish task status"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(task = %report.task_id, error = %e, "failed to serialize status");
            }
        }
    }

    /// Raft fallback reconcile: read tasks from the state store and
    /// converge local containers and agents. Catches up after a supervisor
    /// restart when bus commands may have been missed.
    async fn raft_reconcile(&self) -> Result<usize, SupervisorError> {
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
        let mut running_agents = self.running_agents.lock().await;
        let mut actions = 0;

        // --- start containers/agents for new tasks ----------------------------
        for task in &my_tasks {
            let already_container = running.contains_key(&task.id);
            let already_agent = running_agents.contains(&task.id);
            if already_container || already_agent {
                continue;
            }

            match &task.spec.kind {
                WorkloadKind::Container => match self.runtime.start(&task.id, &task.spec).await {
                    Ok(container_id) => {
                        tracing::info!(
                            task = %task.id,
                            container = %container_id,
                            node = self.node_id,
                            "started container (raft reconcile)"
                        );
                        running.insert(task.id.clone(), container_id);
                        actions += 1;
                    }
                    Err(e) => {
                        tracing::error!(
                            task = %task.id,
                            error = %e,
                            "failed to start container (raft reconcile)"
                        );
                    }
                },
                WorkloadKind::Agent(agent_spec) => {
                    let Some(exec) = &self.agent_executor else {
                        tracing::error!(
                            task = %task.id,
                            "agent task in state but no agent executor configured"
                        );
                        continue;
                    };
                    match exec.start(&task.id, agent_spec).await {
                        Ok(agent_id) => {
                            tracing::info!(
                                task = %task.id,
                                agent = %agent_id,
                                node = self.node_id,
                                "started agent (raft reconcile)"
                            );
                            running_agents.insert(task.id.clone());
                            actions += 1;
                        }
                        Err(e) => {
                            tracing::error!(
                                task = %task.id,
                                error = %e,
                                "failed to start agent (raft reconcile)"
                            );
                        }
                    }
                }
            }
        }

        // --- stop containers for removed tasks --------------------------------
        // Only stop Raft-assigned tasks. Bus-assigned tasks are stopped
        // via bus Stop commands.
        let bus_assigned = self.bus_assigned.lock().await;
        let to_stop_containers: Vec<(TaskId, String)> = running
            .iter()
            .filter(|(task_id, _)| {
                !my_task_ids.contains(*task_id) && !bus_assigned.contains(*task_id)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let to_stop_agents: Vec<TaskId> = running_agents
            .iter()
            .filter(|task_id| !my_task_ids.contains(*task_id) && !bus_assigned.contains(*task_id))
            .cloned()
            .collect();
        drop(bus_assigned);

        for (task_id, container_id) in to_stop_containers {
            match self.runtime.stop(&container_id).await {
                Ok(()) => {
                    tracing::info!(
                        task = %task_id,
                        container = %container_id,
                        node = self.node_id,
                        "stopped container for removed task (raft reconcile)"
                    );
                    running.remove(&task_id);
                    actions += 1;
                }
                Err(e) => {
                    tracing::error!(
                        task = %task_id,
                        container = %container_id,
                        error = %e,
                        "failed to stop container (raft reconcile)"
                    );
                }
            }
        }

        for task_id in to_stop_agents {
            if let Some(exec) = &self.agent_executor {
                match exec.stop(&task_id).await {
                    Ok(()) => {
                        tracing::info!(
                            task = %task_id,
                            node = self.node_id,
                            "stopped agent for removed task (raft reconcile)"
                        );
                        running_agents.remove(&task_id);
                        actions += 1;
                    }
                    Err(e) => {
                        tracing::error!(
                            task = %task_id,
                            error = %e,
                            "failed to stop agent (raft reconcile)"
                        );
                    }
                }
            } else {
                running_agents.remove(&task_id);
                actions += 1;
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

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MockRuntime;
    use minga_bus::MemoryBus;
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
                ..Default::default()
            },
            ips: HashMap::new(),
        }
    }

    fn make_spec() -> TaskSpec {
        TaskSpec {
            image: "nginx:latest".to_string(),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            ..Default::default()
        }
    }

    fn spec_to_payload(spec: &TaskSpec) -> Vec<u8> {
        bincode::serde::encode_to_vec(spec, bincode::config::standard()).unwrap()
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

    async fn setup_with_bus(
        node_id: u64,
    ) -> (
        Supervisor,
        Arc<MockRuntime>,
        Arc<StateStore>,
        Arc<MemoryBus>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(node_id, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        std::mem::forget(dir);

        let runtime = Arc::new(MockRuntime::new());
        let bus = MemoryBus::new();
        let supervisor = Supervisor::with_bus(store.clone(), runtime.clone(), bus.clone(), node_id);
        (supervisor, runtime, store, bus)
    }

    // --- Existing Raft-based tests -------------------------------------------

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

    // --- Bus integration tests -----------------------------------------------

    #[tokio::test]
    async fn bus_assign_command_starts_container() {
        let (supervisor, runtime, _store, bus) = setup_with_bus(1).await;

        // Send an assign command via the bus.
        let cmd = Command::Assign {
            task_id: "t1".to_string(),
            payload: spec_to_payload(&make_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(runtime.container_count(), 1);
        assert_eq!(supervisor.running_count().await, 1);
    }

    #[tokio::test]
    async fn bus_stop_command_stops_container() {
        let (supervisor, runtime, _store, bus) = setup_with_bus(1).await;

        // Start via bus command.
        let cmd = Command::Assign {
            task_id: "t1".to_string(),
            payload: spec_to_payload(&make_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        supervisor.reconcile_once().await.unwrap();
        assert_eq!(runtime.container_count(), 1);

        // Stop via bus command.
        let stop = Command::Stop {
            task_id: "t1".to_string(),
        };
        bus.send_command(1, &stop).await.unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(runtime.container_count(), 0);
        assert_eq!(supervisor.running_count().await, 0);
    }

    #[tokio::test]
    async fn bus_command_for_other_node_ignored() {
        let (supervisor, runtime, _store, bus) = setup_with_bus(1).await;

        // Send command to node 2 — supervisor on node 1 should not receive it.
        let cmd = Command::Assign {
            task_id: "t1".to_string(),
            payload: spec_to_payload(&make_spec()),
        };
        bus.send_command(2, &cmd).await.unwrap();

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(runtime.container_count(), 0);
    }

    #[tokio::test]
    async fn bus_refreshes_lease() {
        let (supervisor, _runtime, _store, bus) = setup_with_bus(1).await;

        assert!(!bus.is_alive(1).await.unwrap());
        supervisor.reconcile_once().await.unwrap();
        assert!(bus.is_alive(1).await.unwrap());
    }

    #[tokio::test]
    async fn bus_publishes_task_status() {
        let (supervisor, _runtime, _store, bus) = setup_with_bus(1).await;

        // Start a container via bus command.
        let cmd = Command::Assign {
            task_id: "t1".to_string(),
            payload: spec_to_payload(&make_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        supervisor.reconcile_once().await.unwrap();

        // Status should be published to the KV.
        let key = subjects::kv_task_status("t1");
        let status_bytes = bus.get_status(&key).await.unwrap();
        assert!(status_bytes.is_some(), "task status should be in KV");

        let report = TaskStatusReport::from_bytes(&status_bytes.unwrap()).unwrap();
        assert_eq!(report.task_id, "t1");
        assert_eq!(report.node_id, 1);
        assert_eq!(report.state, TaskActualState::Running);
    }

    #[tokio::test]
    async fn bus_and_raft_paths_coexist() {
        // A task assigned via Raft should be started by the raft reconcile,
        // and a task assigned via bus should be started by the bus path.
        // Both should coexist without double-starting.
        let (supervisor, runtime, store, bus) = setup_with_bus(1).await;

        // Task via Raft.
        store.upsert_task(make_task("raft-task", 1)).await.unwrap();

        // Task via bus.
        let cmd = Command::Assign {
            task_id: "bus-task".to_string(),
            payload: spec_to_payload(&make_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 2);
        assert_eq!(runtime.container_count(), 2);

        // Second pass: no double-start.
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(runtime.container_count(), 2);
    }

    #[tokio::test]
    async fn bus_assign_is_idempotent() {
        let (supervisor, runtime, _store, bus) = setup_with_bus(1).await;

        let cmd = Command::Assign {
            task_id: "t1".to_string(),
            payload: spec_to_payload(&make_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        bus.send_command(1, &cmd).await.unwrap(); // duplicate

        supervisor.reconcile_once().await.unwrap();
        assert_eq!(runtime.container_count(), 1);

        // Second pass: no additional starts.
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(runtime.container_count(), 1);
    }

    // --- Agent dispatch tests --------------------------------------------------

    use crate::agent::MockAgentExecutor;
    use minga_state::types::{AgentSpec, WorkloadKind};

    fn make_agent_task(id: &str, node_id: u64) -> Task {
        Task {
            id: id.to_string(),
            service_id: "svc".to_string(),
            node_id,
            state: TaskState::Pending,
            spec: TaskSpec {
                image: String::new(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec::default(),
                kind: WorkloadKind::Agent(AgentSpec {
                    module_ref: "./agent.wasm".to_string(),
                    ..Default::default()
                }),
            },
            ips: HashMap::new(),
        }
    }

    fn make_agent_spec() -> TaskSpec {
        TaskSpec {
            image: String::new(),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            kind: WorkloadKind::Agent(AgentSpec {
                module_ref: "./agent.wasm".to_string(),
                ..Default::default()
            }),
        }
    }

    async fn setup_with_agent(
        node_id: u64,
    ) -> (
        Supervisor,
        Arc<MockRuntime>,
        Arc<MockAgentExecutor>,
        Arc<StateStore>,
        Arc<MemoryBus>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(node_id, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        std::mem::forget(dir);

        let runtime = Arc::new(MockRuntime::new());
        let agent_exec = Arc::new(MockAgentExecutor::new());
        let bus = MemoryBus::new();
        let supervisor = Supervisor::with_agent_executor(
            store.clone(),
            runtime.clone(),
            bus.clone(),
            agent_exec.clone(),
            node_id,
        );
        (supervisor, runtime, agent_exec, store, bus)
    }

    #[tokio::test]
    async fn bus_assign_agent_starts_agent() {
        let (supervisor, runtime, agent_exec, _store, bus) = setup_with_agent(1).await;

        let cmd = Command::Assign {
            task_id: "agent-1".to_string(),
            payload: spec_to_payload(&make_agent_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(agent_exec.agent_count(), 1);
        assert_eq!(
            runtime.container_count(),
            0,
            "no container should be started"
        );
        assert_eq!(supervisor.running_agent_count().await, 1);
    }

    #[tokio::test]
    async fn bus_stop_agent_stops_agent() {
        let (supervisor, _runtime, agent_exec, _store, bus) = setup_with_agent(1).await;

        // Start agent via bus.
        let cmd = Command::Assign {
            task_id: "agent-1".to_string(),
            payload: spec_to_payload(&make_agent_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        supervisor.reconcile_once().await.unwrap();
        assert_eq!(agent_exec.agent_count(), 1);

        // Stop via bus.
        let stop = Command::Stop {
            task_id: "agent-1".to_string(),
        };
        bus.send_command(1, &stop).await.unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(agent_exec.agent_count(), 0);
        assert_eq!(supervisor.running_agent_count().await, 0);
    }

    #[tokio::test]
    async fn raft_reconcile_starts_agent_task() {
        let (supervisor, runtime, agent_exec, store, _bus) = setup_with_agent(1).await;

        store
            .upsert_task(make_agent_task("agent-1", 1))
            .await
            .unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(agent_exec.agent_count(), 1);
        assert_eq!(runtime.container_count(), 0);
    }

    #[tokio::test]
    async fn raft_reconcile_stops_agent_when_task_removed() {
        let (supervisor, _runtime, agent_exec, store, _bus) = setup_with_agent(1).await;

        store
            .upsert_task(make_agent_task("agent-1", 1))
            .await
            .unwrap();
        supervisor.reconcile_once().await.unwrap();
        assert_eq!(agent_exec.agent_count(), 1);

        store.remove_task("agent-1".to_string()).await.unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 1);
        assert_eq!(agent_exec.agent_count(), 0);
    }

    #[tokio::test]
    async fn mixed_container_and_agent_tasks() {
        let (supervisor, runtime, agent_exec, store, bus) = setup_with_agent(1).await;

        // Container task via Raft.
        store
            .upsert_task(make_task("container-1", 1))
            .await
            .unwrap();

        // Agent task via bus.
        let cmd = Command::Assign {
            task_id: "agent-1".to_string(),
            payload: spec_to_payload(&make_agent_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 2);
        assert_eq!(runtime.container_count(), 1);
        assert_eq!(agent_exec.agent_count(), 1);
        assert_eq!(supervisor.running_count().await, 2);
    }

    #[tokio::test]
    async fn agent_task_without_executor_is_skipped() {
        // Supervisor without agent executor should skip agent tasks.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        std::mem::forget(dir);

        let runtime = Arc::new(MockRuntime::new());
        let bus = MemoryBus::new();
        let supervisor = Supervisor::with_bus(store.clone(), runtime.clone(), bus, 1);

        store
            .upsert_task(make_agent_task("agent-1", 1))
            .await
            .unwrap();
        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0, "agent task should be skipped without executor");
        assert_eq!(runtime.container_count(), 0);
    }

    #[tokio::test]
    async fn agent_status_published_to_bus() {
        let (supervisor, _runtime, agent_exec, _store, bus) = setup_with_agent(1).await;
        *agent_exec.auto_complete.lock().unwrap() = true;

        let cmd = Command::Assign {
            task_id: "agent-1".to_string(),
            payload: spec_to_payload(&make_agent_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        supervisor.reconcile_once().await.unwrap();

        // Status should be published to KV.
        let key = subjects::kv_task_status("agent-1");
        let status_bytes = bus.get_status(&key).await.unwrap();
        assert!(status_bytes.is_some(), "agent status should be in KV");

        let report = TaskStatusReport::from_bytes(&status_bytes.unwrap()).unwrap();
        assert_eq!(report.task_id, "agent-1");
        assert_eq!(report.node_id, 1);
    }

    #[tokio::test]
    async fn running_tasks_includes_agents() {
        let (supervisor, _runtime, _agent_exec, _store, bus) = setup_with_agent(1).await;

        let cmd = Command::Assign {
            task_id: "agent-1".to_string(),
            payload: spec_to_payload(&make_agent_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        supervisor.reconcile_once().await.unwrap();

        let tasks = supervisor.running_tasks().await;
        assert!(tasks.contains(&"agent-1".to_string()));
    }

    #[tokio::test]
    async fn agent_assign_is_idempotent() {
        let (supervisor, _runtime, agent_exec, _store, bus) = setup_with_agent(1).await;

        let cmd = Command::Assign {
            task_id: "agent-1".to_string(),
            payload: spec_to_payload(&make_agent_spec()),
        };
        bus.send_command(1, &cmd).await.unwrap();
        bus.send_command(1, &cmd).await.unwrap(); // duplicate

        supervisor.reconcile_once().await.unwrap();
        assert_eq!(agent_exec.agent_count(), 1);

        let actions = supervisor.reconcile_once().await.unwrap();
        assert_eq!(actions, 0);
        assert_eq!(agent_exec.agent_count(), 1);
    }
}
