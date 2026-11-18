//! Node supervisor: the reconcile loop that runs tasks on this node.
//!
//! The supervisor consumes assignment records from MINGA_ASSIGN, executes
//! them through the container [`Runtime`], and reports actual state over
//! the bus.
//!
//! Task status is published to MINGA_STATUS, not Raft. Per the architecture
//! principle: "Only desired state and placement go through Raft. Heartbeats,
//! task status and agent run steps go through JetStream."
//!

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{
    subjects, EventBus, KvStream, LogEntry, LogStream, TaskActualState, TaskStatusReport,
};
use minga_state::types::{
    unix_now, NetworkAssignment, NodeId, RestartCondition, TaskAssignment, TaskId, TaskSpec,
    TaskState, WorkloadKind,
};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::agent::{AgentError, AgentExecutor, AgentStatus};
use crate::failure::{self, Failure};
use crate::health::{self, Health};
use crate::runtime::{ContainerStatus, Runtime, RuntimeError};

#[derive(Clone)]
pub struct Assignments {
    bus: Arc<dyn EventBus>,
    prefix: String,
    node_id: Option<NodeId>,
    tenant_id: String,
}

impl Assignments {
    pub fn new(bus: Arc<dyn EventBus>, node_id: Option<NodeId>) -> Self {
        Self::new_for_tenant(bus, node_id, minga_state::types::DEFAULT_TENANT_ID)
    }

    pub fn new_for_tenant(
        bus: Arc<dyn EventBus>,
        node_id: Option<NodeId>,
        tenant_id: &str,
    ) -> Self {
        let prefix = node_id
            .map(|id| subjects::kv_assignment_node_prefix(tenant_id, id))
            .unwrap_or_default();
        Self {
            bus,
            prefix,
            node_id,
            tenant_id: tenant_id.to_owned(),
        }
    }

    pub async fn list(&self) -> Result<Vec<TaskAssignment>, String> {
        let entries = self
            .bus
            .list_assignments(&self.prefix)
            .await
            .map_err(|e| e.to_string())?;
        entries
            .into_iter()
            .filter(|(key, _)| key.starts_with(&format!("{}.node.", self.tenant_id)))
            .map(|(key, bytes)| {
                let assignment: TaskAssignment = minga_state::codec::decode_wire(&bytes)
                    .map_err(|e| format!("invalid assignment {key}: {e}"))?;
                if self.node_id.is_some_and(|node| assignment.node_id != node)
                    || key
                        != subjects::kv_assignment_task(
                            &self.tenant_id,
                            assignment.node_id,
                            &assignment.task_id,
                        )
                {
                    return Err(format!("assignment key/value mismatch: {key}"));
                }
                Ok(assignment)
            })
            .collect()
    }

    pub async fn watch(&self) -> Result<KvStream, String> {
        self.bus
            .watch_assignments(&self.prefix)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn networks(&self, node_id: NodeId) -> Result<Vec<NetworkAssignment>, String> {
        let entries = self
            .bus
            .list_assignments(&subjects::kv_assignment_node_network_prefix(
                &self.tenant_id,
                node_id,
            ))
            .await
            .map_err(|e| e.to_string())?;
        entries
            .into_iter()
            .map(|(key, bytes)| {
                let assignment: NetworkAssignment = minga_state::codec::decode_wire(&bytes)
                    .map_err(|e| format!("invalid network assignment {key}: {e}"))?;
                if key
                    != subjects::kv_assignment_network(
                        &self.tenant_id,
                        node_id,
                        &assignment.network.id,
                    )
                {
                    return Err(format!("network assignment key/value mismatch: {key}"));
                }
                Ok(assignment)
            })
            .collect()
    }

    pub async fn ingress(&self) -> Result<Vec<minga_state::types::IngressAssignment>, String> {
        let Some(node_id) = self.node_id else {
            return Ok(Vec::new());
        };
        let prefix = subjects::kv_assignment_ingress_prefix(&self.tenant_id, node_id);
        self.bus
            .list_assignments(&prefix)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(key, bytes)| {
                let assignment: minga_state::types::IngressAssignment =
                    minga_state::codec::decode_wire(&bytes)
                        .map_err(|e| format!("invalid ingress assignment {key}: {e}"))?;
                if key
                    != subjects::kv_assignment_ingress(
                        &self.tenant_id,
                        node_id,
                        assignment.published,
                        &assignment.protocol,
                    )
                    || assignment.published == 0
                    || !matches!(assignment.protocol.as_str(), "tcp" | "udp" | "sctp")
                    || assignment.backends.is_empty()
                    || assignment.backends.iter().any(|backend| {
                        backend.ip.parse::<std::net::Ipv4Addr>().is_err()
                            || backend.target_port == 0
                    })
                {
                    return Err(format!("invalid ingress assignment: {key}"));
                }
                Ok(assignment)
            })
            .collect()
    }
}
#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("agent error: {0}")]
    Agent(#[from] AgentError),
    #[error("bus error: {0}")]
    Bus(String),
}

/// Tracks which tasks have been started on this node and their container IDs.
type RunningTasks = Arc<Mutex<HashMap<TaskId, String>>>;

/// Tracks agent task IDs currently running on this node.
type RunningAgents = Arc<Mutex<HashSet<TaskId>>>;

/// Tracks how many log entries have been published per task (container or agent).
/// Used to avoid re-publishing the same log lines on each reconcile pass.
type LogOffsets = Arc<Mutex<HashMap<TaskId, usize>>>;

/// Tracks restart state for a failed task.
#[derive(Debug, Clone)]
struct RestartState {
    /// Number of restart attempts so far.
    attempts: u32,
    /// When the next restart should happen.
    next_retry: std::time::Instant,
    /// The task spec (needed to restart).
    spec: TaskSpec,
}

/// Tracks restart state per task ID.
type RestartStates = Arc<Mutex<HashMap<TaskId, RestartState>>>;

/// Node networking for the supervisor: DNS service discovery backed by
/// MINGA_ASSIGN network records. The data plane itself (netns/bridges) is
/// applied by the container runtime.
#[cfg(target_os = "linux")]
pub struct SupervisorNetworking {
    setup: Arc<crate::netns::NetworkSetup>,
    dns_store: Arc<std::sync::RwLock<minga_net::DnsStore>>,
    upstream: Option<std::net::SocketAddr>,
    server: std::sync::Mutex<Option<minga_net::DnsServer>>,
    current_binds: std::sync::Mutex<Vec<std::net::SocketAddr>>,
}

#[cfg(target_os = "linux")]
impl SupervisorNetworking {
    pub fn new(
        setup: Arc<crate::netns::NetworkSetup>,
        upstream: Option<std::net::SocketAddr>,
    ) -> Self {
        Self {
            setup,
            dns_store: Arc::new(std::sync::RwLock::new(minga_net::DnsStore::new())),
            upstream,
            server: std::sync::Mutex::new(None),
            current_binds: std::sync::Mutex::new(Vec::new()),
        }
    }
}

/// Builds DNS records from MINGA_ASSIGN network/DNS entries. Only networks
/// with a task on this node are published: the resolver listens on those
/// gateways, and names from networks this node does not serve must not leak.
#[cfg(any(target_os = "linux", test))]
fn build_dns_store(assignments: &[NetworkAssignment], node_id: NodeId) -> minga_net::DnsStore {
    let mut store = minga_net::DnsStore::new();
    for assignment in assignments
        .iter()
        .filter(|assignment| assignment.tasks.iter().any(|task| task.node_id == node_id))
    {
        for task in &assignment.tasks {
            let Ok(ip) = task.ip.parse::<std::net::Ipv4Addr>() else {
                continue;
            };
            store.add(&task.service_name, ip);
            store.add(format!("tasks.{}", task.service_name), ip);
            store.add(
                minga_net::dns::service_fqdn(&task.service_name, &task.stack),
                ip,
            );
            store.add(
                minga_net::dns::task_fqdn(&task.task_id, &task.service_name, &task.stack),
                ip,
            );
        }
    }
    store
}

/// The node supervisor. Runs a reconcile loop that converges the local
/// container/agent state with the tasks assigned to this node.
///
/// A worker supervisor reads node-scoped assignment records and reports
/// actual state over the bus. It does not consume task commands or access
/// the consensus state store.
pub struct Supervisor {
    node_id: NodeId,
    assignments: Assignments,
    runtime: Arc<dyn Runtime>,
    agent_executor: Option<Arc<dyn AgentExecutor>>,
    bus: Option<Arc<dyn EventBus>>,
    tenant_id: String,
    running: RunningTasks,
    running_agents: RunningAgents,
    log_offsets: LogOffsets,
    restart_states: RestartStates,
    finished: Arc<Mutex<HashSet<TaskId>>>,
    task_assignments: Arc<Mutex<HashMap<TaskId, TaskAssignment>>>,
    health: Arc<Mutex<HashMap<TaskId, Health>>>,
    #[cfg(target_os = "linux")]
    networking: Option<SupervisorNetworking>,
}

#[derive(Default)]
struct TaskStatusUpdate {
    state: Option<TaskActualState>,
    container_id: Option<String>,
    failure: Option<Failure>,
    restart: bool,
    clear_failure: bool,
    refresh: bool,
}

impl Supervisor {
    pub fn new(assignments: Assignments, runtime: Arc<dyn Runtime>, node_id: NodeId) -> Self {
        Self::build(assignments, runtime, None, None, node_id)
    }

    pub fn with_bus(runtime: Arc<dyn Runtime>, bus: Arc<dyn EventBus>, node_id: NodeId) -> Self {
        Self::with_tenant_bus(runtime, bus, node_id, minga_state::types::DEFAULT_TENANT_ID)
    }

    pub fn with_tenant_bus(
        runtime: Arc<dyn Runtime>,
        bus: Arc<dyn EventBus>,
        node_id: NodeId,
        tenant_id: &str,
    ) -> Self {
        let assignments = Assignments::new_for_tenant(bus.clone(), Some(node_id), tenant_id);
        Self::build(assignments, runtime, Some(bus), None, node_id)
    }

    pub fn with_agent_executor(
        runtime: Arc<dyn Runtime>,
        bus: Arc<dyn EventBus>,
        agent_executor: Arc<dyn AgentExecutor>,
        node_id: NodeId,
    ) -> Self {
        Self::with_tenant_agent_executor(
            runtime,
            bus,
            agent_executor,
            node_id,
            minga_state::types::DEFAULT_TENANT_ID,
        )
    }

    pub fn with_tenant_agent_executor(
        runtime: Arc<dyn Runtime>,
        bus: Arc<dyn EventBus>,
        agent_executor: Arc<dyn AgentExecutor>,
        node_id: NodeId,
        tenant_id: &str,
    ) -> Self {
        let assignments = Assignments::new_for_tenant(bus.clone(), Some(node_id), tenant_id);
        Self::build(
            assignments,
            runtime,
            Some(bus),
            Some(agent_executor),
            node_id,
        )
    }

    fn build(
        assignments: Assignments,
        runtime: Arc<dyn Runtime>,
        bus: Option<Arc<dyn EventBus>>,
        agent_executor: Option<Arc<dyn AgentExecutor>>,
        node_id: NodeId,
    ) -> Self {
        let tenant_id = assignments.tenant_id.clone();
        Self {
            node_id,
            assignments,
            runtime,
            agent_executor,
            bus,
            tenant_id,
            running: Arc::new(Mutex::new(HashMap::new())),
            running_agents: Arc::new(Mutex::new(HashSet::new())),
            log_offsets: Arc::new(Mutex::new(HashMap::new())),
            restart_states: Arc::new(Mutex::new(HashMap::new())),
            finished: Arc::new(Mutex::new(HashSet::new())),
            task_assignments: Arc::new(Mutex::new(HashMap::new())),
            health: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(target_os = "linux")]
            networking: None,
        }
    }

    /// Attach node networking (DNS service discovery). The runtime applies
    /// the data plane; the supervisor syncs DNS records and listeners.
    #[cfg(target_os = "linux")]
    pub fn with_networking(mut self, networking: SupervisorNetworking) -> Self {
        self.networking = Some(networking);
        self
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
        let mut actions = self.assignment_reconcile().await;
        if let Some(bus) = &self.bus {
            self.publish_status(bus).await;
            self.stream_logs(bus).await;
        }
        self.check_health().await;

        // --- Restart policy: check failed containers, restart if needed -------
        actions += self.check_restarts().await;

        // --- DNS: keep records and listeners in sync with placement ----------
        #[cfg(target_os = "linux")]
        self.sync_dns().await;
        #[cfg(target_os = "linux")]
        self.sync_ingress().await;

        // running_count() already includes agents.
        let running = self.running_count().await;
        minga_telemetry::METRICS
            .tasks_total
            .with_label_values(&["running"])
            .set(running as f64);

        Ok(actions)
    }

    /// Rebuilds DNS records from network assignment entries and reconciles
    /// listener addresses with the networks' bridge gateways.
    /// The server restarts only when the bind set actually changes.
    #[cfg(target_os = "linux")]
    async fn sync_dns(&self) {
        let Some(net) = &self.networking else {
            return;
        };

        let assignments = match self.assignments.networks(self.node_id).await {
            Ok(assignments) => assignments,
            Err(error) => {
                tracing::warn!(%error, "dns sync: cannot read network assignments");
                return;
            }
        };
        let store = build_dns_store(&assignments, self.node_id);
        *net.dns_store.write().unwrap() = store;
        let mut binds: Vec<std::net::SocketAddr> = assignments
            .iter()
            .filter(|assignment| {
                assignment
                    .tasks
                    .iter()
                    .any(|task| task.node_id == self.node_id)
            })
            .filter_map(|assignment| {
                net.setup
                    .gateway_for(&assignment.network.id, assignment.network.subnet.as_deref())
            })
            .map(|ip| std::net::SocketAddr::new(std::net::IpAddr::V4(ip), 53))
            .collect();
        binds.sort();
        binds.dedup();

        let current = net.current_binds.lock().unwrap().clone();
        if binds == current {
            return;
        }
        // The guard must drop before the await: an `if let` scrutinee
        // temporary lives for the whole body.
        let server = net.server.lock().unwrap().take();
        if let Some(server) = server {
            server.shutdown().await;
        }
        if binds.is_empty() {
            *net.current_binds.lock().unwrap() = binds;
            return;
        }
        match minga_net::DnsServer::start(binds.clone(), net.dns_store.clone(), net.upstream).await
        {
            Ok(server) => {
                tracing::info!(binds = ?binds, "dns: service discovery listening");
                *net.server.lock().unwrap() = Some(server);
                *net.current_binds.lock().unwrap() = binds;
            }
            Err(e) => {
                tracing::error!(error = %e, binds = ?binds, "dns: server start failed");
            }
        }
    }
    /// Reconciles every node-local ingress frontend with the leader's backend sets.
    #[cfg(target_os = "linux")]
    async fn sync_ingress(&self) {
        let Some(net) = &self.networking else {
            return;
        };
        let assignments = match self.assignments.ingress().await {
            Ok(assignments) => assignments,
            Err(error) => {
                tracing::warn!(%error, "ingress sync: cannot read backend assignments");
                return;
            }
        };
        if let Err(error) = net.setup.setup_ingress(&assignments, self.node_id).await {
            tracing::error!(%error, "ingress sync: failed to apply backend assignments");
        }
    }

    /// Check running containers for failures and restart them according
    /// to their restart policy. Uses exponential backoff: delay * 2^attempts.
    async fn check_restarts(&self) -> usize {
        let mut actions = 0;

        // --- Agents -----------------------------------------------------------
        // Agents live in their own tracking map (they have no container), so
        // they need a separate termination check.
        if let Some(exec) = &self.agent_executor {
            let agents = self.running_agents.lock().await;
            let mut failed_agents: Vec<TaskId> = Vec::new();
            for task_id in agents.iter() {
                match exec.status(task_id).await {
                    Some(AgentStatus::Failed) => failed_agents.push(task_id.clone()),
                    Some(AgentStatus::Completed) => {
                        // A completed agent exited successfully — only
                        // `always` restarts it.
                        failed_agents.push(task_id.clone());
                    }
                    _ => {}
                }
            }
            drop(agents);

            for task_id in failed_agents {
                let completed = exec
                    .status(&task_id)
                    .await
                    .map(|s| matches!(s, AgentStatus::Completed))
                    .unwrap_or(false);

                let spec = match self.task_assignments.lock().await.get(&task_id) {
                    Some(assignment) => assignment.spec.clone(),
                    None => {
                        let states = self.restart_states.lock().await;
                        match states.get(&task_id) {
                            Some(rs) => rs.spec.clone(),
                            None => continue,
                        }
                    }
                };

                let policy = &spec.restart_policy;
                let should_restart = match policy.condition {
                    RestartCondition::None => false,
                    // A completed agent exited 0 — `on-failure` does not apply.
                    RestartCondition::OnFailure => !completed,
                    RestartCondition::Any => true,
                };

                if !should_restart {
                    tracing::info!(
                        task = %task_id,
                        "agent exited cleanly, restart policy does not apply"
                    );
                    self.finished.lock().await.insert(task_id.clone());
                    self.running_agents.lock().await.remove(&task_id);
                    continue;
                }

                let attempt = {
                    let mut states = self.restart_states.lock().await;
                    let state = states
                        .entry(task_id.clone())
                        .or_insert_with(|| RestartState {
                            attempts: 0,
                            next_retry: std::time::Instant::now(),
                            spec: spec.clone(),
                        });

                    if let Some(max) = policy.max_attempts {
                        if state.attempts >= max {
                            tracing::warn!(
                                task = %task_id,
                                attempts = state.attempts,
                                max,
                                "agent failed, max restart attempts reached"
                            );
                            states.remove(&task_id);
                            self.finished.lock().await.insert(task_id.clone());
                            self.running_agents.lock().await.remove(&task_id);
                            continue;
                        }
                    }

                    if std::time::Instant::now() < state.next_retry {
                        continue;
                    }

                    state.attempts += 1;
                    let backoff_secs = policy.delay_secs * (1u64 << (state.attempts - 1).min(10));
                    state.next_retry =
                        std::time::Instant::now() + Duration::from_secs(backoff_secs);
                    state.attempts
                };

                let WorkloadKind::Agent(agent_spec) = &spec.kind else {
                    continue;
                };

                match exec.start(&task_id, agent_spec).await {
                    Ok(agent_id) => {
                        minga_telemetry::METRICS.task_restarts_total.inc();
                        tracing::info!(
                            task = %task_id,
                            agent = %agent_id,
                            attempt,
                            "restarted failed agent"
                        );
                        self.running_agents.lock().await.insert(task_id.clone());
                        actions += 1;
                    }
                    Err(e) => {
                        tracing::error!(task = %task_id, error = %e, "failed to restart agent");
                    }
                }
            }
        }

        // --- Containers -------------------------------------------------------
        // Check status of all running containers.
        let running = self.running.lock().await;
        let mut terminated: Vec<(TaskId, String)> = Vec::new();
        for (task_id, container_id) in running.iter() {
            match self.runtime.state(container_id).await {
                Some(state) if state.status.is_terminated() => {
                    terminated.push((task_id.clone(), container_id.clone()));
                }
                _ => {}
            }
        }
        drop(running);

        for (task_id, container_id) in terminated {
            // Remove the terminated container from tracking and clean it up.
            self.running.lock().await.remove(&task_id);
            self.log_offsets.lock().await.remove(&task_id);
            self.health.lock().await.remove(&task_id);

            // Capture the exit code BEFORE stopping: `stop()` deletes the
            // task, which would discard the exit status we need to decide
            // whether the restart policy applies.
            let exit_code = self
                .runtime
                .state(&container_id)
                .await
                .and_then(|s| s.exit_code);

            // Best-effort stop to clean up the container. If it already
            // exited, this only removes the task and container records.
            let _ = self.runtime.stop(&container_id).await;

            let assignment = self.task_assignments.lock().await.get(&task_id).cloned();
            let prior_error = match &self.bus {
                Some(bus) => self
                    .read_task_status(bus, &task_id)
                    .await
                    .and_then(|status| status.last_error),
                None => None,
            };
            let failure = failure::exit(exit_code, prior_error.as_deref());
            let spec = match assignment {
                Some(assignment) => assignment.spec,
                None => {
                    let states = self.restart_states.lock().await;
                    match states.get(&task_id) {
                        Some(rs) => rs.spec.clone(),
                        None => continue,
                    }
                }
            };

            let policy = &spec.restart_policy;
            if policy.condition == RestartCondition::None {
                tracing::info!(task = %task_id, "task terminated, restart policy is `none`");
                let end = if exit_code == Some(0) {
                    TaskState::Stopped
                } else {
                    TaskState::Failed
                };
                self.finished.lock().await.insert(task_id.clone());
                self.mark_task(&task_id, end, Some(failure)).await;
                continue;
            }

            // `on-failure` only restarts a non-zero exit; `any` restarts
            // regardless of how the task exited.
            let should_restart = match policy.condition {
                RestartCondition::None => false,
                RestartCondition::OnFailure => exit_code.map(|c| c != 0).unwrap_or(true),
                RestartCondition::Any => true,
            };

            if !should_restart {
                tracing::info!(
                    task = %task_id,
                    exit_code = ?exit_code,
                    "task exited cleanly, restart policy does not apply"
                );
                self.finished.lock().await.insert(task_id.clone());
                self.mark_task(&task_id, TaskState::Stopped, Some(failure))
                    .await;
                continue;
            }

            self.record_failure(&task_id, failure, true).await;

            // Determine if we should restart, and compute backoff.
            let (attempt, new_spec) = {
                let mut states = self.restart_states.lock().await;
                let state = states
                    .entry(task_id.clone())
                    .or_insert_with(|| RestartState {
                        attempts: 0,
                        next_retry: std::time::Instant::now(),
                        spec: spec.clone(),
                    });

                if let Some(max) = policy.max_attempts {
                    if state.attempts >= max {
                        tracing::warn!(
                            task = %task_id,
                            attempts = state.attempts,
                            max,
                            "task failed, max restart attempts reached"
                        );
                        states.remove(&task_id);
                        drop(states);
                        self.finished.lock().await.insert(task_id.clone());
                        self.mark_task_state(&task_id, TaskState::Failed).await;
                        continue;
                    }
                }

                if std::time::Instant::now() < state.next_retry {
                    continue; // Not yet time to retry.
                }

                state.attempts += 1;
                let attempt = state.attempts;
                let backoff_secs = policy.delay_secs * (1u64 << (attempt - 1).min(10));
                state.next_retry = std::time::Instant::now() + Duration::from_secs(backoff_secs);
                (attempt, state.spec.clone())
            };

            // Restart the task.
            let networks = self
                .task_assignments
                .lock()
                .await
                .get(&task_id)
                .map(|assignment| assignment.networks.clone())
                .unwrap_or_default();
            match &new_spec.kind {
                WorkloadKind::Container => match self
                    .runtime
                    .start_assigned(&task_id, &new_spec, &networks)
                    .await
                {
                    Ok(new_container_id) => {
                        minga_telemetry::METRICS.task_restarts_total.inc();
                        tracing::info!(
                            task = %task_id,
                            container = %new_container_id,
                            attempt,
                            "restarted failed container"
                        );
                        self.running
                            .lock()
                            .await
                            .insert(task_id.clone(), new_container_id);
                        self.container_started(&task_id, &new_spec).await;
                        actions += 1;
                    }
                    Err(e) => {
                        tracing::error!(
                            task = %task_id,
                            error = %e,
                            "failed to restart container"
                        );
                        let failure =
                            failure::start_error(&e, &failure::registry_secrets(&new_spec.image));
                        self.record_failure(&task_id, failure, false).await;
                    }
                },
                WorkloadKind::Agent(agent_spec) => {
                    let Some(exec) = &self.agent_executor else {
                        tracing::error!(
                            task = %task_id,
                            "agent task failed but no agent executor configured"
                        );
                        continue;
                    };
                    match exec.start(&task_id, agent_spec).await {
                        Ok(agent_id) => {
                            minga_telemetry::METRICS.task_restarts_total.inc();
                            tracing::info!(
                                task = %task_id,
                                agent = %agent_id,
                                attempt,
                                "restarted failed agent"
                            );
                            self.running_agents.lock().await.insert(task_id.clone());
                            actions += 1;
                        }
                        Err(e) => {
                            tracing::error!(
                                task = %task_id,
                                error = %e,
                                "failed to restart agent"
                            );
                        }
                    }
                }
            }
        }

        actions
    }

    /// Stream log output from running containers and agents to the bus.
    /// Tracks per-task offsets to avoid re-publishing the same lines.
    async fn stream_logs(&self, bus: &Arc<dyn EventBus>) {
        // --- Container logs ---
        let running = self.running.lock().await;
        for (task_id, container_id) in running.iter() {
            let chunks = self.runtime.logs(container_id, 0).await;
            let mut offsets = self.log_offsets.lock().await;
            let offset = offsets.entry(task_id.clone()).or_insert(0);
            // The runtime's capture is bounded; if it dropped entries we
            // already counted, re-baseline instead of skipping forever.
            if *offset > chunks.len() {
                tracing::warn!(
                    task = %task_id,
                    offset = *offset,
                    available = chunks.len(),
                    "log capture truncated before publishing; re-baselining"
                );
                *offset = chunks.len();
            }
            for chunk in chunks.iter().skip(*offset) {
                let entry = LogEntry {
                    task_id: task_id.clone(),
                    stream: match chunk.stream {
                        crate::runtime::LogStream::Stdout => LogStream::Stdout,
                        crate::runtime::LogStream::Stderr => LogStream::Stderr,
                    },
                    data: chunk.data.clone(),
                    timestamp: now_millis(),
                };
                if let Err(e) = bus
                    .publish_tenant_node_log(&self.tenant_id, self.node_id, task_id, &entry)
                    .await
                {
                    tracing::warn!(task = %task_id, error = %e, "failed to publish log entry");
                }
            }
            *offset = chunks.len();
        }
        drop(running);

        // --- Agent logs (publish result when agent completes) ---
        let agents = self.running_agents.lock().await;
        for task_id in agents.iter() {
            if let Some(exec) = &self.agent_executor {
                let status = exec.status(task_id).await;
                if matches!(
                    status,
                    Some(AgentStatus::Completed) | Some(AgentStatus::Failed)
                ) {
                    let mut offsets = self.log_offsets.lock().await;
                    let offset = offsets.entry(task_id.clone()).or_insert(0);
                    if *offset == 0 {
                        // First time seeing this agent complete — publish the result.
                        if let Some(result) = exec.result(task_id).await {
                            let stream = if matches!(status, Some(AgentStatus::Failed)) {
                                LogStream::Stderr
                            } else {
                                LogStream::Stdout
                            };
                            let data = if !result.stdout.is_empty() {
                                result.stdout.into_bytes()
                            } else if !result.stderr.is_empty() {
                                result.stderr.into_bytes()
                            } else {
                                format!(
                                    "agent completed: fuel={}, duration={}ms, exit_code={}\n",
                                    result.fuel_used,
                                    result.duration.as_millis(),
                                    result.exit_code
                                )
                                .into_bytes()
                            };
                            let entry = LogEntry {
                                task_id: task_id.clone(),
                                stream,
                                data,
                                timestamp: now_millis(),
                            };
                            if let Err(e) = bus
                                .publish_tenant_node_log(
                                    &self.tenant_id,
                                    self.node_id,
                                    task_id,
                                    &entry,
                                )
                                .await
                            {
                                tracing::warn!(
                                    task = %task_id,
                                    error = %e,
                                    "failed to publish agent log"
                                );
                            }
                        }
                        *offset = 1; // Mark as published
                    }
                }
            }
        }
    }

    /// Writes a lifecycle transition to the status KV, not the Raft task record.
    async fn mark_task_state(&self, task_id: &TaskId, state: TaskState) {
        let Some(bus) = &self.bus else {
            return;
        };
        let state = match state {
            TaskState::Pending | TaskState::Assigned | TaskState::Starting => {
                TaskActualState::Starting
            }
            TaskState::Running => TaskActualState::Running,
            TaskState::Stopping | TaskState::Stopped => TaskActualState::Stopped,
            TaskState::Failed => TaskActualState::Failed,
        };
        self.update_task_status(
            bus,
            task_id,
            TaskStatusUpdate {
                state: Some(state),
                ..Default::default()
            },
        )
        .await;
    }

    async fn read_task_status(
        &self,
        bus: &Arc<dyn EventBus>,
        task_id: &TaskId,
    ) -> Option<TaskStatusReport> {
        let key = subjects::kv_node_task_status(self.node_id, task_id);
        match bus.get_tenant_status(&self.tenant_id, &key).await {
            Ok(Some(bytes)) => match TaskStatusReport::from_bytes(&bytes) {
                Ok(report) if report.node_id == self.node_id => Some(report),
                Ok(_) => None,
                Err(error) => {
                    tracing::warn!(task = %task_id, error = %error, "cannot decode task status");
                    None
                }
            },
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(task = %task_id, error = %error, "cannot read task status");
                None
            }
        }
    }

    async fn update_task_status(
        &self,
        bus: &Arc<dyn EventBus>,
        task_id: &TaskId,
        update: TaskStatusUpdate,
    ) {
        let key = subjects::kv_node_task_status(self.node_id, task_id);
        let current = match bus.get_tenant_status(&self.tenant_id, &key).await {
            Ok(Some(bytes)) => match TaskStatusReport::from_bytes(&bytes) {
                Ok(report) if report.node_id == self.node_id => Some(report),
                Ok(_) => None,
                Err(error) => {
                    tracing::warn!(task = %task_id, error = %error, "cannot decode task status");
                    return;
                }
            },
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(task = %task_id, error = %error, "cannot read task status");
                return;
            }
        };
        let (mut report, mut changed) = match current {
            Some(report) => (report, false),
            None => (
                TaskStatusReport {
                    task_id: task_id.clone(),
                    node_id: self.node_id,
                    state: if update.failure.is_some() {
                        TaskActualState::Failed
                    } else {
                        TaskActualState::Starting
                    },
                    container_id: None,
                    observed_at: now_millis(),
                    restarts: 0,
                    last_error: None,
                    last_exit_code: None,
                    running_since_unix: None,
                },
                true,
            ),
        };
        let previous_state = report.state;
        if let Some(state) = update.state {
            changed |= report.state != state;
            report.state = state;
            if state == TaskActualState::Running && previous_state != TaskActualState::Running {
                report.running_since_unix = Some(unix_now());
                changed = true;
            } else if state != TaskActualState::Running {
                changed |= report.running_since_unix.take().is_some();
            }
        }
        if let Some(container_id) = update.container_id {
            if report.container_id.as_ref() != Some(&container_id) {
                report.container_id = Some(container_id);
                changed = true;
            }
        }
        if let Some(failure) = update.failure.as_ref() {
            if report.last_error.as_ref() != Some(&failure.error)
                || report.last_exit_code != failure.exit_code
            {
                report.last_error = Some(failure.error.clone());
                report.last_exit_code = failure.exit_code;
                changed = true;
            }
        }
        if update.restart {
            report.state = TaskActualState::Starting;
            report.running_since_unix = None;
            report.restarts = report.restarts.saturating_add(1);
            changed = true;
        }
        if update.clear_failure && (report.last_error.is_some() || report.last_exit_code.is_some())
        {
            report.last_error = None;
            report.last_exit_code = None;
            changed = true;
        }
        if !changed && !update.refresh {
            return;
        }
        report.observed_at = now_millis();
        match report.to_bytes() {
            Ok(bytes) => {
                if let Err(error) = bus.put_tenant_status(&self.tenant_id, &key, &bytes).await {
                    tracing::warn!(task = %task_id, error = %error, "failed to publish task status");
                }
            }
            Err(error) => {
                tracing::warn!(task = %task_id, error = %error, "failed to serialize task status");
            }
        }
    }

    /// Records a lifecycle transition and optional failure detail in MINGA_STATUS.
    async fn mark_task(&self, task_id: &TaskId, state: TaskState, failure: Option<Failure>) {
        let Some(bus) = &self.bus else {
            return;
        };
        let state = match state {
            TaskState::Pending | TaskState::Assigned | TaskState::Starting => {
                TaskActualState::Starting
            }
            TaskState::Running => TaskActualState::Running,
            TaskState::Stopping | TaskState::Stopped => TaskActualState::Stopped,
            TaskState::Failed => TaskActualState::Failed,
        };
        self.update_task_status(
            bus,
            task_id,
            TaskStatusUpdate {
                state: Some(state),
                failure,
                ..Default::default()
            },
        )
        .await;
    }

    /// Records a container that just started. With a healthcheck, the task
    /// stays `Starting` until its first probe passes.
    async fn container_started(&self, task_id: &TaskId, spec: &TaskSpec) {
        match spec.healthcheck.as_ref().filter(|c| health::is_enforced(c)) {
            Some(check) => {
                let health = Health::new(check, std::time::Instant::now());
                self.health.lock().await.insert(task_id.clone(), health);
                self.mark_task_state(task_id, TaskState::Starting).await;
            }
            None => self.mark_task_state(task_id, TaskState::Running).await,
        }
    }

    /// Records a failure and optional retry count in MINGA_STATUS only.
    async fn record_failure(&self, task_id: &TaskId, failure: Failure, restart: bool) {
        let Some(bus) = &self.bus else {
            return;
        };
        self.update_task_status(
            bus,
            task_id,
            TaskStatusUpdate {
                failure: Some(failure),
                restart,
                ..Default::default()
            },
        )
        .await;
    }

    /// Starts due probes and acts on finished ones. Probes run in their
    /// own tasks: one can take as long as its timeout.
    async fn check_health(&self) {
        let running = self.running.lock().await.clone();
        let now = std::time::Instant::now();
        let mut healthy = Vec::new();
        let mut unhealthy = Vec::new();
        {
            let mut states = self.health.lock().await;
            states.retain(|task_id, _| running.contains_key(task_id));
            for (task_id, state) in states.iter_mut() {
                if state.take_newly_healthy() {
                    healthy.push(task_id.clone());
                }
                if state.is_unhealthy() {
                    unhealthy.push(task_id.clone());
                } else if state.begin_probe(now) {
                    let runtime = self.runtime.clone();
                    let health = self.health.clone();
                    let task_id = task_id.clone();
                    let container_id = running[&task_id].clone();
                    let (args, timeout) = (state.args.clone(), state.timeout);
                    tokio::spawn(async move {
                        let result = runtime.exec(&container_id, &args, timeout).await;
                        if let Some(state) = health.lock().await.get_mut(&task_id) {
                            state.record(matches!(result, Ok(0)), std::time::Instant::now());
                        }
                    });
                }
            }
            for task_id in &unhealthy {
                states.remove(task_id);
            }
        }

        for task_id in healthy {
            self.mark_task_state(&task_id, TaskState::Running).await;
        }
        for task_id in unhealthy {
            tracing::warn!(task = %task_id, "healthcheck failed, killing container");
            match self.runtime.kill(&running[&task_id]).await {
                Ok(()) => {
                    let failure = Failure {
                        error: failure::HEALTHCHECK_KILL.into(),
                        exit_code: None,
                    };
                    self.record_failure(&task_id, failure, false).await;
                }
                Err(e) => {
                    tracing::error!(task = %task_id, error = %e, "failed to kill unhealthy container")
                }
            }
        }
    }

    /// Refreshes current task status in MINGA_STATUS.
    async fn publish_status(&self, bus: &Arc<dyn EventBus>) {
        let running = self.running.lock().await;
        for (task_id, container_id) in running.iter() {
            let state = {
                let health = self.health.lock().await;
                if health
                    .get(task_id)
                    .is_some_and(|health| !health.is_healthy())
                {
                    TaskActualState::Starting
                } else {
                    TaskActualState::Running
                }
            };
            self.update_task_status(
                bus,
                task_id,
                TaskStatusUpdate {
                    state: Some(state),
                    container_id: Some(container_id.clone()),
                    refresh: true,
                    ..Default::default()
                },
            )
            .await;
        }
        drop(running);

        let agents = self.running_agents.lock().await;
        for task_id in agents.iter() {
            let state = if let Some(exec) = &self.agent_executor {
                match exec.status(task_id).await {
                    Some(AgentStatus::Completed) => TaskActualState::Stopped,
                    Some(AgentStatus::Failed) => TaskActualState::Failed,
                    _ => TaskActualState::Running,
                }
            } else {
                TaskActualState::Running
            };
            self.update_task_status(
                bus,
                task_id,
                TaskStatusUpdate {
                    state: Some(state),
                    refresh: true,
                    ..Default::default()
                },
            )
            .await;
        }
    }

    /// Reconciles against the node's assignment KV prefix. A failed read is
    /// not an empty desired set: preserve all tracked and runtime tasks.
    async fn assignment_reconcile(&self) -> usize {
        let assignments = match self.assignments.list().await {
            Ok(assignments) => assignments,
            Err(error) => {
                tracing::warn!(node = self.node_id, %error, "cannot read assignments; preserving running tasks");
                return 0;
            }
        };
        let current: HashMap<TaskId, TaskAssignment> = assignments
            .into_iter()
            .map(|assignment| (assignment.task_id.clone(), assignment))
            .collect();
        *self.task_assignments.lock().await = current.clone();
        self.finished
            .lock()
            .await
            .retain(|task_id| current.contains_key(task_id));
        if let Some(bus) = &self.bus {
            let now = unix_now();
            for task_id in current.keys() {
                let Some(status) = self.read_task_status(bus, task_id).await else {
                    continue;
                };
                let settled = status.state == TaskActualState::Running
                    && status
                        .running_since_unix
                        .is_some_and(|since| since + failure::CLEAR_AFTER_SECS <= now);
                if settled && status.last_error.is_some() {
                    self.update_task_status(
                        bus,
                        task_id,
                        TaskStatusUpdate {
                            clear_failure: true,
                            ..Default::default()
                        },
                    )
                    .await;
                }
            }
        }
        let desired: HashSet<TaskId> = current.keys().cloned().collect();
        let backing_off: HashSet<TaskId> = {
            let now = std::time::Instant::now();
            self.restart_states
                .lock()
                .await
                .iter()
                .filter(|(_, state)| now < state.next_retry)
                .map(|(id, _)| id.clone())
                .collect()
        };
        let mut actions = 0;
        let mut running = self.running.lock().await;
        let mut agents = self.running_agents.lock().await;
        if running.is_empty() && agents.is_empty() {
            for container in self.runtime.list().await {
                if desired.contains(&container.task_id)
                    && container.status == ContainerStatus::Running
                {
                    running.insert(container.task_id, container.id);
                } else if self.runtime.stop(&container.id).await.is_ok() {
                    actions += 1;
                    minga_telemetry::METRICS.task_stops_total.inc();
                }
            }
        }
        for assignment in current.values() {
            let task_id = &assignment.task_id;
            if self.finished.lock().await.contains(task_id) {
                continue;
            }
            if running.contains_key(task_id)
                || agents.contains(task_id)
                || backing_off.contains(task_id)
            {
                continue;
            }
            match &assignment.spec.kind {
                WorkloadKind::Container => {
                    match self
                        .runtime
                        .start_assigned(task_id, &assignment.spec, &assignment.networks)
                        .await
                    {
                        Ok(container_id) => {
                            minga_telemetry::METRICS.task_starts_total.inc();
                            running.insert(task_id.clone(), container_id.clone());
                            actions += 1;
                            self.container_started(task_id, &assignment.spec).await;
                        }
                        Err(error) => {
                            self.record_failure(
                                task_id,
                                failure::start_error(
                                    &error,
                                    &failure::registry_secrets(&assignment.spec.image),
                                ),
                                true,
                            )
                            .await;
                        }
                    }
                }
                WorkloadKind::Agent(agent_spec) => {
                    let Some(exec) = &self.agent_executor else {
                        tracing::error!(task = %task_id, "agent assignment without agent executor");
                        continue;
                    };
                    match exec.start(task_id, agent_spec).await {
                        Ok(_) => {
                            minga_telemetry::METRICS.task_starts_total.inc();
                            agents.insert(task_id.clone());
                            actions += 1;
                            self.mark_task_state(task_id, TaskState::Running).await;
                        }
                        Err(error) => {
                            self.record_failure(task_id, failure::start_error(&error, &[]), false)
                                .await
                        }
                    }
                }
            }
        }
        let removed: Vec<TaskId> = running
            .keys()
            .chain(agents.iter())
            .filter(|task_id| !desired.contains(*task_id))
            .cloned()
            .collect();
        for task_id in removed {
            self.log_offsets.lock().await.remove(&task_id);
            if let Some(container) = running.get(&task_id).cloned() {
                if self.runtime.stop(&container).await.is_ok() {
                    running.remove(&task_id);
                    actions += 1;
                    minga_telemetry::METRICS.task_stops_total.inc();
                }
            } else if agents.contains(&task_id) {
                if let Some(exec) = &self.agent_executor {
                    if exec.stop(&task_id).await.is_err() {
                        continue;
                    }
                }
                agents.remove(&task_id);
                actions += 1;
                minga_telemetry::METRICS.task_stops_total.inc();
            }
        }
        actions
    }

    /// Adopt existing containers present in a successful assignment snapshot;
    /// without that snapshot leave every container untouched.
    async fn recover_running_containers(&self) -> usize {
        let assignments = match self.assignments.list().await {
            Ok(assignments) => assignments,
            Err(error) => {
                tracing::warn!(node = self.node_id, %error, "cannot read assignments for startup recovery");
                return 0;
            }
        };
        let assigned: HashMap<TaskId, TaskAssignment> = assignments
            .into_iter()
            .map(|assignment| (assignment.task_id.clone(), assignment))
            .collect();
        *self.task_assignments.lock().await = assigned.clone();
        let containers = self.runtime.list().await;
        let mut stopped = 0;
        for container in containers {
            if let Some(assignment) = assigned.get(&container.task_id) {
                if container.status == ContainerStatus::Running {
                    tracing::info!(task = %container.task_id, container = %container.id, "adopted running container");
                    self.running
                        .lock()
                        .await
                        .insert(container.task_id, container.id);
                } else {
                    // Reconcile will start its current assignment after cleanup.
                    let _ = self.runtime.stop(&container.id).await;
                }
                let _ = assignment;
                continue;
            }
            if self.runtime.stop(&container.id).await.is_ok() {
                stopped += 1;
                minga_telemetry::METRICS.task_stops_total.inc();
            }
        }
        stopped
    }

    /// Runs the supervisor loop forever, sleeping `interval` between passes.
    pub async fn run(self, interval: Duration) -> ! {
        tracing::info!(node = self.node_id, ?interval, "supervisor loop starting");
        loop {
            match self.runtime.prepare_startup().await {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!(
                        %error,
                        "runtime startup safeguards failed; retrying before task cleanup"
                    );
                    tokio::time::sleep(interval).await;
                }
            }
        }
        // Startup reconciliation: kill and delete any STOPPED or orphan tasks
        // left over from a previous run. Uses list_tasks (not list_containers)
        // so it also catches tasks whose container entry is already gone.
        self.runtime.purge_stopped_tasks().await;
        self.recover_running_containers().await;
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
mod assignment_tests {
    use super::*;
    use crate::runtime::MockRuntime;
    use minga_bus::MemoryBus;

    fn assignment(task_id: &str) -> TaskAssignment {
        assignment_on(task_id, 7)
    }

    fn assignment_on(task_id: &str, node_id: NodeId) -> TaskAssignment {
        TaskAssignment {
            task_id: task_id.to_string(),
            node_id,
            service_id: "stack.service".to_string(),
            service_name: "service".to_string(),
            stack: "stack".to_string(),
            spec: TaskSpec {
                image: "nginx:latest".to_string(),
                ..Default::default()
            },
            networks: Vec::new(),
        }
    }

    async fn write_assignment(bus: &MemoryBus, assignment: &TaskAssignment) {
        let bytes = minga_state::codec::encode_wire(assignment).unwrap();
        bus.put_assignment(
            &subjects::kv_assignment_task(
                minga_state::types::DEFAULT_TENANT_ID,
                assignment.node_id,
                &assignment.task_id,
            ),
            &bytes,
        )
        .await
        .unwrap();
    }

    async fn setup(node_id: NodeId) -> (Supervisor, Arc<MockRuntime>, Arc<MemoryBus>) {
        let runtime = Arc::new(MockRuntime::new());
        let bus = MemoryBus::new();
        let supervisor = Supervisor::with_bus(runtime.clone(), bus.clone(), node_id);
        (supervisor, runtime, bus)
    }
    async fn actual_status(bus: &MemoryBus, node_id: NodeId, task_id: &str) -> TaskStatusReport {
        let bytes = bus
            .get_tenant_status(
                minga_state::types::DEFAULT_TENANT_ID,
                &subjects::kv_node_task_status(node_id, task_id),
            )
            .await
            .unwrap()
            .unwrap();
        TaskStatusReport::from_bytes(&bytes).unwrap()
    }
    fn agent_assignment(task_id: &str) -> TaskAssignment {
        let mut assignment = assignment(task_id);
        assignment.spec.kind = WorkloadKind::Agent(minga_state::types::AgentSpec {
            module_ref: "./agent.wasm".to_string(),
            ..Default::default()
        });
        assignment
    }
    async fn setup_with_agent(
        node_id: NodeId,
    ) -> (
        Supervisor,
        Arc<MockRuntime>,
        Arc<crate::agent::MockAgentExecutor>,
        Arc<MemoryBus>,
    ) {
        let runtime = Arc::new(MockRuntime::new());
        let executor = Arc::new(crate::agent::MockAgentExecutor::new());
        let bus = MemoryBus::new();
        let supervisor = Supervisor::with_agent_executor(
            runtime.clone(),
            bus.clone(),
            executor.clone(),
            node_id,
        );
        (supervisor, runtime, executor, bus)
    }

    struct ToggleBus {
        inner: Arc<MemoryBus>,
        assignments_offline: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl EventBus for ToggleBus {
        async fn publish_event(&self, subject: &str, payload: &[u8]) -> minga_bus::Result<()> {
            self.inner.publish_event(subject, payload).await
        }

        async fn subscribe_events(
            &self,
            patterns: &[&str],
        ) -> minga_bus::Result<minga_bus::EventStream> {
            self.inner.subscribe_events(patterns).await
        }
        async fn subscribe_tenant_events(
            &self,
            tenant_id: &str,
        ) -> minga_bus::Result<minga_bus::EventStream> {
            self.inner.subscribe_tenant_events(tenant_id).await
        }
        async fn configure_tenant_quota(
            &self,
            tenant_id: &str,
            quotas: &minga_state::types::TenantQuotas,
        ) -> minga_bus::Result<()> {
            self.inner.configure_tenant_quota(tenant_id, quotas).await
        }
        async fn append_audit(&self, record: minga_bus::AuditRecord) -> minga_bus::Result<()> {
            self.inner.append_audit(record).await
        }

        async fn list_audit(
            &self,
            tenant_id: &str,
            limit: usize,
        ) -> minga_bus::Result<Vec<minga_bus::AuditRecord>> {
            self.inner.list_audit(tenant_id, limit).await
        }
        async fn update_account_claim(&self, account_jwt: &str) -> minga_bus::Result<()> {
            self.inner.update_account_claim(account_jwt).await
        }

        async fn check_tenant_request_quota(
            &self,
            tenant_id: &str,
            window_start: u64,
            limit: u64,
        ) -> minga_bus::Result<bool> {
            self.inner
                .check_tenant_request_quota(tenant_id, window_start, limit)
                .await
        }
        async fn publish_tenant_event(
            &self,
            tenant_id: &str,
            subject: &str,
            payload: &[u8],
        ) -> minga_bus::Result<()> {
            self.inner
                .publish_tenant_event(tenant_id, subject, payload)
                .await
        }

        async fn put_assignment(&self, key: &str, value: &[u8]) -> minga_bus::Result<()> {
            self.inner.put_assignment(key, value).await
        }

        async fn get_assignment(&self, key: &str) -> minga_bus::Result<Option<Vec<u8>>> {
            self.inner.get_assignment(key).await
        }

        async fn list_assignments(
            &self,
            prefix: &str,
        ) -> minga_bus::Result<Vec<(String, Vec<u8>)>> {
            if self
                .assignments_offline
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(minga_bus::BusError::Connection("offline".into()));
            }
            self.inner.list_assignments(prefix).await
        }

        async fn delete_assignment(&self, key: &str) -> minga_bus::Result<()> {
            self.inner.delete_assignment(key).await
        }

        async fn watch_assignments(&self, prefix: &str) -> minga_bus::Result<KvStream> {
            self.inner.watch_assignments(prefix).await
        }

        async fn put_status(&self, key: &str, value: &[u8]) -> minga_bus::Result<()> {
            self.inner.put_status(key, value).await
        }

        async fn get_status(&self, key: &str) -> minga_bus::Result<Option<Vec<u8>>> {
            self.inner.get_status(key).await
        }

        async fn list_status(&self) -> minga_bus::Result<Vec<(String, Vec<u8>)>> {
            self.inner.list_status().await
        }

        async fn watch_status(&self) -> minga_bus::Result<KvStream> {
            self.inner.watch_status().await
        }
        async fn put_tenant_status(
            &self,
            tenant_id: &str,
            key: &str,
            value: &[u8],
        ) -> minga_bus::Result<()> {
            self.inner.put_tenant_status(tenant_id, key, value).await
        }

        async fn get_tenant_status(
            &self,
            tenant_id: &str,
            key: &str,
        ) -> minga_bus::Result<Option<Vec<u8>>> {
            self.inner.get_tenant_status(tenant_id, key).await
        }

        async fn list_tenant_status(
            &self,
            tenant_id: &str,
        ) -> minga_bus::Result<Vec<(String, Vec<u8>)>> {
            self.inner.list_tenant_status(tenant_id).await
        }

        async fn list_all_tenant_status(
            &self,
            tenant_ids: &[String],
        ) -> minga_bus::Result<Vec<(String, String, Vec<u8>)>> {
            self.inner.list_all_tenant_status(tenant_ids).await
        }

        async fn refresh_lease(&self, node_id: u64, registration: &[u8]) -> minga_bus::Result<()> {
            self.inner.refresh_lease(node_id, registration).await
        }

        async fn is_alive(&self, node_id: u64) -> minga_bus::Result<bool> {
            self.inner.is_alive(node_id).await
        }

        async fn list_alive_nodes(&self) -> minga_bus::Result<HashSet<u64>> {
            self.inner.list_alive_nodes().await
        }

        async fn watch_leases(&self) -> minga_bus::Result<KvStream> {
            self.inner.watch_leases().await
        }

        async fn request(
            &self,
            subject: &str,
            payload: &[u8],
            timeout: Duration,
        ) -> minga_bus::Result<Vec<u8>> {
            self.inner.request(subject, payload, timeout).await
        }

        async fn subscribe_requests(
            &self,
            subject: &str,
        ) -> minga_bus::Result<minga_bus::RequestStream> {
            self.inner.subscribe_requests(subject).await
        }

        async fn reply(&self, subject: &str, payload: &[u8]) -> minga_bus::Result<()> {
            self.inner.reply(subject, payload).await
        }

        async fn publish_log(&self, task_id: &str, entry: &LogEntry) -> minga_bus::Result<()> {
            self.inner.publish_log(task_id, entry).await
        }

        async fn publish_node_log(
            &self,
            node_id: u64,
            task_id: &str,
            entry: &LogEntry,
        ) -> minga_bus::Result<()> {
            self.inner.publish_node_log(node_id, task_id, entry).await
        }
        async fn publish_tenant_node_log(
            &self,
            tenant_id: &str,
            node_id: u64,
            task_id: &str,
            entry: &LogEntry,
        ) -> minga_bus::Result<()> {
            self.inner
                .publish_tenant_node_log(tenant_id, node_id, task_id, entry)
                .await
        }

        async fn subscribe_logs(
            &self,
            task_id: &str,
        ) -> minga_bus::Result<minga_bus::LogEventStream> {
            self.inner.subscribe_logs(task_id).await
        }
        async fn subscribe_tenant_logs(
            &self,
            tenant_id: &str,
            task_id: &str,
        ) -> minga_bus::Result<minga_bus::LogEventStream> {
            self.inner.subscribe_tenant_logs(tenant_id, task_id).await
        }

        async fn log_history(
            &self,
            task_id: &str,
            tail: usize,
        ) -> minga_bus::Result<Vec<LogEntry>> {
            self.inner.log_history(task_id, tail).await
        }
        async fn tenant_log_history(
            &self,
            tenant_id: &str,
            task_id: &str,
            tail: usize,
        ) -> minga_bus::Result<Vec<LogEntry>> {
            self.inner
                .tenant_log_history(tenant_id, task_id, tail)
                .await
        }
    }

    #[tokio::test]
    async fn assignments_are_node_scoped_idempotent_and_stop_removed_tasks() {
        let (supervisor, runtime, bus) = setup(7).await;
        write_assignment(&bus, &assignment("first")).await;
        write_assignment(&bus, &assignment("second")).await;
        write_assignment(&bus, &assignment_on("other-node", 8)).await;

        assert_eq!(supervisor.reconcile_once().await.unwrap(), 2);
        assert_eq!(runtime.container_count(), 2);
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 0);
        bus.delete_assignment(&subjects::kv_assignment_task("default", 7, "first"))
            .await
            .unwrap();
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(runtime.container_count(), 1);
        assert_eq!(supervisor.running_tasks().await, vec!["second".to_string()]);
    }

    #[tokio::test]
    async fn start_failure_does_not_crash_and_retries_assignment() {
        let (supervisor, runtime, bus) = setup(7).await;
        *runtime.fail_starts.lock().unwrap() = true;
        write_assignment(&bus, &assignment("task-1")).await;
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 0);
        assert_eq!(supervisor.running_count().await, 0);

        *runtime.fail_starts.lock().unwrap() = false;
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(runtime.container_count(), 1);
    }

    #[tokio::test]
    async fn supervisor_starts_from_assignment_kv() {
        let bus = MemoryBus::new();
        let assignment = assignment("task-1");
        let bytes = minga_state::codec::encode_wire(&assignment).unwrap();
        bus.put_assignment(
            &subjects::kv_assignment_task("default", 7, "task-1"),
            &bytes,
        )
        .await
        .unwrap();
        let supervisor =
            Supervisor::with_bus(Arc::new(MockRuntime::new()), bus, assignment.node_id);

        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(supervisor.running_tasks().await, vec!["task-1".to_string()]);
    }

    #[tokio::test]
    async fn startup_adopts_assigned_containers_and_stops_the_rest() {
        let (supervisor, runtime, bus) = setup(7).await;
        runtime
            .start(&"kept".to_string(), &TaskSpec::default())
            .await
            .unwrap();
        runtime
            .start(&"orphan".to_string(), &TaskSpec::default())
            .await
            .unwrap();
        runtime
            .start(&"dead-orphan".to_string(), &TaskSpec::default())
            .await
            .unwrap();
        runtime.mark_failed("mock-0003");
        write_assignment(&bus, &assignment("kept")).await;

        assert_eq!(supervisor.recover_running_containers().await, 2);
        assert_eq!(runtime.container_count(), 1);
        assert_eq!(supervisor.running_tasks().await, vec!["kept".to_string()]);
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 0);

        bus.delete_assignment(&subjects::kv_assignment_task("default", 7, "kept"))
            .await
            .unwrap();
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(runtime.container_count(), 0);
    }
    #[tokio::test]
    async fn assignment_from_older_format_version_is_refused_loudly() {
        let bus = MemoryBus::new();
        let mut bytes = minga_state::codec::encode_wire(&assignment("task-1")).unwrap();
        bytes[0] = bytes[0].wrapping_add(1);
        bus.put_assignment(
            &subjects::kv_assignment_task("default", 7, "task-1"),
            &bytes,
        )
        .await
        .unwrap();
        let source = Assignments::new_for_tenant(bus, Some(7), "default");

        assert!(source.list().await.is_err());
    }
    #[tokio::test]
    async fn bus_unreachable_preserves_tasks_then_stops_only_removed_assignments() {
        let inner = MemoryBus::new();
        let bus = Arc::new(ToggleBus {
            inner: inner.clone(),
            assignments_offline: std::sync::atomic::AtomicBool::new(false),
        });
        write_assignment(&inner, &assignment("keep")).await;
        write_assignment(&inner, &assignment("move")).await;
        let runtime = Arc::new(MockRuntime::new());
        let supervisor = Supervisor::with_bus(runtime.clone(), bus.clone(), 7);
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 2);
        assert_eq!(runtime.container_count(), 2);

        bus.assignments_offline
            .store(true, std::sync::atomic::Ordering::SeqCst);
        inner
            .delete_assignment(&subjects::kv_assignment_task("default", 7, "move"))
            .await
            .unwrap();
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 0);
        assert_eq!(runtime.container_count(), 2);

        bus.assignments_offline
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(runtime.container_count(), 1);
        assert_eq!(supervisor.running_tasks().await, vec!["keep".to_string()]);
    }
    #[tokio::test]
    async fn status_logs_and_restart_follow_assignment() {
        let bus = MemoryBus::new();
        let mut desired = assignment("task-1");
        desired.spec.restart_policy = minga_state::types::RestartPolicy {
            condition: RestartCondition::Any,
            delay_secs: 0,
            max_attempts: None,
        };
        let bytes = minga_state::codec::encode_wire(&desired).unwrap();
        bus.put_assignment(
            &subjects::kv_assignment_task("default", 7, "task-1"),
            &bytes,
        )
        .await
        .unwrap();
        let runtime = Arc::new(MockRuntime::new());
        let supervisor = Supervisor::with_bus(runtime.clone(), bus.clone(), 7);
        supervisor.reconcile_once().await.unwrap();

        let status = bus
            .get_tenant_status("default", &subjects::kv_node_task_status(7, "task-1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            TaskStatusReport::from_bytes(&status).unwrap().state,
            TaskActualState::Running
        );
        assert!(!bus
            .tenant_log_history("default", "task-1", 0)
            .await
            .unwrap()
            .is_empty());

        let failed_container = runtime.list().await[0].id.clone();
        runtime.mark_failed(&failed_container);
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(runtime.list().await.len(), 1);
    }
    #[tokio::test]
    async fn healthcheck_gates_running_then_an_unhealthy_task_restarts() {
        let bus = MemoryBus::new();
        let mut desired = assignment("task-1");
        desired.spec.restart_policy = minga_state::types::RestartPolicy {
            condition: RestartCondition::Any,
            delay_secs: 0,
            max_attempts: None,
        };
        desired.spec.healthcheck = Some(minga_state::types::Healthcheck {
            test: vec!["CMD".to_string(), "true".to_string()],
            interval_secs: Some(0),
            timeout_secs: Some(1),
            start_period_secs: Some(0),
            retries: Some(1),
        });
        write_assignment(&bus, &desired).await;
        let runtime = Arc::new(MockRuntime::new());
        let supervisor = Supervisor::with_bus(runtime.clone(), bus.clone(), desired.node_id);
        for _ in 0..100 {
            supervisor.reconcile_once().await.unwrap();
            if actual_status(&bus, 7, "task-1").await.state == TaskActualState::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            actual_status(&bus, 7, "task-1").await.state,
            TaskActualState::Running
        );
        let container = runtime.list().await[0].id.clone();
        runtime.exec_codes.lock().unwrap().insert(container, 1);
        for _ in 0..100 {
            supervisor.reconcile_once().await.unwrap();
            if actual_status(&bus, 7, "task-1").await.restarts > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(actual_status(&bus, 7, "task-1").await.restarts, 1);
    }
    #[tokio::test]
    async fn agent_assignments_start_stop() {
        let (supervisor, runtime, executor, bus) = setup_with_agent(7).await;
        write_assignment(&bus, &agent_assignment("agent-1")).await;
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(runtime.container_count(), 0);
        assert_eq!(executor.agent_count(), 1);

        bus.delete_assignment(&subjects::kv_assignment_task("default", 7, "agent-1"))
            .await
            .unwrap();
        assert_eq!(supervisor.reconcile_once().await.unwrap(), 1);
        assert_eq!(executor.agent_count(), 0);
    }

    #[tokio::test]
    async fn agent_assignment_without_executor_is_not_started() {
        let (supervisor, runtime, bus) = setup(7).await;
        write_assignment(&bus, &agent_assignment("agent-1")).await;

        assert_eq!(supervisor.reconcile_once().await.unwrap(), 0);
        assert_eq!(runtime.container_count(), 0);
    }

    #[tokio::test]
    async fn terminal_container_is_not_restarted_while_assignment_remains() {
        let (supervisor, runtime, bus) = setup(7).await;
        let mut desired = assignment("task-1");
        desired.spec.restart_policy.condition = RestartCondition::None;
        write_assignment(&bus, &desired).await;
        supervisor.reconcile_once().await.unwrap();
        let container = runtime.list().await[0].id.clone();
        runtime.mark_failed(&container);

        supervisor.reconcile_once().await.unwrap();
        assert_eq!(runtime.container_count(), 0);
        supervisor.reconcile_once().await.unwrap();
        assert_eq!(runtime.container_count(), 0);
    }

    #[tokio::test]
    async fn container_logs_are_not_republished_after_the_cursor_advances() {
        let (supervisor, _, bus) = setup(7).await;
        write_assignment(&bus, &assignment("task-1")).await;
        supervisor.reconcile_once().await.unwrap();
        let first_count = bus
            .tenant_log_history("default", "task-1", 0)
            .await
            .unwrap()
            .len();
        assert!(first_count > 0);

        supervisor.reconcile_once().await.unwrap();
        assert_eq!(
            bus.tenant_log_history("default", "task-1", 0)
                .await
                .unwrap()
                .len(),
            first_count
        );
    }
    #[tokio::test]
    async fn completed_agent_result_is_published_as_a_log() {
        let (supervisor, _, executor, bus) = setup_with_agent(7).await;
        *executor.auto_complete.lock().unwrap() = true;
        write_assignment(&bus, &agent_assignment("agent-1")).await;

        supervisor.reconcile_once().await.unwrap();
        assert!(!bus
            .tenant_log_history("default", "agent-1", 0)
            .await
            .unwrap()
            .is_empty());
    }
    #[test]
    fn dns_records_come_from_network_assignment_entries() {
        let record = NetworkAssignment {
            network: minga_state::types::Network {
                id: "network-1".to_string(),
                tenant_id: "default".to_string(),
                name: "network".to_string(),
                driver: "bridge".to_string(),
                attachable: true,
                subnet: Some("10.0.0.0/24".to_string()),
                wg_port: None,
            },
            tasks: vec![
                minga_state::types::NetworkDnsTask {
                    node_id: 7,
                    task_id: "task-1".to_string(),
                    service_name: "web".to_string(),
                    stack: "plant".to_string(),
                    ip: "10.0.0.10".to_string(),
                },
                minga_state::types::NetworkDnsTask {
                    node_id: 8,
                    task_id: "task-2".to_string(),
                    service_name: "web".to_string(),
                    stack: "plant".to_string(),
                    ip: "10.0.0.11".to_string(),
                },
            ],
        };

        let mut remote_only = record.clone();
        remote_only.network.id = "network-2".to_string();
        remote_only.tasks = vec![minga_state::types::NetworkDnsTask {
            node_id: 8,
            task_id: "task-3".to_string(),
            service_name: "web".to_string(),
            stack: "plant".to_string(),
            ip: "10.0.1.12".to_string(),
        }];

        let dns = build_dns_store(&[record, remote_only], 7);
        // Remote tasks on a shared network resolve; a network with no local
        // task contributes nothing.
        assert_eq!(
            dns.resolve(&minga_net::dns::service_fqdn("web", "plant")),
            vec![
                std::net::Ipv4Addr::new(10, 0, 0, 10),
                std::net::Ipv4Addr::new(10, 0, 0, 11)
            ]
        );
    }
}
