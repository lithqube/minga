//! Scheduler: turns Compose services into placed tasks on cluster nodes.
//!
//! Minga's scheduler has two responsibilities:
//!
//! 1. **Translation** ([`translate`]) — the deploy path. Converts a
//!    Compose [`Project`](minga_compose::Project) into state entities
//!    ([`Service`](minga_state::types::Service),
//!    [`Network`](minga_state::types::Network), …) that are proposed
//!    to Raft.
//!
//! 2. **Reconciliation** ([`reconcile`]) — the manager loop. Runs on
//!    the Raft leader, reads the desired services and current tasks,
//!    computes placement ([`placement`]), and writes the diff back to
//!    the state store.
//!
//! Placement uses a spread strategy with resource-capacity checking and
//! constraint evaluation ([`constraints`]). Task IDs are deterministic,
//! so reconciliation is idempotent.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use minga_scheduler::Scheduler;
//! use minga_state::StateStore;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let state = Arc::new(StateStore::open(1, "10.0.0.1:50051", "/var/lib/minga", Default::default()).await?);
//! let scheduler = Scheduler::new(state);
//! scheduler.run(Duration::from_secs(5)).await;
//! # Ok(())
//! # }
//! ```

pub mod constraints;
pub mod netassign;
pub mod placement;
pub mod reconcile;
pub mod secrets;
pub mod translate;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{subjects, EventBus, TaskActualState, TaskStatusReport};
use minga_state::types::{
    Network, NetworkAssignment, NetworkDnsTask, Service, Task, TaskAssignment, TaskState,
};
use minga_state::{StateError, StateStore};
use thiserror::Error;

pub use constraints::{evaluate, matches_str, parse_constraint, Constraint, ConstraintOp};
pub use placement::{check_placeable, place_service, PlacementError};
pub use reconcile::{plan, ReconcileAction};
pub use translate::{parse_memory, translate_project, TranslateError, TranslatedStack};

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("bus error: {0}")]
    Bus(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Merges current task status from MINGA_STATUS with Raft's desired task record.
pub fn task_with_actual_status(mut task: Task, report: Option<&TaskStatusReport>) -> Task {
    if let Some(report) = report.filter(|report| report.node_id == task.node_id) {
        task.state = match report.state {
            TaskActualState::Starting => TaskState::Starting,
            TaskActualState::Running => TaskState::Running,
            TaskActualState::Stopped => TaskState::Stopped,
            TaskActualState::Failed => TaskState::Failed,
        };
        task.restarts = report.restarts;
        task.last_error.clone_from(&report.last_error);
        task.last_exit_code = report.last_exit_code;
        task.running_since_unix = report.running_since_unix;
    } else {
        task.state = TaskState::Starting;
        task.running_since_unix = None;
        task.restarts = 0;
        task.last_error = None;
        task.last_exit_code = None;
    }
    task
}

/// The scheduler: runs the reconcile loop on the Raft leader.
///
/// Only the leader reconciles. Followers stay idle — if leadership
/// changes, the new leader picks up the loop.
pub struct Scheduler {
    state: Arc<StateStore>,
    bus: Option<Arc<dyn EventBus>>,
    supernet: minga_net::Cidr,
    /// Subnet collisions already logged, so each is reported once.
    reported: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
    /// Until this instant the lease set may be incomplete after a failed
    /// lease read, so the pass is skipped instead of removing tasks.
    lease_grace_until: std::sync::Mutex<Option<std::time::Instant>>,
}

/// Leases that expired during a bus outage come back within one renew
/// interval; wait two before trusting a missing lease.
const LEASE_GRACE: Duration = Duration::from_secs(2 * minga_state::types::NODE_RENEW_INTERVAL_SECS);

impl Scheduler {
    pub fn new(state: Arc<StateStore>) -> Self {
        Self {
            state,
            bus: None,
            supernet: minga_net::Cidr::parse(minga_net::DEFAULT_SUPERNET)
                .expect("default supernet parses"),
            reported: Default::default(),
            lease_grace_until: Default::default(),
        }
    }

    /// Allocates network subnets from `supernet` instead of 10.100.0.0/16.
    pub fn with_supernet(mut self, supernet: minga_net::Cidr) -> Self {
        self.supernet = supernet;
        self
    }

    /// Create a scheduler that publishes assignments to the event bus.
    pub fn with_bus(state: Arc<StateStore>, bus: Arc<dyn EventBus>) -> Self {
        Self {
            bus: Some(bus),
            ..Self::new(state)
        }
    }

    /// Runs one reconciliation pass. Returns the actions taken.
    ///
    /// If this node is not the leader, returns an empty vec without
    /// doing any work.
    pub async fn reconcile_once(&self) -> Result<Vec<ReconcileAction>, SchedulerError> {
        if !self.state.is_leader().await {
            return Ok(Vec::new());
        }
        let _timer = minga_telemetry::METRICS
            .scheduler_reconcile_duration_seconds
            .start_timer();

        let mut nodes = self.state.list_nodes().await?;
        if let Some(bus) = &self.bus {
            let live_nodes = match bus.list_alive_nodes().await {
                Ok(nodes) => nodes,
                Err(error) => {
                    tracing::warn!(error = %error, "cannot read node leases; skipping placement");
                    *self.lease_grace_until.lock().unwrap() =
                        Some(std::time::Instant::now() + LEASE_GRACE);
                    return Ok(Vec::new());
                }
            };
            // A missing lease removes that node's tasks. After a bus outage,
            // or with no lease at all, that would stop every task.
            let in_grace = self
                .lease_grace_until
                .lock()
                .unwrap()
                .is_some_and(|until| std::time::Instant::now() < until);
            let none_live =
                !nodes.is_empty() && !nodes.iter().any(|node| live_nodes.contains(&node.id));
            if none_live {
                // Leases may be coming back; a partial set right after this
                // must not count as dead nodes either.
                *self.lease_grace_until.lock().unwrap() =
                    Some(std::time::Instant::now() + LEASE_GRACE);
            }
            // With every registered node live, no lease can be missing in error.
            let all_live = nodes.iter().all(|node| live_nodes.contains(&node.id));
            if all_live {
                // Recovery is complete; a later missing lease is a real failure.
                *self.lease_grace_until.lock().unwrap() = None;
            }
            if none_live || (in_grace && !all_live) {
                tracing::warn!(
                    live = live_nodes.len(),
                    registered = nodes.len(),
                    "node leases may be incomplete; skipping reconciliation"
                );
                return Ok(Vec::new());
            }
            nodes.retain(|node| live_nodes.contains(&node.id));
        }
        let services = self.state.list_services().await?;
        let mut tasks = self.state.list_tasks().await?;
        if let Some(bus) = &self.bus {
            let status_entries = match bus.list_status().await {
                Ok(entries) => entries,
                Err(error) => {
                    tracing::warn!(error = %error, "cannot read task status; skipping reconciliation");
                    return Ok(Vec::new());
                }
            };
            let mut statuses = HashMap::new();
            for (key, bytes) in status_entries {
                if !key.starts_with("node.") || !key.contains(".task.") {
                    continue;
                }
                match TaskStatusReport::from_bytes(&bytes) {
                    Ok(report)
                        if key
                            == subjects::kv_node_task_status(report.node_id, &report.task_id) =>
                    {
                        statuses.insert(report.task_id.clone(), report);
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(key, error = %error, "cannot decode task status"),
                }
            }
            tasks = tasks
                .into_iter()
                .map(|task| {
                    let status = statuses.get(&task.id);
                    task_with_actual_status(task, status)
                })
                .collect();
        }
        let mut networks = self.state.list_networks().await?;
        self.fill_subnets(&mut networks, &tasks).await?;

        let actions = plan(
            &services,
            &tasks,
            &nodes,
            &networks,
            minga_state::types::unix_now(),
        );

        for action in &actions {
            match action {
                ReconcileAction::CreateTask(task) => {
                    tracing::info!(
                        task = %task.id,
                        service = %task.service_id,
                        node = task.node_id,
                        "creating task"
                    );
                    self.state.upsert_task(task.as_ref().clone()).await?;
                    minga_telemetry::METRICS
                        .scheduler_placement_decisions_total
                        .with_label_values(&["placed"])
                        .inc();
                }
                ReconcileAction::RollbackService(service_id) => {
                    let Some(current) = services.iter().find(|s| &s.id == service_id) else {
                        continue;
                    };
                    let Some(previous) = current.previous.clone() else {
                        continue;
                    };
                    tracing::warn!(service = %service_id, "rollout failed, rolling back");
                    let mut rolled_back = *previous;
                    rolled_back.rollback = current.rollback;
                    if let Some(rollback_policy) = &current.rollback {
                        rolled_back.update = *rollback_policy;
                    }
                    self.state.upsert_service(rolled_back).await?;
                }
                ReconcileAction::SettleService(service_id) => {
                    let Some(service) = services.iter().find(|s| &s.id == service_id) else {
                        continue;
                    };
                    tracing::info!(service = %service_id, "rollout settled");
                    self.state
                        .upsert_service(Service {
                            previous: None,
                            ..service.clone()
                        })
                        .await?;
                }
                ReconcileAction::RemoveTask { task_id, .. } => {
                    tracing::info!(task = %task_id, "removing task");
                    self.state.remove_task(task_id.clone()).await?;
                }
            }
        }

        self.publish_projection().await?;
        Ok(actions)
    }

    async fn publish_projection(&self) -> Result<(), SchedulerError> {
        let Some(bus) = &self.bus else {
            return Ok(());
        };
        let services = self.state.list_services().await?;
        let tasks = self.state.list_tasks().await?;
        let networks = self.state.list_networks().await?;
        let service_by_id: HashMap<_, _> = services
            .iter()
            .map(|service| (service.id.as_str(), service))
            .collect();
        let network_by_id: HashMap<_, _> = networks
            .iter()
            .map(|network| (network.id.as_str(), network))
            .collect();
        let mut network_tasks: HashMap<&str, Vec<NetworkDnsTask>> = HashMap::new();
        let mut desired = HashSet::new();
        for task in &tasks {
            let Some(service) = service_by_id.get(task.service_id.as_str()) else {
                continue;
            };
            let task_networks: Vec<Network> = task
                .spec
                .ips
                .keys()
                .filter_map(|id| {
                    network_by_id
                        .get(id.as_str())
                        .map(|network| (*network).clone())
                })
                .collect();
            let assignment = TaskAssignment {
                task_id: task.id.clone(),
                node_id: task.node_id,
                service_id: task.service_id.clone(),
                service_name: service.name.clone(),
                stack: service.env.get("MINGA_STACK").cloned().unwrap_or_default(),
                spec: task.spec.clone(),
                networks: task_networks,
            };
            let key = subjects::kv_assignment_task(task.node_id, &task.id);
            let value = minga_state::codec::encode_wire(&assignment)
                .map_err(|error| SchedulerError::Serialization(error.to_string()))?;
            bus.put_assignment(&key, &value)
                .await
                .map_err(|error| SchedulerError::Bus(error.to_string()))?;
            desired.insert(key);
            for (network_id, ip) in &task.spec.ips {
                if network_by_id.contains_key(network_id.as_str()) {
                    network_tasks
                        .entry(network_id)
                        .or_default()
                        .push(NetworkDnsTask {
                            node_id: task.node_id,
                            task_id: task.id.clone(),
                            service_name: service.name.clone(),
                            stack: assignment.stack.clone(),
                            ip: ip.clone(),
                        });
                }
            }
        }
        for network in networks {
            let value = NetworkAssignment {
                tasks: network_tasks
                    .remove(network.id.as_str())
                    .unwrap_or_default(),
                network: network.clone(),
            };
            let value = minga_state::codec::encode_wire(&value)
                .map_err(|error| SchedulerError::Serialization(error.to_string()))?;
            let key = subjects::kv_assignment_network(&network.id);
            desired.insert(key.clone());
            bus.put_assignment(&key, &value)
                .await
                .map_err(|error| SchedulerError::Bus(error.to_string()))?;
        }
        for (key, _) in bus
            .list_assignments("")
            .await
            .map_err(|error| SchedulerError::Bus(error.to_string()))?
        {
            if !desired.contains(&key) {
                bus.delete_assignment(&key)
                    .await
                    .map_err(|error| SchedulerError::Bus(error.to_string()))?;
            }
        }
        Ok(())
    }

    /// Stores a subnet for every network that has none (Raft serializes
    /// these through the leader, so two networks never race for one /24)
    /// and logs each subnet collision once.
    async fn fill_subnets(
        &self,
        networks: &mut [minga_state::types::Network],
        tasks: &[minga_state::types::Task],
    ) -> Result<(), SchedulerError> {
        let (changed, errors) = netassign::fill_subnets(networks, tasks, &self.supernet);
        for e in errors {
            tracing::error!(error = %e, "cannot assign a network subnet");
        }
        for i in changed {
            let net = &networks[i];
            tracing::info!(network = %net.id, subnet = ?net.subnet, "network subnet assigned");
            self.state.upsert_network(net.clone()).await?;
        }
        let mut reported = self.reported.lock().unwrap_or_else(|e| e.into_inner());
        for (a, b, sa, sb) in netassign::subnet_collisions(networks) {
            if reported.insert((a.clone(), b.clone())) {
                tracing::error!(
                    "networks {a} ({sa}) and {b} ({sb}) overlap, so their tasks break each \
                     other's bridges. Fix: remove the services on one of them and the network \
                     itself, then redeploy; it gets a free /24 (or set a non-overlapping \
                     `ipam` subnet in its Compose file)"
                );
            }
        }
        Ok(())
    }

    /// Runs the reconcile loop forever, sleeping `interval` between passes.
    ///
    /// This function never returns. Errors in individual passes are logged
    /// and the loop continues.
    pub async fn run(self, interval: Duration) -> ! {
        tracing::info!(?interval, "scheduler loop starting");
        loop {
            match self.reconcile_once().await {
                Ok(actions) => {
                    if !actions.is_empty() {
                        tracing::debug!(count = actions.len(), "reconcile pass applied actions");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "reconcile pass failed");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}
