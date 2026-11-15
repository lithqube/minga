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

use std::sync::Arc;
use std::time::Duration;

use minga_bus::{Command, EventBus};
use minga_state::types::Service;
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

/// The scheduler: runs the reconcile loop on the Raft leader.
///
/// Only the leader reconciles. Followers stay idle — if leadership
/// changes, the new leader picks up the loop.
///
/// When constructed with [`Scheduler::with_bus`], the scheduler also
/// publishes task assign/stop commands to the event bus, which the
/// node supervisors consume.
pub struct Scheduler {
    state: Arc<StateStore>,
    bus: Option<Arc<dyn EventBus>>,
    supernet: minga_net::Cidr,
    /// Subnet collisions already logged, so each is reported once.
    reported: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
}

impl Scheduler {
    pub fn new(state: Arc<StateStore>) -> Self {
        Self {
            state,
            bus: None,
            supernet: minga_net::Cidr::parse(minga_net::DEFAULT_SUPERNET)
                .expect("default supernet parses"),
            reported: Default::default(),
        }
    }

    /// Allocates network subnets from `supernet` instead of 10.100.0.0/16.
    pub fn with_supernet(mut self, supernet: minga_net::Cidr) -> Self {
        self.supernet = supernet;
        self
    }

    /// Create a scheduler that publishes commands to the event bus.
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

        let nodes = self.state.list_nodes().await?;
        let services = self.state.list_services().await?;
        let tasks = self.state.list_tasks().await?;
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

                    // Publish assign command to the bus.
                    if let Some(bus) = &self.bus {
                        let payload = minga_state::codec::encode_wire(&task.spec)
                            .map_err(|e| SchedulerError::Serialization(e.to_string()))?;
                        let cmd = Command::Assign {
                            task_id: task.id.clone(),
                            payload,
                        };
                        if let Err(e) = bus.send_command(task.node_id, &cmd).await {
                            tracing::error!(
                                task = %task.id,
                                node = task.node_id,
                                error = %e,
                                "failed to publish assign command"
                            );
                        }
                    }
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
                ReconcileAction::RemoveTask { task_id, node_id } => {
                    tracing::info!(task = %task_id, "removing task");
                    self.state.remove_task(task_id.clone()).await?;

                    // Publish stop command to the bus.
                    if let Some(bus) = &self.bus {
                        let cmd = Command::Stop {
                            task_id: task_id.clone(),
                        };
                        if let Err(e) = bus.send_command(*node_id, &cmd).await {
                            tracing::error!(
                                task = %task_id,
                                node = node_id,
                                error = %e,
                                "failed to publish stop command"
                            );
                        }
                    }
                }
            }
        }

        Ok(actions)
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
