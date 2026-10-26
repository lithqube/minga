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
pub mod placement;
pub mod reconcile;
pub mod translate;

use std::sync::Arc;
use std::time::Duration;

use minga_bus::{Command, EventBus};
use minga_state::{StateError, StateStore};
use thiserror::Error;

pub use constraints::{evaluate, matches_str, parse_constraint, Constraint, ConstraintOp};
pub use placement::place_service;
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
}

impl Scheduler {
    pub fn new(state: Arc<StateStore>) -> Self {
        Self { state, bus: None }
    }

    /// Create a scheduler that publishes commands to the event bus.
    pub fn with_bus(state: Arc<StateStore>, bus: Arc<dyn EventBus>) -> Self {
        Self {
            state,
            bus: Some(bus),
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

        let nodes = self.state.list_nodes().await?;
        let services = self.state.list_services().await?;
        let tasks = self.state.list_tasks().await?;

        let actions = plan(&services, &tasks, &nodes);

        for action in &actions {
            match action {
                ReconcileAction::CreateTask(task) => {
                    tracing::info!(
                        task = %task.id,
                        service = %task.service_id,
                        node = task.node_id,
                        "creating task"
                    );
                    self.state.upsert_task(task.clone()).await?;

                    // Publish assign command to the bus.
                    if let Some(bus) = &self.bus {
                        let payload =
                            bincode::serde::encode_to_vec(&task.spec, bincode::config::standard())
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
