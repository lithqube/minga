use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HealthError {
    #[error("not ready: {0}")]
    NotReady(String),
}

/// Liveness probe — 200 OK means process is alive.
#[derive(Clone, Default)]
pub struct Healthz;

impl Healthz {
    pub fn check(&self) -> Result<(), HealthError> {
        Ok(())
    }
}

/// Readiness probe — 200 OK means node can serve traffic.
#[derive(Clone)]
pub struct Readyz {
    state: Arc<ReadyState>,
}

impl Readyz {
    pub fn new(state: ReadyState) -> Self {
        Self {
            state: Arc::new(state),
        }
    }

    pub fn check(&self) -> Result<(), HealthError> {
        self.state.check()
    }
}

/// Readiness state — updated by components as they become ready.
#[derive(Clone, Default)]
pub struct ReadyState {
    raft_ready: bool,
    bus_connected: bool,
}

impl ReadyState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_raft_ready(&mut self, ready: bool) {
        self.raft_ready = ready;
    }

    pub fn set_bus_connected(&mut self, connected: bool) {
        self.bus_connected = connected;
    }

    fn check(&self) -> Result<(), HealthError> {
        if !self.raft_ready {
            return Err(HealthError::NotReady("raft not ready".into()));
        }
        if !self.bus_connected {
            return Err(HealthError::NotReady("bus not connected".into()));
        }
        Ok(())
    }
}

/// Alias for compatibility with lib.rs exports
pub type HealthState = ReadyState;
