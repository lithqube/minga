use std::sync::atomic::{AtomicBool, Ordering};
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
    state: ReadyState,
}

impl Readyz {
    pub fn new(state: ReadyState) -> Self {
        Self { state }
    }

    pub fn check(&self) -> Result<(), HealthError> {
        self.state.check()
    }
}

/// Readiness flags, updated by components as they become ready. Clones
/// share the same flags, so a probe task can update what a handler reads.
#[derive(Clone)]
pub struct ReadyState {
    raft_ready: Arc<AtomicBool>,
    /// `None` for processes that run without a bus (e.g. `minga api`).
    bus_connected: Option<Arc<AtomicBool>>,
}

impl Default for ReadyState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyState {
    /// Ready when Raft has a leader and the bus is connected.
    pub fn new() -> Self {
        Self {
            raft_ready: Arc::default(),
            bus_connected: Some(Arc::default()),
        }
    }

    /// Ready when Raft has a leader; there is no bus to check.
    pub fn without_bus() -> Self {
        Self {
            raft_ready: Arc::default(),
            bus_connected: None,
        }
    }

    pub fn set_raft_ready(&self, ready: bool) {
        self.raft_ready.store(ready, Ordering::Relaxed);
    }

    pub fn set_bus_connected(&self, connected: bool) {
        if let Some(flag) = &self.bus_connected {
            flag.store(connected, Ordering::Relaxed);
        }
    }

    pub fn check(&self) -> Result<(), HealthError> {
        if !self.raft_ready.load(Ordering::Relaxed) {
            return Err(HealthError::NotReady("raft not ready".into()));
        }
        if let Some(flag) = &self.bus_connected {
            if !flag.load(Ordering::Relaxed) {
                return Err(HealthError::NotReady("bus not connected".into()));
            }
        }
        Ok(())
    }
}

/// Alias for compatibility with lib.rs exports
pub type HealthState = ReadyState;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_flags() {
        let ready = ReadyState::new();
        let probe = Readyz::new(ready.clone());
        assert!(probe.check().is_err());
        ready.set_raft_ready(true);
        assert!(probe.check().is_err(), "bus still down");
        ready.set_bus_connected(true);
        assert!(probe.check().is_ok());

        let api = ReadyState::without_bus();
        let probe = Readyz::new(api.clone());
        api.set_raft_ready(true);
        assert!(probe.check().is_ok());
    }
}
