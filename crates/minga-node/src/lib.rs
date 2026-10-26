//! Node supervisor: runs tasks assigned to this node.
//!
//! The supervisor is the worker-side component of Minga. It runs on every
//! node and reconciles the local container state with the tasks assigned
//! to this node in the Raft state store.
//!
//! # Architecture
//!
//! - **Desired state** (which tasks should run where) lives in Raft —
//!   written by the scheduler on the leader.
//! - **Actual state** (which containers are running, their status) is
//!   tracked locally by the supervisor. It does NOT go through Raft.
//!   Per the architecture principle: heartbeats and task status go
//!   through JetStream, not the consensus log.
//!
//! # Runtime abstraction
//!
//! The supervisor talks to the container runtime through the [`Runtime`]
//! trait. Two implementations are available:
//! - [`MockRuntime`] — in-memory simulation for testing
//! - [`ContainerdRuntime`] — real containers via containerd's gRPC API
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use minga_node::{MockRuntime, Supervisor};
//! use minga_state::StateStore;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let state = Arc::new(StateStore::open(1, "10.0.0.1:50051", "/var/lib/minga", Default::default()).await?);
//! let runtime = Arc::new(MockRuntime::new());
//! let supervisor = Supervisor::new(state, runtime, 1);
//! supervisor.run(Duration::from_secs(2)).await;
//! # Ok(())
//! # }
//! ```

pub mod agent;
pub mod containerd;
pub mod runtime;
pub mod supervisor;

pub use agent::{
    shared_executor, AgentError, AgentExecutor, AgentInfo, AgentRunResult, AgentStatus,
    MockAgentExecutor, WasmtimeAgentExecutor,
};
pub use containerd::ContainerdRuntime;
pub use runtime::{
    shared_runtime, ContainerInfo, ContainerStatus, MockRuntime, Runtime, RuntimeError,
};
pub use supervisor::{Supervisor, SupervisorError};
