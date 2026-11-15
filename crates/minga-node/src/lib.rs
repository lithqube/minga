//! Node supervisor: runs tasks assigned to this node.
//!
//! Every node reconciles local containers with MINGA_ASSIGN records projected
//! from Raft by the scheduler.
//!
//! # Architecture
//!
//! - **Desired state** is stored in Raft and projected to assignment KV
//!   for worker consumption.
//! - **Actual state** (which containers are running, their status) is
//!   observed locally and published through JetStream, not Raft.
//!
//! # Runtime abstraction
//!
//! The supervisor talks to the container runtime through the [`Runtime`]
//! trait. Two implementations are available:
//! - [`MockRuntime`] — in-memory simulation for testing
//! - [`ContainerdRuntime`] — real containers via containerd's gRPC API
//!

pub mod agent;
pub mod containerd;
pub(crate) mod failure;
pub(crate) mod health;
#[cfg(target_os = "linux")]
pub mod netns;
pub mod registration;
pub mod runtime;
pub mod secrets;
pub mod supervisor;
pub mod wireguard;

pub use agent::{
    shared_executor, AgentError, AgentExecutor, AgentInfo, AgentRunResult, AgentStatus,
    MockAgentExecutor, WasmtimeAgentExecutor,
};
pub use containerd::ContainerdRuntime;
#[cfg(target_os = "linux")]
pub use netns::{system_upstream_resolver, NetworkSetup, TaskNetAttachment};
pub use registration::{host_capacity, run_node_registration, NodeIdentity, NodeMembership};
pub use runtime::{
    shared_runtime, ContainerInfo, ContainerStatus, MockRuntime, Runtime, RuntimeError,
};
#[cfg(target_os = "linux")]
pub use supervisor::SupervisorNetworking;
pub use supervisor::{Assignments, Supervisor, SupervisorError};
pub use wireguard::{
    ensure_interface as ensure_wireguard_interface, load_or_generate_keypair,
    run_wireguard_assignments, WireGuardError,
};
