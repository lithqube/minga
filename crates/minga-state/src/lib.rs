//! Cluster state: openraft + redb, sharded Raft groups for desired state and placement.
//!
//! This crate provides [`StateStore`], the interface the rest of Minga uses to
//! read and write cluster state. Only desired state and placement go through
//! Raft; heartbeats, task status and agent run steps go through JetStream.
//!
//! # Architecture
//!
//! - **Raft log** (`RedbLogStore`): stores log entries, votes and the committed
//!   log id in `raft_log.redb`.
//! - **State machine** (`RedbStateMachine`): stores the cluster's desired state
//!   (nodes, services, tasks, networks, volumes, secrets, configs) in
//!   `state.redb`.
//! - **StateStore**: wraps the Raft instance and provides a high-level API.
//!
//! # Example
//!
//! ```no_run
//! use std::collections::BTreeMap;
//! use minga_state::{StateStore, types::{ClusterNode, NodeStatus, ResourceSpec}};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = StateStore::open(1, "10.0.0.1:50051", "/var/lib/minga", BTreeMap::new()).await?;
//!
//! store.upsert_node(ClusterNode {
//!     id: 1,
//!     address: "10.0.0.1:50051".to_string(),
//!     labels: Default::default(),
//!     status: NodeStatus::Active,
//!     capacity: ResourceSpec { cpus: 8.0, memory_bytes: 32 * 1024 * 1024 * 1024, gpus: 1 },
//! }).await?;
//!
//! let nodes = store.list_nodes().await?;
//! assert_eq!(nodes.len(), 1);
//! # Ok(())
//! # }
//! ```

mod command;
mod error;
mod log_store;
mod network;
mod state_machine;
mod store;
pub mod types;

use std::io::Cursor;

pub use command::{StateCommand, StateResponse};
pub use error::StateError;
pub use store::StateStore;

// The openraft type configuration for Minga's state store.
openraft::declare_raft_types!(
    /// Type config for the Minga cluster state Raft group.
    pub TypeConfig:
        D            = StateCommand,
        R            = StateResponse,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
