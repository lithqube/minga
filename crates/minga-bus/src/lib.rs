//! Event bus abstraction for Minga.
//!
//! The bus is the channel for **actual state** — task status reports, node
//! heartbeats, lifecycle events, logs — and for **commands** (assign/stop tasks).
//! Desired state stays in Raft (`minga-state`); the bus carries everything else.
//!
//! Two implementations:
//!
//! - [`MemoryBus`] — in-process, for unit tests and single-node development.
//! - [`NatsBus`] — NATS/JetStream, for production multi-node clusters.
//!
//! All consumers (scheduler, supervisor, API) program against the [`EventBus`]
//! trait, so the transport is swappable without touching business logic.
//!
//! # Subject namespace
//!
//! ```text
//! minga.node.{id}.status     JetStream   node status reports
//! minga.node.{id}.event      JetStream   node lifecycle events
//! minga.task.{id}.status     JetStream   task status reports
//! minga.task.{id}.event      JetStream   task lifecycle events
//! minga.task.{id}.log        JetStream   container log chunks
//! minga.ctl.node.{id}.assign JetStream   task assignment command  [work queue]
//! minga.ctl.node.{id}.stop   JetStream   stop/remove command       [work queue]
//! minga.ctl.node.{id}.ping   core NATS   synchronous health probe  [req/reply]
//! minga.ctl.node.{id}.drain  core NATS   synchronous drain         [req/reply]
//! ```
//!
//! Use the [`subjects`] module helpers to build subject strings.

pub mod memory;
pub mod nats;
pub mod subjects;

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use memory::MemoryBus;
pub use nats::NatsBus;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum BusError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("publish error: {0}")]
    Publish(String),

    #[error("subscribe error: {0}")]
    Subscribe(String),

    #[error("command error: {0}")]
    Command(String),

    #[error("kv error: {0}")]
    Kv(String),

    #[error("lease error: {0}")]
    Lease(String),

    #[error("request timed out after {0:?}")]
    Timeout(Duration),

    #[error("no responder for subject `{0}`")]
    NoResponder(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("bus error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BusError>;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A command sent from the manager to a node (work-queue semantics).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Command {
    /// Start a task. `payload` is the serialized `TaskSpec` (bincode).
    Assign { task_id: String, payload: Vec<u8> },
    /// Stop and remove a task.
    Stop { task_id: String },
}

impl Command {
    /// Serialize to bytes for transport.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (cmd, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))?;
        Ok(cmd)
    }
}

/// An event on the bus (append-only fact).
#[derive(Debug, Clone)]
pub struct BusEvent {
    pub subject: String,
    pub payload: Vec<u8>,
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
}

/// A key-value change notification.
#[derive(Debug, Clone)]
pub struct KvEvent {
    pub key: String,
    /// `None` means the key was deleted or expired.
    pub value: Option<Vec<u8>>,
    pub op: KvOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvOp {
    Put,
    Delete,
}

/// An incoming request (for request/reply pattern).
#[derive(Debug, Clone)]
pub struct BusRequest {
    /// Subject to reply to.
    pub reply_subject: String,
    pub payload: Vec<u8>,
}

/// Stream of events.
pub type EventStream = Pin<Box<dyn Stream<Item = BusEvent> + Send>>;

/// Stream of KV change notifications.
pub type KvStream = Pin<Box<dyn Stream<Item = KvEvent> + Send>>;

/// Stream of incoming requests.
pub type RequestStream = Pin<Box<dyn Stream<Item = BusRequest> + Send>>;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// The event bus: commands, events, status, liveness, and request/reply.
///
/// All methods are async and the trait is dyn-compatible (`Arc<dyn EventBus>`).
#[async_trait]
pub trait EventBus: Send + Sync {
    // -- Events (append-only, durable) ---------------------------------------

    /// Publish an event to a subject. Durable in production (JetStream).
    async fn publish_event(&self, subject: &str, payload: &[u8]) -> Result<()>;

    /// Subscribe to events matching any of the given subject patterns.
    /// Patterns support `*` (one token) and `>` (many tokens) wildcards.
    async fn subscribe_events(&self, patterns: &[&str]) -> Result<EventStream>;

    // -- Commands (work queue, store-and-forward) ----------------------------

    /// Send a command to a specific node. Survives node disconnects.
    async fn send_command(&self, node_id: u64, cmd: &Command) -> Result<()>;

    /// Pull the next command for `node_id`. Blocks until one is available
    /// or the bus is shut down.
    async fn next_command(&self, node_id: u64) -> Result<Command>;

    // -- Status (latest-value KV) -------------------------------------------

    /// Store the latest status value for a key. Overwrites previous value.
    async fn put_status(&self, key: &str, value: &[u8]) -> Result<()>;

    /// Get the latest status value for a key, or `None` if not set.
    async fn get_status(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Watch for changes to any status key.
    async fn watch_status(&self) -> Result<KvStream>;

    // -- Liveness (TTL leases) ----------------------------------------------

    /// Refresh the liveness lease for `node_id`. Call periodically (e.g. every 5s).
    async fn refresh_lease(&self, node_id: u64) -> Result<()>;

    /// Check if `node_id`'s lease is still alive.
    async fn is_alive(&self, node_id: u64) -> Result<bool>;

    // -- Request / Reply (synchronous control) -------------------------------

    /// Send a request and wait for a reply, with a timeout.
    async fn request(&self, subject: &str, payload: &[u8], timeout: Duration) -> Result<Vec<u8>>;

    /// Subscribe to requests on a subject. Returns a stream of incoming requests.
    async fn subscribe_requests(&self, subject: &str) -> Result<RequestStream>;

    /// Send a reply to a request.
    async fn reply(&self, reply_subject: &str, payload: &[u8]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_roundtrip() {
        let cmd = Command::Assign {
            task_id: "plant-7.inference.1".to_string(),
            payload: vec![1, 2, 3],
        };
        let bytes = cmd.to_bytes().unwrap();
        let decoded = Command::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn command_stop_roundtrip() {
        let cmd = Command::Stop {
            task_id: "plant-7.worker.0".to_string(),
        };
        let bytes = cmd.to_bytes().unwrap();
        let decoded = Command::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, decoded);
    }
}
