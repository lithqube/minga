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
//! minga.log.{id}             JetStream   container log chunks
//! minga.ctl.node.{id}.assign JetStream   task assignment command  [work queue]
//! minga.ctl.node.{id}.stop   JetStream   stop/remove command       [work queue]
//! minga.ctl.node.{id}.ping   core NATS   synchronous health probe [req/reply]
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

/// Stream of log entries.
pub type LogEventStream = Pin<Box<dyn Stream<Item = LogEntry> + Send>>;

/// A single log entry from a task (container stdout/stderr or agent output).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub task_id: String,
    pub stream: LogStream,
    pub data: Vec<u8>,
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
}

impl LogEntry {
    pub fn stdout(task_id: &str, data: impl Into<Vec<u8>>) -> Self {
        Self {
            task_id: task_id.to_string(),
            stream: LogStream::Stdout,
            data: data.into(),
            timestamp: now_millis(),
        }
    }

    pub fn stderr(task_id: &str, data: impl Into<Vec<u8>>) -> Self {
        Self {
            task_id: task_id.to_string(),
            stream: LogStream::Stderr,
            data: data.into(),
            timestamp: now_millis(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (entry, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))?;
        Ok(entry)
    }
}

/// Which output stream a log entry came from.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

impl std::fmt::Display for LogStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogStream::Stdout => write!(f, "stdout"),
            LogStream::Stderr => write!(f, "stderr"),
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Status reports (actual state, carried over the bus)
// ---------------------------------------------------------------------------

/// Actual state of a task, reported by the node supervisor over the bus.
///
/// This is the **actual** state — distinct from `TaskState` in `minga-state`,
/// which represents **desired** state in Raft. The supervisor publishes this
/// to the `MINGA_STATUS` KV bucket; the scheduler reads it for placement
/// decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskStatusReport {
    pub task_id: String,
    pub node_id: u64,
    pub state: TaskActualState,
    pub container_id: Option<String>,
    /// Unix timestamp in milliseconds.
    pub observed_at: u64,
}

impl TaskStatusReport {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (report, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))?;
        Ok(report)
    }
}

/// Actual state of a task (reported by the node, not stored in Raft).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskActualState {
    Starting,
    Running,
    Stopped,
    Failed,
}

/// Actual state of a node, reported by the node supervisor over the bus.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeStatusReport {
    pub node_id: u64,
    pub running_tasks: usize,
    /// Unix timestamp in milliseconds.
    pub observed_at: u64,
}

impl NodeStatusReport {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (report, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))?;
        Ok(report)
    }
}

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

    /// List all current status entries as (key, value) pairs.
    ///
    /// This is a snapshot of the latest value per key, not a stream.
    async fn list_status(&self) -> Result<Vec<(String, Vec<u8>)>>;

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

    // -- Logs (append-only, per-task) ----------------------------------------

    /// Publish a log entry for a task. Goes to the `MINGA_LOGS` stream.
    async fn publish_log(&self, task_id: &str, entry: &LogEntry) -> Result<()>;

    /// Subscribe to log entries published **after** this call.
    ///
    /// This is a live tail: it does not replay previously published entries.
    /// Use [`EventBus::log_history`] to read the retained backlog.
    async fn subscribe_logs(&self, task_id: &str) -> Result<LogEventStream>;

    /// Returns up to `tail` of the most recently published log entries for a
    /// task (or for `*`, across all tasks), oldest first.
    ///
    /// `tail == 0` means "all retained entries", bounded by the transport's
    /// retention limits.
    async fn log_history(&self, task_id: &str, tail: usize) -> Result<Vec<LogEntry>>;
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
