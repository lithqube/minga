//! Event bus abstraction for Minga.
//!
//! The bus is the channel for actual state: task status reports, node
//! heartbeats, lifecycle events, logs, and desired task assignments.
//! Desired state stays in Raft (`minga-state`); the bus carries actual state
//! and the manager-authored assignment projection.
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
//! minga.{tenant}.node.{id}.status      JetStream   node status events
//! minga.{tenant}.node.{id}.event       JetStream   node lifecycle events
//! minga.{tenant}.task.{id}.status      JetStream   task lifecycle events
//! minga.{tenant}.task.{id}.event       JetStream   task lifecycle events
//! minga.{tenant}.log.node.{id}.{task}  JetStream   container log chunks
//! KV_MINGA_ASSIGN             KV          manager-authored tenant-qualified assignments
//! KV_MINGA_LEASES              KV          tenant-qualified node liveness leases
//! minga.{tenant}.ctl.node.{id}.ping    core NATS   synchronous health probe [req/reply]
//! minga.{tenant}.ctl.node.{id}.drain   core NATS   synchronous drain [req/reply]
//! ```
//!
//!
//! Use the [`subjects`] module helpers to build subject strings.

pub mod memory;
pub mod nats;
pub mod subjects;
pub mod tls;

use std::collections::HashSet;
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

/// A body-free accounting record for one completed agent-model request.
///
/// `occurred_at_unix_ms` supports bounded-window queries. Token counts and
/// cost are integers so serialization and aggregation never use floats.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageEvent {
    pub request_id: String,
    pub tenant_id: String,
    pub service_id: String,
    pub task_id: String,
    pub customer_id: String,
    pub route: String,
    pub requested_model: String,
    pub answered_model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_hit: bool,
    pub latency_ms: u64,
    pub status: u16,
    pub occurred_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_micros: Option<u64>,
}

/// Bounded per-tenant aggregate returned to cluster operators.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageTotals {
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_micros: u64,
}

/// Stream of usage events.
pub type UsageEventStream = Pin<Box<dyn Stream<Item = UsageEvent> + Send>>;

/// Stream of events.
pub type EventStream = Pin<Box<dyn Stream<Item = BusEvent> + Send>>;

/// Stream of KV change notifications.
pub type KvStream = Pin<Box<dyn Stream<Item = KvEvent> + Send>>;

/// Stream of incoming requests.
pub type RequestStream = Pin<Box<dyn Stream<Item = BusRequest> + Send>>;

pub(crate) fn valid_subject_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) fn usage_subject(tenant_id: &str, service_id: &str) -> Result<String> {
    if !valid_subject_token(tenant_id) || !valid_subject_token(service_id) {
        return Err(BusError::Other(
            "usage tenant and service must be valid subject tokens".to_string(),
        ));
    }
    Ok(format!("minga.{tenant_id}.usage.{service_id}"))
}

pub(crate) fn assignment_prefix_matches(prefix: &str, key: &str) -> bool {
    if prefix.is_empty() || prefix.ends_with('.') {
        key.starts_with(prefix)
    } else {
        key == prefix
    }
}

pub(crate) fn assignment_watch_filter(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('.') {
        format!("{prefix}>")
    } else {
        prefix.to_string()
    }
}

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
    pub restarts: u32,
    pub last_error: Option<String>,
    pub last_exit_code: Option<i32>,
    /// Unix timestamp in seconds.
    pub running_since_unix: Option<u64>,
}

#[derive(Deserialize)]
struct LegacyTaskStatusReport {
    task_id: String,
    node_id: u64,
    state: TaskActualState,
    container_id: Option<String>,
    observed_at: u64,
}

impl TaskStatusReport {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| BusError::Serialization(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if let Ok((report, _)) =
            bincode::serde::decode_from_slice::<Self, _>(bytes, bincode::config::standard())
        {
            return Ok(report);
        }
        let (legacy, _) = bincode::serde::decode_from_slice::<LegacyTaskStatusReport, _>(
            bytes,
            bincode::config::standard(),
        )
        .map_err(|e| BusError::Serialization(e.to_string()))?;
        Ok(Self {
            task_id: legacy.task_id,
            node_id: legacy.node_id,
            state: legacy.state,
            container_id: legacy.container_id,
            observed_at: legacy.observed_at,
            restarts: 0,
            last_error: None,
            last_exit_code: None,
            running_since_unix: (legacy.state == TaskActualState::Running)
                .then_some(legacy.observed_at / 1_000),
        })
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

/// The event bus: events, assignments, status, liveness, and request/reply.
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
    async fn put_assignment(&self, key: &str, value: &[u8]) -> Result<()>;
    async fn get_assignment(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn list_assignments(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
    async fn delete_assignment(&self, key: &str) -> Result<()>;
    /// Watch current and future assignments matching a prefix ending in `.`; otherwise the exact key.
    async fn watch_assignments(&self, prefix: &str) -> Result<KvStream>;

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

    /// Refresh a node lease under its tenant-qualified key.
    async fn refresh_lease(
        &self,
        tenant_id: &str,
        node_id: u64,
        registration: &[u8],
    ) -> Result<()>;

    /// Check whether a tenant's node lease key exists.
    async fn is_alive(&self, tenant_id: &str, node_id: u64) -> Result<bool>;

    /// Return the globally unique node IDs whose lease keys currently exist.
    async fn list_alive_nodes(&self) -> Result<HashSet<u64>>;

    /// Watch tenant-qualified lease changes, including current lease entries.
    async fn watch_leases(&self) -> Result<KvStream>;
    // -- Request / Reply (synchronous control) -------------------------------

    /// Send a request and wait for a reply, with a timeout.
    async fn request(&self, subject: &str, payload: &[u8], timeout: Duration) -> Result<Vec<u8>>;

    /// Subscribe to requests on a subject. Returns a stream of incoming requests.
    async fn subscribe_requests(&self, subject: &str) -> Result<RequestStream>;

    /// Send a reply to a request.
    async fn reply(&self, reply_subject: &str, payload: &[u8]) -> Result<()>;

    // -- Usage (append-only, tenant-isolated) --------------------------------

    /// Persist one request's usage record. Repeated `request_id` values within
    /// the JetStream duplicate window are idempotent.
    async fn publish_usage(&self, event: &UsageEvent) -> Result<()>;

    /// Read the retained usage events for exactly one tenant and time window.
    async fn usage_events(
        &self,
        tenant_id: &str,
        since_unix_ms: u64,
        customer: Option<&str>,
        limit: usize,
    ) -> Result<Vec<UsageEvent>>;

    /// Aggregate retained usage by tenant. No customer-level data is returned.
    async fn usage_totals(&self, since_unix_ms: u64) -> Result<Vec<(String, UsageTotals)>>;


    // -- Logs (append-only, per-task) ----------------------------------------

    async fn publish_log(&self, task_id: &str, entry: &LogEntry) -> Result<()>;

    /// Publish a log entry with its originating node ID. Worker clients are
    /// restricted to their own node; managers publish on behalf of workers.
    async fn publish_node_log(&self, node_id: u64, task_id: &str, entry: &LogEntry) -> Result<()>;

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
    fn task_status_decodes_pre_extension_reports() {
        #[derive(Serialize)]
        struct PriorTaskStatusReport {
            task_id: String,
            node_id: u64,
            state: TaskActualState,
            container_id: Option<String>,
            observed_at: u64,
        }

        let prior = PriorTaskStatusReport {
            task_id: "svc.1".into(),
            node_id: 7,
            state: TaskActualState::Running,
            container_id: Some("container-1".into()),
            observed_at: 123_000,
        };
        let bytes = bincode::serde::encode_to_vec(&prior, bincode::config::standard()).unwrap();
        let report = TaskStatusReport::from_bytes(&bytes).unwrap();

        assert_eq!(report.task_id, "svc.1");
        assert_eq!(report.node_id, 7);
        assert_eq!(report.state, TaskActualState::Running);
        assert_eq!(report.container_id.as_deref(), Some("container-1"));
        assert_eq!(report.observed_at, 123_000);
        assert_eq!(report.restarts, 0);
        assert_eq!(report.last_error, None);
        assert_eq!(report.last_exit_code, None);
        assert_eq!(report.running_since_unix, Some(123));
    }
    #[test]
    fn assignment_watch_uses_exact_keys_or_dot_terminated_prefixes() {
        assert!(assignment_prefix_matches("node.7.task.", "node.7.task.api"));
        assert!(!assignment_prefix_matches(
            "node.7.task.",
            "node.8.task.api"
        ));
        assert!(assignment_prefix_matches("node.7.wg", "node.7.wg"));
        assert!(!assignment_prefix_matches("node.7.wg", "node.7.wg.extra"));
        assert_eq!(assignment_watch_filter("node.7.task."), "node.7.task.>");
        assert_eq!(assignment_watch_filter("node.7.wg"), "node.7.wg");
    }
}
