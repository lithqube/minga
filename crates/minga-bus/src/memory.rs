//! In-memory [`EventBus`] implementation.
//!
//! Used for unit tests and single-node development. All state lives in
//! process memory — nothing survives a restart. The semantics mirror the
//! NATS/JetStream implementation so tests exercise the same code paths.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    subjects, BusError, BusEvent, BusRequest, EventBus, EventStream, KvEvent, KvOp, KvStream,
    LogEntry, LogEventStream, RequestStream, Result, UsageEvent, UsageTotals,
};

/// Default lease TTL: 30 seconds.
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// Channel capacity for events and KV notifications.
const CHANNEL_CAPACITY: usize = 1024;

/// How many log entries the in-memory bus retains per task.
const LOG_HISTORY_CAPACITY: usize = 1000;

// ---------------------------------------------------------------------------
// MemoryBus
// ---------------------------------------------------------------------------

/// In-process event bus. All state is in memory.
pub struct MemoryBus {
    /// Broadcast channel for events.
    events_tx: broadcast::Sender<BusEvent>,
    /// Assignment KV store: key → latest manager-authored value.
    assignments: Mutex<HashMap<String, Vec<u8>>>,
    /// Broadcast channel for assignment KV notifications.
    assignment_tx: broadcast::Sender<KvEvent>,
    /// KV store: key → latest value.
    kv: Mutex<HashMap<String, Vec<u8>>>,
    /// Broadcast channel for KV change notifications.
    kv_tx: broadcast::Sender<KvEvent>,
    /// Liveness leases: (tenant_id, node_id) → refresh and registration payload.
    leases: Mutex<HashMap<(String, u64), Lease>>,
    lease_tx: broadcast::Sender<KvEvent>,
    /// Lease TTL.
    lease_ttl: Duration,
    /// Request handlers: subject → sender for incoming requests.
    request_handlers: Mutex<HashMap<String, mpsc::Sender<BusRequest>>>,
    /// Pending replies: reply_subject → oneshot sender.
    pending_replies: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    /// Counter for generating unique reply subjects.
    reply_counter: AtomicU64,
    /// Broadcast channel for log entries.
    logs_tx: broadcast::Sender<LogEntry>,
    /// Retained log entries per task, oldest first.
    log_history: Mutex<HashMap<String, VecDeque<LogEntry>>>,
    /// Retained usage events, deduplicated by tenant and request ID.
    usage: Mutex<Vec<UsageEvent>>,
    usage_ids: Mutex<HashSet<(String, String)>>,
}
struct Lease {
    refreshed_at: Instant,
    registration: Vec<u8>,
}

impl MemoryBus {
    /// Create a new in-memory bus with the default lease TTL (30s).
    pub fn new() -> Arc<Self> {
        Self::with_lease_ttl(DEFAULT_LEASE_TTL)
    }

    /// Create a new in-memory bus with a custom lease TTL.
    pub fn with_lease_ttl(lease_ttl: Duration) -> Arc<Self> {
        let (events_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (kv_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (assignment_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (lease_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (logs_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Arc::new(Self {
            events_tx,
            assignments: Mutex::new(HashMap::new()),
            assignment_tx,
            kv: Mutex::new(HashMap::new()),
            kv_tx,
            leases: Mutex::new(HashMap::new()),
            lease_tx,
            lease_ttl,
            request_handlers: Mutex::new(HashMap::new()),
            pending_replies: Mutex::new(HashMap::new()),
            reply_counter: AtomicU64::new(0),
            logs_tx,
            log_history: Mutex::new(HashMap::new()),
            usage: Mutex::new(Vec::new()),
            usage_ids: Mutex::new(HashSet::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// EventBus implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl EventBus for MemoryBus {
    async fn publish_event(&self, subject: &str, payload: &[u8]) -> Result<()> {
        let event = BusEvent {
            subject: subject.to_string(),
            payload: payload.to_vec(),
            timestamp: now_millis(),
        };
        // Ignore error if no subscribers — events are ephemeral in MemoryBus.
        let _ = self.events_tx.send(event);
        Ok(())
    }

    async fn subscribe_events(&self, patterns: &[&str]) -> Result<EventStream> {
        let mut rx = self.events_tx.subscribe();
        let (tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let patterns: Vec<String> = patterns.iter().map(|s| s.to_string()).collect();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if patterns.iter().any(|p| subject_matches(p, &event.subject))
                            && tx.send(event).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "event subscriber lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(Box::pin(ReceiverStream { rx: mpsc_rx }))
    }
    async fn put_assignment(&self, key: &str, value: &[u8]) -> Result<()> {
        self.assignments
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_vec());
        let _ = self.assignment_tx.send(KvEvent {
            key: key.to_string(),
            value: Some(value.to_vec()),
            op: KvOp::Put,
        });
        Ok(())
    }

    async fn get_assignment(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.assignments.lock().unwrap().get(key).cloned())
    }

    async fn list_assignments(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(self
            .assignments
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| crate::assignment_prefix_matches(prefix, key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn delete_assignment(&self, key: &str) -> Result<()> {
        let removed = self.assignments.lock().unwrap().remove(key);
        if removed.is_some() {
            let _ = self.assignment_tx.send(KvEvent {
                key: key.to_string(),
                value: None,
                op: KvOp::Delete,
            });
        }
        Ok(())
    }

    async fn watch_assignments(&self, prefix: &str) -> Result<KvStream> {
        let mut rx = self.assignment_tx.subscribe();
        let prefix = prefix.to_string();
        let initial = self
            .assignments
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| crate::assignment_prefix_matches(&prefix, key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let (tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            for (key, value) in initial {
                if tx
                    .send(KvEvent {
                        key,
                        value: Some(value),
                        op: KvOp::Put,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            loop {
                match rx.recv().await {
                    Ok(event) if crate::assignment_prefix_matches(&prefix, &event.key) => {
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Box::pin(ReceiverStream { rx: mpsc_rx }))
    }

    async fn put_status(&self, key: &str, value: &[u8]) -> Result<()> {
        self.kv
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_vec());
        let event = KvEvent {
            key: key.to_string(),
            value: Some(value.to_vec()),
            op: KvOp::Put,
        };
        let _ = self.kv_tx.send(event);
        Ok(())
    }

    async fn get_status(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn list_status(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let kv = self.kv.lock().unwrap();
        Ok(kv.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    async fn watch_status(&self) -> Result<KvStream> {
        let mut rx = self.kv_tx.subscribe();
        let (tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(Box::pin(ReceiverStream { rx: mpsc_rx }))
    }

    async fn refresh_lease(
        &self,
        tenant_id: &str,
        node_id: u64,
        registration: &[u8],
    ) -> Result<()> {
        self.leases.lock().unwrap().insert(
            (tenant_id.to_string(), node_id),
            Lease {
                refreshed_at: Instant::now(),
                registration: registration.to_vec(),
            },
        );
        let event = KvEvent {
            key: subjects::kv_lease(tenant_id, node_id),
            value: Some(registration.to_vec()),
            op: KvOp::Put,
        };
        let _ = self.lease_tx.send(event);
        Ok(())
    }

    async fn is_alive(&self, tenant_id: &str, node_id: u64) -> Result<bool> {
        let leases = self.leases.lock().unwrap();
        Ok(leases
            .get(&(tenant_id.to_string(), node_id))
            .is_some_and(|lease| lease.refreshed_at.elapsed() < self.lease_ttl))
    }

    async fn list_alive_nodes(&self) -> Result<HashSet<u64>> {
        let leases = self.leases.lock().unwrap();
        Ok(leases
            .iter()
            .filter(|(_, lease)| lease.refreshed_at.elapsed() < self.lease_ttl)
            .map(|((_, node_id), _)| *node_id)
            .collect())
    }

    async fn watch_leases(&self) -> Result<KvStream> {
        let mut updates = self.lease_tx.subscribe();
        let current = {
            let leases = self.leases.lock().unwrap();
            leases
                .iter()
                .filter(|(_, lease)| lease.refreshed_at.elapsed() < self.lease_ttl)
                .map(|((tenant_id, node_id), lease)| KvEvent {
                    key: subjects::kv_lease(tenant_id, *node_id),
                    value: Some(lease.registration.clone()),
                    op: KvOp::Put,
                })
                .collect::<Vec<_>>()
        };
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            for event in current {
                if tx.send(event).await.is_err() {
                    return;
                }
            }
            loop {
                match updates.recv().await {
                    Ok(event) => {
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Box::pin(ReceiverStream { rx }))
    }

    async fn request(&self, subject: &str, payload: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        let reply_subject = format!(
            "_INBOX.minga.{}",
            self.reply_counter.fetch_add(1, Ordering::SeqCst)
        );
        let (tx, rx) = oneshot::channel();

        self.pending_replies
            .lock()
            .unwrap()
            .insert(reply_subject.clone(), tx);

        let bus_request = BusRequest {
            reply_subject: reply_subject.clone(),
            payload: payload.to_vec(),
        };

        let sender = {
            let handlers = self.request_handlers.lock().unwrap();
            handlers.get(subject).cloned()
        };

        match sender {
            Some(sender) => {
                sender
                    .send(bus_request)
                    .await
                    .map_err(|_| BusError::NoResponder(subject.to_string()))?;
            }
            None => {
                self.pending_replies.lock().unwrap().remove(&reply_subject);
                return Err(BusError::NoResponder(subject.to_string()));
            }
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(payload)) => Ok(payload),
            Ok(Err(_)) => Err(BusError::Other("reply sender dropped".to_string())),
            Err(_) => {
                self.pending_replies.lock().unwrap().remove(&reply_subject);
                Err(BusError::Timeout(timeout))
            }
        }
    }

    async fn subscribe_requests(&self, subject: &str) -> Result<RequestStream> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        self.request_handlers
            .lock()
            .unwrap()
            .insert(subject.to_string(), tx);
        Ok(Box::pin(ReceiverStream { rx }))
    }

    async fn reply(&self, reply_subject: &str, payload: &[u8]) -> Result<()> {
        let sender = self.pending_replies.lock().unwrap().remove(reply_subject);
        match sender {
            Some(tx) => {
                tx.send(payload.to_vec())
                    .map_err(|_| BusError::Other("reply receiver dropped".to_string()))?;
                Ok(())
            }
            None => Err(BusError::Other(format!(
                "no pending reply for `{reply_subject}`"
            ))),
        }
    }

    async fn publish_usage(&self, event: &UsageEvent) -> Result<()> {
        if !crate::valid_subject_token(&event.tenant_id) || event.request_id.is_empty() {
            return Err(BusError::Other("invalid usage tenant or request ID".into()));
        }
        let key = (event.tenant_id.clone(), event.request_id.clone());
        if !self.usage_ids.lock().unwrap().insert(key) {
            return Ok(());
        }
        minga_telemetry::METRICS.record_agent_usage(
            &event.tenant_id,
            event.input_tokens,
            event.output_tokens,
            event.cost_micros.unwrap_or_default(),
        );
        self.usage.lock().unwrap().push(event.clone());
        Ok(())
    }

    async fn usage_events(
        &self,
        tenant_id: &str,
        since_unix_ms: u64,
        customer: Option<&str>,
        limit: usize,
    ) -> Result<Vec<UsageEvent>> {
        if !crate::valid_subject_token(tenant_id) {
            return Err(BusError::Other("invalid usage tenant".into()));
        }
        Ok(self
            .usage
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                event.tenant_id == tenant_id
                    && event.occurred_at_unix_ms >= since_unix_ms
                    && customer.is_none_or(|customer| event.customer_id == customer)
            })
            .take(limit)
            .cloned()
            .collect())
    }

    async fn usage_totals(&self, since_unix_ms: u64) -> Result<Vec<(String, UsageTotals)>> {
        let events = self.usage.lock().unwrap();
        let mut totals = std::collections::BTreeMap::<String, UsageTotals>::new();
        for event in events.iter().filter(|event| event.occurred_at_unix_ms >= since_unix_ms) {
            let total = totals.entry(event.tenant_id.clone()).or_default();
            total.request_count += 1;
            total.input_tokens += event.input_tokens;
            total.output_tokens += event.output_tokens;
            total.cost_micros += event.cost_micros.unwrap_or_default();
        }
        Ok(totals.into_iter().collect())
    }

    async fn publish_log(&self, task_id: &str, entry: &LogEntry) -> Result<()> {
        if entry.task_id != task_id {
            return Err(BusError::Publish(
                "log subject task ID does not match entry".to_string(),
            ));
        }
        // Retain a bounded history per task so `log_history` can serve the
        // backlog the way the JetStream-backed bus does.
        {
            let mut history = self.log_history.lock().unwrap();
            let entries = history.entry(entry.task_id.clone()).or_default();
            entries.push_back(entry.clone());
            while entries.len() > LOG_HISTORY_CAPACITY {
                entries.pop_front();
            }
        }
        let _ = self.logs_tx.send(entry.clone());
        Ok(())
    }
    async fn publish_node_log(&self, _node_id: u64, task_id: &str, entry: &LogEntry) -> Result<()> {
        self.publish_log(task_id, entry).await
    }

    async fn subscribe_logs(&self, task_id: &str) -> Result<LogEventStream> {
        let mut rx = self.logs_tx.subscribe();
        let task_id = task_id.to_string();
        let (tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(entry) => {
                        if (task_id == "*" || entry.task_id == task_id)
                            && tx.send(entry).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "log subscriber lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(Box::pin(ReceiverStream { rx: mpsc_rx }))
    }

    async fn log_history(&self, task_id: &str, tail: usize) -> Result<Vec<LogEntry>> {
        let history = self.log_history.lock().unwrap();

        // Collect matching entries across tasks when task_id is "*".
        let mut entries: Vec<LogEntry> = Vec::new();
        if task_id == "*" {
            for task_entries in history.values() {
                entries.extend(task_entries.iter().cloned());
            }
        } else if let Some(task_entries) = history.get(task_id) {
            entries.extend(task_entries.iter().cloned());
        }

        // Keep only the most recent `tail` entries (tail == 0 means all).
        if tail > 0 && entries.len() > tail {
            entries.drain(..entries.len() - tail);
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A simple `Stream` wrapper around `mpsc::Receiver`.
struct ReceiverStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Check if a subject matches a NATS-style pattern (`*` = one token, `>` = many).
fn subject_matches(pattern: &str, subject: &str) -> bool {
    let pat: Vec<&str> = pattern.split('.').collect();
    let subj: Vec<&str> = subject.split('.').collect();
    match_tokens(&pat, &subj)
}

fn match_tokens(pat: &[&str], subj: &[&str]) -> bool {
    if pat.is_empty() {
        return subj.is_empty();
    }
    match pat[0] {
        ">" => !subj.is_empty(),
        "*" => !subj.is_empty() && match_tokens(&pat[1..], &subj[1..]),
        literal => !subj.is_empty() && literal == subj[0] && match_tokens(&pat[1..], &subj[1..]),
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subjects;
    use futures::StreamExt;

    // -- Subject matching ----------------------------------------------------

    #[test]
    fn test_subject_matches_exact() {
        assert!(subject_matches(
            "minga.task.abc.status",
            "minga.task.abc.status"
        ));
        assert!(!subject_matches(
            "minga.task.abc.status",
            "minga.task.abc.event"
        ));
    }

    #[test]
    fn test_subject_matches_star() {
        assert!(subject_matches(
            "minga.task.*.status",
            "minga.task.abc.status"
        ));
        assert!(!subject_matches(
            "minga.task.*.status",
            "minga.task.abc.def.status"
        ));
        assert!(!subject_matches("minga.*.status", "minga.task.abc.status"));
    }

    #[test]
    fn test_subject_matches_gt() {
        assert!(subject_matches("minga.task.>", "minga.task.abc.status"));
        assert!(subject_matches("minga.task.>", "minga.task.abc.def.ghi"));
        assert!(!subject_matches("minga.task.>", "minga.node.abc"));
        assert!(!subject_matches("minga.>", "other.abc"));
    }

    #[test]
    fn test_subject_matches_mixed() {
        assert!(subject_matches(
            "minga.*.node.>.status",
            "minga.ctl.node.1.status"
        ));
        assert!(subject_matches(
            "minga.*.node.>.status",
            "minga.ctl.node.1.sub.status"
        ));
        assert!(!subject_matches(
            "minga.*.node.>.status",
            "minga.ctl.task.1.status"
        ));
    }

    // -- Events --------------------------------------------------------------

    #[tokio::test]
    async fn test_publish_and_subscribe_events() {
        let bus = MemoryBus::new();
        let mut stream = bus.subscribe_events(&["minga.task.>"]).await.unwrap();

        bus.publish_event("minga.task.abc.status", b"hello")
            .await
            .unwrap();
        bus.publish_event("minga.node.1.status", b"ignored")
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.subject, "minga.task.abc.status");
        assert_eq!(event.payload, b"hello");
    }

    #[tokio::test]
    async fn test_subscribe_multiple_patterns() {
        let bus = MemoryBus::new();
        let mut stream = bus
            .subscribe_events(&["minga.task.>", "minga.node.>"])
            .await
            .unwrap();

        bus.publish_event("minga.task.abc.event", b"task-event")
            .await
            .unwrap();
        bus.publish_event("minga.node.1.event", b"node-event")
            .await
            .unwrap();
        bus.publish_event("minga.other.thing", b"ignored")
            .await
            .unwrap();

        let e1 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e1.payload, b"task-event");

        let e2 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e2.payload, b"node-event");
    }

    #[tokio::test]
    async fn assignments_are_prefix_scoped_and_deletable() {
        let bus = MemoryBus::new();
        let prefix = "tenant-a.node.7.task.";
        let mut watch = bus.watch_assignments(prefix).await.unwrap();

        bus.put_assignment("tenant-a.node.8.task.other", b"other")
            .await
            .unwrap();
        bus.put_assignment("tenant-a.node.7.task.one", b"assignment")
            .await
            .unwrap();
        bus.put_assignment("tenant-b.node.7.task.one", b"foreign")
            .await
            .unwrap();

        assert_eq!(
            bus.list_assignments(prefix).await.unwrap(),
            vec![(
                "tenant-a.node.7.task.one".to_string(),
                b"assignment".to_vec()
            )]
        );
        let event = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.key, "tenant-a.node.7.task.one");
        assert_eq!(event.value, Some(b"assignment".to_vec()));

        bus.delete_assignment("tenant-a.node.7.task.one")
            .await
            .unwrap();
        assert_eq!(
            bus.get_assignment("tenant-a.node.7.task.one").await.unwrap(),
            None
        );
        let removed = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(removed.op, KvOp::Delete);
    }
    #[tokio::test]
    async fn assignment_watch_includes_existing_values() {
        let bus = MemoryBus::new();
        bus.put_assignment("node.7.wg", b"peer-set").await.unwrap();

        let mut watch = bus.watch_assignments("node.7.wg").await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.key, "node.7.wg");
        assert_eq!(event.value, Some(b"peer-set".to_vec()));
    }

    // -- KV Status -----------------------------------------------------------

    #[tokio::test]
    async fn test_put_and_get_status() {
        let bus = MemoryBus::new();
        assert!(bus.get_status("task.abc").await.unwrap().is_none());

        bus.put_status("task.abc", b"running").await.unwrap();
        assert_eq!(
            bus.get_status("task.abc").await.unwrap(),
            Some(b"running".to_vec())
        );

        // Overwrite
        bus.put_status("task.abc", b"stopped").await.unwrap();
        assert_eq!(
            bus.get_status("task.abc").await.unwrap(),
            Some(b"stopped".to_vec())
        );
    }

    #[tokio::test]
    async fn test_watch_status() {
        let bus = MemoryBus::new();
        let mut watch = bus.watch_status().await.unwrap();

        bus.put_status("task.abc", b"running").await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.key, "task.abc");
        assert_eq!(event.value, Some(b"running".to_vec()));
        assert_eq!(event.op, KvOp::Put);
    }

    #[tokio::test]
    async fn test_watch_status_multiple_keys() {
        let bus = MemoryBus::new();
        let mut watch = bus.watch_status().await.unwrap();

        bus.put_status("task.a", b"v1").await.unwrap();
        bus.put_status("task.b", b"v2").await.unwrap();

        let e1 = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e1.key, "task.a");

        let e2 = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e2.key, "task.b");
    }

    // -- Liveness ------------------------------------------------------------

    #[tokio::test]
    async fn test_lease_alive_and_expired() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(100));

        // No lease yet.
        assert!(!bus.is_alive("tenant-a", 1).await.unwrap());

        // Refresh → alive.
        bus.refresh_lease("tenant-a", 1, b"registration")
            .await
            .unwrap();
        assert!(bus.is_alive("tenant-a", 1).await.unwrap());

        // Wait for expiry.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!bus.is_alive("tenant-a", 1).await.unwrap());
    }

    #[tokio::test]
    async fn test_lease_refresh_extends() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(200));

        bus.refresh_lease("tenant-a", 1, b"registration")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        bus.refresh_lease("tenant-a", 1, b"registration")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(bus.is_alive("tenant-a", 1).await.unwrap());
    }

    #[tokio::test]
    async fn test_lease_per_node() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(100));

        bus.refresh_lease("tenant-a", 1, b"registration")
            .await
            .unwrap();
        assert!(bus.is_alive("tenant-a", 1).await.unwrap());
        assert!(!bus.is_alive("tenant-a", 2).await.unwrap());
    }

    #[tokio::test]
    async fn leases_are_isolated_by_tenant() {
        let bus = MemoryBus::new();
        bus.refresh_lease("tenant-a", 9, b"a").await.unwrap();
        bus.refresh_lease("tenant-b", 9, b"b").await.unwrap();
        assert!(bus.is_alive("tenant-a", 9).await.unwrap());
        assert!(bus.is_alive("tenant-b", 9).await.unwrap());
        assert_eq!(bus.list_alive_nodes().await.unwrap(), HashSet::from([9]));
        let mut leases = bus.watch_leases().await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(1), leases.next())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), leases.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            HashSet::from([first.key, second.key]),
            HashSet::from(["tenant-a.node.9".to_string(), "tenant-b.node.9".to_string()])
        );
    }

    // -- Request / Reply -----------------------------------------------------

    #[tokio::test]
    async fn test_request_reply() {
        let bus = MemoryBus::new();
        let mut requests = bus
            .subscribe_requests(&subjects::ctl_ping("default", 1))
            .await
            .unwrap();

        // Spawn a responder.
        let bus_clone = bus.clone();
        tokio::spawn(async move {
            let req = requests.next().await.unwrap();
            assert_eq!(req.payload, b"ping");
            bus_clone.reply(&req.reply_subject, b"pong").await.unwrap();
        });

        let reply = bus
            .request(
                &subjects::ctl_ping("default", 1),
                b"ping",
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(reply, b"pong");
    }

    #[tokio::test]
    async fn test_request_no_responder() {
        let bus = MemoryBus::new();
        let result = bus
            .request(
                "minga.ctl.node.99.ping",
                b"ping",
                Duration::from_millis(100),
            )
            .await;
        assert!(matches!(result, Err(BusError::NoResponder(_))));
    }

    #[tokio::test]
    async fn test_request_timeout() {
        let bus = MemoryBus::new();
        // Subscribe but never reply.
        let _requests = bus
            .subscribe_requests("minga.ctl.node.1.drain")
            .await
            .unwrap();

        let result = bus
            .request(
                "minga.ctl.node.1.drain",
                b"drain",
                Duration::from_millis(100),
            )
            .await;
        assert!(matches!(result, Err(BusError::Timeout(_))));
    }

    // -- Logs ----------------------------------------------------------------

    #[tokio::test]
    async fn test_publish_and_subscribe_logs() {
        let bus = MemoryBus::new();
        let mut stream = bus.subscribe_logs("task-1").await.unwrap();

        let entry = LogEntry::stdout("task-1", b"hello world\n");
        bus.publish_log("task-1", &entry).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.task_id, "task-1");
        assert_eq!(received.data, b"hello world\n");
        assert_eq!(received.stream, crate::LogStream::Stdout);
    }

    #[tokio::test]
    async fn test_subscribe_logs_filters_by_task() {
        let bus = MemoryBus::new();
        let mut stream = bus.subscribe_logs("task-1").await.unwrap();

        bus.publish_log("task-1", &LogEntry::stdout("task-1", b"for task-1"))
            .await
            .unwrap();
        bus.publish_log("task-2", &LogEntry::stdout("task-2", b"for task-2"))
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.task_id, "task-1");
    }

    #[tokio::test]
    async fn test_subscribe_logs_wildcard() {
        let bus = MemoryBus::new();
        let mut stream = bus.subscribe_logs("*").await.unwrap();

        bus.publish_log("task-1", &LogEntry::stdout("task-1", b"a"))
            .await
            .unwrap();
        bus.publish_log("task-2", &LogEntry::stderr("task-2", b"b"))
            .await
            .unwrap();

        let e1 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e1.task_id, "task-1");

        let e2 = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e2.task_id, "task-2");
        assert_eq!(e2.stream, crate::LogStream::Stderr);
    }

    #[tokio::test]
    async fn test_log_entry_roundtrip() {
        let entry = LogEntry::stderr("task-abc", b"error: something broke\n");
        let bytes = entry.to_bytes().unwrap();
        let decoded = LogEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[tokio::test]
    async fn test_log_history_returns_retained_entries() {
        let bus = MemoryBus::new();

        bus.publish_log("task-1", &LogEntry::stdout("task-1", b"one\n"))
            .await
            .unwrap();
        bus.publish_log("task-2", &LogEntry::stdout("task-2", b"other\n"))
            .await
            .unwrap();

        let history = bus.log_history("task-1", 0).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].data, b"one\n");
    }

    #[tokio::test]
    async fn test_log_history_tail_keeps_the_last_entries() {
        let bus = MemoryBus::new();

        for i in 0..5 {
            bus.publish_log(
                "task-1",
                &LogEntry::stdout("task-1", format!("line {i}\n").as_bytes()),
            )
            .await
            .unwrap();
        }

        let history = bus.log_history("task-1", 2).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].data, b"line 3\n");
        assert_eq!(history[1].data, b"line 4\n");
    }

    #[tokio::test]
    async fn test_log_history_wildcard_covers_all_tasks() {
        let bus = MemoryBus::new();

        bus.publish_log("task-1", &LogEntry::stdout("task-1", b"a\n"))
            .await
            .unwrap();
        bus.publish_log("task-2", &LogEntry::stdout("task-2", b"b\n"))
            .await
            .unwrap();

        let history = bus.log_history("*", 0).await.unwrap();
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn test_list_status_returns_all_entries() {
        let bus = MemoryBus::new();

        bus.put_status("task.a", b"one").await.unwrap();
        bus.put_status("task.b", b"two").await.unwrap();

        let entries = bus.list_status().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(k, v)| k == "task.a" && v == b"one"));
        assert!(entries.iter().any(|(k, v)| k == "task.b" && v == b"two"));
    }

    fn usage_event(request_id: &str, tenant_id: &str, occurred_at_unix_ms: u64) -> UsageEvent {
        UsageEvent {
            request_id: request_id.into(),
            tenant_id: tenant_id.into(),
            service_id: "svc".into(),
            task_id: "task".into(),
            customer_id: "customer".into(),
            route: "/v1/chat".into(),
            requested_model: "requested".into(),
            answered_model: "answered".into(),
            input_tokens: 11,
            output_tokens: 7,
            cache_hit: false,
            latency_ms: 42,
            status: 200,
            occurred_at_unix_ms,
            cost_micros: Some(9),
        }
    }

    #[tokio::test]
    async fn usage_query_is_tenant_and_time_scoped_and_deduplicated() {
        let bus = MemoryBus::new();
        let event = usage_event("req-1", "tenant-a", 100);
        bus.publish_usage(&event).await.unwrap();
        bus.publish_usage(&event).await.unwrap();
        bus.publish_usage(&usage_event("req-2", "tenant-a", 50))
            .await
            .unwrap();
        let mut other_customer = usage_event("req-3", "tenant-a", 100);
        other_customer.customer_id = "other".into();
        bus.publish_usage(&other_customer).await.unwrap();

        let rows = bus
            .usage_events("tenant-a", 100, Some("customer"), 1)
            .await
            .unwrap();
        let totals = bus.usage_totals(100).await.unwrap();
        assert_eq!(totals.len(), 2);
        assert_eq!(totals[0].0, "tenant-a");
        assert_eq!(totals[0].1.request_count, 1);
    }

    #[test]
    fn usage_event_contract_serializes_integer_cost_without_prompt_fields() {
        let event = usage_event("req-1", "tenant-a", 123);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["cost_micros"], 9);
        assert_eq!(json["input_tokens"], 11);
        assert!(json.get("prompt").is_none());
        assert!(serde_json::from_value::<UsageEvent>(json).is_ok());
    }
}
