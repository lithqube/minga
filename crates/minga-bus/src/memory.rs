//! In-memory [`EventBus`] implementation.
//!
//! Used for unit tests and single-node development. All state lives in
//! process memory — nothing survives a restart. The semantics mirror the
//! NATS/JetStream implementation so tests exercise the same code paths.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    subjects, BusError, BusEvent, BusRequest, Command, EventBus, EventStream, KvEvent, KvOp,
    KvStream, RequestStream, Result,
};

/// Default lease TTL: 15 seconds (3 × 5s heartbeat interval).
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(15);

/// Channel capacity for events, commands, and KV notifications.
const CHANNEL_CAPACITY: usize = 1024;

// ---------------------------------------------------------------------------
// MemoryBus
// ---------------------------------------------------------------------------

/// In-process event bus. All state is in memory.
pub struct MemoryBus {
    /// Broadcast channel for events.
    events_tx: broadcast::Sender<BusEvent>,
    /// Per-node command channels.
    commands: Mutex<HashMap<u64, Arc<CommandChannel>>>,
    /// KV store: key → latest value.
    kv: Mutex<HashMap<String, Vec<u8>>>,
    /// Broadcast channel for KV change notifications.
    kv_tx: broadcast::Sender<KvEvent>,
    /// Liveness leases: node_id → last refresh time.
    leases: Mutex<HashMap<u64, Instant>>,
    /// Lease TTL.
    lease_ttl: Duration,
    /// Request handlers: subject → sender for incoming requests.
    request_handlers: Mutex<HashMap<String, mpsc::Sender<BusRequest>>>,
    /// Pending replies: reply_subject → oneshot sender.
    pending_replies: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    /// Counter for generating unique reply subjects.
    reply_counter: AtomicU64,
}

struct CommandChannel {
    tx: mpsc::Sender<Command>,
    rx: tokio::sync::Mutex<mpsc::Receiver<Command>>,
}

impl MemoryBus {
    /// Create a new in-memory bus with the default lease TTL (15s).
    pub fn new() -> Arc<Self> {
        Self::with_lease_ttl(DEFAULT_LEASE_TTL)
    }

    /// Create a new in-memory bus with a custom lease TTL.
    pub fn with_lease_ttl(lease_ttl: Duration) -> Arc<Self> {
        let (events_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (kv_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Arc::new(Self {
            events_tx,
            commands: Mutex::new(HashMap::new()),
            kv: Mutex::new(HashMap::new()),
            kv_tx,
            leases: Mutex::new(HashMap::new()),
            lease_ttl,
            request_handlers: Mutex::new(HashMap::new()),
            pending_replies: Mutex::new(HashMap::new()),
            reply_counter: AtomicU64::new(0),
        })
    }

    fn get_or_create_channel(&self, node_id: u64) -> Arc<CommandChannel> {
        let mut commands = self.commands.lock().unwrap();
        Arc::clone(commands.entry(node_id).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            Arc::new(CommandChannel {
                tx,
                rx: tokio::sync::Mutex::new(rx),
            })
        }))
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

    async fn send_command(&self, node_id: u64, cmd: &Command) -> Result<()> {
        let channel = self.get_or_create_channel(node_id);
        channel
            .tx
            .send(cmd.clone())
            .await
            .map_err(|_| BusError::Command(format!("node {node_id} command channel closed")))?;
        Ok(())
    }

    async fn next_command(&self, node_id: u64) -> Result<Command> {
        let channel = self.get_or_create_channel(node_id);
        let mut rx = channel.rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| BusError::Command(format!("node {node_id} command channel closed")))
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

    async fn refresh_lease(&self, node_id: u64) -> Result<()> {
        self.leases.lock().unwrap().insert(node_id, Instant::now());
        // Also publish a KV event so watchers see the heartbeat.
        let event = KvEvent {
            key: subjects::kv_lease(node_id),
            value: Some(now_millis().to_le_bytes().to_vec()),
            op: KvOp::Put,
        };
        let _ = self.kv_tx.send(event);
        Ok(())
    }

    async fn is_alive(&self, node_id: u64) -> Result<bool> {
        let leases = self.leases.lock().unwrap();
        match leases.get(&node_id) {
            Some(last_refresh) => Ok(last_refresh.elapsed() < self.lease_ttl),
            None => Ok(false),
        }
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

    // -- Commands ------------------------------------------------------------

    #[tokio::test]
    async fn test_send_and_receive_command() {
        let bus = MemoryBus::new();
        let cmd = Command::Assign {
            task_id: "task-1".to_string(),
            payload: vec![1, 2, 3],
        };

        bus.send_command(1, &cmd).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), bus.next_command(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, cmd);
    }

    #[tokio::test]
    async fn test_commands_are_per_node() {
        let bus = MemoryBus::new();
        let cmd1 = Command::Stop {
            task_id: "task-a".to_string(),
        };
        let cmd2 = Command::Stop {
            task_id: "task-b".to_string(),
        };

        bus.send_command(1, &cmd1).await.unwrap();
        bus.send_command(2, &cmd2).await.unwrap();

        let r1 = tokio::time::timeout(Duration::from_secs(1), bus.next_command(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1, cmd1);

        let r2 = tokio::time::timeout(Duration::from_secs(1), bus.next_command(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2, cmd2);
    }

    #[tokio::test]
    async fn test_command_store_and_forward() {
        // Command sent before receiver calls next_command should be buffered.
        let bus = MemoryBus::new();
        let cmd = Command::Assign {
            task_id: "early".to_string(),
            payload: vec![],
        };
        bus.send_command(1, &cmd).await.unwrap();

        // Wait a bit, then receive.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let received = tokio::time::timeout(Duration::from_secs(1), bus.next_command(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, cmd);
    }

    #[tokio::test]
    async fn test_multiple_commands_fifo() {
        let bus = MemoryBus::new();
        for i in 0..3 {
            bus.send_command(
                1,
                &Command::Stop {
                    task_id: format!("t{i}"),
                },
            )
            .await
            .unwrap();
        }
        for i in 0..3 {
            let cmd = tokio::time::timeout(Duration::from_secs(1), bus.next_command(1))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                cmd,
                Command::Stop {
                    task_id: format!("t{i}")
                }
            );
        }
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
        assert!(!bus.is_alive(1).await.unwrap());

        // Refresh → alive.
        bus.refresh_lease(1).await.unwrap();
        assert!(bus.is_alive(1).await.unwrap());

        // Wait for expiry.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!bus.is_alive(1).await.unwrap());
    }

    #[tokio::test]
    async fn test_lease_refresh_extends() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(200));

        bus.refresh_lease(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        bus.refresh_lease(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        // 250ms since first refresh, but only 150ms since second.
        assert!(bus.is_alive(1).await.unwrap());
    }

    #[tokio::test]
    async fn test_lease_per_node() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(100));

        bus.refresh_lease(1).await.unwrap();
        assert!(bus.is_alive(1).await.unwrap());
        assert!(!bus.is_alive(2).await.unwrap());
    }

    // -- Request / Reply -----------------------------------------------------

    #[tokio::test]
    async fn test_request_reply() {
        let bus = MemoryBus::new();
        let mut requests = bus
            .subscribe_requests(&subjects::ctl_ping(1))
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
            .request(&subjects::ctl_ping(1), b"ping", Duration::from_secs(1))
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
}
