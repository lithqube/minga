//! NATS/JetStream [`EventBus`] implementation.
//!
//! Production transport. Connects to a NATS server (sidecar or external),
//! creates streams and KV buckets idempotently, and implements the full
//! [`EventBus`] trait over JetStream streams, KV buckets, and core NATS
//! request/reply.
//!
//! # Streams
//!
//! | Stream | Subjects | Retention | Purpose |
//! |---|---|---|---|
//! | `MINGA_EVENTS` | `minga.node.>`, `minga.task.>`, `minga.egress.>` | Limits (72h) | Status reports, lifecycle events |
//! | `MINGA_COMMANDS` | `minga.ctl.node.>` | WorkQueue (24h) | Task assign/stop commands |
//! | `MINGA_LOGS` | `minga.log.>` | Limits (512 MiB, 24h) | Container log chunks |
//!
//! # KV buckets
//!
//! | Bucket | Keys | Purpose |
//! |---|---|---|
//! | `MINGA_STATUS` | `task.{id}`, `node.{id}` | Latest actual state |
//! | `MINGA_LEASES` | `lease.node.{id}` | Liveness (timestamp-based) |

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use async_nats::jetstream::kv::{Config as KvConfig, Operation as KvOperation};
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use async_nats::jetstream::{self, Context as JetStream};
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use crate::{
    subjects, BusError, BusEvent, BusRequest, Command, EventBus, EventStream, KvEvent, KvOp,
    KvStream, LogEntry, LogEventStream, RequestStream, Result,
};

/// Default lease TTL: 15 seconds.
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// NatsBus
// ---------------------------------------------------------------------------

/// NATS/JetStream event bus.
pub struct NatsBus {
    client: Client,
    js: JetStream,
    status_kv: jetstream::kv::Store,
    leases_kv: jetstream::kv::Store,
    lease_ttl: Duration,
    /// Cached command consumers per node.
    consumers: Mutex<HashMap<u64, jetstream::consumer::PullConsumer>>,
}

impl NatsBus {
    /// The underlying JetStream context.
    ///
    /// Exposed for integration tests and for callers that need direct
    /// JetStream access (stream inspection, admin operations).
    pub fn jetstream(&self) -> &JetStream {
        &self.js
    }

    /// Connect to a NATS server and set up streams/KV buckets.
    pub async fn connect(url: &str) -> Result<Arc<Self>> {
        Self::connect_with_ttl(url, DEFAULT_LEASE_TTL).await
    }

    /// Connect with a custom lease TTL.
    pub async fn connect_with_ttl(url: &str, lease_ttl: Duration) -> Result<Arc<Self>> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;
        let js = jetstream::new(client.clone());

        Self::setup_streams(&js).await?;
        let status_kv = Self::setup_kv(&js, "MINGA_STATUS").await?;
        let leases_kv = Self::setup_kv(&js, "MINGA_LEASES").await?;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            leases_kv,
            lease_ttl,
            consumers: Mutex::new(HashMap::new()),
        }))
    }

    async fn setup_streams(js: &JetStream) -> Result<()> {
        // MINGA_EVENTS — status reports and lifecycle events.
        let events = StreamConfig {
            name: "MINGA_EVENTS".to_string(),
            subjects: vec![
                "minga.node.>".to_string(),
                "minga.task.>".to_string(),
                "minga.egress.>".to_string(),
            ],
            retention: RetentionPolicy::Limits,
            max_age: Duration::from_secs(72 * 3600),
            ..Default::default()
        };
        js.get_or_create_stream(events)
            .await
            .map_err(|e| BusError::Other(format!("MINGA_EVENTS: {e}")))?;

        // MINGA_COMMANDS — work queue for task assign/stop.
        let commands = StreamConfig {
            name: "MINGA_COMMANDS".to_string(),
            subjects: vec!["minga.ctl.node.>".to_string()],
            retention: RetentionPolicy::WorkQueue,
            max_age: Duration::from_secs(24 * 3600),
            ..Default::default()
        };
        js.get_or_create_stream(commands)
            .await
            .map_err(|e| BusError::Other(format!("MINGA_COMMANDS: {e}")))?;

        // MINGA_LOGS — container log chunks.
        // Note: log subjects live under `minga.log.>`, not `minga.task.>`.
        // JetStream rejects streams with overlapping subject filters, and
        // MINGA_EVENTS already claims `minga.task.>`.
        let logs = StreamConfig {
            name: "MINGA_LOGS".to_string(),
            subjects: vec![subjects::log_events_wildcard()],
            retention: RetentionPolicy::Limits,
            max_bytes: 512 * 1024 * 1024,
            max_age: Duration::from_secs(24 * 3600),
            ..Default::default()
        };
        js.get_or_create_stream(logs)
            .await
            .map_err(|e| BusError::Other(format!("MINGA_LOGS: {e}")))?;

        Ok(())
    }

    async fn setup_kv(js: &JetStream, bucket: &str) -> Result<jetstream::kv::Store> {
        let config = KvConfig {
            bucket: bucket.to_string(),
            history: 1,
            ..Default::default()
        };
        js.create_or_update_key_value(config)
            .await
            .map_err(|e| BusError::Kv(format!("{bucket}: {e}")))
    }

    async fn get_command_consumer(
        &self,
        node_id: u64,
    ) -> Result<jetstream::consumer::PullConsumer> {
        // Check cache first.
        {
            let consumers = self.consumers.lock().unwrap();
            if let Some(c) = consumers.get(&node_id) {
                return Ok(c.clone());
            }
        }

        let stream = self
            .js
            .get_stream("MINGA_COMMANDS")
            .await
            .map_err(|e| BusError::Command(format!("get stream: {e}")))?;

        let consumer_name = format!("node-{node_id}");
        let config = PullConsumerConfig {
            durable_name: Some(consumer_name.clone()),
            filter_subject: subjects::ctl_node_wildcard(node_id),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        };

        let consumer = stream
            .get_or_create_consumer(&consumer_name, config)
            .await
            .map_err(|e| BusError::Command(format!("get consumer: {e}")))?;

        self.consumers
            .lock()
            .unwrap()
            .insert(node_id, consumer.clone());

        Ok(consumer)
    }
}

// ---------------------------------------------------------------------------
// EventBus implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl EventBus for NatsBus {
    async fn publish_event(&self, subject: &str, payload: &[u8]) -> Result<()> {
        self.js
            .publish(subject.to_string(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| BusError::Publish(e.to_string()))?;
        Ok(())
    }

    async fn subscribe_events(&self, patterns: &[&str]) -> Result<EventStream> {
        let stream = self
            .js
            .get_stream("MINGA_EVENTS")
            .await
            .map_err(|e| BusError::Subscribe(format!("get stream: {e}")))?;

        let filter_subjects: Vec<String> = patterns.iter().map(|s| s.to_string()).collect();
        // Pull consumers need no delivery subject, unlike push consumers.
        let config = PullConsumerConfig {
            filter_subjects,
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        };

        let consumer = stream
            .create_consumer(config)
            .await
            .map_err(|e| BusError::Subscribe(format!("create consumer: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| BusError::Subscribe(format!("messages: {e}")))?;

        let event_stream = messages.filter_map(|msg| async move {
            match msg {
                Ok(m) => {
                    let _ = m.ack().await;
                    Some(BusEvent {
                        subject: m.subject.to_string(),
                        payload: m.payload.to_vec(),
                        timestamp: now_millis(),
                    })
                }
                Err(e) => {
                    tracing::warn!(error = %e, "event stream error");
                    None
                }
            }
        });

        Ok(Box::pin(event_stream))
    }

    async fn send_command(&self, node_id: u64, cmd: &Command) -> Result<()> {
        let subject = match cmd {
            Command::Assign { .. } => subjects::ctl_assign(node_id),
            Command::Stop { .. } => subjects::ctl_stop(node_id),
        };
        let payload = cmd.to_bytes()?;
        self.js
            .publish(subject, Bytes::from(payload))
            .await
            .map_err(|e| BusError::Command(format!("publish: {e}")))?;
        Ok(())
    }

    async fn next_command(&self, node_id: u64) -> Result<Command> {
        let consumer = self.get_command_consumer(node_id).await?;

        // Fetch one message with a 30s timeout.
        let mut batch = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(30))
            .messages()
            .await
            .map_err(|e| BusError::Command(format!("fetch: {e}")))?;

        let msg = batch
            .next()
            .await
            .ok_or_else(|| BusError::Command("fetch returned no messages".to_string()))?
            .map_err(|e| BusError::Command(format!("message error: {e}")))?;

        let cmd = Command::from_bytes(&msg.payload)?;
        msg.ack()
            .await
            .map_err(|e| BusError::Command(format!("ack: {e}")))?;

        Ok(cmd)
    }

    async fn put_status(&self, key: &str, value: &[u8]) -> Result<()> {
        self.status_kv
            .put(key.to_string(), Bytes::copy_from_slice(value))
            .await
            .map_err(|e| BusError::Kv(format!("put {key}: {e}")))?;
        Ok(())
    }

    async fn get_status(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self
            .status_kv
            .get(key.to_string())
            .await
            .map_err(|e| BusError::Kv(format!("get {key}: {e}")))?
        {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }

    async fn list_status(&self) -> Result<Vec<(String, Vec<u8>)>> {
        use futures::StreamExt;

        let mut keys = self
            .status_kv
            .keys()
            .await
            .map_err(|e| BusError::Kv(format!("keys: {e}")))?;

        let mut entries = Vec::new();
        while let Some(key) = keys.next().await {
            let key = key.map_err(|e| BusError::Kv(format!("keys: {e}")))?;
            if let Some(bytes) = self
                .status_kv
                .get(key.clone())
                .await
                .map_err(|e| BusError::Kv(format!("get {key}: {e}")))?
            {
                entries.push((key, bytes.to_vec()));
            }
        }

        Ok(entries)
    }

    async fn watch_status(&self) -> Result<KvStream> {
        // `watch_all()` uses DeliverPolicy::New — it only yields *future*
        // writes and never replays the current bucket contents. A status
        // watcher must see existing task statuses too, so start from the
        // beginning of the bucket history (history=1, so this is one entry
        // per key) and continue streaming updates.
        let watcher = self
            .status_kv
            .watch_all_from_revision(1)
            .await
            .map_err(|e| BusError::Kv(format!("watch: {e}")))?;

        let kv_stream = watcher.filter_map(|entry_result| async move {
            match entry_result {
                Ok(entry) => {
                    let op = match entry.operation {
                        KvOperation::Put => KvOp::Put,
                        KvOperation::Delete | KvOperation::Purge => KvOp::Delete,
                    };
                    let value = if entry.operation == KvOperation::Put {
                        Some(entry.value.to_vec())
                    } else {
                        None
                    };
                    Some(KvEvent {
                        key: entry.key,
                        value,
                        op,
                    })
                }
                Err(e) => {
                    tracing::warn!(error = %e, "kv watch error");
                    None
                }
            }
        });

        Ok(Box::pin(kv_stream))
    }

    async fn refresh_lease(&self, node_id: u64) -> Result<()> {
        let key = subjects::kv_lease(node_id);
        let value = now_millis().to_le_bytes().to_vec();
        self.leases_kv
            .put(key, Bytes::from(value))
            .await
            .map_err(|e| BusError::Lease(format!("refresh {node_id}: {e}")))?;
        Ok(())
    }

    async fn is_alive(&self, node_id: u64) -> Result<bool> {
        let key = subjects::kv_lease(node_id);
        match self
            .leases_kv
            .get(key)
            .await
            .map_err(|e| BusError::Lease(format!("get {node_id}: {e}")))?
        {
            Some(bytes) => {
                let arr: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| BusError::Lease("invalid lease value".to_string()))?;
                let last_refresh = u64::from_le_bytes(arr);
                let age_ms = now_millis().saturating_sub(last_refresh);
                Ok(Duration::from_millis(age_ms) < self.lease_ttl)
            }
            None => Ok(false),
        }
    }

    async fn request(&self, subject: &str, payload: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
        // async-nats handles timeout internally via its own configuration.
        let msg = self
            .client
            .request(subject.to_string(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| BusError::Connection(format!("request: {e}")))?;
        Ok(msg.payload.to_vec())
    }

    async fn subscribe_requests(&self, subject: &str) -> Result<RequestStream> {
        let subscriber = self
            .client
            .subscribe(subject.to_string())
            .await
            .map_err(|e| BusError::Subscribe(format!("subscribe: {e}")))?;

        let request_stream = subscriber.map(|msg| BusRequest {
            reply_subject: msg.reply.map(|s| s.to_string()).unwrap_or_default(),
            payload: msg.payload.to_vec(),
        });

        Ok(Box::pin(request_stream))
    }

    async fn reply(&self, reply_subject: &str, payload: &[u8]) -> Result<()> {
        self.client
            .publish(reply_subject.to_string(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| BusError::Publish(format!("reply: {e}")))?;
        Ok(())
    }

    async fn publish_log(&self, task_id: &str, entry: &LogEntry) -> Result<()> {
        let subject = subjects::task_log(task_id);
        let payload = entry.to_bytes()?;
        self.js
            .publish(subject, Bytes::from(payload))
            .await
            .map_err(|e| BusError::Publish(format!("log: {e}")))?;
        Ok(())
    }

    async fn subscribe_logs(&self, task_id: &str) -> Result<LogEventStream> {
        let stream = self
            .js
            .get_stream("MINGA_LOGS")
            .await
            .map_err(|e| BusError::Subscribe(format!("get MINGA_LOGS: {e}")))?;

        let filter = if task_id == "*" {
            subjects::log_events_wildcard()
        } else {
            subjects::task_log(task_id)
        };

        // Pull consumers need no delivery subject, unlike push consumers.
        // DeliverPolicy::New means this subscription only sees entries
        // published from now on — the retained backlog is read separately
        // via `log_history`.
        let config = PullConsumerConfig {
            filter_subjects: vec![filter],
            ack_policy: AckPolicy::Explicit,
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        };

        let consumer = stream
            .create_consumer(config)
            .await
            .map_err(|e| BusError::Subscribe(format!("create log consumer: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| BusError::Subscribe(format!("log messages: {e}")))?;

        let log_stream = messages.filter_map(|msg| async move {
            match msg {
                Ok(m) => {
                    let _ = m.ack().await;
                    match LogEntry::from_bytes(&m.payload) {
                        Ok(entry) => Some(entry),
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to deserialize log entry");
                            None
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "log stream error");
                    None
                }
            }
        });

        Ok(Box::pin(log_stream))
    }

    async fn log_history(&self, task_id: &str, tail: usize) -> Result<Vec<LogEntry>> {
        let stream = self
            .js
            .get_stream("MINGA_LOGS")
            .await
            .map_err(|e| BusError::Subscribe(format!("get MINGA_LOGS: {e}")))?;

        let filter = if task_id == "*" {
            subjects::log_events_wildcard()
        } else {
            subjects::task_log(task_id)
        };

        // Read the retained backlog with a short-lived pull consumer that
        // starts at the beginning of the stream, then keep only the last
        // `tail` entries. `tail == 0` keeps everything retained.
        let config = PullConsumerConfig {
            filter_subjects: vec![filter],
            ack_policy: AckPolicy::None,
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        };

        let mut consumer = stream
            .create_consumer(config)
            .await
            .map_err(|e| BusError::Subscribe(format!("create log consumer: {e}")))?;

        // The consumer's pending count tells us how many entries are
        // retained for this filter — that is how far back we need to read.
        let retained = consumer
            .info()
            .await
            .map_err(|e| BusError::Subscribe(format!("log consumer info: {e}")))?
            .num_pending as usize;
        let wanted = if tail == 0 {
            retained
        } else {
            tail.min(retained)
        };
        let mut entries: VecDeque<LogEntry> = VecDeque::with_capacity(wanted);

        // Fetch every retained entry in bounded batches. A single fetch is
        // capped at 200 and would otherwise return the *first* 200 entries.
        let mut remaining = retained;
        while remaining > 0 {
            let mut batch = consumer
                .fetch()
                .max_messages(remaining.min(200))
                .expires(Duration::from_secs(5))
                .messages()
                .await
                .map_err(|e| BusError::Subscribe(format!("fetch log history: {e}")))?;
            let mut received = 0;
            while let Some(msg) = batch.next().await {
                let msg = msg.map_err(|e| BusError::Subscribe(format!("log history: {e}")))?;
                received += 1;
                if let Ok(entry) = LogEntry::from_bytes(&msg.payload) {
                    if wanted > 0 && entries.len() == wanted {
                        entries.pop_front();
                    }
                    entries.push_back(entry);
                }
            }
            if received == 0 {
                return Err(BusError::Subscribe(format!(
                    "log history incomplete: {remaining} retained entries not received"
                )));
            }
            remaining -= received;
        }

        Ok(entries.into_iter().collect())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
