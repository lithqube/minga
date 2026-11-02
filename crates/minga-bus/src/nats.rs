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
//! | `MINGA_DEAD_LETTERS` | `$JS.EVENT.ADVISORY.CONSUMER.{MAX_DELIVERIES,MSG_TERMINATED}.>` | Limits (64 MiB, 7d) | Record of failed deliveries |
//!
//! # Failed deliveries
//!
//! JetStream has no dead-letter queue. Every explicit-ack consumer is
//! bounded to [`MAX_DELIVER`] deliveries with [`REDELIVERY_BACKOFF`]; after
//! that, or after a `+TERM`, the server stops redelivering and publishes an
//! advisory, which `MINGA_DEAD_LETTERS` keeps. The advisory names the stream
//! sequence, not the payload: the body is recoverable only while the source
//! stream still retains it, and the work-queue `MINGA_COMMANDS` drops a
//! terminated message at once.
//!
//! # KV buckets
//!
//! | Bucket | Keys | Purpose |
//! |---|---|---|
//! | `MINGA_STATUS` | `task.{id}`, `node.{id}` | Latest actual state |
//! | `MINGA_LEASES` | `lease.node.{id}` | Liveness (timestamp-based) |
//!
//! # High availability
//!
//! Every constructor takes a comma-separated seed list
//! (`tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222`) and connects
//! only to those servers, ignoring the ones a server advertises, so a TLS
//! connection never reaches a host the operator did not name. Streams and
//! buckets are created with one replica; [`NatsBus::ensure_replicas`]
//! raises the ones in [`REPLICATED`] on a NATS cluster.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use async_nats::jetstream::kv::{Config as KvConfig, Operation as KvOperation};
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use async_nats::jetstream::AckKind;
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

/// Deliveries before JetStream stops redelivering a message and publishes a
/// `MAX_DELIVERIES` advisory.
const MAX_DELIVER: i64 = 5;

/// Redelivery delays after a missed ack. JetStream uses the first entry as
/// the ack wait and requires fewer entries than [`MAX_DELIVER`].
const REDELIVERY_BACKOFF: [Duration; 4] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(300),
];

/// Redelivery delay for a message nak'd after a transient failure.
const NAK_DELAY: Duration = Duration::from_secs(5);

/// Stream that keeps the advisories JetStream publishes for failed deliveries.
const DEAD_LETTER_STREAM: &str = "MINGA_DEAD_LETTERS";

/// Streams (KV buckets are `KV_`-prefixed streams) that losing one NATS
/// server must not lose. `MINGA_LOGS` stays at one replica: it is the
/// largest stream, best effort, and edge disks are small.
const REPLICATED: [&str; 5] = [
    "MINGA_EVENTS",
    "MINGA_COMMANDS",
    DEAD_LETTER_STREAM,
    "KV_MINGA_STATUS",
    "KV_MINGA_LEASES",
];

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

    /// Raises the [`REPLICATED`] streams and buckets to `replicas` copies.
    ///
    /// Only ever scales up: every node runs this on connect, and a node
    /// left at the default of 1 must not shrink what a manager raised.
    /// `replicas` must not exceed the JetStream servers in the cluster;
    /// the server rejects the update otherwise.
    ///
    /// A freshly formed cluster answers "no suitable peers" until every
    /// server has joined JetStream, so this retries for up to 10 seconds,
    /// like stream setup does.
    pub async fn ensure_replicas(&self, replicas: usize) -> Result<()> {
        const MAX_ATTEMPTS: u32 = 20; // 20 × 500ms = 10s
        for attempt in 1..=MAX_ATTEMPTS {
            match self.raise_replicas(replicas).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < MAX_ATTEMPTS => {
                    tracing::debug!(attempt, error = %e, "cannot raise replicas yet; retrying");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("loop returns on success or final failure")
    }

    async fn raise_replicas(&self, replicas: usize) -> Result<()> {
        for name in REPLICATED {
            let mut stream = self
                .js
                .get_stream(name)
                .await
                .map_err(|e| BusError::Other(format!("{name}: {e}")))?;
            let mut config = stream
                .info()
                .await
                .map_err(|e| BusError::Other(format!("{name} info: {e}")))?
                .config
                .clone();
            if config.num_replicas >= replicas {
                continue;
            }
            config.num_replicas = replicas;
            self.js
                .update_stream(config)
                .await
                .map_err(|e| BusError::Other(format!("{name} to {replicas} replicas: {e}")))?;
            tracing::info!(stream = name, replicas, "raised stream replicas");
        }
        Ok(())
    }

    /// Connect to a NATS server and set up streams/KV buckets.
    ///
    /// This is the **plaintext** constructor. It exists for localhost and
    /// development use only; production callers should use
    /// [`NatsBus::connect_secure`], which requires TLS material and refuses
    /// to downgrade.
    pub async fn connect(url: &str) -> Result<Arc<Self>> {
        Self::connect_with_ttl(url, DEFAULT_LEASE_TTL).await
    }

    /// Connect with a custom lease TTL (plaintext — see [`NatsBus::connect`]).
    pub async fn connect_with_ttl(url: &str, lease_ttl: Duration) -> Result<Arc<Self>> {
        Self::connect_inner(url, lease_ttl, None).await
    }

    /// Connect with token authentication (plaintext transport — for mTLS,
    /// see [`NatsBus::connect_secure`]). A compromised token alone must
    /// not be sufficient in production: pair with TLS.
    pub async fn connect_with_token(url: &str, token: &str) -> Result<Arc<Self>> {
        Self::connect_inner(url, DEFAULT_LEASE_TTL, Some(token)).await
    }

    /// This workspace links rustls with both ring and aws-lc-rs enabled
    /// (via dev/test dependencies), so auto-detection is ambiguous and
    /// panics in some binaries. Pin the process default; a provider
    /// already installed by the host application always wins.
    fn ensure_crypto_provider() {
        let _ = async_nats::rustls::crypto::ring::default_provider().install_default();
    }

    async fn connect_inner(
        url: &str,
        lease_ttl: Duration,
        token: Option<&str>,
    ) -> Result<Arc<Self>> {
        Self::ensure_crypto_provider();
        let mut options = async_nats::ConnectOptions::new().ignore_discovered_servers();
        if let Some(token) = token {
            options = options.token(token.to_string());
        }
        let client = options
            .connect(seed_list(url))
            .await
            .map_err(|e| BusError::Connection(e.to_string()))?;
        let js = jetstream::new(client.clone());

        let (status_kv, leases_kv) = Self::setup_when_ready(&js).await?;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            leases_kv,
            lease_ttl,
            consumers: Mutex::new(HashMap::new()),
        }))
    }

    /// Connect to a NATS server over **TLS**, with client certificate
    /// authentication.
    ///
    /// The TLS material is validated before the connection is attempted, and
    /// the connection is configured with `require_tls(true)`: a server that
    /// does not offer TLS fails the handshake instead of silently accepting
    /// plaintext.
    ///
    /// `url` must use the `tls://` scheme and name the host the server
    /// certificate is expected to carry. `config.expected_server_name` is the
    /// name checked against the certificate's SANs.
    pub async fn connect_secure(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
    ) -> Result<Arc<Self>> {
        config
            .preflight()
            .map_err(|e| BusError::Connection(format!("TLS preflight failed: {e}")))?;

        config
            .validate_url(url)
            .map_err(|e| BusError::Connection(format!("TLS URL rejected: {e}")))?;

        let options = config
            .connect_options()
            .map_err(|e| BusError::Connection(format!("TLS configuration failed: {e}")))?;

        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|e| BusError::Connection(format!("TLS connection failed: {e}")))?;

        let js = jetstream::new(client.clone());

        let (status_kv, leases_kv) = Self::setup_when_ready(&js).await?;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            leases_kv,
            lease_ttl: DEFAULT_LEASE_TTL,
            consumers: Mutex::new(HashMap::new()),
        }))
    }

    /// Connect over TLS with a custom lease TTL.
    pub async fn connect_secure_with_ttl(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        lease_ttl: Duration,
    ) -> Result<Arc<Self>> {
        config
            .preflight()
            .map_err(|e| BusError::Connection(format!("TLS preflight failed: {e}")))?;

        config
            .validate_url(url)
            .map_err(|e| BusError::Connection(format!("TLS URL rejected: {e}")))?;

        let options = config
            .connect_options()
            .map_err(|e| BusError::Connection(format!("TLS configuration failed: {e}")))?;

        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|e| BusError::Connection(format!("TLS connection failed: {e}")))?;

        let js = jetstream::new(client.clone());

        let (status_kv, leases_kv) = Self::setup_when_ready(&js).await?;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            leases_kv,
            lease_ttl,
            consumers: Mutex::new(HashMap::new()),
        }))
    }

    /// Creates streams and KV buckets, waiting for JetStream to be ready.
    ///
    /// A freshly started `nats-server` accepts TCP connections before its
    /// JetStream storage is initialized; creating streams or KV buckets in
    /// that window fails with internal errors (`meta.inf.tmp: no such file
    /// or directory`). Retry for a bounded period instead of failing the
    /// whole node startup on a lost race.
    async fn setup_when_ready(
        js: &JetStream,
    ) -> Result<(jetstream::kv::Store, jetstream::kv::Store)> {
        const MAX_ATTEMPTS: u32 = 20; // 20 × 500ms = 10s
        for attempt in 1..=MAX_ATTEMPTS {
            match Self::setup_all(js).await {
                Ok(pair) => return Ok(pair),
                Err(e) if attempt < MAX_ATTEMPTS => {
                    tracing::debug!(
                        attempt,
                        error = %e,
                        "JetStream not ready yet; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("loop returns on success or final failure")
    }

    async fn setup_all(js: &JetStream) -> Result<(jetstream::kv::Store, jetstream::kv::Store)> {
        Self::setup_streams(js).await?;
        let status_kv = Self::setup_kv(js, "MINGA_STATUS").await?;
        let leases_kv = Self::setup_kv(js, "MINGA_LEASES").await?;
        Ok((status_kv, leases_kv))
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

        js.get_or_create_stream(dead_letter_stream_config())
            .await
            .map_err(|e| BusError::Other(format!("{DEAD_LETTER_STREAM}: {e}")))?;

        Ok(())
    }

    async fn setup_kv(js: &JetStream, bucket: &str) -> Result<jetstream::kv::Store> {
        // Get before create: an update would reset replicas raised by
        // `ensure_replicas` back to one.
        if let Ok(store) = js.get_key_value(bucket).await {
            return Ok(store);
        }
        let config = KvConfig {
            bucket: bucket.to_string(),
            history: 1,
            ..Default::default()
        };
        js.create_key_value(config)
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
        let config = bounded(PullConsumerConfig {
            durable_name: Some(consumer_name),
            filter_subject: subjects::ctl_node_wildcard(node_id),
            ..Default::default()
        });

        // Create-or-update, so a durable consumer created before the
        // delivery bounds existed picks them up.
        let consumer = stream
            .create_consumer(config)
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
        let _timer = minga_telemetry::METRICS
            .bus_publish_latency_seconds
            .with_label_values(&["events"])
            .start_timer();
        self.js
            .publish(subject.to_string(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| BusError::Publish(e.to_string()))?
            .await
            .map_err(|e| BusError::Publish(format!("ack: {e}")))?;
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
        let config = bounded(PullConsumerConfig {
            filter_subjects,
            ..Default::default()
        });

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
                    if let Err(e) = m.ack().await {
                        tracing::warn!(error = %e, subject = %m.subject, "event ack failed; JetStream will redeliver");
                    }
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
        let _timer = minga_telemetry::METRICS
            .bus_publish_latency_seconds
            .with_label_values(&["commands"])
            .start_timer();
        let subject = match cmd {
            Command::Assign { .. } => subjects::ctl_assign(node_id),
            Command::Stop { .. } => subjects::ctl_stop(node_id),
        };
        let payload = cmd.to_bytes()?;
        self.js
            .publish(subject, Bytes::from(payload))
            .await
            .map_err(|e| BusError::Command(format!("publish: {e}")))?
            .await
            .map_err(|e| BusError::Command(format!("publish ack: {e}")))?;
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

        let cmd = match Command::from_bytes(&msg.payload) {
            Ok(cmd) => cmd,
            Err(e) => {
                reject(&self.client, &msg, "commands", &e).await;
                return Err(e);
            }
        };
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
        let _timer = minga_telemetry::METRICS
            .bus_publish_latency_seconds
            .with_label_values(&["logs"])
            .start_timer();
        let subject = subjects::task_log(task_id);
        let payload = entry.to_bytes()?;
        // Double await: the first yields a PublishAckFuture (message sent),
        // the second the server's acknowledgment (message stored). Without
        // the ack, a reader racing a publisher can miss entries.
        self.js
            .publish(subject, Bytes::from(payload))
            .await
            .map_err(|e| BusError::Publish(format!("log: {e}")))?
            .await
            .map_err(|e| BusError::Publish(format!("log ack: {e}")))?;
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
        let config = bounded(PullConsumerConfig {
            filter_subjects: vec![filter],
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        });

        let consumer = stream
            .create_consumer(config)
            .await
            .map_err(|e| BusError::Subscribe(format!("create log consumer: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| BusError::Subscribe(format!("log messages: {e}")))?;

        let client = self.client.clone();
        let log_stream = messages.filter_map(move |msg| {
            let client = client.clone();
            async move {
                match msg {
                    Ok(m) => match LogEntry::from_bytes(&m.payload) {
                        Ok(entry) => {
                            if let Err(e) = m.ack().await {
                                tracing::warn!(error = %e, subject = %m.subject, "log ack failed; JetStream will redeliver");
                            }
                            Some(entry)
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to deserialize log entry");
                            reject(&client, &m, "logs", &e).await;
                            None
                        }
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "log stream error");
                        None
                    }
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

/// Splits a comma-separated seed list into the URLs async-nats accepts.
fn seed_list(url: &str) -> Vec<&str> {
    url.split(',').map(str::trim).collect()
}

/// Makes `config` an explicit-ack consumer with bounded, backed-off
/// redelivery, so a poison message cannot redeliver forever.
fn bounded(config: PullConsumerConfig) -> PullConsumerConfig {
    PullConsumerConfig {
        ack_policy: AckPolicy::Explicit,
        max_deliver: MAX_DELIVER,
        backoff: REDELIVERY_BACKOFF.to_vec(),
        ..config
    }
}

/// Limits-retention stream that captures the advisories JetStream publishes
/// when it gives up on a message (max deliveries reached, or `+TERM`).
fn dead_letter_stream_config() -> StreamConfig {
    StreamConfig {
        name: DEAD_LETTER_STREAM.to_string(),
        subjects: vec![
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>".to_string(),
            "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>".to_string(),
        ],
        retention: RetentionPolicy::Limits,
        max_bytes: 64 * 1024 * 1024,
        max_age: Duration::from_secs(7 * 24 * 3600),
        ..Default::default()
    }
}

/// How a message whose handling failed is settled.
#[derive(Debug, PartialEq)]
enum Rejection {
    /// Poison: it will never succeed, stop redelivery now.
    Term(String),
    /// Transient: redeliver after a delay.
    Nak(Duration),
}

fn rejection_for(err: &BusError) -> Rejection {
    match err {
        BusError::Serialization(reason) => Rejection::Term(reason.clone()),
        _ => Rejection::Nak(NAK_DELAY),
    }
}

/// Terms (poison) or naks (transient) a message whose handling failed,
/// counting it as dead-lettered when JetStream will not redeliver it.
async fn reject(client: &Client, msg: &jetstream::Message, topic: &str, err: &BusError) {
    let delivered = msg.info().map(|i| i.delivered).unwrap_or(0);
    let (result, dead_letter) = match rejection_for(err) {
        Rejection::Term(reason) => {
            // async-nats' AckKind::Term carries no reason; the raw `+TERM
            // <reason>` puts it into the MSG_TERMINATED advisory.
            let result = match &msg.reply {
                Some(reply) => client
                    .publish(reply.clone(), Bytes::from(format!("+TERM {reason}")))
                    .await
                    .map_err(|e| e.to_string()),
                None => Err("no reply subject".to_string()),
            };
            (result, Some("terminated"))
        }
        Rejection::Nak(delay) => (
            msg.ack_with(AckKind::Nak(Some(delay)))
                .await
                .map_err(|e| e.to_string()),
            (delivered >= MAX_DELIVER).then_some("max_deliveries"),
        ),
    };
    match result {
        Ok(()) => {
            if let Some(reason) = dead_letter {
                tracing::warn!(topic, subject = %msg.subject, delivered, reason, error = %err, "message dead-lettered");
                minga_telemetry::METRICS
                    .bus_dead_letters_total
                    .with_label_values(&[topic, reason])
                    .inc();
            }
        }
        Err(e) => {
            tracing::warn!(topic, subject = %msg.subject, error = %e, "reject failed; JetStream will redeliver after the ack wait");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_consumer_caps_redelivery() {
        let config = bounded(PullConsumerConfig {
            ack_policy: AckPolicy::None,
            ..Default::default()
        });
        assert_eq!(config.ack_policy, AckPolicy::Explicit);
        assert_eq!(config.max_deliver, MAX_DELIVER);
        // JetStream rejects a consumer whose backoff list is not shorter
        // than max_deliver.
        assert!((config.backoff.len() as i64) < config.max_deliver);
        assert_eq!(config.backoff[0], Duration::from_secs(30));
    }

    #[test]
    fn dead_letter_stream_captures_both_advisories() {
        let config = dead_letter_stream_config();
        assert_eq!(config.retention, RetentionPolicy::Limits);
        assert!(config.max_age > Duration::ZERO && config.max_bytes > 0);
        assert!(config.subjects.iter().any(|s| s.contains("MAX_DELIVERIES")));
        assert!(config.subjects.iter().any(|s| s.contains("MSG_TERMINATED")));
    }

    #[test]
    fn decode_errors_are_poison_others_are_transient() {
        assert_eq!(
            rejection_for(&BusError::Serialization("bad".into())),
            Rejection::Term("bad".into())
        );
        assert_eq!(
            rejection_for(&BusError::Command("handler busy".into())),
            Rejection::Nak(NAK_DELAY)
        );
    }
}
