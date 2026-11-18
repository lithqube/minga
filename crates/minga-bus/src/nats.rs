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
//! | `MINGA_EVENTS` | `minga.node.>`, `minga.task.>`, `minga.egress.>` | Limits (72h) | Lifecycle events |
//! | `MINGA_LOGS` | `minga.log.>` | Limits (512 MiB, 24h) | Container log chunks |
//!
//! # KV buckets
//!
//! | Bucket | Keys | Purpose |
//! |---|---|---|
//! | `MINGA_ASSIGN` | `node.{id}.task.{id}`, `node.{id}.net.{id}` | Manager-authored desired task assignments and, per node, the networks its tasks use with their DNS records |
//! | `MINGA_LEASES` | `lease.node.{id}` | Liveness (server-expiring registration lease) |
//! | `MINGA_STATUS` | `node.{id}.task.{id}` | Actual task status reports |
//!
//! # High availability
//!
//! Every constructor takes a comma-separated seed list
//! (`tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222`) and connects
//! only to those servers, ignoring the ones a server advertises, so a TLS
//! connection never reaches a host the operator did not name. Streams and
//! buckets are created with one replica; [`NatsBus::ensure_replicas`]
//! raises the ones in [`REPLICATED`] on a NATS cluster.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
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
    subjects, BusError, BusEvent, BusRequest, EventBus, EventStream, KvEvent, KvOp, KvStream,
    LogEntry, LogEventStream, RequestStream, Result,
};

/// Default lease TTL: 30 seconds.
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

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

/// Consumer limit set on every KV bucket. nats-server 2.15 otherwise caps a
/// stream at 1,000. shortcut: sized for the T4 target (5,000 workers, two
/// watches each, plus listings) with 10x headroom; raise it if a cluster
/// outgrows that.
const BUCKET_MAX_CONSUMERS: i32 = 100_000;

/// Streams (KV buckets are `KV_`-prefixed streams) that losing one NATS
/// server must not lose. `MINGA_LOGS` stays at one replica: it is the
/// largest stream, best effort, and edge disks are small.
const REPLICATED: [&str; 4] = [
    "MINGA_EVENTS",
    "KV_MINGA_STATUS",
    "KV_MINGA_LEASES",
    "KV_MINGA_ASSIGN",
];

// ---------------------------------------------------------------------------
// NatsBus
// ---------------------------------------------------------------------------

/// NATS/JetStream event bus.
pub struct NatsBus {
    client: Client,
    js: JetStream,
    status_kv: jetstream::kv::Store,
    assignments_kv: jetstream::kv::Store,
    leases_kv: jetstream::kv::Store,
    api_level: Option<usize>,
    worker_node_id: Option<u64>,
}

/// Turns a KV stream message into a put or delete event.
fn kv_event(prefix_subject: &str, message: &jetstream::Message) -> Option<KvEvent> {
    let key = message.subject.strip_prefix(prefix_subject)?.to_string();
    let deleted = message
        .headers
        .as_ref()
        .and_then(|headers| headers.get("KV-Operation"))
        .is_some_and(|op| matches!(op.as_str(), "DEL" | "PURGE"));
    let (op, value) = if deleted {
        (KvOp::Delete, None)
    } else {
        (KvOp::Put, Some(message.payload.to_vec()))
    };
    Some(KvEvent { key, value, op })
}

const MIN_SUPPORTED_JS_API_LEVEL: usize = 4;

/// Returns a warning for a JetStream API level below Minga's supported floor.
pub fn api_level_warning(level: usize, server_version: &str) -> Option<String> {
    (level < MIN_SUPPORTED_JS_API_LEVEL).then(|| {
        format!(
            "NATS server {server_version} reports JetStream API level {level}; \
             Minga requires API level {MIN_SUPPORTED_JS_API_LEVEL} and is tested only on NATS 2.14 and 2.15"
        )
    })
}

impl NatsBus {
    /// The underlying JetStream context.
    ///
    /// Exposed for integration tests and for callers that need direct
    /// JetStream access (stream inspection, admin operations).
    pub fn jetstream(&self) -> &JetStream {
        &self.js
    }

    /// JetStream API level reported by the server at manager startup.
    ///
    /// Workers do not query account info and always return `None`.
    pub fn api_level(&self) -> Option<usize> {
        self.api_level
    }

    async fn read_api_level(js: &JetStream, client: &Client) -> Option<usize> {
        let server_version = client.server_info().version;
        match js.query_account().await {
            Ok(account) => {
                let level = account.requests.level;
                tracing::info!(
                    api_level = level,
                    server_version = %server_version,
                    "JetStream API level"
                );
                if let Some(warning) = api_level_warning(level, &server_version) {
                    tracing::warn!(
                        api_level = level,
                        server_version = %server_version,
                        minimum_api_level = MIN_SUPPORTED_JS_API_LEVEL,
                        warning = %warning,
                        "external NATS server is below the supported JetStream API level"
                    );
                }
                Some(level)
            }
            Err(error) => {
                tracing::warn!(
                    server_version = %server_version,
                    error = %error,
                    minimum_api_level = MIN_SUPPORTED_JS_API_LEVEL,
                    "could not read JetStream API level; Minga is tested only on NATS 2.14 and 2.15"
                );
                None
            }
        }
    }

    /// An ordered consumer on MINGA_ASSIGN filtered to `prefix`, with its
    /// pending count. It is named so the filter travels in the create
    /// subject, where the worker ACL can bind it (see `subjects::worker_acl`).
    async fn assignment_consumer(
        &self,
        prefix: &str,
        deliver_policy: DeliverPolicy,
    ) -> Result<(u64, jetstream::consumer::push::Ordered)> {
        let inbox = self.client.new_inbox();
        let name = inbox.rsplit('.').next().unwrap_or_default().to_string();
        let filter = crate::assignment_watch_filter(prefix);
        let consumer = self
            .assignments_kv
            .stream
            .create_consumer(jetstream::consumer::push::OrderedConfig {
                name: Some(name),
                deliver_subject: inbox,
                description: Some("assignment consumer".to_string()),
                filter_subject: format!("{}{filter}", self.assignments_kv.prefix),
                replay_policy: jetstream::consumer::ReplayPolicy::Instant,
                deliver_policy,
                ..Default::default()
            })
            .await
            .map_err(|error| BusError::Kv(format!("assignment consumer: {error}")))?;
        let pending = consumer.cached_info().num_pending;
        let messages = consumer
            .messages()
            .await
            .map_err(|error| BusError::Kv(format!("assignment messages: {error}")))?;
        Ok((pending, messages))
    }

    fn assignment_event(&self, message: &jetstream::Message) -> Option<KvEvent> {
        kv_event(&self.assignments_kv.prefix, message)
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
        if self.worker_node_id.is_some() {
            return Err(BusError::Other(
                "workers cannot change JetStream replica counts".to_string(),
            ));
        }
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

    fn require_manager(&self, operation: &str) -> Result<()> {
        if self.worker_node_id.is_some() {
            return Err(BusError::Kv(format!("worker identity cannot {operation}")));
        }
        Ok(())
    }

    fn check_worker_key(&self, key: &str) -> Result<()> {
        if let Some(node_id) = self.worker_node_id {
            let task_prefix = subjects::kv_assignment_node_prefix(node_id);
            let ingress_prefix = subjects::kv_assignment_ingress_prefix(node_id);
            if !key.starts_with(&task_prefix) && !key.starts_with(&ingress_prefix) {
                return Err(BusError::Kv(format!(
                    "worker {node_id} cannot access assignment/status key {key}"
                )));
            }
        }
        Ok(())
    }

    fn check_worker_prefix(&self, prefix: &str) -> Result<()> {
        if let Some(node_id) = self.worker_node_id {
            let task_prefix = subjects::kv_assignment_node_prefix(node_id);
            let ingress_prefix = subjects::kv_assignment_ingress_prefix(node_id);
            let network_prefix = subjects::kv_assignment_node_network_prefix(node_id);
            if prefix != task_prefix && prefix != ingress_prefix && prefix != network_prefix {
                return Err(BusError::Kv(format!(
                    "worker {node_id} may list only its own task, ingress and network assignments"
                )));
            }
        }
        Ok(())
    }

    fn check_worker_watch(&self, prefix: &str) -> Result<()> {
        if let Some(node_id) = self.worker_node_id {
            let task_prefix = subjects::kv_assignment_node_prefix(node_id);
            let ingress_prefix = subjects::kv_assignment_ingress_prefix(node_id);
            let wireguard_key = format!("node.{node_id}.wg");
            let network_prefix = subjects::kv_assignment_node_network_prefix(node_id);
            if prefix != task_prefix
                && prefix != ingress_prefix
                && prefix != wireguard_key
                && prefix != network_prefix
            {
                return Err(BusError::Kv(format!(
                    "worker {node_id} may watch only its own assignments and networks"
                )));
            }
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

    /// Connect as a worker without creating streams or buckets.
    ///
    /// Plaintext worker connections are intended only for local development;
    /// production workers should use [`Self::connect_worker_secure`].
    pub async fn connect_worker(url: &str, node_id: u64) -> Result<Arc<Self>> {
        Self::connect_worker_with_token_inner(url, node_id, None).await
    }

    /// Connect a development worker with token authentication, without
    /// creating streams or buckets.
    pub async fn connect_worker_with_token(
        url: &str,
        token: &str,
        node_id: u64,
    ) -> Result<Arc<Self>> {
        Self::connect_worker_with_token_inner(url, node_id, Some(token)).await
    }

    async fn connect_worker_with_token_inner(
        url: &str,
        node_id: u64,
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
            .map_err(|error| BusError::Connection(error.to_string()))?;
        Self::from_worker_client(client, node_id).await
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

        let (status_kv, assignments_kv, leases_kv) = Self::setup_when_ready(&js, lease_ttl).await?;
        let api_level = Self::read_api_level(&js, &client).await;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            assignments_kv,
            leases_kv,
            api_level,
            worker_node_id: None,
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
        let options = Self::secure_options(url, config, None)?;

        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|e| BusError::Connection(format!("TLS connection failed: {e}")))?;

        let js = jetstream::new(client.clone());

        let (status_kv, assignments_kv, leases_kv) =
            Self::setup_when_ready(&js, DEFAULT_LEASE_TTL).await?;
        let api_level = Self::read_api_level(&js, &client).await;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            assignments_kv,
            leases_kv,
            api_level,
            worker_node_id: None,
        }))
    }

    /// Connect over TLS with a custom lease TTL.
    pub async fn connect_secure_with_ttl(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        lease_ttl: Duration,
    ) -> Result<Arc<Self>> {
        let options = Self::secure_options(url, config, None)?;

        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|e| BusError::Connection(format!("TLS connection failed: {e}")))?;

        let js = jetstream::new(client.clone());

        let (status_kv, assignments_kv, leases_kv) = Self::setup_when_ready(&js, lease_ttl).await?;
        let api_level = Self::read_api_level(&js, &client).await;

        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            assignments_kv,
            leases_kv,
            api_level,
            worker_node_id: None,
        }))
    }

    /// Connect a worker over mTLS and open only already-created buckets.
    pub async fn connect_worker_secure(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        node_id: u64,
    ) -> Result<Arc<Self>> {
        let options = Self::secure_options(url, config, None)?;
        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|error| BusError::Connection(format!("TLS connection failed: {error}")))?;
        Self::from_worker_client(client, node_id).await
    }
    /// Connect a worker with NATS JWT credentials over mTLS.
    ///
    /// The credentials authenticate the NATS principal; the client
    /// certificate remains required for transport security.
    pub async fn connect_worker_secure_with_credentials(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        credentials: &str,
        node_id: u64,
    ) -> Result<Arc<Self>> {
        let options = Self::secure_options(url, config, Some(credentials))?;
        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|error| BusError::Connection(format!("TLS connection failed: {error}")))?;
        Self::from_worker_client(client, node_id).await
    }

    /// Connect a manager with NATS JWT credentials over mTLS.
    pub async fn connect_secure_with_credentials(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        credentials: &str,
    ) -> Result<Arc<Self>> {
        let options = Self::secure_options(url, config, Some(credentials))?;
        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|error| BusError::Connection(format!("TLS connection failed: {error}")))?;
        let js = jetstream::new(client.clone());
        let (status_kv, assignments_kv, leases_kv) =
            Self::setup_when_ready(&js, DEFAULT_LEASE_TTL).await?;
        let api_level = Self::read_api_level(&js, &client).await;
        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            assignments_kv,
            leases_kv,
            api_level,
            worker_node_id: None,
        }))
    }

    fn secure_options(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        credentials: Option<&str>,
    ) -> Result<async_nats::ConnectOptions> {
        config
            .preflight()
            .map_err(|error| BusError::Connection(format!("TLS preflight failed: {error}")))?;
        config
            .validate_url(url)
            .map_err(|error| BusError::Connection(format!("TLS URL rejected: {error}")))?;
        let options = match credentials {
            Some(credentials) => config.connect_options_with_credentials(credentials),
            None => config.connect_options(),
        };
        options.map_err(|error| BusError::Connection(format!("TLS configuration failed: {error}")))
    }

    async fn from_worker_client(client: Client, node_id: u64) -> Result<Arc<Self>> {
        let js = jetstream::new(client.clone());
        let status_kv = js
            .get_key_value("MINGA_STATUS")
            .await
            .map_err(|error| BusError::Kv(format!("open MINGA_STATUS: {error}")))?;
        let assignments_kv = js
            .get_key_value("MINGA_ASSIGN")
            .await
            .map_err(|error| BusError::Kv(format!("open MINGA_ASSIGN: {error}")))?;
        let leases_kv = js
            .get_key_value("MINGA_LEASES")
            .await
            .map_err(|error| BusError::Kv(format!("open MINGA_LEASES: {error}")))?;
        Ok(Arc::new(Self {
            client,
            js,
            status_kv,
            assignments_kv,
            leases_kv,
            api_level: None,
            worker_node_id: Some(node_id),
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
        lease_ttl: Duration,
    ) -> Result<(
        jetstream::kv::Store,
        jetstream::kv::Store,
        jetstream::kv::Store,
    )> {
        const MAX_ATTEMPTS: u32 = 20;
        for attempt in 1..=MAX_ATTEMPTS {
            match Self::setup_all(js, lease_ttl).await {
                Ok(stores) => return Ok(stores),
                Err(error) if attempt < MAX_ATTEMPTS => {
                    tracing::debug!(attempt, error = %error, "JetStream not ready yet; retrying");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("loop returns on success or final failure")
    }
    async fn setup_all(
        js: &JetStream,
        lease_ttl: Duration,
    ) -> Result<(
        jetstream::kv::Store,
        jetstream::kv::Store,
        jetstream::kv::Store,
    )> {
        Self::setup_streams(js).await?;
        let status_kv = Self::setup_kv(js, "MINGA_STATUS", Duration::ZERO).await?;
        let assignments_kv = Self::setup_kv(js, "MINGA_ASSIGN", Duration::ZERO).await?;
        let leases_kv = Self::setup_kv(js, "MINGA_LEASES", lease_ttl).await?;
        Ok((status_kv, assignments_kv, leases_kv))
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

    async fn setup_kv(
        js: &JetStream,
        bucket: &str,
        max_age: Duration,
    ) -> Result<jetstream::kv::Store> {
        // Get before create: an update would reset replicas raised by
        // `ensure_replicas` back to one.
        if let Ok(mut store) = js.get_key_value(bucket).await {
            if max_age > Duration::ZERO {
                let mut config = store
                    .stream
                    .info()
                    .await
                    .map_err(|e| BusError::Kv(format!("{bucket} info: {e}")))?
                    .config
                    .clone();
                if config.max_age != max_age {
                    config.max_age = max_age;
                    // The server refuses a duplicate window longer than max
                    // age, so a shorter TTL must shrink it too. Same sizing
                    // as async-nats uses when it creates the bucket.
                    config.duplicate_window = max_age.min(Duration::from_secs(120));
                    js.update_stream(config)
                        .await
                        .map_err(|e| BusError::Kv(format!("{bucket} max age: {e}")))?;
                }
            }
            Self::lift_consumer_cap(js, bucket, &mut store).await?;
            return Ok(store);
        }
        let config = KvConfig {
            bucket: bucket.to_string(),
            history: 1,
            max_age,
            ..Default::default()
        };
        let mut store = js
            .create_key_value(config)
            .await
            .map_err(|e| BusError::Kv(format!("{bucket}: {e}")))?;
        Self::lift_consumer_cap(js, bucket, &mut store).await?;
        Ok(store)
    }

    /// nats-server 2.15 caps a stream at 1,000 consumers unless the stream
    /// sets a positive `max_consumers` (-1 means "server default" there).
    /// Every worker holds two watches on the assignment bucket, so the cap
    /// would block new workers at about 500.
    async fn lift_consumer_cap(
        js: &JetStream,
        bucket: &str,
        store: &mut jetstream::kv::Store,
    ) -> Result<()> {
        let mut config = store
            .stream
            .info()
            .await
            .map_err(|e| BusError::Kv(format!("{bucket} info: {e}")))?
            .config
            .clone();
        if config.max_consumers >= BUCKET_MAX_CONSUMERS {
            return Ok(());
        }
        config.max_consumers = BUCKET_MAX_CONSUMERS;
        if let Err(error) = js.update_stream(config).await {
            // Servers before 2.12.5 refuse to change max_consumers after
            // creation, but they also have no default cap to lift.
            tracing::warn!(
                bucket,
                %error,
                "cannot raise the bucket's consumer limit; fine on servers older than 2.15"
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EventBus implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl EventBus for NatsBus {
    async fn publish_event(&self, subject: &str, payload: &[u8]) -> Result<()> {
        self.require_manager("publish events")?;
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
        self.require_manager("subscribe to events")?;
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

    async fn put_assignment(&self, key: &str, value: &[u8]) -> Result<()> {
        self.require_manager("write assignments")?;
        self.assignments_kv
            .put(key.to_string(), Bytes::copy_from_slice(value))
            .await
            .map_err(|error| BusError::Kv(format!("put assignment {key}: {error}")))?;
        Ok(())
    }

    async fn get_assignment(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.check_worker_key(key)?;
        self.assignments_kv
            .get(key.to_string())
            .await
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .map_err(|error| BusError::Kv(format!("get assignment {key}: {error}")))
    }

    async fn list_assignments(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.check_worker_prefix(prefix)?;
        let (pending, mut messages) = self
            .assignment_consumer(prefix, DeliverPolicy::LastPerSubject)
            .await?;
        // Keyed, so a key updated mid-listing yields its latest value once.
        let mut entries = std::collections::BTreeMap::new();
        if pending == 0 {
            return Ok(Vec::new());
        }
        while let Some(message) = messages.next().await {
            let message =
                message.map_err(|error| BusError::Kv(format!("list assignments: {error}")))?;
            let left = message
                .info()
                .map_err(|error| BusError::Kv(format!("assignment info: {error}")))?
                .pending;
            if let Some(event) = self.assignment_event(&message) {
                match event.value {
                    Some(value) => entries.insert(event.key, value),
                    None => entries.remove(&event.key),
                };
            }
            if left == 0 {
                break;
            }
        }
        Ok(entries.into_iter().collect())
    }

    async fn delete_assignment(&self, key: &str) -> Result<()> {
        self.require_manager("delete assignments")?;
        self.assignments_kv
            .delete(key.to_string())
            .await
            .map_err(|error| BusError::Kv(format!("delete assignment {key}: {error}")))
    }

    async fn watch_assignments(&self, prefix: &str) -> Result<KvStream> {
        self.check_worker_watch(prefix)?;
        let (_, messages) = self.assignment_consumer(prefix, DeliverPolicy::All).await?;
        let prefix_subject = self.assignments_kv.prefix.clone();
        let prefix = prefix.to_string();
        let stream = messages.filter_map(move |message| {
            let prefix_subject = prefix_subject.clone();
            let prefix = prefix.clone();
            async move {
                match message {
                    Ok(message) => kv_event(&prefix_subject, &message)
                        .filter(|event| crate::assignment_prefix_matches(&prefix, &event.key)),
                    Err(error) => {
                        tracing::warn!(error = %error, "assignment watch error");
                        None
                    }
                }
            }
        });
        Ok(Box::pin(stream))
    }
    async fn put_status(&self, key: &str, value: &[u8]) -> Result<()> {
        self.check_worker_key(key)?;
        self.status_kv
            .put(key.to_string(), Bytes::copy_from_slice(value))
            .await
            .map_err(|e| BusError::Kv(format!("put {key}: {e}")))?;
        Ok(())
    }

    async fn get_status(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.check_worker_key(key)?;
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
        self.require_manager("list statuses")?;
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
        self.require_manager("watch statuses")?;
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

    async fn refresh_lease(&self, node_id: u64, registration: &[u8]) -> Result<()> {
        if self
            .worker_node_id
            .is_some_and(|worker_id| worker_id != node_id)
        {
            return Err(BusError::Lease(format!(
                "worker cannot refresh node {node_id}'s lease"
            )));
        }
        let key = subjects::kv_lease(node_id);
        self.leases_kv
            .put(key, Bytes::copy_from_slice(registration))
            .await
            .map_err(|e| BusError::Lease(format!("refresh {node_id}: {e}")))?;
        Ok(())
    }

    async fn is_alive(&self, node_id: u64) -> Result<bool> {
        if self
            .worker_node_id
            .is_some_and(|worker_id| worker_id != node_id)
        {
            return Err(BusError::Lease(format!(
                "worker cannot query node {node_id}'s lease"
            )));
        }
        let key = subjects::kv_lease(node_id);
        Ok(self
            .leases_kv
            .get(key)
            .await
            .map_err(|e| BusError::Lease(format!("get {node_id}: {e}")))?
            .is_some())
    }

    async fn list_alive_nodes(&self) -> Result<HashSet<u64>> {
        self.require_manager("list leases")?;
        let mut keys = self
            .leases_kv
            .keys()
            .await
            .map_err(|e| BusError::Lease(format!("list: {e}")))?;
        let mut nodes = HashSet::new();
        while let Some(key) = keys.next().await {
            let key = key.map_err(|e| BusError::Lease(format!("list key: {e}")))?;
            if let Some(node_id) = key
                .strip_prefix("lease.node.")
                .and_then(|id| id.parse().ok())
            {
                nodes.insert(node_id);
            }
        }
        Ok(nodes)
    }

    async fn watch_leases(&self) -> Result<KvStream> {
        self.require_manager("watch leases")?;
        // Lease entries have history=1, so replay the current snapshot before
        // streaming new leases. Managers can start or regain leadership after
        // nodes have already joined.
        let watcher = self
            .leases_kv
            .watch_all_from_revision(1)
            .await
            .map_err(|e| BusError::Lease(format!("watch: {e}")))?;

        let stream = watcher.filter_map(|entry_result| async move {
            match entry_result {
                Ok(entry) => {
                    let op = match entry.operation {
                        KvOperation::Put => KvOp::Put,
                        KvOperation::Delete | KvOperation::Purge => KvOp::Delete,
                    };
                    let value = (op == KvOp::Put).then(|| entry.value.to_vec());
                    Some(KvEvent {
                        key: entry.key,
                        value,
                        op,
                    })
                }
                Err(error) => {
                    tracing::warn!(error = %error, "lease watch error");
                    None
                }
            }
        });
        Ok(Box::pin(stream))
    }

    async fn request(&self, subject: &str, payload: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
        self.require_manager("send requests")?;
        // async-nats handles timeout internally via its own configuration.
        let msg = self
            .client
            .request(subject.to_string(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| BusError::Connection(format!("request: {e}")))?;
        Ok(msg.payload.to_vec())
    }

    async fn subscribe_requests(&self, subject: &str) -> Result<RequestStream> {
        self.require_manager("subscribe to requests")?;
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
        self.require_manager("send replies")?;
        self.client
            .publish(reply_subject.to_string(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|e| BusError::Publish(format!("reply: {e}")))?;
        Ok(())
    }

    async fn publish_log(&self, task_id: &str, entry: &LogEntry) -> Result<()> {
        let node_id = self.worker_node_id.unwrap_or(0);
        self.publish_node_log(node_id, task_id, entry).await
    }

    async fn publish_node_log(&self, node_id: u64, task_id: &str, entry: &LogEntry) -> Result<()> {
        if self
            .worker_node_id
            .is_some_and(|worker_id| worker_id != node_id)
        {
            return Err(BusError::Publish(format!(
                "worker cannot publish logs for node {node_id}"
            )));
        }
        if entry.task_id != task_id {
            return Err(BusError::Publish(
                "log subject task ID does not match entry".to_string(),
            ));
        }
        let _timer = minga_telemetry::METRICS
            .bus_publish_latency_seconds
            .with_label_values(&["logs"])
            .start_timer();
        let subject = subjects::node_task_log(node_id, task_id);
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
        self.require_manager("subscribe to logs")?;
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
        let task_id = task_id.to_string();
        let log_stream = messages.filter_map(move |msg| {
            let client = client.clone();
            let task_id = task_id.clone();
            async move {
                match msg {
                    Ok(m) => match LogEntry::from_bytes(&m.payload) {
                        Ok(entry) => {
                            if let Err(e) = m.ack().await {
                                tracing::warn!(error = %e, subject = %m.subject, "log ack failed; JetStream will redeliver");
                            }
                            (task_id == "*" || entry.task_id == task_id).then_some(entry)
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
        self.require_manager("read log history")?;
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
                    if task_id == "*" || entry.task_id == task_id {
                        if wanted > 0 && entries.len() == wanted {
                            entries.pop_front();
                        }
                        entries.push_back(entry);
                    }
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

/// Terms poison messages and delays transient failures without retaining a
/// separate dead-letter stream.
async fn reject(client: &Client, msg: &jetstream::Message, topic: &str, err: &BusError) {
    let delivered = msg.info().map(|info| info.delivered).unwrap_or(0);
    let result = match rejection_for(err) {
        Rejection::Term(reason) => match &msg.reply {
            Some(reply) => client
                .publish(reply.clone(), Bytes::from(format!("+TERM {reason}")))
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            None => Err("no reply subject".to_string()),
        },
        Rejection::Nak(delay) => msg
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(|error| error.to_string()),
    };
    match result {
        Ok(()) if delivered >= MAX_DELIVER => {
            tracing::warn!(topic, subject = %msg.subject, delivered, error = %err, "message delivery limit reached");
        }
        Ok(()) => {}
        Err(error) => {
            tracing::warn!(topic, subject = %msg.subject, error, "reject failed; JetStream will redeliver after the ack wait");
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
    fn decode_errors_are_poison_others_are_transient() {
        assert_eq!(
            rejection_for(&BusError::Serialization("bad".into())),
            Rejection::Term("bad".into())
        );
        assert_eq!(
            rejection_for(&BusError::Other("handler busy".into())),
            Rejection::Nak(NAK_DELAY)
        );
    }

    #[test]
    fn api_level_warning_only_reports_levels_below_four() {
        assert_eq!(
            api_level_warning(0, "2.10.22").as_deref(),
            Some(
                "NATS server 2.10.22 reports JetStream API level 0; Minga requires API level 4 and is tested only on NATS 2.14 and 2.15"
            )
        );
        assert_eq!(
            api_level_warning(3, "2.14.0").as_deref(),
            Some(
                "NATS server 2.14.0 reports JetStream API level 3; Minga requires API level 4 and is tested only on NATS 2.14 and 2.15"
            )
        );
        assert_eq!(api_level_warning(4, "2.14.10"), None);
        assert_eq!(api_level_warning(5, "2.15.3"), None);
    }
}
