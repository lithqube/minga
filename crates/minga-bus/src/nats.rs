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
//! | `MINGA_EVENTS` | `minga.*.node.>`, `minga.*.task.>`, `minga.*.egress.>` | Limits (72h) | Manager-published lifecycle events |
//! | `MINGA_LOGS_<tenant>` | `minga.<tenant>.log.>` | Tenant quota | Worker log chunks |
//! | `MINGA_AUDIT_<tenant>` | `minga.<tenant>.audit` | Tenant quota | Tenant audit records |
//! | `MINGA_LOGS` | `legacy.minga.log.>` | Legacy retention | Old unscoped log records; no new writes |
//!
//! # KV buckets
//!
//! | Bucket | Keys | Purpose |
//! |---|---|---|
//! | `MINGA_ASSIGN` | `<tenant>.node.{id}.task.{id}`, `.net.{id}`, `.ingress.*`, `.wg` | Manager-authored tenant/node assignments |
//! | `MINGA_LEASES` | `<tenant>.lease.node.{id}` | Tenant-scoped, server-expiring registration lease |
//! | `MINGA_STATUS_<tenant>` | `node.{id}.task.{id}` | Tenant-scoped actual task status |
//! | `MINGA_USAGE_<tenant>`, `MINGA_BUDGET_<tenant>` | Accounting keys | Tenant quota buckets |
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
use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use crate::{
    subjects, BusError, BusEvent, BusRequest, EventBus, EventStream, KvEvent, KvOp, KvStream,
    LogEntry, LogEventStream, RequestStream, Result, UsageEvent, UsageTotals,
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
const EVENT_SUBJECTS: [&str; 3] = ["minga.*.node.>", "minga.*.task.>", "minga.*.egress.>"];
const DEFAULT_TENANT_STREAM_MAX_BYTES: i64 = 512 * 1024 * 1024;
const MAX_TENANT_STREAM_MAX_BYTES: i64 = 10 * 1024 * 1024 * 1024;
const DEFAULT_TENANT_STREAM_MAX_AGE: Duration = Duration::from_secs(24 * 3600);
const MAX_TENANT_STREAM_MAX_AGE: Duration = Duration::from_secs(365 * 24 * 3600);

/// Streams (KV buckets are `KV_`-prefixed streams) that losing one NATS
/// server must not lose. Shared and per-tenant log streams stay at one replica:
/// they are best effort, and edge disks are small.
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
    worker_tenant_id: Option<String>,
    system_client: tokio::sync::OnceCell<Client>,
    /// Tenants whose buckets and streams this connection already set up, so
    /// audited or rate-limited requests do not repeat the JetStream calls.
    /// shortcut: assumes the server does not lose those resources while this
    /// connection lives; evict on a "not found" error if that ever happens.
    ensured_tenants: std::sync::Mutex<std::collections::HashSet<String>>,
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

/// Visits every well-formed usage record of `tenant_id` at or after
/// `since_unix_ms`. Records are tenant-written, so a malformed or
/// mismatched one is skipped with a warning instead of failing the query.
/// A tenant without a usage stream has no records.
async fn visit_usage(
    js: &JetStream,
    tenant_id: &str,
    since_unix_ms: u64,
    mut visit: impl FnMut(UsageEvent),
) -> Result<()> {
    let name = format!("MINGA_USAGE_{tenant_id}");
    let stream = match js.get_stream(&name).await {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                jetstream::context::GetStreamErrorKind::JetStream(ref js_error)
                    if js_error.error_code() == jetstream::ErrorCode::STREAM_NOT_FOUND
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(BusError::Subscribe(format!("get {name}: {error}"))),
    };
    // Not `ByStartTime`: it cuts by the server's storage time, and a worker
    // clock ahead of the server would hide events whose own time qualifies.
    // The stream keeps a day at most, and the payload filter below decides.
    let mut consumer = stream
        .create_consumer(PullConsumerConfig {
            filter_subjects: vec![format!("minga.{tenant_id}.usage.>")],
            ack_policy: AckPolicy::None,
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .map_err(|error| BusError::Subscribe(format!("usage consumer: {error}")))?;
    let mut pending = consumer
        .info()
        .await
        .map_err(|error| BusError::Subscribe(format!("usage consumer info: {error}")))?
        .num_pending as usize;
    while pending > 0 {
        let mut batch = consumer
            .fetch()
            .max_messages(pending.min(200))
            .expires(Duration::from_secs(5))
            .messages()
            .await
            .map_err(|error| BusError::Subscribe(format!("usage fetch: {error}")))?;
        let mut received = 0usize;
        while let Some(message) = batch.next().await {
            let message =
                message.map_err(|error| BusError::Subscribe(format!("usage message: {error}")))?;
            received += 1;
            match serde_json::from_slice::<UsageEvent>(&message.payload) {
                Ok(event) if event.tenant_id != tenant_id => {
                    tracing::warn!(
                        tenant = tenant_id,
                        "skipping usage record of another tenant"
                    );
                }
                Ok(event) if event.occurred_at_unix_ms >= since_unix_ms => visit(event),
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(tenant = tenant_id, %error, "skipping malformed usage record");
                }
            }
        }
        if received == 0 {
            break;
        }
        pending = pending.saturating_sub(received);
    }
    Ok(())
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
    /// Configure the dedicated SYS-account connection used for resolver updates.
    pub async fn enable_system_account(
        &self,
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        credentials: &str,
    ) -> Result<()> {
        if self.worker_node_id.is_some() {
            return Err(BusError::Other(
                "worker connections cannot configure a SYS-account client".to_string(),
            ));
        }
        let options = Self::secure_options(url, config, Some(credentials))?;
        Self::ensure_crypto_provider();
        let client =
            async_nats::connect_with_options(seed_list(url), options.ignore_discovered_servers())
                .await
                .map_err(|error| {
                    BusError::Connection(format!("SYS-account TLS connection failed: {error}"))
                })?;
        self.system_client
            .set(client)
            .map_err(|_| BusError::Other("SYS-account connection is already configured".into()))
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
    /// pending count. Its random name is a single ACL-matchable token; the
    /// worker ACL constrains the filter subject to that node's key prefix.
    async fn assignment_consumer(
        &self,
        prefix: &str,
        deliver_policy: DeliverPolicy,
    ) -> Result<(u64, jetstream::consumer::push::Ordered)> {
        let inbox = self.client.new_inbox();
        let name = inbox.rsplit('.').next().unwrap_or_default().to_owned();
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
        if let (Some(node_id), Some(tenant_id)) =
            (self.worker_node_id, self.worker_tenant_id.as_deref())
        {
            let task_prefix = subjects::kv_assignment_node_prefix(tenant_id, node_id);
            let ingress_prefix = subjects::kv_assignment_ingress_prefix(tenant_id, node_id);
            if !key.starts_with(&task_prefix) && !key.starts_with(&ingress_prefix) {
                return Err(BusError::Kv(format!(
                    "worker {tenant_id}/{node_id} cannot access assignment/status key {key}"
                )));
            }
        }
        Ok(())
    }

    fn check_worker_prefix(&self, prefix: &str) -> Result<()> {
        if let (Some(node_id), Some(tenant_id)) =
            (self.worker_node_id, self.worker_tenant_id.as_deref())
        {
            let task_prefix = subjects::kv_assignment_node_prefix(tenant_id, node_id);
            let ingress_prefix = subjects::kv_assignment_ingress_prefix(tenant_id, node_id);
            let network_prefix = subjects::kv_assignment_node_network_prefix(tenant_id, node_id);
            if prefix != task_prefix && prefix != ingress_prefix && prefix != network_prefix {
                return Err(BusError::Kv(format!(
                    "worker {tenant_id}/{node_id} may list only its own assignments"
                )));
            }
        }
        Ok(())
    }

    fn check_worker_watch(&self, prefix: &str) -> Result<()> {
        if let (Some(node_id), Some(tenant_id)) =
            (self.worker_node_id, self.worker_tenant_id.as_deref())
        {
            let task_prefix = subjects::kv_assignment_node_prefix(tenant_id, node_id);
            let ingress_prefix = subjects::kv_assignment_ingress_prefix(tenant_id, node_id);
            let wireguard_key = format!("{tenant_id}.node.{node_id}.wg");
            let network_prefix = subjects::kv_assignment_node_network_prefix(tenant_id, node_id);
            if prefix != task_prefix
                && prefix != ingress_prefix
                && prefix != wireguard_key
                && prefix != network_prefix
            {
                return Err(BusError::Kv(format!(
                    "worker {tenant_id}/{node_id} may watch only its own assignments"
                )));
            }
        }
        Ok(())
    }

    pub async fn ensure_tenant_resources(&self, tenant_id: &str) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("initialize tenant bus resources")?;
        if self.ensured_tenants.lock().unwrap().contains(tenant_id) {
            return Ok(());
        }
        for (bucket, age) in [
            (format!("MINGA_STATUS_{tenant_id}"), Duration::ZERO),
            (
                format!("MINGA_USAGE_{tenant_id}"),
                Duration::from_secs(24 * 3600),
            ),
            (format!("MINGA_BUDGET_{tenant_id}"), Duration::ZERO),
        ] {
            let _ = Self::setup_kv(&self.js, &bucket, age).await?;
        }
        let logs_name = format!("MINGA_LOGS_{tenant_id}");
        self.js
            .get_or_create_stream(StreamConfig {
                name: logs_name.clone(),
                subjects: vec![subjects::tenant_log_wildcard(tenant_id)],
                retention: RetentionPolicy::Limits,
                max_bytes: 512 * 1024 * 1024,
                max_age: Duration::from_secs(24 * 3600),
                ..Default::default()
            })
            .await
            .map_err(|error| BusError::Other(format!("{logs_name}: {error}")))?;
        let audit_name = format!("MINGA_AUDIT_{tenant_id}");
        self.js
            .get_or_create_stream(StreamConfig {
                name: audit_name.clone(),
                subjects: vec![format!("minga.{tenant_id}.audit")],
                retention: RetentionPolicy::Limits,
                max_bytes: 64 * 1024 * 1024,
                max_age: Duration::from_secs(365 * 24 * 3600),
                ..Default::default()
            })
            .await
            .map_err(|error| BusError::Other(format!("{audit_name}: {error}")))?;
        let usage_name = format!("MINGA_USAGE_{tenant_id}");
        self.js
            .get_or_create_stream(StreamConfig {
                name: usage_name.clone(),
                subjects: vec![format!("minga.{tenant_id}.usage.>")],
                retention: RetentionPolicy::Limits,
                max_bytes: DEFAULT_TENANT_STREAM_MAX_BYTES,
                max_age: DEFAULT_TENANT_STREAM_MAX_AGE,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .map_err(|error| BusError::Other(format!("{usage_name}: {error}")))?;
        self.ensured_tenants
            .lock()
            .unwrap()
            .insert(tenant_id.to_owned());
        Ok(())
    }
    pub async fn configure_tenant_quota(
        &self,
        tenant_id: &str,
        quotas: &minga_state::types::TenantQuotas,
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        self.ensure_tenant_resources(tenant_id).await?;
        let bytes = quotas
            .jetstream_bytes
            .map(|value| value.max(1).min(MAX_TENANT_STREAM_MAX_BYTES as u64) as i64);
        let age = quotas.jetstream_age_secs.map(|value| {
            Duration::from_secs(value.max(1).min(MAX_TENANT_STREAM_MAX_AGE.as_secs()))
        });
        let resources = [
            (
                format!("MINGA_LOGS_{tenant_id}"),
                DEFAULT_TENANT_STREAM_MAX_BYTES,
                DEFAULT_TENANT_STREAM_MAX_AGE,
                false,
            ),
            (
                format!("MINGA_AUDIT_{tenant_id}"),
                64 * 1024 * 1024,
                Duration::from_secs(365 * 24 * 3600),
                false,
            ),
            (
                format!("MINGA_USAGE_{tenant_id}"),
                DEFAULT_TENANT_STREAM_MAX_BYTES,
                DEFAULT_TENANT_STREAM_MAX_AGE,
                false,
            ),
            (
                format!("KV_MINGA_STATUS_{tenant_id}"),
                DEFAULT_TENANT_STREAM_MAX_BYTES,
                Duration::ZERO,
                true,
            ),
            (
                format!("KV_MINGA_USAGE_{tenant_id}"),
                DEFAULT_TENANT_STREAM_MAX_BYTES,
                Duration::from_secs(24 * 3600),
                true,
            ),
            (
                format!("KV_MINGA_BUDGET_{tenant_id}"),
                DEFAULT_TENANT_STREAM_MAX_BYTES,
                Duration::ZERO,
                true,
            ),
        ];
        for (name, default_bytes, default_age, kv) in resources {
            let mut stream = self
                .js
                .get_stream(&name)
                .await
                .map_err(|error| BusError::Other(format!("{name}: {error}")))?;
            let mut config = stream
                .info()
                .await
                .map_err(|error| BusError::Other(format!("{name} info: {error}")))?
                .config
                .clone();
            config.max_bytes = bytes.unwrap_or(default_bytes);
            config.max_age = age.unwrap_or(default_age);
            if kv || name.starts_with("MINGA_USAGE_") {
                config.duplicate_window = if config.max_age > Duration::ZERO {
                    config.max_age.min(Duration::from_secs(120))
                } else {
                    Duration::from_secs(120)
                };
            }
            self.js
                .update_stream(config)
                .await
                .map_err(|error| BusError::Other(format!("configure {name}: {error}")))?;
        }
        Ok(())
    }

    async fn tenant_status_store(&self, tenant_id: &str) -> Result<jetstream::kv::Store> {
        validate_tenant_id(tenant_id)?;
        if self
            .worker_tenant_id
            .as_deref()
            .is_some_and(|worker| worker != tenant_id)
        {
            return Err(BusError::Kv(
                "worker cannot access another tenant's status bucket".into(),
            ));
        }
        let bucket = format!("MINGA_STATUS_{tenant_id}");
        if self.worker_node_id.is_none() {
            self.ensure_tenant_resources(tenant_id).await?;
        }
        self.js
            .get_key_value(&bucket)
            .await
            .map_err(|error| BusError::Kv(format!("open {bucket}: {error}")))
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
    pub async fn connect_worker(url: &str, node_id: u64, tenant_id: &str) -> Result<Arc<Self>> {
        validate_tenant_id(tenant_id)?;
        Self::connect_worker_with_token_inner(url, node_id, tenant_id, None).await
    }

    pub async fn connect_worker_with_token(
        url: &str,
        token: &str,
        node_id: u64,
        tenant_id: &str,
    ) -> Result<Arc<Self>> {
        validate_tenant_id(tenant_id)?;
        Self::connect_worker_with_token_inner(url, node_id, tenant_id, Some(token)).await
    }

    async fn connect_worker_with_token_inner(
        url: &str,
        node_id: u64,
        tenant_id: &str,
        token: Option<&str>,
    ) -> Result<Arc<Self>> {
        let mut options = worker_options(async_nats::ConnectOptions::new(), node_id, tenant_id);
        if let Some(token) = token {
            options = options.token(token.to_string());
        }
        Self::ensure_crypto_provider();
        let client = options
            .connect(seed_list(url))
            .await
            .map_err(|error| BusError::Connection(error.to_string()))?;
        Self::from_worker_client(client, node_id, tenant_id).await
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
            worker_tenant_id: None,
            system_client: tokio::sync::OnceCell::new(),
            ensured_tenants: Default::default(),
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
            worker_tenant_id: None,
            system_client: tokio::sync::OnceCell::new(),
            ensured_tenants: Default::default(),
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
            worker_tenant_id: None,
            system_client: tokio::sync::OnceCell::new(),
            ensured_tenants: Default::default(),
        }))
    }

    /// Connect a worker over mTLS and open only already-created buckets.
    pub async fn connect_worker_secure(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        node_id: u64,
        tenant_id: &str,
    ) -> Result<Arc<Self>> {
        validate_tenant_id(tenant_id)?;
        let options = Self::secure_options(url, config, None)?;
        Self::ensure_crypto_provider();
        let client = async_nats::connect_with_options(
            seed_list(url),
            worker_options(options, node_id, tenant_id),
        )
        .await
        .map_err(|error| BusError::Connection(format!("TLS connection failed: {error}")))?;
        Self::from_worker_client(client, node_id, tenant_id).await
    }

    pub async fn connect_worker_secure_with_credentials(
        url: &str,
        config: &crate::tls::NatsTlsConfig,
        credentials: &str,
        node_id: u64,
        tenant_id: &str,
    ) -> Result<Arc<Self>> {
        validate_tenant_id(tenant_id)?;
        let options = Self::secure_options(url, config, Some(credentials))?;
        Self::ensure_crypto_provider();
        let client = async_nats::connect_with_options(
            seed_list(url),
            worker_options(options, node_id, tenant_id),
        )
        .await
        .map_err(|error| BusError::Connection(format!("TLS connection failed: {error}")))?;
        Self::from_worker_client(client, node_id, tenant_id).await
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
            worker_tenant_id: None,
            system_client: tokio::sync::OnceCell::new(),
            ensured_tenants: Default::default(),
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

    async fn from_worker_client(
        client: Client,
        node_id: u64,
        tenant_id: &str,
    ) -> Result<Arc<Self>> {
        validate_tenant_id(tenant_id)?;
        let js = jetstream::new(client.clone());
        let status_bucket = format!("MINGA_STATUS_{tenant_id}");
        let status_kv = js
            .get_key_value(&status_bucket)
            .await
            .map_err(|error| BusError::Kv(format!("open {status_bucket}: {error}")))?;
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
            worker_tenant_id: Some(tenant_id.to_owned()),
            system_client: tokio::sync::OnceCell::new(),
            ensured_tenants: Default::default(),
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
        // Leases expire on their own; these two buckets keep keys forever.
        for kv in [&status_kv, &assignments_kv] {
            purge_legacy_keys(kv).await;
        }
        Ok((status_kv, assignments_kv, leases_kv))
    }

    async fn setup_streams(js: &JetStream) -> Result<()> {
        // MINGA_EVENTS — status reports and lifecycle events.
        let event_subjects = EVENT_SUBJECTS.map(str::to_string).to_vec();
        let events = StreamConfig {
            name: "MINGA_EVENTS".to_string(),
            subjects: event_subjects.clone(),
            retention: RetentionPolicy::Limits,
            max_age: Duration::from_secs(72 * 3600),
            ..Default::default()
        };
        let mut stream = js
            .get_or_create_stream(events)
            .await
            .map_err(|e| BusError::Other(format!("MINGA_EVENTS: {e}")))?;
        // A stream created before tenant-first subjects keeps its old filter;
        // without this update every tenant publish fails with "no stream found".
        let mut config = stream
            .info()
            .await
            .map_err(|e| BusError::Other(format!("MINGA_EVENTS info: {e}")))?
            .config
            .clone();
        if config.subjects != event_subjects {
            config.subjects = event_subjects;
            js.update_stream(config)
                .await
                .map_err(|e| BusError::Other(format!("MINGA_EVENTS subjects: {e}")))?;
        }

        // Tenant logs use `minga.<tenant>.log.>`; the legacy stream below
        // stays outside the tenant namespace so these filters cannot overlap.
        let logs = StreamConfig {
            name: "MINGA_LOGS".to_string(),
            subjects: vec!["legacy.minga.log.>".to_string()],
            retention: RetentionPolicy::Limits,
            max_bytes: 512 * 1024 * 1024,
            max_age: Duration::from_secs(24 * 3600),
            ..Default::default()
        };
        let mut stream = js
            .get_or_create_stream(logs)
            .await
            .map_err(|error| BusError::Other(format!("MINGA_LOGS: {error}")))?;
        let mut config = stream
            .info()
            .await
            .map_err(|error| BusError::Other(format!("MINGA_LOGS info: {error}")))?
            .config
            .clone();
        if config.subjects != ["legacy.minga.log.>"] {
            config.subjects = vec!["legacy.minga.log.>".to_string()];
            js.update_stream(config)
                .await
                .map_err(|error| BusError::Other(format!("MINGA_LOGS subjects: {error}")))?;
        }

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

    async fn publish_usage(&self, event: &UsageEvent) -> Result<()> {
        validate_tenant_id(&event.tenant_id)?;
        if event.service_id.is_empty()
            || event.request_id.is_empty()
            || event
                .request_id
                .bytes()
                .any(|byte| matches!(byte, b'\r' | b'\n'))
        {
            return Err(BusError::Publish(
                "invalid usage service or request ID".into(),
            ));
        }
        match self.worker_tenant_id.as_deref() {
            Some(worker_tenant) if worker_tenant == event.tenant_id => {}
            Some(_) => {
                return Err(BusError::Publish(
                    "worker cannot publish usage for another tenant".into(),
                ));
            }
            None => self.ensure_tenant_resources(&event.tenant_id).await?,
        }
        let subject = crate::usage_subject(&event.tenant_id, &event.service_id)?;
        let payload = serde_json::to_vec(event)
            .map_err(|error| BusError::Serialization(error.to_string()))?;
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", event.request_id.as_str());
        let ack = self
            .js
            .publish_with_headers(subject, headers, Bytes::from(payload))
            .await
            .map_err(|error| BusError::Publish(error.to_string()))?
            .await
            .map_err(|error| BusError::Publish(format!("usage ack: {error}")))?;
        if !ack.duplicate {
            minga_telemetry::METRICS.record_agent_usage(
                &event.tenant_id,
                event.input_tokens,
                event.output_tokens,
                event.cost_micros.unwrap_or_default(),
            );
        }
        Ok(())
    }

    async fn usage_events(
        &self,
        tenant_id: &str,
        since_unix_ms: u64,
        customer: Option<&str>,
        limit: usize,
    ) -> Result<Vec<UsageEvent>> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("query usage")?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut events = VecDeque::new();
        visit_usage(&self.js, tenant_id, since_unix_ms, |event| {
            if customer.is_none_or(|customer| event.customer_id == customer) {
                events.push_back(event);
                if events.len() > limit {
                    events.pop_front();
                }
            }
        })
        .await?;
        Ok(events.into())
    }

    async fn usage_totals(&self, since_unix_ms: u64) -> Result<Vec<(String, UsageTotals)>> {
        self.require_manager("aggregate usage")?;
        use futures::TryStreamExt;

        let mut streams = self.js.streams();
        let mut totals = Vec::new();
        while let Some(info) = streams
            .try_next()
            .await
            .map_err(|error| BusError::Subscribe(format!("list streams: {error}")))?
        {
            let Some(tenant_id) = info.config.name.strip_prefix("MINGA_USAGE_") else {
                continue;
            };
            if validate_tenant_id(tenant_id).is_err() {
                continue;
            }
            let mut total = UsageTotals::default();
            visit_usage(&self.js, tenant_id, since_unix_ms, |event| {
                total.request_count = total.request_count.saturating_add(1);
                total.input_tokens = total.input_tokens.saturating_add(event.input_tokens);
                total.output_tokens = total.output_tokens.saturating_add(event.output_tokens);
                total.cost_micros = total
                    .cost_micros
                    .saturating_add(event.cost_micros.unwrap_or_default());
            })
            .await?;
            totals.push((tenant_id.to_owned(), total));
        }
        Ok(totals)
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

    async fn append_audit(&self, record: crate::AuditRecord) -> Result<()> {
        let tenant_id = record.tenant_id.as_str();
        validate_tenant_id(tenant_id)?;
        self.require_manager("append audit records")?;
        self.ensure_tenant_resources(tenant_id).await?;
        let payload = bincode::serde::encode_to_vec(&record, bincode::config::standard())
            .map_err(|error| BusError::Serialization(error.to_string()))?;
        self.js
            .publish(format!("minga.{tenant_id}.audit"), Bytes::from(payload))
            .await
            .map_err(|error| BusError::Publish(error.to_string()))?
            .await
            .map_err(|error| BusError::Publish(format!("audit ack: {error}")))?;
        Ok(())
    }

    async fn list_audit(&self, tenant_id: &str, limit: usize) -> Result<Vec<crate::AuditRecord>> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("list tenant audit records")?;
        self.ensure_tenant_resources(tenant_id).await?;
        let stream = self
            .js
            .get_stream(format!("MINGA_AUDIT_{tenant_id}"))
            .await
            .map_err(|error| BusError::Subscribe(format!("audit stream: {error}")))?;
        let mut consumer = stream
            .create_consumer(PullConsumerConfig {
                filter_subjects: vec![format!("minga.{tenant_id}.audit")],
                ack_policy: AckPolicy::None,
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .map_err(|error| BusError::Subscribe(format!("audit consumer: {error}")))?;
        let mut remaining = consumer
            .info()
            .await
            .map_err(|error| BusError::Subscribe(format!("audit info: {error}")))?
            .num_pending as usize;
        let mut records = VecDeque::new();
        while remaining > 0 {
            let mut batch = consumer
                .fetch()
                .max_messages(remaining.min(200))
                .expires(Duration::from_secs(5))
                .messages()
                .await
                .map_err(|error| BusError::Subscribe(format!("audit fetch: {error}")))?;
            let mut received = 0usize;
            while let Some(message) = batch.next().await {
                let message = message
                    .map_err(|error| BusError::Subscribe(format!("audit message: {error}")))?;
                received += 1;
                let (record, _) = bincode::serde::decode_from_slice::<crate::AuditRecord, _>(
                    &message.payload,
                    bincode::config::standard(),
                )
                .map_err(|error| BusError::Serialization(error.to_string()))?;
                if record.tenant_id != tenant_id {
                    return Err(BusError::Kv(
                        "audit record tenant does not match its stream".into(),
                    ));
                }
                records.push_back(record);
                if limit > 0 {
                    while records.len() > limit {
                        records.pop_front();
                    }
                }
            }
            if received == 0 {
                break;
            }
            remaining = remaining.saturating_sub(received);
        }
        Ok(records.into_iter().collect())
    }
    async fn publish_tenant_event(
        &self,
        tenant_id: &str,
        subject: &str,
        payload: &[u8],
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("publish events")?;
        if !subject.starts_with(&format!("minga.{tenant_id}.task.")) {
            return Err(BusError::Publish(
                "event subject is outside tenant namespace".into(),
            ));
        }
        self.js
            .publish(subject.to_owned(), Bytes::copy_from_slice(payload))
            .await
            .map_err(|error| BusError::Publish(format!("MINGA_EVENTS: {error}")))?
            .await
            .map_err(|error| BusError::Publish(format!("MINGA_EVENTS ack: {error}")))?;
        Ok(())
    }

    async fn subscribe_tenant_events(&self, tenant_id: &str) -> Result<EventStream> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("subscribe to tenant events")?;
        let stream = self
            .js
            .get_stream("MINGA_EVENTS")
            .await
            .map_err(|error| BusError::Subscribe(format!("get MINGA_EVENTS: {error}")))?;
        let consumer = stream
            .create_consumer(bounded(PullConsumerConfig {
                filter_subjects: vec![subjects::tenant_event_wildcard(tenant_id)],
                deliver_policy: DeliverPolicy::New,
                ..Default::default()
            }))
            .await
            .map_err(|error| BusError::Subscribe(format!("tenant event consumer: {error}")))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|error| BusError::Subscribe(format!("tenant event messages: {error}")))?;
        Ok(Box::pin(messages.filter_map(|message| async move {
            match message {
                Ok(message) => {
                    if let Err(error) = message.ack().await {
                        tracing::warn!(error = %error, "tenant event ack failed");
                    }
                    Some(BusEvent {
                        subject: message.subject.to_string(),
                        payload: message.payload.to_vec(),
                        timestamp: now_millis(),
                    })
                }
                Err(error) => {
                    tracing::warn!(error = %error, "tenant event stream error");
                    None
                }
            }
        })))
    }
    async fn tenant_event_history(&self, tenant_id: &str, limit: usize) -> Result<Vec<BusEvent>> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("read retained tenant events")?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let stream = self
            .js
            .get_stream("MINGA_EVENTS")
            .await
            .map_err(|error| BusError::Subscribe(format!("get MINGA_EVENTS: {error}")))?;
        // shortcut: scans the tenant's retained events (72h) per call, like
        // tenant_log_history; seek by start sequence if that retention grows.
        let mut consumer = stream
            .create_consumer(PullConsumerConfig {
                filter_subjects: vec![subjects::tenant_event_wildcard(tenant_id)],
                ack_policy: AckPolicy::None,
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                BusError::Subscribe(format!("tenant event history consumer: {error}"))
            })?;
        let mut remaining = consumer
            .info()
            .await
            .map_err(|error| BusError::Subscribe(format!("tenant event history info: {error}")))?
            .num_pending as usize;
        let mut events = VecDeque::new();
        while remaining > 0 {
            let mut batch = consumer
                .fetch()
                .max_messages(remaining.min(200))
                .expires(Duration::from_secs(5))
                .messages()
                .await
                .map_err(|error| {
                    BusError::Subscribe(format!("tenant event history fetch: {error}"))
                })?;
            let mut received = 0usize;
            while let Some(message) = batch.next().await {
                let message = message.map_err(|error| {
                    BusError::Subscribe(format!("tenant event history message: {error}"))
                })?;
                received += 1;
                if !message
                    .subject
                    .starts_with(&format!("minga.{tenant_id}.task."))
                {
                    return Err(BusError::Subscribe(
                        "tenant event subject does not match its filter".into(),
                    ));
                }
                // The publish time, not the read time.
                let timestamp = message
                    .info()
                    .map(|info| (info.published.unix_timestamp_nanos() / 1_000_000) as u64)
                    .map_err(|error| {
                        BusError::Subscribe(format!("tenant event history metadata: {error}"))
                    })?;
                events.push_back(BusEvent {
                    subject: message.subject.to_string(),
                    payload: message.payload.to_vec(),
                    timestamp,
                });
                while events.len() > limit {
                    events.pop_front();
                }
            }
            if received == 0 {
                break;
            }
            remaining = remaining.saturating_sub(received);
        }
        Ok(events.into_iter().collect())
    }
    async fn configure_tenant_quota(
        &self,
        tenant_id: &str,
        quotas: &minga_state::types::TenantQuotas,
    ) -> Result<()> {
        NatsBus::configure_tenant_quota(self, tenant_id, quotas).await
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
        if self.worker_node_id.is_some() {
            return Err(BusError::Kv(
                "workers must write tenant-scoped status".into(),
            ));
        }
        self.status_kv
            .put(key.to_string(), Bytes::copy_from_slice(value))
            .await
            .map_err(|error| BusError::Kv(format!("put {key}: {error}")))?;
        Ok(())
    }

    async fn get_status(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.require_manager("get status")?;
        Ok(self
            .status_kv
            .get(key.to_string())
            .await
            .map_err(|error| BusError::Kv(format!("get {key}: {error}")))?
            .map(|bytes| bytes.to_vec()))
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
    async fn put_tenant_status(&self, tenant_id: &str, key: &str, value: &[u8]) -> Result<()> {
        if let (Some(node_id), Some(worker_tenant)) =
            (self.worker_node_id, self.worker_tenant_id.as_deref())
        {
            if tenant_id != worker_tenant || !key.starts_with(&format!("node.{node_id}.task.")) {
                return Err(BusError::Kv(format!(
                    "worker {worker_tenant}/{node_id} cannot write status key {tenant_id}/{key}"
                )));
            }
        }
        self.tenant_status_store(tenant_id)
            .await?
            .put(key.to_owned(), Bytes::copy_from_slice(value))
            .await
            .map_err(|error| BusError::Kv(format!("put {tenant_id}/{key}: {error}")))?;
        Ok(())
    }

    async fn get_tenant_status(&self, tenant_id: &str, key: &str) -> Result<Option<Vec<u8>>> {
        if let (Some(node_id), Some(worker_tenant)) =
            (self.worker_node_id, self.worker_tenant_id.as_deref())
        {
            if tenant_id != worker_tenant || !key.starts_with(&format!("node.{node_id}.task.")) {
                return Err(BusError::Kv(format!(
                    "worker {worker_tenant}/{node_id} cannot read status key {tenant_id}/{key}"
                )));
            }
        }
        Ok(self
            .tenant_status_store(tenant_id)
            .await?
            .get(key.to_owned())
            .await
            .map_err(|error| BusError::Kv(format!("get {tenant_id}/{key}: {error}")))?
            .map(|value| value.to_vec()))
    }

    async fn list_tenant_status(&self, tenant_id: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.require_manager("list tenant statuses")?;
        let store = self.tenant_status_store(tenant_id).await?;
        let mut keys = store
            .keys()
            .await
            .map_err(|error| BusError::Kv(format!("list {tenant_id} statuses: {error}")))?;
        let mut entries = Vec::new();
        while let Some(key) = keys.next().await {
            let key =
                key.map_err(|error| BusError::Kv(format!("list {tenant_id} status key: {error}")))?;
            if let Some(value) = store
                .get(key.clone())
                .await
                .map_err(|error| BusError::Kv(format!("get {tenant_id}/{key}: {error}")))?
            {
                entries.push((key, value.to_vec()));
            }
        }
        Ok(entries)
    }

    async fn list_all_tenant_status(
        &self,
        tenant_ids: &[String],
    ) -> Result<Vec<(String, String, Vec<u8>)>> {
        self.require_manager("list all tenant statuses")?;
        let mut entries = Vec::new();
        for tenant_id in tenant_ids {
            for (key, value) in self.list_tenant_status(tenant_id).await? {
                entries.push((tenant_id.clone(), key, value));
            }
        }
        Ok(entries)
    }

    async fn refresh_lease(&self, node_id: u64, registration: &[u8]) -> Result<()> {
        self.refresh_tenant_lease(minga_state::types::DEFAULT_TENANT_ID, node_id, registration)
            .await
    }

    async fn refresh_tenant_lease(
        &self,
        tenant_id: &str,
        node_id: u64,
        registration: &[u8],
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        if self
            .worker_node_id
            .is_some_and(|worker_id| worker_id != node_id)
            || self
                .worker_tenant_id
                .as_deref()
                .is_some_and(|worker| worker != tenant_id)
        {
            return Err(BusError::Lease(format!(
                "worker cannot refresh lease {tenant_id}/{node_id}"
            )));
        }
        let key = subjects::kv_lease(tenant_id, node_id);
        self.leases_kv
            .put(key, Bytes::copy_from_slice(registration))
            .await
            .map_err(|error| BusError::Lease(format!("refresh {tenant_id}/{node_id}: {error}")))?;
        Ok(())
    }

    async fn is_alive(&self, node_id: u64) -> Result<bool> {
        self.is_tenant_node_alive(minga_state::types::DEFAULT_TENANT_ID, node_id)
            .await
    }

    async fn is_tenant_node_alive(&self, tenant_id: &str, node_id: u64) -> Result<bool> {
        validate_tenant_id(tenant_id)?;
        if self
            .worker_node_id
            .is_some_and(|worker_id| worker_id != node_id)
            || self
                .worker_tenant_id
                .as_deref()
                .is_some_and(|worker| worker != tenant_id)
        {
            return Err(BusError::Lease(format!(
                "worker cannot query lease {tenant_id}/{node_id}"
            )));
        }
        Ok(self
            .leases_kv
            .get(subjects::kv_lease(tenant_id, node_id))
            .await
            .map_err(|error| BusError::Lease(format!("get {tenant_id}/{node_id}: {error}")))?
            .is_some())
    }

    async fn list_alive_nodes(&self) -> Result<HashSet<u64>> {
        Ok(self
            .list_tenant_alive_nodes()
            .await?
            .into_iter()
            .map(|(_, node)| node)
            .collect())
    }

    async fn list_tenant_alive_nodes(&self) -> Result<HashSet<(String, u64)>> {
        self.require_manager("list leases")?;
        let mut keys = self
            .leases_kv
            .keys()
            .await
            .map_err(|error| BusError::Lease(format!("list: {error}")))?;
        let mut nodes = HashSet::new();
        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| BusError::Lease(format!("list key: {error}")))?;
            if let Some((tenant_id, node_id)) = subjects::parse_kv_lease(&key) {
                nodes.insert((tenant_id.to_owned(), node_id));
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

    async fn update_account_claim(&self, account_jwt: &str) -> Result<()> {
        self.require_manager("update NATS account claims")?;
        let system_client = self.system_client.get().ok_or_else(|| {
            BusError::Connection("SYS-account credentials are not configured".to_string())
        })?;
        if account_jwt.trim().is_empty() {
            return Err(BusError::Publish("account JWT cannot be empty".to_string()));
        }
        let response = system_client
            .request(
                "$SYS.REQ.CLAIMS.UPDATE".to_string(),
                Bytes::copy_from_slice(account_jwt.as_bytes()),
            )
            .await
            .map_err(|error| BusError::Connection(format!("account claim update: {error}")))?;
        let response = String::from_utf8_lossy(&response.payload);
        if !response.contains("jwt updated") {
            return Err(BusError::Other(format!(
                "full resolver did not confirm account claim update: {response}"
            )));
        }
        Ok(())
    }
    async fn check_tenant_request_quota(
        &self,
        tenant_id: &str,
        window_start: u64,
        limit: u64,
    ) -> Result<bool> {
        self.require_manager("update tenant API quotas")?;
        minga_state::types::Tenant::validate_id(tenant_id)
            .map_err(|error| BusError::Other(format!("invalid tenant ID: {error}")))?;
        if limit == 0 {
            return Ok(false);
        }
        self.ensure_tenant_resources(tenant_id).await?;
        let bucket = format!("MINGA_USAGE_{tenant_id}");
        let store = self
            .js
            .get_key_value(&bucket)
            .await
            .map_err(|error| BusError::Kv(format!("open {bucket}: {error}")))?;
        let key = format!("internal.api-rate.{window_start}");
        for _ in 0..16 {
            let entry = store
                .entry(key.clone())
                .await
                .map_err(|error| BusError::Kv(format!("{bucket}/{key}: {error}")))?;
            match entry {
                Some(entry) => {
                    let count = std::str::from_utf8(&entry.value)
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .ok_or_else(|| {
                            BusError::Kv(format!("{bucket}/{key} has an invalid counter"))
                        })?;
                    if count >= limit {
                        return Ok(false);
                    }
                    if store
                        .update(&key, Bytes::from((count + 1).to_string()), entry.revision)
                        .await
                        .is_ok()
                    {
                        return Ok(true);
                    }
                }
                None => {
                    if store.create(&key, Bytes::from_static(b"1")).await.is_ok() {
                        return Ok(true);
                    }
                }
            }
        }
        Err(BusError::Kv(format!(
            "{bucket}/{key} compare-and-set contention exceeded"
        )))
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
        let tenant_id = self
            .worker_tenant_id
            .as_deref()
            .unwrap_or(minga_state::types::DEFAULT_TENANT_ID);
        self.publish_tenant_node_log(tenant_id, node_id, task_id, entry)
            .await
    }
    async fn publish_tenant_node_log(
        &self,
        tenant_id: &str,
        node_id: u64,
        task_id: &str,
        entry: &LogEntry,
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        if self
            .worker_tenant_id
            .as_deref()
            .is_some_and(|worker| worker != tenant_id)
        {
            return Err(BusError::Publish(
                "worker cannot publish to another tenant's log stream".into(),
            ));
        }
        if self.worker_node_id.is_some_and(|worker| worker != node_id) {
            return Err(BusError::Publish(format!(
                "worker cannot publish logs for node {node_id}"
            )));
        }
        if entry.task_id != task_id {
            return Err(BusError::Publish(
                "log subject task ID does not match entry".into(),
            ));
        }
        if self.worker_node_id.is_none() {
            self.ensure_tenant_resources(tenant_id).await?;
        }
        let subject = subjects::node_task_log(tenant_id, node_id, task_id);
        let payload = entry.to_bytes()?;
        self.js
            .publish(subject, Bytes::from(payload))
            .await
            .map_err(|error| BusError::Publish(format!("tenant log: {error}")))?
            .await
            .map_err(|error| BusError::Publish(format!("tenant log ack: {error}")))?;
        Ok(())
    }

    async fn subscribe_logs(&self, task_id: &str) -> Result<LogEventStream> {
        self.subscribe_tenant_logs(minga_state::types::DEFAULT_TENANT_ID, task_id)
            .await
    }
    async fn subscribe_tenant_logs(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<LogEventStream> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("subscribe to tenant logs")?;
        self.ensure_tenant_resources(tenant_id).await?;
        let stream_name = format!("MINGA_LOGS_{tenant_id}");
        let stream = self
            .js
            .get_stream(&stream_name)
            .await
            .map_err(|error| BusError::Subscribe(format!("get {stream_name}: {error}")))?;
        let filter = if task_id == "*" {
            subjects::tenant_log_wildcard(tenant_id)
        } else {
            format!("minga.{tenant_id}.log.node.*.{task_id}")
        };
        let consumer = stream
            .create_consumer(bounded(PullConsumerConfig {
                filter_subjects: vec![filter],
                deliver_policy: DeliverPolicy::New,
                ..Default::default()
            }))
            .await
            .map_err(|error| {
                BusError::Subscribe(format!("create {stream_name} consumer: {error}"))
            })?;
        let messages = consumer
            .messages()
            .await
            .map_err(|error| BusError::Subscribe(format!("{stream_name} messages: {error}")))?;
        let client = self.client.clone();
        let log_stream = messages.filter_map(move |message| {
            let client = client.clone();
            async move {
                match message {
                    Ok(message) => match LogEntry::from_bytes(&message.payload) {
                        Ok(entry) => {
                            if let Err(error) = message.ack().await {
                                tracing::warn!(error = %error, "tenant log ack failed");
                            }
                            Some(entry)
                        }
                        Err(error) => {
                            reject(&client, &message, "tenant logs", &error).await;
                            None
                        }
                    },
                    Err(error) => {
                        tracing::warn!(error = %error, "tenant log stream error");
                        None
                    }
                }
            }
        });
        Ok(Box::pin(log_stream))
    }

    async fn log_history(&self, task_id: &str, tail: usize) -> Result<Vec<LogEntry>> {
        self.tenant_log_history(minga_state::types::DEFAULT_TENANT_ID, task_id, tail)
            .await
    }
    async fn tenant_log_history(
        &self,
        tenant_id: &str,
        task_id: &str,
        tail: usize,
    ) -> Result<Vec<LogEntry>> {
        validate_tenant_id(tenant_id)?;
        self.require_manager("read tenant log history")?;
        self.ensure_tenant_resources(tenant_id).await?;
        let stream_name = format!("MINGA_LOGS_{tenant_id}");
        let stream = self
            .js
            .get_stream(&stream_name)
            .await
            .map_err(|error| BusError::Subscribe(format!("get {stream_name}: {error}")))?;
        let filter = if task_id == "*" {
            subjects::tenant_log_wildcard(tenant_id)
        } else {
            format!("minga.{tenant_id}.log.node.*.{task_id}")
        };
        let mut consumer = stream
            .create_consumer(PullConsumerConfig {
                filter_subjects: vec![filter],
                ack_policy: AckPolicy::None,
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                BusError::Subscribe(format!("create {stream_name} consumer: {error}"))
            })?;
        let mut remaining = consumer
            .info()
            .await
            .map_err(|error| BusError::Subscribe(format!("{stream_name} consumer info: {error}")))?
            .num_pending as usize;
        let wanted = if tail == 0 {
            remaining
        } else {
            tail.min(remaining)
        };
        let mut entries = VecDeque::with_capacity(wanted);
        while remaining > 0 {
            let mut batch = consumer
                .fetch()
                .max_messages(remaining.min(200))
                .expires(Duration::from_secs(5))
                .messages()
                .await
                .map_err(|error| BusError::Subscribe(format!("fetch {stream_name}: {error}")))?;
            let mut received = 0;
            while let Some(message) = batch.next().await {
                let message = message
                    .map_err(|error| BusError::Subscribe(format!("{stream_name}: {error}")))?;
                received += 1;
                if let Ok(entry) = LogEntry::from_bytes(&message.payload) {
                    if wanted > 0 && entries.len() == wanted {
                        entries.pop_front();
                    }
                    entries.push_back(entry);
                }
            }
            if received == 0 {
                break;
            }
            remaining = remaining.saturating_sub(received);
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
/// Whether `key` uses the layout from before tenant-first names
/// (`node.<id>.…`). A tenant named `node` writes `node.node.<id>.…`, so the
/// second segment being numeric tells them apart.
fn is_legacy_key(key: &str) -> bool {
    let mut parts = key.split('.');
    parts.next() == Some("node")
        && parts
            .next()
            .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
}

/// Purges keys no reader uses since tenant-first names: task status and
/// assignments from before the upgrade would otherwise stay forever. This is
/// housekeeping, so a failure is logged and the connection proceeds.
async fn purge_legacy_keys(kv: &jetstream::kv::Store) {
    let keys: Vec<String> = match kv.keys().await {
        Ok(keys) => {
            keys.filter_map(|key| async move { key.ok() })
                .collect()
                .await
        }
        Err(error) => {
            tracing::warn!(bucket = %kv.name, %error, "cannot list keys to purge legacy entries");
            return;
        }
    };
    let mut purged = 0usize;
    for key in keys.iter().filter(|key| is_legacy_key(key)) {
        match kv.purge(key).await {
            Ok(()) => purged += 1,
            Err(error) => {
                tracing::warn!(bucket = %kv.name, %key, %error, "cannot purge legacy key")
            }
        }
    }
    if purged > 0 {
        tracing::info!(bucket = %kv.name, purged, "purged pre-tenant keys");
    }
}

/// Worker connections use a tenant- and node-scoped inbox; their ACL denies
/// the shared `_INBOX.>`.
fn worker_options(
    options: async_nats::ConnectOptions,
    node_id: u64,
    tenant_id: &str,
) -> async_nats::ConnectOptions {
    options
        .ignore_discovered_servers()
        .custom_inbox_prefix(crate::subjects::worker_inbox_prefix(tenant_id, node_id))
}

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

fn validate_tenant_id(tenant_id: &str) -> Result<()> {
    minga_state::types::Tenant::validate_id(tenant_id)
        .map_err(|error| BusError::Other(format!("invalid tenant ID: {error}")))
}

#[cfg(test)]
mod legacy_key_tests {
    use super::is_legacy_key;

    #[test]
    fn only_pre_tenant_node_keys_are_legacy() {
        assert!(is_legacy_key("node.7.task.web"));
        assert!(is_legacy_key("node.7"));
        assert!(is_legacy_key("node.12.wg"));
        assert!(!is_legacy_key("node.node.7.task.web"), "tenant named node");
        assert!(!is_legacy_key("default.node.7.task.web"));
        assert!(!is_legacy_key("internal.node-join.7"));
        assert!(!is_legacy_key("node."));
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
