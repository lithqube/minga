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
    LogEntry, LogEventStream, RequestStream, Result,
};

/// Default lease TTL: 30 seconds.
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// Channel capacity for events and KV notifications.
const CHANNEL_CAPACITY: usize = 1024;

/// How many log entries the in-memory bus retains per task.
const LOG_HISTORY_CAPACITY: usize = 1000;
/// Maximum retained tenant events for deterministic in-memory diagnostics.
const TENANT_EVENT_HISTORY_CAPACITY: usize = 4096;

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn marker(generation: u64, deleting: bool) -> crate::TenantMarker {
    crate::TenantMarker {
        generation,
        deleting,
        since_unix_ms: unix_ms(),
    }
}

fn fenced(tenant_id: &str) -> BusError {
    BusError::Fenced(format!(
        "the delete of tenant {tenant_id} was taken over or finished by another manager"
    ))
}

// ---------------------------------------------------------------------------
// MemoryBus
// ---------------------------------------------------------------------------

/// In-process event bus. All state is in memory.
pub struct MemoryBus {
    /// Broadcast channel for events.
    events_tx: broadcast::Sender<BusEvent>,
    tenant_events: Mutex<VecDeque<(String, BusEvent)>>,
    /// Test hook: the next `delete_tenant_incarnation` signals `.0`, then waits for `.1`.
    tenant_markers: Mutex<HashMap<String, (crate::TenantMarker, u64)>>,
    marker_revision: AtomicU64,
    delete_gate: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    /// Assignment KV store: key → latest manager-authored value.
    assignments: Mutex<HashMap<String, Vec<u8>>>,
    /// Broadcast channel for assignment KV notifications.
    assignment_tx: broadcast::Sender<KvEvent>,
    /// KV store: key → latest value.
    kv: Mutex<HashMap<String, Vec<u8>>>,
    /// Broadcast channel for KV change notifications.
    kv_tx: broadcast::Sender<KvEvent>,
    /// Liveness leases: `(tenant_id, node_id)` → last refresh and payload.
    leases: Mutex<HashMap<(String, u64), Lease>>,
    /// Broadcast channel for lease changes.
    lease_tx: broadcast::Sender<KvEvent>,
    /// Lease TTL.
    lease_ttl: Duration,
    /// Request handlers: subject → sender for incoming requests.
    request_handlers: Mutex<HashMap<String, mpsc::Sender<BusRequest>>>,
    /// Pending replies: reply_subject → oneshot sender.
    pending_replies: Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    /// Counter for generating unique reply subjects.
    reply_counter: AtomicU64,
    logs_tx: broadcast::Sender<LogEntry>,
    tenant_logs_tx: broadcast::Sender<(String, LogEntry)>,
    /// Retained log entries per task, oldest first.
    log_history: Mutex<HashMap<String, VecDeque<LogEntry>>>,
    tenant_log_history: Mutex<HashMap<(String, String), VecDeque<LogEntry>>>,
    tenant_status: Mutex<HashMap<(String, String), Vec<u8>>>,
    tenant_audit: Mutex<HashMap<String, VecDeque<crate::AuditRecord>>>,
    /// Account JWTs accepted through the deterministic resolver simulation.
    account_claim_updates: Mutex<Vec<String>>,
    /// Cluster-shared request counters keyed by tenant and window.
    tenant_api_requests: Mutex<HashMap<(String, u64), u64>>,
    /// Retained usage events, deduplicated by tenant and request ID.
    usage: Mutex<Vec<crate::UsageEvent>>,
    usage_ids: Mutex<HashSet<(String, String)>>,
    /// Tenants whose bus resources were configured and not yet deleted.
    tenant_resources: Mutex<HashSet<String>>,
    /// Tenants whose status listing fails, set by [`MemoryBus::fail_tenant`].
    failing_status: Mutex<HashSet<String>>,
    /// Tenants whose assignment writes fail, set by [`MemoryBus::fail_tenant`]
    /// or [`MemoryBus::fail_tenant_assignments`].
    failing_assignments: Mutex<HashSet<String>>,
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
        let (tenant_logs_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Arc::new(Self {
            events_tx,
            tenant_resources: Mutex::new(HashSet::new()),
            tenant_events: Mutex::new(VecDeque::new()),
            tenant_markers: Mutex::new(HashMap::new()),
            marker_revision: AtomicU64::new(0),
            delete_gate: Mutex::new(None),
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
            tenant_logs_tx,
            log_history: Mutex::new(HashMap::new()),
            tenant_log_history: Mutex::new(HashMap::new()),
            tenant_status: Mutex::new(HashMap::new()),
            tenant_audit: Mutex::new(HashMap::new()),
            account_claim_updates: Mutex::new(Vec::new()),
            tenant_api_requests: Mutex::new(HashMap::new()),
            usage: Mutex::new(Vec::new()),
            usage_ids: Mutex::new(HashSet::new()),
            failing_status: Mutex::new(HashSet::new()),
            failing_assignments: Mutex::new(HashSet::new()),
        })
    }

    fn check_fence(&self, tenant_id: &str, fence: crate::DeleteFence) -> Result<()> {
        match self.tenant_markers.lock().unwrap().get(tenant_id) {
            Some((m, revision)) if m.deleting && *revision == fence.revision => Ok(()),
            _ => Err(fenced(tenant_id)),
        }
    }

    /// Pauses the next `delete_tenant_incarnation` call before it deletes
    /// anything, as a manager stalled or cut off mid-delete would be. Returns
    /// (signalled when the call is paused, call it to let the call go on).
    pub fn pause_next_tenant_delete(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let gate = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        *self.delete_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    /// Makes every status listing and assignment write or delete of one tenant
    /// fail with a KV error, as a JetStream fault on that tenant's buckets
    /// would, until [`MemoryBus::heal_tenant`]. Other tenants are unaffected.
    pub fn fail_tenant(&self, tenant_id: &str) {
        self.failing_status
            .lock()
            .unwrap()
            .insert(tenant_id.to_owned());
        self.fail_tenant_assignments(tenant_id);
    }

    /// Like [`MemoryBus::fail_tenant`], but only the tenant's assignment writes
    /// and deletes fail; its status still reads.
    pub fn fail_tenant_assignments(&self, tenant_id: &str) {
        self.failing_assignments
            .lock()
            .unwrap()
            .insert(tenant_id.to_owned());
    }

    /// Ends the faults set for a tenant.
    pub fn heal_tenant(&self, tenant_id: &str) {
        self.failing_status.lock().unwrap().remove(tenant_id);
        self.failing_assignments.lock().unwrap().remove(tenant_id);
    }

    fn check_status_fault(&self, tenant_id: &str) -> Result<()> {
        Self::check_fault(&self.failing_status, tenant_id)
    }

    fn check_assignment_fault(&self, key: &str) -> Result<()> {
        // Assignment keys start with the tenant, and a tenant ID has no dot.
        Self::check_fault(
            &self.failing_assignments,
            key.split('.').next().unwrap_or_default(),
        )
    }

    fn check_fault(failing: &Mutex<HashSet<String>>, tenant_id: &str) -> Result<()> {
        if failing.lock().unwrap().contains(tenant_id) {
            return Err(BusError::Kv(format!(
                "injected fault for tenant {tenant_id}"
            )));
        }
        Ok(())
    }
    /// Account JWTs accepted by the in-memory resolver simulation.
    pub fn account_claim_updates(&self) -> Vec<String> {
        self.account_claim_updates.lock().unwrap().clone()
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
    async fn subscribe_tenant_events(&self, tenant_id: &str) -> Result<EventStream> {
        validate_tenant_id(tenant_id)?;
        self.subscribe_events(&[&subjects::tenant_event_wildcard(tenant_id)])
            .await
    }
    async fn tenant_event_history(&self, tenant_id: &str, limit: usize) -> Result<Vec<BusEvent>> {
        validate_tenant_id(tenant_id)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let events = self.tenant_events.lock().unwrap();
        let matching = events
            .iter()
            .filter(|(tenant, _)| tenant == tenant_id)
            .map(|(_, event)| event.clone())
            .collect::<Vec<_>>();
        Ok(matching[matching.len().saturating_sub(limit)..].to_vec())
    }
    async fn configure_tenant_quota(
        &self,
        tenant_id: &str,
        _quotas: &minga_state::types::TenantQuotas,
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        self.tenant_resources
            .lock()
            .unwrap()
            .insert(tenant_id.to_owned());
        Ok(())
    }
    async fn tenant_resources_exist(&self, tenant_id: &str) -> Result<bool> {
        validate_tenant_id(tenant_id)?;
        // Configured, or holding data some write kept without configuring it.
        Ok(self.tenant_resources.lock().unwrap().contains(tenant_id)
            || self.tenant_audit.lock().unwrap().contains_key(tenant_id)
            || self
                .tenant_status
                .lock()
                .unwrap()
                .keys()
                .any(|(tenant, _)| tenant == tenant_id)
            || self
                .tenant_events
                .lock()
                .unwrap()
                .iter()
                .any(|(tenant, _)| tenant == tenant_id)
            || self
                .usage
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.tenant_id == tenant_id)
            || self
                .tenant_log_history
                .lock()
                .unwrap()
                .keys()
                .any(|(tenant, _)| tenant == tenant_id)
            || self
                .leases
                .lock()
                .unwrap()
                .keys()
                .any(|(tenant, _)| tenant == tenant_id)
            || self
                .tenant_api_requests
                .lock()
                .unwrap()
                .keys()
                .any(|(tenant, _)| tenant == tenant_id)
            || self
                .usage_ids
                .lock()
                .unwrap()
                .iter()
                .any(|(tenant, _)| tenant == tenant_id)
            || self
                .assignments
                .lock()
                .unwrap()
                .keys()
                .any(|key| key.starts_with(&format!("{tenant_id}."))))
    }
    async fn claim_tenant(&self, tenant_id: &str, generation: u64) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        let mut markers = self.tenant_markers.lock().unwrap();
        if crate::plan_claim(tenant_id, &markers.get(tenant_id).cloned(), generation)? {
            let revision = self.marker_revision.fetch_add(1, Ordering::SeqCst) + 1;
            markers.insert(tenant_id.to_owned(), (marker(generation, false), revision));
        }
        Ok(())
    }
    async fn begin_tenant_delete(
        &self,
        tenant_id: &str,
        generation: Option<u64>,
    ) -> Result<Option<crate::DeleteFence>> {
        validate_tenant_id(tenant_id)?;
        let exists = self.tenant_resources_exist(tenant_id).await?;
        let mut markers = self.tenant_markers.lock().unwrap();
        let plan = crate::plan_delete(
            tenant_id,
            &markers.get(tenant_id).cloned(),
            generation,
            exists,
            unix_ms(),
        )?;
        Ok(plan.map(|(generation, _)| {
            let revision = self.marker_revision.fetch_add(1, Ordering::SeqCst) + 1;
            markers.insert(tenant_id.to_owned(), (marker(generation, true), revision));
            crate::DeleteFence {
                generation,
                revision,
            }
        }))
    }
    async fn delete_tenant_incarnation(
        &self,
        tenant_id: &str,
        fence: crate::DeleteFence,
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        let gate = self.delete_gate.lock().unwrap().take();
        if let Some((reached, release)) = gate {
            reached.notify_one();
            release.notified().await;
        }
        self.check_fence(tenant_id, fence)?;
        self.delete_tenant_resources(tenant_id).await?;
        let mut markers = self.tenant_markers.lock().unwrap();
        match markers.get(tenant_id) {
            Some((_, revision)) if *revision == fence.revision => {
                markers.remove(tenant_id);
                Ok(())
            }
            _ => Err(fenced(tenant_id)),
        }
    }
    async fn delete_tenant_resources(&self, tenant_id: &str) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        self.tenant_resources.lock().unwrap().remove(tenant_id);
        self.tenant_events
            .lock()
            .unwrap()
            .retain(|(tenant, _)| tenant != tenant_id);
        self.tenant_audit.lock().unwrap().remove(tenant_id);
        self.tenant_status
            .lock()
            .unwrap()
            .retain(|(tenant, _), _| tenant != tenant_id);
        self.tenant_log_history
            .lock()
            .unwrap()
            .retain(|(tenant, _), _| tenant != tenant_id);
        self.tenant_api_requests
            .lock()
            .unwrap()
            .retain(|(tenant, _), _| tenant != tenant_id);
        self.usage
            .lock()
            .unwrap()
            .retain(|event| event.tenant_id != tenant_id);
        self.usage_ids
            .lock()
            .unwrap()
            .retain(|(tenant, _)| tenant != tenant_id);
        self.leases
            .lock()
            .unwrap()
            .retain(|(tenant, _), _| tenant != tenant_id);
        // Assignment keys are tenant-first: `<tenant>.node.<n>.…`.
        let prefix = format!("{tenant_id}.");
        self.assignments
            .lock()
            .unwrap()
            .retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }
    async fn publish_tenant_event(
        &self,
        tenant_id: &str,
        subject: &str,
        payload: &[u8],
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        if !subject.starts_with(&format!("minga.{tenant_id}.task.")) {
            return Err(BusError::Publish(
                "event subject is outside tenant namespace".into(),
            ));
        }
        let event = BusEvent {
            subject: subject.to_owned(),
            payload: payload.to_vec(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        let mut events = self.tenant_events.lock().unwrap();
        events.push_back((tenant_id.to_owned(), event.clone()));
        while events.len() > TENANT_EVENT_HISTORY_CAPACITY {
            events.pop_front();
        }
        drop(events);
        let _ = self.events_tx.send(event);
        Ok(())
    }
    async fn put_assignment(&self, key: &str, value: &[u8]) -> Result<()> {
        self.check_assignment_fault(key)?;
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
        self.check_assignment_fault(key)?;
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
    async fn put_tenant_status(&self, tenant_id: &str, key: &str, value: &[u8]) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        self.tenant_status
            .lock()
            .unwrap()
            .insert((tenant_id.to_owned(), key.to_owned()), value.to_vec());
        Ok(())
    }

    async fn get_tenant_status(&self, tenant_id: &str, key: &str) -> Result<Option<Vec<u8>>> {
        validate_tenant_id(tenant_id)?;
        Ok(self
            .tenant_status
            .lock()
            .unwrap()
            .get(&(tenant_id.to_owned(), key.to_owned()))
            .cloned())
    }

    async fn list_tenant_status(&self, tenant_id: &str) -> Result<Vec<(String, Vec<u8>)>> {
        validate_tenant_id(tenant_id)?;
        self.check_status_fault(tenant_id)?;
        Ok(self
            .tenant_status
            .lock()
            .unwrap()
            .iter()
            .filter(|((tenant, _), _)| tenant == tenant_id)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn list_all_tenant_status(
        &self,
        tenant_ids: &[String],
    ) -> Result<Vec<(String, String, Vec<u8>)>> {
        for tenant in tenant_ids {
            validate_tenant_id(tenant)?;
            self.check_status_fault(tenant)?;
        }
        let allowed: HashSet<&str> = tenant_ids.iter().map(String::as_str).collect();
        Ok(self
            .tenant_status
            .lock()
            .unwrap()
            .iter()
            .filter(|((tenant, _), _)| allowed.contains(tenant.as_str()))
            .map(|((tenant, key), value)| (tenant.clone(), key.clone(), value.clone()))
            .collect())
    }

    async fn append_audit(&self, record: crate::AuditRecord) -> Result<()> {
        validate_tenant_id(&record.tenant_id)?;
        let mut audit = self.tenant_audit.lock().unwrap();
        let records = audit.entry(record.tenant_id.clone()).or_default();
        records.push_back(record);
        while records.len() > LOG_HISTORY_CAPACITY {
            records.pop_front();
        }
        Ok(())
    }

    async fn list_audit(&self, tenant_id: &str, limit: usize) -> Result<Vec<crate::AuditRecord>> {
        validate_tenant_id(tenant_id)?;
        let audit = self.tenant_audit.lock().unwrap();
        let Some(records) = audit.get(tenant_id) else {
            return Ok(Vec::new());
        };
        let skip = if limit > 0 {
            records.len().saturating_sub(limit)
        } else {
            0
        };
        Ok(records.iter().skip(skip).cloned().collect())
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
        self.leases.lock().unwrap().insert(
            (tenant_id.to_owned(), node_id),
            Lease {
                refreshed_at: Instant::now(),
                registration: registration.to_vec(),
            },
        );
        let _ = self.lease_tx.send(KvEvent {
            key: subjects::kv_lease(tenant_id, node_id),
            value: Some(registration.to_vec()),
            op: KvOp::Put,
        });
        Ok(())
    }

    async fn is_alive(&self, node_id: u64) -> Result<bool> {
        self.is_tenant_node_alive(minga_state::types::DEFAULT_TENANT_ID, node_id)
            .await
    }

    async fn is_tenant_node_alive(&self, tenant_id: &str, node_id: u64) -> Result<bool> {
        validate_tenant_id(tenant_id)?;
        let leases = self.leases.lock().unwrap();
        Ok(leases
            .get(&(tenant_id.to_owned(), node_id))
            .is_some_and(|lease| lease.refreshed_at.elapsed() < self.lease_ttl))
    }

    async fn list_alive_nodes(&self) -> Result<HashSet<u64>> {
        Ok(self
            .list_tenant_alive_nodes()
            .await?
            .into_iter()
            .map(|(_, id)| id)
            .collect())
    }

    async fn list_tenant_alive_nodes(&self) -> Result<HashSet<(String, u64)>> {
        let leases = self.leases.lock().unwrap();
        Ok(leases
            .iter()
            .filter(|(_, lease)| lease.refreshed_at.elapsed() < self.lease_ttl)
            .map(|((tenant_id, node_id), _)| (tenant_id.clone(), *node_id))
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

    async fn update_account_claim(&self, account_jwt: &str) -> Result<()> {
        if account_jwt.trim().is_empty() {
            return Err(BusError::Other(
                "account JWT update cannot be empty".to_string(),
            ));
        }
        self.account_claim_updates
            .lock()
            .unwrap()
            .push(account_jwt.to_string());
        Ok(())
    }
    async fn check_tenant_request_quota(
        &self,
        tenant_id: &str,
        window_start: u64,
        limit: u64,
    ) -> Result<bool> {
        minga_state::types::Tenant::validate_id(tenant_id)
            .map_err(|error| BusError::Other(format!("invalid tenant ID: {error}")))?;
        if limit == 0 {
            return Ok(false);
        }
        let mut requests = self.tenant_api_requests.lock().unwrap();
        let count = requests
            .entry((tenant_id.to_owned(), window_start))
            .or_default();
        if *count >= limit {
            return Ok(false);
        }
        *count += 1;
        Ok(true)
    }

    async fn publish_usage(&self, event: &crate::UsageEvent) -> Result<()> {
        minga_state::types::Tenant::validate_id(&event.tenant_id)
            .map_err(|error| BusError::Other(format!("invalid usage tenant: {error}")))?;
        if event.service_id.is_empty() || event.request_id.is_empty() {
            return Err(BusError::Other(
                "invalid usage service or request ID".into(),
            ));
        }
        let key = (event.tenant_id.clone(), event.request_id.clone());
        if !self.usage_ids.lock().unwrap().insert(key) {
            return Ok(());
        }
        self.usage.lock().unwrap().push(event.clone());
        minga_telemetry::METRICS.record_agent_usage(
            &event.tenant_id,
            event.input_tokens,
            event.output_tokens,
            event.cost_micros.unwrap_or_default(),
        );
        Ok(())
    }

    async fn usage_events(
        &self,
        tenant_id: &str,
        since_unix_ms: u64,
        customer: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::UsageEvent>> {
        minga_state::types::Tenant::validate_id(tenant_id)
            .map_err(|error| BusError::Other(format!("invalid usage tenant: {error}")))?;
        let matching: Vec<_> = self
            .usage
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                event.tenant_id == tenant_id
                    && event.occurred_at_unix_ms >= since_unix_ms
                    && customer.is_none_or(|customer| event.customer_id == customer)
            })
            .cloned()
            .collect();
        // Newest `limit` rows, oldest first, like the NATS bus.
        Ok(matching[matching.len().saturating_sub(limit)..].to_vec())
    }

    async fn usage_totals(&self, since_unix_ms: u64) -> Result<Vec<(String, crate::UsageTotals)>> {
        let events = self.usage.lock().unwrap();
        let mut totals = std::collections::BTreeMap::<String, crate::UsageTotals>::new();
        for event in events
            .iter()
            .filter(|event| event.occurred_at_unix_ms >= since_unix_ms)
        {
            let total = totals.entry(event.tenant_id.clone()).or_default();
            total.request_count = total.request_count.saturating_add(1);
            total.input_tokens = total.input_tokens.saturating_add(event.input_tokens);
            total.output_tokens = total.output_tokens.saturating_add(event.output_tokens);
            total.cost_micros = total
                .cost_micros
                .saturating_add(event.cost_micros.unwrap_or_default());
        }
        Ok(totals.into_iter().collect())
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
    async fn publish_tenant_node_log(
        &self,
        tenant_id: &str,
        _node_id: u64,
        task_id: &str,
        entry: &LogEntry,
    ) -> Result<()> {
        validate_tenant_id(tenant_id)?;
        if entry.task_id != task_id {
            return Err(BusError::Publish(
                "log subject task ID does not match entry".to_string(),
            ));
        }
        let mut history = self.tenant_log_history.lock().unwrap();
        let entries = history
            .entry((tenant_id.to_owned(), task_id.to_owned()))
            .or_default();
        entries.push_back(entry.clone());
        while entries.len() > LOG_HISTORY_CAPACITY {
            entries.pop_front();
        }
        let _ = self
            .tenant_logs_tx
            .send((tenant_id.to_owned(), entry.clone()));
        Ok(())
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
    async fn subscribe_tenant_logs(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<LogEventStream> {
        validate_tenant_id(tenant_id)?;
        let mut rx = self.tenant_logs_tx.subscribe();
        let tenant_id = tenant_id.to_owned();
        let task_id = task_id.to_owned();
        let (tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok((tenant, entry))
                        if tenant == tenant_id && (task_id == "*" || entry.task_id == task_id) =>
                    {
                        if tx.send(entry).await.is_err() {
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
    async fn tenant_log_history(
        &self,
        tenant_id: &str,
        task_id: &str,
        tail: usize,
    ) -> Result<Vec<LogEntry>> {
        validate_tenant_id(tenant_id)?;
        let history = self.tenant_log_history.lock().unwrap();
        let mut entries = Vec::new();
        if task_id == "*" {
            for ((tenant, _), task_entries) in history.iter() {
                if tenant == tenant_id {
                    entries.extend(task_entries.iter().cloned());
                }
            }
        } else if let Some(task_entries) = history.get(&(tenant_id.to_owned(), task_id.to_owned()))
        {
            entries.extend(task_entries.iter().cloned());
        }
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

fn validate_tenant_id(tenant_id: &str) -> Result<()> {
    minga_state::types::Tenant::validate_id(tenant_id)
        .map_err(|error| BusError::Other(format!("invalid tenant ID: {error}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subjects;
    use futures::StreamExt;

    #[tokio::test]
    async fn a_tenants_retained_data_counts_as_resources_and_deleting_clears_all_of_it() {
        let bus = MemoryBus::new();
        for tenant in ["gone", "kept"] {
            bus.append_audit(crate::AuditRecord {
                tenant_id: tenant.into(),
                actor: "operator".into(),
                action: "tenant.test".into(),
                resource_path: format!("/tenants/{tenant}"),
                occurred_at_unix: 1,
            })
            .await
            .unwrap();
            bus.put_tenant_status(tenant, "node.1.task.t", b"running")
                .await
                .unwrap();
            bus.refresh_tenant_lease(tenant, 1, b"registration")
                .await
                .unwrap();
            bus.put_assignment(&format!("{tenant}.node.1.task.t"), b"assigned")
                .await
                .unwrap();
        }
        // Nothing was configured, yet data is retained, so the ID is not free.
        assert!(bus.tenant_resources_exist("gone").await.unwrap());

        bus.delete_tenant_resources("gone").await.unwrap();
        assert!(!bus.tenant_resources_exist("gone").await.unwrap());
        assert!(bus.list_audit("gone", 10).await.unwrap().is_empty());
        assert!(bus.list_tenant_status("gone").await.unwrap().is_empty());
        assert!(!bus.is_tenant_node_alive("gone", 1).await.unwrap());
        assert_eq!(
            bus.get_assignment("gone.node.1.task.t").await.unwrap(),
            None
        );
        // Another tenant keeps everything.
        assert!(bus
            .get_assignment("kept.node.1.task.t")
            .await
            .unwrap()
            .is_some());
        assert!(bus.tenant_resources_exist("kept").await.unwrap());
        assert_eq!(bus.list_audit("kept", 10).await.unwrap().len(), 1);
        assert!(bus.is_tenant_node_alive("kept", 1).await.unwrap());
    }

    /// Each kind of retained data alone is enough for the ID to be taken, and
    /// deleting the tenant frees it.
    #[tokio::test]
    async fn each_kind_of_retained_data_alone_keeps_a_tenant_id_taken() {
        let bus = MemoryBus::new();
        bus.check_tenant_request_quota("counter", 100, 10)
            .await
            .unwrap();
        bus.publish_tenant_node_log(
            "logs",
            1,
            "web.1.abc",
            &LogEntry::stdout("web.1.abc", "line"),
        )
        .await
        .unwrap();
        for tenant in ["counter", "logs"] {
            assert!(
                bus.tenant_resources_exist(tenant).await.unwrap(),
                "{tenant}"
            );
            bus.delete_tenant_resources(tenant).await.unwrap();
            assert!(
                !bus.tenant_resources_exist(tenant).await.unwrap(),
                "{tenant}"
            );
        }
        // The counter is gone too: the window starts from zero for a new owner.
        assert!(bus
            .check_tenant_request_quota("counter", 100, 1)
            .await
            .unwrap());
        assert!(!bus
            .check_tenant_request_quota("counter", 100, 1)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn tenant_event_history_is_filtered_and_bounded() {
        let bus = MemoryBus::new();
        bus.publish_tenant_event("tenant-a", "minga.tenant-a.task.one.event", b"first")
            .await
            .unwrap();
        bus.publish_tenant_event("tenant-b", "minga.tenant-b.task.two.event", b"private")
            .await
            .unwrap();
        bus.publish_tenant_event("tenant-a", "minga.tenant-a.task.three.event", b"last")
            .await
            .unwrap();

        let events = bus.tenant_event_history("tenant-a", 1).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subject, "minga.tenant-a.task.three.event");
        assert_eq!(events[0].payload, b"last");
        assert!(bus
            .tenant_event_history("tenant-a", 0)
            .await
            .unwrap()
            .is_empty());
    }

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
        let prefix = "node.7.task.";
        let mut watch = bus.watch_assignments(prefix).await.unwrap();

        bus.put_assignment("node.8.task.other", b"other")
            .await
            .unwrap();
        bus.put_assignment("node.7.task.one", b"assignment")
            .await
            .unwrap();

        assert_eq!(
            bus.list_assignments(prefix).await.unwrap(),
            vec![("node.7.task.one".to_string(), b"assignment".to_vec())]
        );
        let event = tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.key, "node.7.task.one");
        assert_eq!(event.value, Some(b"assignment".to_vec()));

        bus.delete_assignment("node.7.task.one").await.unwrap();
        assert_eq!(bus.get_assignment("node.7.task.one").await.unwrap(), None);
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
        assert!(!bus.is_alive(1).await.unwrap());

        // Refresh → alive.
        bus.refresh_lease(1, b"registration").await.unwrap();
        assert!(bus.is_alive(1).await.unwrap());

        // Wait for expiry.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!bus.is_alive(1).await.unwrap());
    }

    #[tokio::test]
    async fn test_lease_refresh_extends() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(200));

        bus.refresh_lease(1, b"registration").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        bus.refresh_lease(1, b"registration").await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        // 250ms since first refresh, but only 150ms since second.
        assert!(bus.is_alive(1).await.unwrap());
    }

    #[tokio::test]
    async fn same_node_id_in_different_tenants_has_distinct_leases() {
        let bus = MemoryBus::new();
        bus.refresh_tenant_lease("acme", 7, b"acme registration")
            .await
            .unwrap();
        bus.refresh_tenant_lease("beta", 7, b"beta registration")
            .await
            .unwrap();

        assert!(bus.is_tenant_node_alive("acme", 7).await.unwrap());
        assert!(bus.is_tenant_node_alive("beta", 7).await.unwrap());
        assert_eq!(
            bus.list_tenant_alive_nodes().await.unwrap(),
            HashSet::from([("acme".to_owned(), 7), ("beta".to_owned(), 7)])
        );
    }

    #[tokio::test]
    async fn same_node_id_assignments_and_status_are_isolated_by_tenant() {
        let bus = MemoryBus::new();
        let acme_key = subjects::kv_assignment_task("acme", 7, "task");
        let beta_key = subjects::kv_assignment_task("beta", 7, "task");
        bus.put_assignment(&acme_key, b"acme").await.unwrap();
        bus.put_assignment(&beta_key, b"beta").await.unwrap();
        assert_eq!(
            bus.get_assignment(&acme_key).await.unwrap(),
            Some(b"acme".to_vec())
        );
        assert_eq!(
            bus.get_assignment(&beta_key).await.unwrap(),
            Some(b"beta".to_vec())
        );
        bus.put_tenant_status("acme", "node.7.task.task", b"acme")
            .await
            .unwrap();
        bus.put_tenant_status("beta", "node.7.task.task", b"beta")
            .await
            .unwrap();
        assert_eq!(
            bus.get_tenant_status("acme", "node.7.task.task")
                .await
                .unwrap(),
            Some(b"acme".to_vec())
        );
        assert_eq!(
            bus.get_tenant_status("beta", "node.7.task.task")
                .await
                .unwrap(),
            Some(b"beta".to_vec())
        );
    }

    #[tokio::test]
    async fn test_lease_per_node() {
        let bus = MemoryBus::with_lease_ttl(Duration::from_millis(100));

        bus.refresh_lease(1, b"registration").await.unwrap();
        assert!(bus.is_alive(1).await.unwrap());
        assert!(!bus.is_alive(2).await.unwrap());
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
    #[tokio::test]
    async fn tenant_request_quota_is_atomic_and_isolated_by_window_and_tenant() {
        let bus = MemoryBus::new();
        for _ in 0..3 {
            assert!(bus
                .check_tenant_request_quota("tenant-a", 100, 3)
                .await
                .unwrap());
        }
        assert!(!bus
            .check_tenant_request_quota("tenant-a", 100, 3)
            .await
            .unwrap());
        assert!(bus
            .check_tenant_request_quota("tenant-b", 100, 3)
            .await
            .unwrap());
        assert!(bus
            .check_tenant_request_quota("tenant-a", 101, 3)
            .await
            .unwrap());
    }
    #[tokio::test]
    async fn tenant_status_and_audit_are_isolated() {
        let bus = MemoryBus::new();
        bus.put_tenant_status("alpha", "node.1.task.x", b"running")
            .await
            .unwrap();
        bus.put_tenant_status("beta", "node.1.task.x", b"failed")
            .await
            .unwrap();
        assert_eq!(
            bus.get_tenant_status("alpha", "node.1.task.x")
                .await
                .unwrap(),
            Some(b"running".to_vec())
        );
        assert_eq!(bus.list_tenant_status("alpha").await.unwrap().len(), 1);
        assert_eq!(
            bus.list_all_tenant_status(&["alpha".to_string()])
                .await
                .unwrap()
                .len(),
            1
        );
        let record = crate::AuditRecord {
            tenant_id: "alpha".into(),
            actor: "node-1".into(),
            action: "task.start".into(),
            resource_path: "/tasks/task.x".into(),
            occurred_at_unix: 1,
        };
        bus.append_audit(record.clone()).await.unwrap();
        assert_eq!(bus.list_audit("alpha", 100).await.unwrap(), vec![record]);
        assert!(bus.list_audit("beta", 100).await.unwrap().is_empty());
        bus.publish_tenant_node_log("alpha", 1, "task.x", &LogEntry::stdout("task.x", b"alpha"))
            .await
            .unwrap();
        bus.publish_tenant_node_log("beta", 1, "task.x", &LogEntry::stdout("task.x", b"beta"))
            .await
            .unwrap();
        let alpha_logs = bus.tenant_log_history("alpha", "task.x", 10).await.unwrap();
        assert_eq!(alpha_logs.len(), 1);
        assert_eq!(alpha_logs[0].data, b"alpha");
        assert!(bus
            .tenant_log_history("other", "task.x", 10)
            .await
            .unwrap()
            .is_empty());
        assert!(bus
            .get_tenant_status("../beta", "node.1.task.x")
            .await
            .is_err());
    }

    fn usage_event(
        request_id: &str,
        tenant_id: &str,
        customer_id: &str,
        time: u64,
    ) -> crate::UsageEvent {
        crate::UsageEvent {
            request_id: request_id.into(),
            tenant_id: tenant_id.into(),
            service_id: "svc".into(),
            task_id: "task".into(),
            customer_id: customer_id.into(),
            route: "/v1/chat".into(),
            requested_model: "asked".into(),
            answered_model: "served".into(),
            input_tokens: 11,
            output_tokens: 7,
            cache_hit: false,
            latency_ms: 42,
            status: 200,
            occurred_at_unix_ms: time,
            cost_micros: Some(9),
        }
    }

    #[tokio::test]
    async fn usage_query_filters_tenant_customer_time_and_limit_and_deduplicates() {
        let bus = MemoryBus::new();
        let event = usage_event("req-1", "tenant-a", "customer-a", 100);
        bus.publish_usage(&event).await.unwrap();
        bus.publish_usage(&event).await.unwrap();
        bus.publish_usage(&usage_event("req-2", "tenant-a", "customer-b", 100))
            .await
            .unwrap();
        bus.publish_usage(&usage_event("req-1", "tenant-b", "customer-a", 100))
            .await
            .unwrap();
        bus.publish_usage(&usage_event("req-3", "tenant-a", "customer-a", 50))
            .await
            .unwrap();

        let rows = bus
            .usage_events("tenant-a", 100, Some("customer-a"), 1)
            .await
            .unwrap();
        assert_eq!(rows, vec![event]);
        let totals = bus.usage_totals(100).await.unwrap();
        assert_eq!(totals.len(), 2);
        assert_eq!(totals[0].0, "tenant-a");
        assert_eq!(totals[0].1.request_count, 2);

        let newer = usage_event("req-4", "tenant-a", "customer-a", 200);
        bus.publish_usage(&newer).await.unwrap();
        let rows = bus
            .usage_events("tenant-a", 100, Some("customer-a"), 1)
            .await
            .unwrap();
        assert_eq!(rows, vec![newer], "limit keeps the newest rows");
    }

    #[test]
    fn usage_event_serialization_keeps_integer_cost_and_no_prompt_body() {
        let value = serde_json::to_value(usage_event("req-1", "tenant-a", "c", 1)).unwrap();
        assert_eq!(value["cost_micros"], 9);
        assert!(value.get("prompt").is_none());
        assert!(serde_json::from_value::<crate::UsageEvent>(value).is_ok());
    }
}
