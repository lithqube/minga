//! Node self-registration: a node publishes its identity in a bus lease
//! and refreshes it periodically. A leader registers previously unseen
//! leases in Raft; placement reads liveness from the bus.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{BusError, EventBus, KvEvent, KvOp};
use minga_state::join_token::JoinTokenKey;
use minga_state::types::{
    unix_now, ClusterNode, NodeId, NodeRole, NodeStatus, ResourceSpec, NODE_RENEW_INTERVAL_SECS,
};
use minga_state::StateStore;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

/// Who this node is, as announced to the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    #[serde(default = "default_tenant_id")]
    pub tenant_id: String,
    pub node_id: NodeId,
    /// Address other nodes can reach this node's Raft RPC on.
    pub address: String,
    /// User-defined labels (e.g. `gpu=true`), usable in constraints.
    pub labels: HashMap<String, String>,
    /// Compute capacity, e.g. from [`host_capacity`].
    pub capacity: ResourceSpec,
    pub role: NodeRole,
    /// TCP/UDP ports owned by this node's Minga services.
    #[serde(default)]
    pub reserved_ports: Vec<minga_state::types::ReservedPort>,
    /// WireGuard public key. The private key is never registered.
    #[serde(default)]
    pub wg_public_key: Option<String>,
    /// Reachable host:port; `None` means this node is behind NAT.
    #[serde(default)]
    pub wg_endpoint: Option<String>,
    /// Short-lived, operator-issued token binding this node ID to a tenant.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_token: Option<String>,
}

fn default_tenant_id() -> String {
    minga_state::types::DEFAULT_TENANT_ID.to_owned()
}

impl NodeIdentity {
    /// Convert this advertised identity into the legacy default-tenant record.
    pub fn cluster_node(&self) -> ClusterNode {
        self.cluster_node_for_tenant(minga_state::types::DEFAULT_TENANT_ID)
    }

    /// Convert this identity into a record for a tenant verified by its join token.
    pub fn cluster_node_for_tenant(&self, tenant_id: &str) -> ClusterNode {
        ClusterNode {
            id: self.node_id,
            address: self.address.clone(),
            labels: self.labels.clone(),
            status: NodeStatus::Active,
            capacity: self.capacity.clone(),
            wg_public_key: self.wg_public_key.clone(),
            wg_endpoint: self.wg_endpoint.clone(),
            overlay_ip: None,
            tenant_id: tenant_id.to_owned(),
            last_seen_unix: None,
            role: self.role,
            reserved_ports: self.reserved_ports.clone(),
            nats_user_public_key: None,
        }
    }
}

/// Detects this host's CPU count and physical memory via `sysconf`.
///
/// GPUs are not auto-detected; advertise them with a label and an
/// explicit capacity instead. Falls back to 1 CPU / 0 memory (and
/// excludes nothing) if `sysconf` fails — a warning is logged.
pub fn host_capacity() -> ResourceSpec {
    // SAFETY: sysconf is thread-safe and these constants are valid query
    // keys on Linux and macOS.
    let cpus = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };

    if cpus <= 0 || pages <= 0 || page_size <= 0 {
        tracing::warn!(cpus, pages, page_size, "host capacity detection failed");
    }
    ResourceSpec {
        cpus: if cpus > 0 { cpus as f64 } else { 1.0 },
        memory_bytes: if pages > 0 && page_size > 0 {
            (pages as u64).saturating_mul(page_size as u64)
        } else {
            0
        },
        gpus: 0,
    }
}

/// An active node membership: refreshes its bus lease until [`leave`](Self::leave).
pub struct NodeMembership {
    node_id: NodeId,
    renew_task: tokio::task::JoinHandle<()>,
}

impl NodeMembership {
    /// Publishes the registration immediately, then refreshes it every 10 seconds.
    pub async fn start(bus: Arc<dyn EventBus>, identity: NodeIdentity) -> Result<Self, BusError> {
        Self::start_with_interval(bus, identity, Duration::from_secs(NODE_RENEW_INTERVAL_SECS))
            .await
    }

    async fn start_with_interval(
        bus: Arc<dyn EventBus>,
        identity: NodeIdentity,
        interval: Duration,
    ) -> Result<Self, BusError> {
        let registration = minga_state::codec::encode_wire(&identity)
            .map_err(|error| BusError::Serialization(error.to_string()))?;
        bus.refresh_tenant_lease(&identity.tenant_id, identity.node_id, &registration)
            .await?;

        tracing::info!(
            tenant_id = %identity.tenant_id,
            node_id = identity.node_id,
            address = %identity.address,
            cpus = identity.capacity.cpus,
            memory_bytes = identity.capacity.memory_bytes,
            labels = ?identity.labels,
            "node lease published"
        );

        let tenant_id = identity.tenant_id;
        let node_id = identity.node_id;
        let renew_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(error) = bus
                    .refresh_tenant_lease(&tenant_id, node_id, &registration)
                    .await
                {
                    tracing::warn!(tenant_id, node_id, error = %error, "lease refresh failed");
                }
            }
        });
        Ok(Self {
            node_id,
            renew_task,
        })
    }

    /// Stops renewing; the server expires the lease after its TTL.
    pub async fn leave(self) {
        self.renew_task.abort();
        tracing::info!(node_id = self.node_id, "node lease renewal stopped");
    }
}

/// Registers first-seen node leases in Raft while this process is leader.
/// Every manager watches the lease bucket so a leadership change does not
/// lose pending registrations. Existing Raft records are never overwritten;
/// manager-owned status keys keep removed IDs from being re-registered after
/// a manager restart.
pub async fn run_node_registration(
    store: Arc<StateStore>,
    bus: Arc<dyn EventBus>,
    join_token_key: Option<Arc<JoinTokenKey>>,
) {
    let mut registered = loop {
        match load_registered_nodes(&store, bus.as_ref()).await {
            Ok(nodes) => break nodes,
            Err(error) => {
                tracing::warn!(error = %error, "cannot load registered nodes");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    let mut joined = loop {
        let tenant_ids = match store.list_tenants().await {
            Ok(tenants) => tenants
                .into_iter()
                .map(|tenant| tenant.id)
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(error = %error, "cannot list tenants for joined-node load");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        match load_joined_nodes(bus.as_ref(), &tenant_ids).await {
            Ok(nodes) => break nodes,
            Err(error) => {
                tracing::warn!(error = %error, "cannot load joined nodes");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    let mut pending = HashMap::new();
    let mut retry = tokio::time::interval(Duration::from_secs(1));

    loop {
        let mut leases = match bus.watch_leases().await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(error = %error, "cannot watch node leases");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        loop {
            tokio::select! {
                event = leases.next() => {
                    let Some(event) = event else {
                        tracing::warn!("node lease watch ended; reconnecting");
                        break;
                    };
                    refresh_joined_node(bus.as_ref(), &store, &event, &mut joined).await;
                    queue_node_registration_with_join(
                        &store,
                        event,
                        &mut registered,
                        &mut pending,
                        &joined,
                        join_token_key.as_deref(),
                    ).await;
                    if store.is_leader().await {
                        register_pending_nodes_with_join(
                            &store,
                            bus.as_ref(),
                            &mut registered,
                            &mut pending,
                            &mut joined,
                        ).await;
                    }
                }
                _ = retry.tick() => {
                    if store.is_leader().await {
                        register_pending_nodes_with_join(
                            &store,
                            bus.as_ref(),
                            &mut registered,
                            &mut pending,
                            &mut joined,
                        ).await;
                    }
                }
            }
        }
    }
}

const REGISTRATION_STATUS_PREFIX: &str = "internal.node-registration.";
const REGISTERED_STATUS: &[u8] = b"registered";

fn registration_status_key(node_id: NodeId) -> String {
    format!("{REGISTRATION_STATUS_PREFIX}{node_id}")
}

async fn load_registered_nodes(
    store: &StateStore,
    bus: &dyn EventBus,
) -> std::result::Result<HashSet<NodeId>, String> {
    let nodes = store
        .list_nodes()
        .await
        .map_err(|error| error.to_string())?;
    let existing: HashSet<_> = nodes.iter().map(|node| node.id).collect();
    let tenant_ids = store
        .list_tenants()
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|tenant| tenant.id)
        .collect::<Vec<_>>();
    let statuses = bus
        .list_all_tenant_status(&tenant_ids)
        .await
        .map_err(|error| error.to_string())?;
    let mut registered = existing.clone();
    for (_, key, _) in statuses {
        if let Some(node_id) = key
            .strip_prefix(REGISTRATION_STATUS_PREFIX)
            .and_then(|id| id.parse::<NodeId>().ok())
        {
            registered.insert(node_id);
        }
    }

    for node in &nodes {
        let key = registration_status_key(node.id);
        if bus
            .get_tenant_status(&node.tenant_id, &key)
            .await
            .map_err(|error| error.to_string())?
            .is_none()
        {
            bus.put_tenant_status(&node.tenant_id, &key, REGISTERED_STATUS)
                .await
                .map_err(|error| error.to_string())?;
        }
    }

    Ok(registered)
}

use minga_bus::subjects::{
    kv_node_join as joined_status_key, NODE_JOIN_PREFIX as JOINED_STATUS_PREFIX,
};

async fn load_joined_nodes(
    bus: &dyn EventBus,
    tenant_ids: &[String],
) -> std::result::Result<HashMap<NodeId, String>, String> {
    let mut joined = HashMap::new();
    for (tenant_id, key, value) in bus
        .list_all_tenant_status(tenant_ids)
        .await
        .map_err(|error| error.to_string())?
    {
        let Some(node_id) = key
            .strip_prefix(JOINED_STATUS_PREFIX)
            .and_then(|id| id.parse().ok())
        else {
            continue;
        };
        if parse_joined_tenant(node_id, &value).as_deref() == Some(tenant_id.as_str()) {
            joined.insert(node_id, tenant_id);
        }
    }
    Ok(joined)
}

/// A corrupt record is skipped, so the node must present a fresh token,
/// instead of stopping registration for every node on this manager.
fn parse_joined_tenant(node_id: NodeId, value: &[u8]) -> Option<String> {
    let tenant_id = std::str::from_utf8(value).ok()?;
    if let Err(error) = minga_state::types::Tenant::validate_id(tenant_id) {
        tracing::warn!(node_id, error = %error, "ignoring invalid joined-tenant record");
        return None;
    }
    Some(tenant_id.to_owned())
}

/// Picks up a join another manager committed after this one loaded `joined`,
/// e.g. on the new leader after a failover. A cached entry is trusted only
/// while it matches the stored node's tenant; otherwise the durable record
/// (which may name a later join) replaces it.
async fn refresh_joined_node(
    bus: &dyn EventBus,
    store: &StateStore,
    event: &KvEvent,
    joined: &mut HashMap<NodeId, String>,
) {
    let Some((tenant_id, node_id)) = minga_bus::subjects::parse_kv_lease(&event.key) else {
        return;
    };
    let tenant_id = tenant_id.to_owned();
    if event.op == KvOp::Delete {
        return;
    }
    if let Some(cached) = joined.get(&node_id) {
        match store.get_node(node_id).await {
            Ok(Some(node)) if node.tenant_id == *cached => return,
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot check node registration");
                return;
            }
        }
    }
    match bus
        .get_tenant_status(&tenant_id, &joined_status_key(&tenant_id, node_id))
        .await
    {
        Ok(Some(value)) => match parse_joined_tenant(node_id, &value) {
            Some(joined_tenant) if joined_tenant == tenant_id => {
                joined.insert(node_id, joined_tenant);
            }
            _ => {
                joined.remove(&node_id);
            }
        },
        Ok(None) => {
            joined.remove(&node_id);
        }
        Err(error) => tracing::warn!(node_id, error = %error, "cannot read joined-tenant record"),
    }
}

/// A node lists a handful of Minga listeners; more is malformed or abusive.
const MAX_RESERVED_PORTS: usize = 32;

/// Whether a renewed identity carries registration fields the store lacks.
fn registration_changed(node: &ClusterNode, identity: &NodeIdentity) -> bool {
    let wireguard_changed = identity.wg_public_key.as_ref().is_some_and(|key| {
        node.wg_public_key.as_ref() != Some(key) || node.wg_endpoint != identity.wg_endpoint
    });
    wireguard_changed || node.reserved_ports != identity.reserved_ports
}

#[derive(Debug, Clone)]
struct PendingRegistration {
    identity: NodeIdentity,
    tenant_id: String,
    token_expires_at: Option<u64>,
}

#[cfg(test)]
async fn queue_node_registration(
    store: &StateStore,
    event: KvEvent,
    registered: &mut HashSet<NodeId>,
    pending: &mut HashMap<NodeId, PendingRegistration>,
) {
    let joined = HashMap::new();
    queue_node_registration_with_join(store, event, registered, pending, &joined, None).await;
}

async fn queue_node_registration_with_join(
    store: &StateStore,
    event: KvEvent,
    registered: &mut HashSet<NodeId>,
    pending: &mut HashMap<NodeId, PendingRegistration>,
    joined: &HashMap<NodeId, String>,
    join_token_key: Option<&JoinTokenKey>,
) {
    let Some((lease_tenant, node_id)) = minga_bus::subjects::parse_kv_lease(&event.key) else {
        return;
    };
    let lease_tenant = lease_tenant.to_owned();

    if event.op == KvOp::Delete {
        pending.remove(&node_id);
        return;
    }
    let Some(payload) = event.value else {
        return;
    };
    let mut identity: NodeIdentity = match minga_state::codec::decode_wire(&payload) {
        Ok(identity) => identity,
        Err(error) => {
            tracing::debug!(node_id, error = %error, "ignoring lease without a registration payload");
            return;
        }
    };
    if identity.node_id != node_id {
        tracing::warn!(
            lease_node_id = node_id,
            payload_node_id = identity.node_id,
            "lease registration ID does not match its key"
        );
        return;
    }
    let key_ok = identity
        .wg_public_key
        .as_deref()
        .is_none_or(minga_net::wireguard::validate_public_key);
    let endpoint_ok = identity
        .wg_endpoint
        .as_deref()
        .is_none_or(minga_net::wireguard::validate_endpoint);
    if !key_ok || !endpoint_ok {
        tracing::warn!(
            node_id,
            "ignoring registration with an invalid WireGuard key or endpoint"
        );
        return;
    }
    let ports_ok = identity.reserved_ports.len() <= MAX_RESERVED_PORTS
        && identity.reserved_ports.iter().all(|reserved| {
            reserved.port != 0 && matches!(reserved.protocol.as_str(), "tcp" | "udp")
        });
    if !ports_ok {
        tracing::warn!(node_id, "ignoring registration with invalid reserved ports");
        return;
    }

    let stored = match store.get_node(node_id).await {
        Ok(node) => node,
        Err(error) => {
            tracing::warn!(node_id, error = %error, "cannot check node registration");
            return;
        }
    };
    if stored.is_none() && registered.contains(&node_id) {
        return;
    }
    // A record for a node that is gone or now belongs to another tenant is
    // stale: the node must re-join with a token for its current tenant.
    let joined_tenant = joined.get(&node_id).filter(|tenant| {
        stored
            .as_ref()
            .is_some_and(|node| node.tenant_id == **tenant)
    });
    if joined.contains_key(&node_id) && joined_tenant.is_none() {
        tracing::warn!(
            node_id,
            "joined-tenant record is stale; a new join token is required"
        );
    }
    let (tenant_id, token_expires_at) = if let Some(joined_tenant) = joined_tenant {
        if joined_tenant != &lease_tenant {
            tracing::warn!(
                node_id,
                lease_tenant,
                joined_tenant,
                "lease tenant does not match joined tenant"
            );
            return;
        }
        (joined_tenant.clone(), None)
    } else if let Some(token) = identity.join_token.as_deref() {
        let Some(key) = join_token_key else {
            tracing::warn!(node_id, "ignoring join token; no verifier is configured");
            return;
        };
        let claims = match key.verify(token, node_id, unix_now()) {
            Ok(claims) => claims,
            Err(error) => {
                tracing::warn!(node_id, error = %error, "ignoring invalid node join token");
                return;
            }
        };
        if claims.tenant_id != lease_tenant {
            tracing::warn!(node_id, lease_tenant, token_tenant = %claims.tenant_id, "lease tenant does not match join token");
            return;
        }
        if stored
            .as_ref()
            .is_none_or(|node| node.tenant_id != claims.tenant_id)
        {
            tracing::warn!(
                node_id,
                "join token needs a pre-registered node in the token's tenant"
            );
            return;
        }
        (claims.tenant_id, Some(claims.expires_at))
    } else {
        if lease_tenant != minga_state::types::DEFAULT_TENANT_ID {
            tracing::warn!(
                node_id,
                lease_tenant,
                "non-default lease requires a tenant-bound join token"
            );
            return;
        }
        if stored
            .as_ref()
            .is_some_and(|node| node.tenant_id != minga_state::types::DEFAULT_TENANT_ID)
        {
            tracing::warn!(
                node_id,
                "non-default node requires a tenant-bound join token"
            );
            return;
        }
        if stored.is_none() && !store.manager_voters().await.contains(&node_id) {
            match store.list_tenants().await {
                Ok(tenants) if tenants.len() > 1 => {
                    tracing::warn!(
                        node_id,
                        "unregistered node requires a join token once a second tenant exists"
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(node_id, error = %error, "cannot check tenant count");
                    return;
                }
            }
        }
        (minga_state::types::DEFAULT_TENANT_ID.to_owned(), None)
    };
    identity.tenant_id = tenant_id.clone();
    let already_joined = joined_tenant.is_some();
    // After the node record committed, the deadline was cleared; a renewal
    // replacing the entry must not restore it while the status writes retry.
    let token_expires_at = match pending.get(&node_id) {
        Some(previous)
            if previous.token_expires_at.is_none() && previous.tenant_id == tenant_id =>
        {
            None
        }
        _ => token_expires_at,
    };

    match stored {
        Some(node) => {
            registered.insert(node_id);
            if !already_joined || registration_changed(&node, &identity) {
                pending.insert(
                    node_id,
                    PendingRegistration {
                        identity,
                        tenant_id,
                        token_expires_at,
                    },
                );
            }
        }
        None => {
            pending.insert(
                node_id,
                PendingRegistration {
                    identity,
                    tenant_id,
                    token_expires_at,
                },
            );
        }
    }
}

#[cfg(test)]
async fn register_pending_nodes(
    store: &StateStore,
    bus: &dyn EventBus,
    registered: &mut HashSet<NodeId>,
    pending: &mut HashMap<NodeId, PendingRegistration>,
) {
    let mut joined = HashMap::new();
    register_pending_nodes_with_join(store, bus, registered, pending, &mut joined).await;
}

async fn register_pending_nodes_with_join(
    store: &StateStore,
    bus: &dyn EventBus,
    registered: &mut HashSet<NodeId>,
    pending: &mut HashMap<NodeId, PendingRegistration>,
    joined: &mut HashMap<NodeId, String>,
) {
    let node_ids: Vec<_> = pending.keys().copied().collect();
    for node_id in node_ids {
        if !store.is_leader().await {
            return;
        }
        let Some(pending_registration) = pending.get(&node_id) else {
            continue;
        };
        match bus
            .is_tenant_node_alive(&pending_registration.tenant_id, node_id)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                pending.remove(&node_id);
                continue;
            }
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot check node lease");
                continue;
            }
        }
        let token_expires_at = pending_registration.token_expires_at;
        let tenant_id = pending_registration.tenant_id.clone();
        let identity = pending_registration.identity.clone();
        if token_expires_at.is_some_and(|expires_at| expires_at <= unix_now()) {
            tracing::warn!(
                node_id,
                "join token expired before node registration committed"
            );
            pending.remove(&node_id);
            continue;
        }
        // The scheduler addresses a node in the WireGuard mesh by its ID.
        if let Err(error) = minga_net::overlay::mesh_node_ip(node_id) {
            tracing::warn!(node_id, error = %error, "refusing registration: node ID has no mesh address");
            pending.remove(&node_id);
            continue;
        }
        // Raft refuses a key another node holds; checking first keeps a node
        // that renews a copied key to a local read per lease, not a Raft write
        // per retry tick.
        if let Some(key) = identity.wg_public_key.as_deref() {
            match store.list_nodes().await {
                Ok(nodes) => {
                    let holds = |node: &ClusterNode| node.wg_public_key.as_deref() == Some(key);
                    let unchanged = nodes.iter().any(|node| node.id == node_id && holds(node));
                    if !unchanged && nodes.iter().any(|node| node.id != node_id && holds(node)) {
                        tracing::warn!(
                            node_id,
                            "refusing registration: another node holds its WireGuard key"
                        );
                        pending.remove(&node_id);
                        continue;
                    }
                }
                Err(error) => {
                    tracing::warn!(node_id, error = %error, "cannot check node WireGuard key");
                    continue;
                }
            }
        }
        match store.get_node(node_id).await {
            Ok(Some(mut node)) => {
                if node.tenant_id != tenant_id {
                    tracing::warn!(node_id, "join tenant changed before registration commit");
                    pending.remove(&node_id);
                    continue;
                }
                if registration_changed(&node, &identity) {
                    if let Some(public_key) = &identity.wg_public_key {
                        node.wg_public_key = Some(public_key.clone());
                        node.wg_endpoint = identity.wg_endpoint.clone();
                    }
                    node.reserved_ports = identity.reserved_ports.clone();
                    if let Err(error) = store.upsert_node(node).await {
                        tracing::warn!(node_id, error = %error, "cannot update node registration");
                        continue;
                    }
                }
            }
            Ok(None) if tenant_id != minga_state::types::DEFAULT_TENANT_ID => {
                // Tokens are accepted only for an existing record, so the
                // node was deleted after queueing; do not recreate it.
                tracing::warn!(node_id, "node was removed before its join committed");
                pending.remove(&node_id);
                continue;
            }
            Ok(None) => {
                if let Err(error) = store
                    .upsert_node(identity.cluster_node_for_tenant(&tenant_id))
                    .await
                {
                    tracing::warn!(node_id, error = %error, "cannot register node from its bus lease");
                    continue;
                }
            }
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot check node registration");
                continue;
            }
        }
        // The node record is committed; retrying the status writes below
        // must not be cut short by the token expiring meanwhile.
        if let Some(pending_registration) = pending.get_mut(&node_id) {
            pending_registration.token_expires_at = None;
        }
        if let Err(error) = bus
            .put_tenant_status(
                &tenant_id,
                &registration_status_key(node_id),
                REGISTERED_STATUS,
            )
            .await
        {
            tracing::warn!(node_id, error = %error, "cannot persist node registration");
            continue;
        }
        if let Err(error) = bus
            .put_tenant_status(
                &tenant_id,
                &joined_status_key(&tenant_id, node_id),
                tenant_id.as_bytes(),
            )
            .await
        {
            tracing::warn!(node_id, error = %error, "cannot persist node tenant enrollment");
            continue;
        }
        registered.insert(node_id);
        joined.insert(node_id, tenant_id);
        pending.remove(&node_id);
        tracing::info!(node_id, "registered node from its bus lease");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn identity(node_id: NodeId) -> NodeIdentity {
        NodeIdentity {
            tenant_id: minga_state::types::DEFAULT_TENANT_ID.to_owned(),
            node_id,
            address: format!("127.0.0.1:{}", 50051 + node_id),
            labels: HashMap::from([("zone".to_string(), "a".to_string())]),
            capacity: ResourceSpec {
                cpus: 4.0,
                memory_bytes: 1 << 30,
                gpus: 0,
            },
            role: NodeRole::Worker,
            reserved_ports: Vec::new(),
            wg_public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
            wg_endpoint: None,
            join_token: None,
        }
    }

    #[test]
    fn lease_payload_cannot_assign_a_node_to_another_tenant() {
        let mut payload = serde_json::to_value(identity(1)).unwrap();
        payload["tenant_id"] = serde_json::json!("tenant-b");
        let decoded: NodeIdentity = serde_json::from_value(payload).unwrap();

        assert_eq!(
            decoded.cluster_node().tenant_id,
            minga_state::types::DEFAULT_TENANT_ID
        );
    }

    async fn store() -> (Arc<StateStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        (store, dir)
    }

    async fn next_lease(stream: &mut minga_bus::KvStream) -> KvEvent {
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for lease")
            .expect("lease stream ended")
    }

    async fn wait_for_leader(store: &StateStore) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.is_leader().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("single-node store did not elect a leader");
    }

    #[test]
    fn host_capacity_detects_real_values() {
        let cap = host_capacity();
        assert!(cap.cpus >= 1.0, "every host has at least one CPU");
        assert!(cap.memory_bytes > 0, "every host has memory");
    }

    #[tokio::test]
    async fn registration_writes_a_live_active_node() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(1));
        let mut stream = bus.watch_leases().await.unwrap();
        let membership =
            NodeMembership::start_with_interval(bus.clone(), identity(1), Duration::from_secs(10))
                .await
                .unwrap();
        let event = next_lease(&mut stream).await;
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        queue_node_registration(&store, event, &mut registered, &mut pending).await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        let stored = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(stored.status, NodeStatus::Active);
        assert_eq!(
            stored.wg_public_key.as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        assert_eq!(stored.wg_endpoint, None);
        assert_eq!(stored.labels.get("zone").unwrap(), "a");
        assert_eq!(stored.last_seen_unix, None);
        assert!(bus.is_alive(1).await.unwrap());
        assert_eq!(
            bus.get_tenant_status("default", "internal.node-registration.1")
                .await
                .unwrap()
                .as_deref(),
            Some(b"registered".as_slice())
        );

        membership.leave().await;
    }

    #[tokio::test]
    async fn registration_preserves_the_operator_assigned_tenant() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        store
            .create_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "acme".into(),
                name: "Acme".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        let mut node = identity(1).cluster_node();
        node.tenant_id = "acme".into();
        node.wg_public_key = None;
        store.upsert_node(node).await.unwrap();

        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let now = unix_now();
        let mut worker_identity = identity(1);
        worker_identity.tenant_id = "acme".into();
        worker_identity.join_token = Some(key.issue(1, "acme", now, now + 900).unwrap());
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(1));
        let mut stream = bus.watch_leases().await.unwrap();
        let membership = NodeMembership::start_with_interval(
            bus.clone(),
            worker_identity,
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let event = next_lease(&mut stream).await;
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &HashMap::new(),
            Some(&key),
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        let stored = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(stored.tenant_id, "acme");
        assert_eq!(
            stored.wg_public_key.as_deref(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        membership.leave().await;
        let joined = load_joined_nodes(
            bus.as_ref(),
            &["default".into(), "acme".into(), "beta".into()],
        )
        .await
        .unwrap();
        assert_eq!(joined.get(&1).map(String::as_str), Some("acme"));
        let mut renewed_identity = identity(1);
        renewed_identity.tenant_id = "acme".into();
        let lease = minga_state::codec::encode_wire(&renewed_identity).unwrap();
        bus.refresh_tenant_lease("acme", 1, &lease).await.unwrap();
        let renewal = next_lease(&mut stream).await;
        queue_node_registration_with_join(
            &store,
            renewal,
            &mut registered,
            &mut pending,
            &joined,
            Some(&key),
        )
        .await;
        assert!(pending.is_empty());
        assert_eq!(store.get_node(1).await.unwrap().unwrap().tenant_id, "acme");
    }

    #[tokio::test]
    async fn registration_with_invalid_wireguard_fields_is_ignored() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(1));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut bad_key = identity(1);
        bad_key.wg_public_key = Some("not-a-key".into());
        let mut bad_endpoint = identity(2);
        bad_endpoint.wg_endpoint = Some("no-port".into());
        let mut memberships = Vec::new();
        for identity in [bad_key, bad_endpoint] {
            memberships.push(
                NodeMembership::start_with_interval(bus.clone(), identity, Duration::from_secs(10))
                    .await
                    .unwrap(),
            );
        }
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        for _ in 0..2 {
            let event = next_lease(&mut stream).await;
            queue_node_registration(&store, event, &mut registered, &mut pending).await;
        }
        assert!(pending.is_empty());
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;
        assert!(store.list_nodes().await.unwrap().is_empty());

        for membership in memberships {
            membership.leave().await;
        }
    }

    #[tokio::test]
    async fn membership_renews_the_bus_lease() {
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_millis(80));
        let membership = NodeMembership::start_with_interval(
            bus.clone(),
            identity(1),
            Duration::from_millis(20),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(bus.is_alive(1).await.unwrap());
        membership.leave().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!bus.is_alive(1).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "60-second Raft log index proof"]
    async fn idle_manager_does_not_advance_raft_log() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        let bus = minga_bus::MemoryBus::new();
        let membership =
            NodeMembership::start_with_interval(bus.clone(), identity(1), Duration::from_secs(10))
                .await
                .unwrap();
        let registrar = tokio::spawn(run_node_registration(store.clone(), bus, None));

        tokio::time::timeout(Duration::from_secs(3), async {
            while store.get_node(1).await.unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("manager was not registered from its bus lease");
        let before = store
            .last_log_index()
            .expect("Raft metrics should expose the current log index");
        tokio::time::sleep(Duration::from_secs(60)).await;
        let after = store.last_log_index().expect("Raft log index disappeared");

        registrar.abort();
        membership.leave().await;
        assert_eq!(after, before, "idle lease renewals wrote to the Raft log");
    }

    #[tokio::test]
    async fn renewal_does_not_undrain_a_drained_node() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(1));
        let mut stream = bus.watch_leases().await.unwrap();
        let membership =
            NodeMembership::start_with_interval(bus.clone(), identity(1), Duration::from_secs(10))
                .await
                .unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        let mut node = store.get_node(1).await.unwrap().unwrap();
        node.status = NodeStatus::Draining;
        store.upsert_node(node).await.unwrap();

        let payload = minga_state::codec::encode_wire(&identity(1)).unwrap();
        bus.refresh_lease(1, &payload).await.unwrap();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        assert_eq!(
            store.get_node(1).await.unwrap().unwrap().status,
            NodeStatus::Draining
        );
        membership.leave().await;
    }

    #[tokio::test]
    async fn renewal_updates_wireguard_identity_without_overwriting_status() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(1));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut initial = identity(1);
        initial.wg_public_key = Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".into());
        initial.wg_endpoint = Some("edge.example:51820".into());
        let membership = NodeMembership::start_with_interval(
            bus.clone(),
            initial.clone(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        let mut node = store.get_node(1).await.unwrap().unwrap();
        node.status = NodeStatus::Draining;
        store.upsert_node(node).await.unwrap();

        let mut renewed = initial;
        renewed.wg_public_key = Some("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=".into());
        renewed.wg_endpoint = None;
        let payload = minga_state::codec::encode_wire(&renewed).unwrap();
        bus.refresh_lease(1, &payload).await.unwrap();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        let stored = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(stored.status, NodeStatus::Draining);
        assert_eq!(
            stored.wg_public_key.as_deref(),
            Some("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=")
        );
        assert_eq!(stored.wg_endpoint, None);

        // A restart that changes only the listener ports still updates them.
        renewed.reserved_ports = vec![minga_state::types::ReservedPort {
            port: 9100,
            protocol: "tcp".into(),
        }];
        let payload = minga_state::codec::encode_wire(&renewed).unwrap();
        bus.refresh_lease(1, &payload).await.unwrap();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;
        let stored = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(stored.reserved_ports, renewed.reserved_ports);
        assert_eq!(stored.status, NodeStatus::Draining);
        membership.leave().await;
    }

    #[tokio::test]
    async fn renewal_does_not_recreate_a_removed_node() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(1));
        let mut stream = bus.watch_leases().await.unwrap();
        let membership =
            NodeMembership::start_with_interval(bus.clone(), identity(1), Duration::from_secs(10))
                .await
                .unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;
        store.remove_node(1).await.unwrap();
        // Simulate a manager restart: the durable bus marker keeps this
        // removed ID from being treated as a new first sighting.
        registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut joined = load_joined_nodes(bus.as_ref(), &["default".into()])
            .await
            .unwrap();

        let payload = minga_state::codec::encode_wire(&identity(1)).unwrap();
        bus.refresh_lease(1, &payload).await.unwrap();
        let event = next_lease(&mut stream).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &joined,
            None,
        )
        .await;
        register_pending_nodes_with_join(
            &store,
            bus.as_ref(),
            &mut registered,
            &mut pending,
            &mut joined,
        )
        .await;

        assert!(store.get_node(1).await.unwrap().is_none());
        membership.leave().await;
    }

    const KEY_A: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const KEY_B: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";

    async fn create_tenant(store: &StateStore, id: &str) {
        store
            .create_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: id.into(),
                name: id.into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
    }

    async fn preregister(store: &StateStore, node_id: NodeId, tenant: &str) {
        let mut node = identity(node_id).cluster_node_for_tenant(tenant);
        node.wg_public_key = Some(KEY_A.into());
        store.upsert_node(node).await.unwrap();
    }

    async fn publish(
        bus: &minga_bus::MemoryBus,
        stream: &mut minga_bus::KvStream,
        identity: &NodeIdentity,
    ) -> KvEvent {
        let payload = minga_state::codec::encode_wire(identity).unwrap();
        bus.refresh_tenant_lease(&identity.tenant_id, identity.node_id, &payload)
            .await
            .unwrap();
        next_lease(stream).await
    }

    fn with_token(node_id: NodeId, key: &JoinTokenKey, tenant: &str) -> NodeIdentity {
        let now = unix_now();
        let mut identity = identity(node_id);
        identity.tenant_id = tenant.to_owned();
        identity.join_token = Some(key.issue(node_id, tenant, now, now + 900).unwrap());
        identity
    }

    #[tokio::test]
    async fn a_copied_wireguard_key_is_refused_without_a_raft_write() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        preregister(&store, 1, minga_state::types::DEFAULT_TENANT_ID).await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut copier = identity(2);
        copier.wg_public_key = Some(KEY_A.into());
        let event = publish(&bus, &mut stream, &copier).await;
        let before = store.last_log_index();
        let mut registered = HashSet::new();
        let mut pending = HashMap::new();
        queue_node_registration(&store, event, &mut registered, &mut pending).await;
        assert!(pending.contains_key(&2));
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        assert!(store.get_node(2).await.unwrap().is_none());
        assert!(
            pending.is_empty(),
            "a refused key is not retried every tick"
        );
        assert_eq!(store.last_log_index(), before, "no UpsertNode was proposed");
    }

    #[tokio::test]
    async fn renewal_uses_a_join_committed_by_another_manager() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        preregister(&store, 1, "acme").await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        // Another manager committed the join after this one loaded `joined`.
        let mut joined = load_joined_nodes(
            bus.as_ref(),
            &["default".into(), "acme".into(), "beta".into()],
        )
        .await
        .unwrap();
        bus.put_tenant_status("acme", &joined_status_key("acme", 1), b"acme")
            .await
            .unwrap();

        // The worker's token has since expired; it renews without one and
        // a new WireGuard key.
        let mut renewed = identity(1);
        renewed.tenant_id = "acme".to_owned();
        renewed.wg_public_key = Some(KEY_B.into());
        let event = publish(&bus, &mut stream, &renewed).await;
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        refresh_joined_node(bus.as_ref(), &store, &event, &mut joined).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &joined,
            None,
        )
        .await;
        register_pending_nodes_with_join(
            &store,
            bus.as_ref(),
            &mut registered,
            &mut pending,
            &mut joined,
        )
        .await;

        let stored = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(stored.tenant_id, "acme");
        assert_eq!(stored.wg_public_key.as_deref(), Some(KEY_B));
    }

    #[tokio::test]
    async fn non_default_node_needs_a_token_for_its_own_tenant() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        create_tenant(&store, "beta").await;
        preregister(&store, 1, "acme").await;
        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();

        let mut forged = with_token(1, &key, "acme");
        forged.join_token = Some(forged.join_token.unwrap().replace('.', ".x"));
        for lease in [identity(1), with_token(1, &key, "beta"), forged] {
            let event = publish(&bus, &mut stream, &lease).await;
            queue_node_registration_with_join(
                &store,
                event,
                &mut registered,
                &mut pending,
                &HashMap::new(),
                Some(&key),
            )
            .await;
            assert!(pending.is_empty(), "accepted {:?}", lease.join_token);
        }
    }

    #[tokio::test]
    async fn lease_tenant_must_match_join_token_tenant() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        create_tenant(&store, "beta").await;
        preregister(&store, 1, "acme").await;
        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let identity = with_token(1, &key, "acme");
        let payload = minga_state::codec::encode_wire(&identity).unwrap();
        let event = KvEvent {
            key: minga_bus::subjects::kv_lease("beta", 1),
            value: Some(payload),
            op: KvOp::Put,
        };
        let mut pending = HashMap::new();
        queue_node_registration_with_join(
            &store,
            event,
            &mut HashSet::new(),
            &mut pending,
            &HashMap::new(),
            Some(&key),
        )
        .await;

        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn join_token_expiring_before_commit_is_dropped() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let membership =
            NodeMembership::start_with_interval(bus.clone(), identity(2), Duration::from_secs(10))
                .await
                .unwrap();
        let mut registered = HashSet::new();
        let mut pending = HashMap::from([(
            2,
            PendingRegistration {
                identity: identity(2),
                tenant_id: "acme".into(),
                token_expires_at: Some(unix_now() - 1),
            },
        )]);
        let mut joined = HashMap::new();
        register_pending_nodes_with_join(
            &store,
            bus.as_ref(),
            &mut registered,
            &mut pending,
            &mut joined,
        )
        .await;

        assert!(pending.is_empty());
        assert!(store.get_node(2).await.unwrap().is_none());
        assert!(joined.is_empty());
        membership.leave().await;
    }

    #[tokio::test]
    async fn stale_joined_record_requires_a_token_for_the_current_tenant() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        create_tenant(&store, "beta").await;
        // Node 1 once joined acme; an operator re-created it for beta.
        preregister(&store, 1, "beta").await;
        let mut joined = HashMap::from([(1, "acme".to_string())]);
        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();

        let event = publish(&bus, &mut stream, &identity(1)).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &joined,
            Some(&key),
        )
        .await;
        assert!(
            pending.is_empty(),
            "stale record must not admit a tokenless lease"
        );

        let event = publish(&bus, &mut stream, &with_token(1, &key, "beta")).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &joined,
            Some(&key),
        )
        .await;
        register_pending_nodes_with_join(
            &store,
            bus.as_ref(),
            &mut registered,
            &mut pending,
            &mut joined,
        )
        .await;
        assert_eq!(joined.get(&1).map(String::as_str), Some("beta"));
        assert_eq!(
            bus.get_tenant_status("beta", &joined_status_key("beta", 1))
                .await
                .unwrap()
                .as_deref(),
            Some(b"beta".as_slice())
        );
    }

    #[tokio::test]
    async fn tokenless_self_enrollment_stops_once_a_second_tenant_exists() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();

        let event = publish(&bus, &mut stream, &identity(5)).await;
        queue_node_registration(&store, event, &mut registered, &mut pending).await;
        assert!(pending.is_empty(), "unknown worker enrolled into default");

        // Node 1 is this store's own Raft voter, so its manager lease passes.
        let mut manager = identity(1);
        manager.role = NodeRole::Manager;
        let event = publish(&bus, &mut stream, &manager).await;
        queue_node_registration(&store, event, &mut registered, &mut pending).await;
        assert!(pending.contains_key(&1));
    }

    #[tokio::test]
    async fn durable_join_replaces_a_stale_cached_tenant() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        create_tenant(&store, "beta").await;
        preregister(&store, 1, "beta").await;
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        // This manager cached an old acme join; another committed beta since.
        let mut joined = HashMap::from([(1, "acme".to_string())]);
        bus.put_tenant_status("beta", &joined_status_key("beta", 1), b"beta")
            .await
            .unwrap();

        let mut renewed = identity(1);
        renewed.tenant_id = "beta".into();
        renewed.wg_public_key = Some(KEY_B.into());
        let event = publish(&bus, &mut stream, &renewed).await;
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        refresh_joined_node(bus.as_ref(), &store, &event, &mut joined).await;
        assert_eq!(joined.get(&1).map(String::as_str), Some("beta"));
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &joined,
            None,
        )
        .await;
        assert!(pending.contains_key(&1), "tokenless renewal was rejected");
    }

    #[tokio::test]
    async fn join_token_for_a_node_without_a_record_is_rejected() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();

        // An unexpired token outlives its node's deletion.
        let event = publish(&bus, &mut stream, &with_token(3, &key, "acme")).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &HashMap::new(),
            Some(&key),
        )
        .await;
        assert!(pending.is_empty());
        assert!(store.get_node(3).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn node_deleted_between_queue_and_commit_is_not_recreated() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        preregister(&store, 1, "acme").await;
        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        let mut pending = HashMap::new();
        let mut joined = HashMap::new();

        let event = publish(&bus, &mut stream, &with_token(1, &key, "acme")).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &joined,
            Some(&key),
        )
        .await;
        assert!(pending.contains_key(&1));
        store.remove_node(1).await.unwrap();
        register_pending_nodes_with_join(
            &store,
            bus.as_ref(),
            &mut registered,
            &mut pending,
            &mut joined,
        )
        .await;

        assert!(store.get_node(1).await.unwrap().is_none());
        assert!(bus
            .get_tenant_status("acme", &joined_status_key("acme", 1))
            .await
            .unwrap()
            .is_none());
        assert!(pending.is_empty() && joined.is_empty());
    }

    #[tokio::test]
    async fn renewal_keeps_a_cleared_deadline_after_the_node_committed() {
        let (store, _dir) = store().await;
        wait_for_leader(&store).await;
        create_tenant(&store, "acme").await;
        preregister(&store, 1, "acme").await;
        let key = JoinTokenKey::from_bytes(&[9; 32]).unwrap();
        let bus = minga_bus::MemoryBus::with_lease_ttl(Duration::from_secs(5));
        let mut stream = bus.watch_leases().await.unwrap();
        let mut registered = load_registered_nodes(&store, bus.as_ref()).await.unwrap();
        // The node record committed and its status writes failed: the entry
        // stays pending with its deadline cleared.
        let mut pending = HashMap::from([(
            1,
            PendingRegistration {
                identity: identity(1),
                tenant_id: "acme".into(),
                token_expires_at: None,
            },
        )]);

        let mut renewed = with_token(1, &key, "acme");
        renewed.wg_public_key = Some(KEY_B.into());
        let event = publish(&bus, &mut stream, &renewed).await;
        queue_node_registration_with_join(
            &store,
            event,
            &mut registered,
            &mut pending,
            &HashMap::new(),
            Some(&key),
        )
        .await;
        assert_eq!(pending[&1].token_expires_at, None);
    }

    #[tokio::test]
    async fn invalid_joined_record_is_skipped_not_fatal() {
        let bus = minga_bus::MemoryBus::new();
        bus.put_tenant_status("default", &joined_status_key("default", 1), b"Not A Tenant")
            .await
            .unwrap();
        bus.put_tenant_status("acme", &joined_status_key("acme", 2), b"acme")
            .await
            .unwrap();
        let joined = load_joined_nodes(
            bus.as_ref(),
            &["default".into(), "acme".into(), "beta".into()],
        )
        .await
        .unwrap();
        assert_eq!(joined, HashMap::from([(2, "acme".to_string())]));
    }
}
