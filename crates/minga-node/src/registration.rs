//! Node self-registration: a node publishes its identity in a bus lease
//! and refreshes it periodically. A leader registers previously unseen
//! leases in Raft; placement reads liveness from the bus.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{BusError, EventBus, KvEvent, KvOp};
use minga_state::types::{
    ClusterNode, NodeId, NodeRole, NodeStatus, ResourceSpec, NODE_RENEW_INTERVAL_SECS,
};
use minga_state::StateStore;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

fn default_tenant_id() -> String {
    "default".to_string()
}

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
}

impl NodeIdentity {
    /// Convert this advertised identity into the initial Raft node record.
    pub fn cluster_node(&self) -> ClusterNode {
        ClusterNode {
            id: self.node_id,
            address: self.address.clone(),
            labels: self.labels.clone(),
            status: NodeStatus::Active,
            capacity: self.capacity.clone(),
            wg_public_key: self.wg_public_key.clone(),
            nats_user_public_key: None,
            wg_endpoint: self.wg_endpoint.clone(),
            overlay_ip: None,
            last_seen_unix: None,
            role: self.role,
            reserved_ports: self.reserved_ports.clone(),
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
        bus.refresh_lease(&identity.tenant_id, identity.node_id, &registration)
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

        let tenant_id = identity.tenant_id.clone();
        let node_id = identity.node_id;
        let renew_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(error) = bus
                    .refresh_lease(&tenant_id, node_id, &registration)
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
    verified_tenant_id: &str,
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
                    queue_node_registration(
                        &store,
                        event,
                        verified_tenant_id,
                        &mut registered,
                        &mut pending,
                    )
                    .await;
                    if store.is_leader().await {
                        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;
                    }
                }
                _ = retry.tick() => {
                    if store.is_leader().await {
                        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;
                    }
                }
            }
        }
    }
}

const REGISTRATION_STATUS_SUFFIX: &str = ".internal.node-registration.";
const REGISTERED_STATUS: &[u8] = b"registered";

fn registration_status_key(tenant_id: &str, node_id: NodeId) -> String {
    format!("{tenant_id}{REGISTRATION_STATUS_SUFFIX}{node_id}")
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
    let statuses = bus.list_status().await.map_err(|error| error.to_string())?;
    let mut registered = existing.clone();
    let mut persisted = HashSet::new();

    for (key, _) in statuses {
        if let Some(node_id) = key
            .rsplit_once(REGISTRATION_STATUS_SUFFIX)
            .and_then(|(_, id)| id.parse().ok())
        {
            registered.insert(node_id);
            persisted.insert(node_id);
        }
    }

    for node_id in existing.difference(&persisted) {
        bus.put_status(
            &registration_status_key("default", *node_id),
            REGISTERED_STATUS,
        )
        .await
        .map_err(|error| error.to_string())?;
    }

    Ok(registered)
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

async fn queue_node_registration(
    store: &StateStore,
    event: KvEvent,
    verified_tenant_id: &str,
    registered: &mut HashSet<NodeId>,
    pending: &mut HashMap<NodeId, NodeIdentity>,
) {
    let Some((tenant_id, node_id)) = minga_bus::subjects::parse_kv_lease(&event.key) else {
        return;
    };
    if tenant_id != verified_tenant_id {
        tracing::warn!(
            key_tenant_id = tenant_id,
            verified_tenant_id,
            node_id,
            "ignoring lease outside the join-token tenant"
        );
        return;
    }

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
    identity.tenant_id = verified_tenant_id.to_string();
    if identity.node_id != node_id {
        tracing::warn!(
            lease_node_id = node_id,
            payload_node_id = identity.node_id,
            "lease registration ID does not match its key"
        );
        return;
    }
    // Workers supply these; they become every peer's `wg set` arguments.
    let key_ok = identity
        .wg_public_key
        .as_deref()
        .map_or(true, minga_net::wireguard::validate_public_key);
    let endpoint_ok = identity
        .wg_endpoint
        .as_deref()
        .map_or(true, minga_net::wireguard::validate_endpoint);
    if !key_ok || !endpoint_ok {
        tracing::warn!(
            node_id,
            "ignoring registration with an invalid WireGuard key or endpoint"
        );
        return;
    }
    // Workers are untrusted and this list is written to Raft and read by
    // ingress admission, so bound it and accept only real port tuples.
    let ports_ok = identity.reserved_ports.len() <= MAX_RESERVED_PORTS
        && identity.reserved_ports.iter().all(|reserved| {
            reserved.port != 0 && matches!(reserved.protocol.as_str(), "tcp" | "udp")
        });
    if !ports_ok {
        tracing::warn!(node_id, "ignoring registration with invalid reserved ports");
        return;
    }

    match store.get_node(node_id).await {
        Ok(Some(node)) => {
            registered.insert(node_id);
            if registration_changed(&node, &identity) {
                pending.insert(node_id, identity);
            }
        }
        Ok(None) if registered.contains(&node_id) => {}
        Ok(None) => {
            pending.insert(node_id, identity);
        }
        Err(error) => {
            tracing::warn!(node_id, error = %error, "cannot check node registration");
        }
    }
}

async fn register_pending_nodes(
    store: &StateStore,
    bus: &dyn EventBus,
    registered: &mut HashSet<NodeId>,
    pending: &mut HashMap<NodeId, NodeIdentity>,
) {
    let node_ids: Vec<_> = pending.keys().copied().collect();
    for node_id in node_ids {
        if !store.is_leader().await {
            return;
        }
        let tenant_id = &pending[&node_id].tenant_id;
        match bus.is_alive(tenant_id, node_id).await {
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
        match store.get_node(node_id).await {
            Ok(Some(mut node)) => {
                if let Some(identity) = pending.get(&node_id) {
                    if registration_changed(&node, identity) {
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
                if let Err(error) = bus
                    .put_status(
                        &registration_status_key(tenant_id, node_id),
                        REGISTERED_STATUS,
                    )
                    .await
                {
                    tracing::warn!(node_id, error = %error, "cannot persist node registration");
                    continue;
                }
                registered.insert(node_id);
                pending.remove(&node_id);
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot check node registration");
                continue;
            }
        }
        let Some(identity) = pending.get(&node_id) else {
            continue;
        };
        match store.upsert_node(identity.cluster_node()).await {
            Ok(()) => {
                if let Err(error) = bus
                    .put_status(
                        &registration_status_key(&identity.tenant_id, node_id),
                        REGISTERED_STATUS,
                    )
                    .await
                {
                    tracing::warn!(node_id, error = %error, "cannot persist node registration");
                    continue;
                }
                registered.insert(node_id);
                pending.remove(&node_id);
                tracing::info!(node_id, "registered node from its bus lease");
            }
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot register node from its bus lease");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    async fn queue_node_registration(
        store: &StateStore,
        event: KvEvent,
        registered: &mut HashSet<NodeId>,
        pending: &mut HashMap<NodeId, NodeIdentity>,
    ) {
        super::queue_node_registration(store, event, "default", registered, pending).await;
    }

    fn identity(node_id: NodeId) -> NodeIdentity {
        NodeIdentity {
            tenant_id: "default".to_string(),
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
        }
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
        assert!(bus.is_alive("default", 1).await.unwrap());
        assert_eq!(
            bus.get_status("internal.node-registration.1")
                .await
                .unwrap()
                .as_deref(),
            Some(b"registered".as_slice())
        );

        membership.leave().await;
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
        assert!(bus.is_alive("default", 1).await.unwrap());
        membership.leave().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!bus.is_alive("default", 1).await.unwrap());
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
        let registrar = tokio::spawn(run_node_registration(store.clone(), bus, "default"));

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
        bus.refresh_lease("default", 1, &payload).await.unwrap();
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
        bus.refresh_lease("default", 1, &payload).await.unwrap();
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
        bus.refresh_lease("default", 1, &payload).await.unwrap();
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

        let payload = minga_state::codec::encode_wire(&identity(1)).unwrap();
        bus.refresh_lease("default", 1, &payload).await.unwrap();
        queue_node_registration(
            &store,
            next_lease(&mut stream).await,
            &mut registered,
            &mut pending,
        )
        .await;
        register_pending_nodes(&store, bus.as_ref(), &mut registered, &mut pending).await;

        assert!(store.get_node(1).await.unwrap().is_none());
        membership.leave().await;
    }
}
