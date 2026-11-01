//! Node self-registration: a node announces itself to the cluster on
//! startup, renews its membership lease periodically, and marks itself
//! as left on shutdown.
//!
//! Until a node registers, the scheduler cannot see it: a cluster that
//! runs no registration places zero tasks and every deploy silently
//! succeeds with nothing running. Registration writes a [`ClusterNode`]
//! with `status: Active` and a lease timestamp; placement
//! ([`minga_state::types::ClusterNode::is_live_at`]) excludes nodes whose
//! lease expired, so a dead node stops receiving tasks within
//! [`NODE_LEASE_TTL_SECS`](minga_state::types::NODE_LEASE_TTL_SECS).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use minga_state::types::{
    unix_now, ClusterNode, NodeId, NodeStatus, ResourceSpec, NODE_LEASE_TTL_SECS,
    NODE_RENEW_INTERVAL_SECS,
};
use minga_state::{StateError, StateStore};

/// Who this node is, as announced to the cluster.
#[derive(Debug, Clone)]
pub struct NodeIdentity {
    pub node_id: NodeId,
    /// Address other nodes can reach this node's Raft RPC on.
    pub address: String,
    /// User-defined labels (e.g. `gpu=true`), usable in constraints.
    pub labels: HashMap<String, String>,
    /// Compute capacity, e.g. from [`host_capacity`].
    pub capacity: ResourceSpec,
}

impl NodeIdentity {
    fn cluster_node(&self) -> ClusterNode {
        ClusterNode {
            id: self.node_id,
            address: self.address.clone(),
            labels: self.labels.clone(),
            status: NodeStatus::Active,
            capacity: self.capacity.clone(),
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
            last_seen_unix: Some(unix_now()),
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

/// Registers `identity` in the cluster state, renewing any existing
/// record. One-shot: for ongoing membership use [`NodeMembership`].
pub async fn register_self(
    store: &StateStore,
    identity: &NodeIdentity,
) -> Result<ClusterNode, StateError> {
    // Preserve overlay fields if this node was already registered.
    let mut node = identity.cluster_node();
    if let Some(existing) = store.get_node(identity.node_id).await? {
        node.wg_public_key = existing.wg_public_key;
        node.wg_endpoint = existing.wg_endpoint;
        node.overlay_ip = existing.overlay_ip;
    }
    store.upsert_node(node.clone()).await?;
    Ok(node)
}

/// An active node membership: renews the lease until [`leave`](Self::leave).
pub struct NodeMembership {
    store: Arc<StateStore>,
    node_id: NodeId,
    renew_task: tokio::task::JoinHandle<()>,
}

impl NodeMembership {
    /// Registers the node immediately, then renews its lease every
    /// [`NODE_RENEW_INTERVAL_SECS`].
    pub async fn start(store: Arc<StateStore>, identity: NodeIdentity) -> Result<Self, StateError> {
        register_self(&store, &identity).await?;
        tracing::info!(
            node_id = identity.node_id,
            address = %identity.address,
            cpus = identity.capacity.cpus,
            memory_bytes = identity.capacity.memory_bytes,
            labels = ?identity.labels,
            lease_ttl_secs = NODE_LEASE_TTL_SECS,
            "node registered in cluster"
        );

        let renew_store = store.clone();
        let node_id = identity.node_id;
        let renew_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(NODE_RENEW_INTERVAL_SECS));
            tick.tick().await; // first tick fires immediately; skip it
            loop {
                tick.tick().await;
                // The state machine revives the record unless an operator
                // has drained it, in the same log entry as the timestamp.
                if let Err(e) = renew_store.renew_node_lease(identity.cluster_node()).await {
                    tracing::error!(node_id, error = %e, "lease renewal failed");
                }
            }
        });

        Ok(Self {
            store,
            node_id,
            renew_task,
        })
    }

    /// Stops renewing and marks the node as `Left`. The record stays in
    /// state (visible as down) until an operator removes it.
    pub async fn leave(self) -> Result<(), StateError> {
        self.renew_task.abort();
        if let Some(mut node) = self.store.get_node(self.node_id).await? {
            node.status = NodeStatus::Left;
            self.store.upsert_node(node).await?;
        }
        tracing::info!(node_id = self.node_id, "node left the cluster");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn identity(node_id: NodeId) -> NodeIdentity {
        NodeIdentity {
            node_id,
            address: format!("127.0.0.1:{}", 50051 + node_id),
            labels: HashMap::from([("zone".to_string(), "a".to_string())]),
            capacity: ResourceSpec {
                cpus: 4.0,
                memory_bytes: 1 << 30,
                gpus: 0,
            },
        }
    }

    #[test]
    fn host_capacity_detects_real_values() {
        let cap = host_capacity();
        assert!(cap.cpus >= 1.0, "every host has at least one CPU");
        assert!(cap.memory_bytes > 0, "every host has memory");
    }

    #[tokio::test]
    async fn registration_writes_a_live_active_node() {
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap();

        let registered = register_self(&store, &identity(1)).await.unwrap();
        assert_eq!(registered.status, NodeStatus::Active);
        assert!(registered.is_live_at(unix_now(), NODE_LEASE_TTL_SECS));
        assert_eq!(registered.labels.get("zone").unwrap(), "a");

        let stored = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(stored, registered);
    }

    #[tokio::test]
    async fn membership_renews_the_lease() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );

        // Start from a record whose lease is already stale; membership
        // registration must make it live again.
        let membership = NodeMembership::start(store.clone(), identity(1))
            .await
            .unwrap();
        let first = store.get_node(1).await.unwrap().unwrap();
        assert!(first.is_live_at(unix_now(), NODE_LEASE_TTL_SECS));

        // Rewind the lease artificially, then wait for one renewal cycle.
        let mut stale = first.clone();
        stale.last_seen_unix = Some(0);
        store.upsert_node(stale).await.unwrap();
        tokio::time::sleep(Duration::from_secs(NODE_RENEW_INTERVAL_SECS + 2)).await;

        let renewed = store.get_node(1).await.unwrap().unwrap();
        assert!(
            renewed.last_seen_unix > Some(0),
            "membership must renew the lease"
        );
        assert!(renewed.is_live_at(unix_now(), NODE_LEASE_TTL_SECS));

        membership.leave().await.unwrap();
        let left = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(left.status, NodeStatus::Left);
    }

    #[tokio::test]
    async fn renewal_does_not_undrain_a_drained_node() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );

        let membership = NodeMembership::start(store.clone(), identity(1))
            .await
            .unwrap();

        // Operator drains the node; the renewal loop must keep it drained.
        let mut node = store.get_node(1).await.unwrap().unwrap();
        node.status = NodeStatus::Draining;
        store.upsert_node(node).await.unwrap();
        tokio::time::sleep(Duration::from_secs(NODE_RENEW_INTERVAL_SECS + 2)).await;

        let drained = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(drained.status, NodeStatus::Draining);
        assert!(drained.is_live_at(unix_now(), NODE_LEASE_TTL_SECS));

        membership.leave().await.unwrap();
    }
}
