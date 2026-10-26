//! Integration tests for multi-node Raft clusters.
//!
//! These tests start 2-3 StateStore nodes on localhost with unique ports,
//! verify leader election, log replication, and leader failover.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec, Service, ServiceMode};
use minga_state::StateStore;

/// Unique port counter for parallel test isolation.
static PORT: AtomicU16 = AtomicU16::new(52000);

fn next_addr() -> String {
    let port = PORT.fetch_add(1, Ordering::SeqCst);
    format!("127.0.0.1:{port}")
}

fn sample_node(id: u64, addr: &str) -> ClusterNode {
    ClusterNode {
        id,
        address: addr.to_string(),
        labels: HashMap::new(),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus: 8.0,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            gpus: 0,
        },
    }
}

fn sample_service(id: &str) -> Service {
    Service {
        id: id.to_string(),
        name: id.to_string(),
        image: format!("registry.local/{id}:1.0"),
        replicas: 1,
        mode: ServiceMode::Replicated,
        constraints: Vec::new(),
        env: HashMap::new(),
        ports: Vec::new(),
        networks: vec!["default".to_string()],
        resources: ResourceSpec::default(),
    }
}

/// Opens a 3-node cluster. Returns the three stores and their addresses.
///
/// All three nodes are started concurrently. Node 1 initializes the cluster
/// with all three members.
async fn open_cluster() -> (StateStore, StateStore, StateStore, String, String, String) {
    let addr1 = next_addr();
    let addr2 = next_addr();
    let addr3 = next_addr();

    let mut peers: BTreeMap<u64, String> = BTreeMap::new();
    peers.insert(2, addr2.clone());
    peers.insert(3, addr3.clone());

    let mut peers2: BTreeMap<u64, String> = BTreeMap::new();
    peers2.insert(1, addr1.clone());
    peers2.insert(3, addr3.clone());

    let mut peers3: BTreeMap<u64, String> = BTreeMap::new();
    peers3.insert(1, addr1.clone());
    peers3.insert(2, addr2.clone());

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let dir3 = tempfile::tempdir().unwrap();

    // Start all three nodes concurrently.
    let (r1, r2, r3) = tokio::join!(
        StateStore::open(1, &addr1, dir1.path(), peers),
        StateStore::open(2, &addr2, dir2.path(), peers2),
        StateStore::open(3, &addr3, dir3.path(), peers3),
    );

    let store1 = r1.unwrap();
    let store2 = r2.unwrap();
    let store3 = r3.unwrap();

    // Leak the tempdirs — they'll be cleaned up on process exit.
    // (We can't return them because the stores borrow from them... actually
    // StateStore takes the path by reference and creates its own files, so
    // the tempdir must outlive the store. We use mem::forget to leak.)
    std::mem::forget(dir1);
    std::mem::forget(dir2);
    std::mem::forget(dir3);

    (store1, store2, store3, addr1, addr2, addr3)
}

#[tokio::test]
async fn three_nodes_elect_leader() {
    let (store1, store2, store3, _, _, _) = open_cluster().await;

    // Exactly one node should be the leader.
    let l1 = store1.is_leader().await;
    let l2 = store2.is_leader().await;
    let l3 = store3.is_leader().await;
    let leaders = [l1, l2, l3];
    assert_eq!(
        leaders.iter().filter(|&&l| l).count(),
        1,
        "exactly one leader expected, got {leaders:?}"
    );

    // All nodes should agree on who the leader is.
    let leader1 = store1.current_leader().await;
    let leader2 = store2.current_leader().await;
    let leader3 = store3.current_leader().await;
    assert_eq!(leader1, leader2, "nodes 1 and 2 disagree on leader");
    assert_eq!(leader2, leader3, "nodes 2 and 3 disagree on leader");
    assert!(leader1.is_some(), "a leader should be elected");

    store1.shutdown().await.unwrap();
    store2.shutdown().await.unwrap();
    store3.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_replicates_to_all_nodes() {
    let (store1, store2, store3, addr1, _, _) = open_cluster().await;

    // Find the leader and write to it.
    let leader = if store1.is_leader().await {
        &store1
    } else if store2.is_leader().await {
        &store2
    } else {
        &store3
    };

    leader.upsert_node(sample_node(1, &addr1)).await.unwrap();
    leader.upsert_service(sample_service("web")).await.unwrap();

    // All nodes should see the write (eventually — replication is async).
    // Wait for the followers to catch up.
    tokio::time::sleep(Duration::from_millis(500)).await;

    for (i, store) in [&store1, &store2, &store3].iter().enumerate() {
        let node = store.get_node(1).await.unwrap();
        assert!(node.is_some(), "node {i} should see the replicated node");
        assert_eq!(node.unwrap().id, 1);

        let svc = store.get_service("web").await.unwrap();
        assert!(svc.is_some(), "node {i} should see the replicated service");
        assert_eq!(svc.unwrap().replicas, 1);
    }

    store1.shutdown().await.unwrap();
    store2.shutdown().await.unwrap();
    store3.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_writes_replicate() {
    let (store1, store2, store3, _, _, _) = open_cluster().await;

    let leader = if store1.is_leader().await {
        &store1
    } else if store2.is_leader().await {
        &store2
    } else {
        &store3
    };

    // Write 5 services.
    for i in 0..5 {
        leader
            .upsert_service(sample_service(&format!("svc-{i}")))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // All nodes should see all 5 services.
    for store in [&store1, &store2, &store3] {
        let services = store.list_services().await.unwrap();
        assert_eq!(services.len(), 5, "expected 5 services on all nodes");
    }

    store1.shutdown().await.unwrap();
    store2.shutdown().await.unwrap();
    store3.shutdown().await.unwrap();
}

#[tokio::test]
async fn leader_failover() {
    let (store1, store2, store3, _, _, _) = open_cluster().await;

    // Wrap in Options so we can shut down the leader without borrow issues.
    let mut nodes: Vec<(u64, Option<StateStore>)> =
        vec![(1, Some(store1)), (2, Some(store2)), (3, Some(store3))];

    // Identify the leader by checking each node.
    let mut leader_id = 0u64;
    for (id, store) in &nodes {
        if store.as_ref().unwrap().is_leader().await {
            leader_id = *id;
            break;
        }
    }
    assert_ne!(leader_id, 0, "a leader should be elected");

    // Write some data before failover.
    for (id, store) in &nodes {
        if *id == leader_id {
            store
                .as_ref()
                .unwrap()
                .upsert_service(sample_service("before-failover"))
                .await
                .unwrap();
        }
    }

    // Shut down the leader.
    for (id, store) in &mut nodes {
        if *id == leader_id {
            store.take().unwrap().shutdown().await.unwrap();
        }
    }

    // Wait for the remaining nodes to elect a new leader.
    let mut new_leader_id = 0u64;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        for (id, store) in &nodes {
            if *id == leader_id {
                continue;
            }
            if let Some(s) = store {
                if s.is_leader().await {
                    new_leader_id = *id;
                    break;
                }
            }
        }
        if new_leader_id != 0 {
            break;
        }
    }
    assert_ne!(new_leader_id, 0, "a new leader should be elected");
    assert_ne!(new_leader_id, leader_id, "new leader should be different");

    // Write after failover should work.
    for (id, store) in &nodes {
        if *id == new_leader_id {
            store
                .as_ref()
                .unwrap()
                .upsert_service(sample_service("after-failover"))
                .await
                .unwrap();
        }
    }

    // Both surviving nodes should see both writes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (id, store) in &nodes {
        if *id == leader_id {
            continue;
        }
        let s = store.as_ref().unwrap();
        let before = s.get_service("before-failover").await.unwrap();
        assert!(before.is_some(), "node {id}: pre-failover write visible");
        let after = s.get_service("after-failover").await.unwrap();
        assert!(after.is_some(), "node {id}: post-failover write visible");
    }

    // Shut down remaining nodes.
    for (_, store) in &mut nodes {
        if let Some(s) = store.take() {
            s.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn two_node_cluster() {
    // A 2-node cluster should also work (majority = 2).
    let addr1 = next_addr();
    let addr2 = next_addr();

    let mut peers1: BTreeMap<u64, String> = BTreeMap::new();
    peers1.insert(2, addr2.clone());

    let mut peers2: BTreeMap<u64, String> = BTreeMap::new();
    peers2.insert(1, addr1.clone());

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let (r1, r2) = tokio::join!(
        StateStore::open(1, &addr1, dir1.path(), peers1),
        StateStore::open(2, &addr2, dir2.path(), peers2),
    );

    let store1 = r1.unwrap();
    let store2 = r2.unwrap();
    std::mem::forget(dir1);
    std::mem::forget(dir2);

    // One leader.
    let l1 = store1.is_leader().await;
    let l2 = store2.is_leader().await;
    assert_ne!(l1, l2, "exactly one of two nodes should be leader");

    // Write and replicate.
    let leader = if l1 { &store1 } else { &store2 };
    leader.upsert_service(sample_service("test")).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower = if l1 { &store2 } else { &store1 };
    let svc = follower.get_service("test").await.unwrap();
    assert!(svc.is_some(), "follower should see replicated write");

    store1.shutdown().await.unwrap();
    store2.shutdown().await.unwrap();
}
