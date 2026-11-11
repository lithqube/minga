//! Integration tests for multi-node Raft clusters.
//!
//! These tests start 2-3 StateStore nodes on localhost with OS-assigned ports,
//! verify leader election, log replication, and leader failover.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::time::Duration;

use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec, Service, ServiceMode};
use minga_state::StateStore;

/// Upper bound for any wait in this file, so a broken cluster fails instead of hanging.
const WAIT: Duration = Duration::from_secs(30);

/// Returns `N` distinct free localhost addresses chosen by the OS, so parallel
/// runs of this suite (other worktrees, leftover processes) never share ports.
fn free_addrs<const N: usize>() -> [String; N] {
    // Hold every listener until all are bound so the OS can't hand out a port twice.
    let listeners: [std::net::TcpListener; N] =
        std::array::from_fn(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    // shortcut: the port is free between this drop and StateStore binding it; pass a
    // bound listener into StateStore::open if another process ever wins that race.
    listeners.map(|l| l.local_addr().unwrap().to_string())
}

/// Polls `check` until it returns true, panicking with `what` after [`WAIT`].
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WAIT;
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until one of `stores` reports itself leader and returns it.
async fn leader<'a>(stores: &[&'a StateStore]) -> &'a StateStore {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        for store in stores {
            if store.is_leader().await {
                return store;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        last_seen_unix: None,
        role: Default::default(),
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
        agent: None,
        restart_policy: Default::default(),
        secrets: vec![],
        command: vec![],
        entrypoint: vec![],
        labels: HashMap::new(),
        healthcheck: None,
        mounts: vec![],
        user: None,
        update: Default::default(),
        rollback: None,
        previous: None,
        extra_hosts: vec![],
    }
}

/// Opens a 3-node cluster. Returns the three stores and their addresses.
///
/// All three nodes are started concurrently. Node 1 initializes the cluster
/// with all three members.
async fn open_cluster() -> (StateStore, StateStore, StateStore, String, String, String) {
    let [addr1, addr2, addr3] = free_addrs();

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

    // Start all three nodes concurrently. `open` waits for a leader with no
    // deadline, so bound it here; try_join stops at the first failed node.
    let (store1, store2, store3) = tokio::time::timeout(WAIT, async {
        tokio::try_join!(
            StateStore::open(1, &addr1, dir1.path(), peers),
            StateStore::open(2, &addr2, dir2.path(), peers2),
            StateStore::open(3, &addr3, dir3.path(), peers3),
        )
    })
    .await
    .expect("3-node cluster did not start in time")
    .unwrap();

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
async fn follower_writes_forward_to_the_leader() {
    let (store1, store2, store3, _, _, _) = open_cluster().await;

    let stores = [&store1, &store2, &store3];
    let mut follower: Option<&StateStore> = None;
    for store in stores {
        if !store.is_leader().await {
            follower = Some(store);
            break;
        }
    }
    let follower = follower.expect("a 3-node cluster has followers");

    // A write issued against a follower must commit via the leader —
    // node registration and the Docker API depend on this from any node.
    follower
        .upsert_service(sample_service("forwarded-svc"))
        .await
        .expect("follower write must forward to the leader");

    for store in [&store1, &store2, &store3] {
        eventually(
            "the forwarded write must replicate to every node",
            || async move { store.get_service("forwarded-svc").await.unwrap().is_some() },
        )
        .await;
    }
}

#[tokio::test]
async fn three_nodes_elect_leader() {
    let (store1, store2, store3, _, _, _) = open_cluster().await;
    leader(&[&store1, &store2, &store3]).await;

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
    let leader = leader(&[&store1, &store2, &store3]).await;

    leader.upsert_node(sample_node(1, &addr1)).await.unwrap();
    leader.upsert_service(sample_service("web")).await.unwrap();

    // All nodes should see the write (eventually — replication is async).
    for (i, store) in [&store1, &store2, &store3].iter().enumerate() {
        eventually("followers replicate the node and service", || async move {
            store.get_node(1).await.unwrap().is_some()
                && store.get_service("web").await.unwrap().is_some()
        })
        .await;
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

    let leader = leader(&[&store1, &store2, &store3]).await;

    // Write 5 services.
    for i in 0..5 {
        leader
            .upsert_service(sample_service(&format!("svc-{i}")))
            .await
            .unwrap();
    }

    // All nodes should see all 5 services.
    for store in [&store1, &store2, &store3] {
        eventually("expected 5 services on all nodes", || async move {
            store.list_services().await.unwrap().len() == 5
        })
        .await;
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

    let stores: Vec<&StateStore> = nodes.iter().map(|(_, s)| s.as_ref().unwrap()).collect();
    let leader_id = leader(&stores).await.node_id();
    drop(stores);

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
    let survivors: Vec<&StateStore> = nodes.iter().filter_map(|(_, s)| s.as_ref()).collect();
    let new_leader_id = leader(&survivors).await.node_id();
    drop(survivors);
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
    for (_, store) in &nodes {
        let Some(s) = store else { continue };
        eventually(
            "survivors see pre- and post-failover writes",
            || async move {
                s.get_service("before-failover").await.unwrap().is_some()
                    && s.get_service("after-failover").await.unwrap().is_some()
            },
        )
        .await;
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
    let [addr1, addr2] = free_addrs();

    let mut peers1: BTreeMap<u64, String> = BTreeMap::new();
    peers1.insert(2, addr2.clone());

    let mut peers2: BTreeMap<u64, String> = BTreeMap::new();
    peers2.insert(1, addr1.clone());

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let (store1, store2) = tokio::time::timeout(WAIT, async {
        tokio::try_join!(
            StateStore::open(1, &addr1, dir1.path(), peers1),
            StateStore::open(2, &addr2, dir2.path(), peers2),
        )
    })
    .await
    .expect("2-node cluster did not start in time")
    .unwrap();
    std::mem::forget(dir1);
    std::mem::forget(dir2);

    // One leader.
    let leader = leader(&[&store1, &store2]).await;
    let l1 = leader.node_id() == store1.node_id();
    assert_ne!(
        store1.is_leader().await,
        store2.is_leader().await,
        "exactly one of two nodes should be leader"
    );

    // Write and replicate.
    leader.upsert_service(sample_service("test")).await.unwrap();

    let follower = if l1 { &store2 } else { &store1 };
    eventually("follower should see replicated write", || async move {
        follower.get_service("test").await.unwrap().is_some()
    })
    .await;

    store1.shutdown().await.unwrap();
    store2.shutdown().await.unwrap();
}
