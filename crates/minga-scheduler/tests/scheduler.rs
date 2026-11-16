//! Integration tests for the Scheduler with a real StateStore.
//!
//! These tests open a single-node Raft state store, register nodes and
//! services, run the scheduler's reconcile loop, and verify that tasks
//! appear in the state store.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{subjects, EventBus, MemoryBus, TaskActualState, TaskStatusReport};
use minga_scheduler::Scheduler;
use minga_state::types::{
    ClusterNode, FailureAction, Network, NodeRole, NodeStatus, ResourceSpec, Service, ServiceMode,
};
use minga_state::StateStore;

fn make_node(id: u64, cpus: f64, mem: u64) -> ClusterNode {
    ClusterNode {
        id,
        address: format!("10.0.0.{id}:50051"),
        labels: HashMap::new(),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus,
            memory_bytes: mem,
            gpus: 0,
        },
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        last_seen_unix: None,
        role: Default::default(),
    }
}

fn make_service(id: &str, replicas: u32) -> Service {
    Service {
        id: id.to_string(),
        name: id.to_string(),
        image: "nginx:latest".to_string(),
        replicas,
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

#[tokio::test]
async fn bus_lease_controls_node_eligibility() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 1))
        .await
        .unwrap();
    let bus = minga_bus::MemoryBus::new();
    bus.put_assignment("node.99.task.stale", b"stale")
        .await
        .unwrap();
    let scheduler = Scheduler::with_bus(store.clone(), bus.clone());

    assert!(
        scheduler.reconcile_once().await.unwrap().is_empty(),
        "a Raft node record without a live bus lease must not be placed"
    );
    assert!(store.list_tasks().await.unwrap().is_empty());

    bus.refresh_lease(1, b"registration").await.unwrap();
    let actions = scheduler.reconcile_once().await.unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(store.list_tasks().await.unwrap()[0].node_id, 1);
    let assignments = bus.list_assignments("").await.unwrap();
    assert!(!assignments
        .iter()
        .any(|(key, _)| key == "node.99.task.stale"));
    let task_entry = assignments
        .iter()
        .find(|(key, _)| key.starts_with("node.1.task.app.web.1."))
        .expect("leader publishes the placed task assignment");
    let assignment: minga_state::types::TaskAssignment =
        minga_state::codec::decode_wire(&task_entry.1).unwrap();
    assert_eq!(assignment.node_id, 1);

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn manager_without_endpoint_blocks_multi_node_placement() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    let mut manager = make_node(1, 8.0, 1 << 34);
    manager.role = NodeRole::Manager;
    store.upsert_node(manager).await.unwrap();
    store.upsert_node(make_node(2, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 1))
        .await
        .unwrap();
    let bus = MemoryBus::new();
    bus.refresh_lease(1, b"registration").await.unwrap();
    bus.refresh_lease(2, b"registration").await.unwrap();
    let scheduler = Scheduler::with_bus(store.clone(), bus);

    assert!(scheduler.reconcile_once().await.is_err());
    assert!(store.list_tasks().await.unwrap().is_empty());

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn empty_lease_set_does_not_remove_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 2))
        .await
        .unwrap();
    let bus = MemoryBus::with_lease_ttl(Duration::from_millis(50));
    bus.refresh_lease(1, b"registration").await.unwrap();
    let scheduler = Scheduler::with_bus(store.clone(), bus.clone());
    scheduler.reconcile_once().await.unwrap();
    assert_eq!(store.list_tasks().await.unwrap().len(), 2);

    // Every lease expires, as after a bus outage longer than the TTL.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(scheduler.reconcile_once().await.unwrap().is_empty());
    // A live lease for a node Raft does not know about does not count.
    bus.refresh_lease(99, b"unregistered").await.unwrap();
    assert!(scheduler.reconcile_once().await.unwrap().is_empty());
    assert_eq!(
        store.list_tasks().await.unwrap().len(),
        2,
        "an empty lease set must not stop every task"
    );

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn partial_lease_set_after_an_empty_one_does_not_remove_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    for id in [1, 2] {
        store
            .upsert_node(make_node(id, 8.0, 1 << 34))
            .await
            .unwrap();
    }
    // Global tasks are keyed by node, so a missing lease would remove one.
    let mut service = make_service("app.agent", 1);
    service.mode = ServiceMode::Global;
    store.upsert_service(service).await.unwrap();
    let bus = MemoryBus::with_lease_ttl(Duration::from_millis(50));
    for id in [1, 2] {
        bus.refresh_lease(id, b"registration").await.unwrap();
    }
    let scheduler = Scheduler::with_bus(store.clone(), bus.clone());
    scheduler.reconcile_once().await.unwrap();
    assert_eq!(store.list_tasks().await.unwrap().len(), 2);

    // Every lease expires, then only node 1 has renewed so far.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(scheduler.reconcile_once().await.unwrap().is_empty());
    bus.refresh_lease(1, b"registration").await.unwrap();
    assert!(scheduler.reconcile_once().await.unwrap().is_empty());
    assert_eq!(
        store.list_tasks().await.unwrap().len(),
        2,
        "node 2 has not renewed yet; its task must stay"
    );

    // Node 2 renews: recovery is complete and the grace window ends.
    bus.refresh_lease(2, b"registration").await.unwrap();
    scheduler.reconcile_once().await.unwrap();
    // A later lost lease is a real node failure, acted on right away.
    tokio::time::sleep(Duration::from_millis(100)).await;
    bus.refresh_lease(1, b"registration").await.unwrap();
    assert!(
        !scheduler.reconcile_once().await.unwrap().is_empty(),
        "a finished grace window must not delay removing node 2's task"
    );

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_projects_wireguard_peer_sets_from_live_placement() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    let mut manager = make_node(1, 8.0, 1 << 34);
    manager.role = NodeRole::Manager;
    manager.wg_public_key = Some("manager-key".into());
    manager.wg_endpoint = Some("192.0.2.1:51820".into());
    let mut worker = make_node(2, 8.0, 1 << 34);
    worker.wg_public_key = Some("worker-key".into());
    let mut isolated = make_node(3, 8.0, 1 << 34);
    isolated.wg_public_key = Some("isolated-key".into());
    store.upsert_node(manager.clone()).await.unwrap();
    store.upsert_node(worker.clone()).await.unwrap();
    store.upsert_node(isolated).await.unwrap();
    for (id, subnet) in [("shared", "10.1.0.0/24"), ("other", "10.2.0.0/24")] {
        store
            .upsert_network(Network {
                id: id.into(),
                name: id.into(),
                driver: "overlay".into(),
                attachable: false,
                subnet: Some(subnet.into()),
                wg_port: None,
            })
            .await
            .unwrap();
    }
    let mut manager_service = make_service("app.manager", 1);
    manager_service.networks = vec!["shared".into()];
    let mut worker_service = make_service("app.worker", 1);
    worker_service.networks = vec!["shared".into()];
    store.upsert_service(manager_service.clone()).await.unwrap();
    store.upsert_service(worker_service.clone()).await.unwrap();

    let mut manager_task =
        minga_scheduler::place_service(&manager_service, &[manager], &[], &[], 0).remove(0);
    manager_task
        .spec
        .ips
        .insert("shared".into(), "10.1.0.10".into());
    store.upsert_task(manager_task).await.unwrap();
    let mut worker_task =
        minga_scheduler::place_service(&worker_service, &[worker], &[], &[], 0).remove(0);
    worker_task
        .spec
        .ips
        .insert("shared".into(), "10.1.0.20".into());
    store.upsert_task(worker_task).await.unwrap();
    let bus = MemoryBus::new();
    for id in [1, 2, 3] {
        bus.refresh_lease(id, b"registration").await.unwrap();
    }
    bus.put_assignment("node.99.wg", b"stale").await.unwrap();
    let scheduler = Scheduler::with_bus(store.clone(), bus.clone());
    scheduler.reconcile_once().await.unwrap();

    let entries = bus.list_assignments("").await.unwrap();
    assert!(!entries.iter().any(|(key, _)| key == "node.99.wg"));
    let decode = |id| {
        let value = entries
            .iter()
            .find(|(key, _)| key == &format!("node.{id}.wg"))
            .expect("peer set published for eligible node");
        minga_state::codec::decode_wire::<minga_net::wireguard::WireGuardPeerSet>(&value.1).unwrap()
    };
    let manager_set = decode(1);
    assert_eq!(manager_set.networks.len(), 2);
    assert!(manager_set
        .networks
        .iter()
        .any(|network| network.network_id == "shared" && network.local));
    assert!(manager_set
        .networks
        .iter()
        .any(|network| network.network_id == "other" && !network.local));
    let manager_peer = manager_set
        .peers
        .iter()
        .find(|peer| peer.node_id == 2)
        .unwrap();
    assert_eq!(manager_peer.public_key, "worker-key");
    assert_eq!(manager_peer.endpoint, None);
    assert_eq!(manager_peer.keepalive, 0);
    assert_eq!(
        manager_peer.path,
        minga_net::wireguard::WireGuardPeerPath::Direct
    );
    assert_eq!(manager_peer.allowed_ips, ["10.1.0.20/32"]);
    assert!(manager_set.peers.iter().any(|peer| peer.node_id == 3));

    let worker_set = decode(2);
    assert_eq!(worker_set.networks.len(), 2);
    assert!(worker_set
        .networks
        .iter()
        .any(|network| network.network_id == "shared" && network.local));
    assert!(worker_set.peers.iter().any(|peer| peer.node_id == 1));
    assert!(!worker_set.peers.iter().any(|peer| peer.node_id == 3));
    let manager_peer = worker_set
        .peers
        .iter()
        .find(|peer| peer.node_id == 1)
        .unwrap();
    assert_eq!(manager_peer.allowed_ips, ["10.1.0.10/32"]);
    assert_eq!(manager_peer.keepalive, 25);
    let network_entry = entries
        .iter()
        .find(|(key, _)| key == "net.shared")
        .expect("network assignment published");
    let network_assignment: minga_state::types::NetworkAssignment =
        minga_state::codec::decode_wire(&network_entry.1).unwrap();
    assert!(network_assignment
        .tasks
        .iter()
        .any(|task| task.service_name == "app.manager" && task.ip == "10.1.0.10"));
    assert!(network_assignment
        .tasks
        .iter()
        .any(|task| task.service_name == "app.worker" && task.ip == "10.1.0.20"));

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn rollout_uses_failed_status_from_bus_kv() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    let node = make_node(1, 8.0, 1 << 34);
    store.upsert_node(node.clone()).await.unwrap();

    let previous = make_service("app.web", 1);
    let mut service = make_service("app.web", 1);
    service.image = "nginx:2".to_string();
    service.update.failure_action = FailureAction::Rollback;
    service.previous = Some(Box::new(previous.clone()));
    let task = minga_scheduler::place_service(&service, &[node], &[], &[], 0).remove(0);
    store.upsert_service(service).await.unwrap();
    store.upsert_task(task.clone()).await.unwrap();

    let bus = MemoryBus::new();
    bus.refresh_lease(1, b"registration").await.unwrap();
    let status = TaskStatusReport {
        task_id: task.id.clone(),
        node_id: 1,
        state: TaskActualState::Failed,
        container_id: None,
        observed_at: 1,
        restarts: 0,
        last_error: Some("image pull failed".to_string()),
        last_exit_code: None,
        running_since_unix: None,
    };
    bus.put_status(
        &subjects::kv_node_task_status(task.node_id, &task.id),
        &status.to_bytes().unwrap(),
    )
    .await
    .unwrap();

    let scheduler = Scheduler::with_bus(store.clone(), bus);
    let actions = scheduler.reconcile_once().await.unwrap();
    assert!(matches!(
        actions.as_slice(),
        [minga_scheduler::ReconcileAction::RollbackService(id)] if id == "app.web"
    ));
    assert_eq!(
        store.list_services().await.unwrap()[0].image,
        previous.image
    );

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_creates_tasks_for_service() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    // Register two nodes.
    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store.upsert_node(make_node(2, 8.0, 1 << 34)).await.unwrap();

    // Deploy a service with 2 replicas.
    store
        .upsert_service(make_service("app.web", 2))
        .await
        .unwrap();

    // Run one reconcile pass.
    let scheduler = Scheduler::new(store.clone());
    let actions = scheduler.reconcile_once().await.unwrap();

    // Two tasks should have been created.
    assert_eq!(actions.len(), 2);

    // Verify tasks are in the state store.
    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 2);
    let ids: Vec<&str> = tasks.iter().map(|t| t.id.as_str()).collect();
    assert!(ids.iter().any(|id| id.starts_with("app.web.1.")));
    assert!(ids.iter().any(|id| id.starts_with("app.web.2.")));

    // Tasks should be spread across both nodes.
    let node_ids: Vec<u64> = tasks.iter().map(|t| t.node_id).collect();
    assert!(node_ids.contains(&1));
    assert!(node_ids.contains(&2));

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 1))
        .await
        .unwrap();

    let scheduler = Scheduler::new(store.clone());

    // First pass: creates the task.
    let actions = scheduler.reconcile_once().await.unwrap();
    assert_eq!(actions.len(), 1);

    // Second pass: no changes.
    let actions = scheduler.reconcile_once().await.unwrap();
    assert!(actions.is_empty(), "second reconcile should be a no-op");

    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 1);

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_removes_tasks_when_service_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 2))
        .await
        .unwrap();

    let scheduler = Scheduler::new(store.clone());

    // First pass: creates 2 tasks.
    scheduler.reconcile_once().await.unwrap();
    assert_eq!(store.list_tasks().await.unwrap().len(), 2);

    // Delete the service.
    store.remove_service("app.web".to_string()).await.unwrap();

    // Second pass: removes both tasks.
    let actions = scheduler.reconcile_once().await.unwrap();
    assert_eq!(actions.len(), 2);

    let tasks = store.list_tasks().await.unwrap();
    assert!(tasks.is_empty(), "all tasks should be removed");

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_handles_scale_down() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store.upsert_node(make_node(2, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 3))
        .await
        .unwrap();

    let scheduler = Scheduler::new(store.clone());

    // First pass: creates 3 tasks.
    scheduler.reconcile_once().await.unwrap();
    assert_eq!(store.list_tasks().await.unwrap().len(), 3);

    // Scale down to 1.
    store
        .upsert_service(make_service("app.web", 1))
        .await
        .unwrap();

    // Second pass: removes 2 tasks.
    let actions = scheduler.reconcile_once().await.unwrap();
    assert_eq!(actions.len(), 2, "should remove 2 excess tasks");

    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(tasks[0].id.starts_with("app.web.1."));

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_global_service() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store.upsert_node(make_node(2, 8.0, 1 << 34)).await.unwrap();
    store.upsert_node(make_node(3, 8.0, 1 << 34)).await.unwrap();

    let mut service = make_service("app.agent", 0);
    service.mode = ServiceMode::Global;
    store.upsert_service(service).await.unwrap();

    let scheduler = Scheduler::new(store.clone());
    let actions = scheduler.reconcile_once().await.unwrap();

    // Global: one task per node.
    assert_eq!(actions.len(), 3);

    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 3);
    let node_ids: Vec<u64> = tasks.iter().map(|t| t.node_id).collect();
    assert_eq!(node_ids, vec![1, 2, 3]);

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_no_nodes_no_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    // No nodes registered, but a service is deployed.
    store
        .upsert_service(make_service("app.web", 2))
        .await
        .unwrap();

    let scheduler = Scheduler::new(store.clone());
    let actions = scheduler.reconcile_once().await.unwrap();

    // No eligible nodes → no tasks created.
    assert!(actions.is_empty());
    assert!(store.list_tasks().await.unwrap().is_empty());

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn reconcile_loop_runs_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    store.upsert_node(make_node(1, 8.0, 1 << 34)).await.unwrap();
    store
        .upsert_service(make_service("app.web", 1))
        .await
        .unwrap();

    let scheduler = Scheduler::new(store.clone());

    // Run a few reconcile passes with a short interval.
    for _ in 0..3 {
        let _ = scheduler.reconcile_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // After convergence, exactly 1 task exists and further passes are no-ops.
    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 1);

    let actions = scheduler.reconcile_once().await.unwrap();
    assert!(actions.is_empty());

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn nat_workers_relay_through_the_lowest_manager_and_fail_over_after_lease_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    let mut manager_a = make_node(1, 8.0, 1 << 34);
    manager_a.role = NodeRole::Manager;
    manager_a.wg_public_key = Some("manager-a-key".into());
    manager_a.wg_endpoint = Some("192.0.2.1:51820".into());
    let mut manager_b = make_node(4, 8.0, 1 << 34);
    manager_b.role = NodeRole::Manager;
    manager_b.wg_public_key = Some("manager-b-key".into());
    manager_b.wg_endpoint = Some("192.0.2.4:51820".into());
    let mut worker_a = make_node(2, 8.0, 1 << 34);
    worker_a.wg_public_key = Some("worker-a-key".into());
    let mut worker_b = make_node(3, 8.0, 1 << 34);
    worker_b.wg_public_key = Some("worker-b-key".into());
    let mut worker_c = make_node(5, 8.0, 1 << 34);
    worker_c.wg_public_key = Some("worker-c-key".into());
    worker_c.wg_endpoint = Some("192.0.2.5:51820".into());
    for node in [
        manager_a,
        manager_b,
        worker_a.clone(),
        worker_b.clone(),
        worker_c.clone(),
    ] {
        store.upsert_node(node).await.unwrap();
    }
    store
        .upsert_network(Network {
            id: "shared".into(),
            name: "shared".into(),
            driver: "overlay".into(),
            attachable: false,
            subnet: Some("10.1.0.0/24".into()),
            wg_port: None,
        })
        .await
        .unwrap();

    for (service_id, worker, ip) in [
        ("app.a", worker_a, "10.1.0.20"),
        ("app.b", worker_b, "10.1.0.30"),
        ("app.c", worker_c, "10.1.0.40"),
    ] {
        let mut service = make_service(service_id, 1);
        service.networks = vec!["shared".into()];
        store.upsert_service(service.clone()).await.unwrap();
        let mut task = minga_scheduler::place_service(&service, &[worker], &[], &[], 0).remove(0);
        task.spec.ips.insert("shared".into(), ip.into());
        store.upsert_task(task).await.unwrap();
    }

    let bus = MemoryBus::with_lease_ttl(Duration::from_secs(1));
    for id in [1, 2, 3, 4, 5] {
        bus.refresh_lease(id, b"registration").await.unwrap();
    }
    let scheduler = Scheduler::with_bus(store.clone(), bus.clone());
    scheduler.reconcile_once().await.unwrap();

    let decode = |entries: &[(String, Vec<u8>)], id| {
        let value = entries
            .iter()
            .find(|(key, _)| key == &format!("node.{id}.wg"))
            .expect("peer set published");
        minga_state::codec::decode_wire::<minga_net::wireguard::WireGuardPeerSet>(&value.1).unwrap()
    };
    let entries = bus.list_assignments("").await.unwrap();
    let relay = decode(&entries, 1);
    assert!(relay
        .networks
        .iter()
        .any(|network| network.network_id == "shared" && network.relay));
    assert_eq!(
        relay
            .peers
            .iter()
            .find(|peer| peer.node_id == 2)
            .unwrap()
            .allowed_ips,
        ["10.1.0.20/32"]
    );
    assert_eq!(
        relay
            .peers
            .iter()
            .find(|peer| peer.node_id == 2)
            .unwrap()
            .path,
        minga_net::wireguard::WireGuardPeerPath::Relay
    );
    let worker = decode(&entries, 2);
    let manager_peer = worker.peers.iter().find(|peer| peer.node_id == 1).unwrap();
    assert_eq!(
        manager_peer.path,
        minga_net::wireguard::WireGuardPeerPath::Relay
    );
    assert_eq!(manager_peer.allowed_ips, ["10.1.0.30/32"]);
    assert!(!worker.peers.iter().any(|peer| peer.node_id == 3));
    let direct_peer = worker.peers.iter().find(|peer| peer.node_id == 5).unwrap();
    assert_eq!(
        direct_peer.path,
        minga_net::wireguard::WireGuardPeerPath::Direct
    );
    assert_eq!(direct_peer.endpoint.as_deref(), Some("192.0.2.5:51820"));
    assert_eq!(direct_peer.allowed_ips, ["10.1.0.40/32"]);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for id in [2, 3, 4, 5] {
        bus.refresh_lease(id, b"registration").await.unwrap();
    }
    scheduler.reconcile_once().await.unwrap();

    let entries = bus.list_assignments("").await.unwrap();
    let worker = decode(&entries, 2);
    let manager_peer = worker
        .peers
        .iter()
        .find(|peer| peer.node_id == 4)
        .expect("standby manager becomes home");
    assert_eq!(
        manager_peer.path,
        minga_net::wireguard::WireGuardPeerPath::Relay
    );
    assert_eq!(manager_peer.allowed_ips, ["10.1.0.30/32"]);
    assert!(!worker.peers.iter().any(|peer| peer.node_id == 1));
    assert!(!entries.iter().any(|(key, _)| key == "node.1.wg"));

    drop(scheduler);
    Arc::try_unwrap(store)
        .ok()
        .expect("scheduler dropped")
        .shutdown()
        .await
        .unwrap();
}
