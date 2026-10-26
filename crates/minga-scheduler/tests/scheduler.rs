//! Integration tests for the Scheduler with a real StateStore.
//!
//! These tests open a single-node Raft state store, register nodes and
//! services, run the scheduler's reconcile loop, and verify that tasks
//! appear in the state store.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use minga_scheduler::Scheduler;
use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec, Service, ServiceMode};
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
    }
}

#[tokio::test]
async fn reconcile_creates_tasks_for_service() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:50051", dir.path(), BTreeMap::new())
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
    assert!(ids.contains(&"app.web.1"));
    assert!(ids.contains(&"app.web.2"));

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
        StateStore::open(1, "127.0.0.1:50052", dir.path(), BTreeMap::new())
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
        StateStore::open(1, "127.0.0.1:50053", dir.path(), BTreeMap::new())
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
        StateStore::open(1, "127.0.0.1:50054", dir.path(), BTreeMap::new())
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
    assert_eq!(tasks[0].id, "app.web.1");

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
        StateStore::open(1, "127.0.0.1:50055", dir.path(), BTreeMap::new())
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
        StateStore::open(1, "127.0.0.1:50056", dir.path(), BTreeMap::new())
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
        StateStore::open(1, "127.0.0.1:50057", dir.path(), BTreeMap::new())
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
