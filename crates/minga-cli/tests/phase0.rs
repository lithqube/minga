//! Phase 0 gate: a 10-service Compose stack runs on 3 nodes.
//!
//! This is the end-to-end integration test that proves the full Minga
//! pipeline works:
//!
//! 1. Three nodes form a Raft cluster (multi-node consensus).
//! 2. Three cluster nodes are registered in the state store.
//! 3. A 10-service Compose file is loaded and translated to state entities.
//! 4. Services are written to the Raft state (via the leader).
//! 5. The scheduler places tasks across all three nodes.
//! 6. Each node's supervisor picks up its assigned tasks and starts
//!    containers (via MockRuntime).
//! 7. We verify: tasks are distributed across all 3 nodes, and each
//!    node's supervisor has running containers for its assigned tasks.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use minga_node::{MockRuntime, Supervisor};
use minga_scheduler::Scheduler;
use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec};
use minga_state::StateStore;

static PORT: AtomicU16 = AtomicU16::new(53000);

fn next_addr() -> String {
    let port = PORT.fetch_add(1, Ordering::SeqCst);
    format!("127.0.0.1:{port}")
}

fn cluster_node(id: u64, addr: &str) -> ClusterNode {
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

/// A 10-service Compose stack for the Phase 0 gate.
const COMPOSE_10: &str = r#"
name: plant-7
services:
  inference:
    image: registry.local/vllm:0.6.3
    deploy:
      replicas: 1
      resources:
        limits:
          cpus: "2.0"
          memory: 4G
  agent-api:
    image: registry.local/agent-api:1.4.0
    ports:
      - "443:8080"
    deploy:
      replicas: 2
    depends_on:
      - inference
  sensors:
    image: registry.local/sensor-reader:2.1
    deploy:
      mode: global
  debug-ui:
    image: registry.local/debug-ui:latest
    ports:
      - "9000:9000"
    deploy:
      replicas: 1
  db:
    image: postgres:16
    deploy:
      replicas: 1
  cache:
    image: redis:7
    deploy:
      replicas: 1
  worker:
    image: registry.local/worker:2.0
    deploy:
      replicas: 2
    depends_on:
      - db
      - cache
  gateway:
    image: registry.local/gateway:1.0
    ports:
      - "80:80"
    deploy:
      replicas: 1
    depends_on:
      - agent-api
  monitor:
    image: registry.local/monitor:0.9
    deploy:
      replicas: 1
  logger:
    image: registry.local/logger:1.0
    deploy:
      mode: global
"#;

#[tokio::test]
async fn phase0_gate_10_services_3_nodes() {
    // --- 1. Start a 3-node Raft cluster ---------------------------------------
    let addr1 = next_addr();
    let addr2 = next_addr();
    let addr3 = next_addr();

    let mut peers1: BTreeMap<u64, String> = BTreeMap::new();
    peers1.insert(2, addr2.clone());
    peers1.insert(3, addr3.clone());

    let mut peers2: BTreeMap<u64, String> = BTreeMap::new();
    peers2.insert(1, addr1.clone());
    peers2.insert(3, addr3.clone());

    let mut peers3: BTreeMap<u64, String> = BTreeMap::new();
    peers3.insert(1, addr1.clone());
    peers3.insert(2, addr2.clone());

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let dir3 = tempfile::tempdir().unwrap();

    let (r1, r2, r3) = tokio::join!(
        StateStore::open(1, &addr1, dir1.path(), peers1),
        StateStore::open(2, &addr2, dir2.path(), peers2),
        StateStore::open(3, &addr3, dir3.path(), peers3),
    );
    let store1 = Arc::new(r1.expect("node 1 should start"));
    let store2 = Arc::new(r2.expect("node 2 should start"));
    let store3 = Arc::new(r3.expect("node 3 should start"));
    std::mem::forget(dir1);
    std::mem::forget(dir2);
    std::mem::forget(dir3);

    // --- 2. Find the leader ---------------------------------------------------

    let mut leader: Option<&Arc<StateStore>> = None;
    for store in [&store1, &store2, &store3] {
        if store.is_leader().await {
            leader = Some(store);
            break;
        }
    }
    let leader = leader.expect("a leader should be elected");
    tracing::info!("leader elected");

    // --- 3. Register 3 cluster nodes ------------------------------------------
    leader.upsert_node(cluster_node(1, &addr1)).await.unwrap();
    leader.upsert_node(cluster_node(2, &addr2)).await.unwrap();
    leader.upsert_node(cluster_node(3, &addr3)).await.unwrap();

    // Wait for replication.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for store in [&store1, &store2, &store3] {
        let nodes = store.list_nodes().await.unwrap();
        assert_eq!(nodes.len(), 3, "all nodes should see 3 registered nodes");
    }

    // --- 4. Load and translate the 10-service Compose stack -------------------
    let loaded = minga_compose::load(COMPOSE_10, &minga_compose::LoadOptions::default())
        .expect("compose file should load");
    assert_eq!(loaded.project.services.len(), 10, "expected 10 services");

    let translated =
        minga_scheduler::translate_project(&loaded.project, "plant-7").expect("translation");
    assert_eq!(translated.services.len(), 10);

    // --- 5. Write services to Raft --------------------------------------------
    for svc in &translated.services {
        leader.upsert_service(svc.clone()).await.unwrap();
    }
    for net in &translated.networks {
        leader.upsert_network(net.clone()).await.unwrap();
    }

    // Wait for replication.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for store in [&store1, &store2, &store3] {
        let services = store.list_services().await.unwrap();
        assert_eq!(services.len(), 10, "all nodes should see 10 services");
    }

    // --- 6. Run the scheduler (places tasks across 3 nodes) -------------------
    let scheduler = Scheduler::new(leader.clone());
    let actions = scheduler.reconcile_once().await.unwrap();
    assert!(!actions.is_empty(), "scheduler should create tasks");
    tracing::info!(actions = actions.len(), "scheduler created tasks");

    // Wait for task replication.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- 7. Verify tasks are distributed across all 3 nodes -------------------
    let all_tasks = leader.list_tasks().await.unwrap();

    // Count expected tasks:
    // - inference: 1 replica
    // - agent-api: 2 replicas
    // - sensors: global → 3 (one per node)
    // - debug-ui: 1 replica
    // - db: 1 replica
    // - cache: 1 replica
    // - worker: 2 replicas
    // - gateway: 1 replica
    // - monitor: 1 replica
    // - logger: global → 3 (one per node)
    // Total: 1+2+3+1+1+1+2+1+1+3 = 16 tasks
    let expected_tasks: u32 = 1 + 2 + 3 + 1 + 1 + 1 + 2 + 1 + 1 + 3;
    assert_eq!(
        all_tasks.len() as u32,
        expected_tasks,
        "expected {expected_tasks} tasks, got {}",
        all_tasks.len()
    );

    // Tasks should be on all 3 nodes.
    let nodes_with_tasks: std::collections::HashSet<u64> =
        all_tasks.iter().map(|t| t.node_id).collect();
    assert_eq!(
        nodes_with_tasks.len(),
        3,
        "tasks should be distributed across all 3 nodes, got nodes: {nodes_with_tasks:?}"
    );

    // Global services should have one task per node.
    let sensors_tasks: Vec<_> = all_tasks
        .iter()
        .filter(|t| t.service_id == "plant-7.sensors")
        .collect();
    assert_eq!(
        sensors_tasks.len(),
        3,
        "sensors (global) should have 3 tasks"
    );
    let logger_tasks: Vec<_> = all_tasks
        .iter()
        .filter(|t| t.service_id == "plant-7.logger")
        .collect();
    assert_eq!(logger_tasks.len(), 3, "logger (global) should have 3 tasks");

    // --- 8. Run supervisors on each node --------------------------------------
    let runtime1 = Arc::new(MockRuntime::new());
    let runtime2 = Arc::new(MockRuntime::new());
    let runtime3 = Arc::new(MockRuntime::new());

    let sup1 = Supervisor::new(store1.clone(), runtime1.clone(), 1);
    let sup2 = Supervisor::new(store2.clone(), runtime2.clone(), 2);
    let sup3 = Supervisor::new(store3.clone(), runtime3.clone(), 3);

    let (a1, a2, a3) = tokio::join!(
        sup1.reconcile_once(),
        sup2.reconcile_once(),
        sup3.reconcile_once(),
    );
    let total_started = a1.unwrap() + a2.unwrap() + a3.unwrap();
    assert_eq!(
        total_started,
        all_tasks.len(),
        "supervisors should start all {0} tasks",
        all_tasks.len()
    );

    // --- 9. Verify containers are running on each node ------------------------
    // Each node should have containers for its assigned tasks.
    let tasks_per_node: HashMap<u64, usize> = {
        let mut map: HashMap<u64, usize> = HashMap::new();
        for t in &all_tasks {
            *map.entry(t.node_id).or_insert(0) += 1;
        }
        map
    };

    assert_eq!(
        runtime1.container_count(),
        tasks_per_node.get(&1).copied().unwrap_or(0)
    );
    assert_eq!(
        runtime2.container_count(),
        tasks_per_node.get(&2).copied().unwrap_or(0)
    );
    assert_eq!(
        runtime3.container_count(),
        tasks_per_node.get(&3).copied().unwrap_or(0)
    );

    // Total containers across all nodes should equal total tasks.
    let total_containers =
        runtime1.container_count() + runtime2.container_count() + runtime3.container_count();
    assert_eq!(total_containers, all_tasks.len());

    // --- 10. Verify idempotency: re-running everything is a no-op -------------
    let actions = scheduler.reconcile_once().await.unwrap();
    assert!(actions.is_empty(), "scheduler should be idempotent");

    let (a1, a2, a3) = tokio::join!(
        sup1.reconcile_once(),
        sup2.reconcile_once(),
        sup3.reconcile_once(),
    );
    assert_eq!(
        a1.unwrap() + a2.unwrap() + a3.unwrap(),
        0,
        "supervisors should be idempotent"
    );

    tracing::info!(
        tasks = all_tasks.len(),
        nodes = 3,
        containers = total_containers,
        "Phase 0 gate PASSED: 10-service stack running on 3 nodes"
    );
}
