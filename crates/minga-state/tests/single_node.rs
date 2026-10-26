//! Integration tests for a single-node StateStore.
//!
//! Each test creates a StateStore in a temp directory, exercises it,
//! and shuts it down. Tests run in parallel (separate temp dirs).

use std::collections::{BTreeMap, HashMap};

use minga_state::types::{
    ClusterNode, Config, Network, NodeStatus, PortMapping, ResourceSpec, Secret, Service,
    ServiceMode, Task, TaskSpec, TaskState, Volume,
};
use minga_state::StateStore;

/// Unique port counter for parallel test isolation.
static PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(51000);

/// Creates a StateStore in a unique temp directory with a unique RPC port.
async fn test_store(test_name: &str) -> (StateStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(test_name);
    let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let addr = format!("127.0.0.1:{port}");
    let store = StateStore::open(1, &addr, &path, BTreeMap::new())
        .await
        .unwrap();
    (store, dir)
}

fn sample_node(id: u64) -> ClusterNode {
    ClusterNode {
        id,
        address: format!("10.0.0.{id}:50051"),
        labels: HashMap::from([("gpu".to_string(), "true".to_string())]),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus: 8.0,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            gpus: 1,
        },
    }
}

fn sample_service(id: &str) -> Service {
    Service {
        id: id.to_string(),
        name: id.to_string(),
        image: format!("registry.local/{id}:1.0"),
        replicas: 2,
        mode: ServiceMode::Replicated,
        constraints: vec!["node.labels.gpu == true".to_string()],
        env: HashMap::from([("LOG_LEVEL".to_string(), "info".to_string())]),
        ports: vec![PortMapping {
            target: 8080,
            published: Some(443),
            protocol: "tcp".to_string(),
        }],
        networks: vec!["backplane".to_string()],
        resources: ResourceSpec {
            cpus: 2.0,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            gpus: 0,
        },
    }
}

fn sample_task(id: &str, service_id: &str, node_id: u64) -> Task {
    Task {
        id: id.to_string(),
        service_id: service_id.to_string(),
        node_id,
        state: TaskState::Assigned,
        spec: TaskSpec {
            image: format!("registry.local/{service_id}:1.0"),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
        },
    }
}

#[tokio::test]
async fn elects_itself_leader() {
    let (store, _dir) = test_store("elects_itself_leader").await;
    assert!(store.is_leader().await);
    assert_eq!(store.current_leader().await, Some(1));
    assert_eq!(store.node_id(), 1);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_node() {
    let (store, _dir) = test_store("write_and_read_node").await;

    let node = sample_node(1);
    store.upsert_node(node.clone()).await.unwrap();

    let read = store.get_node(1).await.unwrap().unwrap();
    assert_eq!(read, node);

    let nodes = store.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0], node);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn remove_node() {
    let (store, _dir) = test_store("remove_node").await;

    store.upsert_node(sample_node(1)).await.unwrap();
    store.upsert_node(sample_node(2)).await.unwrap();
    assert_eq!(store.list_nodes().await.unwrap().len(), 2);

    store.remove_node(1).await.unwrap();
    assert!(store.get_node(1).await.unwrap().is_none());
    assert!(store.get_node(2).await.unwrap().is_some());
    assert_eq!(store.list_nodes().await.unwrap().len(), 1);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_service() {
    let (store, _dir) = test_store("write_and_read_service").await;

    let svc = sample_service("inference");
    store.upsert_service(svc.clone()).await.unwrap();

    let read = store.get_service("inference").await.unwrap().unwrap();
    assert_eq!(read, svc);

    let services = store.list_services().await.unwrap();
    assert_eq!(services.len(), 1);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_task() {
    let (store, _dir) = test_store("write_and_read_task").await;

    let task = sample_task("task-1", "inference", 1);
    store.upsert_task(task.clone()).await.unwrap();

    let read = store.get_task("task-1").await.unwrap().unwrap();
    assert_eq!(read, task);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_network_volume_secret_config() {
    let (store, _dir) = test_store("write_and_read_all_types").await;

    let net = Network {
        id: "backplane".to_string(),
        name: "backplane".to_string(),
        driver: "overlay".to_string(),
        attachable: true,
    };
    store.upsert_network(net.clone()).await.unwrap();
    assert_eq!(store.get_network("backplane").await.unwrap(), Some(net));

    let vol = Volume {
        id: "models".to_string(),
        name: "models".to_string(),
        driver: "local".to_string(),
    };
    store.upsert_volume(vol.clone()).await.unwrap();
    assert_eq!(store.get_volume("models").await.unwrap(), Some(vol));

    let secret = Secret {
        id: "api_token".to_string(),
        name: "api_token".to_string(),
    };
    store.upsert_secret(secret.clone()).await.unwrap();
    assert_eq!(store.get_secret("api_token").await.unwrap(), Some(secret));

    let config = Config {
        id: "agent_config".to_string(),
        name: "agent_config".to_string(),
    };
    store.upsert_config(config.clone()).await.unwrap();
    assert_eq!(
        store.get_config("agent_config").await.unwrap(),
        Some(config)
    );

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_writes_in_sequence() {
    let (store, _dir) = test_store("multiple_writes").await;

    for i in 1..=5 {
        store.upsert_node(sample_node(i)).await.unwrap();
    }
    assert_eq!(store.list_nodes().await.unwrap().len(), 5);

    for i in 1..=3 {
        store
            .upsert_service(sample_service(&format!("svc-{i}")))
            .await
            .unwrap();
    }
    assert_eq!(store.list_services().await.unwrap().len(), 3);

    for i in 1..=10 {
        store
            .upsert_task(sample_task(&format!("task-{i}"), "svc-1", 1))
            .await
            .unwrap();
    }
    assert_eq!(store.list_tasks().await.unwrap().len(), 10);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn state_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persistence");
    let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let addr = format!("127.0.0.1:{port}");

    // First run: write some data.
    {
        let store = StateStore::open(1, &addr, &path, BTreeMap::new())
            .await
            .unwrap();
        store.upsert_node(sample_node(1)).await.unwrap();
        store
            .upsert_service(sample_service("inference"))
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    // Second run: data should still be there.
    {
        let store = StateStore::open(1, &addr, &path, BTreeMap::new())
            .await
            .unwrap();
        let node = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(node.id, 1);
        assert_eq!(node.status, NodeStatus::Active);

        let svc = store.get_service("inference").await.unwrap().unwrap();
        assert_eq!(svc.replicas, 2);

        store.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn upsert_overwrites_existing() {
    let (store, _dir) = test_store("upsert_overwrites").await;

    let mut node = sample_node(1);
    store.upsert_node(node.clone()).await.unwrap();

    // Update the node.
    node.status = NodeStatus::Draining;
    node.labels.insert("zone".to_string(), "a".to_string());
    store.upsert_node(node.clone()).await.unwrap();

    let read = store.get_node(1).await.unwrap().unwrap();
    assert_eq!(read.status, NodeStatus::Draining);
    assert_eq!(read.labels.get("zone").map(String::as_str), Some("a"));
    assert_eq!(store.list_nodes().await.unwrap().len(), 1);

    store.shutdown().await.unwrap();
}
