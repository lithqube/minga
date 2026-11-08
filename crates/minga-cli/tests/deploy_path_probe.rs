//! Regression test for the cluster-membership path (roadmap Gate 1).
//!
//! This test pins the *shape* of the deployment path:
//!
//!   1. a node registers itself through the production registration path
//!      ([`minga_node::register_self`]), not through test scaffolding,
//!   2. a service created through the Docker-compatible API lands in Raft,
//!   3. the scheduler places tasks and the supervisor starts them,
//!   4. with no registered node, service creation fails loudly (409)
//!      instead of succeeding silently with zero tasks running.
//!
//! See `docs/roadmap-v0.1.0.md` (Gate 1) for the release gate this
//! belongs to.

use std::collections::BTreeMap;
use std::sync::Arc;

use minga_bus::MemoryBus;
use minga_node::NodeIdentity;
use minga_scheduler::Scheduler;
use minga_state::types::ResourceSpec;
use minga_state::StateStore;

/// Starts a single-node state store (which becomes its own leader).
async fn store() -> (Arc<StateStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    (store, dir)
}

/// The identity a real node would announce with these settings.
fn identity(id: u64) -> NodeIdentity {
    NodeIdentity {
        node_id: id,
        address: format!("127.0.0.1:{}", 50051 + id),
        labels: Default::default(),
        capacity: ResourceSpec {
            cpus: 8.0,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            gpus: 0,
        },
        role: Default::default(),
    }
}

/// A service spec equivalent to what `docker stack deploy` POSTs.
fn service_spec(name: &str, image: &str, replicas: u32) -> serde_json::Value {
    serde_json::json!({
        "Name": name,
        "Labels": {"com.docker.stack.namespace": "probe"},
        "TaskTemplate": {
            "ContainerSpec": {
                "Image": image,
                "Env": ["FOO=bar"],
                "Command": ["nginx"],
                "Args": ["-g", "daemon off;"]
            },
            "Resources": {
                "Limits": {"NanoCPUs": 500_000_000, "MemoryBytes": 268_435_456}
            }
        },
        "Mode": {"Replicated": {"Replicas": replicas}}
    })
}

/// POSTs a service spec to the Docker-compatible API and returns the HTTP
/// status. This is the exact request `docker stack deploy` makes.
async fn post_service(router: axum::Router, spec: serde_json::Value) -> u16 {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let req = Request::builder()
        .method("POST")
        .uri("/v1.41/services/create")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&spec).unwrap()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    resp.status().as_u16()
}

#[tokio::test]
async fn docker_api_service_creation_lands_in_raft() {
    let (store, _dir) = store().await;

    // The node registers itself, exactly as `minga manager` does at startup.
    minga_node::register_self(&store, &identity(1))
        .await
        .unwrap();

    let ready_state = Arc::new(minga_telemetry::ReadyState::new());
    let router = minga_api::build_router(store.clone(), ready_state);

    let status = post_service(router, service_spec("probe_web", "nginx:1.27", 2)).await;
    assert_eq!(status, 201, "the API should accept a valid service spec");

    let services = store.list_services().await.unwrap();
    assert_eq!(services.len(), 1, "the service must reach Raft state");

    let svc = &services[0];
    assert_eq!(svc.id, "probe_web");
    assert_eq!(svc.image, "nginx:1.27");
    assert_eq!(svc.replicas, 2);
    assert_eq!(svc.env.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(
        svc.env.get("MINGA_STACK").map(String::as_str),
        Some("probe"),
        "the stack namespace label must be carried into the environment"
    );
}

#[tokio::test]
async fn service_creation_fails_loudly_when_no_node_is_registered() {
    let (store, _dir) = store().await;

    let ready_state = Arc::new(minga_telemetry::ReadyState::new());
    let router = minga_api::build_router(store.clone(), ready_state);

    // Nobody registered: the cluster has zero nodes. The deploy must be a
    // hard failure, not a silent success with nothing running.
    let status = post_service(router, service_spec("probe_web", "nginx:1.27", 2)).await;
    assert_eq!(
        status, 409,
        "a service that matches zero eligible nodes must be rejected"
    );
    assert!(
        store.list_services().await.unwrap().is_empty(),
        "a rejected service must not reach Raft state"
    );
    assert!(store.list_tasks().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_registered_node_lets_the_service_run_end_to_end() {
    let (store, _dir) = store().await;

    // Register the node through the production path — the same call
    // `minga manager`/`minga worker` makes at startup.
    minga_node::register_self(&store, &identity(1))
        .await
        .unwrap();
    assert_eq!(store.list_nodes().await.unwrap().len(), 1);

    let ready_state = Arc::new(minga_telemetry::ReadyState::new());
    let router = minga_api::build_router(store.clone(), ready_state);
    let status = post_service(router, service_spec("probe_web", "nginx:1.27", 2)).await;
    assert_eq!(status, 201);

    let bus = MemoryBus::new();
    let scheduler = Scheduler::with_bus(store.clone(), bus.clone());
    let actions = scheduler.reconcile_once().await.unwrap();
    assert_eq!(
        actions.len(),
        2,
        "both replicas must be placed, got {actions:?}"
    );

    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 2);
    for task in &tasks {
        assert_eq!(task.node_id, 1);
        assert_eq!(task.spec.image, "nginx:1.27");
    }

    // The supervisor then starts them.
    let runtime = Arc::new(minga_node::MockRuntime::new());
    let supervisor = minga_node::Supervisor::with_bus(
        store.clone(),
        runtime.clone() as Arc<dyn minga_node::Runtime>,
        bus,
        1,
    );
    let supervisor_actions = supervisor.reconcile_once().await.unwrap();
    assert_eq!(supervisor_actions, 2);
    assert_eq!(
        runtime.container_count(),
        2,
        "both tasks must be running under the runtime"
    );
}
