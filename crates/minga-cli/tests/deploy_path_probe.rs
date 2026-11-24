//! Regression test for the cluster-membership path (roadmap Gate 1).
//!
//! This test pins the *shape* of the deployment path:
//!
//!   1. a node publishes its identity in a JetStream lease and the leader
//!      registers it from that bus payload,
//!   2. a service created through the Docker-compatible API lands in Raft,
//!   3. the scheduler places tasks and the supervisor starts them,
//!   4. with no registered node, service creation fails loudly (409)
//!      instead of succeeding silently with zero tasks running.
//!
//! See `docs/roadmap-v0.1.0.md` (Gate 1) for the release gate this
//! belongs to.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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
        tenant_id: "default".into(),
        address: format!("127.0.0.1:{}", 50051 + id),
        labels: Default::default(),
        capacity: ResourceSpec {
            cpus: 8.0,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            gpus: 0,
        },
        role: Default::default(),
        reserved_ports: Vec::new(),
        wg_public_key: None,
        wg_endpoint: None,
        join_token: None,
    }
}

async fn register_node(
    store: Arc<StateStore>,
    bus: Arc<MemoryBus>,
    identity: NodeIdentity,
) -> (minga_node::NodeMembership, tokio::task::JoinHandle<()>) {
    let membership = minga_node::NodeMembership::start(bus.clone(), identity)
        .await
        .unwrap();
    let registration = tokio::spawn(minga_node::run_node_registration(store.clone(), bus, None));
    tokio::time::timeout(Duration::from_secs(3), async {
        while store.get_node(1).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("node was not registered from its bus lease");
    (membership, registration)
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

    let bus = MemoryBus::new();
    let (membership, registration) = register_node(store.clone(), bus.clone(), identity(1)).await;

    let ready_state = Arc::new(minga_telemetry::ReadyState::new());
    let router = minga_api::server::build_router_with_bus_and_auth(
        store.clone(),
        bus.clone(),
        ready_state,
        None,
    );

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
    registration.abort();
    membership.leave().await;
}

#[tokio::test]
async fn service_creation_fails_loudly_when_no_node_is_registered() {
    let (store, _dir) = store().await;

    let ready_state = Arc::new(minga_telemetry::ReadyState::new());
    let router = minga_api::server::build_router_with_bus_and_auth(
        store.clone(),
        MemoryBus::new(),
        ready_state,
        None,
    );

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

    let bus = MemoryBus::new();
    let (membership, registration) = register_node(store.clone(), bus.clone(), identity(1)).await;
    assert_eq!(store.list_nodes().await.unwrap().len(), 1);

    let ready_state = Arc::new(minga_telemetry::ReadyState::new());
    let router = minga_api::server::build_router_with_bus_and_auth(
        store.clone(),
        bus.clone(),
        ready_state,
        None,
    );
    let status = post_service(router, service_spec("probe_web", "nginx:1.27", 2)).await;
    assert_eq!(status, 201);

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
    let supervisor = minga_node::Supervisor::with_tenant_bus(
        runtime.clone() as Arc<dyn minga_node::Runtime>,
        bus,
        1,
        "default",
    );
    let supervisor_actions = supervisor.reconcile_once().await.unwrap();
    assert_eq!(supervisor_actions, 2);
    assert_eq!(
        runtime.container_count(),
        2,
        "both tasks must be running under the runtime"
    );
    registration.abort();
    membership.leave().await;
}

const SHOP: &str = r#"
services:
  web:
    image: nginx:1.27
  db:
    image: postgres:16
"#;

/// Deploys `SHOP` as stack `shop` the way `minga deploy` does (the same
/// translation, the same request bodies, the service `Name` being
/// `<stack>.<service>`) as a caller of `tenant`, and returns the service
/// names the scheduler sends the node for DNS on the stack's network.
async fn deployed_dns_names(tenant: &str) -> Vec<(String, String)> {
    use axum::body::Body;
    use axum::http::Request;
    use minga_bus::EventBus;
    use tower::ServiceExt;

    let (store, _dir) = store().await;
    if tenant != "default" {
        store
            .create_tenant(minga_state::types::Tenant {
                id: tenant.into(),
                name: tenant.into(),
                quotas: Default::default(),
                nats_revoked_user_public_keys: Vec::new(),
                created_at: 1,
                generation: 0,
            })
            .await
            .unwrap();
    }
    store
        .upsert_node(identity(1).cluster_node_for_tenant(tenant))
        .await
        .unwrap();
    let bus = MemoryBus::new();
    bus.refresh_tenant_lease(tenant, 1, b"registration")
        .await
        .unwrap();
    // A node of another tenant gets tasks only once its join was accepted.
    bus.put_tenant_status(
        tenant,
        &minga_bus::subjects::kv_node_join(tenant, 1),
        tenant.as_bytes(),
    )
    .await
    .unwrap();
    let router = minga_api::server::build_router_with_bus_and_auth(
        store.clone(),
        bus.clone(),
        Arc::new(minga_telemetry::ReadyState::new()),
        None,
    );

    let loaded = minga_compose::load(SHOP, &minga_compose::LoadOptions::default()).unwrap();
    let translated = minga_scheduler::translate_project(&loaded.project, "shop", None).unwrap();
    let mut requests = Vec::new();
    for net in &translated.networks {
        requests.push((
            "networks/create",
            serde_json::json!({
                "Name": net.name,
                "Driver": net.driver,
                "Attachable": net.attachable,
                "Labels": {"com.docker.stack.namespace": "shop"},
            }),
        ));
    }
    for svc in &translated.services {
        let spec = minga_api::translate::service_to_docker(svc).Spec;
        requests.push(("services/create", serde_json::to_value(&spec).unwrap()));
    }
    for (path, body) in requests {
        let mut req = Request::builder()
            .method("POST")
            .uri(format!("/v1.41/{path}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        req.extensions_mut()
            .insert(minga_api::auth::ClientIdentity {
                name: "deployer".into(),
                role: Some(minga_api::auth::ApiRole::Deployer),
                tenant: Some(tenant.into()),
            });
        let resp = router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let reply = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let reply = String::from_utf8_lossy(&reply);
        assert_eq!(status, 201, "{path} as {tenant}: {reply}");
    }

    Scheduler::with_bus(store.clone(), bus.clone())
        .reconcile_once()
        .await
        .unwrap();
    let prefix = minga_bus::subjects::kv_assignment_node_network_prefix(tenant, 1);
    let mut names: Vec<(String, String)> = bus
        .list_assignments("")
        .await
        .unwrap()
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .flat_map(|(_, value)| {
            minga_state::codec::decode_wire::<minga_state::types::NetworkAssignment>(value)
                .unwrap()
                .tasks
        })
        .map(|task| (task.service_name, task.stack))
        .collect();
    names.sort();
    names
}

/// Regression for #193: v1.3.0-rc.1 sent the node `shop.db`, so `db`,
/// `tasks.db` and `db.shop.minga.local` did not resolve.
#[tokio::test]
async fn a_deployed_service_is_assigned_its_bare_compose_name_for_dns() {
    let bare = [("db", "shop"), ("web", "shop")].map(|(n, s)| (n.to_string(), s.to_string()));
    assert_eq!(deployed_dns_names("default").await, bare);
}

/// A tenant other than `default` has the ID `<tenant>.<stack>.<service>`;
/// the DNS name is still the bare Compose name.
#[tokio::test]
async fn a_tenants_deployed_service_is_assigned_its_bare_compose_name_for_dns() {
    let bare = [("db", "shop"), ("web", "shop")].map(|(n, s)| (n.to_string(), s.to_string()));
    assert_eq!(deployed_dns_names("acme").await, bare);
}
