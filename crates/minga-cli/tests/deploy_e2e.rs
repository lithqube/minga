//! End-to-end test for `minga deploy` (roadmap Gate 2).
//!
//! Drives the real `minga` binary against a real `minga api` server: the
//! Compose file is translated, written through the Docker Engine API into
//! Raft, and the scheduler places the tasks — the same chain a live
//! cluster runs. Asserts on the state store, not on printed output.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use minga_node::NodeIdentity;
use minga_state::types::ResourceSpec;
use minga_state::StateStore;

const COMPOSE: &str = r#"
services:
  web:
    image: nginx:1.27
    deploy:
      replicas: 2
  redis:
    image: redis:7
"#;

const COMPOSE_WITH_VOLUME: &str = r#"
services:
  db:
    image: postgres:16
    volumes:
      - data:/var/lib/postgresql/data
volumes:
  data:
"#;

/// Starts a state store, an API server, a registered node and the
/// scheduler loop. Returns the API base URL; the store inspects results.
async fn cluster() -> (Arc<StateStore>, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    minga_node::register_self(
        &store,
        &NodeIdentity {
            node_id: 1,
            address: "127.0.0.1:50051".to_string(),
            labels: Default::default(),
            capacity: ResourceSpec {
                cpus: 8.0,
                memory_bytes: 32 * 1024 * 1024 * 1024,
                gpus: 0,
            },
        },
    )
    .await
    .unwrap();

    // The scheduler the deploy waits on: place every 200ms.
    let scheduler = minga_scheduler::Scheduler::new(store.clone());
    tokio::spawn(async move { scheduler.run(Duration::from_millis(200)).await });

    let server = minga_api::ApiServer::start(store.clone(), "127.0.0.1:0")
        .await
        .unwrap();
    let url = format!("http://{}", server.addr());
    (store, url, dir)
}

/// Runs `minga deploy` and returns (success, stdout+stderr).
fn run_deploy(compose: &str, stack: &str, api_url: &str, extra: &[&str]) -> (bool, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compose.yaml");
    std::fs::write(&path, compose).unwrap();

    let mut args = vec![
        "deploy",
        "-c",
        path.to_str().unwrap(),
        stack,
        "--api-url",
        api_url,
        "--wait",
        "15",
    ];
    args.extend_from_slice(extra);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_minga"))
        .args(&args)
        .stdin(Stdio::null())
        .output()
        .expect("minga binary should run");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deploy_lands_services_and_places_tasks() {
    let (store, url, _dir) = cluster().await;

    let (ok, out) = run_deploy(COMPOSE, "shop", &url, &[]);
    assert!(ok, "deploy should succeed:\n{out}");
    assert!(out.contains("[ok ] shop.web"));
    assert!(out.contains("[ok ] shop.redis"));
    assert!(
        out.contains("2/2 task(s) placed"),
        "report placement:\n{out}"
    );

    // Services and tasks reached Raft state.
    let services = store.list_services().await.unwrap();
    assert_eq!(services.len(), 2);
    let tasks = store.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 3, "web×2 + redis×1, got {tasks:?}");
    assert!(tasks.iter().all(|t| t.node_id == 1));

    // The default network was created.
    assert!(store.get_network("shop_default").await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redeploy_is_idempotent() {
    let (store, url, _dir) = cluster().await;

    let (ok, _) = run_deploy(COMPOSE, "shop", &url, &[]);
    assert!(ok);
    let (ok, out) = run_deploy(COMPOSE, "shop", &url, &[]);
    assert!(ok, "redeploy should succeed:\n{out}");
    assert!(out.contains("service shop.web: updated"));

    // Still exactly one copy of each service.
    assert_eq!(store.list_services().await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deploy_with_volumes_lands_and_mounts() {
    let (store, url, _dir) = cluster().await;

    let (ok, out) = run_deploy(COMPOSE_WITH_VOLUME, "datastack", &url, &[]);
    assert!(ok, "a stack with volumes deploys:\n{out}");

    // The mount reference reaches state: kind Volume, stack-prefixed id.
    let services = store.list_services().await.unwrap();
    let db = services
        .iter()
        .find(|s| s.id == "datastack.db")
        .expect("db service in state");
    assert_eq!(db.mounts.len(), 1);
    assert_eq!(db.mounts[0].kind, minga_state::types::MountKind::Volume);
    assert_eq!(db.mounts[0].source, "datastack_data");
    assert_eq!(db.mounts[0].target, "/var/lib/postgresql/data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unplaceable_service_fails_the_deploy() {
    let (store, url, _dir) = cluster().await;

    // 64 CPUs on an 8-CPU node: rejected by the API's placeability check.
    let compose = r#"
services:
  huge:
    image: nginx:1.27
    deploy:
      resources:
        limits:
          cpus: "64"
"#;
    let (ok, out) = run_deploy(compose, "toobig", &url, &[]);
    assert!(!ok, "an unplaceable service must fail the deploy");
    assert!(out.contains("insufficient capacity"), "report:\n{out}");
    assert!(out.contains("[ERR] toobig.huge"), "report:\n{out}");
    assert!(store.list_services().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_api_fails_with_a_clear_message() {
    // Nothing listens here.
    let (ok, out) = run_deploy(COMPOSE, "shop", "http://127.0.0.1:1", &[]);
    assert!(!ok);
    assert!(
        out.contains("cannot reach the Minga API"),
        "message:\n{out}"
    );
}
