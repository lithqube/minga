//! End-to-end test for `minga state export` / `import` (docs/backup.md).
//!
//! Deploys a stack to one real API server, exports it with the real `minga`
//! binary, imports into a second fresh server and compares desired state.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
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
    environment:
      MODE: prod
    deploy:
      replicas: 2
  redis:
    image: redis:7
"#;

/// A state store, an API server, a registered node and the scheduler loop.
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
            role: Default::default(),
        },
    )
    .await
    .unwrap();
    let scheduler = minga_scheduler::Scheduler::new(store.clone());
    tokio::spawn(async move { scheduler.run(Duration::from_millis(200)).await });
    let server = minga_api::ApiServer::start(store.clone(), "127.0.0.1:0")
        .await
        .unwrap();
    let url = format!("http://{}", server.addr());
    // Keep the server alive for the test's duration.
    std::mem::forget(server);
    (store, url, dir)
}

/// Runs `minga <args>`; returns (success, stdout, stderr).
///
/// Every proxy variable points at a closed port, so each call also proves the
/// token and the snapshot never go through `HTTP_PROXY`/`ALL_PROXY`.
fn minga(args: &[&str]) -> (bool, String, String) {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_minga"));
    cmd.args(args).stdin(Stdio::null());
    for var in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        cmd.env(var, &proxy);
    }
    cmd.env_remove("NO_PROXY").env_remove("no_proxy");
    let out = cmd.output().expect("minga binary should run");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn deploy(url: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compose.yaml");
    std::fs::write(&path, COMPOSE).unwrap();
    let (ok, out, err) = minga(&[
        "deploy",
        "-c",
        path.to_str().unwrap(),
        "shop",
        "--api-url",
        url,
        "--wait",
        "15",
    ]);
    assert!(ok, "deploy failed:\n{out}{err}");
}

/// Exports `url` into `<dir>/state.json` and returns the path.
fn export(url: &str, dir: &tempfile::TempDir) -> std::path::PathBuf {
    let path = dir.path().join("state.json");
    let (ok, out, err) = minga(&[
        "state",
        "export",
        "-o",
        path.to_str().unwrap(),
        "--api-url",
        url,
    ]);
    assert!(ok, "export failed:\n{out}{err}");
    path
}

fn import(url: &str, file: &std::path::Path, extra: &[&str]) -> (bool, String, String) {
    let mut args = vec!["state", "import", file.to_str().unwrap(), "--api-url", url];
    args.extend_from_slice(extra);
    minga(&args)
}

async fn sorted_services(store: &StateStore) -> Vec<minga_state::types::Service> {
    let mut s = store.list_services().await.unwrap();
    s.sort_by(|a, b| a.id.cmp(&b.id));
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_then_import_restores_desired_state() {
    let (a, a_url, _a_dir) = cluster().await;
    deploy(&a_url);
    let out_dir = tempfile::tempdir().unwrap();
    let file = export(&a_url, &out_dir);

    // 0600, and nodes/tasks are not in the file.
    let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    assert_eq!(json["format_version"], minga_state::codec::FORMAT_VERSION);
    assert!(json.get("tasks").is_none() && json.get("nodes").is_none());

    let (b, b_url, _b_dir) = cluster().await;
    let (ok, out, err) = import(&b_url, &file, &[]);
    assert!(ok, "import failed:\n{out}{err}");

    let a_services = sorted_services(&a).await;
    assert_eq!(a_services.len(), 2);
    assert_eq!(sorted_services(&b).await, a_services);
    let mut an = a.list_networks().await.unwrap();
    let mut bn = b.list_networks().await.unwrap();
    an.sort_by(|x, y| x.id.cmp(&y.id));
    bn.sort_by(|x, y| x.id.cmp(&y.id));
    assert!(!an.is_empty());
    assert_eq!(an, bn);

    // The restored node's scheduler re-places the tasks.
    let mut tasks = 0;
    for _ in 0..50 {
        tasks = b.list_tasks().await.unwrap().len();
        if tasks == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(tasks, 3, "web×2 + redis×1 re-placed after restore");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_conflicts_without_overwrite_and_replaces_with_it() {
    let (a, a_url, _a_dir) = cluster().await;
    deploy(&a_url);
    let out_dir = tempfile::tempdir().unwrap();
    let file = export(&a_url, &out_dir);
    let original = sorted_services(&a).await;

    // Importing into the cluster it came from conflicts, and changes nothing.
    let (ok, out, err) = import(&a_url, &file, &[]);
    assert!(!ok, "second import must fail:\n{out}{err}");
    assert!(
        err.contains("409") && err.contains("already exist"),
        "{err}"
    );

    // Drift a service, then restore it with --overwrite.
    let mut drifted = original[0].clone();
    drifted.replicas += 5;
    a.upsert_service(drifted).await.unwrap();
    let (ok, out, err) = import(&a_url, &file, &["--overwrite"]);
    assert!(ok, "overwrite import failed:\n{out}{err}");
    assert_eq!(sorted_services(&a).await, original);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_refuses_other_format_versions() {
    let (a, a_url, _a_dir) = cluster().await;
    deploy(&a_url);
    let out_dir = tempfile::tempdir().unwrap();
    let file = export(&a_url, &out_dir);
    let original = std::fs::read(&file).unwrap();
    let (b, b_url, _b_dir) = cluster().await;

    for version in [minga_state::codec::FORMAT_VERSION + 1, 1] {
        let mut json: serde_json::Value = serde_json::from_slice(&original).unwrap();
        json["format_version"] = version.into();
        let bad = out_dir.path().join("bad.json");
        std::fs::write(&bad, serde_json::to_vec(&json).unwrap()).unwrap();
        let (ok, out, err) = import(&b_url, &bad, &["--overwrite"]);
        assert!(!ok, "version {version} must be refused:\n{out}{err}");
        assert!(err.contains("format version"), "{err}");
    }
    assert!(b.list_services().await.unwrap().is_empty());
    drop(a);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_file_handling() {
    let (_a, a_url, _a_dir) = cluster().await;
    deploy(&a_url);
    let dir = tempfile::tempdir().unwrap();
    let file = export(&a_url, &dir);

    // An existing file is not overwritten...
    std::fs::write(&file, "keep me").unwrap();
    let (ok, _, err) = minga(&[
        "state",
        "export",
        "-o",
        file.to_str().unwrap(),
        "--api-url",
        &a_url,
    ]);
    assert!(!ok && err.contains("--force"), "{err}");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "keep me");

    // ...unless --force, which also tightens a loose mode.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    let (ok, _, err) = minga(&[
        "state",
        "export",
        "-o",
        file.to_str().unwrap(),
        "--force",
        "--api-url",
        &a_url,
    ]);
    assert!(ok, "{err}");
    assert_eq!(
        std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    serde_json::from_slice::<serde_json::Value>(&std::fs::read(&file).unwrap()).unwrap();

    // No -o: JSON on stdout.
    let (ok, out, err) = minga(&["state", "export", "--api-url", &a_url]);
    assert!(ok, "{err}");
    let json: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(json["services"].as_array().unwrap().len(), 2);
}
