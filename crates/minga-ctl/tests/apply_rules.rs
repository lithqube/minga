//! `info`, `upgrade preflight` and `upgrade apply-rules` against a real API,
//! running the built binary (ADR 0009, #188).

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use common::{ctl, Cluster, TOKEN};
use minga_bus::MemoryBus;
use minga_state::StateStore;
use serde_json::{json, Value};

/// Runs `minga-ctl` off the async runtime, which keeps serving the API.
async fn run(url: &str, home: &Path, args: &[&str]) -> (i32, String, String) {
    let (url, home) = (url.to_owned(), home.to_path_buf());
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        ctl(&home, &url, &args)
    })
    .await
    .unwrap()
}

/// A one-manager cluster and the result of one `minga-ctl` call against it.
async fn one_manager(home: &Path, args: &[&str]) -> (Cluster, (i32, String, String)) {
    let cluster = common::cluster().await;
    let result = run(&cluster.url, home, args).await;
    (cluster, result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn info_reports_the_level_in_force_and_what_each_manager_implements() {
    let home = tempfile::tempdir().unwrap();
    let (_cluster, (code, out, err)) = one_manager(home.path(), &["-o", "json", "info"]).await;
    assert_eq!(code, 0, "{err}");
    let info: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        info["apply_rules"],
        json!({"level": 1, "implemented": 1, "managers": [{"id": 1, "implemented": 1}]})
    );

    let (_cluster, (code, out, err)) = one_manager(home.path(), &["info"]).await;
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("apply-rule level in force"), "{out}");
    assert!(
        out.contains("manager 1 implements apply-rule level"),
        "{out}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_reports_the_apply_rule_levels() {
    let home = tempfile::tempdir().unwrap();
    let (_cluster, (_code, out, err)) = one_manager(
        home.path(),
        &["-o", "json", "upgrade", "preflight", "--to", "1.3.0"],
    )
    .await;
    let report: Value = serde_json::from_str(&out).unwrap_or_else(|e| panic!("{e}: {out} {err}"));
    let finding = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|finding| finding["id"] == "apply-rules")
        .unwrap_or_else(|| panic!("no apply-rules finding: {out}"));
    assert_eq!(finding["status"], "ok");
    assert!(
        finding["evidence"]
            .as_str()
            .unwrap()
            .contains("manager 1: 1"),
        "{finding}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_rules_at_the_level_in_force_changes_nothing() {
    let home = tempfile::tempdir().unwrap();
    let (cluster, (code, out, err)) =
        one_manager(home.path(), &["upgrade", "apply-rules", "--yes"]).await;
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("already at level 1"), "{out}");
    assert_eq!(cluster.store.apply_rules().await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_rules_to_a_level_no_manager_implements_is_refused() {
    let home = tempfile::tempdir().unwrap();
    let (cluster, (code, _out, err)) = one_manager(
        home.path(),
        &["upgrade", "apply-rules", "--to", "2", "--yes"],
    )
    .await;
    assert_ne!(code, 0);
    assert!(err.contains("manager 1 (level 1)"), "{err}");
    assert_eq!(cluster.store.apply_rules().await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_rules_is_refused_while_a_manager_implements_a_lower_level() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let store1 = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir1.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr2 = listener.local_addr().unwrap().to_string();
    drop(listener);
    let store2 = StateStore::open_join(2, &addr2, dir2.path()).await.unwrap();
    store1.add_manager(2, addr2).await.unwrap();
    // Manager 1 runs a build that implements level 2; manager 2 still level 1.
    store1.pretend_implemented_apply_rules(2);
    let server = minga_api::ApiServer::start_with_bus_and_auth(
        store1.clone(),
        MemoryBus::new(),
        "127.0.0.1:0",
        Some(TOKEN.to_string()),
    )
    .await
    .unwrap();
    let url = format!("http://{}", server.addr());
    let home = tempfile::tempdir().unwrap();

    let (code, out, err) = run(&url, home.path(), &["-o", "json", "info"]).await;
    assert_eq!(code, 0, "{err}");
    let info: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        info["apply_rules"]["managers"],
        json!([{"id": 1, "implemented": 2}, {"id": 2, "implemented": 1}])
    );

    let (code, _, err) = run(&url, home.path(), &["upgrade", "apply-rules", "--yes"]).await;
    assert_ne!(code, 0);
    assert!(err.contains("manager 2 (level 1)"), "{err}");
    assert_eq!(store1.apply_rules().await.unwrap(), 1);

    // The server task may still hold `store1`; the temp dirs go with the process.
    server.shutdown();
    store2.shutdown().await.unwrap();
}
