//! `node`, `network`, `manager`, `info` and `version` against a real API,
//! running the built binary.

mod common;

use std::path::Path;

use common::{ctl, Cluster};
use serde_json::Value;

/// Runs `minga-ctl` off the async runtime, which keeps serving the API.
async fn run(cluster: &Cluster, home: &Path, args: &[&str]) -> (i32, String, String) {
    let (url, home) = (cluster.url.clone(), home.to_path_buf());
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        ctl(&home, &url, &args)
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_resources_round_trip() {
    let cluster = common::cluster().await;
    let home = tempfile::tempdir().unwrap();
    let home = home.path();

    let (code, out, err) = run(&cluster, home, &["--output", "json", "node", "ls"]).await;
    assert_eq!(code, 0, "{err}");
    let nodes: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(nodes.as_array().unwrap().len(), 1);

    let (code, _, err) = run(&cluster, home, &["manager", "ls"]).await;
    assert_eq!(code, 0, "{err}");

    let (code, _, err) = run(&cluster, home, &["network", "create", "edge"]).await;
    assert_eq!(code, 0, "{err}");
    let ids = cluster.ids("networks").await;
    assert_eq!(ids.len(), 1);
    let (code, _, err) = run(&cluster, home, &["network", "rm", "--yes", &ids[0]]).await;
    assert_eq!(code, 0, "{err}");
    assert!(cluster.ids("networks").await.is_empty());

    for command in ["info", "version"] {
        let (code, _, err) = run(&cluster, home, &[command]).await;
        assert_eq!(code, 0, "{command}: {err}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_node_fails_with_not_found() {
    let cluster = common::cluster().await;
    let home = tempfile::tempdir().unwrap();
    let (code, _, err) = run(&cluster, home.path(), &["node", "inspect", "999"]).await;
    assert_ne!(code, 0);
    assert!(err.contains("not found"), "{err}");
}
