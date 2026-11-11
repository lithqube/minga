//! `minga-ctl health` and `metrics` against a real `minga api` server with a
//! token (plan sections 5.5 and 5.6). No scheduler runs, so services have no
//! tasks; that is what the replica checks are tested against.

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use minga_node::NodeIdentity;
use minga_state::types::ResourceSpec;
use minga_state::StateStore;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "health-test-token";

struct Cluster {
    url: String,
    _dir: tempfile::TempDir,
    _server: minga_api::ApiServer,
}

async fn cluster() -> Cluster {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    // A node so services are placeable; no scheduler, so they get no tasks.
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
    let server =
        minga_api::ApiServer::start_with_auth(store, "127.0.0.1:0", Some(TOKEN.to_string()))
            .await
            .unwrap();
    Cluster {
        url: format!("http://{}", server.addr()),
        _dir: dir,
        _server: server,
    }
}

/// An HTTP server that answers every request with 200.
async fn ok_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nOK",
                    )
                    .await;
            });
        }
    });
    format!("http://{addr}/up")
}

/// Runs `minga-ctl` against `endpoint`; returns (exit code, stdout, stderr).
async fn ctl(endpoint: &str, token: &str, args: &[&str]) -> (i32, String, String) {
    let (endpoint, token) = (endpoint.to_string(), token.to_string());
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let home = tempfile::tempdir().unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_minga-ctl"))
            .args(&args)
            .env("XDG_CONFIG_HOME", home.path().join("config"))
            .env("HOME", home.path().join("home"))
            .env("MINGA_CTL_ENDPOINT", endpoint)
            .env("MINGA_CTL_TOKEN", token)
            .env_remove("MINGA_CTL_CONTEXT")
            .output()
            .expect("minga-ctl should run");
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    })
    .await
    .unwrap()
}

fn row<'a>(rows: &'a Value, check: &str) -> &'a Value {
    rows.as_array()
        .unwrap()
        .iter()
        .find(|r| r["check"] == check)
        .unwrap_or_else(|| panic!("no `{check}` row in {rows}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_reports_the_api_checks() {
    let c = cluster().await;

    // /readyz turns 200 once Raft knows a leader (the server probes every second).
    let mut rows = Value::Null;
    for _ in 0..50 {
        let (code, out, _) = ctl(&c.url, TOKEN, &["--json", "health"]).await;
        rows = serde_json::from_str(&out).unwrap();
        if code == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(row(&rows, "api /healthz")["status"], "ok", "{rows}");
    assert_eq!(row(&rows, "api /readyz")["status"], "ok", "{rows}");

    let (code, out, _) = ctl(&c.url, TOKEN, &["health"]).await;
    assert_eq!(code, 0);
    assert!(out.starts_with("CHECK"), "{out}");
    assert!(out.contains("api /healthz"), "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_check_exits_two() {
    let c = cluster().await;
    // A wrong token makes /healthz answer 401: the check fails, the rows still print.
    let (code, out, err) = ctl(&c.url, "wrong", &["--json", "health"]).await;
    assert_eq!(code, 2, "{err}");
    let rows: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(row(&rows, "api /healthz")["status"], "fail", "{rows}");
    assert!(
        row(&rows, "api /healthz")["detail"]
            .as_str()
            .unwrap()
            .starts_with("401"),
        "{rows}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_stack_exits_three() {
    let c = cluster().await;
    let (code, _, err) = ctl(&c.url, TOKEN, &["health", "--stack", "nope"]).await;
    assert_eq!(code, 3, "{err}");
    assert!(err.contains("stack `nope` not found"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stack_health_checks_replicas_and_label_urls() {
    let c = cluster().await;
    let up = ok_server().await;
    let http = reqwest::Client::new();
    let service = |name: &str, replicas: u64, url: &str| {
        json!({
            "Name": name,
            "Labels": {
                "com.docker.stack.namespace": "shop",
                "minga.ctl.health.url": url,
            },
            "TaskTemplate": {"ContainerSpec": {"Image": "nginx:1.27"}},
            "Mode": {"Replicated": {"Replicas": replicas}},
        })
    };
    let unauthenticated = format!("{}/healthz", c.url);
    for body in [
        // Zero desired, none running: ok, and its URL answers 200.
        service("shop.up", 0, &up),
        // One desired, none running (no scheduler): fail; the URL is refused.
        service("shop.web", 1, "file:///etc/passwd"),
        // The URL is the token-protected API: nothing may be sent, so it is 401.
        service("shop.nocreds", 0, &unauthenticated),
    ] {
        let response = http
            .post(format!("{}/v1.41/services/create", c.url))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "{}", response.status());
    }

    let (code, out, err) = ctl(&c.url, TOKEN, &["--json", "health", "--stack", "shop"]).await;
    assert_eq!(code, 2, "{err}");
    let rows: Value = serde_json::from_str(&out).unwrap();

    assert_eq!(row(&rows, "shop.up")["status"], "ok", "{rows}");
    assert_eq!(row(&rows, "shop.up url")["status"], "ok", "{rows}");
    assert!(row(&rows, "shop.up url")["detail"]
        .as_str()
        .unwrap()
        .starts_with("200 in "));

    assert_eq!(row(&rows, "shop.web")["status"], "fail", "{rows}");
    assert_eq!(row(&rows, "shop.web")["detail"], "0/1 running");
    assert_eq!(row(&rows, "shop.web url")["status"], "fail", "{rows}");
    assert!(row(&rows, "shop.web url")["detail"]
        .as_str()
        .unwrap()
        .contains("file://"));

    let nocreds = row(&rows, "shop.nocreds url");
    assert_eq!(nocreds["status"], "fail", "{rows}");
    assert!(
        nocreds["detail"].as_str().unwrap().starts_with("401"),
        "{rows}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_lists_real_metric_names() {
    let c = cluster().await;

    let (code, out, err) = ctl(&c.url, TOKEN, &["--json", "metrics"]).await;
    assert_eq!(code, 0, "{err}");
    let samples: Value = serde_json::from_str(&out).unwrap();
    let leader = samples
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["metric"] == "minga_raft_leader")
        .unwrap_or_else(|| panic!("minga_raft_leader missing from {out}"));
    assert!(leader["value"].is_number(), "{leader}");
    assert!(leader["labels"].as_object().unwrap().is_empty(), "{leader}");

    let (code, out, err) = ctl(&c.url, TOKEN, &["metrics"]).await;
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("METRIC"), "{out}");
    assert!(out.contains("raft leader"), "{out}");
    // The two metrics that do not exist are named, not faked.
    assert!(
        err.contains("no image pull latency or update count"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_with_a_wrong_token_exits_two() {
    let c = cluster().await;
    let (code, _, err) = ctl(&c.url, "wrong", &["metrics"]).await;
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("rejected the credentials"), "{err}");
}
