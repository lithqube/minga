//! `minga-ctl` read commands against a real `minga api` server.
//!
//! Starts a state store, a registered node, the scheduler and an API server
//! with a token (plan section 6), creates services through the Docker API,
//! then runs the built binary with `MINGA_CTL_ENDPOINT` and `MINGA_CTL_TOKEN`.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use minga_node::NodeIdentity;
use minga_state::types::ResourceSpec;
use minga_state::StateStore;
use serde_json::{json, Value};

const TOKEN: &str = "test-token-1234";

struct Cluster {
    url: String,
    _dir: tempfile::TempDir,
    _server: minga_api::ApiServer,
}

/// A node, the scheduler and a token-protected API with two stacks:
/// `shop` (web ×2 with a healthcheck, db ×1) and `blog` (app ×1), plus one
/// unlabelled service and a secret.
async fn cluster() -> Cluster {
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
    tokio::spawn(async move { scheduler.run(Duration::from_millis(100)).await });

    let server =
        minga_api::ApiServer::start_with_auth(store, "127.0.0.1:0", Some(TOKEN.to_string()))
            .await
            .unwrap();
    let url = format!("http://{}", server.addr());

    let http = reqwest::Client::new();
    let post = |path: &str, body: Value| {
        let request = http
            .post(format!("{url}/v1.41/{path}"))
            .bearer_auth(TOKEN)
            .json(&body);
        let path = path.to_string();
        async move {
            let response = request.send().await.unwrap();
            assert!(
                response.status().is_success(),
                "{path}: {}",
                response.status()
            );
        }
    };
    let service = |name: &str, stack: Option<&str>, replicas: u64, healthcheck: bool| {
        let mut container = json!({ "Image": "nginx:1.27" });
        if healthcheck {
            container["Healthcheck"] = json!({ "Test": ["CMD", "true"] });
        }
        let mut labels = json!({});
        if let Some(stack) = stack {
            labels["com.docker.stack.namespace"] = json!(stack);
        }
        json!({
            "Name": name,
            "Labels": labels,
            "TaskTemplate": { "ContainerSpec": container },
            "Mode": { "Replicated": { "Replicas": replicas } }
        })
    };
    post(
        "services/create",
        service("shop.web", Some("shop"), 2, true),
    )
    .await;
    post(
        "services/create",
        service("shop.db", Some("shop"), 1, false),
    )
    .await;
    post(
        "services/create",
        service("blog.app", Some("blog"), 1, false),
    )
    .await;
    post("services/create", service("loose", None, 1, false)).await;
    post("secrets/create", json!({ "Name": "shop_db_password" })).await;

    // Wait for the scheduler to create all five tasks.
    for _ in 0..50 {
        let tasks: Value = http
            .get(format!("{url}/v1.41/tasks"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if tasks.as_array().map_or(0, Vec::len) >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Cluster {
        url,
        _dir: dir,
        _server: server,
    }
}

/// Runs `minga-ctl` against `endpoint` with `token`; returns (exit code, stdout, stderr).
fn ctl(endpoint: &str, token: &str, args: &[&str]) -> (i32, String, String) {
    let home = tempfile::tempdir().unwrap();
    ctl_in(home.path(), endpoint, token, args)
}

fn ctl_in(home: &Path, endpoint: &str, token: &str, args: &[&str]) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_minga-ctl"))
        .args(args)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("HOME", home.join("home"))
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
}

fn json_out(stdout: &str) -> Value {
    serde_json::from_str(stdout).unwrap_or_else(|e| panic!("not JSON ({e}): {stdout}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_commands_against_a_real_api() {
    let cluster = cluster().await;
    let url = cluster.url.clone();
    let run = move |args: Vec<&'static str>| {
        let url = url.clone();
        tokio::task::spawn_blocking(move || ctl(&url, TOKEN, &args))
    };

    // stack ls: grouped by label; the unlabelled service is not a stack.
    let (code, out, err) = run(vec!["--json", "stack", "ls"]).await.unwrap();
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        json_out(&out),
        json!([{"name": "blog", "services": 1}, {"name": "shop", "services": 2}])
    );

    // stack ps: three shop tasks, health from the healthcheck.
    let (code, out, err) = run(vec!["--json", "stack", "ps", "shop"]).await.unwrap();
    assert_eq!(code, 0, "{err}");
    let tasks = json_out(&out);
    let tasks = tasks.as_array().unwrap();
    assert_eq!(tasks.len(), 3, "{out}");
    for task in tasks {
        let service = task["service"].as_str().unwrap();
        assert!(service.starts_with("shop."), "{task}");
        assert!(task["id"].as_str().unwrap().starts_with(service), "{task}");
        if service == "shop.db" {
            assert_eq!(task["health"], "-", "{task}");
        }
    }

    // service ls --stack shop: desired replicas from the spec.
    let (code, out, err) = run(vec!["--json", "service", "ls", "--stack", "shop"])
        .await
        .unwrap();
    assert_eq!(code, 0, "{err}");
    let services = json_out(&out);
    let ids: Vec<&str> = services
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["shop.db", "shop.web"]);
    assert_eq!(services[1]["desired"], 2);
    assert_eq!(services[1]["mode"], "replicated");
    assert_eq!(services[1]["image"], "nginx:1.27");

    // service ls (table): every service, unlabelled ones with stack `-`.
    let (code, out, err) = run(vec!["service", "ls"]).await.unwrap();
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("ID"), "{out}");
    assert!(
        out.lines()
            .any(|l| l.starts_with("loose ") && l.contains(" - ")),
        "{out}"
    );

    // service ps: the service's own tasks only; --quiet prints task IDs.
    let (code, out, err) = run(vec!["--quiet", "service", "ps", "shop.web"])
        .await
        .unwrap();
    assert_eq!(code, 0, "{err}");
    let ids: Vec<&str> = out.lines().collect();
    assert_eq!(ids.len(), 2, "{out}");
    assert!(ids.iter().all(|id| id.starts_with("shop.web.")), "{out}");

    // secret list: metadata only.
    let (code, out, err) = run(vec!["--json", "secret", "list"]).await.unwrap();
    assert_eq!(code, 0, "{err}");
    assert_eq!(json_out(&out)[0]["name"], "shop_db_password", "{out}");

    // Not found: exit 3 with the context name.
    let (code, _, err) = run(vec!["stack", "ps", "nope"]).await.unwrap();
    assert_eq!(code, 3, "{err}");
    assert!(
        err.contains("stack `nope` not found in context `default`"),
        "{err}"
    );
    let (code, _, err) = run(vec!["service", "ps", "shop.nope"]).await.unwrap();
    assert_eq!(code, 3, "{err}");
    assert!(err.contains("service `shop.nope` not found"), "{err}");

    // An ID that would change the URL path is refused before any request.
    let (code, _, err) = run(vec!["service", "ps", "../nodes"]).await.unwrap();
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("invalid service"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_proxies_are_ignored_so_the_token_never_reaches_one() {
    let cluster = cluster().await;
    let url = cluster.url.clone();
    // A port nothing listens on: a request sent to the proxy would fail.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let output = tokio::task::spawn_blocking(move || {
        let home = tempfile::tempdir().unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_minga-ctl"));
        cmd.args(["stack", "ls"])
            .env("XDG_CONFIG_HOME", home.path().join("config"))
            .env("HOME", home.path().join("home"))
            .env("MINGA_CTL_ENDPOINT", &url)
            .env("MINGA_CTL_TOKEN", TOKEN)
            .env_remove("MINGA_CTL_CONTEXT");
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
        for var in ["NO_PROXY", "no_proxy"] {
            cmd.env_remove(var);
        }
        cmd.output().unwrap()
    })
    .await
    .unwrap();
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{err}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("shop"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_token_is_exit_two_with_the_hint() {
    let cluster = cluster().await;
    let url = cluster.url.clone();
    let (code, _, err) = tokio::task::spawn_blocking(move || ctl(&url, "wrong", &["stack", "ls"]))
        .await
        .unwrap();
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("rejected the credentials (401"), "{err}");
    assert!(err.contains("hint: check remote_token_file"), "{err}");
}

#[test]
fn an_unreachable_api_is_exit_two() {
    // A port nothing listens on: bind, read the port, close.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let (code, _, err) = ctl(&format!("http://127.0.0.1:{port}"), TOKEN, &["stack", "ls"]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("cannot reach the Minga API"), "{err}");
}

#[test]
fn a_plain_http_remote_endpoint_is_refused_before_connecting() {
    let (code, _, err) = ctl("http://10.0.0.1:2375", TOKEN, &["stack", "ls"]);
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("refusing plain HTTP to 10.0.0.1"), "{err}");
}
