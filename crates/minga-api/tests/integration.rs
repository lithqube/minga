//! Integration tests for the Docker Engine API server.
//!
//! Two test groups:
//!
//! 1. **HTTP workflow tests** (always run): start a real API server with
//!    a real `StateStore`, exercise the full Docker Engine API via HTTP
//!    using `reqwest`. Simulates the `docker stack deploy` flow.
//!
//! 2. **Docker CLI tests** (skip when Docker daemon unavailable): use
//!    `testcontainers` to run a `docker:cli` container that connects to
//!    the API server and runs real `docker` commands.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use minga_api::ApiServer;
use minga_state::StateStore;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Starts a test API server on a random port. Returns (server, base_url, tempdir).
///
/// Binds to `0.0.0.0` so testcontainers can reach it from inside containers.
/// Also registers the test node in the state store so `docker node ls` works.
async fn start_test_server() -> (ApiServer, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );

    // Register the test node so node ls returns it.
    store
        .upsert_node(minga_state::types::ClusterNode {
            id: 1,
            address: "127.0.0.1:50051".to_string(),
            labels: Default::default(),
            status: minga_state::types::NodeStatus::Active,
            capacity: minga_state::types::ResourceSpec {
                cpus: 8.0,
                memory_bytes: 32 * 1024 * 1024 * 1024,
                gpus: 0,
            },
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
        })
        .await
        .unwrap();

    // Bind to 0.0.0.0 so containers can reach the server.
    let server = ApiServer::start(store, "0.0.0.0:0").await.unwrap();
    // Report the address as 127.0.0.1 for local HTTP tests.
    let port = server.addr().rsplit_once(':').unwrap().1;
    let base_url = format!("http://127.0.0.1:{port}");
    (server, base_url, dir)
}

/// Creates a reqwest client with a short timeout.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

/// URL-encodes a string for use in query parameters.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// HTTP workflow tests (always run)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ping_endpoint() {
    let (_server, base, _dir) = start_test_server().await;
    let resp = client().get(format!("{base}/_ping")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert_eq!(text, "OK");
}

#[tokio::test]
async fn version_endpoint() {
    let (_server, base, _dir) = start_test_server().await;
    let resp = client()
        .get(format!("{base}/v1.41/version"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["ApiVersion"], "1.41");
    assert_eq!(v["Platform"]["Name"], "Minga");
}

#[tokio::test]
async fn info_endpoint_shows_swarm() {
    let (_server, base, _dir) = start_test_server().await;
    let resp = client()
        .get(format!("{base}/v1.41/info"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let info: Value = resp.json().await.unwrap();
    assert_eq!(info["Swarm"]["LocalNodeState"], "active");
    assert_eq!(info["Swarm"]["ControlAvailable"], true);
    assert!(info["Swarm"]["Cluster"].is_object());
}

#[tokio::test]
async fn full_stack_deploy_workflow() {
    let (_server, base, _dir) = start_test_server().await;
    let http = client();

    // --- Step 1: Create network (as docker stack deploy does) ---
    let net_body = json!({
        "Name": "myapp_default",
        "Driver": "overlay",
        "Attachable": true,
        "Labels": {"com.docker.stack.namespace": "myapp"},
        "IPAM": {
            "Driver": "default",
            "Config": [{"Subnet": "10.100.0.0/24", "Gateway": "10.100.0.1"}]
        }
    });
    let resp = http
        .post(format!("{base}/v1.41/networks/create"))
        .json(&net_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "network create failed");
    let net_resp: Value = resp.json().await.unwrap();
    assert_eq!(net_resp["ID"], "myapp_default");

    // --- Step 2: Create secret ---
    let secret_body = json!({
        "Name": "myapp_db_password",
        "Data": "c3VwZXJzZWNyZXQ=",
        "Labels": {"com.docker.stack.namespace": "myapp"}
    });
    let resp = http
        .post(format!("{base}/v1.41/secrets/create"))
        .json(&secret_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "secret create failed");
    let sec_resp: Value = resp.json().await.unwrap();
    assert_eq!(sec_resp["ID"], "myapp_db_password");

    // --- Step 3: Create config ---
    let config_body = json!({
        "Name": "myapp_nginx_conf",
        "Data": "c2VydmVyIHsgbGlzdGVuIDgwOyB9",
        "Labels": {"com.docker.stack.namespace": "myapp"}
    });
    let resp = http
        .post(format!("{base}/v1.41/configs/create"))
        .json(&config_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "config create failed");

    // --- Step 4: Create service (web) ---
    let svc_body = json!({
        "Name": "myapp_web",
        "Labels": {"com.docker.stack.namespace": "myapp"},
        "TaskTemplate": {
            "ContainerSpec": {
                "Image": "nginx:1.25-alpine",
                "Env": ["NGINX_HOST=localhost", "NGINX_PORT=80"],
                "Mounts": [{
                    "Type": "bind",
                    "Target": "/etc/nginx/conf.d/default.conf",
                    "Config": {"ConfigName": "myapp_nginx_conf"}
                }],
                "Secrets": [{
                    "SecretID": "myapp_db_password",
                    "SecretName": "myapp_db_password",
                    "File": {"Name": "db_password", "UID": "0", "GID": "0", "Mode": 292}
                }]
            },
            "Resources": {
                "Limits": {"NanoCPUs": 500000000, "MemoryBytes": 268435456},
                "Reservations": {"NanoCPUs": 250000000, "MemoryBytes": 134217728}
            },
            "RestartPolicy": {"Condition": "any", "MaxAttempts": 3},
            "Networks": [{"Target": "myapp_default", "Aliases": ["web"]}]
        },
        "Mode": {"Replicated": {"Replicas": 3}},
        "UpdateConfig": {"Parallelism": 1, "Delay": 10_000_000_000_u64, "FailureAction": "pause"},
        "EndpointSpec": {
            "Mode": "vip",
            "Ports": [{"Protocol": "tcp", "PublishedPort": 8080, "TargetPort": 80, "PublishMode": "ingress"}]
        }
    });
    let resp = http
        .post(format!("{base}/v1.41/services/create"))
        .json(&svc_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "service create failed: {:?}",
        resp.text().await
    );
    let svc_resp: Value = resp.json().await.unwrap();
    assert_eq!(svc_resp["ID"], "myapp_web");

    // --- Step 5: Create second service (db) ---
    let db_body = json!({
        "Name": "myapp_db",
        "Labels": {"com.docker.stack.namespace": "myapp"},
        "TaskTemplate": {
            "ContainerSpec": {
                "Image": "postgres:16-alpine",
                "Env": ["POSTGRES_PASSWORD=supersecret"]
            },
            "Resources": {
                "Limits": {"NanoCPUs": 1000000000, "MemoryBytes": 536870912}
            },
            "Networks": [{"Target": "myapp_default", "Aliases": ["db"]}]
        },
        "Mode": {"Replicated": {"Replicas": 1}}
    });
    let resp = http
        .post(format!("{base}/v1.41/services/create"))
        .json(&db_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "db service create failed");

    // --- Step 6: docker service ls (list all services) ---
    let resp = http
        .get(format!("{base}/v1.41/services"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let services: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(services.len(), 2, "expected 2 services, got {services:?}");

    // --- Step 7: docker stack ls (filter by stack namespace) ---
    let filters = json!({"label": ["com.docker.stack.namespace=myapp"]});
    let filters_str = url_encode(&serde_json::to_string(&filters).unwrap());
    let resp = http
        .get(format!("{base}/v1.41/services?filters={filters_str}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let stack_services: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(
        stack_services.len(),
        2,
        "stack filter should return both services"
    );

    // Filter by different stack should return nothing.
    let filters = json!({"label": ["com.docker.stack.namespace=other"]});
    let filters_str = url_encode(&serde_json::to_string(&filters).unwrap());
    let resp = http
        .get(format!("{base}/v1.41/services?filters={filters_str}"))
        .send()
        .await
        .unwrap();
    let other_services: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(
        other_services.len(),
        0,
        "other stack should have no services"
    );

    // --- Step 8: docker service inspect ---
    let resp = http
        .get(format!("{base}/v1.41/services/myapp_web"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let svc: Value = resp.json().await.unwrap();
    assert_eq!(svc["ID"], "myapp_web");
    assert_eq!(
        svc["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "nginx:1.25-alpine"
    );
    assert_eq!(svc["Spec"]["Labels"]["com.docker.stack.namespace"], "myapp");
    assert_eq!(svc["Spec"]["Mode"]["Replicated"]["Replicas"], 3);

    // --- Step 9: docker node ls ---
    let resp = http
        .get(format!("{base}/v1.41/nodes"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let nodes: Vec<Value> = resp.json().await.unwrap();
    assert!(!nodes.is_empty(), "should have at least one node");

    // --- Step 10: docker network ls ---
    let resp = http
        .get(format!("{base}/v1.41/networks"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let networks: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(networks.len(), 1);
    assert_eq!(networks[0]["Name"], "myapp_default");
    assert_eq!(networks[0]["Driver"], "overlay");

    // --- Step 11: docker secret ls (data should not be exposed) ---
    let resp = http
        .get(format!("{base}/v1.41/secrets"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let secrets: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(secrets.len(), 1);
    assert!(
        secrets[0]["Spec"].get("Data").is_none(),
        "secret data must not be exposed"
    );

    // --- Step 12: docker config ls ---
    let resp = http
        .get(format!("{base}/v1.41/configs"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let configs: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(configs.len(), 1);
    assert!(
        configs[0]["Spec"].get("Data").is_none(),
        "config data must not be exposed"
    );

    // --- Step 13: docker service rm ---
    let resp = http
        .delete(format!("{base}/v1.41/services/myapp_db"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "delete should return 204");

    // Verify only one service remains.
    let resp = http
        .get(format!("{base}/v1.41/services"))
        .send()
        .await
        .unwrap();
    let services: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(services.len(), 1);
    assert_eq!(services[0]["ID"], "myapp_web");

    // --- Step 14: docker network rm ---
    let resp = http
        .delete(format!("{base}/v1.41/networks/myapp_default"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // --- Step 15: docker secret rm ---
    let resp = http
        .delete(format!("{base}/v1.41/secrets/myapp_db_password"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // --- Step 16: docker config rm ---
    let resp = http
        .delete(format!("{base}/v1.41/configs/myapp_nginx_conf"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // --- Step 17: docker service rm (last one) ---
    let resp = http
        .delete(format!("{base}/v1.41/services/myapp_web"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify everything is cleaned up.
    let resp = http
        .get(format!("{base}/v1.41/services"))
        .send()
        .await
        .unwrap();
    let services: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(services.len(), 0, "all services should be removed");
}

#[tokio::test]
async fn error_responses_are_docker_compatible() {
    let (_server, base, _dir) = start_test_server().await;
    let http = client();

    // 404 for non-existent service.
    let resp = http
        .get(format!("{base}/v1.41/services/nonexistent"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: Value = resp.json().await.unwrap();
    assert!(body["message"].as_str().unwrap().contains("not found"));

    // 409 for duplicate service.
    let svc = json!({
        "Name": "dup_test",
        "TaskTemplate": {"ContainerSpec": {"Image": "redis:7"}},
        "Mode": {"Replicated": {"Replicas": 1}}
    });
    http.post(format!("{base}/v1.41/services/create"))
        .json(&svc)
        .send()
        .await
        .unwrap();
    let resp = http
        .post(format!("{base}/v1.41/services/create"))
        .json(&svc)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    assert!(body["message"].as_str().unwrap().contains("already exists"));

    // 400 for missing image.
    let bad_svc = json!({
        "Name": "bad_svc",
        "TaskTemplate": {"ContainerSpec": {"Image": ""}},
        "Mode": {"Replicated": {"Replicas": 1}}
    });
    let resp = http
        .post(format!("{base}/v1.41/services/create"))
        .json(&bad_svc)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // 404 for unimplemented endpoint.
    let resp = http
        .get(format!("{base}/v1.41/containers/json"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: Value = resp.json().await.unwrap();
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("not implemented"));
}

#[tokio::test]
async fn service_update_changes_replicas() {
    let (_server, base, _dir) = start_test_server().await;
    let http = client();

    // Create a service with 1 replica.
    let svc = json!({
        "Name": "scalable",
        "TaskTemplate": {"ContainerSpec": {"Image": "nginx:latest"}},
        "Mode": {"Replicated": {"Replicas": 1}}
    });
    let resp = http
        .post(format!("{base}/v1.41/services/create"))
        .json(&svc)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Update to 5 replicas.
    let update = json!({
        "Name": "scalable",
        "TaskTemplate": {"ContainerSpec": {"Image": "nginx:latest"}},
        "Mode": {"Replicated": {"Replicas": 5}}
    });
    let resp = http
        .post(format!("{base}/v1.41/services/scalable/update?version=1"))
        .json(&update)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "update failed: {:?}", resp.text().await);

    // Verify the update.
    let resp = http
        .get(format!("{base}/v1.41/services/scalable"))
        .send()
        .await
        .unwrap();
    let svc: Value = resp.json().await.unwrap();
    assert_eq!(svc["Spec"]["Mode"]["Replicated"]["Replicas"], 5);
}

#[tokio::test]
async fn tasks_endpoint_returns_empty_initially() {
    let (_server, base, _dir) = start_test_server().await;
    let http = client();

    let resp = http
        .get(format!("{base}/v1.41/tasks"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tasks: Vec<Value> = resp.json().await.unwrap();
    assert!(tasks.is_empty());
}

// ---------------------------------------------------------------------------
// Docker CLI tests via testcontainers (skip when Docker unavailable)
// ---------------------------------------------------------------------------

/// Checks if a Docker daemon is actually reachable and responding.
///
/// Sends a real HTTP GET /_ping to the Docker daemon over the Unix socket
/// and verifies the response is `HTTP/1.1 200 OK` with body `OK`.
async fn docker_available() -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let socket_paths = [
        "/var/run/docker.sock",
        "/Users/admin/.orbstack/run/docker.sock",
        "/run/containerd/containerd.sock",
    ];

    for path in &socket_paths {
        let Ok(mut stream) = tokio::net::UnixStream::connect(path).await else {
            continue;
        };
        let request = "GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n";
        if stream.write_all(request.as_bytes()).await.is_err() {
            continue;
        }
        let mut buf = Vec::new();
        let read_result =
            tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut buf)).await;
        let Ok(Ok(_)) = read_result else { continue };
        let response = String::from_utf8_lossy(&buf);
        if response.starts_with("HTTP/1.1 200") && response.contains("OK") {
            return true;
        }
    }

    if let Ok(url) = std::env::var("DOCKER_HOST") {
        if let Some(addr) = url.strip_prefix("tcp://") {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            if let Ok(resp) = client.get(format!("http://{addr}/_ping")).send().await {
                if resp.status().is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    if text.trim() == "OK" {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Finds the host's IP address that containers can use to reach it.
///
/// On Docker Desktop / OrbStack (macOS), `host.docker.internal` should work
/// but doesn't always resolve. This function finds a working IP by:
/// 1. Trying `host.docker.internal` first (Docker Desktop, OrbStack).
/// 2. Falling back to the host's LAN IP (found via `ifconfig` / `ip addr`).
async fn find_host_ip_for_container() -> Option<String> {
    use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

    // Strategy 1: Try host.docker.internal (works on Docker Desktop).
    let image = GenericImage::new("alpine", "3.19").with_cmd(vec![
        "getent",
        "hosts",
        "host.docker.internal",
    ]);
    if let Ok(container) = image.start().await {
        let stdout = container.stdout_to_vec().await.unwrap_or_default();
        let output = String::from_utf8_lossy(&stdout).trim().to_string();
        if let Some(ip) = output.split_whitespace().next() {
            if ip.chars().all(|c| c.is_ascii_digit() || c == '.') {
                return Some(ip.to_string());
            }
        }
    }

    // Strategy 2: Get the host's non-loopback IP.
    let output = std::process::Command::new("sh")
        .args([
            "-c",
            "ifconfig 2>/dev/null | grep 'inet ' | grep -v 127.0.0.1 | awk '{print $2}' | head -1 || ip addr show 2>/dev/null | grep 'inet ' | grep -v 127.0.0.1 | awk '{print $2}' | cut -d/ -f1 | head -1",
        ])
        .output()
        .ok()?;
    let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if ip.is_empty() || !ip.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return None;
    }
    Some(ip)
}

/// A running docker CLI container that can execute docker commands via `exec()`.
///
/// Uses a long-running container (`sleep infinity`) so we can `exec()` docker
/// commands inside it and reliably capture their output. One-shot containers
/// exit before testcontainers can read their logs.
struct DockerCliContainer {
    _container: testcontainers::ContainerAsync<testcontainers::GenericImage>,
}

impl DockerCliContainer {
    /// Starts a docker:25-cli container that stays alive and can exec commands.
    async fn start(host_ip: &str, port: u16) -> Option<Self> {
        use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

        let image = GenericImage::new("docker", "25-cli")
            .with_env_var("DOCKER_HOST", format!("tcp://{host_ip}:{port}"))
            .with_cmd(vec!["sleep", "infinity"]);

        match image.start().await {
            Ok(container) => Some(Self {
                _container: container,
            }),
            Err(e) => {
                eprintln!("skipping testcontainer: {e}");
                None
            }
        }
    }

    /// Runs a docker CLI command inside the container and returns combined stdout+stderr.
    ///
    /// Uses `exec()` which blocks until the command exits, so output is always complete.
    async fn run(&self, args: &[&str]) -> Option<String> {
        use testcontainers::core::ExecCommand;
        use tokio::io::AsyncReadExt;

        // Prepend "docker" since exec bypasses the docker-entrypoint.sh script.
        let mut cmd: Vec<String> = vec!["docker".to_string()];
        cmd.extend(args.iter().map(|s| s.to_string()));

        let mut result = match self._container.exec(ExecCommand::new(cmd)).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("exec failed: {e}");
                return None;
            }
        };

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        // Read both streams. They block until the command exits.
        let _ = result.stdout().read_to_end(&mut stdout).await;
        let _ = result.stderr().read_to_end(&mut stderr).await;

        let mut output = String::from_utf8_lossy(&stdout).into_owned();
        let stderr_str = String::from_utf8_lossy(&stderr);
        if !stderr_str.is_empty() {
            output.push_str(&stderr_str);
        }
        Some(output)
    }
}

/// Extracts the port from the base URL (e.g. "http://127.0.0.1:12345" → 12345).
fn extract_port(base_url: &str) -> u16 {
    base_url
        .trim_start_matches("http://")
        .rsplit_once(':')
        .unwrap()
        .1
        .parse()
        .unwrap()
}

#[tokio::test]
async fn docker_cli_service_ls_via_testcontainer() {
    if !docker_available().await {
        eprintln!("skipping: Docker daemon not available");
        return;
    }

    let (_server, base, _dir) = start_test_server().await;
    let port = extract_port(&base);
    let Some(host_ip) = find_host_ip_for_container().await else {
        eprintln!("skipping: cannot determine host IP for container");
        return;
    };
    eprintln!("host IP for container: {host_ip}");

    // Create a service via the API first.
    let http = client();
    let svc = json!({
        "Name": "tc_test_svc",
        "Labels": {"com.docker.stack.namespace": "tc_test"},
        "TaskTemplate": {"ContainerSpec": {"Image": "nginx:latest"}},
        "Mode": {"Replicated": {"Replicas": 2}}
    });
    let resp = http
        .post(format!("{base}/v1.41/services/create"))
        .json(&svc)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let Some(cli) = DockerCliContainer::start(&host_ip, port).await else {
        return;
    };
    let Some(output) = cli.run(&["service", "ls"]).await else {
        return;
    };
    eprintln!("docker service ls output:\n{output}");
    assert!(
        output.contains("tc_test_svc"),
        "docker service ls should list tc_test_svc, got: {output}"
    );
}

#[tokio::test]
async fn docker_cli_node_ls_via_testcontainer() {
    if !docker_available().await {
        eprintln!("skipping: Docker daemon not available");
        return;
    }

    let (_server, base, _dir) = start_test_server().await;
    let port = extract_port(&base);
    let Some(host_ip) = find_host_ip_for_container().await else {
        eprintln!("skipping: cannot determine host IP for container");
        return;
    };

    let Some(cli) = DockerCliContainer::start(&host_ip, port).await else {
        return;
    };
    let Some(output) = cli.run(&["node", "ls"]).await else {
        return;
    };
    eprintln!("docker node ls output:\n{output}");
    assert!(
        output.contains("HOSTNAME") || output.contains("ID"),
        "docker node ls should list nodes, got: {output}"
    );
}

#[tokio::test]
async fn docker_cli_info_via_testcontainer() {
    if !docker_available().await {
        eprintln!("skipping: Docker daemon not available");
        return;
    }

    let (_server, base, _dir) = start_test_server().await;
    let port = extract_port(&base);
    let Some(host_ip) = find_host_ip_for_container().await else {
        eprintln!("skipping: cannot determine host IP for container");
        return;
    };

    let Some(cli) = DockerCliContainer::start(&host_ip, port).await else {
        return;
    };
    let Some(output) = cli
        .run(&["info", "--format", "{{.Swarm.LocalNodeState}}"])
        .await
    else {
        return;
    };
    eprintln!("docker info output: {output}");
    assert!(
        output.contains("active"),
        "docker info should show Swarm active, got: {output}"
    );
}

#[tokio::test]
async fn docker_cli_version_via_testcontainer() {
    if !docker_available().await {
        eprintln!("skipping: Docker daemon not available");
        return;
    }

    let (_server, base, _dir) = start_test_server().await;
    let port = extract_port(&base);
    let Some(host_ip) = find_host_ip_for_container().await else {
        eprintln!("skipping: cannot determine host IP for container");
        return;
    };

    let Some(cli) = DockerCliContainer::start(&host_ip, port).await else {
        return;
    };
    let Some(output) = cli
        .run(&["version", "--format", "{{.Server.Version}}"])
        .await
    else {
        return;
    };
    eprintln!("docker version output: {output}");
    assert!(
        output.contains("0.0.1") || output.contains("minga"),
        "docker version should show minga server version, got: {output}"
    );
}
