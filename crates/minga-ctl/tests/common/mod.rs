//! A real `minga api` with a node and the scheduler, for write tests.

#![allow(dead_code)] // each test binary uses a different part

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use minga_node::NodeIdentity;
use minga_state::types::{ResourceSpec, Service};
use minga_state::StateStore;
use serde_json::{json, Value};

pub const TOKEN: &str = "test-token-1234";

pub struct Cluster {
    pub url: String,
    pub store: Arc<StateStore>,
    http: reqwest::Client,
    _dir: tempfile::TempDir,
    _server: minga_api::ApiServer,
}

/// A registered node, the scheduler and a token-protected API.
pub async fn cluster() -> Cluster {
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

    let server = minga_api::ApiServer::start_with_auth(
        store.clone(),
        "127.0.0.1:0",
        Some(TOKEN.to_string()),
    )
    .await
    .unwrap();
    Cluster {
        url: format!("http://{}", server.addr()),
        store,
        http: reqwest::Client::new(),
        _dir: dir,
        _server: server,
    }
}

impl Cluster {
    /// `POST /v1.41/<path>`; panics unless it succeeds.
    pub async fn post(&self, path: &str, body: Value) {
        let response = self
            .http
            .post(format!("{}/v1.41/{path}", self.url))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        assert!(status.is_success(), "{path}: {status} {text}");
    }

    /// `DELETE /v1.41/<path>`; panics unless it succeeds.
    pub async fn delete(&self, path: &str) {
        let response = self
            .http
            .delete(format!("{}/v1.41/{path}", self.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{path}: {}",
            response.status()
        );
    }

    /// The service as the state store holds it.
    pub async fn stored(&self, id: &str) -> Option<Service> {
        let services = self.store.list_services().await.unwrap();
        services.into_iter().find(|s| s.id == id)
    }

    /// IDs of the networks, secrets or configs in the store.
    pub async fn ids(&self, collection: &str) -> Vec<String> {
        let list: Value = self
            .http
            .get(format!("{}/v1.41/{collection}", self.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let mut ids: Vec<String> = list
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["ID"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    }

    /// Waits until the scheduler has created at least `n` tasks.
    pub async fn wait_for_tasks(&self, n: usize) {
        for _ in 0..50 {
            if self.store.list_tasks().await.unwrap().len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the scheduler did not create {n} tasks");
    }
}

/// A Docker service spec in stack `stack` with `container` merged into its
/// container spec.
pub fn service(name: &str, stack: &str, mode: Value, container: Value) -> Value {
    let mut spec = json!({ "Image": "nginx:1.27" });
    for (k, v) in container.as_object().unwrap() {
        spec[k] = v.clone();
    }
    json!({
        "Name": name,
        "Labels": { "com.docker.stack.namespace": stack },
        "TaskTemplate": { "ContainerSpec": spec },
        "Mode": mode,
    })
}

/// Runs `minga-ctl` with config and state under `home`; returns (exit
/// code, stdout, stderr). Stdin is not a terminal.
pub fn ctl(home: &Path, endpoint: &str, args: &[&str]) -> (i32, String, String) {
    ctl_with(home, endpoint, args, &[])
}

/// [`ctl`] with extra environment variables.
pub fn ctl_with(
    home: &Path,
    endpoint: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_minga-ctl"))
        .args(args)
        .envs(env.iter().copied())
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_STATE_HOME", home.join("state"))
        .env("HOME", home.join("home"))
        .env("USER", "tester")
        .env("MINGA_CTL_ENDPOINT", endpoint)
        .env("MINGA_CTL_TOKEN", TOKEN)
        .env_remove("MINGA_CTL_CONTEXT")
        .output()
        .expect("minga-ctl should run");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// A stored service without the rollback target the server keeps, which is
/// not part of the Docker spec.
pub fn without_previous(service: Service) -> Service {
    Service {
        previous: None,
        ..service
    }
}
