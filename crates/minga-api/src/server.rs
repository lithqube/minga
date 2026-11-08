//! Axum router and server setup for the Docker Engine API.
//!
//! Routes are versioned: `/{version}/...` where `version` is `v1.41`
//! (or any `vX.Y` the client sends). The version segment is captured
//! but not validated — Minga always responds with its own API version.

use std::sync::Arc;

use axum::{
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use minga_state::StateStore;
use minga_telemetry::{ReadyState, METRICS};
use tokio::net::TcpListener;

use crate::handlers::{self, AppState};

/// Combined state for all routes.
#[derive(Clone)]
struct ApiState {
    app_state: Arc<AppState>,
    /// Bearer token required on all endpoints, when set.
    auth_token: Option<Arc<str>>,
}

/// Allow extracting `Arc<AppState>` from `ApiState` for existing handlers.
impl axum::extract::FromRef<ApiState> for Arc<AppState> {
    fn from_ref(state: &ApiState) -> Self {
        state.app_state.clone()
    }
}

/// Constant-time string comparison (no early exit on length or content).
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= (a.get(i % a.len().max(1)).copied().unwrap_or(0)
            ^ b.get(i % b.len().max(1)).copied().unwrap_or(0)) as usize;
    }
    diff == 0
}

/// Bearer-token gate. When a token is configured, every request must
/// carry `Authorization: Bearer <token>`; anything else is a 401 with a
/// Docker-compatible error body.
async fn bearer_auth(
    axum::extract::State(state): axum::extract::State<ApiState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(expected) = &state.auth_token {
        let presented = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match presented {
            Some(token) if constant_time_eq(token, expected) => next.run(req).await,
            _ => (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"message": "authentication required"})),
            )
                .into_response(),
        }
    } else {
        next.run(req).await
    }
}

/// Builds the axum router with all Docker Engine API routes.
pub fn build_router(store: Arc<StateStore>, ready_state: Arc<ReadyState>) -> Router {
    build_router_with_auth(store, ready_state, None)
}

/// Builds the router, requiring a bearer token when `auth_token` is set.
pub fn build_router_with_auth(
    store: Arc<StateStore>,
    ready_state: Arc<ReadyState>,
    auth_token: Option<String>,
) -> Router {
    let app_state = Arc::new(AppState::new(store));

    let api_state = ApiState {
        app_state,
        auth_token: auth_token.map(Arc::from),
    };

    Router::new()
        // Unversioned endpoints.
        .route("/_ping", get(handlers::ping))
        // Versioned endpoints: /{version}/...
        .route("/:version/version", get(handlers::get_version))
        .route("/:version/info", get(handlers::get_info))
        // Nodes.
        .route(
            "/:version/nodes",
            get(handlers::list_nodes).post(handlers::create_node),
        )
        .route("/:version/nodes/:id", get(handlers::get_node))
        .route("/:version/nodes/:id/update", post(handlers::update_node))
        // Services.
        .route("/:version/services", get(handlers::list_services))
        .route("/:version/services/create", post(handlers::create_service))
        .route("/:version/services/:id", get(handlers::get_service))
        .route("/:version/services/:id", delete(handlers::delete_service))
        .route(
            "/:version/services/:id/update",
            post(handlers::update_service),
        )
        // Tasks.
        .route("/:version/tasks", get(handlers::list_tasks))
        .route("/:version/tasks/:id", get(handlers::get_task))
        // Networks.
        .route("/:version/networks", get(handlers::list_networks))
        .route("/:version/networks/create", post(handlers::create_network))
        .route("/:version/networks/:id", get(handlers::get_network))
        .route("/:version/networks/:id", delete(handlers::delete_network))
        // Secrets.
        .route("/:version/secrets", get(handlers::list_secrets))
        .route("/:version/secrets/create", post(handlers::create_secret))
        .route("/:version/secrets/:id", get(handlers::get_secret))
        .route("/:version/secrets/:id", delete(handlers::delete_secret))
        // Configs.
        .route("/:version/configs", get(handlers::list_configs))
        .route("/:version/configs/create", post(handlers::create_config))
        .route("/:version/configs/:id", get(handlers::get_config))
        .route("/:version/configs/:id", delete(handlers::delete_config))
        // Fallback.
        .fallback(handlers::fallback)
        .with_state(api_state.clone())
        // Observability endpoints: /metrics, /healthz, /readyz.
        .merge(minga_telemetry::router(ready_state.as_ref().clone()))
        .layer(axum::middleware::from_fn_with_state(api_state, bearer_auth))
}

/// The API server. Owns the router, the TCP listener task and the
/// readiness probe.
pub struct ApiServer {
    addr: String,
    handle: tokio::task::JoinHandle<()>,
    probe: tokio::task::JoinHandle<()>,
}

impl ApiServer {
    /// Starts the API server on the given address.
    ///
    /// - `store`: the cluster state store (shared with the manager/worker).
    /// - `bind`: the address to bind (e.g. `0.0.0.0:2375`).
    pub async fn start(store: Arc<StateStore>, bind: &str) -> Result<Self, ApiServerError> {
        Self::start_with_auth(store, bind, None).await
    }

    /// Starts the API server; when `auth_token` is set, every request
    /// must carry it as `Authorization: Bearer <token>`.
    pub async fn start_with_auth(
        store: Arc<StateStore>,
        bind: &str,
        auth_token: Option<String>,
    ) -> Result<Self, ApiServerError> {
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|e| ApiServerError::Bind(format!("cannot bind {bind}: {e}")))?;

        let addr = listener
            .local_addr()
            .map_err(|e| ApiServerError::Bind(format!("cannot get local addr: {e}")))?
            .to_string();

        // `minga api` has no bus: ready means Raft knows a leader. Spawned
        // only once startup can no longer fail, so a failed bind leaks no task.
        let ready_state = ReadyState::without_bus();
        let probe = {
            let (store, ready) = (store.clone(), ready_state.clone());
            tokio::spawn(async move {
                loop {
                    ready.set_raft_ready(store.current_leader().await.is_some());
                    METRICS
                        .raft_leader
                        .set(if store.is_leader().await { 1.0 } else { 0.0 });
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            })
        };
        let router = build_router_with_auth(store, Arc::new(ready_state), auth_token);

        let handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                tracing::error!(error = %e, "API server error");
            }
        });

        tracing::info!(addr = %addr, "Docker Engine API server listening");

        Ok(Self {
            addr,
            handle,
            probe,
        })
    }

    /// The address the server is bound to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Stops the API server.
    pub fn shutdown(self) {
        self.handle.abort();
        self.probe.abort();
    }
}

/// Errors from starting the API server.
#[derive(Debug, thiserror::Error)]
pub enum ApiServerError {
    #[error("{0}")]
    Bind(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    /// Creates a test state store and router, with one active node
    /// registered — the state any cluster serving the API is really in.
    async fn test_router() -> (Router, tempfile::TempDir) {
        test_router_with_node(true).await
    }

    /// Same as [`test_router`], but with no registered node, for the
    /// zero-eligible-node failure paths.
    async fn test_router_without_node() -> (Router, tempfile::TempDir) {
        test_router_with_node(false).await
    }

    async fn test_router_with_node(register: bool) -> (Router, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        if register {
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
                    last_seen_unix: None,
                    role: Default::default(),
                })
                .await
                .unwrap();
        }
        let ready_state = Arc::new(ReadyState::new());
        let router = build_router(store, ready_state);
        (router, dir)
    }

    async fn send_request(router: Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&body).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&body).into_owned())
            })
        };
        (status, json)
    }

    #[tokio::test]
    async fn ping() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/_ping")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"OK");
    }

    #[tokio::test]
    async fn get_version() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/v1.41/version")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["ApiVersion"], "1.41");
        assert_eq!(json["MinAPIVersion"], "1.24");
        assert_eq!(json["Platform"]["Name"], "Minga");
    }

    #[tokio::test]
    async fn get_info() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/v1.41/info")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["Swarm"]["LocalNodeState"], "active");
        assert_eq!(json["Swarm"]["ControlAvailable"], true);
        assert!(json["Swarm"]["Cluster"].is_object());
    }

    #[tokio::test]
    async fn list_nodes() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/v1.41/nodes")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1, "the registered test node must be listed");
        assert_eq!(arr[0]["ID"], "1");
        assert_eq!(arr[0]["Status"]["State"], "ready");
    }

    #[tokio::test]
    async fn create_and_list_service() {
        let (router, _dir) = test_router().await;

        let spec = serde_json::json!({
            "Name": "test_web",
            "Labels": {"com.docker.stack.namespace": "test"},
            "TaskTemplate": {
                "ContainerSpec": {
                    "Image": "nginx:latest",
                    "Env": ["FOO=bar"]
                },
                "Resources": {
                    "Limits": {"NanoCPUs": 500000000, "MemoryBytes": 536870912}
                }
            },
            "Mode": {"Replicated": {"Replicas": 2}},
            "EndpointSpec": {
                "Ports": [{"Protocol": "tcp", "PublishedPort": 8080, "TargetPort": 80}]
            }
        });

        // Create.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/services/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&spec).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["ID"], "test_web");

        // List.
        let req = Request::builder()
            .uri("/v1.41/services")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["ID"], "test_web");
        assert_eq!(
            arr[0]["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
            "nginx:latest"
        );
        assert_eq!(
            arr[0]["Spec"]["Labels"]["com.docker.stack.namespace"],
            "test"
        );
    }

    #[tokio::test]
    async fn create_service_conflict() {
        let (router, _dir) = test_router().await;

        let spec = serde_json::json!({
            "Name": "dup_svc",
            "TaskTemplate": {
                "ContainerSpec": {"Image": "redis:7"}
            },
            "Mode": {"Replicated": {"Replicas": 1}}
        });

        // First create.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/services/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&spec).unwrap()))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);

        // Second create should conflict.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/services/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&spec).unwrap()))
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(json["message"].as_str().unwrap().contains("already exists"));
    }

    #[tokio::test]
    async fn get_service_not_found() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/v1.41/services/nonexistent")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(json["message"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn delete_service() {
        let (router, _dir) = test_router().await;

        // Create.
        let spec = serde_json::json!({
            "Name": "to_delete",
            "TaskTemplate": {"ContainerSpec": {"Image": "alpine:3"}},
            "Mode": {"Replicated": {"Replicas": 1}}
        });
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/services/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&spec).unwrap()))
            .unwrap();
        send_request(router.clone(), req).await;

        // Delete.
        let req = Request::builder()
            .method("DELETE")
            .uri("/v1.41/services/to_delete")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Verify gone.
        let req = Request::builder()
            .uri("/v1.41/services/to_delete")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn service_filter_by_stack_label() {
        let (router, _dir) = test_router().await;

        // Create two services in different stacks.
        for (name, stack) in [("stack_a_web", "stack_a"), ("stack_b_web", "stack_b")] {
            let spec = serde_json::json!({
                "Name": name,
                "Labels": {"com.docker.stack.namespace": stack},
                "TaskTemplate": {"ContainerSpec": {"Image": "nginx:latest"}},
                "Mode": {"Replicated": {"Replicas": 1}}
            });
            let req = Request::builder()
                .method("POST")
                .uri("/v1.41/services/create")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&spec).unwrap()))
                .unwrap();
            send_request(router.clone(), req).await;
        }

        // Filter by stack_a.
        let filters = serde_json::json!({
            "label": ["com.docker.stack.namespace=stack_a"]
        });
        let encoded = serde_json::to_string(&filters).unwrap();
        let uri = format!("/v1.41/services?filters={}", url_encode(&encoded));
        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["ID"], "stack_a_web");
    }

    /// Minimal percent-encoding for query string values.
    fn url_encode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char);
                }
                _ => {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn create_and_list_network() {
        let (router, _dir) = test_router().await;

        let req_body = serde_json::json!({
            "Name": "test_default",
            "Driver": "overlay",
            "IPAM": {
                "Driver": "default",
                "Config": [{"Subnet": "10.100.0.0/24", "Gateway": "10.100.0.1"}]
            },
            "Attachable": true,
            "Labels": {"com.docker.stack.namespace": "test"}
        });

        // Create.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/networks/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&req_body).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["ID"], "test_default");

        // List.
        let req = Request::builder()
            .uri("/v1.41/networks")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["Name"], "test_default");
        assert_eq!(arr[0]["Driver"], "overlay");
        assert_eq!(arr[0]["IPAM"]["Config"][0]["Subnet"], "10.100.0.0/24");
    }

    #[tokio::test]
    async fn create_secret_with_inline_data_is_rejected() {
        let (router, _dir) = test_router().await;

        // Docker's API allows an inline value. Minga must refuse it rather
        // than accept and silently discard the bytes.
        let body = serde_json::json!({
            "Name": "test_inline_secret",
            "Data": "c2VjcmV0MTIz"
        });

        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/secrets/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;

        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "inline secret data must be refused, got: {json}"
        );
        assert!(
            json["message"]
                .as_str()
                .unwrap_or_default()
                .contains("inline data"),
            "the error must explain why, got: {json}"
        );
        // The value must not appear anywhere in the response.
        assert!(
            !json.to_string().contains("c2VjcmV0MTIz"),
            "the secret value must never be echoed: {json}"
        );

        // And nothing was created.
        let req = Request::builder()
            .uri("/v1.41/secrets")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert!(
            arr.iter().all(|s| s["Name"] != "test_inline_secret"),
            "the refused secret must not exist: {json}"
        );
    }

    #[tokio::test]
    async fn create_and_list_secret() {
        let (router, _dir) = test_router().await;

        // Metadata only: Minga never accepts a secret value over the API.
        let body = serde_json::json!({
            "Name": "test_db_password",
            "Labels": {"com.docker.stack.namespace": "test"}
        });

        // Create.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/secrets/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["ID"], "test_db_password");

        // List.
        let req = Request::builder()
            .uri("/v1.41/secrets")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["ID"], "test_db_password");
        assert_eq!(arr[0]["Spec"]["Name"], "test_db_password");
        // Data should NOT be in the response.
        assert!(arr[0]["Spec"].get("Data").is_none());
    }

    #[tokio::test]
    async fn create_and_list_config() {
        let (router, _dir) = test_router().await;

        let body = serde_json::json!({
            "Name": "test_nginx_conf",
            "Data": "c2VydmVyIHsgbGlzdGVuIDgwOyB9",
            "Labels": {"com.docker.stack.namespace": "test"}
        });

        // Create.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/configs/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["ID"], "test_nginx_conf");

        // List.
        let req = Request::builder()
            .uri("/v1.41/configs")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["ID"], "test_nginx_conf");
        // Data should NOT be in the response.
        assert!(arr[0]["Spec"].get("Data").is_none());
    }

    #[tokio::test]
    async fn fallback_not_found() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/v1.41/containers/json")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(json["message"]
            .as_str()
            .unwrap()
            .contains("not implemented"));
    }

    #[tokio::test]
    async fn list_tasks_empty() {
        let (router, _dir) = test_router().await;
        let req = Request::builder()
            .uri("/v1.41/tasks")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn api_server_start_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );

        // Bind to port 0 (OS-assigned).
        let server = ApiServer::start(store, "127.0.0.1:0").await.unwrap();
        assert!(server.addr().starts_with("127.0.0.1:"));
        assert_ne!(server.addr(), "127.0.0.1:0");

        // Make a raw HTTP request to verify it's actually serving.
        let addr = server.addr().to_string();
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = stream;
        stream
            .write_all(b"GET /_ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf);
        assert!(response.contains("200 OK"), "response: {response}");
        assert!(response.contains("OK"), "response: {response}");

        server.shutdown();
    }

    #[tokio::test]
    async fn create_node_registers_and_inspects() {
        let (router, _dir) = test_router().await;

        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "address": "10.0.0.2:50051",
                    "labels": {"gpu": "true"},
                    "cpus": 4.0,
                    "memory_bytes": 8589934592u64
                }))
                .unwrap(),
            ))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["ID"], "2", "next free id after the test node");
        assert_eq!(json["Spec"]["Labels"]["gpu"], "true");
        assert_eq!(
            json["Description"]["Resources"]["NanoCPUs"],
            4_000_000_000u64
        );

        let req = Request::builder()
            .uri("/v1.41/nodes/2")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["Status"]["State"], "ready");
        assert_eq!(json["Status"]["Addr"], "10.0.0.2:50051");
    }

    #[tokio::test]
    async fn create_node_rejects_duplicates_and_empty_address() {
        let (router, _dir) = test_router().await;

        // Duplicate id: the test node is 1.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({"id": 1, "address": "10.0.0.9:50051"}))
                    .unwrap(),
            ))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CONFLICT);

        // Duplicate address.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({"address": "127.0.0.1:50051"})).unwrap(),
            ))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CONFLICT);

        // Empty address.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({"address": "  "})).unwrap(),
            ))
            .unwrap();
        let (status, _) = send_request(router, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn update_node_drains_and_relabels() {
        let (router, _dir) = test_router().await;

        // Node 1 is the only node, hence the leader: role "manager".
        let body = serde_json::json!({
            "Name": "node-1",
            "Role": "manager",
            "Availability": "drain",
            "Labels": {"zone": "b"}
        });
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes/1/update?version=5")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);

        let req = Request::builder()
            .uri("/v1.41/nodes/1")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["Status"]["State"], "down", "drained node is down");
        assert_eq!(json["Spec"]["Labels"]["zone"], "b");
        assert!(
            json["Spec"]["Labels"].get("minga.node.id").is_some(),
            "the render-time id label is still attached"
        );
    }

    #[tokio::test]
    async fn update_node_rejects_what_minga_cannot_do() {
        let (router, _dir) = test_router().await;

        // Role change: node 1 is the only node, hence the leader.
        let body = serde_json::json!({"Role": "worker", "Availability": "active"});
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes/1/update")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(json["message"].as_str().unwrap().contains("role"));

        // Pause has no Minga semantics.
        let body = serde_json::json!({"Role": "manager", "Availability": "pause"});
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes/1/update")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(json["message"].as_str().unwrap().contains("pause"));

        // Rename.
        let body =
            serde_json::json!({"Name": "fancy", "Role": "manager", "Availability": "active"});
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes/1/update")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Unknown node.
        let body = serde_json::json!({"Role": "worker", "Availability": "active"});
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes/999/update")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, _) = send_request(router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn bearer_auth_rejects_and_accepts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        let router = build_router_with_auth(
            store,
            Arc::new(ReadyState::new()),
            Some("s3cret-token".to_string()),
        );

        // No header → 401.
        let req = Request::builder()
            .uri("/v1.41/version")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Wrong token → 401.
        let req = Request::builder()
            .uri("/v1.41/version")
            .header("Authorization", "Bearer wrong")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Right token → 200.
        let req = Request::builder()
            .uri("/v1.41/version")
            .header("Authorization", "Bearer s3cret-token")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["ApiVersion"], "1.41");
    }

    #[tokio::test]
    async fn create_service_fails_loudly_without_nodes() {
        let (router, _dir) = test_router_without_node().await;

        let spec = serde_json::json!({
            "Name": "nowhere_to_run",
            "TaskTemplate": {"ContainerSpec": {"Image": "nginx:latest"}},
            "Mode": {"Replicated": {"Replicas": 1}}
        });
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/services/create")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&spec).unwrap()))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            json["message"]
                .as_str()
                .unwrap()
                .contains("no eligible nodes"),
            "error must name the cause: {json}"
        );

        // Nothing was written to state.
        let req = Request::builder()
            .uri("/v1.41/services")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.as_array().unwrap().is_empty());
    }
}
