//! Axum router and server setup for the Docker Engine API.
//!
//! Routes are versioned: `/{version}/...` where `version` is `v1.41`
//! (or any `vX.Y` the client sends). The version segment is captured
//! but not validated — Minga always responds with its own API version.

use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::Extension,
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use minga_bus::{AuditRecord, EventBus};
use minga_state::{
    join_token::JoinTokenKey,
    types::{unix_now, DEFAULT_TENANT_ID},
    StateStore,
};
use minga_telemetry::{ReadyState, METRICS};
use rustls::pki_types::CertificateDer;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::auth::{ApiRole, Caller, ClientIdentity};
use crate::handlers::{self, AppState};

/// Marker that distinguishes token authentication from a certificate identity.
#[derive(Clone, Copy)]
struct TokenAuthenticated;

#[derive(Clone, Copy)]
struct InvalidClientIdentity;

/// How the caller proved who they are.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
    Token,
    Certificate,
    /// A tokenless direct client on a cluster whose only tenant is `default`.
    Legacy,
}
/// Combined state for all routes.
#[derive(Clone)]
struct ApiState {
    app_state: Arc<AppState>,
    /// Token accepted for cluster administration, when configured.
    auth_token: Option<Arc<str>>,
    store: Arc<StateStore>,
    tls_enabled: bool,
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

/// The token an `Authorization` header presents: `Bearer <token>`, or HTTP
/// Basic auth with the token as the password and any username. Basic exists
/// because Traefik's Swarm provider can only send `username`/`password`.
fn presented_token(header: &str) -> Option<String> {
    use base64::Engine as _;
    if let Some(token) = header.strip_prefix("Bearer ") {
        return Some(token.to_string());
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(header.strip_prefix("Basic ")?.trim())
        .ok()?;
    let credentials = String::from_utf8(decoded).ok()?;
    Some(credentials.split_once(':')?.1.to_string())
}

/// Authenticates the request, then checks tenant scope, operator-only paths
/// and role. Identity comes from a token, a verified certificate, or the
/// legacy direct caller, and always resolves to an existing tenant first.
async fn authenticate(
    axum::extract::State(state): axum::extract::State<ApiState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let (caller, source) = match resolve_caller(&state, req.headers(), req.extensions()).await {
        Ok(resolved) => resolved,
        Err(denied) => return denied,
    };
    if let Err(denied) = authorize(&caller, source, req.method(), req.uri().path()) {
        return denied;
    }
    if req.uri().path().starts_with("/minga/nodes/")
        && req.uri().path().ends_with("/join-token")
        && !state.tls_enabled
    {
        return auth_forbidden("node credentials may only be issued over mutual TLS");
    }
    if let Some(tenant_id) = caller.tenant_id.as_deref() {
        let tenant = match state.store.get_tenant(tenant_id).await {
            Ok(Some(tenant)) => tenant,
            Ok(None) => return auth_forbidden("tenant no longer exists"),
            Err(error) => return auth_unavailable(error),
        };
        if let Some(limit) = tenant.quotas.api_requests_per_second {
            let Some(bus) = state.app_state.bus.as_ref() else {
                return quota_unavailable("event bus is not configured");
            };
            match bus
                .check_tenant_request_quota(tenant_id, unix_now(), limit)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return (
                        axum::http::StatusCode::TOO_MANY_REQUESTS,
                        axum::Json(serde_json::json!({
                            "message": format!(
                                "tenant quota exceeded: api_requests_per_second limit {limit}"
                            )
                        })),
                    )
                        .into_response();
                }
                Err(error) => return quota_unavailable(error),
            }
        }
    }
    if !matches!(req.method().as_str(), "GET" | "HEAD" | "OPTIONS") {
        if let Some(tenant_id) = caller.tenant_id.as_deref() {
            let Some(bus) = state.app_state.bus.as_ref() else {
                return audit_unavailable("event bus is not configured");
            };
            let record = AuditRecord {
                tenant_id: tenant_id.to_owned(),
                actor: caller.name.clone(),
                action: req.method().to_string(),
                resource_path: req.uri().path().to_owned(),
                occurred_at_unix: unix_now(),
            };
            if let Err(error) = bus.append_audit(record).await {
                return audit_unavailable(format!("tenant audit append failed: {error}"));
            }
        }
    }
    if source == Source::Token {
        req.extensions_mut().insert(TokenAuthenticated);
    }
    req.extensions_mut().insert(caller);
    next.run(req).await
}

fn auth_unavailable(error: impl std::fmt::Display) -> axum::response::Response {
    tracing::error!(%error, "cannot resolve API caller tenant");
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(serde_json::json!({"message": "authentication failed"})),
    )
        .into_response()
}
fn quota_unavailable(error: impl std::fmt::Display) -> axum::response::Response {
    tracing::error!(%error, "tenant request quota service unavailable");
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({"message": "tenant request quota service unavailable"})),
    )
        .into_response()
}
fn audit_unavailable(error: impl std::fmt::Display) -> axum::response::Response {
    tracing::error!(%error, "tenant audit service unavailable");
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({"message": "tenant audit service unavailable"})),
    )
        .into_response()
}

fn auth_unauthorized(message: &str) -> axum::response::Response {
    (
        axum::http::StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({"message": message})),
    )
        .into_response()
}

// The Err is the HTTP reply itself, built at most once per request.
#[allow(clippy::result_large_err)]
/// Whether the default tenant is the cluster's only tenant. Read lazily:
/// a certificate naming its tenant needs one `get_tenant`, not the full list.
async fn sole_default_tenant(store: &StateStore) -> Result<bool, axum::response::Response> {
    let tenants = store.list_tenants().await.map_err(auth_unavailable)?;
    Ok(tenants.len() == 1 && tenants[0].id == DEFAULT_TENANT_ID)
}

// The Err is the HTTP reply itself, built at most once per request.
#[allow(clippy::result_large_err)]
/// Resolves who is calling and how they proved it.
async fn resolve_caller(
    state: &ApiState,
    headers: &axum::http::HeaderMap,
    extensions: &axum::http::Extensions,
) -> Result<(Caller, Source), axum::response::Response> {
    // An empty configured token must never match (`Basic Og==` presents "").
    let token_valid = state.auth_token.as_deref().is_some_and(|expected| {
        !expected.is_empty()
            && headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(presented_token)
                .is_some_and(|presented| constant_time_eq(&presented, expected))
    });
    if token_valid {
        let tenant_id = sole_default_tenant(&state.store)
            .await?
            .then(|| DEFAULT_TENANT_ID.to_owned());
        let caller = Caller {
            name: "token".to_owned(),
            role: ApiRole::Admin,
            tenant_id,
            cluster_operator: true,
        };
        return Ok((caller, Source::Token));
    }
    if extensions.get::<InvalidClientIdentity>().is_some() {
        return Err(auth_forbidden("client certificate identity is invalid; reissue it with one CN, one OU, and at most one O"));
    }
    if let Some(identity) = extensions.get::<ClientIdentity>() {
        let role = identity.role.ok_or_else(|| {
            auth_forbidden("client certificate has an unsupported role; reissue it with OU viewer, agent, deployer, or admin")
        })?;
        let (tenant_id, cluster_operator) = match identity.tenant.as_deref() {
            Some("*") => (None, true),
            Some(tenant) => {
                let known = state
                    .store
                    .get_tenant(tenant)
                    .await
                    .map_err(auth_unavailable)?;
                if known.is_none() {
                    return Err(auth_forbidden("client certificate names an unknown tenant"));
                }
                (Some(tenant.to_owned()), false)
            }
            None if sole_default_tenant(&state.store).await? => {
                (Some(DEFAULT_TENANT_ID.to_owned()), false)
            }
            None => return Err(auth_forbidden(
                "client certificate has no organization (O); reissue it with the tenant id in O",
            )),
        };
        let caller = Caller {
            name: identity.name.clone(),
            role,
            tenant_id,
            cluster_operator,
        };
        return Ok((caller, Source::Certificate));
    }
    if state.auth_token.is_some() {
        return Err(auth_unauthorized("authentication required"));
    }
    if state.tls_enabled {
        return Err(auth_unauthorized("verified client certificate required"));
    }
    if let Some(why) = rebinding_risk(headers) {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({
                "message": format!("{why}; a tokenless API only answers direct clients (set --api-token to lift this)")
            })),
        )
            .into_response());
    }
    if sole_default_tenant(&state.store).await? {
        let caller = Caller {
            name: "legacy-client".to_owned(),
            role: ApiRole::Admin,
            tenant_id: Some(DEFAULT_TENANT_ID.to_owned()),
            cluster_operator: false,
        };
        return Ok((caller, Source::Legacy));
    }
    Err(auth_forbidden(
        "no tenant identity is available; reissue the client certificate with the tenant id in O",
    ))
}

// The Err is the HTTP reply itself, built at most once per request.
#[allow(clippy::result_large_err)]
/// Tenant scope, operator-only paths and role. A cluster operator still needs
/// the role the operation requires: the `O=*` certificate does not lift it.
fn authorize(
    caller: &Caller,
    source: Source,
    method: &axum::http::Method,
    path: &str,
) -> Result<(), axum::response::Response> {
    if tenant_scoped_path(path) && caller.tenant_id.is_none() && !agent_usage_path(path) {
        return Err(auth_forbidden(
            "a certificate for a specific tenant is required to access tenant resources",
        ));
    }
    // The Docker CLI cannot send a token, and a sole-tenant cluster has nothing
    // to hide from its one tenant, so `docker info` / `docker node ls` and
    // health probes keep working there. Writes and `/minga/*` stay operator-only.
    let legacy_read = source == Source::Legacy
        && matches!(*method, axum::http::Method::GET | axum::http::Method::HEAD)
        && !path.starts_with("/minga/");
    let tenant_node_read = matches!(*method, axum::http::Method::GET | axum::http::Method::HEAD)
        && !path.starts_with("/minga/")
        && path.split('/').nth(2) == Some("nodes");
    if cluster_operator_path(path) && !caller.cluster_operator && !legacy_read && !tenant_node_read
    {
        return Err(auth_forbidden("cluster operator identity required"));
    }
    if *method == axum::http::Method::POST
        && path == "/minga/change-requests"
        && caller.role != ApiRole::Agent
    {
        return Err(auth_forbidden(
            "only an agent certificate may propose changes",
        ));
    }
    // Maker-checker needs a named reviewer: a shared token or a tokenless
    // legacy client resolves to a generic name, not a tenant's reviewer.
    if tenant_change_request_path(path)
        && path != "/minga/change-requests"
        && source != Source::Certificate
    {
        return Err(auth_forbidden(
            "deciding a change request requires a tenant reviewer certificate",
        ));
    }
    if !caller.role.permits(request_minimum_role(method, path)) {
        return Err(auth_forbidden("caller role does not permit this operation"));
    }
    Ok(())
}

fn auth_forbidden(message: &str) -> axum::response::Response {
    (
        axum::http::StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({"message": message})),
    )
        .into_response()
}
fn agent_usage_path(path: &str) -> bool {
    let mut segments = path.trim_matches('/').split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (Some(_), Some("agent"), Some("usage"), None)
    )
}

fn tenant_change_request_path(path: &str) -> bool {
    if path == "/minga/change-requests" {
        return true;
    }
    let mut segments = path.trim_matches('/').split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (
            Some("minga"),
            Some("change-requests"),
            Some(id),
            Some("approve" | "reject"),
            None
        ) if !id.is_empty()
    )
}

fn tenant_scoped_path(path: &str) -> bool {
    tenant_change_request_path(path)
        || (!path.starts_with("/minga/")
            && path.split('/').any(|segment| {
                matches!(
                    segment,
                    "services"
                        | "tasks"
                        | "networks"
                        | "secrets"
                        | "configs"
                        | "audit"
                        | "state"
                        | "agent"
                )
            }))
}

fn cluster_operator_path(path: &str) -> bool {
    (path.starts_with("/minga/") && !tenant_change_request_path(path))
        || path.split('/').nth(2) == Some("nodes")
        || path.split('/').nth(2) == Some("info")
        || matches!(path, "/healthz" | "/readyz" | "/metrics")
}

fn request_minimum_role(method: &axum::http::Method, path: &str) -> ApiRole {
    use axum::http::Method;
    if tenant_change_request_path(path) {
        if path == "/minga/change-requests" && matches!(*method, Method::GET | Method::HEAD) {
            ApiRole::Viewer
        } else if path == "/minga/change-requests" {
            ApiRole::Agent
        } else {
            ApiRole::Deployer
        }
    } else if path.split('/').nth(2) == Some("audit") || path == "/minga/audit" {
        ApiRole::Admin
    } else if method == Method::GET || method == Method::HEAD {
        ApiRole::Viewer
    } else if path.split('/').nth(2) == Some("nodes") {
        ApiRole::Admin
    } else if matches!(
        path.split('/').nth(2),
        Some("services" | "networks" | "configs" | "secrets")
    ) {
        ApiRole::Deployer
    } else {
        ApiRole::Admin
    }
}

/// Why a request to a tokenless API may come from a web page, if it may.
///
/// Without a token the bind address is the only boundary, and DNS rebinding
/// lets a page in a local browser cross it: the page reaches us under its own
/// hostname, and browsers send `Origin` on cross-site and same-origin writes.
/// Docker and minga clients send no `Origin`, and a `Host` that is an IP or a `localhost` name.
fn rebinding_risk(headers: &axum::http::HeaderMap) -> Option<&'static str> {
    if headers.contains_key(axum::http::header::ORIGIN) {
        return Some("request carries an Origin header");
    }
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // `[::1]:2375`, `10.0.0.5:2375`, `localhost`.
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(""),
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    // Browsers resolve `*.localhost` to loopback themselves (RFC 6761), so no
    // page can rebind it; the Docker client sends `api.moby.localhost`.
    let lower = name.to_ascii_lowercase();
    if name.is_empty()
        || lower == "localhost"
        || lower.ends_with(".localhost")
        || name.parse::<std::net::IpAddr>().is_ok()
    {
        None
    } else {
        Some("Host is a DNS name")
    }
}
/// Tenant and manager administration needs a cluster operator holding the
/// admin role: an `O=*` certificate with a lesser role is refused.
async fn require_operator_admin(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if req
        .extensions()
        .get::<Caller>()
        .is_some_and(|caller| caller.cluster_operator && caller.role == ApiRole::Admin)
    {
        next.run(req).await
    } else {
        auth_forbidden("cluster operator identity with the admin role required")
    }
}

async fn require_admin_role(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if req.extensions().get::<TokenAuthenticated>().is_some() {
        next.run(req).await
    } else {
        auth_forbidden("state backup and restore require the configured API token")
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
    build_router_with_app_state(
        Arc::new(AppState::new(store)),
        ready_state,
        auth_token,
        false,
    )
}

/// Builds the router with bus-backed task actual state.
pub fn build_router_with_bus_and_auth(
    store: Arc<StateStore>,
    bus: Arc<dyn EventBus>,
    ready_state: Arc<ReadyState>,
    auth_token: Option<String>,
) -> Router {
    build_router_with_app_state(
        Arc::new(AppState::with_bus(store, bus)),
        ready_state,
        auth_token,
        false,
    )
}

fn build_router_with_app_state(
    app_state: Arc<AppState>,
    ready_state: Arc<ReadyState>,
    auth_token: Option<String>,
    tls_enabled: bool,
) -> Router {
    let store = app_state.store.clone();
    let api_state = ApiState {
        app_state,
        auth_token: auth_token.map(Arc::from),
        store,
        tls_enabled,
    };

    Router::new()
        // Unversioned endpoints.
        .route("/_ping", get(handlers::ping))
        .route(
            "/minga/audit",
            get(handlers::list_cluster_audit)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/managers",
            get(handlers::list_managers)
                .post(handlers::add_manager)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/apply-rules",
            get(handlers::get_apply_rules)
                .post(handlers::raise_apply_rules)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/managers/:id",
            delete(handlers::remove_manager)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/managers/migrate-v1",
            post(handlers::migrate_manager_voters)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/nodes/:id/join-token",
            post(handlers::issue_node_join_token)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/tenants",
            get(handlers::list_tenants)
                .post(handlers::create_tenant)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/tenants/:id",
            get(handlers::get_tenant)
                .put(handlers::update_tenant)
                .delete(handlers::delete_tenant)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route(
            "/minga/change-requests",
            get(handlers::list_change_requests).post(handlers::create_change_request),
        )
        .route(
            "/minga/change-requests/:id/approve",
            post(handlers::approve_change_request),
        )
        .route(
            "/minga/change-requests/:id/reject",
            post(handlers::reject_change_request),
        )
        // Versioned endpoints: /{version}/...
        .route("/:version/version", get(handlers::get_version))
        .route(
            "/:version/minga/cluster",
            get(handlers::get_cluster_diagnostics)
                .layer(axum::middleware::from_fn(require_operator_admin)),
        )
        .route("/:version/info", get(handlers::get_info))
        .route("/:version/audit", get(handlers::list_audit))
        .route("/:version/agent/usage", get(handlers::agent_usage))
        .route("/:version/agent/events", get(handlers::agent_events))
        // Nodes.
        .route(
            "/:version/nodes",
            get(handlers::list_nodes).post(handlers::create_node),
        )
        .route(
            "/:version/nodes/:id",
            get(handlers::get_node).delete(handlers::remove_node),
        )
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
        .route("/:version/tasks/:id/logs", get(handlers::task_logs))
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
        // State backup and restore (admin-only once roles exist, ADR 0005).
        .route(
            "/:version/state",
            get(handlers::export_state)
                .post(handlers::import_state)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
                .layer(axum::middleware::from_fn(require_admin_role)),
        )
        // Fallback.
        .fallback(handlers::fallback)
        .with_state(api_state.clone())
        // Observability endpoints: /metrics, /healthz, /readyz.
        .merge(minga_telemetry::router(ready_state.as_ref().clone()))
        .layer(axum::middleware::from_fn_with_state(
            api_state,
            authenticate,
        ))
}

/// Server TLS material. The client CA is used to require and verify mTLS peers.
#[derive(Debug, Clone)]
pub struct ApiTlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: PathBuf,
}

impl ApiTlsConfig {
    fn server_config(&self) -> Result<Arc<rustls::ServerConfig>, std::io::Error> {
        let load_certs = |path: &Path| {
            rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(path)?))
                .collect::<Result<Vec<CertificateDer<'static>>, _>>()
        };
        let mut roots = rustls::RootCertStore::empty();
        for cert in load_certs(&self.client_ca)? {
            roots
                .add(cert)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let certs = load_certs(&self.cert)?;
        let key_file = std::fs::File::open(&self.key)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = key_file.metadata()?.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "API private key {} has mode {mode:o}; it must not be readable by group or others (chmod 600)",
                        self.key.display()
                    ),
                ));
            }
        }
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "no private key found")
        })?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        Ok(Arc::new(config))
    }
}

/// Limits of the API listener on top of the connection limits every Minga
/// listener has.
#[derive(Debug, Clone, Copy)]
struct ApiLimits {
    connection: minga_telemetry::serve::Limits,
    /// A request body must arrive in full within this time. The header-read
    /// timeout ends with the head, so without it a body that never completes
    /// holds its connection slot for good.
    body: Duration,
    /// `POST /state` carries up to 64 MiB, so it gets longer.
    state_import_body: Duration,
    /// TLS connections one certificate tenant may hold at once.
    per_tenant_connections: usize,
    /// TLS connection slots only cluster operators (`O=*`) may use.
    operator_reserve: usize,
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self {
            connection: minga_telemetry::serve::Limits::default(),
            body: Duration::from_secs(30),
            state_import_body: Duration::from_secs(300),
            per_tenant_connections: 128,
            operator_reserve: 64,
        }
    }
}

/// Fails a request body that has not fully arrived by its deadline, so the
/// extractor reading it errors and the connection closes.
fn with_body_deadline(router: Router, limits: ApiLimits) -> Router {
    use futures::StreamExt;
    router.layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| async move {
            let budget = if request.uri().path().split('/').nth(2) == Some("state") {
                limits.state_import_body
            } else {
                limits.body
            };
            let deadline = tokio::time::Instant::now() + budget;
            let request = request.map(|body| {
                axum::body::Body::from_stream(futures::stream::unfold(
                    Some(body.into_data_stream()),
                    move |stream| async move {
                        let mut stream = stream?;
                        match tokio::time::timeout_at(deadline, stream.next()).await {
                            Ok(chunk) => chunk.map(|chunk| (chunk, Some(stream))),
                            Err(_) => {
                                Some((Err(axum::Error::new("request body deadline passed")), None))
                            }
                        }
                    },
                ))
            });
            next.run(request).await
        },
    ))
}

async fn serve_tls_bounded(
    listener: TcpListener,
    router: Router,
    acceptor: TlsAcceptor,
    limits: ApiLimits,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(
        limits.connection.max_connections,
    ));
    // Tenants share all slots but the operator reserve, and each tenant gets
    // at most its own cap, so one tenant cannot lock out the others (#123).
    let tenant_slots = Arc::new(tokio::sync::Semaphore::new(
        limits
            .connection
            .max_connections
            .saturating_sub(limits.operator_reserve),
    ));
    // shortcut: one entry per tenant name a CA-signed certificate carried, never
    // pruned; prune idle entries if certificates ever name unbounded tenants.
    let per_tenant: Arc<
        std::sync::Mutex<std::collections::HashMap<Option<String>, Arc<tokio::sync::Semaphore>>>,
    > = Arc::default();
    // Dropping the set, when the server's task is aborted, aborts every open
    // connection with it.
    let mut connections = tokio::task::JoinSet::new();
    loop {
        while connections.try_join_next().is_some() {}
        let permit = match slots.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                tracing::warn!(%error, "TLS API accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let router = router.clone();
        let tenant_slots = tenant_slots.clone();
        let per_tenant = per_tenant.clone();
        connections.spawn(async move {
            let _permit = permit;
            let header_read_timeout = limits.connection.header_read_timeout;
            if tokio::time::timeout(header_read_timeout, stream.readable())
                .await
                .is_err()
            {
                return;
            }
            let Ok(Ok(stream)) =
                tokio::time::timeout(header_read_timeout, acceptor.accept(stream)).await
            else {
                return;
            };
            let Some(cert) = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
            else {
                return;
            };
            let identity = ClientIdentity::from_certificate(cert);
            let tenant = identity
                .as_ref()
                .and_then(|identity| identity.tenant.clone());
            let _tenant_permits = if tenant.as_deref() == Some("*") {
                None
            } else {
                let own = per_tenant
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .entry(tenant.clone())
                    .or_insert_with(|| {
                        Arc::new(tokio::sync::Semaphore::new(limits.per_tenant_connections))
                    })
                    .clone();
                let (Ok(own), Ok(shared)) =
                    (own.try_acquire_owned(), tenant_slots.try_acquire_owned())
                else {
                    tracing::debug!(?tenant, "TLS API connection over the tenant cap");
                    return;
                };
                Some((own, shared))
            };
            let router = match identity {
                Some(identity) => router.layer(Extension(identity)),
                None => router.layer(Extension(InvalidClientIdentity)),
            };
            // HTTP/1.1 only, like the plaintext listeners: no ALPN is offered
            // and an HTTP/2 preface is rejected within the header timeout.
            if let Err(error) =
                minga_telemetry::serve::serve_connection(stream, router, limits.connection).await
            {
                tracing::debug!(%error, "TLS API connection closed");
            }
        });
    }
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
    /// must carry it as `Authorization: Bearer <token>` or as the Basic-auth
    /// password.
    pub async fn start_with_auth(
        store: Arc<StateStore>,
        bind: &str,
        auth_token: Option<String>,
    ) -> Result<Self, ApiServerError> {
        Self::start_inner(store, None, bind, auth_token, None, None, None).await
    }

    /// Starts the API server with actual task state read from the event bus.
    pub async fn start_with_bus_and_auth(
        store: Arc<StateStore>,
        bus: Arc<dyn EventBus>,
        bind: &str,
        auth_token: Option<String>,
    ) -> Result<Self, ApiServerError> {
        Self::start_inner(store, Some(bus), bind, auth_token, None, None, None).await
    }

    /// Starts the API over bounded mutual TLS, requiring a CA-verified caller certificate.
    pub async fn start_with_tls(
        store: Arc<StateStore>,
        bind: &str,
        auth_token: Option<String>,
        tls: ApiTlsConfig,
    ) -> Result<Self, ApiServerError> {
        Self::start_inner(store, None, bind, auth_token, Some(tls), None, None).await
    }

    /// Starts the bus-backed API with optional mutual TLS.
    pub async fn start_with_bus_and_auth_and_tls(
        store: Arc<StateStore>,
        bus: Arc<dyn EventBus>,
        bind: &str,
        auth_token: Option<String>,
        tls: Option<ApiTlsConfig>,
    ) -> Result<Self, ApiServerError> {
        Self::start_inner(store, Some(bus), bind, auth_token, tls, None, None).await
    }

    /// Starts the bus-backed API with optional TLS and manager-local signing.
    pub async fn start_with_bus_and_auth_and_tls_and_join_token_key(
        store: Arc<StateStore>,
        bus: Arc<dyn EventBus>,
        bind: &str,
        auth_token: Option<String>,
        tls: Option<ApiTlsConfig>,
        join_token_key: Option<Arc<JoinTokenKey>>,
    ) -> Result<Self, ApiServerError> {
        Self::start_inner(
            store,
            Some(bus),
            bind,
            auth_token,
            tls,
            join_token_key,
            None,
        )
        .await
    }
    /// Starts the bus-backed API with tenant-scoped NATS credential issuance.
    pub async fn start_with_bus_and_auth_and_tls_and_signers(
        store: Arc<StateStore>,
        bus: Arc<dyn EventBus>,
        bind: &str,
        auth_token: Option<String>,
        tls: Option<ApiTlsConfig>,
        join_token_key: Option<Arc<JoinTokenKey>>,
        nats_credential_issuer: Option<Arc<crate::nats_credentials::NatsCredentialIssuer>>,
    ) -> Result<Self, ApiServerError> {
        Self::start_inner(
            store,
            Some(bus),
            bind,
            auth_token,
            tls,
            join_token_key,
            nats_credential_issuer,
        )
        .await
    }

    async fn start_inner(
        store: Arc<StateStore>,
        bus: Option<Arc<dyn EventBus>>,
        bind: &str,
        auth_token: Option<String>,
        tls: Option<ApiTlsConfig>,
        join_token_key: Option<Arc<JoinTokenKey>>,
        nats_credential_issuer: Option<Arc<crate::nats_credentials::NatsCredentialIssuer>>,
    ) -> Result<Self, ApiServerError> {
        let acceptor = tls
            .as_ref()
            .map(ApiTlsConfig::server_config)
            .transpose()
            .map_err(|error| ApiServerError::Tls(error.to_string()))?
            .map(TlsAcceptor::from);
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|e| ApiServerError::Bind(format!("cannot bind {bind}: {e}")))?;

        let addr = listener
            .local_addr()
            .map_err(|e| ApiServerError::Bind(format!("cannot get local addr: {e}")))?
            .to_string();

        // Spawned only once startup can no longer fail, so a failed bind leaks no task.
        let ready_state = ReadyState::without_bus();
        let mut app_state = match bus {
            Some(bus) => AppState::with_bus(store.clone(), bus),
            None => AppState::new(store.clone()),
        };
        if let Some(key) = join_token_key {
            app_state = app_state.with_join_token_key(key);
        }
        if let Some(issuer) = nats_credential_issuer {
            app_state = app_state.with_nats_credential_issuer(issuer);
        }
        let app_state = Arc::new(app_state);
        let probe = {
            let (store, ready, app) = (store, ready_state.clone(), app_state.clone());
            tokio::spawn(async move {
                let mut revocations_published = false;
                loop {
                    ready.set_raft_ready(store.raft_ready());
                    let leader = store.is_leader().await;
                    METRICS.raft_leader.set(if leader { 1.0 } else { 0.0 });
                    revocations_published =
                        crate::handlers::sync_nats_revocations(&app, leader, revocations_published)
                            .await;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            })
        };
        let ready_state = Arc::new(ready_state);
        let limits = ApiLimits::default();
        let router = with_body_deadline(
            build_router_with_app_state(app_state, ready_state, auth_token, acceptor.is_some()),
            limits,
        );
        let handle = if let Some(acceptor) = acceptor {
            tokio::spawn(serve_tls_bounded(listener, router, acceptor, limits))
        } else {
            tokio::spawn(minga_telemetry::serve::serve_bounded(
                listener,
                router,
                limits.connection,
                std::future::pending(),
            ))
        };

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
    #[error("TLS configuration: {0}")]
    Tls(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use minga_bus::{subjects, EventBus, MemoryBus, TaskActualState, TaskStatusReport};
    use minga_state::types::{Network, Task, TaskSpec, TaskState, Volume};
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    #[test]
    fn tenant_audit_is_admin_only_and_tenant_scoped() {
        let path = "/v1.41/audit";
        let admin = Caller {
            name: "tenant-admin".into(),
            role: ApiRole::Admin,
            tenant_id: Some("acme".into()),
            cluster_operator: false,
        };
        assert!(authorize(&admin, Source::Certificate, &axum::http::Method::GET, path).is_ok());
        assert!(authorize(
            &admin,
            Source::Certificate,
            &axum::http::Method::GET,
            "/minga/audit",
        )
        .is_err());

        let viewer = Caller {
            role: ApiRole::Viewer,
            ..admin.clone()
        };
        assert!(authorize(&viewer, Source::Certificate, &axum::http::Method::GET, path).is_err());

        let operator = Caller {
            name: "operator".into(),
            role: ApiRole::Admin,
            tenant_id: None,
            cluster_operator: true,
        };
        assert!(authorize(
            &operator,
            Source::Certificate,
            &axum::http::Method::GET,
            path
        )
        .is_err());
        assert!(authorize(
            &operator,
            Source::Certificate,
            &axum::http::Method::GET,
            "/minga/audit",
        )
        .is_ok());
    }

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
                    tenant_id: minga_state::types::DEFAULT_TENANT_ID.to_owned(),
                    last_seen_unix: None,
                    role: Default::default(),
                    reserved_ports: Vec::new(),
                    nats_user_public_key: None,
                })
                .await
                .unwrap();
        }
        let ready_state = Arc::new(ReadyState::new());
        let router = build_router_with_bus_and_auth(store, MemoryBus::new(), ready_state, None);
        (router, dir)
    }

    async fn admin_router() -> (Router, Arc<StateStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        let router = build_router_with_bus_and_auth(
            store.clone(),
            MemoryBus::new(),
            Arc::new(ReadyState::new()),
            Some("admin-token".to_string()),
        );
        (router, store, dir)
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

    fn operator_request(mut req: Request<Body>) -> Request<Body> {
        req.extensions_mut().insert(ClientIdentity {
            name: "test-operator".into(),
            role: Some(ApiRole::Admin),
            tenant: Some("*".into()),
        });
        req
    }

    async fn send_operator_request(
        router: Router,
        req: Request<Body>,
    ) -> (StatusCode, serde_json::Value) {
        send_request(router, operator_request(req)).await
    }

    #[tokio::test]
    async fn metrics_preserve_usage_for_operators_only() {
        let (router, _store, _dir) = admin_router().await;
        minga_telemetry::METRICS.record_agent_usage("api-metrics-tenant", 11, 13, 17);
        minga_telemetry::METRICS
            .agent_usage_events_dropped_total
            .with_label_values(&["api-metrics-tenant"])
            .inc();
        let req = || {
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap()
        };
        let (status, body) = send_operator_request(router.clone(), req()).await;
        assert_eq!(status, StatusCode::OK);
        let text = body.as_str().unwrap();
        for family in [
            "requests",
            "input_tokens",
            "output_tokens",
            "cost_micros",
            "events_dropped",
        ] {
            assert!(
                text.contains(&format!(
                    "minga_agent_usage_{family}_total{{tenant=\"api-metrics-tenant\"}}"
                )),
                "missing {family}: {text}"
            );
        }
        let mut tenant_request = req();
        tenant_request.extensions_mut().insert(ClientIdentity {
            name: "tenant-admin".into(),
            role: Some(ApiRole::Admin),
            tenant: Some("default".into()),
        });
        let (status, _) = send_request(router.clone(), tenant_request).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = send_request(router, req()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    fn service_record(id: &str, tenant_id: &str) -> minga_state::types::Service {
        minga_state::types::Service {
            id: id.into(),
            tenant_id: tenant_id.into(),
            name: id.into(),
            image: "nginx:latest".into(),
            replicas: 1,
            mode: minga_state::types::ServiceMode::Replicated,
            is_job: false,
            constraints: vec![],
            env: Default::default(),
            ports: vec![],
            networks: vec![],
            resources: minga_state::types::ResourceSpec {
                cpus: 0.0,
                memory_bytes: 0,
                gpus: 0,
            },
            agent: None,
            egress: None,
            restart_policy: Default::default(),
            secrets: vec![],
            command: vec![],
            entrypoint: vec![],
            labels: Default::default(),
            healthcheck: None,
            mounts: vec![],
            user: None,
            extra_hosts: vec![],
            update: Default::default(),
            rollback: None,
            previous: None,
        }
    }

    fn node_record(id: u64, tenant_id: &str) -> minga_state::types::ClusterNode {
        minga_state::types::ClusterNode {
            id,
            address: format!("127.0.0.1:{}", 50000 + id),
            labels: Default::default(),
            status: minga_state::types::NodeStatus::Active,
            capacity: minga_state::types::ResourceSpec {
                cpus: 8.0,
                memory_bytes: 1024,
                gpus: 0,
            },
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
            tenant_id: tenant_id.to_owned(),
            last_seen_unix: None,
            role: Default::default(),
            reserved_ports: Vec::new(),
            nats_user_public_key: None,
        }
    }

    #[tokio::test]
    async fn operator_certificates_need_the_role_the_operation_requires() {
        let (router, _store, _dir) = admin_router().await;
        let writes = [
            (
                "/minga/tenants",
                r#"{"id":"tenant-x","name":"x","quotas":{}}"#,
            ),
            ("/v1.41/nodes/1/update", "{}"),
        ];
        for role in [ApiRole::Viewer, ApiRole::Deployer] {
            for (uri, body) in writes {
                let mut req = Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap();
                req.extensions_mut().insert(ClientIdentity {
                    name: "operator".into(),
                    role: Some(role),
                    tenant: Some("*".into()),
                });
                let (status, _) = send_request(router.clone(), req).await;
                assert_eq!(status, StatusCode::FORBIDDEN, "{role:?} on {uri}");
            }
        }
    }

    fn identity_request(
        method: &str,
        uri: &str,
        body: &str,
        name: &str,
        role: ApiRole,
        tenant: &str,
    ) -> Request<Body> {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        req.extensions_mut().insert(ClientIdentity {
            name: name.into(),
            role: Some(role),
            tenant: Some(tenant.into()),
        });
        req
    }

    #[tokio::test]
    async fn apply_rules_route_is_for_cluster_operators_only() {
        let (router, store, _dir) = admin_router().await;
        store
            .upsert_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "tenant-a".into(),
                name: "A".into(),
                quotas: Default::default(),
                created_at: 0,
            })
            .await
            .unwrap();
        for (method, body) in [("GET", ""), ("POST", r#"{"level":1}"#)] {
            // A tenant administrator, and an `O=*` identity below the admin role.
            for (role, tenant) in [
                (ApiRole::Admin, "tenant-a"),
                (ApiRole::Deployer, "*"),
                (ApiRole::Viewer, "*"),
            ] {
                let req =
                    identity_request(method, "/minga/apply-rules", body, "caller", role, tenant);
                let (status, _) = send_request(router.clone(), req).await;
                assert_eq!(
                    status,
                    StatusCode::FORBIDDEN,
                    "{method} as {role:?} in {tenant}"
                );
            }
            // No credential at all.
            let req = Request::builder()
                .method(method)
                .uri("/minga/apply-rules")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let (status, _) = send_request(router.clone(), req).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} without credentials"
            );
        }
        assert_eq!(store.apply_rules().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn apply_rules_route_reads_and_raises_for_an_operator_and_the_admin_token() {
        let (router, store, _dir) = admin_router().await;
        let expected = serde_json::json!({
            "level": 1,
            "implemented": 1,
            "managers": [{"id": 1, "implemented": 1}],
        });

        let req = identity_request("GET", "/minga/apply-rules", "", "ops", ApiRole::Admin, "*");
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, expected);

        let req = Request::builder()
            .uri("/minga/apply-rules")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, expected);

        // This build implements level 1 only, so a raise past it is a conflict.
        let req = identity_request(
            "POST",
            "/minga/apply-rules",
            r#"{"level":2}"#,
            "ops",
            ApiRole::Admin,
            "*",
        );
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(body["message"].as_str().unwrap().contains("up to level 1"));

        // Raising to the level in force answers with the status.
        let req = identity_request(
            "POST",
            "/minga/apply-rules",
            r#"{"level":1}"#,
            "ops",
            ApiRole::Admin,
            "*",
        );
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, expected);
        assert_eq!(store.apply_rules().await.unwrap(), 1);

        let req = identity_request(
            "POST",
            "/minga/apply-rules",
            r#"{"level":"two"}"#,
            "ops",
            ApiRole::Admin,
            "*",
        );
        let (status, _) = send_request(router, req).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn apply_rules_raise_is_refused_while_a_manager_implements_a_lower_level() {
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
        let store2 = Arc::new(StateStore::open_join(2, &addr2, dir2.path()).await.unwrap());
        store1.add_manager(2, addr2).await.unwrap();
        // Manager 1 is upgraded to a build that implements level 2; manager 2 is not.
        store1.pretend_implemented_apply_rules(2);
        let router = build_router_with_bus_and_auth(
            store1.clone(),
            MemoryBus::new(),
            Arc::new(ReadyState::new()),
            Some("admin-token".into()),
        );

        let req = Request::builder()
            .uri("/minga/apply-rules")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["implemented"], 2);
        assert_eq!(
            body["managers"],
            serde_json::json!([
                {"id": 1, "implemented": 2},
                {"id": 2, "implemented": 1},
            ])
        );

        let req = Request::builder()
            .method("POST")
            .uri("/minga/apply-rules")
            .header("Authorization", "Bearer admin-token")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"level":2}"#))
            .unwrap();
        let (status, body) = send_request(router, req).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("manager 2 (level 1)"),
            "{body}"
        );
        assert_eq!(store1.apply_rules().await.unwrap(), 1);

        Arc::try_unwrap(store1)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        Arc::try_unwrap(store2)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tokenless_sole_tenant_caller_reads_inventory_but_cannot_administer() {
        let (_router, store, _dir) = admin_router().await;
        let router = build_router_with_auth(store.clone(), Arc::new(ReadyState::new()), None);
        let call = |method: &'static str, uri: &'static str| {
            let router = router.clone();
            async move {
                let req = Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap();
                send_request(router, req).await.0
            }
        };
        assert_eq!(call("GET", "/v1.41/info").await, StatusCode::OK);
        assert_eq!(call("GET", "/v1.41/nodes").await, StatusCode::OK);
        assert_eq!(
            call("POST", "/v1.41/nodes/1/update").await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(call("GET", "/minga/tenants").await, StatusCode::FORBIDDEN);
        assert_eq!(call("POST", "/minga/tenants").await, StatusCode::FORBIDDEN);

        // A second tenant ends the compatibility access.
        store
            .upsert_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "tenant-a".into(),
                name: "A".into(),
                quotas: Default::default(),
                created_at: 0,
            })
            .await
            .unwrap();
        assert_eq!(call("GET", "/v1.41/info").await, StatusCode::FORBIDDEN);
        assert_eq!(call("GET", "/v1.41/nodes").await, StatusCode::FORBIDDEN);
    }

    fn test_ca() -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        (params.self_signed(&key).unwrap(), key)
    }

    fn test_leaf(
        ca: &(rcgen::Certificate, rcgen::KeyPair),
        sans: &[&str],
        subject: &[(rcgen::DnType, &str)],
        usage: rcgen::ExtendedKeyUsagePurpose,
    ) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params =
            rcgen::CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        for (kind, value) in subject {
            params.distinguished_name.push(kind.clone(), *value);
        }
        params.extended_key_usages.push(usage);
        let cert = params.signed_by(&key, &ca.0, &ca.1).unwrap();
        (cert, key)
    }

    /// One GET over a real TLS connection; `None` when the connection fails.
    async fn tls_status(
        addr: &str,
        ca: &rcgen::Certificate,
        client: Option<&(rcgen::Certificate, rcgen::KeyPair)>,
        path: &str,
    ) -> Option<u16> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut tls = tls_connect(addr, ca, client).await?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        tls.write_all(request.as_bytes()).await.ok()?;
        let mut reply = Vec::new();
        // A server that drops the connection mid-read leaves what it sent.
        let _ = tls.read_to_end(&mut reply).await;
        String::from_utf8_lossy(&reply)
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    }

    async fn tls_connect(
        addr: &str,
        ca: &rcgen::Certificate,
        client: Option<&(rcgen::Certificate, rcgen::KeyPair)>,
    ) -> Option<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
        let config = match client {
            Some((cert, key)) => builder
                .with_client_auth_cert(
                    vec![cert.der().clone()],
                    rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        let tcp = tokio::net::TcpStream::connect(addr).await.ok()?;
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .ok()
    }

    #[tokio::test]
    async fn mtls_accepts_ca_signed_clients_and_maps_them_to_roles() {
        use std::os::unix::fs::PermissionsExt;
        let ca = test_ca();
        let server = test_leaf(
            &ca,
            &["localhost"],
            &[(rcgen::DnType::CommonName, "minga-api")],
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        );
        let client = |ou: &str| {
            test_leaf(
                &ca,
                &[],
                &[
                    (rcgen::DnType::CommonName, "ci"),
                    (rcgen::DnType::OrganizationalUnitName, ou),
                    (rcgen::DnType::OrganizationName, "default"),
                ],
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            )
        };
        let files = tempfile::tempdir().unwrap();
        let write = |name: &str, pem: String, mode: u32| {
            let path = files.path().join(name);
            std::fs::write(&path, pem).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            path
        };
        let tls = ApiTlsConfig {
            cert: write("server.pem", server.0.pem(), 0o644),
            key: write("server-key.pem", server.1.serialize_pem(), 0o600),
            client_ca: write("ca.pem", ca.0.pem(), 0o644),
        };
        let (_router, store, _dir) = admin_router().await;
        let api = ApiServer::start_with_tls(store, "127.0.0.1:0", None, tls)
            .await
            .unwrap();
        let addr = api.addr().to_owned();

        let viewer = client("viewer");
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&viewer), "/v1.41/version").await,
            Some(200)
        );
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&viewer), "/v1.41/services").await,
            Some(200)
        );
        // A tenant certificate is not a cluster operator.
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&viewer), "/minga/tenants").await,
            Some(403)
        );
        // `agent` is a role: it reads like a viewer but is still no operator.
        let agent = client("agent");
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&agent), "/v1.41/version").await,
            Some(200)
        );
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&agent), "/minga/tenants").await,
            Some(403)
        );
        // An OU that is not a role is refused, not guessed at.
        let unknown = client("intern");
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&unknown), "/v1.41/version").await,
            Some(403)
        );
        // A certificate from another CA, or none, never reaches the router.
        let rogue_ca = test_ca();
        let rogue = test_leaf(
            &rogue_ca,
            &[],
            &[
                (rcgen::DnType::CommonName, "ci"),
                (rcgen::DnType::OrganizationalUnitName, "admin"),
                (rcgen::DnType::OrganizationName, "default"),
            ],
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        );
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&rogue), "/v1.41/version").await,
            None
        );
        assert_eq!(tls_status(&addr, &ca.0, None, "/v1.41/version").await, None);
        // HTTP/1.1 only: an HTTP/2 preface that then goes quiet is closed, so
        // it does not hold a connection slot.
        let mut h2 = tls_connect(&addr, &ca.0, Some(&viewer)).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut h2, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let mut rest = Vec::new();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::io::AsyncReadExt::read_to_end(&mut h2, &mut rest),
        )
        .await;
        assert!(closed.is_ok(), "an idle HTTP/2 connection stayed open");
        api.shutdown();
    }

    /// A TLS API on `limits` whose store also knows `tenants`.
    async fn limited_tls_api(
        limits: ApiLimits,
        tenants: &[&str],
    ) -> (
        String,
        (rcgen::Certificate, rcgen::KeyPair),
        [tempfile::TempDir; 2],
    ) {
        use std::os::unix::fs::PermissionsExt;
        let ca = test_ca();
        let server = test_leaf(
            &ca,
            &["localhost"],
            &[(rcgen::DnType::CommonName, "minga-api")],
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        );
        let files = tempfile::tempdir().unwrap();
        let write = |name: &str, pem: String, mode: u32| {
            let path = files.path().join(name);
            std::fs::write(&path, pem).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            path
        };
        let tls = ApiTlsConfig {
            cert: write("server.pem", server.0.pem(), 0o644),
            key: write("server-key.pem", server.1.serialize_pem(), 0o600),
            client_ca: write("ca.pem", ca.0.pem(), 0o644),
        };
        let (_router, store, dir) = admin_router().await;
        for &id in tenants {
            store
                .upsert_tenant(minga_state::types::Tenant {
                    nats_revoked_user_public_keys: Vec::new(),
                    id: id.into(),
                    name: id.into(),
                    quotas: Default::default(),
                    created_at: 1,
                })
                .await
                .unwrap();
        }
        let router = with_body_deadline(
            build_router_with_app_state(
                Arc::new(AppState::with_bus(store, MemoryBus::new())),
                Arc::new(ReadyState::new()),
                Some("admin-token".to_string()),
                true,
            ),
            limits,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let acceptor = TlsAcceptor::from(tls.server_config().unwrap());
        tokio::spawn(serve_tls_bounded(listener, router, acceptor, limits));
        (addr, ca, [files, dir])
    }

    fn client_cert(
        ca: &(rcgen::Certificate, rcgen::KeyPair),
        role: &str,
        tenant: &str,
    ) -> (rcgen::Certificate, rcgen::KeyPair) {
        test_leaf(
            ca,
            &[],
            &[
                (rcgen::DnType::CommonName, "client"),
                (rcgen::DnType::OrganizationalUnitName, role),
                (rcgen::DnType::OrganizationName, tenant),
            ],
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        )
    }

    /// Opens a connection, waits for one served request so the server has
    /// counted it, then starts a POST to `path` whose body never completes.
    /// `headers` are extra request header lines, each ending in CRLF.
    async fn stalled_body(
        addr: &str,
        ca: &rcgen::Certificate,
        client: &(rcgen::Certificate, rcgen::KeyPair),
        path: &str,
        headers: &str,
    ) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut tls = tls_connect(addr, ca, Some(client)).await.unwrap();
        tls.write_all(b"GET /v1.41/version HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 16];
        assert!(tls.read(&mut buf).await.unwrap() > 0);
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\n{headers}Content-Type: application/json\r\nContent-Length: 100\r\n\r\n{{"
        );
        tls.write_all(head.as_bytes()).await.unwrap();
        tls
    }

    /// Reads until the server closes the connection; false after `within`.
    async fn closed_within(
        tls: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
        within: Duration,
    ) -> bool {
        let mut buf = [0u8; 1024];
        tokio::time::timeout(within, async {
            loop {
                match tokio::io::AsyncReadExt::read(tls, &mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        })
        .await
        .is_ok()
    }

    #[tokio::test]
    async fn a_tenant_holding_its_cap_of_stalled_bodies_locks_out_no_one_else() {
        let limits = ApiLimits {
            connection: minga_telemetry::serve::Limits {
                max_connections: 6,
                ..Default::default()
            },
            per_tenant_connections: 2,
            operator_reserve: 2,
            ..ApiLimits::default()
        };
        let (addr, ca, _dirs) = limited_tls_api(limits, &["tenant-a", "tenant-b"]).await;
        let tenant_a = client_cert(&ca, "deployer", "tenant-a");
        let tenant_b = client_cert(&ca, "deployer", "tenant-b");
        let operator = client_cert(&ca, "admin", "*");
        let create = "/v1.41/services/create";

        let _a1 = stalled_body(&addr, &ca.0, &tenant_a, create, "").await;
        let _a2 = stalled_body(&addr, &ca.0, &tenant_a, create, "").await;
        // Tenant A is at its cap; tenant B and the operator are not.
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&tenant_a), "/v1.41/version").await,
            None
        );
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&tenant_b), "/v1.41/version").await,
            Some(200)
        );
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&operator), "/minga/tenants").await,
            Some(200)
        );
        // With every tenant slot held, the operator reserve still serves.
        let _b1 = stalled_body(&addr, &ca.0, &tenant_b, create, "").await;
        let _b2 = stalled_body(&addr, &ca.0, &tenant_b, create, "").await;
        let tenant_c = client_cert(&ca, "deployer", "tenant-c");
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&tenant_c), "/v1.41/version").await,
            None
        );
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&operator), "/minga/tenants").await,
            Some(200)
        );
    }

    #[tokio::test]
    async fn a_stalled_request_body_is_closed_at_its_deadline() {
        let limits = ApiLimits {
            body: Duration::from_millis(300),
            state_import_body: Duration::from_millis(1500),
            per_tenant_connections: 1,
            ..ApiLimits::default()
        };
        // A sole default tenant, so the API token may import state.
        let (addr, ca, _dirs) = limited_tls_api(limits, &[]).await;
        let tenant = client_cert(&ca, "deployer", "default");
        let create = "/v1.41/services/create";
        let mut stalled = stalled_body(&addr, &ca.0, &tenant, create, "").await;
        assert!(
            closed_within(&mut stalled, Duration::from_secs(2)).await,
            "a stalled body kept its connection"
        );
        // Its slot is free again: the tenant's cap is one connection.
        assert_eq!(
            tls_status(&addr, &ca.0, Some(&tenant), "/v1.41/version").await,
            Some(200)
        );

        // `/state` import gets longer, but not forever.
        let operator = client_cert(&ca, "admin", "*");
        let token = "Authorization: Bearer admin-token\r\n";
        let mut import = stalled_body(&addr, &ca.0, &operator, "/v1.41/state", token).await;
        assert!(
            !closed_within(&mut import, Duration::from_millis(800)).await,
            "a /state import got only the default body deadline"
        );
        assert!(
            closed_within(&mut import, Duration::from_secs(3)).await,
            "a stalled /state import kept its connection"
        );
    }

    #[tokio::test]
    async fn a_body_dripped_byte_by_byte_is_closed_at_its_deadline() {
        use tokio::io::AsyncWriteExt;
        let limits = ApiLimits {
            body: Duration::from_millis(300),
            ..ApiLimits::default()
        };
        let (addr, ca, _dirs) = limited_tls_api(limits, &[]).await;
        let tenant = client_cert(&ca, "deployer", "default");
        let mut drip = stalled_body(&addr, &ca.0, &tenant, "/v1.41/services/create", "").await;
        // Each byte lands well inside the deadline; the whole body does not.
        let started = tokio::time::Instant::now();
        while started.elapsed() < Duration::from_secs(3) {
            if drip.write_all(b" ").await.is_err()
                || drip.flush().await.is_err()
                || closed_within(&mut drip, Duration::from_millis(100)).await
            {
                break;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a body dripped byte by byte kept its connection"
        );
    }

    #[tokio::test]
    async fn a_service_name_cannot_be_taken_over_by_another_tenant() {
        let (_router, store, _dir) = admin_router().await;
        for id in ["tenant-a", "tenant-b"] {
            store
                .create_tenant(minga_state::types::Tenant {
                    nats_revoked_user_public_keys: Vec::new(),
                    id: id.into(),
                    name: id.into(),
                    quotas: Default::default(),
                    created_at: 1,
                })
                .await
                .unwrap();
        }
        store
            .upsert_service(service_record("web", "tenant-a"))
            .await
            .unwrap();
        let err = store
            .upsert_service(service_record("web", "tenant-b"))
            .await
            .unwrap_err();
        assert!(matches!(err, minga_state::StateError::NameInUse), "{err:?}");
        let kept = store.get_service("web").await.unwrap().unwrap();
        assert_eq!(kept.tenant_id, "tenant-a");
        // The owner can still update its own service.
        store
            .upsert_service(service_record("web", "tenant-a"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tenant_crud_requires_admin_and_protects_owned_resources() {
        let (router, store, _dir) = admin_router().await;

        let req = Request::builder()
            .uri("/minga/tenants")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let req = Request::builder()
            .uri("/minga/tenants")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, tenants) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tenants.as_array().unwrap().len(), 1);
        assert_eq!(tenants[0]["id"], "default");

        let body = serde_json::json!({
            "id": "tenant-a",
            "name": "Hotel A",
            "quotas": {"services": 5}
        });
        let req = Request::builder()
            .method("POST")
            .uri("/minga/tenants")
            .header("Authorization", "Bearer admin-token")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, created) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        let created_at = created["created_at"].as_u64().unwrap();
        assert_eq!(created["quotas"]["services"], 5);

        let body = serde_json::json!({
            "name": "Hotel Alpha",
            "quotas": {"services": 10}
        });
        let req = Request::builder()
            .method("PUT")
            .uri("/minga/tenants/tenant-a")
            .header("Authorization", "Bearer admin-token")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let req = Request::builder()
            .uri("/minga/tenants/tenant-a")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, updated) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(updated["name"], "Hotel Alpha");
        assert_eq!(updated["quotas"]["services"], 10);
        assert_eq!(updated["created_at"], created_at);

        store
            .upsert_volume(Volume {
                id: "tenant-a-data".into(),
                tenant_id: "tenant-a".into(),
                name: "data".into(),
                driver: "local".into(),
            })
            .await
            .unwrap();
        let req = Request::builder()
            .method("DELETE")
            .uri("/minga/tenants/tenant-a")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CONFLICT);

        store
            .remove_volume("tenant-a-data".into(), "tenant-a")
            .await
            .unwrap();
        let req = Request::builder()
            .method("DELETE")
            .uri("/minga/tenants/tenant-a")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let req = Request::builder()
            .method("DELETE")
            .uri("/minga/tenants/default")
            .header("Authorization", "Bearer admin-token")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router, req).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn client_certificates_scope_docker_routes_to_their_tenant() {
        let (router, store, _dir) = admin_router().await;
        for id in ["tenant-a", "tenant-b"] {
            store
                .upsert_tenant(minga_state::types::Tenant {
                    nats_revoked_user_public_keys: Vec::new(),
                    id: id.into(),
                    name: id.into(),
                    quotas: Default::default(),
                    created_at: 1,
                })
                .await
                .unwrap();
        }
        for (id, tenant_id) in [
            ("tenant-a-service", "tenant-a"),
            ("tenant-b-service", "tenant-b"),
        ] {
            store
                .upsert_service(service_record(id, tenant_id))
                .await
                .unwrap();
        }
        for (id, tenant_id) in [(1, "tenant-a"), (2, "tenant-b")] {
            store.upsert_node(node_record(id, tenant_id)).await.unwrap();
        }

        let request = |uri: &str, tenant: Option<&str>| {
            let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            request.extensions_mut().insert(ClientIdentity {
                name: "client".into(),
                role: Some(ApiRole::Deployer),
                tenant: tenant.map(str::to_owned),
            });
            request
        };
        let (status, services) =
            send_request(router.clone(), request("/v1.41/services", Some("tenant-a"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(services.as_array().unwrap().len(), 1);
        assert_eq!(services[0]["ID"], "tenant-a-service");
        let (status, nodes) =
            send_request(router.clone(), request("/v1.41/nodes", Some("tenant-a"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(nodes.as_array().unwrap().len(), 1);
        assert_eq!(nodes[0]["ID"], "1");
        let (status, _) =
            send_request(router.clone(), request("/v1.41/nodes/2", Some("tenant-a"))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = send_request(
            router.clone(),
            request("/v1.41/services/tenant-b-service", Some("tenant-a")),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = send_request(router.clone(), request("/v1.41/services", None)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let mut operator = request("/minga/tenants", Some("*"));
        operator
            .extensions_mut()
            .get_mut::<ClientIdentity>()
            .unwrap()
            .role = Some(ApiRole::Admin);
        let (status, tenants) = send_request(router.clone(), operator).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tenants.as_array().unwrap().len(), 3);

        let mut operator = request("/v1.41/services", Some("*"));
        operator
            .extensions_mut()
            .get_mut::<ClientIdentity>()
            .unwrap()
            .role = Some(ApiRole::Admin);
        let (status, _) = send_request(router, operator).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn tenant_admin_reaches_audit_and_usage_routes() {
        let (router, store, _dir) = admin_router().await;
        store
            .upsert_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "tenant-a".into(),
                name: "tenant-a".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        for uri in ["/v1.41/audit", "/v1.41/agent/usage", "/v1.41/agent/events"] {
            let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            request.extensions_mut().insert(ClientIdentity {
                name: "tenant-admin".into(),
                role: Some(ApiRole::Admin),
                tenant: Some("tenant-a".into()),
            });
            let (status, body) = send_request(router.clone(), request).await;
            assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        }
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
        let (status, json) = send_operator_request(router, req).await;
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
        let (status, json) = send_operator_request(router, req).await;
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
    async fn task_logs_are_bounded_and_scoped_to_the_callers_tenant() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .upsert_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "tenant-b".into(),
                name: "tenant-b".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        for (service_id, task_id, tenant_id) in [
            ("service-a", "task-a", "default"),
            ("service-b", "task-b", "tenant-b"),
        ] {
            store
                .upsert_service(service_record(service_id, tenant_id))
                .await
                .unwrap();
            store
                .upsert_task(Task {
                    id: task_id.into(),
                    tenant_id: tenant_id.into(),
                    service_id: service_id.into(),
                    node_id: 1,
                    state: TaskState::Assigned,
                    spec: TaskSpec {
                        image: "nginx:latest".into(),
                        ..Default::default()
                    },
                    restarts: 0,
                    running_since_unix: None,
                    last_error: None,
                    last_exit_code: None,
                })
                .await
                .unwrap();
        }
        let bus = MemoryBus::new();
        bus.publish_tenant_node_log(
            "default",
            1,
            "task-a",
            &minga_bus::LogEntry::stdout("task-a", b"tenant-a-log".to_vec()),
        )
        .await
        .unwrap();
        bus.publish_tenant_node_log(
            "tenant-b",
            1,
            "task-b",
            &minga_bus::LogEntry::stdout("task-b", b"tenant-b-secret".to_vec()),
        )
        .await
        .unwrap();
        let router = build_router_with_bus_and_auth(store, bus, Arc::new(ReadyState::new()), None);
        let request = |uri: &str| {
            let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            request.extensions_mut().insert(ClientIdentity {
                name: "tenant-viewer".into(),
                role: Some(ApiRole::Viewer),
                tenant: Some("default".into()),
            });
            request
        };

        let (status, logs) =
            send_request(router.clone(), request("/v1.41/tasks/task-a/logs?tail=0")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(logs.as_array().unwrap().len(), 1);
        assert_eq!(logs[0]["data"], serde_json::json!(b"tenant-a-log".to_vec()));
        assert_ne!(
            logs[0]["data"],
            serde_json::json!(b"tenant-b-secret".to_vec())
        );

        let (status, _) = send_request(router, request("/v1.41/tasks/task-b/logs")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn the_change_diff_hides_environment_values_but_names_their_keys() {
        let mut before = service_record("svc-env", "default");
        before.env = [("KEEP", "same"), ("TOKEN", "old-value"), ("GONE", "x")]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        let mut after = before.clone();
        after.env.insert("TOKEN".into(), "new-value".into());
        after.env.remove("GONE");
        after.env.insert("ADDED".into(), "y".into());
        after.replicas += 1;
        let view = serde_json::to_value(crate::handlers::ChangeRequestView::from(
            minga_state::types::ChangeRequest {
                id: "cr-env".into(),
                tenant_id: "default".into(),
                service_id: "svc-env".into(),
                requester: "agent".into(),
                created_at_unix: 1,
                expires_at_unix: 2,
                kind: minga_state::types::ChangeKind::Rollback,
                before,
                after,
                status: minga_state::types::ChangeRequestStatus::Pending,
                reviewer: None,
                decided_at_unix: None,
                audit: Vec::new(),
            },
        ))
        .unwrap();
        let text = view.to_string();
        for secret in ["old-value", "new-value", "\"x\"", "\"y\""] {
            assert!(!text.contains(secret), "{secret} leaked: {text}");
        }
        assert_eq!(
            view["hidden_changes"],
            serde_json::json!(["env.ADDED", "env.GONE", "env.TOKEN"])
        );
        let fields: Vec<_> = view["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|change| change["field"].as_str().unwrap())
            .collect();
        assert_eq!(fields, ["env", "replicas"]);
        assert_eq!(view["changes"][1]["after"], 2);
    }

    #[test]
    fn deciding_a_change_request_needs_a_reviewer_certificate() {
        let caller = Caller {
            name: "token".into(),
            role: ApiRole::Admin,
            tenant_id: Some("default".into()),
            cluster_operator: true,
        };
        let post = axum::http::Method::POST;
        for path in [
            "/minga/change-requests/cr-1/approve",
            "/minga/change-requests/cr-1/reject",
        ] {
            assert!(authorize(&caller, Source::Token, &post, path).is_err());
            assert!(authorize(&caller, Source::Legacy, &post, path).is_err());
            assert!(authorize(&caller, Source::Certificate, &post, path).is_ok());
        }
        // Listing still works with a token in a sole-tenant cluster.
        let get = axum::http::Method::GET;
        assert!(authorize(&caller, Source::Token, &get, "/minga/change-requests").is_ok());
    }

    #[tokio::test]
    async fn change_requests_are_tenant_scoped_and_maker_checker_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .upsert_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "tenant-b".into(),
                name: "tenant-b".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        store
            .upsert_service(service_record("svc-a", "default"))
            .await
            .unwrap();
        store
            .upsert_service(service_record("svc-b", "tenant-b"))
            .await
            .unwrap();
        // Approval runs placement admission, so the tenant needs a node.
        store.upsert_node(node_record(1, "default")).await.unwrap();

        let bus = MemoryBus::new();
        let router = build_router_with_bus_and_auth(
            store.clone(),
            bus.clone(),
            Arc::new(ReadyState::new()),
            None,
        );
        let request = |method: &str,
                       uri: &str,
                       role: ApiRole,
                       tenant: &str,
                       actor: &str,
                       body: Option<serde_json::Value>| {
            let mut builder = Request::builder().method(method).uri(uri);
            if body.is_some() {
                builder = builder.header("content-type", "application/json");
            }
            let mut request = builder
                .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                .unwrap();
            request.extensions_mut().insert(ClientIdentity {
                name: actor.into(),
                role: Some(role),
                tenant: Some(tenant.into()),
            });
            request
        };

        let (status, _) = send_request(
            router.clone(),
            request(
                "POST",
                "/minga/change-requests",
                ApiRole::Deployer,
                "default",
                "deployer",
                Some(serde_json::json!({
                    "action": "scale",
                    "service_id": "svc-a",
                    "replicas": 2
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        for image in ["web:\u{202e}2", "web:\u{200b}2", "web:é", "web:\u{e0001}2"] {
            let (status, body) = send_request(
                router.clone(),
                request(
                    "POST",
                    "/minga/change-requests",
                    ApiRole::Agent,
                    "default",
                    "agent",
                    Some(serde_json::json!({
                        "action":"deploy", "service_id":"svc-a", "image":image
                    })),
                ),
            )
            .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "accepted {image:?}: {body}"
            );
        }

        // A global service ignores `replicas`: scale and stop are refused.
        let mut global = service_record("svc-global", "default");
        global.mode = minga_state::types::ServiceMode::Global;
        store.upsert_service(global).await.unwrap();
        for body in [
            serde_json::json!({"action": "scale", "service_id": "svc-global", "replicas": 2}),
            serde_json::json!({"action": "stop", "service_id": "svc-global"}),
        ] {
            let (status, _) = send_request(
                router.clone(),
                request(
                    "POST",
                    "/minga/change-requests",
                    ApiRole::Agent,
                    "default",
                    "agent",
                    Some(body),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }

        // A rollback that only changes ports still shows the reviewer that it does.
        let mut rolled = service_record("svc-rollback", "default");
        let mut earlier = rolled.clone();
        earlier.ports = vec![minga_state::types::PortMapping {
            host_ip: None,
            target: 80,
            published: Some(18080),
            protocol: "tcp".into(),
            mode: minga_state::types::PortMode::Ingress,
        }];
        rolled.previous = Some(Box::new(earlier));
        store.upsert_service(rolled).await.unwrap();
        let (status, rollback) = send_request(
            router.clone(),
            request(
                "POST",
                "/minga/change-requests",
                ApiRole::Agent,
                "default",
                "agent",
                Some(serde_json::json!({"action": "rollback", "service_id": "svc-rollback"})),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(rollback["before"]["image"], rollback["after"]["image"]);
        assert_eq!(rollback["before"]["ports"], serde_json::json!([]));
        assert_eq!(rollback["after"]["ports"][0]["published"], 18080);
        assert_eq!(rollback["changed_fields"], serde_json::json!(["ports"]));
        assert!(rollback["after"].get("env").is_none());
        assert_eq!(rollback["changes"][0]["field"], "ports");
        assert_eq!(rollback["changes"][0]["before"], serde_json::json!([]));
        assert_eq!(rollback["changes"][0]["after"][0]["published"], 18080);

        let (status, proposal) = send_request(
            router.clone(),
            request(
                "POST",
                "/minga/change-requests",
                ApiRole::Agent,
                "default",
                "agent",
                Some(serde_json::json!({
                    "action": "scale",
                    "service_id": "svc-a",
                    "replicas": 2
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let id = proposal["id"].as_str().unwrap().to_owned();
        for (tenant, expected) in [("default", 1), ("tenant-b", 0)] {
            let (status, filtered) = send_request(
                router.clone(),
                request(
                    "GET",
                    &format!("/minga/change-requests?id={id}&limit=1"),
                    ApiRole::Viewer,
                    tenant,
                    "viewer",
                    None,
                ),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(filtered.as_array().unwrap().len(), expected);
            if expected == 1 {
                assert_eq!(filtered[0]["id"], id);
            }
        }

        let (status, default_list) = send_request(
            router.clone(),
            request(
                "GET",
                "/minga/change-requests",
                ApiRole::Viewer,
                "default",
                "viewer-a",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(default_list.as_array().unwrap().len(), 2);

        let (status, _) = send_request(
            router.clone(),
            request(
                "POST",
                "/minga/change-requests",
                ApiRole::Agent,
                "tenant-b",
                "agent-b",
                Some(serde_json::json!({
                    "action": "scale",
                    "service_id": "svc-a",
                    "replicas": 2
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, tenant_b_list) = send_request(
            router.clone(),
            request(
                "GET",
                "/minga/change-requests",
                ApiRole::Viewer,
                "tenant-b",
                "viewer-b",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(tenant_b_list.as_array().unwrap().is_empty());

        let (status, _) = send_request(
            router.clone(),
            request(
                "POST",
                &format!("/minga/change-requests/{id}/approve"),
                ApiRole::Deployer,
                "tenant-b",
                "deployer-b",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = send_request(
            router.clone(),
            request(
                "POST",
                &format!("/minga/change-requests/{id}/approve"),
                ApiRole::Deployer,
                "default",
                "agent",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            store.get_service("svc-a").await.unwrap().unwrap().replicas,
            1
        );

        let (status, decision) = send_request(
            router.clone(),
            request(
                "POST",
                &format!("/minga/change-requests/{id}/approve"),
                ApiRole::Deployer,
                "default",
                "deployer",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(decision["status"], "approved");
        assert_eq!(
            store.get_service("svc-a").await.unwrap().unwrap().replicas,
            2
        );
        let (status, rejected_request) = send_request(
            router.clone(),
            request(
                "POST",
                "/minga/change-requests",
                ApiRole::Agent,
                "default",
                "agent-2",
                Some(serde_json::json!({
                    "action": "scale",
                    "service_id": "svc-a",
                    "replicas": 3
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let rejected_id = rejected_request["id"].as_str().unwrap();
        let (status, rejected) = send_request(
            router.clone(),
            request(
                "POST",
                &format!("/minga/change-requests/{rejected_id}/reject"),
                ApiRole::Deployer,
                "default",
                "deployer-2",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(rejected["status"], "rejected");

        // The status filter runs before the limit: the one pending request
        // (the rollback) is found even behind newer decided ones.
        let (status, pending) = send_request(
            router.clone(),
            request(
                "GET",
                "/minga/change-requests?status=pending&limit=1",
                ApiRole::Viewer,
                "default",
                "viewer-a",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let pending = pending.as_array().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0]["service_id"], "svc-rollback");

        let expired_before = store.get_service("svc-a").await.unwrap().unwrap();
        let mut expired_after = expired_before.clone();
        expired_after.replicas += 1;
        store
            .create_change_request(minga_state::types::ChangeRequest {
                id: "cr-expired-api-test".into(),
                tenant_id: "default".into(),
                service_id: "svc-a".into(),
                requester: "expired-agent".into(),
                created_at_unix: 1,
                expires_at_unix: 2,
                kind: minga_state::types::ChangeKind::Scale,
                before: expired_before,
                after: expired_after,
                status: minga_state::types::ChangeRequestStatus::Pending,
                reviewer: None,
                decided_at_unix: None,
                audit: Vec::new(),
            })
            .await
            .unwrap();
        let (status, _) = send_request(
            router,
            request(
                "POST",
                "/minga/change-requests/cr-expired-api-test/approve",
                ApiRole::Deployer,
                "default",
                "deployer-3",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let expired = store
            .get_change_request("default", "cr-expired-api-test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            expired.status,
            minga_state::types::ChangeRequestStatus::Expired
        );
        assert_eq!(
            store.get_service("svc-a").await.unwrap().unwrap().replicas,
            2
        );

        let audit = bus.list_audit("default", 20).await.unwrap();
        assert!(audit
            .iter()
            .any(|record| record.action == "change_request.rejected"));
        assert!(audit
            .iter()
            .any(|record| record.action == "change_request.expired"));

        assert!(audit
            .iter()
            .any(|record| record.action == "change_request.created"));
        assert!(audit
            .iter()
            .any(|record| record.action == "change_request.approved_and_applied"));
    }

    #[tokio::test]
    async fn task_view_uses_bus_actual_state_and_keeps_traefik_attachments() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .upsert_service(service_record("svc", "default"))
            .await
            .unwrap();
        let mut task = Task {
            id: "svc.1".into(),
            tenant_id: "default".to_string(),
            service_id: "svc".into(),
            node_id: 1,
            state: TaskState::Assigned,
            spec: TaskSpec {
                image: "nginx:latest".into(),
                ..Default::default()
            },
            restarts: 99,
            running_since_unix: Some(1),
            last_error: Some("stale Raft error".into()),
            last_exit_code: Some(99),
        };
        task.spec.ips.insert("front".into(), "10.20.0.7".into());
        store.upsert_task(task).await.unwrap();
        store
            .upsert_network(Network {
                id: "front".into(),
                tenant_id: "default".to_string(),
                name: "front".into(),
                driver: "overlay".into(),
                attachable: true,
                subnet: Some("10.20.0.0/24".into()),
                wg_port: None,
            })
            .await
            .unwrap();

        let bus = MemoryBus::new();
        let report = TaskStatusReport {
            task_id: "svc.1".into(),
            node_id: 1,
            state: TaskActualState::Running,
            container_id: Some("container-1".into()),
            observed_at: 1_000,
            restarts: 2,
            last_error: None,
            last_exit_code: None,
            running_since_unix: Some(999),
        };
        bus.put_tenant_status(
            "default",
            &subjects::kv_node_task_status(report.node_id, "svc.1"),
            &report.to_bytes().unwrap(),
        )
        .await
        .unwrap();
        let router = build_router_with_bus_and_auth(store, bus, Arc::new(ReadyState::new()), None);
        let req = Request::builder()
            .uri("/v1.41/tasks/svc.1")
            .body(Body::empty())
            .unwrap();

        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["Status"]["State"], "running");
        assert_eq!(json["Status"]["Message"], "started");
        assert_eq!(json["DesiredState"], "running");
        assert_eq!(json["MingaRestarts"], 2);
        assert_eq!(
            json["NetworksAttachments"][0]["Addresses"][0],
            "10.20.0.7/24"
        );
    }

    #[tokio::test]
    async fn missing_status_does_not_replay_raft_actual_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .upsert_service(service_record("svc", "default"))
            .await
            .unwrap();
        store
            .upsert_task(Task {
                id: "svc.1".into(),
                tenant_id: "default".to_string(),
                service_id: "svc".into(),
                node_id: 1,
                state: TaskState::Assigned,
                spec: TaskSpec::default(),
                restarts: 9,
                running_since_unix: Some(1),
                last_error: Some("stale Raft error".into()),
                last_exit_code: Some(9),
            })
            .await
            .unwrap();
        let router = build_router_with_bus_and_auth(
            store,
            MemoryBus::new(),
            Arc::new(ReadyState::new()),
            None,
        );
        let req = Request::builder()
            .uri("/v1.41/tasks/svc.1")
            .body(Body::empty())
            .unwrap();

        let (status, json) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["Status"]["State"], "preparing");
        assert_eq!(json["Status"]["Err"], serde_json::Value::Null);
        assert_eq!(json["Status"]["ContainerStatus"], serde_json::Value::Null);
        assert_eq!(json["DesiredState"], "running");
        assert_eq!(json["MingaRestarts"], 0);
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
        let (status, json) = send_operator_request(router.clone(), req).await;
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
        let (status, json) = send_operator_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["Status"]["State"], "ready");
        assert_eq!(json["Status"]["Addr"], "10.0.0.2:50051");
    }

    #[tokio::test]
    async fn create_node_refuses_an_id_with_no_mesh_address() {
        let (router, _dir) = test_router().await;
        for id in [0u64, 1 << 40] {
            let req = Request::builder()
                .method("POST")
                .uri("/v1.41/nodes")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({"id": id, "address": "10.0.0.9:50051"}))
                        .unwrap(),
                ))
                .unwrap();
            let (status, json) = send_operator_request(router.clone(), req).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "id {id}: {json}");
        }
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
        let (status, _) = send_operator_request(router.clone(), req).await;
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
        let (status, _) = send_operator_request(router.clone(), req).await;
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
        let (status, _) = send_operator_request(router, req).await;
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
        let (status, _) = send_operator_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);

        let req = Request::builder()
            .uri("/v1.41/nodes/1")
            .body(Body::empty())
            .unwrap();
        let (status, json) = send_operator_request(router, req).await;
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
        let (status, json) = send_operator_request(router.clone(), req).await;
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
        let (status, json) = send_operator_request(router.clone(), req).await;
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
        let (status, _) = send_operator_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Unknown node.
        let body = serde_json::json!({"Role": "worker", "Availability": "active"});
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/nodes/999/update")
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, _) = send_operator_request(router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn tokenless_api_refuses_browser_shaped_requests() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        let router = build_router_with_auth(store, Arc::new(ReadyState::new()), None);
        let status = |host: &str, origin: Option<&str>| {
            let mut req = Request::builder()
                .uri("/v1.41/version")
                .header("Host", host);
            if let Some(origin) = origin {
                req = req.header("Origin", origin);
            }
            let router = router.clone();
            let req = req.body(Body::empty()).unwrap();
            async move { send_request(router, req).await.0 }
        };

        // Docker and minga clients: an IP or localhost, no Origin.
        for host in [
            "127.0.0.1:2375",
            "[::1]:2375",
            "localhost:2375",
            "10.0.0.5:2375",
            "api.moby.localhost",
        ] {
            assert_eq!(status(host, None).await, StatusCode::OK, "{host}");
        }
        // DNS rebinding: the page's own hostname, or any request with an Origin.
        assert_eq!(
            status("evil.example:2375", None).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status("127.0.0.1:2375", Some("http://evil.example:2375")).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn empty_configured_token_never_authenticates() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        let router =
            build_router_with_auth(store, Arc::new(ReadyState::new()), Some(String::new()));
        // "Og==" is base64(":"), so Basic auth presents an empty password.
        for header in ["Basic Og==", "Bearer "] {
            let req = Request::builder()
                .uri("/minga/tenants")
                .header("Authorization", header)
                .body(Body::empty())
                .unwrap();
            let (status, _) = send_request(router.clone(), req).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{header}");
        }
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
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["ApiVersion"], "1.41");

        // Basic auth, token as the password (Traefik's Swarm provider):
        // "traefik:s3cret-token" → 200, "traefik:wrong" → 401.
        for (credentials, expected) in [
            ("dHJhZWZpazpzM2NyZXQtdG9rZW4=", StatusCode::OK),
            ("dHJhZWZpazp3cm9uZw==", StatusCode::UNAUTHORIZED),
        ] {
            let req = Request::builder()
                .uri("/v1.41/version")
                .header("Authorization", format!("Basic {credentials}"))
                .body(Body::empty())
                .unwrap();
            let (status, _) = send_request(router.clone(), req).await;
            assert_eq!(status, expected, "Basic {credentials}");
        }
    }
    #[tokio::test]
    async fn manager_membership_routes_require_admin_auth_and_change_membership() {
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
        let store2 = Arc::new(StateStore::open_join(2, &addr2, dir2.path()).await.unwrap());
        let router = build_router_with_bus_and_auth(
            store1.clone(),
            MemoryBus::new(),
            Arc::new(ReadyState::new()),
            Some("manager-secret".into()),
        );

        let req = Request::builder()
            .uri("/v1.41/minga/cluster")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let req = Request::builder()
            .uri("/minga/managers")
            .body(Body::from(r#"{"id":2,"raft_addr":"127.0.0.1:1"}"#))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let req = Request::builder()
            .method("POST")
            .uri("/minga/managers")
            .header("Authorization", "Bearer manager-secret")
            .header("Content-Type", "application/json")
            .body(Body::from(format!(r#"{{"id":2,"raft_addr":"{addr2}"}}"#)))
            .unwrap();
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["id"], 2);

        let req = Request::builder()
            .uri("/v1.41/minga/cluster")
            .header("Authorization", "Bearer manager-secret")
            .body(Body::empty())
            .unwrap();
        let (status, diagnostics) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(diagnostics["server_version"], crate::models::MINGA_VERSION);
        assert_eq!(diagnostics["api_version"], crate::models::API_VERSION);
        assert_eq!(
            diagnostics["state_format_version"],
            minga_state::codec::FORMAT_VERSION
        );
        assert!(diagnostics["build_revision"]
            .as_str()
            .is_some_and(|revision| !revision.is_empty()));
        assert_eq!(diagnostics["raft"]["node_id"], 1);
        assert!(diagnostics["raft"]["ready"].as_bool().unwrap());
        assert_eq!(diagnostics["raft"]["leader_id"], 1);
        assert_eq!(diagnostics["raft"]["members"].as_array().unwrap().len(), 2);
        assert_eq!(diagnostics["raft"]["members"][1]["id"], 2);
        assert_eq!(diagnostics["raft"]["members"][1]["address"], addr2.as_str());
        assert!(diagnostics["raft"]["members"][1]["voter"]
            .as_bool()
            .unwrap());

        let req = Request::builder()
            .method("DELETE")
            .uri("/minga/managers/2")
            .header("Authorization", "Bearer manager-secret")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let req = Request::builder()
            .method("POST")
            .uri("/minga/managers/migrate-v1")
            .header("Authorization", "Bearer manager-secret")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"manager_ids":[1]}"#))
            .unwrap();
        let (status, body) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["removed_voters"], serde_json::json!([]));

        let req = Request::builder()
            .method("DELETE")
            .uri("/minga/managers/1")
            .header("Authorization", "Bearer manager-secret")
            .body(Body::empty())
            .unwrap();
        let (status, body) = send_request(router, req).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["message"].as_str().unwrap().contains("last manager"));

        Arc::try_unwrap(store1)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        Arc::try_unwrap(store2)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn diagnostics_report_persisted_voters_and_unreachable_member() {
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
        let store2 = Arc::new(StateStore::open_join(2, &addr2, dir2.path()).await.unwrap());
        let router = build_router_with_bus_and_auth(
            store1.clone(),
            MemoryBus::new(),
            Arc::new(ReadyState::new()),
            Some("manager-secret".into()),
        );

        let req = Request::builder()
            .method("POST")
            .uri("/minga/managers")
            .header("Authorization", "Bearer manager-secret")
            .header("Content-Type", "application/json")
            .body(Body::from(format!(r#"{{"id":2,"raft_addr":"{addr2}"}}"#)))
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(store1.manager_voters().await, vec![1, 2]);

        Arc::try_unwrap(store2)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = store1.raft_diagnostics().await;
                let missing_voter = status.members.iter().any(|member| {
                    member.id == 2 && member.voter && member.transport_reachable == Some(false)
                });
                if missing_voter && status.quorum_ack_recent == Some(false) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("diagnostics should expose a missing voter and stale quorum acknowledgment");

        let req = Request::builder()
            .uri("/v1.41/minga/cluster")
            .header("Authorization", "Bearer manager-secret")
            .body(Body::empty())
            .unwrap();
        let (status, diagnostics) = send_request(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!diagnostics["raft"]["ready"].as_bool().unwrap());
        assert_eq!(diagnostics["raft"]["quorum_ack_recent"], false);
        assert_eq!(diagnostics["raft"]["members"].as_array().unwrap().len(), 2);
        assert_eq!(diagnostics["raft"]["members"][1]["id"], 2);
        assert!(!diagnostics["raft"]["members"][1]["transport_reachable"]
            .as_bool()
            .unwrap());

        Arc::try_unwrap(store1)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
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
        assert_eq!(
            json["message"],
            "service cannot be placed with current cluster capacity or constraints"
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

    #[tokio::test]
    async fn state_import_round_trips_v9_agents_and_runs_agent_admission() {
        let (router, store, _dir) = admin_router().await;
        let mut service = service_record("stack_agent", "default");
        service.agent = Some(Box::new(
            serde_json::from_str(
                r#"{"module_ref":"a.wasm","sandbox_env":{"K":"v"},"capabilities":{"grants":["env"]}}"#,
            )
            .unwrap(),
        ));
        store.upsert_service(service).await.unwrap();

        let export = |router: Router| async move {
            let req = Request::builder()
                .uri("/v1.41/state")
                .header("Authorization", "Bearer admin-token")
                .body(Body::empty())
                .unwrap();
            let (status, json) = send_request(router, req).await;
            assert_eq!(status, StatusCode::OK, "{json}");
            json
        };
        let import = |router: Router, body: serde_json::Value| async move {
            let req = Request::builder()
                .method("POST")
                .uri("/v1.41/state?overwrite=true")
                .header("Authorization", "Bearer admin-token")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap();
            send_request(router, req).await
        };

        // A genuine v9 export always carries `capabilities` and round-trips.
        let exported = export(router.clone()).await;
        let agent = &exported["services"][0]["agent"];
        assert_eq!(agent["capabilities"]["grants"], serde_json::json!(["env"]));
        let (status, json) = import(router.clone(), exported.clone()).await;
        assert!(status.is_success(), "{status}: {json}");
        assert_eq!(
            export(router.clone()).await["services"],
            exported["services"]
        );

        // In v9, an omitted or null capabilities means no grants, so the
        // env map is refused as at deploy instead of getting an implicit env.
        for capabilities in [None, Some(serde_json::Value::Null)] {
            let mut body = exported.clone();
            let agent = body["services"][0]["agent"].as_object_mut().unwrap();
            match capabilities {
                None => agent.remove("capabilities"),
                Some(value) => agent.insert("capabilities".into(), value),
            };
            let (status, json) = import(router.clone(), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
            assert!(json["message"].as_str().unwrap().contains("env grant"));
        }

        // Explicit capabilities get the same admission checks as a deploy.
        for (capabilities, expected) in [
            (serde_json::json!({"grants": []}), "env grant"),
            (
                serde_json::json!({"grants": ["env"], "dirs": [{"host": "srv", "guest": "/d"}]}),
                "must be absolute",
            ),
        ] {
            let mut body = exported.clone();
            body["services"][0]["agent"]["capabilities"] = capabilities;
            let (status, json) = import(router.clone(), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
            assert!(
                json["message"].as_str().unwrap().contains(expected),
                "{json}"
            );
        }

        // Serde decodes arrays by position, so non-object services and
        // agents are refused before the omitted-capabilities rule runs. This
        // array would decode with `sandbox_env` set and capabilities omitted.
        let positional: serde_json::Value =
            serde_json::from_str(r#"["a.wasm",null,null,null,null,null,null,{"K":"v"},null]"#)
                .unwrap();
        let mut agent_array = exported.clone();
        agent_array["services"][0]["agent"] = positional;
        let mut previous_array = exported.clone();
        previous_array["services"][0]["previous"] = serde_json::json!([]);
        let mut service_array = exported.clone();
        service_array["services"][0] = serde_json::json!([]);
        for body in [agent_array, previous_array, service_array] {
            let (status, json) = import(router.clone(), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
            assert!(
                json["message"]
                    .as_str()
                    .unwrap()
                    .contains("must be a JSON object"),
                "{json}"
            );
        }
    }

    #[tokio::test]
    async fn state_routes_need_the_token_and_check_version_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        let router = build_router_with_bus_and_auth(
            store,
            MemoryBus::new(),
            Arc::new(ReadyState::new()),
            Some("s3cret".to_string()),
        );

        let req = Request::builder()
            .uri("/v1.41/state")
            .body(Body::empty())
            .unwrap();
        let (status, _) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // A newer file whose shape we cannot parse is refused on its version.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/state")
            .header("Authorization", "Bearer s3cret")
            .header("Content-Type", "application/json")
            .body(Body::from(
                r#"{"format_version": 99, "services": "new shape"}"#,
            ))
            .unwrap();
        let (status, json) = send_request(router.clone(), req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(json["message"]
            .as_str()
            .unwrap()
            .contains("format version 99"));

        // An older version the store migrates is accepted.
        let req = Request::builder()
            .method("POST")
            .uri("/v1.41/state")
            .header("Authorization", "Bearer s3cret")
            .header("Content-Type", "application/json")
            .body(Body::from(
                r#"{"format_version": 8, "exported_at_unix": 0, "tenants": [], "services": [],
                    "networks": [], "volumes": [], "secrets": [], "configs": []}"#,
            ))
            .unwrap();
        let (status, json) = send_request(router, req).await;
        assert!(status.is_success(), "{status}: {json}");
    }
}
