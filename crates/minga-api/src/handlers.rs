//! Axum route handlers for the Docker Engine API subset.
//!
//! Each handler is an async function that extracts path/query/body
//! parameters, calls into the [`StateStore`](minga_state::StateStore),
//! and returns a Docker-compatible JSON response.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use minga_state::StateStore;

use crate::{
    error::{conflict, not_found, ok_message, ApiError, ApiResult},
    models::{self, Filters},
    translate,
};

/// Shared state for all handlers.
pub struct AppState {
    pub store: Arc<StateStore>,
}

impl AppState {
    pub fn new(store: Arc<StateStore>) -> Self {
        Self { store }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts label filters from a filters map.
fn label_filters(filters: &Filters) -> Vec<String> {
    filters.get("label").cloned().unwrap_or_default()
}

/// Checks if a name matches any of the name filters.
fn name_matches(name: &str, filters: &[String]) -> bool {
    filters.is_empty() || filters.iter().any(|f| f == name)
}

// ---------------------------------------------------------------------------
// Ping / Version / Info
// ---------------------------------------------------------------------------

/// `GET /_ping` — health check. Returns `OK` with `text/plain`.
pub async fn ping() -> &'static str {
    "OK"
}

/// `GET /v{version}/version` — version info.
pub async fn get_version() -> Json<models::VersionInfo> {
    Json(translate::version_info())
}

/// `GET /v{version}/info` — system info.
pub async fn get_info(State(app): State<Arc<AppState>>) -> ApiResult<Json<models::SystemInfo>> {
    let node_id = app.store.node_id();
    let is_leader = app.store.is_leader().await;

    let nodes = app.store.list_nodes().await?;
    let services = app.store.list_services().await?;
    let tasks = app.store.list_tasks().await?;

    let running = tasks
        .iter()
        .filter(|t| t.state == minga_state::types::TaskState::Running)
        .count();

    let manager_count = if is_leader { 1 } else { 0 };

    let addr = nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.address.clone())
        .unwrap_or_default();

    let stats = translate::ClusterStats {
        node_id,
        node_addr: addr,
        is_leader,
        total_nodes: nodes.len(),
        manager_count,
        running_tasks: running,
        total_tasks: tasks.len(),
        services_count: services.len(),
    };

    Ok(Json(translate::system_info(&stats)))
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// `GET /v{version}/nodes` — list all nodes.
pub async fn list_nodes(
    State(app): State<Arc<AppState>>,
    Query(query): Query<models::NodeListQuery>,
) -> ApiResult<Json<Vec<models::Node>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let label_filters = label_filters(&filters);
    let my_id = app.store.node_id();
    let is_leader = app.store.is_leader().await;
    let leader_id = app.store.current_leader().await;

    let nodes = app.store.list_nodes().await?;
    let result: Vec<models::Node> = nodes
        .iter()
        .filter(|n| models::labels_match(&n.labels, &label_filters))
        .map(|n| {
            translate::node_to_docker(n, Some(n.id) == leader_id || (n.id == my_id && is_leader))
        })
        .collect();

    Ok(Json(result))
}

/// `GET /v{version}/nodes/{id}` — inspect a node.
pub async fn get_node(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Node>> {
    let nodes = app.store.list_nodes().await?;
    let leader_id = app.store.current_leader().await;

    let node = nodes
        .iter()
        .find(|n| n.id.to_string() == id || n.address == id)
        .ok_or_else(|| not_found("node", &id))?;

    let is_leader = Some(node.id) == leader_id;
    Ok(Json(translate::node_to_docker(node, is_leader)))
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

/// `GET /v{version}/services` — list services.
pub async fn list_services(
    State(app): State<Arc<AppState>>,
    Query(query): Query<models::ServiceListQuery>,
) -> ApiResult<Json<Vec<models::Service>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let label_filters = label_filters(&filters);
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();

    let services = app.store.list_services().await?;
    let tasks = app.store.list_tasks().await?;

    let result: Vec<models::Service> = services
        .iter()
        .filter(|s| {
            // Label filter: check MINGA_STACK env as a proxy for labels.
            let labels_ok = if label_filters.is_empty() {
                true
            } else {
                let mut pseudo_labels = std::collections::HashMap::new();
                if let Some(stack) = s.env.get("MINGA_STACK") {
                    pseudo_labels.insert("com.docker.stack.namespace".to_string(), stack.clone());
                }
                models::labels_match(&pseudo_labels, &label_filters)
            };
            let name_ok = name_matches(&s.name, &name_filters);
            let id_ok =
                id_filters.is_empty() || id_filters.iter().any(|f| f == &s.id || f == &s.name);
            labels_ok && name_ok && id_ok
        })
        .map(|s| {
            let mut docker_svc = translate::service_to_docker(s);
            // Attach tasks for this service.
            docker_svc.TaskList = tasks
                .iter()
                .filter(|t| t.service_id == s.id)
                .map(translate::task_to_docker)
                .collect();
            docker_svc
        })
        .collect();

    Ok(Json(result))
}

/// `POST /v{version}/services/create` — create a service.
pub async fn create_service(
    State(app): State<Arc<AppState>>,
    Json(spec): Json<models::ServiceSpec>,
) -> ApiResult<(StatusCode, Json<models::ServiceCreateResponse>)> {
    let svc = translate::service_spec_to_state(&spec).map_err(ApiError::BadRequest)?;

    // Check for existing service with the same name.
    if let Some(existing) = app.store.get_service(&svc.id).await? {
        return Err(conflict(format!(
            "service {} already exists",
            existing.name
        )));
    }

    app.store.upsert_service(svc.clone()).await?;

    tracing::info!(service = %svc.id, image = %svc.image, "service created via API");

    Ok((
        StatusCode::CREATED,
        Json(models::ServiceCreateResponse {
            ID: svc.id,
            Warning: None,
        }),
    ))
}

/// `GET /v{version}/services/{id}` — inspect a service.
pub async fn get_service(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Service>> {
    let services = app.store.list_services().await?;
    let tasks = app.store.list_tasks().await?;

    let svc = services
        .iter()
        .find(|s| s.id == id || s.name == id)
        .ok_or_else(|| not_found("service", &id))?;

    let mut docker_svc = translate::service_to_docker(svc);
    docker_svc.TaskList = tasks
        .iter()
        .filter(|t| t.service_id == svc.id)
        .map(translate::task_to_docker)
        .collect();

    Ok(Json(docker_svc))
}

/// `DELETE /v{version}/services/{id}` — remove a service.
pub async fn delete_service(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let services = app.store.list_services().await?;
    let svc = services
        .iter()
        .find(|s| s.id == id || s.name == id)
        .ok_or_else(|| not_found("service", &id))?;

    // Remove all tasks for this service first.
    let tasks = app.store.list_tasks().await?;
    for task in tasks.iter().filter(|t| t.service_id == svc.id) {
        app.store.remove_task(task.id.clone()).await?;
    }

    app.store.remove_service(svc.id.clone()).await?;

    tracing::info!(service = %svc.id, "service removed via API");

    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v{version}/services/{id}/update` — update a service.
pub async fn update_service(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
    Query(_query): Query<models::ServiceUpdateQuery>,
    Json(spec): Json<models::ServiceSpec>,
) -> ApiResult<Json<serde_json::Value>> {
    let services = app.store.list_services().await?;
    let existing = services
        .iter()
        .find(|s| s.id == id || s.name == id)
        .ok_or_else(|| not_found("service", &id))?;

    // Translate the new spec, preserving the ID.
    let mut svc = translate::service_spec_to_state(&spec).map_err(ApiError::BadRequest)?;
    svc.id = existing.id.clone();
    svc.name = existing.name.clone();

    app.store.upsert_service(svc.clone()).await?;

    tracing::info!(service = %svc.id, "service updated via API");

    Ok(ok_message(""))
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

/// `GET /v{version}/tasks` — list tasks.
pub async fn list_tasks(
    State(app): State<Arc<AppState>>,
    Query(query): Query<models::TaskListQuery>,
) -> ApiResult<Json<Vec<models::Task>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let label_filters = label_filters(&filters);
    let service_filters = filters.get("service").cloned().unwrap_or_default();
    let node_filters = filters.get("node").cloned().unwrap_or_default();
    let desired_state_filters = filters.get("desired-state").cloned().unwrap_or_default();

    let tasks = app.store.list_tasks().await?;
    let services = app.store.list_services().await?;

    let result: Vec<models::Task> = tasks
        .iter()
        .filter(|t| {
            // Service filter.
            if !service_filters.is_empty()
                && !service_filters.iter().any(|f| {
                    f == &t.service_id
                        || services
                            .iter()
                            .find(|s| s.id == t.service_id)
                            .map(|s| &s.name == f)
                            .unwrap_or(false)
                })
            {
                return false;
            }
            // Node filter.
            if !node_filters.is_empty() && !node_filters.iter().any(|f| f == &t.node_id.to_string())
            {
                return false;
            }
            // Desired-state filter.
            if !desired_state_filters.is_empty() {
                let desired = match t.state {
                    minga_state::types::TaskState::Stopping
                    | minga_state::types::TaskState::Stopped
                    | minga_state::types::TaskState::Failed => "shutdown",
                    _ => "running",
                };
                if !desired_state_filters.iter().any(|f| f == desired) {
                    return false;
                }
            }
            // Label filter (stack namespace).
            if !label_filters.is_empty() {
                if let Some(svc) = services.iter().find(|s| s.id == t.service_id) {
                    let mut pseudo_labels = std::collections::HashMap::new();
                    if let Some(stack) = svc.env.get("MINGA_STACK") {
                        pseudo_labels
                            .insert("com.docker.stack.namespace".to_string(), stack.clone());
                    }
                    if !models::labels_match(&pseudo_labels, &label_filters) {
                        return false;
                    }
                }
            }
            true
        })
        .map(translate::task_to_docker)
        .collect();

    Ok(Json(result))
}

/// `GET /v{version}/tasks/{id}` — inspect a task.
pub async fn get_task(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Task>> {
    let task = app
        .store
        .get_task(&id)
        .await?
        .ok_or_else(|| not_found("task", &id))?;

    Ok(Json(translate::task_to_docker(&task)))
}

// ---------------------------------------------------------------------------
// Networks
// ---------------------------------------------------------------------------

/// `GET /v{version}/networks` — list networks.
pub async fn list_networks(
    State(app): State<Arc<AppState>>,
    Query(query): Query<models::NetworkListQuery>,
) -> ApiResult<Json<Vec<models::NetworkResource>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();
    let label_filters = label_filters(&filters);

    let networks = app.store.list_networks().await?;
    let result: Vec<models::NetworkResource> = networks
        .iter()
        .filter(|n| name_matches(&n.name, &name_filters))
        .filter(|n| id_filters.is_empty() || id_filters.iter().any(|f| f == &n.id))
        .filter(|_| label_filters.is_empty()) // Minga networks don't have labels yet
        .map(translate::network_to_docker)
        .collect();

    Ok(Json(result))
}

/// `POST /v{version}/networks/create` — create a network.
pub async fn create_network(
    State(app): State<Arc<AppState>>,
    Json(req): Json<models::NetworkCreateRequest>,
) -> ApiResult<(StatusCode, Json<models::NetworkCreateResponse>)> {
    // Check for existing network with the same name.
    if let Some(existing) = app.store.get_network(&req.Name).await? {
        return Err(conflict(format!(
            "network {} already exists",
            existing.name
        )));
    }

    let net = translate::network_create_to_state(&req);
    app.store.upsert_network(net.clone()).await?;

    tracing::info!(network = %net.id, driver = %net.driver, "network created via API");

    Ok((
        StatusCode::CREATED,
        Json(models::NetworkCreateResponse {
            ID: net.id,
            Warning: None,
        }),
    ))
}

/// `GET /v{version}/networks/{id}` — inspect a network.
pub async fn get_network(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::NetworkResource>> {
    // Docker clients may use either the network ID or name.
    let net = app
        .store
        .get_network(&id)
        .await?
        .ok_or_else(|| not_found("network", &id))?;

    Ok(Json(translate::network_to_docker(&net)))
}

/// `DELETE /v{version}/networks/{id}` — remove a network.
pub async fn delete_network(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let net = app
        .store
        .get_network(&id)
        .await?
        .ok_or_else(|| not_found("network", &id))?;

    app.store.remove_network(net.id.clone()).await?;

    tracing::info!(network = %net.id, "network removed via API");

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// `GET /v{version}/secrets` — list secrets.
pub async fn list_secrets(
    State(app): State<Arc<AppState>>,
    Query(query): Query<models::SecretListQuery>,
) -> ApiResult<Json<Vec<models::Secret>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();

    let secrets = app.store.list_secrets().await?;
    let result: Vec<models::Secret> = secrets
        .iter()
        .filter(|s| name_matches(&s.name, &name_filters))
        .filter(|s| id_filters.is_empty() || id_filters.iter().any(|f| f == &s.id))
        .map(translate::secret_to_docker)
        .collect();

    Ok(Json(result))
}

/// `POST /v{version}/secrets/create` — create a secret.
pub async fn create_secret(
    State(app): State<Arc<AppState>>,
    Json(spec): Json<models::SecretSpec>,
) -> ApiResult<(StatusCode, Json<models::SecretCreateResponse>)> {
    if let Some(existing) = app.store.get_secret(&spec.Name).await? {
        return Err(conflict(format!("secret {} already exists", existing.name)));
    }

    let secret = translate::secret_spec_to_state(&spec);
    app.store.upsert_secret(secret.clone()).await?;

    tracing::info!(secret = %secret.id, "secret created via API");

    Ok((
        StatusCode::CREATED,
        Json(models::SecretCreateResponse { ID: secret.id }),
    ))
}

/// `GET /v{version}/secrets/{id}` — inspect a secret.
pub async fn get_secret(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Secret>> {
    let secret = app
        .store
        .get_secret(&id)
        .await?
        .ok_or_else(|| not_found("secret", &id))?;

    Ok(Json(translate::secret_to_docker(&secret)))
}

/// `DELETE /v{version}/secrets/{id}` — remove a secret.
pub async fn delete_secret(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let secret = app
        .store
        .get_secret(&id)
        .await?
        .ok_or_else(|| not_found("secret", &id))?;

    app.store.remove_secret(secret.id.clone()).await?;

    tracing::info!(secret = %secret.id, "secret removed via API");

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

/// `GET /v{version}/configs` — list configs.
pub async fn list_configs(
    State(app): State<Arc<AppState>>,
    Query(query): Query<models::ConfigListQuery>,
) -> ApiResult<Json<Vec<models::Config>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();

    let configs = app.store.list_configs().await?;
    let result: Vec<models::Config> = configs
        .iter()
        .filter(|c| name_matches(&c.name, &name_filters))
        .filter(|c| id_filters.is_empty() || id_filters.iter().any(|f| f == &c.id))
        .map(translate::config_to_docker)
        .collect();

    Ok(Json(result))
}

/// `POST /v{version}/configs/create` — create a config.
pub async fn create_config(
    State(app): State<Arc<AppState>>,
    Json(spec): Json<models::ConfigSpec>,
) -> ApiResult<(StatusCode, Json<models::ConfigCreateResponse>)> {
    if let Some(existing) = app.store.get_config(&spec.Name).await? {
        return Err(conflict(format!("config {} already exists", existing.name)));
    }

    let config = translate::config_spec_to_state(&spec);
    app.store.upsert_config(config.clone()).await?;

    tracing::info!(config = %config.id, "config created via API");

    Ok((
        StatusCode::CREATED,
        Json(models::ConfigCreateResponse { ID: config.id }),
    ))
}

/// `GET /v{version}/configs/{id}` — inspect a config.
pub async fn get_config(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Config>> {
    let config = app
        .store
        .get_config(&id)
        .await?
        .ok_or_else(|| not_found("config", &id))?;

    Ok(Json(translate::config_to_docker(&config)))
}

/// `DELETE /v{version}/configs/{id}` — remove a config.
pub async fn delete_config(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let config = app
        .store
        .get_config(&id)
        .await?
        .ok_or_else(|| not_found("config", &id))?;

    app.store.remove_config(config.id.clone()).await?;

    tracing::info!(config = %config.id, "config removed via API");

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Fallback
// ---------------------------------------------------------------------------

/// Fallback handler for unimplemented endpoints.
pub async fn fallback() -> ApiResult<StatusCode> {
    Err(ApiError::NotFound(
        "endpoint not implemented by Minga".to_string(),
    ))
}
