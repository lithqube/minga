//! Axum route handlers for the Docker Engine API subset.
//!
//! Each handler is an async function that extracts path/query/body
//! parameters, calls into the [`StateStore`](minga_state::StateStore),
//! and returns a Docker-compatible JSON response.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    Json,
};
use minga_bus::{subjects, EventBus, TaskActualState, TaskStatusReport};
use minga_state::types::{
    unix_now, ClusterNode, NodeStatus as MingaNodeStatus, ResourceSpec, Service as StateService,
    Task, Tenant, TenantQuotas,
};
use minga_state::StateStore;
use serde::Deserialize;

use crate::{
    auth::Caller,
    error::{bad_request, conflict, internal, not_found, ok_message, ApiError, ApiResult},
    models::{self, Filters},
    translate,
};

/// Shared state for all handlers.
pub struct AppState {
    pub store: Arc<StateStore>,
    pub bus: Option<Arc<dyn EventBus>>,
}

impl AppState {
    pub fn new(store: Arc<StateStore>) -> Self {
        Self { store, bus: None }
    }

    pub fn with_bus(store: Arc<StateStore>, bus: Arc<dyn EventBus>) -> Self {
        Self {
            store,
            bus: Some(bus),
        }
    }

    async fn task_statuses(&self) -> ApiResult<HashMap<String, TaskStatusReport>> {
        let Some(bus) = &self.bus else {
            return Ok(HashMap::new());
        };
        let mut statuses = HashMap::new();
        for (key, bytes) in bus
            .list_status()
            .await
            .map_err(|error| internal(error.to_string()))?
        {
            if !key.starts_with("node.") || !key.contains(".task.") {
                continue;
            }
            match TaskStatusReport::from_bytes(&bytes) {
                Ok(report)
                    if key == subjects::kv_node_task_status(report.node_id, &report.task_id) =>
                {
                    statuses.insert(report.task_id.clone(), report);
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(key, error = %error, "cannot decode task status");
                }
            }
        }
        Ok(statuses)
    }
}

fn tenant_scope(caller: &Caller) -> ApiResult<&str> {
    caller
        .tenant_id()
        .ok_or_else(|| ApiError::NotFound("resource not found".to_owned()))
}
#[derive(Debug, Deserialize)]
pub struct AddManagerRequest {
    id: u64,
    raft_addr: String,
}

#[derive(Debug, Deserialize)]
pub struct ManagerMigrationRequest {
    manager_ids: Vec<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTenantRequest {
    id: String,
    name: String,
    #[serde(default)]
    quotas: TenantQuotas,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTenantRequest {
    name: String,
    #[serde(default)]
    quotas: TenantQuotas,
}

/// Lists the current Raft voters.
pub async fn list_managers(State(app): State<Arc<AppState>>) -> ApiResult<Json<Vec<u64>>> {
    Ok(Json(app.store.manager_voters().await))
}

/// Adds a manager as a learner, catches it up, and promotes it to a voter.
pub async fn add_manager(
    State(app): State<Arc<AppState>>,
    Json(request): Json<AddManagerRequest>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    app.store.add_manager(request.id, request.raft_addr).await?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"id": request.id})),
    ))
}

/// Removes a manager from the Raft voter set.
pub async fn remove_manager(
    State(app): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> ApiResult<StatusCode> {
    app.store.remove_manager(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Replaces the v1.2.0 voter set with the explicitly selected managers.
pub async fn migrate_manager_voters(
    State(app): State<Arc<AppState>>,
    Json(request): Json<ManagerMigrationRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    let removed = app
        .store
        .migrate_manager_voters(request.manager_ids)
        .await?;
    Ok(Json(serde_json::json!({"removed_voters": removed})))
}

/// Lists tenants. This route is available only to cluster operators.
pub async fn list_tenants(State(app): State<Arc<AppState>>) -> ApiResult<Json<Vec<Tenant>>> {
    Ok(Json(app.store.list_tenants().await?))
}

/// Gets one tenant by its stable ID.
pub async fn get_tenant(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Tenant>> {
    let tenant = app
        .store
        .get_tenant(&id)
        .await?
        .ok_or_else(|| not_found("tenant", &id))?;
    Ok(Json(tenant))
}

/// Creates a tenant with operator-supplied desired quotas.
pub async fn create_tenant(
    State(app): State<Arc<AppState>>,
    Json(request): Json<CreateTenantRequest>,
) -> ApiResult<(StatusCode, Json<Tenant>)> {
    let tenant = Tenant {
        id: request.id,
        name: request.name,
        quotas: request.quotas,
        created_at: unix_now(),
    };
    app.store.create_tenant(tenant.clone()).await?;
    Ok((StatusCode::CREATED, Json(tenant)))
}

/// Updates a tenant's display name and quotas.
pub async fn update_tenant(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<UpdateTenantRequest>,
) -> ApiResult<StatusCode> {
    app.store
        .update_tenant(id, request.name, request.quotas)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Deletes an empty tenant.
pub async fn delete_tenant(
    State(app): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    app.store.remove_tenant(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn task_status_for_api<'a>(
    app: &AppState,
    task: &Task,
    statuses: &'a HashMap<String, TaskStatusReport>,
) -> Option<Cow<'a, TaskStatusReport>> {
    if let Some(report) = statuses
        .get(&task.id)
        .filter(|report| report.task_id == task.id && report.node_id == task.node_id)
    {
        return Some(Cow::Borrowed(report));
    }
    app.bus.as_ref().map(|_| {
        Cow::Owned(TaskStatusReport {
            task_id: task.id.clone(),
            node_id: task.node_id,
            state: TaskActualState::Starting,
            container_id: None,
            observed_at: 0,
            restarts: 0,
            last_error: None,
            last_exit_code: None,
            running_since_unix: None,
        })
    })
}

// ---------------------------------------------------------------------------
// State backup and restore
// ---------------------------------------------------------------------------

/// `GET /v{version}/state` — export desired state (see `docs/backup.md`).
///
/// Admin-only once roles exist (docs/adr/0005-api-identity-and-roles.md,
/// Proposed): the snapshot carries service environment values.
pub async fn export_state(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
) -> ApiResult<Json<minga_state::StateSnapshot>> {
    let tenant_id = tenant_scope(&caller)?;
    let mut snapshot = app.store.export().await?;
    snapshot.tenants.retain(|tenant| tenant.id == tenant_id);
    snapshot
        .services
        .retain(|service| service.tenant_id == tenant_id);
    snapshot
        .networks
        .retain(|network| network.tenant_id == tenant_id);
    snapshot
        .volumes
        .retain(|volume| volume.tenant_id == tenant_id);
    snapshot
        .secrets
        .retain(|secret| secret.tenant_id == tenant_id);
    snapshot
        .configs
        .retain(|config| config.tenant_id == tenant_id);
    Ok(Json(snapshot))
}

/// Query of `POST /v{version}/state`.
#[derive(serde::Deserialize)]
pub struct ImportQuery {
    #[serde(default)]
    overwrite: bool,
}

/// `POST /v{version}/state` — import a snapshot; 409 when a service already
/// exists unless `?overwrite=true`. Admin-only once roles exist (see
/// [`export_state`]).
pub async fn import_state(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(q): Query<ImportQuery>,
    Json(body): Json<serde_json::Value>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant_id = tenant_scope(&caller)?;
    // Check the version before the shape: a newer file may not parse as ours.
    let want = minga_state::codec::FORMAT_VERSION;
    match body.get("format_version").and_then(|v| v.as_u64()) {
        Some(v) if v == u64::from(want) => {}
        other => {
            return Err(bad_request(format!(
                "snapshot has format version {}, this build reads {want}",
                other.map_or("none".to_string(), |v| v.to_string())
            )))
        }
    }
    let mut snapshot: minga_state::StateSnapshot = serde_json::from_value(body)?;
    let tenant = app
        .store
        .get_tenant(tenant_id)
        .await?
        .ok_or_else(|| not_found("tenant", tenant_id))?;
    if snapshot.tenants.iter().any(|tenant| tenant.id != tenant_id)
        || snapshot
            .services
            .iter()
            .any(|resource| resource.tenant_id != tenant_id)
        || snapshot
            .networks
            .iter()
            .any(|resource| resource.tenant_id != tenant_id)
        || snapshot
            .volumes
            .iter()
            .any(|resource| resource.tenant_id != tenant_id)
        || snapshot
            .secrets
            .iter()
            .any(|resource| resource.tenant_id != tenant_id)
        || snapshot
            .configs
            .iter()
            .any(|resource| resource.tenant_id != tenant_id)
    {
        return Err(ApiError::NotFound("resource not found".to_owned()));
    }

    for service in &snapshot.services {
        for reference in &service.secrets {
            let in_snapshot = snapshot
                .secrets
                .iter()
                .any(|secret| secret.id == reference.secret_id);
            let in_store = app
                .store
                .get_secret(&reference.secret_id)
                .await?
                .is_some_and(|secret| secret.tenant_id == tenant_id);
            if !in_snapshot && !in_store {
                return Err(not_found("secret", &reference.secret_id));
            }
        }
        for network_id in &service.networks {
            let in_snapshot = snapshot
                .networks
                .iter()
                .any(|network| network.id == *network_id);
            let in_store = app
                .store
                .get_network(network_id)
                .await?
                .is_some_and(|network| network.tenant_id == tenant_id);
            if !in_snapshot && !in_store {
                return Err(not_found("network", network_id));
            }
        }
    }
    // Quotas and tenant metadata are operator-managed, not tenant-importable.
    snapshot.tenants = vec![tenant];
    let services = snapshot.services.len();
    app.store.import(snapshot, q.overwrite).await?;
    tracing::info!(
        tenant = %tenant_id,
        services,
        overwrite = q.overwrite,
        "tenant state imported via API"
    );
    Ok(ok_message(format!("imported {services} service(s)")))
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

/// Deploy-time hard failure: rejects a service that would get zero tasks
/// (no eligible node, or no free capacity) instead of letting the deploy
/// succeed silently. Replicated services scaled to 0 always pass.
///
/// On update, `updating` excludes the service's own current tasks from
/// capacity accounting — they are replaced, so their resources free up.
async fn ensure_placeable(
    app: &AppState,
    svc: &StateService,
    updating: Option<&str>,
) -> ApiResult<()> {
    let nodes = app.store.list_nodes().await?;
    let services = app.store.list_services().await?;
    if let Err(error) = validate_ingress_ports(svc, &services, &nodes) {
        return Err(conflict(error));
    }
    let statuses = app.task_statuses().await?;
    let all_tasks: Vec<_> = app
        .store
        .list_tasks()
        .await?
        .into_iter()
        .map(|task| {
            let status = statuses.get(&task.id);
            minga_scheduler::task_with_actual_status(task, status)
        })
        .collect();
    let networks = app.store.list_networks().await?;
    let tasks: Vec<_> = match updating {
        Some(id) => all_tasks
            .into_iter()
            .filter(|t| t.service_id != id)
            .collect(),
        None => all_tasks,
    };
    minga_scheduler::check_placeable(svc, &nodes, &tasks, &networks, unix_now()).map_err(|_| {
        conflict("service cannot be placed with current cluster capacity or constraints")
    })
}

fn validate_ingress_ports(
    service: &StateService,
    services: &[StateService],
    nodes: &[ClusterNode],
) -> Result<(), String> {
    // The state machine applies the same rule at commit; this is the early,
    // readable rejection. Host-mode clashes on one node are placement's job.
    if minga_state::types::published_port_conflict(service, services).is_some() {
        return Err("published port conflicts with another cluster service".to_owned());
    }
    // An ingress port opens on every node, so no node's listener may own it.
    for port in &service.ports {
        let Some(published) = port.published else {
            continue;
        };
        if port.mode != minga_state::types::PortMode::Ingress {
            continue;
        }
        for node in nodes {
            if node.reserved_ports.iter().any(|reserved| {
                reserved.port == published && reserved.protocol.eq_ignore_ascii_case(&port.protocol)
            }) {
                return Err("ingress port is reserved on a cluster node".to_owned());
            }
        }
    }
    Ok(())
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
pub async fn get_info(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
) -> ApiResult<Json<models::SystemInfo>> {
    let tenant_id = caller.tenant_id();
    let node_id = app.store.node_id();
    let is_leader = app.store.is_leader().await;

    let nodes = app.store.list_nodes().await?;
    let services: Vec<_> = app
        .store
        .list_services()
        .await?
        .into_iter()
        .filter(|service| tenant_id.map_or(true, |tenant_id| service.tenant_id == tenant_id))
        .collect();
    let tasks: Vec<_> = app
        .store
        .list_tasks()
        .await?
        .into_iter()
        .filter(|task| tenant_id.map_or(true, |tenant_id| task.tenant_id == tenant_id))
        .collect();
    let statuses = app.task_statuses().await?;

    let running = tasks
        .iter()
        .filter(|task| {
            statuses.get(&task.id).is_some_and(|status| {
                status.node_id == task.node_id
                    && status.state == minga_bus::TaskActualState::Running
            })
        })
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

/// `POST /v{version}/nodes` — register a node out-of-band (Minga extension).
pub async fn create_node(
    State(app): State<Arc<AppState>>,
    Json(req): Json<models::NodeCreateRequest>,
) -> ApiResult<(StatusCode, Json<models::Node>)> {
    if req.address.trim().is_empty() {
        return Err(bad_request("node address must not be empty"));
    }

    let nodes = app.store.list_nodes().await?;
    let id = match req.id {
        Some(id) => {
            if nodes.iter().any(|n| n.id == id) {
                return Err(conflict(format!("node {id} already exists")));
            }
            id
        }
        None => nodes.iter().map(|n| n.id).max().unwrap_or(0) + 1,
    };
    if nodes.iter().any(|n| n.address == req.address) {
        return Err(conflict(format!(
            "node address {} is already registered",
            req.address
        )));
    }

    let node = ClusterNode {
        id,
        address: req.address,
        labels: req.labels,
        status: MingaNodeStatus::Active,
        capacity: ResourceSpec {
            cpus: req.cpus.unwrap_or(1.0),
            memory_bytes: req.memory_bytes.unwrap_or(0),
            gpus: req.gpus.unwrap_or(0),
        },
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        // Operator-registered nodes carry no lease: they stay eligible
        // until explicitly removed or drained.
        last_seen_unix: None,
        role: Default::default(),
        reserved_ports: Vec::new(),
    };
    app.store.upsert_node(node.clone()).await?;
    tracing::info!(node = id, address = %node.address, "node registered via API");

    let is_leader = app.store.current_leader().await == Some(id);
    Ok((
        StatusCode::CREATED,
        Json(translate::node_to_docker(&node, is_leader)),
    ))
}

/// `POST /v{version}/nodes/{id}/update` — update labels and availability.
///
/// Only what Minga can honestly do is accepted: label replacement and
/// `active`/`drain` availability. Role is derived from Raft leadership and
/// names are `node-{id}`; requests to change either fail loudly.
pub async fn update_node(
    State(app): State<Arc<AppState>>,
    Path((_version, id)): Path<(String, String)>,
    Query(_query): Query<models::NodeUpdateQuery>,
    Json(spec): Json<models::NodeSpec>,
) -> ApiResult<Json<models::Node>> {
    let nodes = app.store.list_nodes().await?;
    let mut node = nodes
        .into_iter()
        .find(|n| n.id.to_string() == id || n.address == id)
        .ok_or_else(|| not_found("node", &id))?;

    let is_leader = app.store.current_leader().await == Some(node.id);
    let current_role = if is_leader { "manager" } else { "worker" };
    if !spec.Role.is_empty() && spec.Role != current_role {
        return Err(conflict(format!(
            "changing node role ({current_role} → {}) is not supported",
            spec.Role
        )));
    }
    if let Some(name) = &spec.Name {
        let current = format!("node-{}", node.id);
        if *name != current {
            return Err(bad_request(format!(
                "renaming nodes is not supported (current name: {current})"
            )));
        }
    }

    node.status = match spec.Availability.as_str() {
        "" | "active" => MingaNodeStatus::Active,
        "drain" => MingaNodeStatus::Draining,
        "pause" => {
            return Err(bad_request(
                "pause availability is not supported: Minga has no pause semantics, use drain",
            ));
        }
        other => return Err(bad_request(format!("unknown availability {other:?}"))),
    };
    node.labels = spec.Labels;

    app.store.upsert_node(node.clone()).await?;
    tracing::info!(node = node.id, status = ?node.status, "node updated via API");

    Ok(Json(translate::node_to_docker(&node, is_leader)))
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

/// `GET /v{version}/services` — list services.
pub async fn list_services(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<models::ServiceListQuery>,
) -> ApiResult<Json<Vec<models::Service>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let label_filters = label_filters(&filters);
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();

    let tenant_id = tenant_scope(&caller)?;
    let services: Vec<_> = app
        .store
        .list_services()
        .await?
        .into_iter()
        .filter(|service| service.tenant_id == tenant_id)
        .collect();
    let tasks: Vec<_> = app
        .store
        .list_tasks()
        .await?
        .into_iter()
        .filter(|task| task.tenant_id == tenant_id)
        .collect();
    let statuses = app.task_statuses().await?;
    let networks: Vec<_> = app
        .store
        .list_networks()
        .await?
        .into_iter()
        .filter(|network| network.tenant_id == tenant_id)
        .collect();

    let result: Vec<models::Service> = services
        .iter()
        .filter(|s| {
            // Label filter: state labels first, MINGA_STACK env as fallback.
            let labels_ok = if label_filters.is_empty() {
                true
            } else {
                let mut pseudo_labels = s.labels.clone();
                if pseudo_labels.is_empty() {
                    if let Some(stack) = s.env.get("MINGA_STACK") {
                        pseudo_labels
                            .insert("com.docker.stack.namespace".to_string(), stack.clone());
                    }
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
            docker_svc.TaskList = tasks
                .iter()
                .filter(|task| task.service_id == s.id)
                .map(|task| {
                    let actual = task_status_for_api(&app, task, &statuses);
                    translate::task_to_docker(task, actual.as_deref(), &networks)
                })
                .collect();
            docker_svc
        })
        .collect();

    Ok(Json(result))
}

/// `POST /v{version}/services/create` — create a service.
pub async fn create_service(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Json(spec): Json<models::ServiceSpec>,
) -> ApiResult<(StatusCode, Json<models::ServiceCreateResponse>)> {
    let tenant_id = tenant_scope(&caller)?;
    let mut svc = translate::service_spec_to_state(&spec).map_err(ApiError::BadRequest)?;
    svc.tenant_id = tenant_id.to_owned();
    for reference in &svc.secrets {
        let secret = app.store.get_secret(&reference.secret_id).await?;
        match secret {
            Some(secret) if secret.tenant_id == tenant_id => {}
            Some(_) => return Err(not_found("secret", &reference.secret_id)),
            None => {
                return Err(bad_request(format!(
                    "secret `{}` is not declared; create its metadata and provision it on eligible nodes first",
                    reference.secret_id
                )))
            }
        }
    }
    for network_id in &svc.networks {
        let network = app.store.get_network(network_id).await?;
        if !network.is_some_and(|network| network.tenant_id == tenant_id) {
            return Err(not_found("network", network_id));
        }
    }

    if app.store.get_service(&svc.id).await?.is_some() {
        return Err(conflict("service ID already exists"));
    }

    ensure_placeable(&app, &svc, None).await?;

    app.store
        .upsert_service(svc.clone())
        .await
        .map_err(|error| match error {
            minga_state::StateError::PortConflict(_) => {
                conflict("published port conflicts with another cluster service")
            }
            other => ApiError::from(other),
        })?;

    tracing::info!(tenant = %tenant_id, service = %svc.id, image = %svc.image, "service created via API");

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
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Service>> {
    let tenant_id = tenant_scope(&caller)?;
    let services = app.store.list_services().await?;
    let tasks = app.store.list_tasks().await?;
    let statuses = app.task_statuses().await?;
    let networks: Vec<_> = app
        .store
        .list_networks()
        .await?
        .into_iter()
        .filter(|network| network.tenant_id == tenant_id)
        .collect();

    let svc = services
        .iter()
        .find(|service| service.tenant_id == tenant_id && (service.id == id || service.name == id))
        .ok_or_else(|| not_found("service", &id))?;

    let mut docker_svc = translate::service_to_docker(svc);
    docker_svc.TaskList = tasks
        .iter()
        .filter(|task| task.tenant_id == tenant_id && task.service_id == svc.id)
        .map(|task| {
            let actual = task_status_for_api(&app, task, &statuses);
            translate::task_to_docker(task, actual.as_deref(), &networks)
        })
        .collect();

    Ok(Json(docker_svc))
}

/// `DELETE /v{version}/services/{id}` — remove a service.
pub async fn delete_service(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let tenant_id = tenant_scope(&caller)?;
    let services = app.store.list_services().await?;
    let svc = services
        .iter()
        .find(|service| service.tenant_id == tenant_id && (service.id == id || service.name == id))
        .ok_or_else(|| not_found("service", &id))?;

    let tasks = app.store.list_tasks().await?;
    for task in tasks
        .iter()
        .filter(|task| task.tenant_id == tenant_id && task.service_id == svc.id)
    {
        app.store.remove_task(task.id.clone()).await?;
    }

    app.store.remove_service(svc.id.clone()).await?;

    tracing::info!(tenant = %tenant_id, service = %svc.id, "service removed via API");

    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v{version}/services/{id}/update` — update a service.
pub async fn update_service(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
    Query(_query): Query<models::ServiceUpdateQuery>,
    Json(spec): Json<models::ServiceSpec>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant_id = tenant_scope(&caller)?;
    let services = app.store.list_services().await?;
    let existing = services
        .iter()
        .find(|service| service.tenant_id == tenant_id && (service.id == id || service.name == id))
        .ok_or_else(|| not_found("service", &id))?;

    let mut svc = translate::service_spec_to_state(&spec).map_err(ApiError::BadRequest)?;
    svc.tenant_id = tenant_id.to_owned();
    for reference in &svc.secrets {
        match app.store.get_secret(&reference.secret_id).await? {
            Some(secret) if secret.tenant_id == tenant_id => {}
            Some(_) => return Err(not_found("secret", &reference.secret_id)),
            None => {
                return Err(bad_request(format!(
                    "secret `{}` is not declared; create its metadata and provision it on eligible nodes first",
                    reference.secret_id
                )))
            }
        }
    }
    for network_id in &svc.networks {
        let network = app.store.get_network(network_id).await?;
        if !network.is_some_and(|network| network.tenant_id == tenant_id) {
            return Err(not_found("network", network_id));
        }
    }
    svc.id = existing.id.clone();
    svc.name = existing.name.clone();

    let comparable = |s: &minga_state::types::Service| minga_state::types::Service {
        previous: None,
        update: Default::default(),
        rollback: None,
        ..s.clone()
    };
    svc.previous = if comparable(&svc) == comparable(existing) {
        existing.previous.clone()
    } else {
        Some(Box::new(minga_state::types::Service {
            previous: None,
            ..existing.clone()
        }))
    };

    ensure_placeable(&app, &svc, Some(&svc.id)).await?;

    app.store
        .upsert_service(svc.clone())
        .await
        .map_err(|error| match error {
            minga_state::StateError::PortConflict(_) => {
                conflict("published port conflicts with another cluster service")
            }
            other => ApiError::from(other),
        })?;

    tracing::info!(tenant = %tenant_id, service = %svc.id, "service updated via API");

    Ok(ok_message(""))
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

/// `GET /v{version}/tasks` — list tasks.
pub async fn list_tasks(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<models::TaskListQuery>,
) -> ApiResult<Json<Vec<models::Task>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let label_filters = label_filters(&filters);
    let service_filters = filters.get("service").cloned().unwrap_or_default();
    let node_filters = filters.get("node").cloned().unwrap_or_default();
    let desired_state_filters = filters.get("desired-state").cloned().unwrap_or_default();

    let tenant_id = tenant_scope(&caller)?;
    let mut tasks: Vec<_> = app
        .store
        .list_tasks()
        .await?
        .into_iter()
        .filter(|task| task.tenant_id == tenant_id)
        .collect();
    let services: Vec<_> = app
        .store
        .list_services()
        .await?
        .into_iter()
        .filter(|service| service.tenant_id == tenant_id)
        .collect();

    tasks.retain(|task| services.iter().any(|service| service.id == task.service_id));
    let statuses = app.task_statuses().await?;
    let networks: Vec<_> = app
        .store
        .list_networks()
        .await?
        .into_iter()
        .filter(|network| network.tenant_id == tenant_id)
        .collect();
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
        .map(|task| {
            let actual = task_status_for_api(&app, task, &statuses);
            translate::task_to_docker(task, actual.as_deref(), &networks)
        })
        .collect();

    Ok(Json(result))
}

/// `GET /v{version}/tasks/{id}` — inspect a task.
pub async fn get_task(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Task>> {
    let tenant_id = tenant_scope(&caller)?;
    let task = app
        .store
        .get_task(&id)
        .await?
        .filter(|task| task.tenant_id == tenant_id)
        .ok_or_else(|| not_found("task", &id))?;
    if !app
        .store
        .get_service(&task.service_id)
        .await?
        .is_some_and(|service| service.tenant_id == tenant_id)
    {
        return Err(not_found("task", &id));
    }
    let statuses = app.task_statuses().await?;
    let networks: Vec<_> = app
        .store
        .list_networks()
        .await?
        .into_iter()
        .filter(|network| network.tenant_id == tenant_id)
        .collect();

    let actual = task_status_for_api(&app, &task, &statuses);

    Ok(Json(translate::task_to_docker(
        &task,
        actual.as_deref(),
        &networks,
    )))
}

// ---------------------------------------------------------------------------
// Networks
// ---------------------------------------------------------------------------

/// `GET /v{version}/networks` — list networks.
pub async fn list_networks(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<models::NetworkListQuery>,
) -> ApiResult<Json<Vec<models::NetworkResource>>> {
    let filters = models::parse_filters(query.filters.as_deref());
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();
    let label_filters = label_filters(&filters);

    let tenant_id = tenant_scope(&caller)?;
    let networks: Vec<_> = app
        .store
        .list_networks()
        .await?
        .into_iter()
        .filter(|network| network.tenant_id == tenant_id)
        .collect();
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
    Extension(caller): Extension<Caller>,
    Json(req): Json<models::NetworkCreateRequest>,
) -> ApiResult<(StatusCode, Json<models::NetworkCreateResponse>)> {
    let tenant_id = tenant_scope(&caller)?;
    if let Some(network) = app.store.get_network(&req.Name).await? {
        if network.tenant_id == tenant_id {
            return Ok((
                StatusCode::CREATED,
                Json(models::NetworkCreateResponse {
                    ID: network.id,
                    Warning: None,
                }),
            ));
        }
        return Err(conflict("network name is already in use"));
    }

    minga_net::overlay::check_driver_opts(&req.Name, req.Options.keys().map(String::as_str))
        .map_err(bad_request)?;
    let mut net = translate::network_create_to_state(&req);
    net.tenant_id = tenant_id.to_owned();
    // An explicit subnet must not overlap another network's. Networks
    // without one get theirs from the leader's next reconcile pass.
    if let Some(raw) = &net.subnet {
        let subnet = minga_net::Cidr::parse(raw).map_err(|e| bad_request(e.to_string()))?;
        for other in app.store.list_networks().await? {
            let Some(theirs) = other.subnet.as_deref() else {
                continue;
            };
            if minga_net::Cidr::parse(theirs).is_ok_and(|t| t.overlaps(&subnet)) {
                return Err(conflict(
                    "network subnet overlaps an existing cluster network",
                ));
            }
        }
    }
    app.store.upsert_network(net.clone()).await?;

    tracing::info!(tenant = %tenant_id, network = %net.id, driver = %net.driver, "network created via API");

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
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::NetworkResource>> {
    let tenant_id = tenant_scope(&caller)?;
    let net = app
        .store
        .get_network(&id)
        .await?
        .filter(|network| network.tenant_id == tenant_id)
        .ok_or_else(|| not_found("network", &id))?;

    Ok(Json(translate::network_to_docker(&net)))
}

/// `DELETE /v{version}/networks/{id}` — remove a network.
pub async fn delete_network(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let tenant_id = tenant_scope(&caller)?;
    let net = app
        .store
        .get_network(&id)
        .await?
        .filter(|network| network.tenant_id == tenant_id)
        .ok_or_else(|| not_found("network", &id))?;

    app.store.remove_network(net.id.clone()).await?;

    tracing::info!(tenant = %tenant_id, network = %net.id, "network removed via API");

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// `GET /v{version}/secrets` — list secrets.
pub async fn list_secrets(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<models::SecretListQuery>,
) -> ApiResult<Json<Vec<models::Secret>>> {
    let tenant_id = tenant_scope(&caller)?;
    let filters = models::parse_filters(query.filters.as_deref());
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();

    let secrets: Vec<_> = app
        .store
        .list_secrets()
        .await?
        .into_iter()
        .filter(|secret| secret.tenant_id == tenant_id)
        .collect();
    let result: Vec<models::Secret> = secrets
        .iter()
        .filter(|s| name_matches(&s.name, &name_filters))
        .filter(|s| id_filters.is_empty() || id_filters.iter().any(|f| f == &s.id))
        .map(translate::secret_to_docker)
        .collect();

    Ok(Json(result))
}

/// `POST /v{version}/secrets/create` — create a secret.
///
/// Only **metadata** is accepted. Docker's API allows an inline `Data`
/// payload, but Minga has no secure path for a value submitted over the
/// wire: it would have to travel through the API, into Raft, and out to
/// every node. Accepting the field and silently dropping it would report
/// success for a secret that does not exist, so a non-empty `Data` is
/// refused with a clear error instead.
pub async fn create_secret(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Json(spec): Json<models::SecretSpec>,
) -> ApiResult<(StatusCode, Json<models::SecretCreateResponse>)> {
    let tenant_id = tenant_scope(&caller)?;
    if !spec.Data.is_empty() {
        return Err(bad_request(
            "creating a secret with inline data is not supported: \
             Minga secrets carry metadata only, and the value must be \
             provisioned on the node's secret store",
        ));
    }

    if app.store.get_secret(&spec.Name).await?.is_some() {
        return Err(conflict("secret ID already exists"));
    }

    let mut secret = translate::secret_spec_to_state(&spec);
    secret.tenant_id = tenant_id.to_owned();
    app.store.upsert_secret(secret.clone()).await?;

    tracing::info!(tenant = %tenant_id, secret = %secret.id, "secret created via API");

    Ok((
        StatusCode::CREATED,
        Json(models::SecretCreateResponse { ID: secret.id }),
    ))
}

/// `GET /v{version}/secrets/{id}` — inspect a secret.
pub async fn get_secret(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Secret>> {
    let tenant_id = tenant_scope(&caller)?;
    let secret = app
        .store
        .get_secret(&id)
        .await?
        .filter(|secret| secret.tenant_id == tenant_id)
        .ok_or_else(|| not_found("secret", &id))?;

    Ok(Json(translate::secret_to_docker(&secret)))
}

/// `DELETE /v{version}/secrets/{id}` — remove a secret.
pub async fn delete_secret(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let tenant_id = tenant_scope(&caller)?;
    let secret = app
        .store
        .get_secret(&id)
        .await?
        .filter(|secret| secret.tenant_id == tenant_id)
        .ok_or_else(|| not_found("secret", &id))?;

    app.store.remove_secret(secret.id.clone()).await?;

    tracing::info!(tenant = %tenant_id, secret = %secret.id, "secret removed via API");

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

/// `GET /v{version}/configs` — list configs.
pub async fn list_configs(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<models::ConfigListQuery>,
) -> ApiResult<Json<Vec<models::Config>>> {
    let tenant_id = tenant_scope(&caller)?;
    let filters = models::parse_filters(query.filters.as_deref());
    let name_filters = filters.get("name").cloned().unwrap_or_default();
    let id_filters = filters.get("id").cloned().unwrap_or_default();

    let configs: Vec<_> = app
        .store
        .list_configs()
        .await?
        .into_iter()
        .filter(|config| config.tenant_id == tenant_id)
        .collect();
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
    Extension(caller): Extension<Caller>,
    Json(spec): Json<models::ConfigSpec>,
) -> ApiResult<(StatusCode, Json<models::ConfigCreateResponse>)> {
    let tenant_id = tenant_scope(&caller)?;
    if app.store.get_config(&spec.Name).await?.is_some() {
        return Err(conflict("config ID already exists"));
    }

    let mut config = translate::config_spec_to_state(&spec);
    config.tenant_id = tenant_id.to_owned();
    app.store.upsert_config(config.clone()).await?;

    tracing::info!(tenant = %tenant_id, config = %config.id, "config created via API");

    Ok((
        StatusCode::CREATED,
        Json(models::ConfigCreateResponse { ID: config.id }),
    ))
}

/// `GET /v{version}/configs/{id}` — inspect a config.
pub async fn get_config(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Config>> {
    let tenant_id = tenant_scope(&caller)?;
    let config = app
        .store
        .get_config(&id)
        .await?
        .filter(|config| config.tenant_id == tenant_id)
        .ok_or_else(|| not_found("config", &id))?;

    Ok(Json(translate::config_to_docker(&config)))
}

/// `DELETE /v{version}/configs/{id}` — remove a config.
pub async fn delete_config(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let tenant_id = tenant_scope(&caller)?;
    let config = app
        .store
        .get_config(&id)
        .await?
        .filter(|config| config.tenant_id == tenant_id)
        .ok_or_else(|| not_found("config", &id))?;

    app.store.remove_config(config.id.clone()).await?;

    tracing::info!(tenant = %tenant_id, config = %config.id, "config removed via API");

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

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::{NodeStatus, Task, TaskSpec, TaskState};
    use std::collections::{BTreeMap, HashMap};

    /// An AppState with a single active node of `cpus` CPUs.
    async fn app_with_node(cpus: f64) -> (Arc<AppState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .upsert_node(ClusterNode {
                id: 1,
                address: "127.0.0.1:50051".to_string(),
                labels: HashMap::new(),
                status: NodeStatus::Active,
                capacity: ResourceSpec {
                    cpus,
                    memory_bytes: 0,
                    gpus: 0,
                },
                wg_public_key: None,
                wg_endpoint: None,
                overlay_ip: None,
                last_seen_unix: None,
                role: Default::default(),
                reserved_ports: Vec::new(),
            })
            .await
            .unwrap();
        (Arc::new(AppState::new(store)), dir)
    }

    fn spec(name: &str, nano_cpus: i64) -> models::ServiceSpec {
        serde_json::from_value(serde_json::json!({
            "Name": name,
            "TaskTemplate": {
                "ContainerSpec": {"Image": "nginx:latest"},
                "Resources": {"Limits": {"NanoCPUs": nano_cpus}}
            },
            "Mode": {"Replicated": {"Replicas": 1}}
        }))
        .unwrap()
    }

    fn net_req(name: &str, body: serde_json::Value) -> Json<models::NetworkCreateRequest> {
        let mut v = serde_json::json!({"Name": name});
        v.as_object_mut()
            .unwrap()
            .extend(body.as_object().unwrap().clone());
        Json(serde_json::from_value(v).unwrap())
    }

    fn default_caller() -> Extension<Caller> {
        Extension(Caller {
            name: "test".into(),
            role: crate::auth::ApiRole::Admin,
            tenant_id: Some("default".into()),
            cluster_operator: false,
        })
    }

    #[tokio::test]
    async fn overlapping_ipam_subnet_does_not_disclose_other_networks() {
        let (app, _dir) = app_with_node(1.0).await;
        let ipam = |s: &str| serde_json::json!({"IPAM": {"Config": [{"Subnet": s}]}});
        let _ = create_network(
            State(app.clone()),
            default_caller(),
            net_req("a", ipam("10.100.5.0/24")),
        )
        .await
        .expect("first subnet is free");
        let err = create_network(
            State(app.clone()),
            default_caller(),
            net_req("b", ipam("10.100.5.128/25")),
        )
        .await
        .unwrap_err();
        let ApiError::Conflict(msg) = err else {
            panic!("want 409, got {err:?}");
        };
        assert_eq!(msg, "network subnet overlaps an existing cluster network");
        assert!(app.store.get_network("b").await.unwrap().is_none());
        let _ = create_network(
            State(app.clone()),
            default_caller(),
            net_req("c", ipam("10.100.6.0/24")),
        )
        .await
        .expect("non-overlapping subnet");
    }

    #[tokio::test]
    async fn unknown_network_driver_option_is_refused() {
        let (app, _dir) = app_with_node(1.0).await;
        let opts = |k: &str| serde_json::json!({"Options": {k: "1"}});
        let _ = create_network(
            State(app.clone()),
            default_caller(),
            net_req("a", opts("encrypted")),
        )
        .await
        .expect("encrypted is accepted");
        let err = create_network(
            State(app.clone()),
            default_caller(),
            net_req("b", opts("com.docker.network.mtu")),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "{err:?}");
    }

    #[tokio::test]
    async fn update_excludes_the_services_own_tasks_from_capacity() {
        let (app, _dir) = app_with_node(1.0).await;

        // web takes half the node's CPU and (simulated) is running there.
        let _ = create_service(
            State(app.clone()),
            default_caller(),
            Json(spec("web", 500_000_000)),
        )
        .await
        .expect("web should fit");
        app.store
            .upsert_task(Task {
                id: "web.1".to_string(),
                tenant_id: "default".to_string(),
                service_id: "web".to_string(),
                node_id: 1,
                state: TaskState::Running,
                spec: TaskSpec {
                    resources: ResourceSpec {
                        cpus: 0.5,
                        memory_bytes: 0,
                        gpus: 0,
                    },
                    ..Default::default()
                },
                restarts: 0,
                running_since_unix: None,
                last_error: None,
                last_exit_code: None,
            })
            .await
            .unwrap();

        // Another service asking for more than the remaining 0.5 CPU fails.
        let err = create_service(
            State(app.clone()),
            default_caller(),
            Json(spec("other", 600_000_000)),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ApiError::Conflict(m) if m == "service cannot be placed with current cluster capacity or constraints"),
            "expected a generic placement conflict, got {err:?}"
        );
        // Updating web itself may use up to the full node: its own task's
        // capacity is freed by the replacement.
        let _ = update_service(
            State(app.clone()),
            default_caller(),
            Path(("v1.41".to_string(), "web".to_string())),
            Query(
                serde_json::from_value::<models::ServiceUpdateQuery>(serde_json::json!({}))
                    .unwrap(),
            ),
            Json(spec("web", 900_000_000)),
        )
        .await
        .expect("update may reuse the capacity of the task it replaces");

        // But not more than the whole node.
        let err = update_service(
            State(app.clone()),
            default_caller(),
            Path(("v1.41".to_string(), "web".to_string())),
            Query(
                serde_json::from_value::<models::ServiceUpdateQuery>(serde_json::json!({}))
                    .unwrap(),
            ),
            Json(spec("web", 1_500_000_000)),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ApiError::Conflict(m) if m == "service cannot be placed with current cluster capacity or constraints"),
            "expected a generic placement conflict, got {err:?}"
        );
    }
}
