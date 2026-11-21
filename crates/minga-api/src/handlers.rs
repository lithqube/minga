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
use minga_bus::{
    subjects, AuditRecord, EventBus, TaskActualState, TaskStatusReport, UsageEvent, UsageTotals,
};
use minga_state::types::{
    unix_now, ChangeKind, ChangeRequest, ChangeRequestAudit, ChangeRequestStatus, ClusterNode,
    NodeStatus as MingaNodeStatus, ResourceSpec, Service as StateService, Task, Tenant,
    TenantQuotas,
};
use minga_state::{join_token::JoinTokenKey, StateStore};
use serde::{Deserialize, Serialize};

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
    pub join_token_key: Option<Arc<JoinTokenKey>>,
    pub nats_credential_issuer: Option<Arc<crate::nats_credentials::NatsCredentialIssuer>>,
}

impl AppState {
    pub fn new(store: Arc<StateStore>) -> Self {
        Self {
            store,
            bus: None,
            join_token_key: None,
            nats_credential_issuer: None,
        }
    }

    pub fn with_bus(store: Arc<StateStore>, bus: Arc<dyn EventBus>) -> Self {
        Self {
            store,
            bus: Some(bus),
            join_token_key: None,
            nats_credential_issuer: None,
        }
    }

    pub fn with_join_token_key(mut self, key: Arc<JoinTokenKey>) -> Self {
        self.join_token_key = Some(key);
        self
    }
    pub fn with_nats_credential_issuer(
        mut self,
        issuer: Arc<crate::nats_credentials::NatsCredentialIssuer>,
    ) -> Self {
        self.nats_credential_issuer = Some(issuer);
        self
    }

    async fn task_statuses(&self) -> ApiResult<HashMap<String, HashMap<String, TaskStatusReport>>> {
        let Some(bus) = &self.bus else {
            return Ok(HashMap::new());
        };
        let tenant_ids = self
            .store
            .list_tenants()
            .await?
            .into_iter()
            .map(|tenant| tenant.id)
            .collect::<Vec<_>>();
        let mut statuses = HashMap::new();
        for (tenant_id, key, bytes) in bus
            .list_all_tenant_status(&tenant_ids)
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
                    statuses
                        .entry(tenant_id)
                        .or_insert_with(HashMap::new)
                        .insert(report.task_id.clone(), report);
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
    Extension(caller): Extension<Caller>,
    Json(request): Json<CreateTenantRequest>,
) -> ApiResult<(StatusCode, Json<Tenant>)> {
    let tenant = Tenant {
        nats_revoked_user_public_keys: Vec::new(),
        id: request.id,
        name: request.name,
        quotas: request.quotas,
        created_at: unix_now(),
    };
    Tenant::validate_id(&tenant.id).map_err(bad_request)?;
    if app.store.get_tenant(&tenant.id).await?.is_some() {
        return Err(conflict(format!("tenant {} already exists", tenant.id)));
    }
    configure_tenant_bus(&app, &tenant).await?;
    append_tenant_audit(
        &app,
        &tenant.id,
        &caller.name,
        "tenant.create.requested",
        &format!("/tenants/{}", tenant.id),
    )
    .await?;
    app.store.create_tenant(tenant.clone()).await?;
    Ok((StatusCode::CREATED, Json(tenant)))
}

async fn append_tenant_audit(
    app: &AppState,
    tenant_id: &str,
    actor: &str,
    action: &str,
    resource_path: &str,
) -> ApiResult<()> {
    let bus = app
        .bus
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("tenant audit service is not configured".into()))?;
    bus.append_audit(AuditRecord {
        tenant_id: tenant_id.into(),
        actor: actor.into(),
        action: action.into(),
        resource_path: resource_path.into(),
        occurred_at_unix: unix_now(),
    })
    .await
    .map_err(|error| ApiError::Unavailable(format!("tenant audit append failed: {error}")))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangeProposal {
    Deploy { service_id: String, image: String },
    Scale { service_id: String, replicas: u32 },
    Rollback { service_id: String },
    Stop { service_id: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeRequestQuery {
    limit: Option<usize>,
    /// Only requests in this state, filtered before `limit`, so a pending
    /// request stays findable behind newer decided ones.
    status: Option<ChangeRequestStatus>,
}

#[derive(Debug, Serialize)]
/// What a reviewer sees of one side of a change: the fields whose values are
/// safe to show. `env`, secret references and egress stay out (they can hold
/// sensitive values); a change to them still appears by name in `changed_fields`.
struct ChangeServiceSummary {
    image: String,
    replicas: u32,
    mode: minga_state::types::ServiceMode,
    ports: Vec<minga_state::types::PortMapping>,
    networks: Vec<String>,
    constraints: Vec<String>,
}

impl From<&StateService> for ChangeServiceSummary {
    fn from(service: &StateService) -> Self {
        Self {
            image: service.image.clone(),
            replicas: service.replicas,
            mode: service.mode,
            ports: service.ports.clone(),
            networks: service.networks.clone(),
            constraints: service.constraints.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChangeRequestView {
    id: String,
    tenant_id: String,
    service_id: String,
    requester: String,
    created_at_unix: u64,
    expires_at_unix: u64,
    kind: ChangeKind,
    before: ChangeServiceSummary,
    after: ChangeServiceSummary,
    /// Every spec field approval would change, by name only: a rollback can
    /// change ports or networks while image and replicas stay the same.
    /// Values are omitted because `env` can hold secrets.
    changed_fields: Vec<String>,
    /// The exact diff, one entry per changed field, with environment values
    /// replaced by `<hidden>`.
    changes: Vec<FieldChange>,
    /// Environment keys whose hidden values differ, as `env.KEY` paths.
    hidden_changes: Vec<String>,
    status: ChangeRequestStatus,
    reviewer: Option<String>,
    decided_at_unix: Option<u64>,
    audit: Vec<ChangeRequestAudit>,
}

#[derive(Debug, Serialize)]
struct FieldChange {
    field: String,
    before: serde_json::Value,
    after: serde_json::Value,
}

/// Maps whose values can be secrets: shown by key only.
const HIDDEN_MAPS: [&str; 2] = ["env", "sandbox_env"];

/// `value` with every hidden map's values replaced by `<hidden>`, recording
/// in `changed` the paths of hidden values that differ from `other`.
fn redact(
    value: &serde_json::Value,
    other: &serde_json::Value,
    path: &str,
    changed: &mut Vec<String>,
) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, field)| {
                    let path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    let other_field = other.get(key).unwrap_or(&Value::Null);
                    let shown = match field.as_object() {
                        Some(map) if HIDDEN_MAPS.contains(&key.as_str()) => Value::Object(
                            map.iter()
                                .map(|(name, secret)| {
                                    if other_field.get(name) != Some(secret) {
                                        changed.push(format!("{path}.{name}"));
                                    }
                                    (name.clone(), Value::String("<hidden>".into()))
                                })
                                .collect(),
                        ),
                        _ => redact(field, other_field, &path, changed),
                    };
                    (key.clone(), shown)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    let other_item = other.get(index).unwrap_or(&Value::Null);
                    redact(item, other_item, &format!("{path}[{index}]"), changed)
                })
                .collect(),
        ),
        _ => value.clone(),
    }
}

/// The redacted diff of every changed field, and the hidden paths that changed.
fn service_changes(
    before: &StateService,
    after: &StateService,
    fields: &[String],
) -> (Vec<FieldChange>, Vec<String>) {
    let (before, after) = (
        serde_json::to_value(before).unwrap_or_default(),
        serde_json::to_value(after).unwrap_or_default(),
    );
    let null = serde_json::Value::Null;
    let mut hidden = Vec::new();
    let changes = fields
        .iter()
        .map(|field| {
            let (old, new) = (
                before.get(field).unwrap_or(&null),
                after.get(field).unwrap_or(&null),
            );
            // Both directions, so added, removed and changed keys all show.
            let wrap = |value: &serde_json::Value| serde_json::json!({ field.as_str(): value });
            let old_shown = redact(&wrap(old), &wrap(new), "", &mut hidden);
            let new_shown = redact(&wrap(new), &wrap(old), "", &mut hidden);
            FieldChange {
                field: field.clone(),
                before: old_shown[field.as_str()].clone(),
                after: new_shown[field.as_str()].clone(),
            }
        })
        .collect();
    hidden.sort();
    hidden.dedup();
    (changes, hidden)
}

/// Top-level service fields that differ, ignoring the rollback history.
fn changed_service_fields(before: &StateService, after: &StateService) -> Vec<String> {
    let as_object = |service: &StateService| match serde_json::to_value(service) {
        Ok(serde_json::Value::Object(fields)) => fields,
        _ => serde_json::Map::new(),
    };
    let (before, after) = (as_object(before), as_object(after));
    let mut names: Vec<String> = before
        .keys()
        .chain(after.keys())
        .filter(|name| name.as_str() != "previous" && before.get(*name) != after.get(*name))
        .cloned()
        .collect();
    names.sort();
    names.dedup();
    names
}

impl From<ChangeRequest> for ChangeRequestView {
    fn from(request: ChangeRequest) -> Self {
        let changed_fields = changed_service_fields(&request.before, &request.after);
        let (changes, hidden_changes) =
            service_changes(&request.before, &request.after, &changed_fields);
        Self {
            id: request.id,
            tenant_id: request.tenant_id,
            service_id: request.service_id,
            requester: request.requester,
            created_at_unix: request.created_at_unix,
            expires_at_unix: request.expires_at_unix,
            kind: request.kind,
            before: ChangeServiceSummary::from(&request.before),
            after: ChangeServiceSummary::from(&request.after),
            changed_fields,
            changes,
            hidden_changes,
            status: request.status,
            reviewer: request.reviewer,
            decided_at_unix: request.decided_at_unix,
            audit: request.audit,
        }
    }
}

const CHANGE_REQUEST_TTL_SECS: u64 = 15 * 60;

fn new_change_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!(
        "cr-{:x}-{:x}-{:x}",
        nanos,
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

pub async fn create_change_request(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Json(proposal): Json<ChangeProposal>,
) -> ApiResult<(StatusCode, Json<ChangeRequestView>)> {
    let tenant_id = tenant_scope(&caller)?;
    let (service_id, kind) = match &proposal {
        ChangeProposal::Deploy { service_id, .. } => (service_id.clone(), ChangeKind::Deploy),
        ChangeProposal::Scale { service_id, .. } => (service_id.clone(), ChangeKind::Scale),
        ChangeProposal::Rollback { service_id } => (service_id.clone(), ChangeKind::Rollback),
        ChangeProposal::Stop { service_id } => (service_id.clone(), ChangeKind::Stop),
    };
    if service_id.is_empty() || service_id.len() > 400 {
        return Err(bad_request("service_id must be between 1 and 400 bytes"));
    }
    let before = app
        .store
        .get_service(&service_id)
        .await?
        .filter(|service| service.tenant_id == tenant_id)
        .ok_or_else(|| not_found("service", &service_id))?;
    // A global service runs one task per eligible node and ignores
    // `replicas`, so a replica change would be approved but never take effect.
    if matches!(kind, ChangeKind::Scale | ChangeKind::Stop)
        && before.mode == minga_state::types::ServiceMode::Global
    {
        return Err(bad_request(
            "scale and stop apply only to replicated services",
        ));
    }
    let mut after = before.clone();
    match proposal {
        ChangeProposal::Deploy { image, .. } => {
            if before.agent.is_some()
                || image.trim().is_empty()
                || image.len() > 512
                || image.chars().any(char::is_control)
            {
                return Err(bad_request(
                    "deploy requires a container image of 1–512 printable bytes",
                ));
            }
            let mut prior = before.clone();
            prior.previous = None;
            after.previous = Some(Box::new(prior));
            after.image = image;
        }
        ChangeProposal::Scale { replicas, .. } => {
            if !(1..=1000).contains(&replicas) {
                return Err(bad_request("replicas must be between 1 and 1000"));
            }
            after.replicas = replicas;
        }
        ChangeProposal::Rollback { .. } => {
            let Some(mut previous) = before.previous.as_deref().cloned() else {
                return Err(conflict("service has no rollback revision"));
            };
            if previous.check_agent_egress().is_err() {
                return Err(conflict("the rollback revision is no longer a valid spec"));
            }
            let mut current = before.clone();
            current.previous = None;
            previous.previous = Some(Box::new(current));
            after = previous;
        }
        ChangeProposal::Stop { .. } => after.replicas = 0,
    }
    let created_at_unix = unix_now();
    let request = ChangeRequest {
        id: new_change_request_id(),
        tenant_id: tenant_id.to_owned(),
        service_id: service_id.clone(),
        requester: caller.name.clone(),
        created_at_unix,
        expires_at_unix: created_at_unix.saturating_add(CHANGE_REQUEST_TTL_SECS),
        kind,
        before,
        after,
        status: ChangeRequestStatus::Pending,
        reviewer: None,
        decided_at_unix: None,
        audit: Vec::new(),
    };
    let persisted = app.store.create_change_request(request).await?;
    record_change_audit(
        &app,
        tenant_id,
        &caller.name,
        "change_request.created",
        &persisted.id,
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(ChangeRequestView::from(persisted)),
    ))
}

pub async fn list_change_requests(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<ChangeRequestQuery>,
) -> ApiResult<Json<Vec<ChangeRequestView>>> {
    let tenant_id = tenant_scope(&caller)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 100);
    // Reads only this tenant's key range, which holds at most 256 requests.
    let mut requests = app.store.list_change_requests(tenant_id).await?;
    if let Some(status) = query.status {
        requests.retain(|request| request.status == status);
    }
    requests.sort_unstable_by(|left, right| {
        right
            .created_at_unix
            .cmp(&left.created_at_unix)
            .then_with(|| left.id.cmp(&right.id))
    });
    requests.truncate(limit);
    Ok(Json(
        requests.into_iter().map(ChangeRequestView::from).collect(),
    ))
}

/// Appends a change-request audit line after the Raft write committed. A
/// failure is logged, not returned: the write has happened, and a retry
/// would create a second request or hit a terminal one.
async fn record_change_audit(app: &AppState, tenant_id: &str, actor: &str, action: &str, id: &str) {
    let path = format!("/minga/change-requests/{id}");
    if let Err(error) = append_tenant_audit(app, tenant_id, actor, action, &path).await {
        tracing::warn!(%error, tenant_id, action, id, "change request committed but its audit line was not appended");
    }
}

/// The change request from the local replica. A follower can answer a create
/// (which commits on the leader) before applying it, so a miss waits briefly.
/// shortcut: up to 2s of replication lag; past that the 404 is honest for this
/// node, upgrade to a leader read-index if decisions must route through any node.
async fn committed_change_request(
    app: &AppState,
    tenant_id: &str,
    id: &str,
) -> ApiResult<Option<ChangeRequest>> {
    for _ in 0..40 {
        if let Some(request) = app.store.get_change_request(tenant_id, id).await? {
            return Ok(Some(request));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(app.store.get_change_request(tenant_id, id).await?)
}

async fn decide_change_request(
    app: &AppState,
    caller: &Caller,
    id: &str,
    approve: bool,
) -> ApiResult<Json<ChangeRequestView>> {
    let tenant_id = tenant_scope(caller)?;
    let Some(request) = committed_change_request(app, tenant_id, id).await? else {
        return Err(not_found("change request", id));
    };
    // Approval writes the reviewed spec directly, so it passes the same
    // admission as a service update. Raft still re-checks the base spec.
    if approve
        && request.status == ChangeRequestStatus::Pending
        && unix_now() < request.expires_at_unix
    {
        check_service_secrets(app, tenant_id, &request.after).await?;
        ensure_placeable(app, &request.after, Some(&request.service_id)).await?;
    }
    let decided = app
        .store
        .decide_change_request(
            id.to_owned(),
            tenant_id.to_owned(),
            caller.name.clone(),
            approve,
            unix_now(),
        )
        .await?;
    // Raft returns only transitions this decision made, so each audit line
    // is attributed to the reviewer who caused it.
    let (action, refused) = match decided.status {
        ChangeRequestStatus::Approved => ("change_request.approved_and_applied", None),
        ChangeRequestStatus::Rejected => ("change_request.rejected", None),
        ChangeRequestStatus::Expired => ("change_request.expired", Some("change request expired")),
        ChangeRequestStatus::Superseded => (
            "change_request.superseded",
            Some("service changed after this request was created"),
        ),
        ChangeRequestStatus::Pending => {
            return Err(internal("change request decision left it pending"));
        }
    };
    record_change_audit(app, tenant_id, &caller.name, action, id).await;
    if let Some(message) = refused {
        return Err(conflict(message));
    }
    Ok(Json(ChangeRequestView::from(decided)))
}

pub async fn approve_change_request(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> ApiResult<Json<ChangeRequestView>> {
    decide_change_request(&app, &caller, &id, true).await
}

pub async fn reject_change_request(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> ApiResult<Json<ChangeRequestView>> {
    decide_change_request(&app, &caller, &id, false).await
}

/// Updates a tenant's display name and quotas.
pub async fn update_tenant(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
    Json(request): Json<UpdateTenantRequest>,
) -> ApiResult<StatusCode> {
    let current = app
        .store
        .get_tenant(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound("tenant not found".into()))?;
    append_tenant_audit(
        &app,
        &current.id,
        &caller.name,
        "tenant.update.requested",
        &format!("/tenants/{}", current.id),
    )
    .await?;
    app.store
        .update_tenant(id.clone(), request.name, request.quotas)
        .await?;
    let tenant = app
        .store
        .get_tenant(id)
        .await?
        .ok_or_else(|| ApiError::NotFound("tenant not found".into()))?;
    configure_tenant_bus(&app, &tenant).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn configure_tenant_bus(app: &AppState, tenant: &Tenant) -> ApiResult<()> {
    let bus = app.bus.as_ref().ok_or_else(|| {
        ApiError::Unavailable("tenant bus resource manager is not configured".into())
    })?;
    bus.configure_tenant_quota(&tenant.id, &tenant.quotas)
        .await
        .map_err(|error| {
            ApiError::Unavailable(format!(
                "cannot configure bus resources for tenant {}: {error}",
                tenant.id
            ))
        })
}

/// Deletes an empty tenant.
pub async fn delete_tenant(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    append_tenant_audit(
        &app,
        &id,
        &caller.name,
        "tenant.delete.requested",
        &format!("/tenants/{id}"),
    )
    .await?;
    app.store.remove_tenant(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /v{version}/audit` — list only the authenticated tenant's audit trail.
pub async fn list_audit(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<AuditQuery>,
) -> ApiResult<Json<Vec<AuditRecord>>> {
    let tenant_id = tenant_scope(&caller)?;
    let bus = app
        .bus
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("tenant audit service is not configured".into()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let records = bus
        .list_audit(tenant_id, limit)
        .await
        .map_err(|error| ApiError::Unavailable(format!("tenant audit query failed: {error}")))?;
    Ok(Json(records))
}

#[derive(Debug, Deserialize)]
pub(crate) struct AgentUsageQuery {
    customer: Option<String>,
    since_unix_ms: Option<u64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum AgentUsageResponse {
    Tenant { events: Vec<UsageEvent> },
    Cluster { tenants: Vec<TenantUsageTotal> },
}

#[derive(Serialize)]
pub(crate) struct TenantUsageTotal {
    tenant_id: String,
    #[serde(flatten)]
    totals: UsageTotals,
}

/// `GET /v{version}/agent/usage` — tenant events or operator totals.
///
/// Tenant identity is resolved from the authenticated caller, never from a
/// query parameter. Cluster operators receive aggregates without task,
/// customer, or request-level data.
pub(crate) async fn agent_usage(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<AgentUsageQuery>,
) -> ApiResult<Json<AgentUsageResponse>> {
    let bus = app
        .bus
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("agent usage service is not configured".into()))?;
    let since_unix_ms = query.since_unix_ms.unwrap_or(0);
    if caller.cluster_operator {
        if query.customer.is_some() {
            return Err(bad_request(
                "cluster operators can query tenant totals only, not customer usage",
            ));
        }
        let tenants = bus
            .usage_totals(since_unix_ms)
            .await
            .map_err(|error| ApiError::Unavailable(format!("agent usage query failed: {error}")))?
            .into_iter()
            .map(|(tenant_id, totals)| TenantUsageTotal { tenant_id, totals })
            .collect();
        return Ok(Json(AgentUsageResponse::Cluster { tenants }));
    }

    let tenant_id = tenant_scope(&caller)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let mut events = bus
        .usage_events(tenant_id, since_unix_ms, query.customer.as_deref(), limit)
        .await
        .map_err(|error| ApiError::Unavailable(format!("agent usage query failed: {error}")))?;
    events.truncate(limit);
    Ok(Json(AgentUsageResponse::Tenant { events }))
}
#[derive(Debug, Deserialize)]
pub(crate) struct AgentEventsQuery {
    limit: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct TenantEventSummary {
    subject: String,
    observed_at_unix_ms: u64,
    payload_size_bytes: usize,
}

/// `GET /v{version}/agent/events` — bounded tenant event metadata.
pub(crate) async fn agent_events(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Query(query): Query<AgentEventsQuery>,
) -> ApiResult<Json<Vec<TenantEventSummary>>> {
    let tenant_id = tenant_scope(&caller)?;
    let bus = app
        .bus
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("tenant event service is not configured".into()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let events = bus
        .tenant_event_history(tenant_id, limit)
        .await
        .map_err(|_| ApiError::Unavailable("tenant event query failed".into()))?;
    Ok(Json(
        events
            .into_iter()
            .filter(|event| {
                event
                    .subject
                    .starts_with(&format!("minga.{tenant_id}.task."))
            })
            .map(|event| TenantEventSummary {
                subject: event.subject,
                observed_at_unix_ms: event.timestamp,
                payload_size_bytes: event.payload.len(),
            })
            .collect(),
    ))
}

/// `GET /minga/audit` — list the cluster audit trail for operators.
pub async fn list_cluster_audit(
    State(app): State<Arc<AppState>>,
    Query(query): Query<AuditQuery>,
) -> ApiResult<Json<Vec<AuditRecord>>> {
    let bus = app
        .bus
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("tenant audit service is not configured".into()))?;
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let mut records = Vec::new();
    for tenant in app.store.list_tenants().await? {
        records.extend(bus.list_audit(&tenant.id, limit).await.map_err(|error| {
            ApiError::Unavailable(format!("tenant audit query failed: {error}"))
        })?);
    }
    records.sort_by(|left, right| {
        right
            .occurred_at_unix
            .cmp(&left.occurred_at_unix)
            .then_with(|| left.tenant_id.cmp(&right.tenant_id))
    });
    records.truncate(limit);
    Ok(Json(records))
}

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    limit: Option<usize>,
}
fn task_status_for_api<'a>(
    app: &AppState,
    task: &Task,
    statuses: &'a HashMap<String, HashMap<String, TaskStatusReport>>,
) -> Option<Cow<'a, TaskStatusReport>> {
    if let Some(report) = statuses
        .get(&task.tenant_id)
        .and_then(|tenant_statuses| tenant_statuses.get(&task.id))
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
    Json(mut body): Json<serde_json::Value>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant_id = tenant_scope(&caller)?;
    // Check the version before the shape: a newer file may not parse as ours.
    // Older versions the store migrates on import are accepted.
    let oldest = minga_state::codec::V2_FORMAT_VERSION;
    let want = minga_state::codec::FORMAT_VERSION;
    let version = match body.get("format_version").and_then(|v| v.as_u64()) {
        Some(v) if (u64::from(oldest)..=u64::from(want)).contains(&v) => v,
        other => {
            return Err(bad_request(format!(
                "snapshot has format version {}, this build reads {oldest} to {want}",
                other.map_or("none".to_string(), |v| v.to_string())
            )))
        }
    };
    // v9 snapshots always carry capabilities, so an omitted one means none,
    // as in the agent label; only older ones get the migration's `env`.
    if version > u64::from(minga_state::codec::V8_FORMAT_VERSION) {
        // Serde decodes arrays by position, so non-objects would skip this.
        fn deny_omitted(service: &mut serde_json::Value) -> Result<(), String> {
            let service = service
                .as_object_mut()
                .ok_or_else(|| "each service must be a JSON object".to_owned())?;
            if let Some(agent) = service.get_mut("agent").filter(|a| !a.is_null()) {
                crate::translate::deny_omitted_capabilities(agent)?;
            }
            match service.get_mut("previous") {
                Some(previous) if !previous.is_null() => deny_omitted(previous),
                _ => Ok(()),
            }
        }
        if let Some(services) = body.get_mut("services").and_then(|s| s.as_array_mut()) {
            for service in services {
                deny_omitted(service).map_err(bad_request)?;
            }
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
        // The same admission checks as a deploy, so a bad agent is refused
        // here instead of crash-looping on its node. Rollback targets in
        // `previous` are checked too, secrets and route secrets included.
        let mut secret_ids = Vec::new();
        let mut spec = Some(service);
        while let Some(current) = spec {
            if let Some(agent) = &current.agent {
                crate::translate::check_agent(agent)
                    .map_err(|e| bad_request(format!("service '{}': {e}", service.id)))?;
            }
            current
                .check_agent_egress()
                .map_err(|e| bad_request(format!("service '{}': {e}", service.id)))?;
            if let Some(policy) = &current.egress {
                crate::translate::check_egress_policy(policy).map_err(|e| {
                    bad_request(format!(
                        "service '{}': invalid egress policy: {e}",
                        service.id
                    ))
                })?;
            }
            secret_ids.extend(
                current
                    .secrets
                    .iter()
                    .map(|reference| reference.secret_id.as_str()),
            );
            secret_ids.extend(current.egress.iter().flat_map(|policy| policy.secret_ids()));
            spec = current.previous.as_deref();
        }
        for secret_id in secret_ids {
            let in_snapshot = snapshot.secrets.iter().any(|secret| secret.id == secret_id);
            let in_store = app
                .store
                .get_secret(secret_id)
                .await?
                .is_some_and(|secret| secret.tenant_id == tenant_id);
            if !in_snapshot && !in_store {
                return Err(not_found("secret", secret_id));
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

/// Every secret a service names, in `secrets` or an egress route, must be
/// declared and owned by the caller's tenant; another tenant's is a 404.
async fn check_service_secrets(
    app: &AppState,
    tenant_id: &str,
    svc: &StateService,
) -> ApiResult<()> {
    let egress_secrets = svc.egress.iter().flat_map(|policy| policy.secret_ids());
    for secret_id in svc
        .secrets
        .iter()
        .map(|reference| reference.secret_id.as_str())
        .chain(egress_secrets)
    {
        match app.store.get_secret(secret_id).await? {
            Some(secret) if secret.tenant_id == tenant_id => {}
            Some(_) => return Err(not_found("secret", secret_id)),
            None => {
                return Err(bad_request(format!(
                    "secret `{secret_id}` is not declared; create its metadata and provision it on eligible nodes first"
                )))
            }
        }
    }
    Ok(())
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
            let status = statuses
                .get(&task.tenant_id)
                .and_then(|tenant_statuses| tenant_statuses.get(&task.id));
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
    let tenants = app.store.list_tenants().await?;
    let placement_nodes: Vec<_> = nodes
        .iter()
        .filter(|node| tenants.len() < 2 || node.role != minga_state::types::NodeRole::Manager)
        .cloned()
        .collect();
    minga_scheduler::check_placeable(svc, &placement_nodes, &tasks, &networks, unix_now()).map_err(
        |_| conflict("service cannot be placed with current cluster capacity or constraints"),
    )
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
    // An ingress port opens on every node of the service's tenant — the
    // scheduler's ingress fan-out is per-tenant — so only a same-tenant
    // node's reservation may block it. A port another tenant reserved on
    // its own node never sees this service's traffic.
    for port in &service.ports {
        let Some(published) = port.published else {
            continue;
        };
        if port.mode != minga_state::types::PortMode::Ingress {
            continue;
        }
        for node in nodes
            .iter()
            .filter(|node| node.tenant_id == service.tenant_id)
        {
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

/// Returns the running Minga build and the persisted/local Raft observations.
pub async fn get_cluster_diagnostics(
    State(app): State<Arc<AppState>>,
) -> Json<models::ClusterDiagnostics> {
    Json(models::ClusterDiagnostics {
        server_version: models::MINGA_VERSION.to_owned(),
        build_revision: models::MINGA_GIT_COMMIT.map(str::to_owned),
        api_version: models::API_VERSION.to_owned(),
        state_format_version: minga_state::codec::FORMAT_VERSION,
        raft: app.store.raft_diagnostics().await,
    })
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
        .filter(|service| tenant_id.is_none_or(|tenant_id| service.tenant_id == tenant_id))
        .collect();
    let tasks: Vec<_> = app
        .store
        .list_tasks()
        .await?
        .into_iter()
        .filter(|task| tenant_id.is_none_or(|tenant_id| task.tenant_id == tenant_id))
        .collect();
    let statuses = app.task_statuses().await?;

    let running = tasks
        .iter()
        .filter(|task| {
            statuses
                .get(&task.tenant_id)
                .and_then(|tenant_statuses| tenant_statuses.get(&task.id))
                .is_some_and(|status| {
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
    Extension(caller): Extension<Caller>,
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
        .filter(|node| {
            caller
                .tenant_id()
                .is_none_or(|tenant_id| node.tenant_id == tenant_id)
        })
        .filter(|node| models::labels_match(&node.labels, &label_filters))
        .map(|node| {
            translate::node_to_docker(
                node,
                Some(node.id) == leader_id || (node.id == my_id && is_leader),
            )
        })
        .collect();

    Ok(Json(result))
}

/// `GET /v{version}/nodes/{id}` — inspect a node in the caller's tenant.
pub async fn get_node(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<Json<models::Node>> {
    let nodes = app.store.list_nodes().await?;
    let leader_id = app.store.current_leader().await;

    let node = nodes
        .iter()
        .find(|node| {
            (node.id.to_string() == id || node.address == id)
                && caller
                    .tenant_id()
                    .is_none_or(|tenant_id| node.tenant_id == tenant_id)
        })
        .ok_or_else(|| not_found("node", &id))?;

    let is_leader = Some(node.id) == leader_id;
    Ok(Json(translate::node_to_docker(node, is_leader)))
}

/// `POST /v{version}/nodes` — register a node out-of-band (Minga extension).
pub async fn create_node(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<models::NodeCreateRequest>,
) -> ApiResult<(StatusCode, Json<models::Node>)> {
    if req.address.trim().is_empty() {
        return Err(bad_request("node address must not be empty"));
    }
    let tenant_id = req
        .tenant_id
        .unwrap_or_else(|| minga_state::types::DEFAULT_TENANT_ID.to_owned());
    minga_state::types::Tenant::validate_id(&tenant_id).map_err(bad_request)?;
    if app.store.get_tenant(&tenant_id).await?.is_none() {
        return Err(not_found("tenant", &tenant_id));
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
        tenant_id,
        // Operator-registered nodes carry no lease: they stay eligible
        // until explicitly removed or drained.
        last_seen_unix: None,
        role: Default::default(),
        reserved_ports: Vec::new(),
        nats_user_public_key: None,
    };
    append_tenant_audit(
        &app,
        &node.tenant_id,
        &caller.name,
        "node.create.requested",
        &format!("/nodes/{}", node.id),
    )
    .await?;
    app.store.upsert_node(node.clone()).await?;
    tracing::info!(node = id, address = %node.address, "node registered via API");

    let is_leader = app.store.current_leader().await == Some(id);
    Ok((
        StatusCode::CREATED,
        Json(translate::node_to_docker(&node, is_leader)),
    ))
}

/// Serializes join-token issue, node removal and revocation publishing. Each
/// reads a node's NATS key or the revocation list and then writes; interleaved,
/// a removal can miss a freshly issued key or an older account claim can
/// overwrite a newer one and un-revoke a user.
/// shortcut: process-local; revocation runs only on the Raft leader, so move
/// this into Raft if credential issuance ever runs on followers.
pub(crate) static NODE_CREDENTIALS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `POST /minga/nodes/{id}/join-token` — mint a tenant-bound token and NATS credentials.
pub async fn issue_node_join_token(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> ApiResult<Json<models::NodeJoinToken>> {
    let node_id = id.parse::<u64>().map_err(|_| not_found("node", &id))?;
    let credentials_guard = NODE_CREDENTIALS.lock().await;
    let node = app
        .store
        .get_node(node_id)
        .await?
        .ok_or_else(|| not_found("node", &id))?;
    let key = app.join_token_key.as_ref().ok_or_else(|| {
        crate::error::ApiError::Unavailable("node join-token signer is not configured".into())
    })?;
    let issuer = app.nats_credential_issuer.as_ref().ok_or_else(|| {
        crate::error::ApiError::Unavailable("tenant NATS signer is not configured".into())
    })?;
    append_tenant_audit(
        &app,
        &node.tenant_id,
        &caller.name,
        "node.join-token.issue.requested",
        &format!("/nodes/{}", node.id),
    )
    .await?;
    let issued_at = unix_now();
    if let Some(previous_key) = node.nats_user_public_key.as_deref() {
        revoke_nats_user(&app, previous_key, &credentials_guard).await?;
    }
    let (user_public_key, credentials) =
        issuer.issue(&node.tenant_id, node.id).map_err(internal)?;
    let expires_at = issued_at.saturating_add(15 * 60);
    let token = key
        .issue(node.id, node.tenant_id.clone(), issued_at, expires_at)
        .map_err(|error| internal(error.to_string()))?;
    app.store
        .set_node_nats_user_public_key(node.id, Some(user_public_key))
        .await?;
    tracing::info!(
        node = node.id,
        tenant = %node.tenant_id,
        expires_at,
        "node join token and NATS credentials issued"
    );
    Ok(Json(models::NodeJoinToken {
        node_id: node.id,
        tenant_id: node.tenant_id,
        expires_at,
        token,
        credentials,
    }))
}

/// `DELETE /v{version}/nodes/{id}` — revoke bus identity before removing a node.
pub async fn remove_node(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let node_id = id.parse::<u64>().map_err(|_| not_found("node", &id))?;
    // Read the key under the lock so a concurrent join-token issue cannot
    // leave a key this removal never revokes.
    let credentials_guard = NODE_CREDENTIALS.lock().await;
    let node = app
        .store
        .get_node(node_id)
        .await?
        .ok_or_else(|| not_found("node", &id))?;
    append_tenant_audit(
        &app,
        &node.tenant_id,
        &caller.name,
        "node.remove.requested",
        &format!("/nodes/{node_id}"),
    )
    .await?;
    if let Some(public_key) = node.nats_user_public_key.as_deref() {
        revoke_nats_user(&app, public_key, &credentials_guard).await?;
    }
    app.store.remove_node(node_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn revoke_nats_user(
    app: &AppState,
    public_key: &str,
    guard: &tokio::sync::MutexGuard<'_, ()>,
) -> ApiResult<()> {
    if !app.store.is_leader().await {
        return Err(crate::error::ApiError::Unavailable(
            "NATS credential revocation must run on the Raft leader".into(),
        ));
    }
    let issuer = app.nats_credential_issuer.as_ref().ok_or_else(|| {
        crate::error::ApiError::Unavailable("tenant NATS signer is not configured".into())
    })?;
    if app.bus.is_none() {
        return Err(crate::error::ApiError::Unavailable(
            "NATS revocation bus is not configured".into(),
        ));
    }
    issuer
        .validate_user_public_key(public_key)
        .map_err(internal)?;
    app.store
        .record_nats_user_revocation(public_key.to_owned())
        .await?;
    publish_nats_revocations(app, guard).await
}

/// Publishes the account claim carrying every revocation stored in Raft. The
/// leader also runs this when it takes over, so a manager whose resolver
/// lost the claim cannot accept a revoked node's credentials again.
/// One leader-probe tick: republish the revocations once per leadership term,
/// retrying on the next tick after a failure. Returns whether this term's
/// revocations are published.
pub(crate) async fn sync_nats_revocations(
    app: &AppState,
    is_leader: bool,
    published: bool,
) -> bool {
    if !is_leader || app.nats_credential_issuer.is_none() || app.bus.is_none() {
        return false;
    }
    if published {
        return true;
    }
    let guard = NODE_CREDENTIALS.lock().await;
    match publish_nats_revocations(app, &guard).await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(%error, "cannot republish NATS credential revocations; retrying");
            false
        }
    }
}

pub(crate) async fn publish_nats_revocations(
    app: &AppState,
    _guard: &tokio::sync::MutexGuard<'_, ()>,
) -> ApiResult<()> {
    let issuer = app.nats_credential_issuer.as_ref().ok_or_else(|| {
        crate::error::ApiError::Unavailable("tenant NATS signer is not configured".into())
    })?;
    let bus = app.bus.as_ref().ok_or_else(|| {
        crate::error::ApiError::Unavailable("NATS revocation bus is not configured".into())
    })?;
    let revoked_users = app
        .store
        .list_tenants()
        .await?
        .into_iter()
        .flat_map(|tenant| tenant.nats_revoked_user_public_keys)
        .collect::<Vec<_>>();
    if revoked_users.is_empty() {
        return Ok(());
    }
    let account_jwt = issuer
        .account_claim_with_revocations(&revoked_users, unix_now())
        .map_err(internal)?;
    bus.update_account_claim(&account_jwt)
        .await
        .map_err(|error| {
            crate::error::ApiError::Unavailable(format!(
                "cannot publish node credential revocation: {error}"
            ))
        })?;
    Ok(())
}
/// `POST /v{version}/nodes/{id}/update` — update labels and availability.
///
/// Only what Minga can honestly do is accepted: label replacement and
/// `active`/`drain` availability. Role is derived from Raft leadership and
/// names are `node-{id}`; requests to change either fail loudly.
pub async fn update_node(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
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

    append_tenant_audit(
        &app,
        &node.tenant_id,
        &caller.name,
        "node.update.requested",
        &format!("/nodes/{}", node.id),
    )
    .await?;
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
    check_service_secrets(&app, tenant_id, &svc).await?;
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
        app.store
            .remove_task(task.id.clone(), &task.tenant_id)
            .await?;
    }

    app.store
        .remove_service(svc.id.clone(), &svc.tenant_id)
        .await?;

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
    // A Docker client that drops the egress label must not drop the policy.
    if svc.egress.is_none() {
        svc.egress = existing.egress.clone();
        translate::sync_required_egress_label(&mut svc);
    }
    svc.check_agent_egress()
        .map_err(|e| bad_request(format!("service '{id}': {e}")))?;
    check_service_secrets(&app, tenant_id, &svc).await?;
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
    // A spec this update would refuse cannot become its rollback target.
    if svc
        .previous
        .as_deref()
        .is_some_and(|previous| previous.check_agent_egress().is_err())
    {
        svc.previous = None;
    }

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

#[derive(Debug, Deserialize)]
pub struct TaskLogsQuery {
    tail: Option<usize>,
}

/// `GET /v{version}/tasks/{id}/logs` — read retained logs in the caller's tenant.
pub async fn task_logs(
    State(app): State<Arc<AppState>>,
    Extension(caller): Extension<Caller>,
    Path((_version, task_id)): Path<(String, String)>,
    Query(query): Query<TaskLogsQuery>,
) -> ApiResult<Json<Vec<minga_bus::LogEntry>>> {
    let tenant_id = tenant_scope(&caller)?;
    let task = app
        .store
        .get_task(&task_id)
        .await?
        .filter(|task| task.tenant_id == tenant_id)
        .ok_or_else(|| not_found("task", &task_id))?;
    if !app
        .store
        .get_service(&task.service_id)
        .await?
        .is_some_and(|service| service.tenant_id == tenant_id)
    {
        return Err(not_found("task", &task_id));
    }
    let bus = app
        .bus
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("task log service is not configured".into()))?;
    let tail = query.tail.unwrap_or(100).clamp(1, 200);
    let entries = bus
        .tenant_log_history(tenant_id, &task_id, tail)
        .await
        .map_err(|_| ApiError::Unavailable("task log query failed".into()))?;
    Ok(Json(entries))
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
    // An explicit subnet must stay inside the overlay and not overlap another
    // network's. Networks without one get theirs from the leader's next
    // reconcile pass.
    if let Some(raw) = &net.subnet {
        let subnet = minga_net::Cidr::parse(raw).map_err(|e| bad_request(e.to_string()))?;
        let supernet = app.store.overlay_supernet().unwrap_or_else(|| {
            minga_net::Cidr::parse(minga_net::DEFAULT_SUPERNET).expect("default supernet parses")
        });
        minga_net::overlay::check_network_subnet(&subnet, Some(&supernet)).map_err(bad_request)?;
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

    app.store
        .remove_network(net.id.clone(), &net.tenant_id)
        .await?;

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

    app.store
        .remove_secret(secret.id.clone(), &secret.tenant_id)
        .await?;

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

    app.store
        .remove_config(config.id.clone(), &config.tenant_id)
        .await?;

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
    use axum::response::IntoResponse;
    use minga_bus::MemoryBus;
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
                tenant_id: minga_state::types::DEFAULT_TENANT_ID.to_owned(),
                last_seen_unix: None,
                role: Default::default(),
                reserved_ports: Vec::new(),
                nats_user_public_key: None,
            })
            .await
            .unwrap();
        (Arc::new(AppState::with_bus(store, MemoryBus::new())), dir)
    }

    #[tokio::test]
    async fn cluster_audit_merges_tenant_trails_by_time() {
        let (app, _dir) = app_with_node(1.0).await;
        app.store
            .create_tenant(Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "acme".into(),
                name: "Acme".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        let bus = app.bus.as_ref().unwrap();
        for (tenant_id, occurred_at_unix) in [("default", 10), ("acme", 20)] {
            bus.append_audit(AuditRecord {
                tenant_id: tenant_id.into(),
                actor: "operator".into(),
                action: "tenant.update".into(),
                resource_path: format!("/tenants/{tenant_id}"),
                occurred_at_unix,
            })
            .await
            .unwrap();
        }

        let Json(records) = list_cluster_audit(State(app), Query(AuditQuery { limit: Some(1) }))
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tenant_id, "acme");
    }

    #[tokio::test]
    async fn agent_usage_is_tenant_scoped_and_operator_output_is_aggregate_only() {
        let (app, _dir) = app_with_node(1.0).await;
        let bus = app.bus.as_ref().unwrap();
        for (tenant_id, customer_id, request_id) in [
            ("default", "customer-a", "request-a"),
            ("acme", "customer-b", "request-b"),
        ] {
            bus.publish_usage(&UsageEvent {
                request_id: request_id.into(),
                tenant_id: tenant_id.into(),
                service_id: "inference".into(),
                task_id: format!("{tenant_id}.task"),
                customer_id: customer_id.into(),
                route: "primary".into(),
                requested_model: "model-a".into(),
                answered_model: "model-a".into(),
                input_tokens: 3,
                output_tokens: 5,
                cache_hit: false,
                latency_ms: 10,
                status: 200,
                occurred_at_unix_ms: 100,
                cost_micros: Some(7),
            })
            .await
            .unwrap();
        }

        let tenant_caller = Extension(Caller {
            name: "tenant".into(),
            role: crate::auth::ApiRole::Admin,
            tenant_id: Some("default".into()),
            cluster_operator: false,
        });
        let Json(AgentUsageResponse::Tenant { events }) = agent_usage(
            State(app.clone()),
            tenant_caller,
            Query(AgentUsageQuery {
                customer: None,
                since_unix_ms: Some(0),
                limit: Some(100),
            }),
        )
        .await
        .unwrap() else {
            panic!("tenant caller must receive usage events");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant_id, "default");

        let operator = Extension(Caller {
            name: "operator".into(),
            role: crate::auth::ApiRole::Admin,
            tenant_id: None,
            cluster_operator: true,
        });
        let Json(AgentUsageResponse::Cluster { tenants }) = agent_usage(
            State(app.clone()),
            operator.clone(),
            Query(AgentUsageQuery {
                customer: None,
                since_unix_ms: Some(0),
                limit: None,
            }),
        )
        .await
        .unwrap() else {
            panic!("cluster operator must receive aggregate totals");
        };
        assert_eq!(tenants.len(), 2);
        let json = serde_json::to_value(&tenants[0]).unwrap();
        assert!(json.get("request_count").is_some());
        assert!(json.get("totals").is_none());
        assert!(json.get("customer_id").is_none());

        assert!(agent_usage(
            State(app),
            operator,
            Query(AgentUsageQuery {
                customer: Some("customer-a".into()),
                since_unix_ms: Some(0),
                limit: None,
            }),
        )
        .await
        .is_err());
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
    async fn operator_registered_node_accepts_tenant_assignment_and_defaults_to_default() {
        let (app, _dir) = app_with_node(1.0).await;
        app.store
            .create_tenant(minga_state::types::Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "acme".into(),
                name: "Acme".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();

        let tenant_request = serde_json::from_value(serde_json::json!({
            "id": 2,
            "address": "127.0.0.1:50052",
            "tenant_id": "acme"
        }))
        .unwrap();
        let _response = create_node(State(app.clone()), default_caller(), Json(tenant_request))
            .await
            .unwrap();
        let registered = app.store.get_node(2).await.unwrap().unwrap();
        assert_eq!(registered.tenant_id, "acme");

        let default_request = serde_json::from_value(serde_json::json!({
            "id": 3,
            "address": "127.0.0.1:50053"
        }))
        .unwrap();
        let _response = create_node(State(app.clone()), default_caller(), Json(default_request))
            .await
            .unwrap();
        let registered = app.store.get_node(3).await.unwrap().unwrap();
        assert_eq!(registered.tenant_id, minga_state::types::DEFAULT_TENANT_ID);

        let unknown_tenant_request = serde_json::from_value(serde_json::json!({
            "id": 4,
            "address": "127.0.0.1:50054",
            "tenant_id": "missing"
        }))
        .unwrap();
        assert!(matches!(
            create_node(
                State(app.clone()),
                default_caller(),
                Json(unknown_tenant_request)
            )
            .await,
            Err(ApiError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn deleting_node_revokes_its_bus_user_before_removing_it() {
        let (base, _dir) = app_with_node(1.0).await;
        let issuer = Arc::new(crate::nats_credentials::test_issuer("default"));
        let (public_key, _) = issuer.issue("default", 1).unwrap();
        base.store
            .set_node_nats_user_public_key(1, Some(public_key.clone()))
            .await
            .unwrap();
        let bus = minga_bus::memory::MemoryBus::new();
        let app = Arc::new(
            AppState::with_bus(base.store.clone(), bus).with_nats_credential_issuer(issuer),
        );

        assert_eq!(
            remove_node(
                State(app.clone()),
                default_caller(),
                Path(("v1.41".into(), "1".into())),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(app.store.get_node(1).await.unwrap().is_none());
        assert!(app
            .store
            .get_tenant("default")
            .await
            .unwrap()
            .unwrap()
            .nats_revoked_user_public_keys
            .contains(&public_key));
    }

    #[tokio::test]
    async fn new_leader_republishes_stored_revocations_once_per_term() {
        use nats_jwt_rs::{account::Account, Claims};

        let (base, _dir) = app_with_node(1.0).await;
        let issuer = Arc::new(crate::nats_credentials::test_issuer("default"));
        let (public_key, _) = issuer.issue("default", 1).unwrap();
        base.store
            .record_nats_user_revocation(public_key.clone())
            .await
            .unwrap();
        let bus = minga_bus::memory::MemoryBus::new();
        let app =
            AppState::with_bus(base.store.clone(), bus.clone()).with_nats_credential_issuer(issuer);

        assert!(!sync_nats_revocations(&app, false, false).await);
        assert!(bus.account_claim_updates().is_empty());
        assert!(sync_nats_revocations(&app, true, false).await);
        assert!(sync_nats_revocations(&app, true, true).await);
        let updates = bus.account_claim_updates();
        assert_eq!(updates.len(), 1, "one publish per leadership term");
        let claim = Claims::<Account>::decode(&updates[0]).unwrap();
        assert!(claim.nats.revocations.unwrap().contains_key(&public_key));
    }

    #[tokio::test]
    async fn deleting_node_fails_closed_when_revocation_bus_is_missing() {
        let (base, _dir) = app_with_node(1.0).await;
        let issuer = Arc::new(crate::nats_credentials::test_issuer("default"));
        let (public_key, _) = issuer.issue("default", 1).unwrap();
        base.store
            .set_node_nats_user_public_key(1, Some(public_key.clone()))
            .await
            .unwrap();
        let app = Arc::new(AppState::new(base.store.clone()).with_nats_credential_issuer(issuer));

        {
            let guard = NODE_CREDENTIALS.lock().await;
            assert!(matches!(
                revoke_nats_user(&app, &public_key, &guard).await,
                Err(ApiError::Unavailable(_))
            ));
        }
        assert!(matches!(
            remove_node(
                State(app.clone()),
                default_caller(),
                Path(("v1.41".into(), "1".into())),
            )
            .await,
            Err(ApiError::Unavailable(_))
        ));
        assert!(app.store.get_node(1).await.unwrap().is_some());
    }
    #[tokio::test]
    async fn reissuing_node_credentials_revokes_the_previous_nkey() {
        let (base, _dir) = app_with_node(1.0).await;
        let issuer = Arc::new(crate::nats_credentials::test_issuer("default"));
        let key = Arc::new(JoinTokenKey::from_bytes(&[9; 32]).unwrap());
        let bus = minga_bus::memory::MemoryBus::new();
        let app = Arc::new(
            AppState::with_bus(base.store.clone(), bus)
                .with_join_token_key(key)
                .with_nats_credential_issuer(issuer),
        );

        let Json(first) =
            issue_node_join_token(State(app.clone()), default_caller(), Path("1".into()))
                .await
                .unwrap();
        let old_public_key = app
            .store
            .get_node(1)
            .await
            .unwrap()
            .unwrap()
            .nats_user_public_key
            .unwrap();
        let Json(second) =
            issue_node_join_token(State(app.clone()), default_caller(), Path("1".into()))
                .await
                .unwrap();

        assert_ne!(first.credentials, second.credentials);
        assert_eq!(
            app.store
                .get_tenant("default")
                .await
                .unwrap()
                .unwrap()
                .nats_revoked_user_public_keys,
            vec![old_public_key.clone()]
        );
        let new_public_key = app
            .store
            .get_node(1)
            .await
            .unwrap()
            .unwrap()
            .nats_user_public_key
            .unwrap();
        assert_ne!(new_public_key, old_public_key);
    }
    #[tokio::test]
    async fn node_join_token_is_signed_for_the_preregistered_tenant() {
        let (mut app, _dir) = app_with_node(1.0).await;
        let key = Arc::new(JoinTokenKey::from_bytes(&[5; 32]).unwrap());
        Arc::get_mut(&mut app).unwrap().join_token_key = Some(key.clone());
        Arc::get_mut(&mut app).unwrap().nats_credential_issuer =
            Some(Arc::new(crate::nats_credentials::test_issuer("acme")));
        app.store
            .create_tenant(Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "acme".into(),
                name: "Acme".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        let request = serde_json::from_value(serde_json::json!({
            "id": 2,
            "address": "127.0.0.1:50052",
            "tenant_id": "acme"
        }))
        .unwrap();
        let _ = create_node(State(app.clone()), default_caller(), Json(request))
            .await
            .unwrap();

        let Json(response) =
            issue_node_join_token(State(app.clone()), default_caller(), Path("2".into()))
                .await
                .unwrap();
        let claims = key
            .verify(&response.token, 2, minga_state::types::unix_now())
            .unwrap();
        assert!(response.credentials.contains("BEGIN NATS USER JWT"));
        let registered = app.store.get_node(2).await.unwrap().unwrap();
        assert!(registered.nats_user_public_key.is_some());
        assert_eq!(response.tenant_id, "acme");
        assert_eq!(claims.tenant_id, "acme");
        assert_eq!(claims.node_id, response.node_id);
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
    async fn network_subnet_outside_the_overlay_is_refused() {
        let (app, _dir) = app_with_node(1.0).await;
        let ipam = |s: &str| serde_json::json!({"IPAM": {"Config": [{"Subnet": s}]}});
        for (name, subnet) in [
            ("outside", "192.168.1.0/24"),
            ("wide", "10.0.0.0/8"),
            ("mesh", "10.200.0.0/24"),
            ("host", "10.100.5.7/32"),
            ("tiny", "10.100.5.0/29"),
        ] {
            let err = create_network(
                State(app.clone()),
                default_caller(),
                net_req(name, ipam(subnet)),
            )
            .await
            .unwrap_err();
            assert!(matches!(err, ApiError::BadRequest(_)), "{subnet}: {err:?}");
            assert!(app.store.get_network(name).await.unwrap().is_none());
        }
        let _ = create_network(
            State(app.clone()),
            default_caller(),
            net_req("inside", ipam("10.100.5.0/28")),
        )
        .await
        .expect("a /28 inside the supernet");
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

    fn egress_policy(secret: &str) -> minga_state::types::EgressPolicy {
        minga_state::types::EgressPolicy {
            required: false,
            routes: vec![minga_state::types::EgressRoute {
                name: "primary".into(),
                upstream_url: "https://api.example".into(),
                auth: Some(minga_state::types::EgressAuth::AwsSigv4 {
                    access_key_secret: "default-key".into(),
                    secret_key_secret: "default-key".into(),
                    session_token_secret: Some(secret.into()),
                    region: "us-east-1".into(),
                    service: "bedrock".into(),
                }),
                fallback: vec![],
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            }],
            models: vec![],
        }
    }

    #[tokio::test]
    async fn egress_route_secrets_of_another_tenant_are_not_found() {
        let (app, _dir) = app_with_node(1.0).await;
        let mut node = app.store.get_node(1).await.unwrap().unwrap();
        node.labels.insert(
            minga_state::types::EGRESS_PROXY_NODE_LABEL.into(),
            "true".into(),
        );
        app.store.upsert_node(node).await.unwrap();
        app.store
            .create_tenant(Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "acme".into(),
                name: "Acme".into(),
                quotas: Default::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        for (id, tenant_id) in [("default-key", "default"), ("acme-key", "acme")] {
            app.store
                .upsert_secret(minga_state::types::Secret {
                    id: id.into(),
                    tenant_id: tenant_id.into(),
                    name: id.into(),
                })
                .await
                .unwrap();
        }

        // Snapshot import.
        let mut svc = translate::service_spec_to_state(&spec("web", 0)).unwrap();
        svc.egress = Some(egress_policy("acme-key"));
        let snapshot = |svc: &StateService| {
            serde_json::to_value(minga_state::StateSnapshot {
                format_version: minga_state::codec::FORMAT_VERSION,
                exported_at_unix: 0,
                tenants: vec![],
                services: vec![svc.clone()],
                networks: vec![],
                volumes: vec![],
                secrets: vec![],
                configs: vec![],
            })
            .unwrap()
        };
        let err = import_state(
            State(app.clone()),
            default_caller(),
            Query(ImportQuery { overwrite: false }),
            Json(snapshot(&svc)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApiError::NotFound(_)), "{err:?}");
        assert!(app.store.get_service("web").await.unwrap().is_none());

        // Docker API create carries the policy in its label.
        let mut docker = translate::service_to_docker(&svc).Spec;
        let err = create_service(State(app.clone()), default_caller(), Json(docker.clone()))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotFound(_)), "{err:?}");

        // The caller's own secrets pass the ownership check.
        svc.egress = Some(egress_policy("default-key"));
        let _ = import_state(
            State(app.clone()),
            default_caller(),
            Query(ImportQuery { overwrite: false }),
            Json(snapshot(&svc)),
        )
        .await
        .expect("own egress secrets import");

        // An update without the egress label keeps the stored policy.
        docker.Labels.remove(translate::EGRESS_LABEL);
        let _ = update_service(
            State(app.clone()),
            default_caller(),
            Path(("v1.41".to_string(), "web".to_string())),
            Query(
                serde_json::from_value::<models::ServiceUpdateQuery>(serde_json::json!({}))
                    .unwrap(),
            ),
            Json(docker),
        )
        .await
        .expect("update without the egress label");
        assert_eq!(
            app.store.get_service("web").await.unwrap().unwrap().egress,
            svc.egress
        );

        // An agent cannot carry the policy, neither inherited on update nor
        // imported.
        let mut agent = svc.clone();
        agent.agent = Some(Box::new(minga_state::types::AgentSpec::default()));
        let mut docker = translate::service_to_docker(&agent).Spec;
        docker.Labels.remove(translate::EGRESS_LABEL);
        let err = update_service(
            State(app.clone()),
            default_caller(),
            Path(("v1.41".to_string(), "web".to_string())),
            Query(
                serde_json::from_value::<models::ServiceUpdateQuery>(serde_json::json!({}))
                    .unwrap(),
            ),
            Json(docker),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "{err:?}");
        assert!(app
            .store
            .get_service("web")
            .await
            .unwrap()
            .unwrap()
            .agent
            .is_none());
        // Import applies the deploy rules to the policy itself.
        let mut long = svc.clone();
        long.id = "long".into();
        long.name = "long".into();
        long.egress.as_mut().unwrap().routes[0].name = "r".repeat(257);
        let err = import_state(
            State(app.clone()),
            default_caller(),
            Query(ImportQuery { overwrite: false }),
            Json(snapshot(&long)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "{err:?}");
        assert!(app.store.get_service("long").await.unwrap().is_none());

        agent.id = "bot".into();
        agent.name = "bot".into();
        let err = import_state(
            State(app.clone()),
            default_caller(),
            Query(ImportQuery { overwrite: false }),
            Json(snapshot(&agent)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "{err:?}");
        assert!(app.store.get_service("bot").await.unwrap().is_none());
    }
    #[tokio::test]
    async fn create_and_update_refuse_unimplemented_hardening_json_with_bad_request() {
        let (app, _dir) = app_with_node(1.0).await;
        let cases = [
            ("read-only", "ReadOnly", serde_json::json!(true)),
            (
                "privileges",
                "Privileges",
                serde_json::json!({"CredentialSpec": {"Config": "credentialspec.json"}}),
            ),
            (
                "privileges-seccomp",
                "Privileges",
                serde_json::json!({"Seccomp": {"Mode": "custom", "Profile": "{}"}}),
            ),
            (
                "privileges-apparmor",
                "Privileges",
                serde_json::json!({"AppArmor": {"Mode": "disabled"}}),
            ),
            (
                "capability-drop",
                "CapabilityDrop",
                serde_json::json!(["CAP_NET_RAW"]),
            ),
            (
                "ulimits",
                "Ulimits",
                serde_json::json!([{"Name": "nofile", "Soft": 1024, "Hard": 2048}]),
            ),
            ("pids", "Pids", serde_json::json!(100)),
        ];
        for (name, field, value) in cases {
            let mut payload = serde_json::json!({
                "Name": format!("rejected-{name}"),
                "TaskTemplate": {
                    "ContainerSpec": {"Image": "nginx:latest"},
                    "Resources": {"Limits": {}}
                }
            });
            let target = if field == "Pids" {
                &mut payload["TaskTemplate"]["Resources"]["Limits"]
            } else {
                &mut payload["TaskTemplate"]["ContainerSpec"]
            };
            target[field] = value;
            let create_spec: models::ServiceSpec = serde_json::from_value(payload).unwrap();
            let err = create_service(State(app.clone()), default_caller(), Json(create_spec))
                .await
                .unwrap_err();
            assert_eq!(
                err.into_response().status(),
                axum::http::StatusCode::BAD_REQUEST,
                "{name} ({field})"
            );
            assert!(
                app.store
                    .get_service(&format!("rejected-{name}"))
                    .await
                    .unwrap()
                    .is_none(),
                "{name} ({field})"
            );
        }

        let no_new_privileges: models::ServiceSpec = serde_json::from_value(serde_json::json!({
            "Name": "nnp-enabled",
            "TaskTemplate": {
                "ContainerSpec": {
                    "Image": "nginx:latest",
                    "Privileges": {"NoNewPrivileges": true}
                }
            }
        }))
        .unwrap();
        let _ = create_service(
            State(app.clone()),
            default_caller(),
            Json(no_new_privileges),
        )
        .await
        .unwrap();
        assert!(app
            .store
            .get_service("nnp-enabled")
            .await
            .unwrap()
            .is_some());

        let _ = create_service(State(app.clone()), default_caller(), Json(spec("web", 0)))
            .await
            .unwrap();
        let update_spec: models::ServiceSpec = serde_json::from_value(serde_json::json!({
            "Name": "web",
            "TaskTemplate": {
                "ContainerSpec": {
                    "Image": "alpine:latest",
                    "ReadOnly": true
                }
            }
        }))
        .unwrap();
        let err = update_service(
            State(app.clone()),
            default_caller(),
            Path(("v1.41".to_string(), "web".to_string())),
            Query(
                serde_json::from_value::<models::ServiceUpdateQuery>(serde_json::json!({}))
                    .unwrap(),
            ),
            Json(update_spec),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.into_response().status(),
            axum::http::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            app.store.get_service("web").await.unwrap().unwrap().image,
            "nginx:latest"
        );
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
        assert!(
            app.store
                .get_service("web")
                .await
                .unwrap()
                .unwrap()
                .previous
                .is_some(),
            "a compliant spec stays the rollback target"
        );

        // A stored agent with an egress policy (admitted before the check)
        // must not become the rollback target of a compliant update.
        let mut legacy = app.store.get_service("web").await.unwrap().unwrap();
        legacy.agent = Some(Box::default());
        legacy.egress = Some(Default::default());
        app.store.upsert_service(legacy).await.unwrap();
        let _ = update_service(
            State(app.clone()),
            default_caller(),
            Path(("v1.41".to_string(), "web".to_string())),
            Query(
                serde_json::from_value::<models::ServiceUpdateQuery>(serde_json::json!({}))
                    .unwrap(),
            ),
            Json(spec("web", 800_000_000)),
        )
        .await
        .expect("the new spec has no agent");
        let updated = app.store.get_service("web").await.unwrap().unwrap();
        assert!(updated.agent.is_none() && updated.previous.is_none());

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

    fn ingress_service(tenant_id: &str, published: u16) -> StateService {
        StateService {
            is_job: false,
            id: format!("svc-{tenant_id}"),
            tenant_id: tenant_id.to_string(),
            name: format!("svc-{tenant_id}"),
            image: "nginx:latest".to_string(),
            replicas: 1,
            mode: minga_state::types::ServiceMode::Replicated,
            constraints: Vec::new(),
            env: HashMap::new(),
            ports: vec![minga_state::types::PortMapping {
                target: 443,
                published: Some(published),
                protocol: "tcp".to_string(),
                mode: minga_state::types::PortMode::Ingress,
            }],
            networks: Vec::new(),
            resources: ResourceSpec::default(),
            agent: None,
            egress: None,
            restart_policy: Default::default(),
            secrets: Vec::new(),
            command: Vec::new(),
            entrypoint: Vec::new(),
            labels: HashMap::new(),
            healthcheck: None,
            mounts: Vec::new(),
            user: None,
            update: Default::default(),
            rollback: None,
            previous: None,
            extra_hosts: Vec::new(),
        }
    }

    fn worker_node(id: u64, tenant_id: &str, reserved: &[u16]) -> ClusterNode {
        ClusterNode {
            id,
            address: format!("127.0.0.1:{}", 50051 + id),
            labels: HashMap::new(),
            status: NodeStatus::Active,
            capacity: ResourceSpec {
                cpus: 4.0,
                memory_bytes: 8 << 30,
                gpus: 0,
            },
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
            tenant_id: tenant_id.to_string(),
            last_seen_unix: None,
            role: Default::default(),
            reserved_ports: reserved
                .iter()
                .map(|port| minga_state::types::ReservedPort {
                    port: *port,
                    protocol: "tcp".to_string(),
                })
                .collect(),
            nats_user_public_key: None,
        }
    }

    #[test]
    fn cross_tenant_reserved_port_does_not_block_ingress() {
        let service = ingress_service("acme", 443);
        let nodes = vec![worker_node(1, "default", &[443])];
        assert!(
            validate_ingress_ports(&service, &[], &nodes).is_ok(),
            "a port tenant `default` reserved on its own node must not block tenant `acme` ingress"
        );
    }

    #[test]
    fn same_tenant_reserved_port_still_blocks_ingress() {
        let service = ingress_service("acme", 443);
        let nodes = vec![worker_node(1, "acme", &[443])];
        assert_eq!(
            validate_ingress_ports(&service, &[], &nodes),
            Err("ingress port is reserved on a cluster node".to_owned())
        );
    }

    #[tokio::test]
    async fn service_admission_scopes_reserved_ports_to_the_service_tenant() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .create_tenant(Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: "acme".into(),
                name: "acme".into(),
                quotas: TenantQuotas::default(),
                created_at: 1,
            })
            .await
            .unwrap();
        // Tenant `default` reserves 443 on its node; tenant `acme` does not.
        store
            .upsert_node(worker_node(1, "default", &[443]))
            .await
            .unwrap();
        store
            .upsert_node(worker_node(2, "acme", &[]))
            .await
            .unwrap();
        let app = Arc::new(AppState::with_bus(store, MemoryBus::new()));

        // The admission every write path runs (create, update and
        // change-request approval) must not see the other tenant's
        // reservation.
        let service = ingress_service("acme", 443);
        ensure_placeable(&app, &service, None).await.expect(
            "tenant `acme` ingress on 443 must pass while only tenant `default` reserves it",
        );

        // Once `acme`'s own node reserves the port, admission refuses.
        app.store
            .upsert_node(worker_node(2, "acme", &[443]))
            .await
            .unwrap();
        let err = ensure_placeable(&app, &service, None)
            .await
            .expect_err("tenant `acme`'s own reservation must block its ingress on 443");
        assert!(
            matches!(&err, ApiError::Conflict(m) if m == "ingress port is reserved on a cluster node"),
            "expected the reserved-port conflict, got {err:?}"
        );
    }
}
