//! Cluster state types: the desired state that goes through Raft.
//!
//! These are the application-level types stored in the state machine. They are
//! deliberately simpler than the Compose model — the scheduler translates
//! Compose services into these types before proposing them to Raft.

use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

/// Raft node ID.
pub type NodeId = u64;

/// Service identifier (e.g. `plant-7.inference`).
pub type ServiceId = String;

/// Task identifier.
pub type TaskId = String;

/// Network identifier.
pub type NetworkId = String;

/// Volume identifier.
pub type VolumeId = String;

/// Secret identifier.
pub type SecretId = String;
/// Tenant identifier. Tenant IDs are stable and used as an isolation boundary.
pub type TenantId = String;

/// Tenant that owns all state written before tenant-aware deployments.
pub const DEFAULT_TENANT_ID: &str = "default";

fn default_tenant_id() -> TenantId {
    DEFAULT_TENANT_ID.to_owned()
}

/// A cluster tenant and its desired quotas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tenant {
    pub id: TenantId,
    pub name: String,
    #[serde(default)]
    pub quotas: TenantQuotas,
    /// Cluster-wide revoked worker NKeys, stored on the permanent default tenant
    /// so deleting another tenant cannot make an old NATS identity live again.
    #[serde(default)]
    pub nats_revoked_user_public_keys: Vec<String>,
    /// Unix timestamp in seconds when the tenant was created.
    pub created_at: u64,
}

impl Tenant {
    /// Validates the tenant ID and display name.
    pub fn validate(&self) -> Result<(), &'static str> {
        Self::validate_id(&self.id)?;
        if self.name.trim().is_empty() {
            return Err("tenant name must not be empty");
        }
        Ok(())
    }

    /// The cluster-wide ID of the service, network, secret or config that
    /// `tenant_id` calls `name`. The `default` tenant keeps the bare name, as
    /// every object created before names were scoped does; any other tenant's
    /// objects get `<tenant>.<name>`, so two tenants can each have a `web`.
    /// `.` cannot occur in a tenant ID, so the first dot always splits them.
    pub fn object_id(tenant_id: &str, name: &str) -> String {
        if tenant_id == DEFAULT_TENANT_ID {
            name.to_owned()
        } else {
            format!("{tenant_id}.{name}")
        }
    }

    /// Longest qualified object ID: task IDs add `.<slot>.<revision>` to a
    /// service ID, and containerd refuses identifiers over 76 characters.
    pub const MAX_OBJECT_ID_LEN: usize = 64;

    /// Checks a client-chosen name before it is qualified. The `default`
    /// tenant is not checked, so names that worked before keep working.
    pub fn validate_object_name(tenant_id: &str, name: &str) -> Result<(), String> {
        if tenant_id == DEFAULT_TENANT_ID {
            return Ok(());
        }
        let mut chars = name.chars();
        let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
        if !first_ok
            || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
            || name.ends_with('.')
        {
            return Err(format!(
                "name `{name}` must start with a letter or digit and use only letters, digits, '.', '_' and '-'"
            ));
        }
        let id = Self::object_id(tenant_id, name);
        if id.len() > Self::MAX_OBJECT_ID_LEN {
            return Err(format!(
                "name `{name}` is too long: with the tenant it makes {} characters, at most {} are allowed",
                id.len(),
                Self::MAX_OBJECT_ID_LEN
            ));
        }
        Ok(())
    }

    /// Tenant IDs use the same safe token alphabet as NATS subjects.
    pub fn validate_id(id: &str) -> Result<(), &'static str> {
        if id.is_empty()
            || !id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte)
            })
        {
            return Err("tenant ID must contain only lowercase ASCII letters, digits, '-' or '_'");
        }
        Ok(())
    }
}

/// A scoped proposal for a supported service desired-state change.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Deploy,
    Scale,
    Rollback,
    Stop,
}

/// Lifecycle state of a maker-checker request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeRequestStatus {
    Pending,
    Approved,
    Rejected,
    Expired,
    Superseded,
}

/// One durable lifecycle/audit fact attached to a change request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeRequestAudit {
    pub action: String,
    pub actor: String,
    pub occurred_at_unix: u64,
}

/// A tenant-owned service change held in Raft until a separate reviewer decides.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeRequest {
    pub id: String,
    pub tenant_id: TenantId,
    pub service_id: ServiceId,
    pub requester: String,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
    pub kind: ChangeKind,
    pub before: Service,
    pub after: Service,
    pub status: ChangeRequestStatus,
    pub reviewer: Option<String>,
    pub decided_at_unix: Option<u64>,
    pub audit: Vec<ChangeRequestAudit>,
}

/// Optional per-tenant resource and shared-plane limits. `None` means no limit.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantQuotas {
    pub services: Option<u64>,
    pub tasks: Option<u64>,
    pub secrets: Option<u64>,
    pub networks: Option<u64>,
    #[serde(default)]
    pub volumes: Option<u64>,
    #[serde(default)]
    pub configs: Option<u64>,
    pub jobs: Option<u64>,
    pub desired_state_bytes: Option<u64>,
    pub api_requests_per_second: Option<u64>,
    pub jetstream_bytes: Option<u64>,
    pub jetstream_age_secs: Option<u64>,
}

/// Config identifier.
pub type ConfigId = String;

/// How a published port is exposed on cluster nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PortMode {
    /// Publish the port through the cluster ingress.
    #[default]
    Ingress,
    /// Publish the port directly on the node running the task.
    Host,
}

/// A port reserved by a node for a host-mode service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReservedPort {
    pub port: u16,
    pub protocol: String,
}

/// A machine in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterNode {
    pub id: NodeId,
    /// gRPC address for node-to-node communication.
    pub address: String,
    /// User-defined labels, e.g. `gpu=true`, `zone=a`.
    pub labels: HashMap<String, String>,
    pub status: NodeStatus,
    pub capacity: ResourceSpec,
    /// WireGuard public key (base64), set when the node joins the overlay.
    #[serde(default)]
    pub wg_public_key: Option<String>,
    /// WireGuard endpoint (host:port), set when the node is reachable.
    #[serde(default)]
    pub wg_endpoint: Option<String>,
    /// This node's IP in the WireGuard overlay mesh.
    #[serde(default)]
    pub overlay_ip: Option<String>,
    /// Tenant this worker serves. Managers use `default` as a compatibility value.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    /// Unix time of the node's last lease renewal. `None` means the node
    /// was registered out-of-band (e.g. `POST /nodes`) and carries no
    /// lease: it is considered live regardless of age.
    #[serde(default)]
    pub last_seen_unix: Option<u64>,
    /// Manager or worker; matched by `node.role` placement constraints.
    #[serde(default)]
    pub role: NodeRole,
    /// Host-mode ports unavailable for scheduling on this node.
    #[serde(default)]
    pub reserved_ports: Vec<ReservedPort>,
    /// NATS user public NKey, stored to revoke this node's credentials.
    #[serde(default)]
    pub nats_user_public_key: Option<String>,
}

/// Checks a `user` value is `user[:group]`, each part a number or a name of
/// letters, digits, `.`, `_` or `-`. The node resolves names from the
/// image's `/etc/passwd` and `/etc/group` when the task starts.
pub fn validate_user(user: &str) -> Result<(), &'static str> {
    let valid = |part: &str| {
        !part.is_empty()
            && !part.starts_with('-')
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    };
    let (name, group) = match user.split_once(':') {
        Some((name, group)) => (name, Some(group)),
        None => (user, None),
    };
    if valid(name) && group.is_none_or(valid) {
        Ok(())
    } else {
        Err("expected `user[:group]`, each a number or a name of letters, digits, `.`, `_` or `-`")
    }
}

/// Parses a numeric `uid[:gid]`; a missing gid is 0. Names need the image's
/// `/etc/passwd`, which the node reads (see `validate_user`).
pub fn parse_user(user: &str) -> Result<(u32, u32), &'static str> {
    let (uid, gid) = user.split_once(':').unwrap_or((user, "0"));
    Ok((
        uid.parse().map_err(|_| "named users are not supported")?,
        gid.parse().map_err(|_| "named groups are not supported")?,
    ))
}

/// The role a node was started with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Manager,
    #[default]
    Worker,
}

/// Whether a replacement task starts before or after the one it replaces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateOrder {
    #[default]
    StopFirst,
    StartFirst,
}

/// How a service's tasks are replaced when its spec changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdatePolicy {
    /// Tasks replaced at a time; 0 means all at once.
    pub parallelism: u32,
    pub order: UpdateOrder,
    /// Seconds a replaced batch must stay up before the next one starts.
    #[serde(default)]
    pub delay_secs: u64,
    /// Seconds a replacement must stay up before the rollout counts as done.
    #[serde(default)]
    pub monitor_secs: u64,
    #[serde(default)]
    pub failure_action: FailureAction,
}

/// What a rollout does when a replacement fails.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureAction {
    #[default]
    Pause,
    Continue,
    Rollback,
}

impl Default for UpdatePolicy {
    fn default() -> Self {
        Self {
            parallelism: 1,
            order: UpdateOrder::StopFirst,
            delay_secs: 0,
            monitor_secs: 5,
            failure_action: FailureAction::Pause,
        }
    }
}

/// How often a self-registered node renews its lease, in seconds.
pub const NODE_RENEW_INTERVAL_SECS: u64 = 10;

/// Current unix time in seconds.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Joining,
    Active,
    Draining,
    Left,
}

/// Compute and memory resources.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ResourceSpec {
    pub cpus: f64,
    pub memory_bytes: u64,
    pub gpus: u32,
}

/// A reference to a secret, carried in desired state.
///
/// **This holds no secret value.** It names a secret and the path it will
/// take inside the container. The value itself lives only in the
/// node-local secret store and is delivered as a read-only file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretRef {
    /// The cluster-scoped secret ID (e.g. `stack_db_password`).
    pub secret_id: String,
    /// The path inside the container, e.g. `/run/secrets/db_password`.
    pub target: String,
    /// Optional version/digest of the provisioned value.
    #[serde(default)]
    pub version: Option<String>,
}

impl SecretRef {
    /// Build a reference to `secret_id` mounted at `target`.
    pub fn new(secret_id: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            secret_id: secret_id.into(),
            target: target.into(),
            version: None,
        }
    }
}

/// Reserved state label for Compose's deny-by-default egress policy.
pub const REQUIRED_EGRESS_LABEL: &str = "com.minga.egress.required";
/// Reserved node label set when the worker runs its egress proxy
/// (`--egress-bind`). Services with egress routes are placed only there.
pub const EGRESS_PROXY_NODE_LABEL: &str = "com.minga.egress.proxy";
/// Provider authentication reference for a named egress route.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "scheme", rename_all = "snake_case", deny_unknown_fields)]
pub enum EgressAuth {
    Bearer {
        secret: String,
    },
    ApiKey {
        secret: String,
    },
    AwsSigv4 {
        access_key_secret: String,
        secret_key_secret: String,
        #[serde(default)]
        session_token_secret: Option<String>,
        region: String,
        service: String,
    },
}

/// One tenant-owned upstream route. Secret fields are IDs, never values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EgressRoute {
    pub name: String,
    pub upstream_url: String,
    #[serde(default)]
    pub auth: Option<EgressAuth>,
    #[serde(default)]
    pub fallback: Vec<String>,
    #[serde(default)]
    pub input_price_micros_per_million: Option<u64>,
    #[serde(default)]
    pub output_price_micros_per_million: Option<u64>,
}

/// Route and model allow-lists for one service.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct EgressPolicy {
    pub required: bool,
    pub routes: Vec<EgressRoute>,
    pub models: Vec<String>,
}

impl EgressPolicy {
    /// Every secret ID the routes reference.
    pub fn secret_ids(&self) -> impl Iterator<Item = &str> {
        self.routes
            .iter()
            .filter_map(|route| route.auth.as_ref())
            .flat_map(|auth| match auth {
                EgressAuth::Bearer { secret } | EgressAuth::ApiKey { secret } => {
                    vec![secret.as_str()]
                }
                EgressAuth::AwsSigv4 {
                    access_key_secret,
                    secret_key_secret,
                    session_token_secret,
                    ..
                } => [Some(access_key_secret), Some(secret_key_secret)]
                    .into_iter()
                    .chain([session_token_secret.as_ref()])
                    .flatten()
                    .map(String::as_str)
                    .collect(),
            })
    }
}

impl EgressPolicy {
    /// The same secret IDs as [`EgressPolicy::secret_ids`], to be rewritten in place.
    pub fn secret_ids_mut(&mut self) -> Vec<&mut String> {
        let mut ids = Vec::new();
        for route in &mut self.routes {
            match route.auth.as_mut() {
                Some(EgressAuth::Bearer { secret } | EgressAuth::ApiKey { secret }) => {
                    ids.push(secret);
                }
                Some(EgressAuth::AwsSigv4 {
                    access_key_secret,
                    secret_key_secret,
                    session_token_secret,
                    ..
                }) => {
                    ids.push(access_key_secret);
                    ids.push(secret_key_secret);
                    ids.extend(session_token_secret.as_mut());
                }
                None => {}
            }
        }
        ids
    }
}

impl TaskSpec {
    /// Whether the task talks to the node egress proxy with a minted token:
    /// it requires egress or names at least one route.
    pub fn uses_egress_proxy(&self) -> bool {
        self.labels
            .get(REQUIRED_EGRESS_LABEL)
            .is_some_and(|value| value == "true")
            || self.egress.as_ref().is_some_and(|workload| {
                workload.policy.required || !workload.policy.routes.is_empty()
            })
    }
}

impl EgressPolicy {
    /// Whether placement needs a node running the egress proxy.
    pub fn needs_proxy(&self) -> bool {
        self.required || !self.routes.is_empty()
    }
}

/// Egress policy bound to the task identity when placed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EgressWorkload {
    pub tenant_id: TenantId,
    pub service_id: ServiceId,
    pub policy: EgressPolicy,
}

/// A desired service, translated from a Compose service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Service {
    pub id: ServiceId,
    /// Owning tenant; legacy records belong to the default tenant.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    pub name: String,
    /// Container image. Empty string for agent (WASM sandbox) services.
    pub image: String,
    pub replicas: u32,
    pub mode: ServiceMode,
    /// Docker job-mode service, counted against the tenant's job quota.
    #[serde(default)]
    pub is_job: bool,
    /// Placement constraints, e.g. `node.labels.gpu == true`.
    pub constraints: Vec<String>,
    pub env: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub networks: Vec<NetworkId>,
    pub resources: ResourceSpec,
    /// Agent workload spec. `None` for container services.
    #[serde(default)]
    pub agent: Option<Box<AgentSpec>>,
    /// Tenant-owned container egress routes and model policy.
    #[serde(default)]
    pub egress: Option<EgressPolicy>,
    /// Restart policy for tasks of this service.
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    /// Secret references for this service. References only — no values.
    #[serde(default)]
    pub secrets: Vec<SecretRef>,
    /// Overrides the image's Cmd. Empty = image default.
    #[serde(default)]
    pub command: Vec<String>,
    /// Overrides the image's Entrypoint. Empty = image default.
    #[serde(default)]
    pub entrypoint: Vec<String>,
    /// Service labels (Compose `labels` / Docker `Spec.Labels`). The stack
    /// namespace label is included.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Container healthcheck, carried for inspection and (later)
    /// supervisor enforcement.
    #[serde(default)]
    pub healthcheck: Option<Healthcheck>,
    /// Volume and bind mounts.
    #[serde(default)]
    pub mounts: Vec<ServiceMount>,
    /// Numeric `uid[:gid]` to run as. `None` keeps the image's user.
    #[serde(default)]
    pub user: Option<String>,
    /// Additional `/etc/hosts` entries: `[(hostname, ip)]`.
    #[serde(default)]
    pub extra_hosts: Vec<(String, String)>,
    /// How tasks are replaced when the spec changes.
    #[serde(default)]
    pub update: UpdatePolicy,
    /// How tasks are replaced when rolling back to `previous`.
    #[serde(default)]
    pub rollback: Option<UpdatePolicy>,
    /// The spec this one replaced, kept until the rollout settles.
    #[serde(default)]
    pub previous: Option<Box<Service>>,
}

impl Service {
    /// Agent egress tokens do not carry a route policy yet, so an agent
    /// service with an egress policy is refused at admission instead of
    /// running without it.
    pub fn check_agent_egress(&self) -> Result<(), &'static str> {
        if self.agent.is_some() && self.egress.is_some() {
            return Err(AGENT_EGRESS_UNSUPPORTED);
        }
        Ok(())
    }
}

/// Why an agent service cannot declare an egress policy.
pub const AGENT_EGRESS_UNSUPPORTED: &str = "x-minga-egress is not supported on x-minga-agent services yet: agent egress tokens carry no route policy";

/// A container healthcheck (Compose/Docker shape).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Healthcheck {
    /// e.g. `["CMD-SHELL", "curl -f localhost"]`, `["CMD", ...]` or `["NONE"]`.
    pub test: Vec<String>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub start_period_secs: Option<u64>,
    #[serde(default)]
    pub retries: Option<u32>,
}

/// What backs a [`ServiceMount`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MountKind {
    /// A named volume; the node materializes it under its volume dir.
    Volume,
    /// A host path bind-mounted as-is.
    Bind,
}

/// A volume or bind mount declared by a service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceMount {
    pub kind: MountKind,
    /// Volume ID (stack-prefixed) for `Volume`, host path for `Bind`.
    pub source: String,
    /// Path inside the container.
    pub target: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceMode {
    Replicated,
    Global,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortMapping {
    /// Node address the port is published on; `None` publishes it on every
    /// node address. A record written before this field existed decodes as
    /// `None`, and `None` is not written, so old records keep their bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<IpAddr>,
    pub target: u16,
    pub published: Option<u16>,
    pub protocol: String,
    #[serde(default)]
    pub mode: PortMode,
}

/// Why `service`'s published ports collide, if they do. A published
/// port/protocol may appear once per service, and once per cluster when
/// either side publishes it through ingress. Two host-mode services may
/// share one, since placement keeps them on different nodes.
pub fn published_port_conflict<'a>(
    service: &Service,
    others: impl IntoIterator<Item = &'a Service>,
) -> Option<String> {
    let published = |port: &PortMapping| {
        port.published
            .map(|published| (published, port.protocol.to_ascii_lowercase()))
    };
    for (index, port) in service.ports.iter().enumerate() {
        let Some(tuple) = published(port) else {
            continue;
        };
        if service.ports[..index]
            .iter()
            .any(|other| published(other).as_ref() == Some(&tuple))
        {
            return Some(format!(
                "port {}/{} is published more than once",
                tuple.0, tuple.1
            ));
        }
    }
    for other in others.into_iter().filter(|other| other.id != service.id) {
        for port in &service.ports {
            let Some(tuple) = published(port) else {
                continue;
            };
            if other.ports.iter().any(|other_port| {
                published(other_port).as_ref() == Some(&tuple)
                    && (port.mode == PortMode::Ingress || other_port.mode == PortMode::Ingress)
            }) {
                return Some(if other.tenant_id == service.tenant_id {
                    format!(
                        "ingress port {}/{} is already used by service {}",
                        tuple.0, tuple.1, other.id
                    )
                } else {
                    format!("ingress port {}/{} is already in use", tuple.0, tuple.1)
                });
            }
        }
    }
    None
}

/// A task: one replica of a service placed on a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    /// Owning tenant; legacy records belong to the default tenant.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    pub service_id: ServiceId,
    pub node_id: NodeId,
    pub state: TaskState,
    pub spec: TaskSpec,
    /// When the task last became `Running`.
    #[serde(default)]
    pub running_since_unix: Option<u64>,
    /// Times the container exited or failed to start and was retried.
    #[serde(default)]
    pub restarts: u32,
    /// Why the task last exited, failed to start or was killed: one line,
    /// at most 300 characters, never a secret. Stays set while the task
    /// restarts; cleared once it has been `Running` for a while.
    #[serde(default)]
    pub last_error: Option<String>,
    /// Exit code of the last run, when the runtime reported one.
    #[serde(default)]
    pub last_exit_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Assigned,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl TaskState {
    /// A stopped or failed task no longer runs and does not count toward
    /// the tenant's task quota.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TaskSpec {
    pub image: String,
    pub env: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub resources: ResourceSpec,
    /// Workload kind: container (default) or agent (WASM sandbox).
    #[serde(default)]
    pub kind: WorkloadKind,
    /// Restart policy for failed tasks.
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    /// Secret references for this task. References only — no values.
    #[serde(default)]
    pub secrets: Vec<SecretRef>,
    /// IP addresses assigned to this task, keyed by network ID. Lives on
    /// the spec (not only the task record) so it travels with the task
    /// through the event bus to the node's runtime.
    #[serde(default)]
    pub ips: HashMap<NetworkId, String>,
    /// Overrides the image's Cmd. Empty = image default.
    #[serde(default)]
    pub command: Vec<String>,
    /// Overrides the image's Entrypoint. Empty = image default.
    #[serde(default)]
    pub entrypoint: Vec<String>,
    /// Service labels, propagated onto the container and the task API
    /// response (Traefik's swarm provider reads them).
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Egress policy and tenant/service identity attached by placement.
    #[serde(default)]
    pub egress: Option<EgressWorkload>,
    /// Container healthcheck, carried for inspection.
    #[serde(default)]
    pub healthcheck: Option<Healthcheck>,
    /// Volume and bind mounts to apply in the OCI spec.
    #[serde(default)]
    pub mounts: Vec<ServiceMount>,
    /// Numeric `uid[:gid]` to run as. `None` keeps the image's user.
    #[serde(default)]
    pub user: Option<String>,
    /// Additional `/etc/hosts` entries: `[(hostname, ip)]`.
    #[serde(default)]
    pub extra_hosts: Vec<(String, String)>,
}

/// Restart policy for a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RestartPolicy {
    /// When to restart: `none`, `on-failure`, or `any` (always).
    #[serde(default)]
    pub condition: RestartCondition,
    /// Base delay between restarts in seconds. Exponential backoff: delay * 2^attempt.
    #[serde(default = "default_restart_delay")]
    pub delay_secs: u64,
    /// Maximum number of restart attempts. `None` = unlimited.
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

fn default_restart_delay() -> u64 {
    1
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            condition: RestartCondition::None,
            delay_secs: default_restart_delay(),
            max_attempts: None,
        }
    }
}

/// When a failed task should be restarted.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum RestartCondition {
    /// Never restart (default).
    #[default]
    None,
    /// Restart only if the task exited with a non-zero code.
    OnFailure,
    /// Always restart, regardless of exit code.
    Any,
}

/// The kind of workload a task runs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum WorkloadKind {
    /// A container workload (default, backward-compatible).
    #[default]
    Container,
    /// An AI-agent workload running in a WASM sandbox.
    Agent(Box<AgentSpec>),
}

/// Capabilities explicitly granted to an agent workload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentCapabilities {
    /// Host capabilities granted to the workload.
    #[serde(default)]
    pub grants: BTreeSet<AgentGrant>,
    /// Read-only host directories exposed at guest paths.
    #[serde(default)]
    pub dirs: Vec<AgentDirGrant>,
}

impl AgentCapabilities {
    /// Most directory grants one agent may declare.
    pub const MAX_DIRS: usize = 64;
    /// Longest host or guest path, in bytes (Linux `PATH_MAX`).
    pub const MAX_PATH_LEN: usize = 4096;

    /// What the v8 to v9 migration grants a record that has no
    /// capabilities: `env` when it declares an env map, nothing else.
    pub fn v8_migrated(sandbox_env: &HashMap<String, String>) -> Self {
        Self {
            grants: if sandbox_env.is_empty() {
                BTreeSet::new()
            } else {
                BTreeSet::from([AgentGrant::Env])
            },
            dirs: Vec::new(),
        }
    }

    /// Checks the directory grants that need no node configuration: at most
    /// [`Self::MAX_DIRS`], absolute host and guest paths of at most
    /// [`Self::MAX_PATH_LEN`] bytes without `..`, and no guest path declared
    /// twice. Roots and existence are checked on the node.
    pub fn validate_dir_paths(&self) -> Result<(), String> {
        fn plain_absolute(path: &std::path::Path) -> bool {
            path.is_absolute()
                && !path
                    .components()
                    .any(|component| component == std::path::Component::ParentDir)
        }
        if self.dirs.len() > Self::MAX_DIRS {
            return Err(format!(
                "agent declares {} directory grants, at most {} are allowed",
                self.dirs.len(),
                Self::MAX_DIRS
            ));
        }
        let mut guest_paths = BTreeSet::new();
        for dir in &self.dirs {
            if dir.host.len() > Self::MAX_PATH_LEN || dir.guest.len() > Self::MAX_PATH_LEN {
                return Err(format!(
                    "agent directory paths must be at most {} bytes",
                    Self::MAX_PATH_LEN
                ));
            }
            if !plain_absolute(std::path::Path::new(&dir.host)) {
                return Err(format!(
                    "agent directory host path `{}` must be absolute and contain no `..`",
                    dir.host
                ));
            }
            let guest = std::path::Path::new(&dir.guest);
            if !plain_absolute(guest) {
                return Err(format!(
                    "agent directory guest path `{}` must be absolute and contain no `..`",
                    dir.guest
                ));
            }
            // Components drop `.` and repeated separators.
            let normalized: std::path::PathBuf = guest.components().collect();
            if !guest_paths.insert(normalized) {
                return Err(format!(
                    "agent directory guest path `{}` is declared more than once",
                    dir.guest
                ));
            }
        }
        Ok(())
    }
}

/// A host capability available to an agent workload.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AgentGrant {
    /// Access to the explicitly declared environment map.
    Env,
    /// Access to wall and monotonic clocks.
    Clock,
    /// Access to the host random source.
    Random,
    /// Access to standard input, output, and error streams.
    Stdio,
    /// Access to the node's configured outbound HTTP egress proxy.
    Http,
}

/// A read-only directory granted to an agent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentDirGrant {
    /// Host directory path, checked against node operator roots.
    pub host: String,
    /// Absolute guest mount path.
    pub guest: String,
}

/// Specification for an agent (WASM sandbox) workload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(from = "AgentSpecRecord")]
pub struct AgentSpec {
    /// Path or reference to the WASM module on the node's filesystem.
    /// Resolved by the agent executor at start time.
    pub module_ref: String,
    /// Maximum fuel (instruction budget). `None` = unlimited.
    pub fuel_limit: Option<u64>,
    /// Maximum memory in bytes. `None` = runtime default.
    pub memory_limit: Option<u64>,
    /// Maximum execution time in seconds. `None` = runtime default (30s).
    pub timeout_secs: Option<u64>,
    /// Token budget for LLM egress. `None` = no budget enforcement.
    pub token_budget: Option<u64>,
    /// LLM API upstream URL (goes through the egress proxy).
    pub egress_url: Option<String>,
    /// Initial prompt / input for the agent.
    pub prompt: Option<String>,
    /// Additional environment variables, exposed only with the `env` grant.
    #[serde(default)]
    pub sandbox_env: HashMap<String, String>,
    /// Explicit host capabilities. Records written before v9 omit them; see
    /// [`AgentSpecRecord`].
    pub capabilities: AgentCapabilities,
}

/// Decoded shape of [`AgentSpec`]; `capabilities` is absent before format v9.
/// No stored version ever had other keys, so unknown ones are refused.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSpecRecord {
    module_ref: String,
    fuel_limit: Option<u64>,
    memory_limit: Option<u64>,
    timeout_secs: Option<u64>,
    token_budget: Option<u64>,
    egress_url: Option<String>,
    prompt: Option<String>,
    #[serde(default)]
    sandbox_env: HashMap<String, String>,
    capabilities: Option<AgentCapabilities>,
}

impl From<AgentSpecRecord> for AgentSpec {
    fn from(record: AgentSpecRecord) -> Self {
        // Before v9 the declared env map always reached the sandbox. Granting
        // `env` to those records keeps upgraded agents starting; the tenant
        // declared both the map and the agent, so nothing widens.
        let capabilities = record
            .capabilities
            .unwrap_or_else(|| AgentCapabilities::v8_migrated(&record.sandbox_env));
        Self {
            module_ref: record.module_ref,
            fuel_limit: record.fuel_limit,
            memory_limit: record.memory_limit,
            timeout_secs: record.timeout_secs,
            token_budget: record.token_budget,
            egress_url: record.egress_url,
            prompt: record.prompt,
            sandbox_env: record.sandbox_env,
            capabilities,
        }
    }
}

/// An overlay network.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Network {
    pub id: NetworkId,
    /// Owning tenant; legacy records belong to the default tenant.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    pub name: String,
    pub driver: String,
    pub attachable: bool,
    /// Subnet CIDR for the overlay (e.g. "10.100.0.0/24").
    #[serde(default)]
    pub subnet: Option<String>,
    /// WireGuard listen port for this network (default 51820).
    #[serde(default)]
    pub wg_port: Option<u16>,
}

/// A task's desired runtime assignment, projected into MINGA_ASSIGN.
///
/// Actual lifecycle state remains in MINGA_STATUS; this value contains only
/// the desired spec and the network data needed to run it locally.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskAssignment {
    pub task_id: TaskId,
    pub node_id: NodeId,
    pub service_id: ServiceId,
    pub service_name: String,
    pub stack: String,
    pub spec: TaskSpec,
    pub networks: Vec<Network>,
}

/// Network metadata and the DNS records needed by local supervisors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkAssignment {
    pub network: Network,
    pub tasks: Vec<NetworkDnsTask>,
}

/// A backend for a published ingress port.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngressBackend {
    pub node_id: NodeId,
    pub ip: String,
    pub target_port: u16,
}

/// A published ingress port and its scheduled task backends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngressAssignment {
    /// Node address the port is published on; `None` is every node address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<IpAddr>,
    pub published: u16,
    pub protocol: String,
    pub backends: Vec<IngressBackend>,
    /// Source address used for SNAT by the local overlay ingress.
    pub source_overlay_ip: Option<String>,
}

/// One task's DNS contribution to a network assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkDnsTask {
    pub node_id: NodeId,
    pub task_id: TaskId,
    pub service_name: String,
    pub stack: String,
    pub ip: String,
}

/// A named volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Volume {
    pub id: VolumeId,
    /// Owning tenant; legacy records belong to the default tenant.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    pub name: String,
    pub driver: String,
}

/// A cluster-level secret (value is not stored here; this is metadata).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Secret {
    pub id: SecretId,
    /// Owning tenant; legacy records belong to the default tenant.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    pub name: String,
}

/// A cluster-level config (value is not stored here; this is metadata).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub id: ConfigId,
    /// Owning tenant; legacy records belong to the default tenant.
    #[serde(default = "default_tenant_id")]
    pub tenant_id: TenantId,
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_ids_are_bare_for_default_and_qualified_for_other_tenants() {
        assert_eq!(Tenant::object_id(DEFAULT_TENANT_ID, "web"), "web");
        assert_eq!(Tenant::object_id("acme", "web"), "acme.web");
        assert_eq!(Tenant::object_id("acme", "app.web"), "acme.app.web");
        // Two tenants' same name never share an ID, and a tenant's ID has no dot to confuse the split.
        assert_ne!(
            Tenant::object_id("acme", "web"),
            Tenant::object_id("beta", "web")
        );
        assert!(Tenant::validate_id("a.b").is_err());
    }

    #[test]
    fn object_names_of_other_tenants_are_checked_and_default_is_not() {
        assert!(Tenant::validate_object_name("acme", "app_db-1.x").is_ok());
        for bad in ["", " x", "x y", "-x", ".x", "x.", "a/b", "é"] {
            assert!(
                Tenant::validate_object_name("acme", bad).is_err(),
                "{bad:?}"
            );
        }
        // 64 characters with the tenant and its dot is the most.
        let longest = "n".repeat(Tenant::MAX_OBJECT_ID_LEN - "acme.".len());
        assert!(Tenant::validate_object_name("acme", &longest).is_ok());
        assert!(Tenant::validate_object_name("acme", &format!("{longest}n")).is_err());
        assert!(Tenant::validate_object_name(DEFAULT_TENANT_ID, "any name at all").is_ok());
    }

    #[test]
    fn egress_secret_ids_can_be_rewritten_in_place() {
        let mut policy = EgressPolicy {
            required: false,
            routes: vec![EgressRoute {
                name: "r".into(),
                upstream_url: "https://example.test".into(),
                auth: Some(EgressAuth::AwsSigv4 {
                    access_key_secret: "a".into(),
                    secret_key_secret: "b".into(),
                    session_token_secret: Some("c".into()),
                    region: "r".into(),
                    service: "s".into(),
                }),
                fallback: vec![],
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            }],
            models: vec![],
        };
        for id in policy.secret_ids_mut() {
            id.insert_str(0, "acme.");
        }
        assert_eq!(
            policy.secret_ids().collect::<Vec<_>>(),
            ["acme.a", "acme.b", "acme.c"]
        );
    }

    #[test]
    fn tenant_ids_use_nats_safe_tokens() {
        for id in ["default", "site-7", "tenant_1", "9"] {
            assert!(Tenant::validate_id(id).is_ok(), "{id}");
        }
        for id in ["", "Tenant", "two.parts", "$system", "bad/id"] {
            assert!(Tenant::validate_id(id).is_err(), "{id}");
        }
        assert!(Tenant {
            id: "tenant-a".into(),
            name: " ".into(),
            quotas: TenantQuotas::default(),
            nats_revoked_user_public_keys: Vec::new(),
            created_at: 1,
        }
        .validate()
        .is_err());
    }
    fn service_with_ingress_port(id: &str, tenant_id: &str) -> Service {
        Service {
            id: id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            egress: None,
            name: id.to_owned(),
            image: "nginx".to_owned(),
            replicas: 1,
            mode: ServiceMode::Replicated,
            is_job: false,
            constraints: Vec::new(),
            env: HashMap::new(),
            ports: vec![PortMapping {
                host_ip: None,
                target: 80,
                published: Some(8080),
                protocol: "tcp".to_owned(),
                mode: PortMode::Ingress,
            }],
            networks: Vec::new(),
            resources: ResourceSpec::default(),
            agent: None,
            restart_policy: RestartPolicy::default(),
            secrets: Vec::new(),
            command: Vec::new(),
            entrypoint: Vec::new(),
            labels: HashMap::new(),
            healthcheck: None,
            mounts: Vec::new(),
            user: None,
            extra_hosts: Vec::new(),
            update: UpdatePolicy::default(),
            rollback: None,
            previous: None,
        }
    }

    #[test]
    fn cross_tenant_ingress_conflict_does_not_disclose_service_id() {
        let owner = service_with_ingress_port("tenant-a.web", "tenant-a");
        let request = service_with_ingress_port("tenant-b.web", "tenant-b");
        let conflict = published_port_conflict(&request, [&owner]).unwrap();

        assert_eq!(conflict, "ingress port 8080/tcp is already in use");
        assert!(!conflict.contains("tenant-a.web"));
    }

    #[test]
    fn same_tenant_ingress_conflict_names_service() {
        let owner = service_with_ingress_port("tenant-a.web", "tenant-a");
        let request = service_with_ingress_port("tenant-a.api", "tenant-a");

        assert_eq!(
            published_port_conflict(&request, [&owner]).as_deref(),
            Some("ingress port 8080/tcp is already used by service tenant-a.web")
        );
    }

    #[test]
    fn one_service_cannot_publish_a_port_twice_even_on_different_host_ips() {
        let mut service = service_with_ingress_port("tenant-a.web", "tenant-a");
        let mut narrow = service.ports[0].clone();
        narrow.host_ip = Some("127.0.0.1".parse().unwrap());
        narrow.target = 81;
        service.ports.push(narrow);

        assert_eq!(
            published_port_conflict(&service, []).as_deref(),
            Some("port 8080/tcp is published more than once")
        );
    }

    #[test]
    fn workload_kind_default_is_container() {
        assert_eq!(WorkloadKind::default(), WorkloadKind::Container);
    }

    #[test]
    fn taskspec_default_kind_is_container() {
        let spec = TaskSpec::default();
        assert_eq!(spec.kind, WorkloadKind::Container);
    }

    #[test]
    fn workload_kind_roundtrip_container() {
        let kind = WorkloadKind::Container;
        let bytes = crate::codec::encode(&kind).unwrap();
        let back: WorkloadKind = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, kind);
    }

    #[test]
    fn only_pre_v9_agent_records_get_an_implicit_env_grant() {
        let legacy: AgentSpec =
            serde_json::from_str(r#"{"module_ref":"a.wasm","sandbox_env":{"K":"v"}}"#).unwrap();
        assert_eq!(
            legacy.capabilities.grants,
            BTreeSet::from([AgentGrant::Env])
        );

        let explicit: AgentSpec = serde_json::from_str(
            r#"{"module_ref":"a.wasm","sandbox_env":{"K":"v"},"capabilities":{}}"#,
        )
        .unwrap();
        assert!(explicit.capabilities.grants.is_empty());

        let no_env: AgentSpec = serde_json::from_str(r#"{"module_ref":"a.wasm"}"#).unwrap();
        assert!(no_env.capabilities.grants.is_empty());
    }

    #[test]
    fn workload_kind_roundtrip_agent() {
        let kind = WorkloadKind::Agent(Box::new(AgentSpec {
            module_ref: "./agent.wasm".to_string(),
            fuel_limit: Some(1_000_000),
            memory_limit: Some(128 * 1024 * 1024),
            timeout_secs: Some(60),
            token_budget: Some(100_000),
            egress_url: Some("https://api.openai.com".to_string()),
            prompt: Some("hello".to_string()),
            sandbox_env: HashMap::from([("MODEL".to_string(), "gpt-4o".to_string())]),
            capabilities: AgentCapabilities::default(),
        }));
        let bytes = crate::codec::encode(&kind).unwrap();
        let back: WorkloadKind = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, kind);
    }

    #[test]
    fn taskspec_roundtrip_with_agent_kind() {
        let spec = TaskSpec {
            image: String::new(),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            kind: WorkloadKind::Agent(Box::new(AgentSpec {
                module_ref: "./agent.wasm".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let bytes = crate::codec::encode(&spec).unwrap();
        let back: TaskSpec = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn service_roundtrip_with_agent() {
        let svc = Service {
            id: "stack.agent".to_string(),
            tenant_id: "default".to_string(),
            egress: None,
            name: "agent".to_string(),
            image: String::new(),
            replicas: 1,
            mode: ServiceMode::Replicated,
            is_job: false,
            constraints: vec![],
            env: HashMap::new(),
            ports: vec![],
            networks: vec![],
            resources: ResourceSpec::default(),
            agent: Some(Box::new(AgentSpec {
                module_ref: "./agent.wasm".to_string(),
                ..Default::default()
            })),
            restart_policy: Default::default(),
            secrets: vec![SecretRef::new("stack_password", "/run/secrets/db_password")],
            command: vec![],
            entrypoint: vec![],
            labels: HashMap::new(),
            healthcheck: None,
            mounts: vec![],
            user: None,
            update: Default::default(),
            rollback: None,
            previous: None,
            extra_hosts: vec![],
        };
        let bytes = crate::codec::encode(&svc).unwrap();
        let back: Service = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, svc);
    }

    #[test]
    fn task_spec_roundtrip_carries_secret_references() {
        let spec = TaskSpec {
            image: "alpine:3".to_string(),
            secrets: vec![SecretRef::new("stack_password", "/run/secrets/db_password")],
            ..Default::default()
        };
        let bytes = crate::codec::encode(&spec).unwrap();
        let back: TaskSpec = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, spec);
        assert_eq!(back.secrets.len(), 1);
        assert_eq!(back.secrets[0].secret_id, "stack_password");
    }

    #[test]
    fn agent_spec_json_roundtrip() {
        let spec = AgentSpec {
            module_ref: "./agent.wasm".to_string(),
            fuel_limit: Some(1_000_000),
            memory_limit: None,
            timeout_secs: Some(30),
            token_budget: Some(50_000),
            egress_url: Some("https://api.anthropic.com".to_string()),
            prompt: Some("Summarize this".to_string()),
            sandbox_env: HashMap::new(),
            capabilities: AgentCapabilities {
                grants: BTreeSet::from([AgentGrant::Env, AgentGrant::Http]),
                dirs: vec![AgentDirGrant {
                    host: "/srv/data".into(),
                    guest: "/data".into(),
                }],
            },
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: AgentSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn restart_policy_roundtrip() {
        let policy = RestartPolicy {
            condition: RestartCondition::OnFailure,
            delay_secs: 5,
            max_attempts: Some(3),
        };
        let bytes = crate::codec::encode(&policy).unwrap();
        let back: RestartPolicy = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, policy);
    }

    #[test]
    fn restart_policy_default_is_none() {
        let policy = RestartPolicy::default();
        assert_eq!(policy.condition, RestartCondition::None);
        assert_eq!(policy.delay_secs, 1);
        assert_eq!(policy.max_attempts, None);
    }

    #[test]
    fn taskspec_with_restart_policy_roundtrip() {
        let spec = TaskSpec {
            image: "nginx:latest".to_string(),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            restart_policy: RestartPolicy {
                condition: RestartCondition::Any,
                delay_secs: 10,
                max_attempts: Some(5),
            },
            ..Default::default()
        };
        let bytes = crate::codec::encode(&spec).unwrap();
        let back: TaskSpec = crate::codec::decode(&bytes).unwrap();
        assert_eq!(back, spec);
    }
}
