//! Cluster state types: the desired state that goes through Raft.
//!
//! These are the application-level types stored in the state machine. They are
//! deliberately simpler than the Compose model — the scheduler translates
//! Compose services into these types before proposing them to Raft.

use std::collections::HashMap;

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

/// Config identifier.
pub type ConfigId = String;

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
    /// Unix time of the node's last lease renewal. `None` means the node
    /// was registered out-of-band (e.g. `POST /nodes`) and carries no
    /// lease: it is considered live regardless of age.
    #[serde(default)]
    pub last_seen_unix: Option<u64>,
    /// Manager or worker; matched by `node.role` placement constraints.
    #[serde(default)]
    pub role: NodeRole,
}

/// Parses a numeric `uid[:gid]`; a missing gid is 0. Names would need the
/// image's `/etc/passwd`, so they fail loudly instead.
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

/// How long a node's lease remains valid without renewal, in seconds.
/// Three renewal intervals: tolerates one missed renewal plus clock skew.
pub const NODE_LEASE_TTL_SECS: u64 = 3 * NODE_RENEW_INTERVAL_SECS;

impl ClusterNode {
    /// Whether the node's lease is valid at `now_unix`. Nodes without a
    /// lease (`last_seen_unix: None`) are always live.
    pub fn is_live_at(&self, now_unix: u64, ttl_secs: u64) -> bool {
        match self.last_seen_unix {
            None => true,
            Some(ts) => now_unix.saturating_sub(ts) <= ttl_secs,
        }
    }
}

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

/// A desired service, translated from a Compose service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Service {
    pub id: ServiceId,
    pub name: String,
    /// Container image. Empty string for agent (WASM sandbox) services.
    pub image: String,
    pub replicas: u32,
    pub mode: ServiceMode,
    /// Placement constraints, e.g. `node.labels.gpu == true`.
    pub constraints: Vec<String>,
    pub env: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub networks: Vec<NetworkId>,
    pub resources: ResourceSpec,
    /// Agent workload spec. `None` for container services.
    #[serde(default)]
    pub agent: Option<AgentSpec>,
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
    /// How tasks are replaced when the spec changes.
    #[serde(default)]
    pub update: UpdatePolicy,
    /// The spec this one replaced, kept until the rollout settles.
    #[serde(default)]
    pub previous: Option<Box<Service>>,
}

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
    pub target: u16,
    pub published: Option<u16>,
    pub protocol: String,
}

/// A task: one replica of a service placed on a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
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
    /// Container healthcheck, carried for inspection.
    #[serde(default)]
    pub healthcheck: Option<Healthcheck>,
    /// Volume and bind mounts to apply in the OCI spec.
    #[serde(default)]
    pub mounts: Vec<ServiceMount>,
    /// Numeric `uid[:gid]` to run as. `None` keeps the image's user.
    #[serde(default)]
    pub user: Option<String>,
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
    Agent(AgentSpec),
}

/// Specification for an agent (WASM sandbox) workload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
    /// Additional environment variables for the sandbox.
    #[serde(default)]
    pub sandbox_env: HashMap<String, String>,
}

/// An overlay network.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Network {
    pub id: NetworkId,
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

/// A named volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Volume {
    pub id: VolumeId,
    pub name: String,
    pub driver: String,
}

/// A cluster-level secret (value is not stored here; this is metadata).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Secret {
    pub id: SecretId,
    pub name: String,
}

/// A cluster-level config (value is not stored here; this is metadata).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub id: ConfigId,
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn workload_kind_bincode_roundtrip_container() {
        let kind = WorkloadKind::Container;
        let bytes = bincode::serde::encode_to_vec(&kind, bincode::config::standard()).unwrap();
        let (back, _): (WorkloadKind, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back, kind);
    }

    #[test]
    fn workload_kind_bincode_roundtrip_agent() {
        let kind = WorkloadKind::Agent(AgentSpec {
            module_ref: "./agent.wasm".to_string(),
            fuel_limit: Some(1_000_000),
            memory_limit: Some(128 * 1024 * 1024),
            timeout_secs: Some(60),
            token_budget: Some(100_000),
            egress_url: Some("https://api.openai.com".to_string()),
            prompt: Some("hello".to_string()),
            sandbox_env: HashMap::from([("MODEL".to_string(), "gpt-4o".to_string())]),
        });
        let bytes = bincode::serde::encode_to_vec(&kind, bincode::config::standard()).unwrap();
        let (back, _): (WorkloadKind, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back, kind);
    }

    #[test]
    fn taskspec_bincode_roundtrip_with_agent_kind() {
        let spec = TaskSpec {
            image: String::new(),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            kind: WorkloadKind::Agent(AgentSpec {
                module_ref: "./agent.wasm".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = bincode::serde::encode_to_vec(&spec, bincode::config::standard()).unwrap();
        let (back, _): (TaskSpec, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn service_bincode_roundtrip_with_agent() {
        let svc = Service {
            id: "stack.agent".to_string(),
            name: "agent".to_string(),
            image: String::new(),
            replicas: 1,
            mode: ServiceMode::Replicated,
            constraints: vec![],
            env: HashMap::new(),
            ports: vec![],
            networks: vec![],
            resources: ResourceSpec::default(),
            agent: Some(AgentSpec {
                module_ref: "./agent.wasm".to_string(),
                ..Default::default()
            }),
            restart_policy: Default::default(),
            secrets: vec![SecretRef::new("stack_password", "/run/secrets/db_password")],
            command: vec![],
            entrypoint: vec![],
            labels: HashMap::new(),
            healthcheck: None,
            mounts: vec![],
            user: None,
            update: Default::default(),
            previous: None,
        };
        let bytes = bincode::serde::encode_to_vec(&svc, bincode::config::standard()).unwrap();
        let (back, _): (Service, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back, svc);
    }

    #[test]
    fn task_spec_roundtrip_carries_secret_references() {
        let spec = TaskSpec {
            image: "alpine:3".to_string(),
            secrets: vec![SecretRef::new("stack_password", "/run/secrets/db_password")],
            ..Default::default()
        };
        let bytes = bincode::serde::encode_to_vec(&spec, bincode::config::standard()).unwrap();
        let (back, _): (TaskSpec, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
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
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: AgentSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn restart_policy_bincode_roundtrip() {
        let policy = RestartPolicy {
            condition: RestartCondition::OnFailure,
            delay_secs: 5,
            max_attempts: Some(3),
        };
        let bytes = bincode::serde::encode_to_vec(&policy, bincode::config::standard()).unwrap();
        let (back, _): (RestartPolicy, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
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
        let bytes = bincode::serde::encode_to_vec(&spec, bincode::config::standard()).unwrap();
        let (back, _): (TaskSpec, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back, spec);
    }
}
