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
    /// IP addresses assigned to this task, keyed by network ID.
    #[serde(default)]
    pub ips: HashMap<NetworkId, String>,
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
        };
        let bytes = bincode::serde::encode_to_vec(&svc, bincode::config::standard()).unwrap();
        let (back, _): (Service, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back, svc);
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
}
