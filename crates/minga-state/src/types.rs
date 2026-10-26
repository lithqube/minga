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
    pub image: String,
    pub replicas: u32,
    pub mode: ServiceMode,
    /// Placement constraints, e.g. `node.labels.gpu == true`.
    pub constraints: Vec<String>,
    pub env: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub networks: Vec<NetworkId>,
    pub resources: ResourceSpec,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSpec {
    pub image: String,
    pub env: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub resources: ResourceSpec,
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
