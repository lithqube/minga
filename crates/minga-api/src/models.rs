//! Docker Engine API request/response models.
//!
//! These types mirror the Docker Engine API (v1.41) JSON shapes so that
//! `docker stack deploy`, `docker service ls`, and other Docker CLI
//! commands work against Minga without modification.
//!
//! Only the fields Minga actually uses are modeled in detail; everything
//! else uses `#[serde(default)]` and `skip_serializing_if` so unknown
//! fields from the client are accepted and ignored.

// Docker API uses PascalCase and SCREAMING_CASE field names in JSON.
// We keep them as-is in Rust for 1:1 mapping; serde handles the JSON.
#![allow(non_snake_case)]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Common
// ---------------------------------------------------------------------------

/// API version string (e.g. `1.41`).
pub const API_VERSION: &str = "1.41";
/// Minimum API version Minga supports.
pub const MIN_API_VERSION: &str = "1.24";
/// Minga version reported in `/version` and `/info`.
pub const MINGA_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Docker-compatible error response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
}

/// Object version (used in most Docker API responses).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Version {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Index: Option<u64>,
}

// ---------------------------------------------------------------------------
// Ping / Version / Info
// ---------------------------------------------------------------------------

/// Response body for `GET /version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub Platform: PlatformInfo,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Components: Vec<ComponentVersion>,
    pub Version: String,
    pub ApiVersion: String,
    pub MinAPIVersion: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub GitCommit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub GoVersion: Option<String>,
    pub Os: String,
    pub Arch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub KernelVersion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub BuildTime: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub Name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentVersion {
    pub Name: String,
    pub Version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Details: Option<HashMap<String, String>>,
}

/// Response body for `GET /info`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub ID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Containers: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainersRunning: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainersPaused: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainersStopped: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Images: Option<i32>,
    pub Driver: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub LoggingDriver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CgroupDriver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CgroupVersion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub Plugins: Option<PluginsInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub MemoryLimit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SwapLimit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub KernelMemoryTCP: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub OomKillDisable: Option<bool>,
    pub Swarm: SwarmInfo,
    pub ServerVersion: String,
    pub OperatingSystem: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub OSType: Option<String>,
    pub Architecture: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub DockerRootDir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ExperimentalBuild: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub LiveRestoreEnabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfo {
    #[serde(rename = "Type")]
    pub plugin_type: String,
    pub Name: String,
}

/// Docker `PluginsInfo` — grouped plugin lists by category.
/// Used in `SystemInfo.Plugins`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PluginsInfo {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Volume: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Network: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Authorization: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmInfo {
    pub NodeID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub NodeAddr: Option<String>,
    pub LocalNodeState: String,
    pub ControlAvailable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub RemoteManagers: Option<Vec<PeerNode>>,
    pub Cluster: Option<ClusterInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Managers: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Nodes: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerNode {
    pub NodeID: String,
    pub Addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub ID: String,
    pub Version: Version,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CreatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdatedAt: Option<DateTime<Utc>>,
    pub Spec: ClusterSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub RootRotationInProgress: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub DataPathPort: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub DefaultAddrPool: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SubnetSize: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSpec {
    pub Name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    pub Orchestration: OrchestrationSpec,
    pub Raft: RaftSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Dispatcher: Option<DispatcherSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CAConfig: Option<CAConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub TaskDefaults: Option<TaskDefaults>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub EncryptionConfig: Option<EncryptionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub TaskHistoryRetentionLimit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SnapshotInterval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub KeepOldSnapshots: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub LogEntriesForSlowFollowers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ElectionTick: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub HeartbeatTick: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatcherSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub HeartbeatPeriod: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CAConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub NodeCertExpiry: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ExternalCAs: Vec<ExternalCA>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SigningCACert: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SigningCAKey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ForceRotate: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalCA {
    pub Protocol: String,
    pub URL: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Options: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub CACert: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDefaults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub LogDriver: Option<Driver>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub AutoLockManagers: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Driver {
    pub Name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Options: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Response body for `GET /nodes` and `GET /nodes/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub ID: String,
    pub Version: Version,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CreatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdatedAt: Option<DateTime<Utc>>,
    pub Spec: NodeSpec,
    pub Description: NodeDescription,
    pub Status: NodeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ManagerStatus: Option<ManagerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Name: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    pub Role: String,
    pub Availability: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDescription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Hostname: Option<String>,
    pub Platform: Platform,
    pub Resources: NodeResources,
    pub Engine: EngineDescription,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub TLSInfo: Option<TLSInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Platform {
    pub Architecture: String,
    pub OS: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub NanoCPUs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub MemoryBytes: Option<i64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub GenericResources: HashMap<String, Vec<GenericResource>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericResource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub NamedResourceSpec: Option<NamedGenericResource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub DiscreteResourceSpec: Option<DiscreteGenericResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedGenericResource {
    pub Kind: String,
    pub Value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscreteGenericResource {
    pub Kind: String,
    pub Value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineDescription {
    pub EngineVersion: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Plugins: Vec<PluginInfo>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TLSInfo {
    pub TrustRoot: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CertIssuerPublicKey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CertIssuerSubject: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub State: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagerStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Leader: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Reachability: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Addr: Option<String>,
}

/// Query params for `GET /nodes`.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

/// Request body for `POST /services/create`.
/// This is the Docker `ServiceSpec`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Name: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    pub TaskTemplate: TaskTemplate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Mode: Option<ServiceMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdateConfig: Option<UpdateConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub RollbackConfig: Option<UpdateConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Networks: Vec<ServiceNetwork>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub EndpointSpec: Option<EndpointSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskTemplate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainerSpec: Option<ContainerSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Resources: Option<Resources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub RestartPolicy: Option<RestartPolicy>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Networks: Vec<ServiceNetwork>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub LogDriver: Option<Driver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ForceUpdate: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub Image: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Env: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Command: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Hostname: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Mounts: Vec<Mount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub User: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Privileges: Option<Privileges>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ReadOnly: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub Healthcheck: Option<Healthcheck>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Hosts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub StopGracePeriod: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub StopSignal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub TTY: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub OpenStdin: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Secrets: Option<Vec<SecretReference>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Configs: Option<Vec<ConfigReference>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mount {
    #[serde(rename = "Type")]
    pub mount_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Source: Option<String>,
    pub Target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ReadOnly: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub BindOptions: Option<BindOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub VolumeOptions: Option<VolumeOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub TmpfsOptions: Option<TmpfsOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Propagation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub NoCopy: Option<bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub DriverConfig: Option<Driver>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmpfsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SizeBytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Mode: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Privileges {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CredentialSpec: Option<CredentialSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub SELinuxContext: Option<SELinuxContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Config: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub File: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Registry: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SELinuxContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Disable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub User: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Healthcheck {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Test: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Retries: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub StartPeriod: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub StartInterval: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretReference {
    pub SecretID: String,
    pub SecretName: String,
    pub File: Option<FileReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileReference {
    pub Name: String,
    pub UID: String,
    pub GID: String,
    pub Mode: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigReference {
    pub ConfigID: String,
    pub ConfigName: String,
    pub File: Option<FileReference>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Limits: Option<ResourceSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Reservations: Option<ResourceSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub NanoCPUs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub MemoryBytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub GenericResources: Option<Vec<GenericResource>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Pids: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestartPolicy {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Delay: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub MaxAttempts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Window: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceNetwork {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Target: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Aliases: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub DriverOpts: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub IPv4Address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub IPv6Address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceMode {
    Replicated { Replicas: u64 },
    Global {},
    ReplicatedJob { Replicas: u64 },
    GlobalJob { Replicas: u64 },
}

impl Default for ServiceMode {
    fn default() -> Self {
        ServiceMode::Replicated { Replicas: 1 }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Parallelism: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Delay: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub FailureAction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Monitor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub MaxFailureRatio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Order: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Mode: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Ports: Vec<PortConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PortConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Name: Option<String>,
    pub Protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub TargetPort: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub PublishedPort: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub PublishMode: Option<String>,
}

/// Response body for `GET /services` and `GET /services/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub ID: String,
    pub Version: Version,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CreatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdatedAt: Option<DateTime<Utc>>,
    pub Spec: ServiceSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Endpoint: Option<Endpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdateStatus: Option<UpdateStatus>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub TaskList: Vec<Task>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Spec: Option<EndpointSpec>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Ports: Vec<PortConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub VirtualIPs: Option<Vec<EndpointVirtualIP>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointVirtualIP {
    pub NetworkID: String,
    pub Addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStatus {
    pub State: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub StartedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CompletedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Message: Option<String>,
}

/// Response body for `POST /services/create`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCreateResponse {
    pub ID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Warning: Option<String>,
}

/// Request body for `POST /services/{id}/update`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceUpdateQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registryAuthFrom: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback: Option<String>,
}

/// Query params for `GET /services`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<bool>,
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

/// Response body for `GET /tasks` and `GET /tasks/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub ID: String,
    pub Version: Version,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CreatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Name: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    pub Spec: TaskSpec,
    pub ServiceID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Slot: Option<i32>,
    pub NodeID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub AssignedGenericResources: Option<Vec<GenericResource>>,
    pub Status: TaskStatus,
    pub DesiredState: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainerSpec: Option<ContainerSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Resources: Option<Resources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub RestartPolicy: Option<RestartPolicy>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Networks: Vec<ServiceNetwork>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub LogDriver: Option<Driver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ForceUpdate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub Runtime: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub Placement: Option<Placement>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Platforms: Vec<Platform>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Placement {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Constraints: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Preferences: Vec<PlacementPreference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Platforms: Option<Vec<Platform>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub MaxReplicas: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementPreference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Spread: Option<PlacementSpread>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementSpread {
    pub SpreadDescriptor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Timestamp: Option<DateTime<Utc>>,
    pub State: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainerStatus: Option<ContainerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ContainerID: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub PID: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ExitCode: Option<i32>,
}

/// Query params for `GET /tasks`.
#[derive(Debug, Clone, Deserialize)]
pub struct TaskListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all: Option<bool>,
}

// ---------------------------------------------------------------------------
// Networks
// ---------------------------------------------------------------------------

/// Request body for `POST /networks/create`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkCreateRequest {
    pub Name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CheckDuplicate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Driver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub EnableIPv6: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub IPAM: Option<IPAM>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Internal: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Attachable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Ingress: Option<bool>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Options: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IPAM {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Driver: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub Config: Vec<IPAMConfig>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Options: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IPAMConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Subnet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub IPRange: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Gateway: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub AuxiliaryAddresses: HashMap<String, String>,
}

/// Response body for `POST /networks/create`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkCreateResponse {
    pub ID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Warning: Option<String>,
}

/// Response body for `GET /networks` and `GET /networks/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkResource {
    pub Name: String,
    pub ID: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Created: Option<DateTime<Utc>>,
    pub Scope: String,
    pub Driver: String,
    pub EnableIPv6: bool,
    pub IPAM: IPAM,
    pub Internal: bool,
    pub Attachable: bool,
    pub Ingress: bool,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Options: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Containers: HashMap<String, NetworkContainer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkContainer {
    pub Name: String,
    pub EndpointID: String,
    pub MacAddress: String,
    pub IPv4Address: String,
    pub IPv6Address: String,
}

/// Query params for `GET /networks`.
#[derive(Debug, Clone, Deserialize)]
pub struct NetworkListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verbose: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dangling: Option<bool>,
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// Request body for `POST /secrets/create`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretSpec {
    pub Name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    pub Data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Templating: Option<Driver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Driver: Option<Driver>,
}

/// Response body for `GET /secrets` and `GET /secrets/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secret {
    pub ID: String,
    pub Version: Version,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CreatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdatedAt: Option<DateTime<Utc>>,
    pub Spec: SecretSpecResponse,
}

/// Secret spec in responses (Data is not returned).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretSpecResponse {
    pub Name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Templating: Option<Driver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Driver: Option<Driver>,
}

/// Response body for `POST /secrets/create`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretCreateResponse {
    pub ID: String,
}

/// Query params for `GET /secrets`.
#[derive(Debug, Clone, Deserialize)]
pub struct SecretListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

/// Request body for `POST /configs/create`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigSpec {
    pub Name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    pub Data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Templating: Option<Driver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Driver: Option<Driver>,
}

/// Response body for `GET /configs` and `GET /configs/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub ID: String,
    pub Version: Version,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub CreatedAt: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub UpdatedAt: Option<DateTime<Utc>>,
    pub Spec: ConfigSpecResponse,
}

/// Config spec in responses (Data is not returned).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigSpecResponse {
    pub Name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub Labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Templating: Option<Driver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Driver: Option<Driver>,
}

/// Response body for `POST /configs/create`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigCreateResponse {
    pub ID: String,
}

/// Query params for `GET /configs`.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigListQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
}

// ---------------------------------------------------------------------------
// Filter parsing
// ---------------------------------------------------------------------------

/// Parsed Docker filter JSON: `{"label": ["key=value"], "name": ["foo"]}`.
pub type Filters = HashMap<String, Vec<String>>;

/// Parses a Docker `filters` query parameter (JSON-encoded).
pub fn parse_filters(raw: Option<&str>) -> Filters {
    raw.and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

/// Checks if a label map matches all `label` filters.
/// Filters can be `key` (existence) or `key=value`.
pub fn labels_match(labels: &HashMap<String, String>, filters: &[String]) -> bool {
    filters.iter().all(|f| {
        if let Some((k, v)) = f.split_once('=') {
            labels.get(k).map(|lv| lv == v).unwrap_or(false)
        } else {
            labels.contains_key(f)
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_filters_valid() {
        let raw = r#"{"label":["com.docker.stack.namespace=mystack"],"name":["web"]}"#;
        let f = parse_filters(Some(raw));
        assert_eq!(
            f.get("label").unwrap(),
            &vec!["com.docker.stack.namespace=mystack".to_string()]
        );
        assert_eq!(f.get("name").unwrap(), &vec!["web".to_string()]);
    }

    #[test]
    fn parse_filters_none() {
        let f = parse_filters(None);
        assert!(f.is_empty());
    }

    #[test]
    fn parse_filters_invalid_json() {
        let f = parse_filters(Some("not json"));
        assert!(f.is_empty());
    }

    #[test]
    fn labels_match_key_value() {
        let mut labels = HashMap::new();
        labels.insert(
            "com.docker.stack.namespace".to_string(),
            "mystack".to_string(),
        );
        labels.insert("env".to_string(), "prod".to_string());

        assert!(labels_match(
            &labels,
            &["com.docker.stack.namespace=mystack".to_string()]
        ));
        assert!(!labels_match(
            &labels,
            &["com.docker.stack.namespace=other".to_string()]
        ));
    }

    #[test]
    fn labels_match_existence() {
        let mut labels = HashMap::new();
        labels.insert("foo".to_string(), "bar".to_string());

        assert!(labels_match(&labels, &["foo".to_string()]));
        assert!(!labels_match(&labels, &["baz".to_string()]));
    }

    #[test]
    fn labels_match_multiple() {
        let mut labels = HashMap::new();
        labels.insert("a".to_string(), "1".to_string());
        labels.insert("b".to_string(), "2".to_string());

        assert!(labels_match(
            &labels,
            &["a=1".to_string(), "b=2".to_string()]
        ));
        assert!(!labels_match(
            &labels,
            &["a=1".to_string(), "b=3".to_string()]
        ));
    }

    #[test]
    fn service_spec_deserialize() {
        let json = r#"{
            "Name": "mystack_web",
            "Labels": {"com.docker.stack.namespace": "mystack"},
            "TaskTemplate": {
                "ContainerSpec": {
                    "Image": "nginx:1.25",
                    "Env": ["FOO=bar", "BAZ=qux"]
                },
                "Resources": {
                    "Limits": {"NanoCPUs": 500000000, "MemoryBytes": 536870912}
                },
                "RestartPolicy": {"Condition": "any"}
            },
            "Mode": {"Replicated": {"Replicas": 3}},
            "EndpointSpec": {
                "Ports": [{"Protocol": "tcp", "PublishedPort": 8080, "TargetPort": 80}]
            }
        }"#;
        let spec: ServiceSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.Name.as_deref(), Some("mystack_web"));
        assert_eq!(
            spec.Labels.get("com.docker.stack.namespace").unwrap(),
            "mystack"
        );
        let container = spec.TaskTemplate.ContainerSpec.as_ref().unwrap();
        assert_eq!(container.Image, "nginx:1.25");
        assert_eq!(container.Env, vec!["FOO=bar", "BAZ=qux"]);
        let limits = spec
            .TaskTemplate
            .Resources
            .as_ref()
            .unwrap()
            .Limits
            .as_ref()
            .unwrap();
        assert_eq!(limits.NanoCPUs, Some(500_000_000));
        assert_eq!(limits.MemoryBytes, Some(536_870_912));
        match spec.Mode {
            Some(ServiceMode::Replicated { Replicas }) => assert_eq!(Replicas, 3),
            _ => panic!("expected Replicated mode"),
        }
        let ports = &spec.EndpointSpec.as_ref().unwrap().Ports;
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].Protocol, "tcp");
        assert_eq!(ports[0].PublishedPort, Some(8080));
        assert_eq!(ports[0].TargetPort, Some(80));
    }

    #[test]
    fn service_mode_global() {
        let json = r#"{"Global": {}}"#;
        let mode: ServiceMode = serde_json::from_str(json).unwrap();
        assert!(matches!(mode, ServiceMode::Global {}));
    }

    #[test]
    fn network_create_deserialize() {
        let json = r#"{
            "Name": "mystack_default",
            "Driver": "overlay",
            "EnableIPv6": false,
            "IPAM": {
                "Driver": "default",
                "Config": [{"Subnet": "10.100.0.0/24", "Gateway": "10.100.0.1"}]
            },
            "Attachable": true,
            "Labels": {"com.docker.stack.namespace": "mystack"}
        }"#;
        let req: NetworkCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.Name, "mystack_default");
        assert_eq!(req.Driver.as_deref(), Some("overlay"));
        assert_eq!(req.Attachable, Some(true));
        let ipam = req.IPAM.unwrap();
        assert_eq!(ipam.Config.len(), 1);
        assert_eq!(ipam.Config[0].Subnet.as_deref(), Some("10.100.0.0/24"));
    }

    #[test]
    fn secret_spec_deserialize() {
        let json = r#"{
            "Name": "mystack_db_password",
            "Data": "c2VjcmV0MTIz",
            "Labels": {"com.docker.stack.namespace": "mystack"}
        }"#;
        let spec: SecretSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.Name, "mystack_db_password");
        assert_eq!(spec.Data, "c2VjcmV0MTIz");
    }

    #[test]
    fn config_spec_deserialize() {
        let json = r#"{
            "Name": "mystack_nginx_conf",
            "Data": "c2VydmVyIHsgbGlzdGVuIDgwOyB9",
            "Labels": {"com.docker.stack.namespace": "mystack"}
        }"#;
        let spec: ConfigSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.Name, "mystack_nginx_conf");
    }

    #[test]
    fn version_info_serialize() {
        let info = VersionInfo {
            Platform: PlatformInfo {
                Name: "Minga".to_string(),
            },
            Components: vec![],
            Version: "0.0.1".to_string(),
            ApiVersion: API_VERSION.to_string(),
            MinAPIVersion: MIN_API_VERSION.to_string(),
            GitCommit: None,
            GoVersion: None,
            Os: "linux".to_string(),
            Arch: "aarch64".to_string(),
            KernelVersion: None,
            BuildTime: None,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["Platform"]["Name"], "Minga");
        assert_eq!(json["ApiVersion"], "1.41");
        assert_eq!(json["MinAPIVersion"], "1.24");
    }

    #[test]
    fn service_serialize_roundtrip() {
        let svc = Service {
            ID: "abc123".to_string(),
            Version: Version { Index: Some(1) },
            CreatedAt: None,
            UpdatedAt: None,
            Spec: ServiceSpec {
                Name: Some("web".to_string()),
                ..Default::default()
            },
            Endpoint: None,
            UpdateStatus: None,
            TaskList: vec![],
        };
        let json = serde_json::to_string(&svc).unwrap();
        let back: Service = serde_json::from_str(&json).unwrap();
        assert_eq!(back.ID, "abc123");
        assert_eq!(back.Spec.Name.as_deref(), Some("web"));
    }

    #[test]
    fn error_response_serialize() {
        let err = ErrorResponse {
            message: "service not found".to_string(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["message"], "service not found");
    }

    #[test]
    fn task_serialize() {
        let task = Task {
            ID: "task1".to_string(),
            Version: Version { Index: Some(5) },
            CreatedAt: None,
            UpdatedAt: None,
            Name: Some("web.1".to_string()),
            Labels: HashMap::new(),
            Spec: TaskSpec {
                ContainerSpec: Some(ContainerSpec {
                    Image: "nginx:latest".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ServiceID: "svc1".to_string(),
            Slot: Some(1),
            NodeID: "node1".to_string(),
            AssignedGenericResources: None,
            Status: TaskStatus {
                Timestamp: None,
                State: "running".to_string(),
                Message: None,
                Err: None,
                ContainerStatus: None,
            },
            DesiredState: "running".to_string(),
        };
        let json = serde_json::to_value(&task).unwrap();
        assert_eq!(json["ID"], "task1");
        assert_eq!(json["ServiceID"], "svc1");
        assert_eq!(json["Status"]["State"], "running");
        assert_eq!(json["DesiredState"], "running");
    }

    #[test]
    fn node_serialize() {
        let node = Node {
            ID: "node1".to_string(),
            Version: Version { Index: Some(1) },
            CreatedAt: None,
            UpdatedAt: None,
            Spec: NodeSpec {
                Name: Some("manager1".to_string()),
                Labels: HashMap::new(),
                Role: "manager".to_string(),
                Availability: "active".to_string(),
            },
            Description: NodeDescription {
                Hostname: Some("manager1".to_string()),
                Platform: Platform {
                    Architecture: "aarch64".to_string(),
                    OS: "linux".to_string(),
                },
                Resources: NodeResources {
                    NanoCPUs: Some(8_000_000_000),
                    MemoryBytes: Some(34_359_738_368),
                    GenericResources: HashMap::new(),
                },
                Engine: EngineDescription {
                    EngineVersion: "25.0.0-minga".to_string(),
                    Plugins: vec![],
                    Labels: HashMap::new(),
                },
                TLSInfo: None,
            },
            Status: NodeStatus {
                State: "ready".to_string(),
                Message: None,
                Addr: Some("10.0.0.1:50051".to_string()),
            },
            ManagerStatus: Some(ManagerStatus {
                Leader: Some(true),
                Reachability: Some("reachable".to_string()),
                Addr: Some("10.0.0.1:2377".to_string()),
            }),
        };
        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(json["ID"], "node1");
        assert_eq!(json["Spec"]["Role"], "manager");
        assert_eq!(json["Status"]["State"], "ready");
        assert_eq!(json["ManagerStatus"]["Leader"], true);
    }

    #[test]
    fn network_resource_serialize() {
        let net = NetworkResource {
            Name: "mystack_default".to_string(),
            ID: "net1".to_string(),
            Created: None,
            Scope: "swarm".to_string(),
            Driver: "overlay".to_string(),
            EnableIPv6: false,
            IPAM: IPAM {
                Driver: Some("default".to_string()),
                Config: vec![IPAMConfig {
                    Subnet: Some("10.100.0.0/24".to_string()),
                    IPRange: None,
                    Gateway: Some("10.100.0.1".to_string()),
                    AuxiliaryAddresses: HashMap::new(),
                }],
                Options: HashMap::new(),
            },
            Internal: false,
            Attachable: true,
            Ingress: false,
            Labels: HashMap::new(),
            Options: HashMap::new(),
            Containers: HashMap::new(),
        };
        let json = serde_json::to_value(&net).unwrap();
        assert_eq!(json["Name"], "mystack_default");
        assert_eq!(json["Driver"], "overlay");
        assert_eq!(json["IPAM"]["Config"][0]["Subnet"], "10.100.0.0/24");
    }
}
