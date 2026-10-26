//! Translation between Docker Engine API types and Minga state types.
//!
//! Two directions:
//!
//! - **Inbound** ([`service_spec_to_state`]): Docker `ServiceSpec` (from
//!   `POST /services/create`) → Minga `Service`. Used when a Docker client
//!   creates a service.
//!
//! - **Outbound** ([`service_to_docker`], [`node_to_docker`],
//!   [`task_to_docker`], …): Minga state types → Docker API response
//!   types. Used when a Docker client lists or inspects resources.

use std::collections::HashMap;

use chrono::Utc;
use minga_state::types::{
    ClusterNode, Config as MingaConfig, Network, NetworkId, NodeId, NodeStatus as MingaNodeStatus,
    PortMapping, ResourceSpec, Secret as MingaSecret, Service, ServiceMode, Task, TaskState,
};

use crate::models;

// ---------------------------------------------------------------------------
// Inbound: Docker → Minga
// ---------------------------------------------------------------------------

/// Translates a Docker `ServiceSpec` into a Minga `Service`.
///
/// The Docker service `Name` (e.g. `mystack_web`) becomes the Minga
/// service ID. The stack namespace label (`com.docker.stack.namespace`)
/// is preserved in the service's env as `MINGA_STACK`.
pub fn service_spec_to_state(spec: &models::ServiceSpec) -> Result<Service, String> {
    let name = spec
        .Name
        .clone()
        .ok_or_else(|| "service spec missing Name".to_string())?;

    let container = spec
        .TaskTemplate
        .ContainerSpec
        .as_ref()
        .ok_or_else(|| format!("service '{name}': TaskTemplate.ContainerSpec is required"))?;

    if container.Image.is_empty() {
        return Err(format!("service '{name}': ContainerSpec.Image is required"));
    }

    // Parse env list ("KEY=VAL") into a map.
    let mut env: HashMap<String, String> = HashMap::new();
    for e in &container.Env {
        if let Some((k, v)) = e.split_once('=') {
            env.insert(k.to_string(), v.to_string());
        } else {
            env.insert(e.clone(), String::new());
        }
    }

    // Stack namespace label.
    if let Some(stack) = spec.Labels.get("com.docker.stack.namespace") {
        env.entry("MINGA_STACK".to_string())
            .or_insert_with(|| stack.clone());
    }

    // Resources.
    let mut resources = ResourceSpec::default();
    if let Some(res) = &spec.TaskTemplate.Resources {
        if let Some(limits) = &res.Limits {
            if let Some(nano_cpus) = limits.NanoCPUs {
                resources.cpus = nano_cpus as f64 / 1_000_000_000.0;
            }
            if let Some(mem) = limits.MemoryBytes {
                resources.memory_bytes = mem.max(0) as u64;
            }
        }
    }

    // Networks from TaskTemplate.
    let networks: Vec<NetworkId> = spec
        .TaskTemplate
        .Networks
        .iter()
        .filter_map(|n| n.Target.clone())
        .collect();

    // Ports from EndpointSpec.
    let ports: Vec<PortMapping> = spec
        .EndpointSpec
        .as_ref()
        .map(|ep| {
            ep.Ports
                .iter()
                .filter_map(|p| {
                    p.TargetPort.map(|target| PortMapping {
                        target: target as u16,
                        published: p.PublishedPort.map(|pp| pp as u16),
                        protocol: p.Protocol.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Mode and replicas.
    let (replicas, mode) = match &spec.Mode {
        Some(models::ServiceMode::Replicated { Replicas }) => {
            (*Replicas as u32, ServiceMode::Replicated)
        }
        Some(models::ServiceMode::Global {}) => (0, ServiceMode::Global),
        Some(models::ServiceMode::ReplicatedJob { Replicas }) => {
            (*Replicas as u32, ServiceMode::Replicated)
        }
        Some(models::ServiceMode::GlobalJob { .. }) => (0, ServiceMode::Global),
        None => (1, ServiceMode::Replicated),
    };

    // Placement constraints from TaskTemplate (not in ServiceSpec directly,
    // but Docker CLI sends them in TaskSpec for tasks; for services they're
    // typically not set, so we default to empty).
    let constraints: Vec<String> = Vec::new();

    Ok(Service {
        id: name.clone(),
        name,
        image: container.Image.clone(),
        replicas,
        mode,
        constraints,
        env,
        ports,
        networks,
        resources,
        agent: None,
    })
}

/// Translates a Docker `NetworkCreateRequest` into a Minga `Network`.
pub fn network_create_to_state(req: &models::NetworkCreateRequest) -> Network {
    let subnet = req
        .IPAM
        .as_ref()
        .and_then(|ipam| ipam.Config.first())
        .and_then(|c| c.Subnet.clone());

    Network {
        id: req.Name.clone(),
        name: req.Name.clone(),
        driver: req.Driver.clone().unwrap_or_else(|| "overlay".to_string()),
        attachable: req.Attachable.unwrap_or(true),
        subnet,
        wg_port: None,
    }
}

/// Translates a Docker `SecretSpec` into a Minga `Secret`.
pub fn secret_spec_to_state(spec: &models::SecretSpec) -> MingaSecret {
    MingaSecret {
        id: spec.Name.clone(),
        name: spec.Name.clone(),
    }
}

/// Translates a Docker `ConfigSpec` into a Minga `Config`.
pub fn config_spec_to_state(spec: &models::ConfigSpec) -> MingaConfig {
    MingaConfig {
        id: spec.Name.clone(),
        name: spec.Name.clone(),
    }
}

// ---------------------------------------------------------------------------
// Outbound: Minga → Docker
// ---------------------------------------------------------------------------

/// Translates a Minga `Service` into a Docker `Service` API response.
pub fn service_to_docker(svc: &Service) -> models::Service {
    let mut labels: HashMap<String, String> = HashMap::new();
    if let Some(stack) = svc.env.get("MINGA_STACK") {
        labels.insert("com.docker.stack.namespace".to_string(), stack.clone());
    }

    let container_spec = models::ContainerSpec {
        Image: svc.image.clone(),
        Env: svc
            .env
            .iter()
            .filter(|(k, _)| k.as_str() != "MINGA_STACK")
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    format!("{k}={v}")
                }
            })
            .collect(),
        ..Default::default()
    };

    let mut limits = models::ResourceSpec::default();
    if svc.resources.cpus > 0.0 {
        limits.NanoCPUs = Some((svc.resources.cpus * 1_000_000_000.0) as i64);
    }
    if svc.resources.memory_bytes > 0 {
        limits.MemoryBytes = Some(svc.resources.memory_bytes as i64);
    }
    let resources = models::Resources {
        Limits: Some(limits),
        Reservations: None,
    };

    let networks: Vec<models::ServiceNetwork> = svc
        .networks
        .iter()
        .map(|n| models::ServiceNetwork {
            Target: Some(n.clone()),
            Aliases: vec![svc.name.clone()],
            DriverOpts: HashMap::new(),
            IPv4Address: None,
            IPv6Address: None,
        })
        .collect();

    let mode = match svc.mode {
        ServiceMode::Replicated => models::ServiceMode::Replicated {
            Replicas: svc.replicas as u64,
        },
        ServiceMode::Global => models::ServiceMode::Global {},
    };

    let ports: Vec<models::PortConfig> = svc
        .ports
        .iter()
        .map(|p| models::PortConfig {
            Name: None,
            Protocol: p.protocol.clone(),
            TargetPort: Some(p.target as u32),
            PublishedPort: p.published.map(|pp| pp as u32),
            PublishMode: Some("ingress".to_string()),
        })
        .collect();

    let endpoint_spec = if ports.is_empty() {
        None
    } else {
        Some(models::EndpointSpec {
            Mode: Some("vip".to_string()),
            Ports: ports,
        })
    };

    models::Service {
        ID: svc.id.clone(),
        Version: models::Version { Index: Some(1) },
        CreatedAt: Some(Utc::now()),
        UpdatedAt: Some(Utc::now()),
        Spec: models::ServiceSpec {
            Name: Some(svc.name.clone()),
            Labels: labels,
            TaskTemplate: models::TaskTemplate {
                ContainerSpec: Some(container_spec),
                Resources: Some(resources),
                RestartPolicy: Some(models::RestartPolicy {
                    Condition: Some("any".to_string()),
                    ..Default::default()
                }),
                Networks: networks.clone(),
                LogDriver: None,
                ForceUpdate: None,
            },
            Mode: Some(mode),
            UpdateConfig: None,
            RollbackConfig: None,
            Networks: networks,
            EndpointSpec: endpoint_spec,
        },
        Endpoint: None,
        UpdateStatus: Some(models::UpdateStatus {
            State: "completed".to_string(),
            StartedAt: None,
            CompletedAt: Some(Utc::now()),
            Message: None,
        }),
        TaskList: vec![],
    }
}

/// Translates a Minga `ClusterNode` into a Docker `Node` API response.
pub fn node_to_docker(node: &ClusterNode, is_leader: bool) -> models::Node {
    let role = if is_leader { "manager" } else { "worker" };
    let state = match node.status {
        MingaNodeStatus::Joining => "down",
        MingaNodeStatus::Active => "ready",
        MingaNodeStatus::Draining => "down",
        MingaNodeStatus::Left => "down",
    };

    let mut labels = node.labels.clone();
    labels.insert("minga.node.id".to_string(), node.id.to_string());

    let manager_status = if is_leader {
        Some(models::ManagerStatus {
            Leader: Some(true),
            Reachability: Some("reachable".to_string()),
            Addr: Some(node.address.clone()),
        })
    } else {
        None
    };

    models::Node {
        ID: node.id.to_string(),
        Version: models::Version { Index: Some(1) },
        CreatedAt: Some(Utc::now()),
        UpdatedAt: Some(Utc::now()),
        Spec: models::NodeSpec {
            Name: Some(format!("node-{}", node.id)),
            Labels: labels,
            Role: role.to_string(),
            Availability: "active".to_string(),
        },
        Description: models::NodeDescription {
            Hostname: Some(format!("node-{}", node.id)),
            Platform: models::Platform {
                Architecture: std::env::consts::ARCH.to_string(),
                OS: std::env::consts::OS.to_string(),
            },
            Resources: models::NodeResources {
                NanoCPUs: Some((node.capacity.cpus * 1_000_000_000.0) as i64),
                MemoryBytes: Some(node.capacity.memory_bytes as i64),
                GenericResources: HashMap::new(),
            },
            Engine: models::EngineDescription {
                EngineVersion: format!("{}-minga", models::MINGA_VERSION),
                Plugins: vec![],
                Labels: HashMap::new(),
            },
            TLSInfo: None,
        },
        Status: models::NodeStatus {
            State: state.to_string(),
            Message: None,
            Addr: Some(node.address.clone()),
        },
        ManagerStatus: manager_status,
    }
}

/// Translates a Minga `Task` into a Docker `Task` API response.
pub fn task_to_docker(task: &Task) -> models::Task {
    let (state, desired_state) = match task.state {
        TaskState::Pending => ("pending", "running"),
        TaskState::Assigned => ("assigned", "running"),
        TaskState::Starting => ("preparing", "running"),
        TaskState::Running => ("running", "running"),
        TaskState::Stopping => ("shutdown", "shutdown"),
        TaskState::Stopped => ("complete", "shutdown"),
        TaskState::Failed => ("failed", "shutdown"),
    };

    let slot = task
        .id
        .rsplit('.')
        .next()
        .and_then(|s| s.parse::<i32>().ok());

    let container_spec = models::ContainerSpec {
        Image: task.spec.image.clone(),
        Env: task
            .spec
            .env
            .iter()
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    format!("{k}={v}")
                }
            })
            .collect(),
        ..Default::default()
    };

    models::Task {
        ID: task.id.clone(),
        Version: models::Version { Index: Some(1) },
        CreatedAt: Some(Utc::now()),
        UpdatedAt: Some(Utc::now()),
        Name: Some(format!("{}.{}", task.service_id, slot.unwrap_or(0))),
        Labels: HashMap::new(),
        Spec: models::TaskSpec {
            ContainerSpec: Some(container_spec),
            Resources: None,
            RestartPolicy: None,
            Networks: vec![],
            LogDriver: None,
            ForceUpdate: None,
            Runtime: None,
            Placement: None,
            Platforms: vec![],
        },
        ServiceID: task.service_id.clone(),
        Slot: slot,
        NodeID: task.node_id.to_string(),
        AssignedGenericResources: None,
        Status: models::TaskStatus {
            Timestamp: Some(Utc::now()),
            State: state.to_string(),
            Message: None,
            Err: None,
            ContainerStatus: None,
        },
        DesiredState: desired_state.to_string(),
    }
}

/// Translates a Minga `Network` into a Docker `NetworkResource` API response.
pub fn network_to_docker(net: &Network) -> models::NetworkResource {
    let ipam_config = net.subnet.as_ref().map(|s| models::IPAMConfig {
        Subnet: Some(s.clone()),
        IPRange: None,
        Gateway: None,
        AuxiliaryAddresses: HashMap::new(),
    });

    models::NetworkResource {
        Name: net.name.clone(),
        ID: net.id.clone(),
        Created: Some(Utc::now()),
        Scope: "swarm".to_string(),
        Driver: net.driver.clone(),
        EnableIPv6: false,
        IPAM: models::IPAM {
            Driver: Some("default".to_string()),
            Config: ipam_config.into_iter().collect(),
            Options: HashMap::new(),
        },
        Internal: false,
        Attachable: net.attachable,
        Ingress: false,
        Labels: HashMap::new(),
        Options: HashMap::new(),
        Containers: HashMap::new(),
    }
}

/// Translates a Minga `Secret` into a Docker `Secret` API response.
pub fn secret_to_docker(secret: &MingaSecret) -> models::Secret {
    models::Secret {
        ID: secret.id.clone(),
        Version: models::Version { Index: Some(1) },
        CreatedAt: Some(Utc::now()),
        UpdatedAt: Some(Utc::now()),
        Spec: models::SecretSpecResponse {
            Name: secret.name.clone(),
            Labels: HashMap::new(),
            Templating: None,
            Driver: None,
        },
    }
}

/// Translates a Minga `Config` into a Docker `Config` API response.
pub fn config_to_docker(config: &MingaConfig) -> models::Config {
    models::Config {
        ID: config.id.clone(),
        Version: models::Version { Index: Some(1) },
        CreatedAt: Some(Utc::now()),
        UpdatedAt: Some(Utc::now()),
        Spec: models::ConfigSpecResponse {
            Name: config.name.clone(),
            Labels: HashMap::new(),
            Templating: None,
            Driver: None,
        },
    }
}

/// Cluster stats for building a `SystemInfo` response.
pub struct ClusterStats {
    pub node_id: NodeId,
    pub node_addr: String,
    pub is_leader: bool,
    pub total_nodes: usize,
    pub manager_count: usize,
    pub running_tasks: usize,
    pub total_tasks: usize,
    pub services_count: usize,
}

/// Builds a `SystemInfo` response from cluster state.
pub fn system_info(stats: &ClusterStats) -> models::SystemInfo {
    let node_id = stats.node_id;
    let node_addr = &stats.node_addr;
    let is_leader = stats.is_leader;
    let total_nodes = stats.total_nodes;
    let manager_count = stats.manager_count;
    let running_tasks = stats.running_tasks;
    let total_tasks = stats.total_tasks;
    let services_count = stats.services_count;
    let swarm = models::SwarmInfo {
        NodeID: node_id.to_string(),
        NodeAddr: Some(node_addr.to_string()),
        LocalNodeState: "active".to_string(),
        ControlAvailable: is_leader,
        Error: None,
        RemoteManagers: None,
        Cluster: Some(models::ClusterInfo {
            ID: "minga-cluster".to_string(),
            Version: models::Version { Index: Some(1) },
            CreatedAt: Some(Utc::now()),
            UpdatedAt: Some(Utc::now()),
            Spec: models::ClusterSpec {
                Name: "default".to_string(),
                Labels: HashMap::new(),
                Orchestration: models::OrchestrationSpec {
                    TaskHistoryRetentionLimit: Some(5),
                },
                Raft: models::RaftSpec {
                    SnapshotInterval: Some(10000),
                    KeepOldSnapshots: Some(0),
                    LogEntriesForSlowFollowers: Some(500),
                    ElectionTick: Some(3),
                    HeartbeatTick: Some(1),
                },
                Dispatcher: Some(models::DispatcherSpec {
                    HeartbeatPeriod: Some(5_000_000_000),
                }),
                CAConfig: None,
                TaskDefaults: None,
                EncryptionConfig: None,
            },
            RootRotationInProgress: Some(false),
            DataPathPort: Some(4789),
            DefaultAddrPool: Some(vec!["10.100.0.0/16".to_string()]),
            SubnetSize: Some(24),
        }),
        Managers: Some(manager_count as i32),
        Nodes: Some(total_nodes as i32),
    };

    models::SystemInfo {
        ID: format!("MING:{node_id:024X}"),
        Containers: Some(total_tasks as i32),
        ContainersRunning: Some(running_tasks as i32),
        ContainersPaused: Some(0),
        ContainersStopped: Some((total_tasks - running_tasks) as i32),
        Images: Some(services_count as i32),
        Driver: "overlay2".to_string(),
        LoggingDriver: Some("json-file".to_string()),
        CgroupDriver: Some("systemd".to_string()),
        CgroupVersion: Some("2".to_string()),
        Plugins: Some(models::PluginsInfo {
            Volume: vec!["local".to_string()],
            Network: vec!["overlay".to_string(), "bridge".to_string()],
            Authorization: vec![],
        }),
        MemoryLimit: Some(true),
        SwapLimit: Some(true),
        KernelMemoryTCP: Some(true),
        OomKillDisable: Some(false),
        Swarm: swarm,
        ServerVersion: format!("{}-minga", models::MINGA_VERSION),
        OperatingSystem: format!("Minga ({})", std::env::consts::OS),
        OSType: Some(std::env::consts::OS.to_string()),
        Architecture: std::env::consts::ARCH.to_string(),
        Name: Some(format!("minga-{node_id}")),
        DockerRootDir: Some("/var/lib/minga".to_string()),
        ExperimentalBuild: Some(false),
        LiveRestoreEnabled: Some(false),
    }
}

/// Builds a `VersionInfo` response.
pub fn version_info() -> models::VersionInfo {
    models::VersionInfo {
        Platform: models::PlatformInfo {
            Name: "Minga".to_string(),
        },
        Components: vec![models::ComponentVersion {
            Name: "Engine".to_string(),
            Version: models::MINGA_VERSION.to_string(),
            Details: None,
        }],
        Version: models::MINGA_VERSION.to_string(),
        ApiVersion: models::API_VERSION.to_string(),
        MinAPIVersion: models::MIN_API_VERSION.to_string(),
        GitCommit: None,
        GoVersion: None,
        Os: std::env::consts::OS.to_string(),
        Arch: std::env::consts::ARCH.to_string(),
        KernelVersion: None,
        BuildTime: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::{NodeStatus as MingaNodeStatus, TaskSpec};

    fn sample_docker_spec() -> models::ServiceSpec {
        let mut labels = HashMap::new();
        labels.insert(
            "com.docker.stack.namespace".to_string(),
            "mystack".to_string(),
        );

        models::ServiceSpec {
            Name: Some("mystack_web".to_string()),
            Labels: labels,
            TaskTemplate: models::TaskTemplate {
                ContainerSpec: Some(models::ContainerSpec {
                    Image: "nginx:1.25".to_string(),
                    Env: vec!["FOO=bar".to_string(), "EMPTY".to_string()],
                    ..Default::default()
                }),
                Resources: Some(models::Resources {
                    Limits: Some(models::ResourceSpec {
                        NanoCPUs: Some(500_000_000),
                        MemoryBytes: Some(536_870_912),
                        ..Default::default()
                    }),
                    Reservations: None,
                }),
                Networks: vec![models::ServiceNetwork {
                    Target: Some("mystack_default".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            Mode: Some(models::ServiceMode::Replicated { Replicas: 3 }),
            EndpointSpec: Some(models::EndpointSpec {
                Mode: Some("vip".to_string()),
                Ports: vec![models::PortConfig {
                    Name: None,
                    Protocol: "tcp".to_string(),
                    TargetPort: Some(80),
                    PublishedPort: Some(8080),
                    PublishMode: Some("ingress".to_string()),
                }],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn service_spec_to_state_basic() {
        let spec = sample_docker_spec();
        let svc = service_spec_to_state(&spec).unwrap();

        assert_eq!(svc.id, "mystack_web");
        assert_eq!(svc.name, "mystack_web");
        assert_eq!(svc.image, "nginx:1.25");
        assert_eq!(svc.replicas, 3);
        assert_eq!(svc.mode, ServiceMode::Replicated);
        assert_eq!(svc.env.get("FOO").unwrap(), "bar");
        assert_eq!(svc.env.get("EMPTY").unwrap(), "");
        assert_eq!(svc.env.get("MINGA_STACK").unwrap(), "mystack");
        assert!((svc.resources.cpus - 0.5).abs() < 0.001);
        assert_eq!(svc.resources.memory_bytes, 536_870_912);
        assert_eq!(svc.networks, vec!["mystack_default".to_string()]);
        assert_eq!(svc.ports.len(), 1);
        assert_eq!(svc.ports[0].target, 80);
        assert_eq!(svc.ports[0].published, Some(8080));
        assert_eq!(svc.ports[0].protocol, "tcp");
    }

    #[test]
    fn service_spec_to_state_global_mode() {
        let mut spec = sample_docker_spec();
        spec.Mode = Some(models::ServiceMode::Global {});
        let svc = service_spec_to_state(&spec).unwrap();
        assert_eq!(svc.mode, ServiceMode::Global);
        assert_eq!(svc.replicas, 0);
    }

    #[test]
    fn service_spec_to_state_missing_name() {
        let spec = models::ServiceSpec {
            Name: None,
            ..Default::default()
        };
        let err = service_spec_to_state(&spec).unwrap_err();
        assert!(err.contains("missing Name"));
    }

    #[test]
    fn service_spec_to_state_missing_container() {
        let spec = models::ServiceSpec {
            Name: Some("test".to_string()),
            TaskTemplate: models::TaskTemplate {
                ContainerSpec: None,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = service_spec_to_state(&spec).unwrap_err();
        assert!(err.contains("ContainerSpec is required"));
    }

    #[test]
    fn service_spec_to_state_missing_image() {
        let spec = models::ServiceSpec {
            Name: Some("test".to_string()),
            TaskTemplate: models::TaskTemplate {
                ContainerSpec: Some(models::ContainerSpec {
                    Image: String::new(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let err = service_spec_to_state(&spec).unwrap_err();
        assert!(err.contains("Image is required"));
    }

    #[test]
    fn service_spec_to_state_default_mode() {
        let mut spec = sample_docker_spec();
        spec.Mode = None;
        let svc = service_spec_to_state(&spec).unwrap();
        assert_eq!(svc.replicas, 1);
        assert_eq!(svc.mode, ServiceMode::Replicated);
    }

    #[test]
    fn service_spec_to_state_no_ports() {
        let mut spec = sample_docker_spec();
        spec.EndpointSpec = None;
        let svc = service_spec_to_state(&spec).unwrap();
        assert!(svc.ports.is_empty());
    }

    #[test]
    fn service_roundtrip() {
        let spec = sample_docker_spec();
        let svc = service_spec_to_state(&spec).unwrap();
        let docker_svc = service_to_docker(&svc);

        assert_eq!(docker_svc.ID, svc.id);
        assert_eq!(docker_svc.Spec.Name.as_deref(), Some(svc.name.as_str()));
        assert_eq!(
            docker_svc
                .Spec
                .TaskTemplate
                .ContainerSpec
                .as_ref()
                .unwrap()
                .Image,
            svc.image
        );
        assert_eq!(
            docker_svc
                .Spec
                .Labels
                .get("com.docker.stack.namespace")
                .unwrap(),
            "mystack"
        );
        match docker_svc.Spec.Mode {
            Some(models::ServiceMode::Replicated { Replicas }) => {
                assert_eq!(Replicas, 3);
            }
            _ => panic!("expected Replicated"),
        }
    }

    #[test]
    fn service_to_docker_env_excludes_minga_stack() {
        let svc = Service {
            id: "test".to_string(),
            name: "test".to_string(),
            image: "nginx".to_string(),
            replicas: 1,
            mode: ServiceMode::Replicated,
            constraints: vec![],
            env: {
                let mut e = HashMap::new();
                e.insert("FOO".to_string(), "bar".to_string());
                e.insert("MINGA_STACK".to_string(), "mystack".to_string());
                e
            },
            ports: vec![],
            networks: vec![],
            resources: ResourceSpec::default(),
            agent: None,
        };
        let docker = service_to_docker(&svc);
        let env = &docker.Spec.TaskTemplate.ContainerSpec.as_ref().unwrap().Env;
        assert!(env.contains(&"FOO=bar".to_string()));
        assert!(!env.iter().any(|e| e.starts_with("MINGA_STACK")));
    }

    #[test]
    fn node_to_docker_leader() {
        let node = ClusterNode {
            id: 1,
            address: "10.0.0.1:50051".to_string(),
            labels: HashMap::new(),
            status: MingaNodeStatus::Active,
            capacity: ResourceSpec {
                cpus: 8.0,
                memory_bytes: 32 * 1024 * 1024 * 1024,
                gpus: 1,
            },
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
        };
        let docker = node_to_docker(&node, true);
        assert_eq!(docker.ID, "1");
        assert_eq!(docker.Spec.Role, "manager");
        assert_eq!(docker.Status.State, "ready");
        assert_eq!(docker.ManagerStatus.as_ref().unwrap().Leader, Some(true)); // Option<bool> comparison is fine
        assert_eq!(docker.Description.Resources.NanoCPUs, Some(8_000_000_000));
    }

    #[test]
    fn node_to_docker_worker() {
        let node = ClusterNode {
            id: 2,
            address: "10.0.0.2:50051".to_string(),
            labels: HashMap::new(),
            status: MingaNodeStatus::Active,
            capacity: ResourceSpec::default(),
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
        };
        let docker = node_to_docker(&node, false);
        assert_eq!(docker.Spec.Role, "worker");
        assert!(docker.ManagerStatus.is_none());
    }

    #[test]
    fn node_to_docker_draining() {
        let node = ClusterNode {
            id: 3,
            address: "10.0.0.3:50051".to_string(),
            labels: HashMap::new(),
            status: MingaNodeStatus::Draining,
            capacity: ResourceSpec::default(),
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
        };
        let docker = node_to_docker(&node, false);
        assert_eq!(docker.Status.State, "down");
    }

    #[test]
    fn task_to_docker_running() {
        let task = Task {
            id: "svc1.1".to_string(),
            service_id: "svc1".to_string(),
            node_id: 1,
            state: TaskState::Running,
            spec: TaskSpec {
                image: "nginx:latest".to_string(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec::default(),
                ..Default::default()
            },
            ips: HashMap::new(),
        };
        let docker = task_to_docker(&task);
        assert_eq!(docker.ID, "svc1.1");
        assert_eq!(docker.ServiceID, "svc1");
        assert_eq!(docker.NodeID, "1");
        assert_eq!(docker.Status.State, "running");
        assert_eq!(docker.DesiredState, "running");
        assert_eq!(docker.Slot, Some(1));
    }

    #[test]
    fn task_to_docker_pending() {
        let task = Task {
            id: "svc1.1".to_string(),
            service_id: "svc1".to_string(),
            node_id: 1,
            state: TaskState::Pending,
            spec: TaskSpec {
                image: "nginx".to_string(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec::default(),
                ..Default::default()
            },
            ips: HashMap::new(),
        };
        let docker = task_to_docker(&task);
        assert_eq!(docker.Status.State, "pending");
        assert_eq!(docker.DesiredState, "running");
    }

    #[test]
    fn task_to_docker_failed() {
        let task = Task {
            id: "svc1.2".to_string(),
            service_id: "svc1".to_string(),
            node_id: 2,
            state: TaskState::Failed,
            spec: TaskSpec {
                image: "nginx".to_string(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec::default(),
                ..Default::default()
            },
            ips: HashMap::new(),
        };
        let docker = task_to_docker(&task);
        assert_eq!(docker.Status.State, "failed");
        assert_eq!(docker.DesiredState, "shutdown");
    }

    #[test]
    fn network_to_docker_with_subnet() {
        let net = Network {
            id: "mystack_default".to_string(),
            name: "mystack_default".to_string(),
            driver: "overlay".to_string(),
            attachable: true,
            subnet: Some("10.100.0.0/24".to_string()),
            wg_port: None,
        };
        let docker = network_to_docker(&net);
        assert_eq!(docker.Name, "mystack_default");
        assert_eq!(docker.Driver, "overlay");
        assert_eq!(docker.IPAM.Config.len(), 1);
        assert_eq!(
            docker.IPAM.Config[0].Subnet.as_deref(),
            Some("10.100.0.0/24")
        );
    }

    #[test]
    fn network_to_docker_without_subnet() {
        let net = Network {
            id: "default".to_string(),
            name: "default".to_string(),
            driver: "bridge".to_string(),
            attachable: false,
            subnet: None,
            wg_port: None,
        };
        let docker = network_to_docker(&net);
        assert!(docker.IPAM.Config.is_empty());
    }

    #[test]
    fn network_create_to_state_translation() {
        let req = models::NetworkCreateRequest {
            Name: "mystack_default".to_string(),
            Driver: Some("overlay".to_string()),
            IPAM: Some(models::IPAM {
                Driver: Some("default".to_string()),
                Config: vec![models::IPAMConfig {
                    Subnet: Some("10.100.0.0/24".to_string()),
                    ..Default::default()
                }],
                Options: HashMap::new(),
            }),
            Attachable: Some(true),
            ..Default::default()
        };
        let net = network_create_to_state(&req);
        assert_eq!(net.id, "mystack_default");
        assert_eq!(net.driver, "overlay");
        assert_eq!(net.subnet.as_deref(), Some("10.100.0.0/24"));
        assert!(net.attachable);
    }

    #[test]
    fn network_create_to_state_defaults_translation() {
        let req = models::NetworkCreateRequest {
            Name: "mynet".to_string(),
            ..Default::default()
        };
        let net = network_create_to_state(&req);
        assert_eq!(net.driver, "overlay");
        assert!(net.attachable);
        assert!(net.subnet.is_none());
    }

    #[test]
    fn secret_spec_to_state_translation() {
        let spec = models::SecretSpec {
            Name: "my_secret".to_string(),
            Data: "c2VjcmV0".to_string(),
            ..Default::default()
        };
        let secret = secret_spec_to_state(&spec);
        assert_eq!(secret.id, "my_secret");
        assert_eq!(secret.name, "my_secret");
    }

    #[test]
    fn config_spec_to_state_translation() {
        let spec = models::ConfigSpec {
            Name: "my_config".to_string(),
            Data: "ZGF0YQ==".to_string(),
            ..Default::default()
        };
        let config = config_spec_to_state(&spec);
        assert_eq!(config.id, "my_config");
        assert_eq!(config.name, "my_config");
    }

    #[test]
    fn secret_to_docker_translation() {
        let secret = MingaSecret {
            id: "sec1".to_string(),
            name: "db_password".to_string(),
        };
        let docker = secret_to_docker(&secret);
        assert_eq!(docker.ID, "sec1");
        assert_eq!(docker.Spec.Name, "db_password");
    }

    #[test]
    fn config_to_docker_translation() {
        let config = MingaConfig {
            id: "cfg1".to_string(),
            name: "nginx_conf".to_string(),
        };
        let docker = config_to_docker(&config);
        assert_eq!(docker.ID, "cfg1");
        assert_eq!(docker.Spec.Name, "nginx_conf");
    }

    #[test]
    fn system_info_basic() {
        let stats = ClusterStats {
            node_id: 1,
            node_addr: "10.0.0.1:50051".to_string(),
            is_leader: true,
            total_nodes: 3,
            manager_count: 1,
            running_tasks: 5,
            total_tasks: 10,
            services_count: 4,
        };
        let info = system_info(&stats);
        assert_eq!(info.ID, format!("MING:{:024X}", 1u64));
        assert_eq!(info.Swarm.NodeID, "1");
        assert_eq!(info.Swarm.LocalNodeState, "active");
        assert!(info.Swarm.ControlAvailable);
        assert_eq!(info.Swarm.Nodes, Some(3));
        assert_eq!(info.Swarm.Managers, Some(1));
        assert_eq!(info.Containers, Some(10));
        assert_eq!(info.ContainersRunning, Some(5));
        assert!(info.Swarm.Cluster.is_some());
    }

    #[test]
    fn version_info_basic() {
        let v = version_info();
        assert_eq!(v.ApiVersion, "1.41");
        assert_eq!(v.MinAPIVersion, "1.24");
        assert_eq!(v.Platform.Name, "Minga");
        assert_eq!(v.Components.len(), 1);
        assert_eq!(v.Components[0].Name, "Engine");
    }
}
