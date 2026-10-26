//! Translates a Compose [`Project`] into cluster state entities.
//!
//! This is the deploy path: `minga deploy` loads a Compose file, the
//! scheduler translates it into state [`Service`]s, [`Network`]s,
//! [`Volume`]s, [`Secret`]s and [`Config`]s, and those are proposed to
//! Raft. The reconcile loop (see [`crate::reconcile`]) then turns the
//! desired services into tasks.
//!
//! Naming convention: a Compose service `inference` in project `plant-7`
//! becomes state service `plant-7.inference`. Networks and volumes are
//! prefixed with the project name and underscore: `plant-7_default`.

use std::collections::HashMap;

use minga_compose::{AgentExtension, DeployMode, DeviceCount, Project, DEFAULT_NETWORK};
use minga_state::types::{
    AgentSpec, Config as StateConfig, Network, ResourceSpec, Secret, Service, ServiceMode, Volume,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TranslateError {
    #[error("service `{0}`: {1}")]
    Service(String, String),
    #[error("invalid memory value {0:?} in service `{1}`: {2}")]
    Memory(String, String, String),
}

/// The full set of state entities translated from a Compose project.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TranslatedStack {
    pub services: Vec<Service>,
    pub networks: Vec<Network>,
    pub volumes: Vec<Volume>,
    pub secrets: Vec<Secret>,
    pub configs: Vec<StateConfig>,
}

/// Translates a loaded Compose project into state entities for a stack.
///
/// `stack` is the deploy name (e.g. `plant-7`). It prefixes every entity
/// ID so multiple stacks can coexist in the same cluster.
pub fn translate_project(
    project: &Project,
    stack: &str,
) -> Result<TranslatedStack, TranslateError> {
    let mut out = TranslatedStack::default();

    for (name, svc) in &project.services {
        out.services
            .push(translate_service(name, svc, stack, project)?);
    }

    out.networks = translate_networks(project, stack);
    out.volumes = translate_volumes(project, stack);
    out.secrets = translate_secrets(project, stack);
    out.configs = translate_configs(project, stack);

    Ok(out)
}

fn translate_service(
    name: &str,
    svc: &minga_compose::Service,
    stack: &str,
    project: &Project,
) -> Result<Service, TranslateError> {
    // Check for x-minga-agent extension — agent services don't need an image.
    let agent_ext = svc
        .agent_extension()
        .map_err(|e| TranslateError::Service(name.to_string(), e))?;

    let image = match &agent_ext {
        Some(_) => String::new(), // agent services have no container image
        None => svc.image.clone().filter(|i| !i.is_empty()).ok_or_else(|| {
            TranslateError::Service(name.to_string(), "`image` is required".into())
        })?,
    };

    let deploy = svc.deploy.clone().unwrap_or_default();

    let (replicas, mode) = match deploy.mode {
        DeployMode::Replicated => (deploy.replicas.unwrap_or(1), ServiceMode::Replicated),
        DeployMode::Global => (0, ServiceMode::Global),
        DeployMode::ReplicatedJob | DeployMode::GlobalJob => {
            return Err(TranslateError::Service(
                name.to_string(),
                "job modes are not supported yet".into(),
            ));
        }
    };

    let constraints = deploy
        .placement
        .as_ref()
        .map(|p| p.constraints.clone())
        .unwrap_or_default();

    // Environment: ListOrMap → HashMap. Keys with no value (pass-through
    // from the deploy environment) are resolved here from the process env.
    let env: HashMap<String, String> = svc
        .environment
        .0
        .iter()
        .map(|(k, v)| {
            let val = match v {
                Some(val) => val.clone(),
                None => std::env::var(k).unwrap_or_default(),
            };
            (k.clone(), val)
        })
        .collect();

    // Ports: take the start of each range (Phase 0 — no port range expansion).
    let ports = svc
        .ports
        .iter()
        .map(|p| minga_state::types::PortMapping {
            target: p.target.start,
            published: p.published.map(|pr| pr.start),
            protocol: p.protocol.clone(),
        })
        .collect();

    // Networks: service networks or the default network.
    let networks: Vec<String> = if svc.networks.is_empty() {
        vec![network_id(stack, DEFAULT_NETWORK)]
    } else {
        svc.networks.keys().map(|n| network_id(stack, n)).collect()
    };

    let resources = translate_resources(name, &deploy)?;

    // Validate that referenced networks exist in the project.
    for net_name in svc.networks.keys() {
        if net_name != DEFAULT_NETWORK && !project.networks.contains_key(net_name) {
            return Err(TranslateError::Service(
                name.to_string(),
                format!("network `{net_name}` is not declared under top-level `networks`"),
            ));
        }
    }

    // Build the agent spec from the extension, if present.
    let agent = agent_ext.map(agent_extension_to_spec);

    Ok(Service {
        id: format!("{stack}.{name}"),
        name: name.to_string(),
        image,
        replicas,
        mode,
        constraints,
        env,
        ports,
        networks,
        resources,
        agent,
    })
}

/// Converts a Compose `x-minga-agent` extension into a state `AgentSpec`.
fn agent_extension_to_spec(ext: AgentExtension) -> AgentSpec {
    AgentSpec {
        module_ref: ext.module,
        fuel_limit: ext.fuel_limit,
        memory_limit: ext.memory_limit,
        timeout_secs: ext.timeout,
        token_budget: ext.token_budget,
        egress_url: ext.egress_url,
        prompt: ext.prompt,
        sandbox_env: ext.env,
    }
}

fn translate_resources(
    name: &str,
    deploy: &minga_compose::Deploy,
) -> Result<ResourceSpec, TranslateError> {
    let Some(resources) = &deploy.resources else {
        return Ok(ResourceSpec::default());
    };
    let Some(limits) = &resources.limits else {
        return Ok(ResourceSpec::default());
    };

    let memory_bytes = match &limits.memory {
        Some(m) => {
            parse_memory(m).map_err(|e| TranslateError::Memory(m.clone(), name.to_string(), e))?
        }
        None => 0,
    };

    // Count GPU device requests.
    let gpus: u32 = limits
        .devices
        .iter()
        .filter(|d| d.capabilities.iter().any(|c| c == "gpu"))
        .map(|d| match d.count {
            Some(DeviceCount::All) => u32::MAX,
            Some(DeviceCount::Count(n)) => n,
            None => 1,
        })
        .sum();

    Ok(ResourceSpec {
        cpus: limits.cpus.unwrap_or(0.0),
        memory_bytes,
        gpus,
    })
}

/// Extract a subnet CIDR from a Compose IPAM config (raw YAML value).
/// Looks for `ipam.config[0].subnet`.
fn parse_ipam_subnet(ipam: &Option<serde_yaml::Value>) -> Option<String> {
    let ipam = ipam.as_ref()?;
    let config = ipam.get("config")?.as_sequence()?;
    let first = config.first()?;
    first.get("subnet")?.as_str().map(String::from)
}

fn translate_networks(project: &Project, stack: &str) -> Vec<Network> {
    let mut networks: Vec<Network> = project
        .networks
        .iter()
        .map(|(name, net)| Network {
            id: network_id(stack, name),
            name: net.name.clone().unwrap_or_else(|| network_id(stack, name)),
            driver: net.driver.clone().unwrap_or_else(|| "overlay".to_string()),
            attachable: net.attachable.unwrap_or(false),
            subnet: parse_ipam_subnet(&net.ipam),
            wg_port: None,
        })
        .collect();

    // Ensure the default network exists if any service uses it implicitly.
    let uses_default = project.services.values().any(|svc| svc.networks.is_empty());
    let default_id = network_id(stack, DEFAULT_NETWORK);
    if uses_default && !networks.iter().any(|n| n.id == default_id) {
        networks.push(Network {
            id: default_id.clone(),
            name: default_id,
            driver: "overlay".to_string(),
            attachable: false,
            subnet: None,
            wg_port: None,
        });
    }

    networks
}

fn translate_volumes(project: &Project, stack: &str) -> Vec<Volume> {
    project
        .volumes
        .iter()
        .map(|(name, vol)| Volume {
            id: volume_id(stack, name),
            name: vol.name.clone().unwrap_or_else(|| volume_id(stack, name)),
            driver: vol.driver.clone().unwrap_or_else(|| "local".to_string()),
        })
        .collect()
}

fn translate_secrets(project: &Project, stack: &str) -> Vec<Secret> {
    project
        .secrets
        .iter()
        .map(|(name, _)| Secret {
            id: secret_id(stack, name),
            name: name.clone(),
        })
        .collect()
}

fn translate_configs(project: &Project, stack: &str) -> Vec<StateConfig> {
    project
        .configs
        .iter()
        .map(|(name, _)| StateConfig {
            id: config_id(stack, name),
            name: name.clone(),
        })
        .collect()
}

// --- ID helpers ---------------------------------------------------------------

fn network_id(stack: &str, name: &str) -> String {
    format!("{stack}_{name}")
}

fn volume_id(stack: &str, name: &str) -> String {
    format!("{stack}_{name}")
}

fn secret_id(stack: &str, name: &str) -> String {
    format!("{stack}_{name}")
}

fn config_id(stack: &str, name: &str) -> String {
    format!("{stack}_{name}")
}

// --- memory parsing -----------------------------------------------------------

/// Parses a Compose memory string into bytes.
///
/// Accepts plain numbers (bytes) and suffixes `b`, `k`/`kb`/`kib`,
/// `m`/`mb`/`mib`, `g`/`gb`/`gib`, `t`/`tb`/`tib` (powers of 1024).
/// Decimals are allowed: `1.5G`.
pub fn parse_memory(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty memory value".to_string());
    }

    let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num_str, suffix) = s.split_at(split);

    let value: f64 = num_str
        .trim()
        .parse()
        .map_err(|_| format!("invalid number {num_str:?}"))?;
    if value < 0.0 {
        return Err(format!("negative memory value {s:?}"));
    }

    let multiplier: f64 = match suffix.trim().to_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => return Err(format!("unknown memory suffix {other:?}")),
    };

    Ok((value * multiplier) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_plain_bytes() {
        assert_eq!(parse_memory("512").unwrap(), 512);
        assert_eq!(parse_memory("0").unwrap(), 0);
    }

    #[test]
    fn memory_suffixes() {
        assert_eq!(parse_memory("1K").unwrap(), 1024);
        assert_eq!(parse_memory("512M").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory("2T").unwrap(), 2 * 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn memory_ib_suffixes() {
        assert_eq!(parse_memory("1KiB").unwrap(), 1024);
        assert_eq!(parse_memory("512MiB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory("1GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn memory_decimals() {
        assert_eq!(
            parse_memory("1.5G").unwrap(),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
        );
        assert_eq!(parse_memory("0.5K").unwrap(), 512);
    }

    #[test]
    fn memory_case_insensitive() {
        assert_eq!(parse_memory("512m").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory("1g").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn memory_errors() {
        assert!(parse_memory("").is_err());
        assert!(parse_memory("abc").is_err());
        assert!(parse_memory("1X").is_err());
        assert!(parse_memory("-1G").is_err());
    }

    // --- agent service translation tests ---------------------------------------

    fn make_compose_service(service_yaml: &str) -> minga_compose::Service {
        // Use the real loader to parse a single-service compose file.
        let yaml = format!("services:\n  test:\n{}", indent(service_yaml));
        let loaded = minga_compose::load(&yaml, &minga_compose::LoadOptions::default())
            .expect("compose should load");
        loaded.project.services.get("test").cloned().unwrap()
    }

    fn indent(s: &str) -> String {
        s.lines()
            .map(|line| format!("    {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn empty_project() -> Project {
        minga_compose::load("services: {}", &minga_compose::LoadOptions::default())
            .expect("empty compose should load")
            .project
    }

    #[test]
    fn translate_agent_service() {
        let svc = make_compose_service(
            r#"
            x-minga-agent:
              module: ./agent.wasm
              fuel_limit: 1000000000
              memory_limit: 134217728
              timeout: 60
              token_budget: 100000
              egress_url: https://api.openai.com
              prompt: "You are helpful"
              env:
                MODEL: gpt-4o
            deploy:
              replicas: 2
            "#,
        );
        let result = translate_service("my-agent", &svc, "test", &empty_project()).unwrap();
        assert_eq!(result.id, "test.my-agent");
        assert!(
            result.image.is_empty(),
            "agent service should have no image"
        );
        assert_eq!(result.replicas, 2);
        let agent = result.agent.expect("should have agent spec");
        assert_eq!(agent.module_ref, "./agent.wasm");
        assert_eq!(agent.fuel_limit, Some(1_000_000_000));
        assert_eq!(agent.memory_limit, Some(134_217_728));
        assert_eq!(agent.timeout_secs, Some(60));
        assert_eq!(agent.token_budget, Some(100_000));
        assert_eq!(agent.egress_url.as_deref(), Some("https://api.openai.com"));
        assert_eq!(agent.prompt.as_deref(), Some("You are helpful"));
        assert_eq!(
            agent.sandbox_env.get("MODEL").map(String::as_str),
            Some("gpt-4o")
        );
    }

    #[test]
    fn translate_agent_service_minimal() {
        let svc = make_compose_service(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            "#,
        );
        let result = translate_service("agent", &svc, "test", &empty_project()).unwrap();
        assert!(result.image.is_empty());
        let agent = result.agent.expect("should have agent spec");
        assert_eq!(agent.module_ref, "./agent.wasm");
        assert!(agent.fuel_limit.is_none());
        assert!(agent.token_budget.is_none());
    }

    #[test]
    fn translate_container_service_has_no_agent() {
        let svc = make_compose_service(
            r#"
            image: nginx:latest
            deploy:
              replicas: 1
            "#,
        );
        let result = translate_service("web", &svc, "test", &empty_project()).unwrap();
        assert_eq!(result.image, "nginx:latest");
        assert!(
            result.agent.is_none(),
            "container service should have no agent spec"
        );
    }

    #[test]
    fn translate_container_service_requires_image() {
        // Bypass the loader (which validates image at project level) to test
        // translate_service's own validation.
        let svc = minga_compose::Service {
            image: None,
            ..Default::default()
        };
        let result = translate_service("web", &svc, "test", &empty_project());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("image"), "error should mention image: {err}");
    }

    #[test]
    fn translate_agent_service_no_image_needed() {
        // Agent service without image should NOT fail
        let svc = make_compose_service(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            "#,
        );
        let result = translate_service("agent", &svc, "test", &empty_project());
        assert!(result.is_ok(), "agent service without image should succeed");
    }

    #[test]
    fn translate_mixed_stack() {
        // A project with both container and agent services
        let container_svc = make_compose_service("image: nginx:latest\n");
        let agent_svc = make_compose_service(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            "#,
        );
        let project = empty_project();

        let container = translate_service("web", &container_svc, "stack", &project).unwrap();
        let agent = translate_service("bot", &agent_svc, "stack", &project).unwrap();

        assert_eq!(container.image, "nginx:latest");
        assert!(container.agent.is_none());
        assert!(agent.image.is_empty());
        assert!(agent.agent.is_some());
    }

    #[test]
    fn translate_agent_service_with_resources() {
        let svc = make_compose_service(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            deploy:
              replicas: 1
              resources:
                limits:
                  cpus: "2.0"
                  memory: 512M
            "#,
        );
        let result = translate_service("agent", &svc, "test", &empty_project()).unwrap();
        assert_eq!(result.resources.cpus, 2.0);
        assert_eq!(result.resources.memory_bytes, 512 * 1024 * 1024);
        assert!(result.agent.is_some());
    }

    #[test]
    fn translate_agent_service_with_constraints() {
        let svc = make_compose_service(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            deploy:
              placement:
                constraints:
                  - node.labels.gpu == true
            "#,
        );
        let result = translate_service("agent", &svc, "test", &empty_project()).unwrap();
        assert_eq!(
            result.constraints,
            vec!["node.labels.gpu == true".to_string()]
        );
    }

    #[test]
    fn translate_invalid_agent_extension_fails() {
        let svc = make_compose_service(
            r#"
            x-minga-agent:
              fuel_limit: "not-a-number"
            "#,
        );
        let result = translate_service("agent", &svc, "test", &empty_project());
        assert!(result.is_err(), "invalid agent extension should fail");
    }
}
