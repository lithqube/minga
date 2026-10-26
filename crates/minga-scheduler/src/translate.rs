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

use minga_compose::{DeployMode, DeviceCount, Project, DEFAULT_NETWORK};
use minga_state::types::{
    Config as StateConfig, Network, ResourceSpec, Secret, Service, ServiceMode, Volume,
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
    let image =
        svc.image.clone().filter(|i| !i.is_empty()).ok_or_else(|| {
            TranslateError::Service(name.to_string(), "`image` is required".into())
        })?;

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
    })
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

fn translate_networks(project: &Project, stack: &str) -> Vec<Network> {
    let mut networks: Vec<Network> = project
        .networks
        .iter()
        .map(|(name, net)| Network {
            id: network_id(stack, name),
            name: net.name.clone().unwrap_or_else(|| network_id(stack, name)),
            driver: net.driver.clone().unwrap_or_else(|| "overlay".to_string()),
            attachable: net.attachable.unwrap_or(false),
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
}
