//! A loaded, validated project: profiles applied, references checked and a
//! startup order computed from `depends_on`.

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use serde_yaml::Value;

use crate::error::ComposeError;
use crate::model::{ComposeFile, DeployMode, FileObject, Network, Service, Volume, VolumeKind};

/// The network a service joins when it lists none.
pub const DEFAULT_NETWORK: &str = "default";

#[derive(Debug, Clone, PartialEq)]
pub struct Project {
    pub name: Option<String>,
    /// Services enabled by the active profiles, in file order.
    pub services: IndexMap<String, Service>,
    /// Services left out because none of their profiles is active.
    pub disabled_services: Vec<String>,
    pub networks: IndexMap<String, Network>,
    pub volumes: IndexMap<String, Volume>,
    pub secrets: IndexMap<String, FileObject>,
    pub configs: IndexMap<String, FileObject>,
    pub extensions: IndexMap<String, Value>,
    order: Vec<String>,
}

impl Project {
    /// Service names in an order where every service comes after the
    /// services it depends on. Ties keep file order.
    pub fn startup_order(&self) -> &[String] {
        &self.order
    }

    pub(crate) fn build(
        file: ComposeFile,
        profiles: &[String],
        errors: &mut Vec<ComposeError>,
        warnings: &mut Vec<String>,
    ) -> Self {
        if file.version.is_some() {
            warnings.push("`version` is obsolete and ignored".to_string());
        }

        let all_profiles = profiles.iter().any(|p| p == "*");
        let (services, disabled): (IndexMap<_, _>, IndexMap<_, _>) =
            file.services.into_iter().partition(|(_, svc)| {
                svc.profiles.is_empty()
                    || all_profiles
                    || svc.profiles.iter().any(|p| profiles.contains(p))
            });

        let mut project = Self {
            name: file.name,
            services,
            disabled_services: disabled.keys().cloned().collect(),
            networks: file.networks,
            volumes: file.volumes,
            secrets: file.secrets,
            configs: file.configs,
            extensions: file.extensions,
            order: Vec::new(),
        };
        project.check_dependencies(errors, warnings);
        project.check_services(errors);
        project.check_file_objects(errors);
        project.check_port_conflicts(errors);
        project.order = project.compute_order(errors);
        project
    }

    /// Reports bad `depends_on` entries. Optional ones (`required: false`)
    /// that cannot be met are dropped with a warning instead.
    fn check_dependencies(&mut self, errors: &mut Vec<ComposeError>, warnings: &mut Vec<String>) {
        let active: HashSet<String> = self.services.keys().cloned().collect();
        let disabled: HashSet<String> = self.disabled_services.iter().cloned().collect();
        for (name, svc) in &mut self.services {
            let at = format!("services.{name}.depends_on");
            let mut skipped = Vec::new();
            for (dep, d) in &svc.depends_on {
                let problem = if dep == name {
                    "a service cannot depend on itself".to_string()
                } else if disabled.contains(dep) {
                    format!("depends on `{dep}`, which no active profile enables")
                } else if !active.contains(dep) {
                    format!("depends on undefined service `{dep}`")
                } else {
                    continue;
                };
                if d.required {
                    errors.push(ComposeError::invalid(&at, problem));
                } else {
                    warnings.push(format!(
                        "{at}: {problem}; skipped because `required` is false"
                    ));
                    skipped.push(dep.clone());
                }
            }
            for dep in skipped {
                svc.depends_on.shift_remove(&dep);
            }
        }
    }

    fn check_services(&self, errors: &mut Vec<ComposeError>) {
        for (name, svc) in &self.services {
            let at = format!("services.{name}");
            if svc.image.as_deref().map_or(true, str::is_empty) {
                errors.push(ComposeError::invalid(&at, "`image` is required"));
            }
            if let Some(deploy) = &svc.deploy {
                let global = matches!(deploy.mode, DeployMode::Global | DeployMode::GlobalJob);
                if global && deploy.replicas.is_some() {
                    errors.push(ComposeError::invalid(
                        format!("{at}.deploy.replicas"),
                        "cannot be set when mode is global",
                    ));
                }
            }
            for net in svc.networks.keys() {
                if net != DEFAULT_NETWORK && !self.networks.contains_key(net) {
                    errors.push(ComposeError::invalid(
                        format!("{at}.networks"),
                        format!("network `{net}` is not declared under top-level `networks`"),
                    ));
                }
            }
            for (i, vol) in svc.volumes.iter().enumerate() {
                if vol.kind != VolumeKind::Volume {
                    continue;
                }
                if let Some(src) = &vol.source {
                    if !self.volumes.contains_key(src) {
                        errors.push(ComposeError::invalid(
                            format!("{at}.volumes[{i}]"),
                            format!("volume `{src}` is not declared under top-level `volumes`"),
                        ));
                    }
                }
            }
            for (kind, refs, declared) in [
                ("secret", &svc.secrets, &self.secrets),
                ("config", &svc.configs, &self.configs),
            ] {
                for r in refs {
                    if !declared.contains_key(&r.source) {
                        errors.push(ComposeError::invalid(
                            format!("{at}.{kind}s"),
                            format!(
                                "{kind} `{}` is not declared under top-level `{kind}s`",
                                r.source
                            ),
                        ));
                    }
                }
            }
        }
    }

    fn check_file_objects(&self, errors: &mut Vec<ComposeError>) {
        for (kind, items) in [("secrets", &self.secrets), ("configs", &self.configs)] {
            for (name, obj) in items {
                let sources = [
                    obj.file.is_some(),
                    obj.environment.is_some(),
                    obj.content.is_some(),
                    obj.external == Some(true),
                ]
                .into_iter()
                .filter(|s| *s)
                .count();
                if sources != 1 {
                    errors.push(ComposeError::invalid(
                        format!("{kind}.{name}"),
                        "set exactly one of `file`, `environment`, `content` or `external: true`",
                    ));
                }
                if kind == "secrets" && obj.content.is_some() {
                    errors.push(ComposeError::invalid(
                        format!("{kind}.{name}"),
                        "`content` is only allowed for configs",
                    ));
                }
            }
        }
    }

    /// Two services publishing the same port and protocol on the same host IP
    /// would fight over it on every node of the routing mesh.
    fn check_port_conflicts(&self, errors: &mut Vec<ComposeError>) {
        let mut seen: HashMap<(u16, String, String), &str> = HashMap::new();
        for (name, svc) in &self.services {
            for port in &svc.ports {
                let Some(published) = port.published else {
                    continue;
                };
                let ip = port.host_ip.clone().unwrap_or_default();
                for p in published.start..=published.end {
                    let key = (p, port.protocol.clone(), ip.clone());
                    match seen.get(&key) {
                        Some(other) if *other != name.as_str() => {
                            errors.push(ComposeError::invalid(
                                format!("services.{name}.ports"),
                                format!(
                                    "port {p}/{} is already published by `{other}`",
                                    port.protocol
                                ),
                            ))
                        }
                        _ => {
                            seen.insert(key, name);
                        }
                    }
                }
            }
        }
    }

    /// Kahn's algorithm over `depends_on`, keeping file order among ready services.
    fn compute_order(&self, errors: &mut Vec<ComposeError>) -> Vec<String> {
        let mut pending: Vec<&str> = self.services.keys().map(String::as_str).collect();
        let mut done: HashSet<&str> = HashSet::new();
        let mut order = Vec::with_capacity(pending.len());
        while !pending.is_empty() {
            let ready = pending.iter().position(|name| {
                self.services[*name]
                    .depends_on
                    .keys()
                    // Self-dependencies are already reported by check_dependencies.
                    .all(|d| {
                        d.as_str() == *name
                            || done.contains(d.as_str())
                            || !self.services.contains_key(d)
                    })
            });
            let Some(i) = ready else {
                errors.push(ComposeError::invalid(
                    "services",
                    format!("dependency cycle between: {}", pending.join(", ")),
                ));
                break;
            };
            let name = pending.remove(i);
            done.insert(name);
            order.push(name.to_string());
        }
        order
    }
}
