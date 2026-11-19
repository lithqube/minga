//! `x-minga-agent` extension: declares a service as an AI-agent workload
//! running in a WASM sandbox instead of a container.
//!
//! # Example
//!
//! ```yaml
//! services:
//!   research-agent:
//!     x-minga-agent:
//!       module: ./agents/research.wasm
//!       fuel_limit: 1000000000
//!       memory_limit: 134217728
//!       timeout: 60
//!       token_budget: 100000
//!       egress_url: https://api.openai.com
//!       capabilities:
//!         grants: [env, http]
//!       prompt: "You are a research assistant"
//!       env:
//!         MODEL: gpt-4o
//!     deploy:
//!       replicas: 2
//! ```

use std::collections::{BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::model::Service;

/// Host capabilities an agent may request in its Compose extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityGrant {
    /// Read the explicitly declared `env` map.
    Env,
    /// Read wall and monotonic clocks.
    Clock,
    /// Use the host random source.
    Random,
    /// Use standard input, output, and error streams.
    Stdio,
    /// Send HTTP through the node's configured egress proxy.
    Http,
}

/// Capability grants attached to an agent workload.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCapabilities {
    /// Capabilities granted to the workload.
    #[serde(default)]
    pub grants: BTreeSet<CapabilityGrant>,
    /// Read-only host directories mounted at guest paths.
    #[serde(default)]
    pub dirs: Vec<AgentDirGrant>,
}

/// A read-only directory grant.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDirGrant {
    /// Host path, restricted by node operator configuration.
    pub host: String,
    /// Absolute guest mount path.
    pub guest: String,
}

/// The `x-minga-agent` extension block on a Compose service.
///
/// When present, the service is scheduled as an agent (WASM sandbox)
/// workload instead of a container. The `image` key is not required.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExtension {
    /// Path or reference to the WASM module on the node's filesystem.
    pub module: String,
    /// Maximum fuel (instruction budget). `None` = unlimited.
    #[serde(default, deserialize_with = "opt_u64")]
    pub fuel_limit: Option<u64>,
    /// Maximum memory in bytes. `None` = runtime default.
    #[serde(default, deserialize_with = "opt_u64")]
    pub memory_limit: Option<u64>,
    /// Maximum execution time in seconds. `None` = runtime default (30s).
    #[serde(default, deserialize_with = "opt_u64")]
    pub timeout: Option<u64>,
    /// Token budget for LLM egress. `None` = no budget enforcement.
    #[serde(default, deserialize_with = "opt_u64")]
    pub token_budget: Option<u64>,
    /// LLM API upstream URL (goes through the egress proxy).
    #[serde(default)]
    pub egress_url: Option<String>,
    /// Initial prompt / input for the agent.
    #[serde(default)]
    pub prompt: Option<String>,
    /// Additional environment variables for the sandbox.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Explicit host capabilities. Omitted grants deny every host feature.
    #[serde(default)]
    pub capabilities: AgentCapabilities,
}

/// The extension key used in Compose files.
pub const AGENT_EXTENSION_KEY: &str = "x-minga-agent";

impl Service {
    /// Returns the parsed `x-minga-agent` extension, if present.
    ///
    /// Returns `None` if the extension is absent. Returns `Err` if the
    /// extension is present but malformed.
    pub fn agent_extension(&self) -> Result<Option<AgentExtension>, String> {
        let Some(value) = self.extensions.get(AGENT_EXTENSION_KEY) else {
            return Ok(None);
        };
        let ext: AgentExtension = serde_yaml::from_value(value.clone())
            .map_err(|e| format!("invalid {AGENT_EXTENSION_KEY}: {e}"))?;
        ext.validate()
            .map_err(|e| format!("invalid {AGENT_EXTENSION_KEY}: {e}"))?;
        Ok(Some(ext))
    }

    /// Returns `true` if this service declares an `x-minga-agent` extension.
    pub fn is_agent(&self) -> bool {
        self.extensions.contains_key(AGENT_EXTENSION_KEY)
    }
}
/// Authentication for one named egress route. Values are secret IDs only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

/// One tenant-owned upstream route.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

/// The `x-minga-egress` policy for a service.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct EgressExtension {
    pub required: bool,
    pub routes: Vec<EgressRoute>,
    pub models: Vec<String>,
}

pub const EGRESS_EXTENSION_KEY: &str = "x-minga-egress";

impl Service {
    pub fn egress_extension(&self) -> Result<Option<EgressExtension>, String> {
        let Some(value) = self.extensions.get(EGRESS_EXTENSION_KEY) else {
            return Ok(None);
        };
        let extension: EgressExtension = serde_yaml::from_value(value.clone())
            .map_err(|error| format!("invalid {EGRESS_EXTENSION_KEY}: {error}"))?;
        extension
            .validate()
            .map_err(|error| format!("invalid {EGRESS_EXTENSION_KEY}: {error}"))?;
        Ok(Some(extension))
    }
}

impl EgressExtension {
    fn validate(&self) -> Result<(), String> {
        if self.routes.len() > 64 {
            return Err("at most 64 egress routes are allowed per service".into());
        }
        if self.required && self.routes.is_empty() {
            return Err("required egress needs at least one named route".into());
        }
        let mut by_name = HashMap::with_capacity(self.routes.len());
        if self.routes.is_empty() && !self.models.is_empty() {
            return Err("model allow-lists require at least one egress route".into());
        }
        for model in &self.models {
            let stars = model.bytes().filter(|byte| *byte == b'*').count();
            if model.is_empty() || stars > 1 || (stars == 1 && !model.ends_with('*')) {
                return Err(format!("invalid model allow-list pattern `{model}`"));
            }
        }
        for route in &self.routes {
            if route.name.is_empty()
                || !route
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                return Err(format!("invalid egress route name `{}`", route.name));
            }
            if by_name.insert(route.name.as_str(), route).is_some() {
                return Err(format!("duplicate egress route `{}`", route.name));
            }
            let url = url::Url::parse(&route.upstream_url)
                .map_err(|error| format!("invalid egress URL for `{}`: {error}", route.name))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(format!(
                    "egress route `{}` must use an HTTP(S) URL without userinfo, query, or fragment",
                    route.name
                ));
            }
            if let Some(auth) = &route.auth {
                match auth {
                    EgressAuth::Bearer { secret } | EgressAuth::ApiKey { secret }
                        if secret.trim().is_empty() =>
                    {
                        return Err(format!(
                            "egress route `{}` has an empty secret ID",
                            route.name
                        ));
                    }
                    EgressAuth::AwsSigv4 {
                        access_key_secret,
                        secret_key_secret,
                        region,
                        service,
                        ..
                    } if access_key_secret.trim().is_empty()
                        || secret_key_secret.trim().is_empty()
                        || region.trim().is_empty()
                        || service.trim().is_empty() =>
                    {
                        return Err(format!(
                            "egress route `{}` has incomplete AWS SigV4 settings",
                            route.name
                        ));
                    }
                    _ => {}
                }
            }
            if route.input_price_micros_per_million.is_some()
                != route.output_price_micros_per_million.is_some()
            {
                return Err(format!(
                    "egress route `{}` must specify both token prices or neither",
                    route.name
                ));
            }
        }
        for route in &self.routes {
            let mut seen = BTreeSet::new();
            for fallback in &route.fallback {
                if fallback == &route.name {
                    return Err(format!(
                        "egress route `{}` cannot fall back to itself",
                        route.name
                    ));
                }
                if !by_name.contains_key(fallback.as_str()) {
                    return Err(format!(
                        "egress route `{}` falls back to unknown route `{fallback}`",
                        route.name
                    ));
                }
                if !seen.insert(fallback) {
                    return Err(format!(
                        "egress route `{}` repeats fallback `{fallback}`",
                        route.name
                    ));
                }
            }
        }
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for route in &self.routes {
            if has_fallback_cycle(route.name.as_str(), &by_name, &mut visiting, &mut visited) {
                return Err("egress fallback routes must be acyclic".into());
            }
        }
        Ok(())
    }
}

fn has_fallback_cycle(
    name: &str,
    routes: &HashMap<&str, &EgressRoute>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> bool {
    if visited.contains(name) {
        return false;
    }
    if !visiting.insert(name.to_owned()) {
        return true;
    }
    if let Some(route) = routes.get(name) {
        for fallback in &route.fallback {
            if has_fallback_cycle(fallback, routes, visiting, visited) {
                return true;
            }
        }
    }
    visiting.remove(name);
    visited.insert(name.to_owned());
    false
}

impl AgentExtension {
    fn validate(&self) -> Result<(), String> {
        if !self.env.is_empty() && !self.capabilities.grants.contains(&CapabilityGrant::Env) {
            return Err("env requires the `env` capability grant".into());
        }

        let mut guest_paths = BTreeSet::new();
        for dir in &self.capabilities.dirs {
            let host = Path::new(&dir.host);
            if !host.is_absolute() {
                return Err(format!(
                    "directory host path `{}` must be absolute",
                    dir.host
                ));
            }
            if host
                .components()
                .any(|component| component == Component::ParentDir)
            {
                return Err(format!(
                    "directory host path `{}` must not contain `..`",
                    dir.host
                ));
            }

            let guest = Path::new(&dir.guest);
            if !guest.is_absolute() {
                return Err(format!(
                    "directory guest path `{}` must be absolute",
                    dir.guest
                ));
            }
            if guest
                .components()
                .any(|component| component == Component::ParentDir)
            {
                return Err(format!(
                    "directory guest path `{}` must not contain `..`",
                    dir.guest
                ));
            }
            let normalized = guest
                .components()
                .fold(PathBuf::new(), |mut path, component| {
                    path.push(component.as_os_str());
                    path
                });
            if !guest_paths.insert(normalized) {
                return Err(format!(
                    "directory guest path `{}` is declared more than once",
                    dir.guest
                ));
            }
        }
        Ok(())
    }
}

/// Deserializes an optional u64 from a YAML value that may be a string or number.
fn opt_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<Value>::deserialize(deserializer)?;
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("expected a positive integer")),
        Some(Value::String(s)) => s.parse::<u64>().map(Some).map_err(serde::de::Error::custom),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected a number, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use serde_yaml::Mapping;

    fn make_service_with_ext(yaml: &str) -> Service {
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        let mut svc = Service::default();
        if let Value::Mapping(m) = value {
            for (k, v) in m {
                if let Some(key) = k.as_str() {
                    svc.extensions.insert(key.to_string(), v);
                }
            }
        }
        svc
    }

    #[test]
    fn parse_full_agent_extension() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              module: ./agent.wasm
              fuel_limit: 1000000000
              memory_limit: 134217728
              timeout: 60
              token_budget: 100000
              egress_url: https://api.openai.com
              prompt: "You are helpful"
              capabilities:
                grants: [env]
              env:
                MODEL: gpt-4o
                TEMP: "0.7"
            "#,
        );
        let ext = svc.agent_extension().unwrap().unwrap();
        assert_eq!(ext.module, "./agent.wasm");
        assert_eq!(ext.fuel_limit, Some(1_000_000_000));
        assert_eq!(ext.memory_limit, Some(134_217_728));
        assert_eq!(ext.timeout, Some(60));
        assert_eq!(ext.token_budget, Some(100_000));
        assert_eq!(ext.egress_url.as_deref(), Some("https://api.openai.com"));
        assert_eq!(ext.prompt.as_deref(), Some("You are helpful"));
        assert_eq!(ext.env.get("MODEL").map(String::as_str), Some("gpt-4o"));
        assert_eq!(ext.env.get("TEMP").map(String::as_str), Some("0.7"));
        assert!(ext.capabilities.grants.contains(&CapabilityGrant::Env));
    }

    #[test]
    fn parse_minimal_agent_extension() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            "#,
        );
        let ext = svc.agent_extension().unwrap().unwrap();
        assert_eq!(ext.module, "./agent.wasm");
        assert!(ext.fuel_limit.is_none());
        assert!(ext.memory_limit.is_none());
        assert!(ext.timeout.is_none());
        assert!(ext.token_budget.is_none());
        assert!(ext.egress_url.is_none());
        assert!(ext.prompt.is_none());
        assert!(ext.env.is_empty());
        assert!(ext.capabilities.grants.is_empty());
        assert!(ext.capabilities.dirs.is_empty());
    }
    #[test]
    fn invalid_capability_grants_and_directory_paths_are_rejected() {
        for yaml in [
            r#"
            x-minga-agent:
              module: ./agent.wasm
              capabilities:
                grants: [sockets]
            "#,
            r#"
            x-minga-agent:
              module: ./agent.wasm
              env:
                MODEL: local
            "#,
            r#"
            x-minga-agent:
              module: ./agent.wasm
              capabilities:
                dirs:
                  - host: relative
                    guest: /data
            "#,
            r#"
            x-minga-agent:
              module: ./agent.wasm
              capabilities:
                dirs:
                  - host: /srv/data/../secrets
                    guest: /data
            "#,
            r#"
            x-minga-agent:
              module: ./agent.wasm
              capabilities:
                dirs:
                  - host: /srv/data
                    guest: relative
            "#,
            r#"
            x-minga-agent:
              module: ./agent.wasm
              capabilities:
                dirs:
                  - host: /srv/data
                    guest: /data
                  - host: /srv/other
                    guest: /data/
            "#,
        ] {
            let svc = make_service_with_ext(yaml);
            assert!(
                svc.agent_extension().is_err(),
                "unexpectedly accepted:\n{yaml}"
            );
        }
    }

    #[test]
    fn no_extension_returns_none() {
        let svc = Service::default();
        assert!(svc.agent_extension().unwrap().is_none());
        assert!(!svc.is_agent());
    }

    #[test]
    fn is_agent_detects_extension() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            "#,
        );
        assert!(svc.is_agent());
    }

    #[test]
    fn invalid_extension_returns_error() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              fuel_limit: "not-a-number"
            "#,
        );
        assert!(svc.agent_extension().is_err());
    }

    #[test]
    fn missing_module_returns_error() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              fuel_limit: 1000
            "#,
        );
        // module is required (no default), so this should fail
        assert!(svc.agent_extension().is_err());
    }

    #[test]
    fn string_numbers_are_parsed() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              module: ./agent.wasm
              fuel_limit: "5000000"
              timeout: "120"
            "#,
        );
        let ext = svc.agent_extension().unwrap().unwrap();
        assert_eq!(ext.fuel_limit, Some(5_000_000));
        assert_eq!(ext.timeout, Some(120));
    }

    #[test]
    fn unknown_keys_rejected() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              module: ./agent.wasm
              bogus_key: true
            "#,
        );
        assert!(svc.agent_extension().is_err());
    }

    #[test]
    fn extensions_map_preserved() {
        let svc = make_service_with_ext(
            r#"
            x-minga-agent:
              module: ./agent.wasm
            x-custom-other:
              foo: bar
            "#,
        );
        assert!(svc.extensions.contains_key("x-minga-agent"));
        assert!(svc.extensions.contains_key("x-custom-other"));
        assert!(svc.is_agent());
    }

    #[test]
    fn agent_extension_serde_default() {
        // Ensure AgentExtension can be constructed with Default
        let ext = AgentExtension::default();
        assert!(ext.module.is_empty());
        assert!(ext.fuel_limit.is_none());
        assert!(ext.capabilities.grants.is_empty());
        assert!(ext.capabilities.dirs.is_empty());
    }
    #[test]
    fn parses_required_egress_routes_and_models() {
        let required = make_service_with_ext(
            r#"
            x-minga-egress:
              required: true
              routes:
                - name: primary
                  upstream_url: https://api.openai.com
                  auth:
                    scheme: bearer
                    secret: openai_key
                  fallback: [backup]
                - name: backup
                  upstream_url: https://backup.example/v1
              models: [gpt-4o-*]
            "#,
        );
        let extension = required.egress_extension().unwrap().unwrap();
        assert!(extension.required);
        assert_eq!(extension.routes.len(), 2);
        assert_eq!(extension.routes[0].fallback, vec!["backup".to_string()]);
        assert_eq!(extension.models, vec!["gpt-4o-*".to_string()]);
        assert_eq!(Service::default().egress_extension().unwrap(), None);
        assert_eq!(EgressExtension::default().routes, []);
    }

    #[test]
    fn rejects_invalid_egress_route_graph_and_unknown_keys() {
        let cycle = make_service_with_ext(
            r#"
            x-minga-egress:
              required: true
              routes:
                - name: primary
                  upstream_url: https://api.openai.com
                  fallback: [backup]
                - name: backup
                  upstream_url: https://backup.example
                  fallback: [primary]
            "#,
        );
        assert!(cycle.egress_extension().is_err());

        let unknown = make_service_with_ext(
            r#"
            x-minga-egress:
              required: true
              routes:
                - name: primary
                  upstream_url: https://api.openai.com
              ignored: true
            "#,
        );
        assert!(unknown.egress_extension().is_err());
    }

    // Suppress unused import warning when IndexMap/Mapping aren't used in all configs
    #[allow(dead_code)]
    fn _unused(_: IndexMap<String, Value>, _: Mapping) {}
}
