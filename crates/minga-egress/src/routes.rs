//! Resolved per-workload egress route configuration.

use std::collections::HashMap;

/// Credentials resolved from the node-local tenant secret store.
#[derive(Clone)]
pub enum ResolvedAuth {
    Bearer(String),
    ApiKey(String),
    AwsSigv4 {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
        region: String,
        service: String,
    },
}

impl std::fmt::Debug for ResolvedAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer(_) => f.write_str("Bearer(***)"),
            Self::ApiKey(_) => f.write_str("ApiKey(***)"),
            Self::AwsSigv4 {
                region, service, ..
            } => f
                .debug_struct("AwsSigv4")
                .field("access_key", &"***")
                .field("secret_key", &"***")
                .field("session_token", &"***")
                .field("region", region)
                .field("service", service)
                .finish(),
        }
    }
}

/// One resolved named route. Secret values exist only in worker memory.
#[derive(Debug, Clone)]
pub struct ResolvedRoute {
    pub name: String,
    pub upstream_url: String,
    pub auth: Option<ResolvedAuth>,
    pub fallback: Vec<String>,
    pub input_price_micros_per_million: Option<u64>,
    pub output_price_micros_per_million: Option<u64>,
}

/// Per-workload route and model allow-list.
#[derive(Debug, Clone, Default)]
pub struct ResolvedEgressPolicy {
    pub routes: HashMap<String, ResolvedRoute>,
    pub models: Vec<String>,
}

impl ResolvedEgressPolicy {
    /// Whether `model` matches the exact or trailing-`*` allow-list entries.
    pub fn allows_model(&self, model: &str) -> bool {
        self.models.is_empty()
            || self.models.iter().any(|pattern| {
                pattern
                    .strip_suffix('*')
                    .map_or(pattern == model, |prefix| model.starts_with(prefix))
            })
    }

    /// An opaque revision of the policy's non-secret settings. Cache keys
    /// carry it, so a task restarted with a changed route never sees the
    /// old provider's entries. Credential values are not part of it.
    pub fn revision(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut names: Vec<&String> = self.routes.keys().collect();
        names.sort();
        for name in names {
            let route = &self.routes[name];
            route.name.hash(&mut hasher);
            route.upstream_url.hash(&mut hasher);
            route.fallback.hash(&mut hasher);
            route.input_price_micros_per_million.hash(&mut hasher);
            route.output_price_micros_per_million.hash(&mut hasher);
            match &route.auth {
                None => 0u8.hash(&mut hasher),
                Some(ResolvedAuth::Bearer(_)) => 1u8.hash(&mut hasher),
                Some(ResolvedAuth::ApiKey(_)) => 2u8.hash(&mut hasher),
                Some(ResolvedAuth::AwsSigv4 {
                    region, service, ..
                }) => {
                    3u8.hash(&mut hasher);
                    region.hash(&mut hasher);
                    service.hash(&mut hasher);
                }
            }
        }
        self.models.hash(&mut hasher);
        hasher.finish()
    }

    /// Resolve a route name and all fallback names without cycles.
    pub fn route_chain(&self, first: &str) -> Result<Vec<&ResolvedRoute>, String> {
        let mut names = vec![first.to_string()];
        let mut visited = std::collections::HashSet::new();
        let mut routes = Vec::new();
        while let Some(name) = names.pop() {
            if !visited.insert(name.clone()) {
                continue;
            }
            let route = self
                .routes
                .get(&name)
                .ok_or_else(|| format!("unknown egress route `{name}`"))?;
            for fallback in route.fallback.iter().rev() {
                names.push(fallback.clone());
            }
            routes.push(route);
        }
        Ok(routes)
    }
}

/// Trusted identity attached to a live sandbox token.
#[derive(Debug, Clone)]
pub struct WorkloadIdentity {
    pub workload_id: String,
    pub tenant_id: String,
    pub service_id: String,
    pub policy: Option<ResolvedEgressPolicy>,
}

impl WorkloadIdentity {
    pub fn legacy(workload_id: &str) -> Self {
        Self {
            workload_id: workload_id.to_owned(),
            tenant_id: minga_state::types::DEFAULT_TENANT_ID.to_owned(),
            service_id: workload_id.to_owned(),
            policy: None,
        }
    }
}
