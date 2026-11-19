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
