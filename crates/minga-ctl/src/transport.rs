//! How a command reaches the cluster (plan section 2.2).
//!
//! [`connect`] is the one place that turns a resolved context into an
//! [`ApiClient`] and a metrics URL. Today only the direct transport exists;
//! build-order step 3 adds the SSH tunnel here, keeping the tunnel's child
//! process alive inside [`Connection`] for as long as the command runs.

use crate::api::ApiClient;
use crate::config::{self, Resolved};
use crate::error::CtlError;

/// A live connection for one command.
pub struct Connection {
    /// Client for the Docker Engine API.
    pub api: ApiClient,
    /// Root URL that serves `/metrics` (the API root unless the context sets
    /// `metrics_endpoint`).
    pub metrics: String,
}

/// Connects to the resolved context's cluster.
pub async fn connect(resolved: &Resolved, debug: bool) -> Result<Connection, CtlError> {
    if let Some(ssh) = &resolved.context.ssh {
        return Err(CtlError::Usage(format!(
            "context `{}` reaches the API over ssh ({ssh}); the SSH tunnel is not built yet",
            resolved.name
        )));
    }

    // Direct transport: http:// to loopback, or https:// (plan item 4).
    config::check_endpoint_policy(None, &resolved.endpoint)?;
    let metrics = match &resolved.context.metrics_endpoint {
        Some(url) => {
            config::check_endpoint_policy(None, url)?;
            url.trim_end_matches('/').to_string()
        }
        None => resolved.endpoint.trim_end_matches('/').to_string(),
    };
    let api = ApiClient::new(
        &resolved.endpoint,
        resolved.token.clone(),
        resolved.context.tls_cert.as_deref(),
        &resolved.name,
        debug,
    )?;
    Ok(Connection { api, metrics })
}
