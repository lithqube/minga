//! Shared NATS connection helper for the CLI.
//!
//! Every Minga process that talks to NATS goes through [`connect_bus`], so
//! there is exactly one place where TLS is decided. A caller cannot
//! accidentally use a different trust policy from its siblings.
//!
//! Rules enforced here:
//!
//! - TLS material is all-or-nothing. A half-configured setup is an error,
//!   never a silent downgrade to plaintext.
//! - In secure mode the URL must be `tls://` and must name the expected
//!   server certificate.
//! - The URL scheme and the TLS configuration must agree: a `tls://` URL
//!   without material is rejected rather than connected in the clear.

use std::sync::Arc;

use minga_bus::tls::NatsTlsConfig;
use minga_bus::EventBus;

/// Errors from building a bus connection.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("incomplete TLS configuration: {0}")]
    TlsConfig(String),

    #[error("cannot connect to NATS at {url}: {reason}")]
    Connect { url: String, reason: String },
}

/// Connect to NATS, using TLS when the environment provides material.
///
/// `url` is the server URL as given by the user. The environment variables
/// `MINGA_NATS_TLS_CA`, `MINGA_NATS_TLS_CERT`, `MINGA_NATS_TLS_KEY` and
/// `MINGA_NATS_TLS_SERVER_NAME` select secure mode.
///
/// In secure mode `url` may be a comma-separated seed list for a NATS
/// cluster. `replicas` raises the replicated streams and buckets (see
/// `NatsBus::ensure_replicas`); pass 1 to leave them as they are.
///
/// The returned bus is boxed as `Arc<dyn EventBus>` so all callers share one
/// type regardless of transport.
pub async fn connect_bus(
    url: &str,
    token: Option<&str>,
    replicas: usize,
) -> Result<Arc<dyn EventBus>, ConnectError> {
    let tls = NatsTlsConfig::from_env().map_err(|e| ConnectError::TlsConfig(e.to_string()))?;
    // Explicit flag wins; MINGA_NATS_TOKEN is the fallback.
    let token: Option<String> = token
        .map(String::from)
        .or_else(|| std::env::var("MINGA_NATS_TOKEN").ok());

    let connect_error = |e: minga_bus::BusError| ConnectError::Connect {
        url: url.to_string(),
        reason: e.to_string(),
    };
    let bus = match tls {
        Some(config) => minga_bus::NatsBus::connect_secure(url, &config)
            .await
            .map_err(connect_error)?,
        None => {
            // Plaintext is a loopback-only development mode. A remote
            // worker or manager must provision TLS material instead of
            // accidentally sending bus traffic across the network in clear.
            // A token adds credential-checking but does not make plaintext
            // routable.
            let authority = url.strip_prefix("nats://").and_then(|rest| {
                if rest.contains(['/', '?', '@', ',']) {
                    return None;
                }
                rest.rsplit_once(':')
            });
            let loopback = authority.is_some_and(|(host, port)| {
                matches!(host, "127.0.0.1" | "localhost" | "[::1]")
                    && port.parse::<u16>().is_ok_and(|port| port != 0)
            });
            if !loopback {
                return Err(ConnectError::TlsConfig(
                    "plaintext NATS is restricted to loopback; configure TLS for remote access"
                        .to_string(),
                ));
            }
            match &token {
                Some(token) => minga_bus::NatsBus::connect_with_token(url, token).await,
                None => minga_bus::NatsBus::connect(url).await,
            }
            .map_err(connect_error)?
        }
    };
    bus.ensure_replicas(replicas).await.map_err(connect_error)?;
    Ok(bus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_url_without_material_is_rejected() {
        // SAFETY: single-threaded test process; env is process-global.
        for var in [
            "MINGA_NATS_TLS_CA",
            "MINGA_NATS_TLS_CERT",
            "MINGA_NATS_TLS_KEY",
            "MINGA_NATS_TLS_SERVER_NAME",
        ] {
            std::env::remove_var(var);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        match rt.block_on(connect_bus("tls://localhost:4222", None, 1)) {
            Ok(_) => panic!("tls:// without material must be refused"),
            Err(ConnectError::TlsConfig(_)) => {}
            Err(other) => panic!("unexpected error variant: {other}"),
        }
    }
}
