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
    #[error("NATS credentials file is invalid: {0}")]
    CredentialsFile(String),
}

/// Connect to NATS, using TLS when the environment provides material.
///
/// `url` is the server URL as given by the user. The environment variables
/// `MINGA_NATS_TLS_CA`, `MINGA_NATS_TLS_CERT`, `MINGA_NATS_TLS_KEY` and
/// `MINGA_NATS_TLS_SERVER_NAME` select secure mode.
/// `MINGA_NATS_CREDENTIALS_FILE` selects a NATS user credentials file. Its
/// contents are used only over TLS, and the file must be owner-only.
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
    system_credentials_file: Option<&std::path::Path>,
) -> Result<Arc<dyn EventBus>, ConnectError> {
    connect_nats_bus(url, token, replicas, system_credentials_file)
        .await
        .map(|bus| bus as Arc<dyn EventBus>)
}

/// Connects a manager sidecar now and keeps retrying the requested replica
/// count while its NATS route cluster forms.
pub async fn connect_bus_with_replica_retry(
    url: &str,
    token: Option<&str>,
    replicas: usize,
    system_credentials_file: Option<&std::path::Path>,
) -> Result<Arc<dyn EventBus>, ConnectError> {
    let bus = connect_nats_bus(url, token, 1, system_credentials_file).await?;
    let replica_bus = bus.clone();
    tokio::spawn(async move {
        loop {
            if let Err(error) = replica_bus.ensure_replicas(replicas).await {
                tracing::debug!(%error, replicas, "NATS cluster replicas not ready yet");
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
    Ok(bus)
}

async fn connect_nats_bus(
    url: &str,
    token: Option<&str>,
    replicas: usize,
    system_credentials_file: Option<&std::path::Path>,
) -> Result<Arc<minga_bus::NatsBus>, ConnectError> {
    let tls = NatsTlsConfig::from_env().map_err(|e| ConnectError::TlsConfig(e.to_string()))?;
    // Explicit flag wins; environment variables provide process-wide defaults.
    let token = token
        .map(str::to_owned)
        .or_else(|| std::env::var("MINGA_NATS_TOKEN").ok());
    let credentials = credentials_from_env()?;
    if token.is_some() && credentials.is_some() {
        return Err(ConnectError::TlsConfig(
            "NATS token and JWT credentials cannot be used together".to_string(),
        ));
    }

    let connect_error = |e: minga_bus::BusError| ConnectError::Connect {
        url: url.to_string(),
        reason: e.to_string(),
    };
    let system_tls = tls.clone();
    let bus = match tls {
        Some(config) => match credentials.as_deref() {
            Some(credentials) => {
                minga_bus::NatsBus::connect_secure_with_credentials(url, &config, credentials)
                    .await
                    .map_err(connect_error)?
            }
            None => minga_bus::NatsBus::connect_secure(url, &config)
                .await
                .map_err(connect_error)?,
        },
        None => {
            if credentials.is_some() {
                return Err(ConnectError::TlsConfig(
                    "NATS JWT credentials require TLS transport".to_string(),
                ));
            }
            // Plaintext is a loopback-only development mode. A token adds
            // credential-checking but does not make plaintext routable.
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
    let system_credentials_path = system_credentials_file
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("MINGA_NATS_SYS_CREDENTIALS_FILE").map(Into::into));
    let system_credentials = system_credentials_path
        .as_deref()
        .map(read_credentials_file)
        .transpose()?;
    if (std::env::var_os("MINGA_NATS_OPERATOR_JWT").is_some() || system_credentials_file.is_some())
        && system_credentials.is_none()
    {
        return Err(ConnectError::TlsConfig(
            "operator/JWT mode requires MINGA_NATS_SYS_CREDENTIALS_FILE for resolver updates"
                .to_string(),
        ));
    }
    if let Some(credentials) = system_credentials.as_deref() {
        let config = system_tls.as_ref().ok_or_else(|| {
            ConnectError::TlsConfig("SYS-account credentials require NATS TLS".to_string())
        })?;
        bus.enable_system_account(url, config, credentials)
            .await
            .map_err(connect_error)?;
    }
    bus.ensure_replicas(replicas).await.map_err(connect_error)?;
    Ok(bus)
}

fn credentials_from_env() -> Result<Option<String>, ConnectError> {
    let Some(path) = std::env::var_os("MINGA_NATS_CREDENTIALS_FILE") else {
        return Ok(None);
    };
    let path = std::path::PathBuf::from(path);
    read_credentials_file(&path).map(Some)
}

fn read_credentials_file(path: &std::path::Path) -> Result<String, ConnectError> {
    let bytes =
        crate::read_owner_only_file(path, 64 * 1024).map_err(ConnectError::CredentialsFile)?;
    String::from_utf8(bytes)
        .map_err(|_| ConnectError::CredentialsFile(format!("{} is not UTF-8", path.display())))
}

/// Connect a worker with read-only assignment access and owner-scoped writes.
///
/// Unlike [`connect_bus`], this never creates buckets or raises replicas.
pub async fn connect_worker_bus(
    url: &str,
    token: Option<&str>,
    node_id: u64,
    tenant_id: &str,
) -> Result<Arc<dyn EventBus>, ConnectError> {
    let tls =
        NatsTlsConfig::from_env().map_err(|error| ConnectError::TlsConfig(error.to_string()))?;
    let credentials = credentials_from_env()?;
    let token = token
        .map(String::from)
        .or_else(|| std::env::var("MINGA_NATS_TOKEN").ok());
    let connect_error = |error: minga_bus::BusError| ConnectError::Connect {
        url: url.to_string(),
        reason: error.to_string(),
    };
    if token.is_some() && credentials.is_some() {
        return Err(ConnectError::TlsConfig(
            "NATS token and JWT credentials cannot be used together".to_string(),
        ));
    }
    let bus = match tls {
        Some(config) => match credentials.as_deref() {
            Some(credentials) => minga_bus::NatsBus::connect_worker_secure_with_credentials(
                url,
                &config,
                credentials,
                node_id,
                tenant_id,
            )
            .await
            .map_err(connect_error)?,
            None => minga_bus::NatsBus::connect_worker_secure(url, &config, node_id, tenant_id)
                .await
                .map_err(connect_error)?,
        },
        None => {
            if credentials.is_some() {
                return Err(ConnectError::TlsConfig(
                    "NATS JWT credentials require TLS transport".to_string(),
                ));
            }
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
            match token {
                Some(token) => {
                    minga_bus::NatsBus::connect_worker_with_token(url, &token, node_id, tenant_id)
                        .await
                }
                None => minga_bus::NatsBus::connect_worker(url, node_id, tenant_id).await,
            }
            .map_err(connect_error)?
        }
    };
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
        match rt.block_on(connect_bus("tls://localhost:4222", None, 1, None)) {
            Ok(_) => panic!("tls:// without material must be refused"),
            Err(ConnectError::TlsConfig(_)) => {}
            Err(other) => panic!("unexpected error variant: {other}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn credentials_file_requires_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker.creds");
        std::fs::write(&path, "private credentials").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_credentials_file(&path).unwrap(), "private credentials");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = read_credentials_file(&path).unwrap_err();
        assert!(matches!(error, ConnectError::CredentialsFile(_)));
        assert!(!error.to_string().contains("private credentials"));
    }
}
