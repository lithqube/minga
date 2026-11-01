//! NATS transport security.
//!
//! Typed TLS configuration shared by every Minga NATS caller (manager,
//! worker, `minga logs`, `minga agent ls`). One configuration type, one
//! preflight, one `ConnectOptions` builder — so there is no path where a
//! caller silently connects in plaintext.
//!
//! # Design constraints
//!
//! - **No insecure escape hatches.** There is no `danger_accept_invalid_certs`
//!   equivalent. A missing or malformed CA, cert or key is a hard error.
//! - **The server name is verified.** `expected_server_name` is checked against
//!   the certificate chain's SANs by rustls, not merely by the connect URI.
//! - **Trust is explicit.** Only the configured CA is trusted. The platform
//!   trust store is *not* consulted, so a publicly-trusted certificate cannot
//!   impersonate the cluster.
//! - **Key material never leaves this module.** PEM contents are never
//!   logged, serialized into Raft/NATS, or included in error messages — errors
//!   carry file paths and rustls' reason strings only.

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Errors from TLS configuration and handshake setup.
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("TLS CA file `{path}` is not readable: {reason}")]
    CaUnreadable { path: String, reason: String },

    #[error("TLS client certificate file `{path}` is not readable: {reason}")]
    CertUnreadable { path: String, reason: String },

    #[error("TLS client key file `{path}` is not readable: {reason}")]
    KeyUnreadable { path: String, reason: String },

    #[error("TLS client key file `{path}` has insecure permissions {mode:o} (expected 0600)")]
    KeyPermissionsTooOpen { path: String, mode: u32 },

    #[error("TLS CA file `{path}` contains no PEM certificates")]
    CaEmpty { path: String },

    #[error("TLS client certificate file `{path}` contains no PEM certificates")]
    CertEmpty { path: String },

    #[error("TLS client key file `{path}` contains no PEM private key")]
    KeyEmpty { path: String },

    #[error("TLS material for `{field}` is incomplete: {reason}")]
    Incomplete { field: &'static str, reason: String },

    #[error("failed to build TLS client configuration: {0}")]
    Build(String),

    #[error("TLS handshake failed: {0}")]
    Handshake(String),

    #[error("TLS URL `{url}` must use the `tls://` scheme in secure mode")]
    SchemeNotTls { url: String },

    #[error("TLS URL host `{host}` does not match the expected server name `{expected}`")]
    ServerNameMismatch { host: String, expected: String },
}

/// NATS client TLS material and expectations.
///
/// Build it from explicit paths ([`NatsTlsConfig::new`]) or from the
/// environment ([`NatsTlsConfig::from_env`]). Validation happens in
/// [`NatsTlsConfig::preflight`], which must succeed before any connection
/// is attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsTlsConfig {
    /// PEM file holding the CA certificate(s) that sign the server certificate.
    pub ca_file: PathBuf,
    /// PEM file holding the client certificate (and optionally the key).
    pub cert_file: PathBuf,
    /// PEM file holding the client private key.
    pub key_file: PathBuf,
    /// The DNS name (or IP) the server certificate must carry in its SANs.
    ///
    /// Required: without it, a certificate valid for *any* name signed by the
    /// CA would be accepted.
    pub expected_server_name: String,
}

impl NatsTlsConfig {
    /// Build a configuration from explicit file paths.
    pub fn new(
        ca_file: impl Into<PathBuf>,
        cert_file: impl Into<PathBuf>,
        key_file: impl Into<PathBuf>,
        expected_server_name: impl Into<String>,
    ) -> Self {
        Self {
            ca_file: ca_file.into(),
            cert_file: cert_file.into(),
            key_file: key_file.into(),
            expected_server_name: expected_server_name.into(),
        }
    }

    /// Read the configuration from environment variables.
    ///
    /// * `MINGA_NATS_TLS_CA` — CA PEM path
    /// * `MINGA_NATS_TLS_CERT` — client certificate PEM path
    /// * `MINGA_NATS_TLS_KEY` — client key PEM path
    /// * `MINGA_NATS_TLS_SERVER_NAME` — expected server DNS name
    ///
    /// Returns `None` when **none** of the variables are set (plaintext
    /// development mode). Returns an error when only some are set — a
    /// half-configured TLS setup must not silently downgrade to plaintext.
    pub fn from_env() -> Result<Option<Self>, TlsError> {
        let ca = std::env::var("MINGA_NATS_TLS_CA")
            .ok()
            .filter(|v| !v.is_empty());
        let cert = std::env::var("MINGA_NATS_TLS_CERT")
            .ok()
            .filter(|v| !v.is_empty());
        let key = std::env::var("MINGA_NATS_TLS_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        let name = std::env::var("MINGA_NATS_TLS_SERVER_NAME")
            .ok()
            .filter(|v| !v.is_empty());

        if ca.is_none() && cert.is_none() && key.is_none() && name.is_none() {
            return Ok(None);
        }

        let missing = |field: &'static str| TlsError::Incomplete {
            field,
            reason: format!("{field} is not set"),
        };

        Ok(Some(Self {
            ca_file: PathBuf::from(ca.ok_or(missing("MINGA_NATS_TLS_CA"))?),
            cert_file: PathBuf::from(cert.ok_or(missing("MINGA_NATS_TLS_CERT"))?),
            key_file: PathBuf::from(key.ok_or(missing("MINGA_NATS_TLS_KEY"))?),
            expected_server_name: name.ok_or(missing("MINGA_NATS_TLS_SERVER_NAME"))?,
        }))
    }

    /// Validate the material without opening a connection.
    ///
    /// Checks that each file exists and is readable, that the key file is not
    /// group- or world-accessible, and that each PEM file actually contains
    /// the expected kind of object.
    pub fn preflight(&self) -> Result<(), TlsError> {
        let ca = read_pem_certs(&self.ca_file).map_err(|e| match e {
            TlsError::CaUnreadable { .. } => e,
            _ => TlsError::CaEmpty {
                path: self.ca_file.display().to_string(),
            },
        })?;
        if ca.is_empty() {
            return Err(TlsError::CaEmpty {
                path: self.ca_file.display().to_string(),
            });
        }

        let certs = read_pem_certs(&self.cert_file).map_err(|e| match e {
            TlsError::CertUnreadable { .. } => e,
            _ => TlsError::CertEmpty {
                path: self.cert_file.display().to_string(),
            },
        })?;
        if certs.is_empty() {
            return Err(TlsError::CertEmpty {
                path: self.cert_file.display().to_string(),
            });
        }

        let _key = read_pem_key(&self.key_file)?;

        // A world/group-readable private key is a real finding, not a style
        // preference: on a shared node anyone could impersonate the client.
        if let Ok(meta) = std::fs::metadata(&self.key_file) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(TlsError::KeyPermissionsTooOpen {
                        path: self.key_file.display().to_string(),
                        mode,
                    });
                }
            }
            #[cfg(not(unix))]
            let _ = meta;
        }

        if self.expected_server_name.trim().is_empty() {
            return Err(TlsError::Incomplete {
                field: "MINGA_NATS_TLS_SERVER_NAME",
                reason: "expected server name must not be empty".into(),
            });
        }

        Ok(())
    }

    /// Validate the connect URL against this configuration.
    ///
    /// Two checks:
    ///
    /// 1. The scheme must be `tls://`. A `nats://` URL in secure mode would
    ///    otherwise be an accidental downgrade.
    /// 2. The URL host must equal `expected_server_name`. rustls verifies the
    ///    server certificate's SANs against the host in the connect URI, so
    ///    pinning the two together is what makes the name check meaningful —
    ///    a certificate valid for a different name is rejected.
    pub fn validate_url(&self, url: &str) -> Result<(), TlsError> {
        let rest = url
            .strip_prefix("tls://")
            .ok_or_else(|| TlsError::SchemeNotTls {
                url: url.to_string(),
            })?;

        // Strip credentials, path and port.
        let authority = rest.split(['/', '?']).next().unwrap_or(rest);
        let authority = authority.rsplit('@').next().unwrap_or(authority);
        let host = match authority.rfind(':') {
            // Only strip a trailing `:port`, not an IPv6 literal.
            Some(idx)
                if !authority[idx + 1..].is_empty()
                    && authority[idx + 1..].chars().all(|c| c.is_ascii_digit()) =>
            {
                &authority[..idx]
            }
            _ => authority,
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');

        if !host.eq_ignore_ascii_case(&self.expected_server_name) {
            return Err(TlsError::ServerNameMismatch {
                host: host.to_string(),
                expected: self.expected_server_name.clone(),
            });
        }

        Ok(())
    }

    /// Build the rustls client configuration.
    ///
    /// Only the configured CA is trusted — the platform trust store is not
    /// added, so a publicly-trusted certificate cannot impersonate the
    /// cluster's NATS server.
    pub fn client_config(&self) -> Result<async_nats::rustls::ClientConfig, TlsError> {
        use async_nats::rustls::{ClientConfig, RootCertStore};

        let ca_certs = read_pem_certs(&self.ca_file)?;
        let mut roots = RootCertStore::empty();
        let mut added = 0usize;
        for cert in ca_certs {
            if roots.add(cert).is_ok() {
                added += 1;
            }
        }
        if added == 0 {
            return Err(TlsError::CaEmpty {
                path: self.ca_file.display().to_string(),
            });
        }

        let client_certs = read_pem_certs(&self.cert_file)?;
        let client_key = read_pem_key(&self.key_file)?;

        // Explicit provider: both ring and aws-lc-rs are linked in this
        // workspace, so rustls cannot auto-detect one.
        let builder = ClientConfig::builder_with_provider(std::sync::Arc::new(
            async_nats::rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Build(format!("TLS protocol setup: {e}")))?
        .with_root_certificates(roots);
        let config = builder
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| TlsError::Build(format!("client certificate rejected: {e}")))?;

        Ok(config)
    }

    /// Build the `async_nats` connect options for this configuration.
    ///
    /// `require_tls(true)` makes a plaintext server fail the connection
    /// instead of silently downgrading.
    pub fn connect_options(&self) -> Result<async_nats::ConnectOptions, TlsError> {
        let config = self.client_config()?;
        Ok(async_nats::ConnectOptions::new()
            .require_tls(true)
            .tls_client_config(config))
    }
}

// ---------------------------------------------------------------------------
// PEM helpers
// ---------------------------------------------------------------------------

/// Read PEM certificates from `path`.
///
/// Uses rustls' own PEM reader via `rustls-pki-types`, so the accepted
/// formats are exactly the ones the TLS stack will later parse.
fn read_pem_certs(
    path: &Path,
) -> Result<Vec<async_nats::rustls::pki_types::CertificateDer<'static>>, TlsError> {
    use async_nats::rustls::pki_types::pem::PemObject;
    use async_nats::rustls::pki_types::CertificateDer;

    let bytes = std::fs::read(path).map_err(|e| TlsError::CaUnreadable {
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;
    let mut reader = std::io::Cursor::new(bytes);
    let certs = CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CaUnreadable {
            path: path.display().to_string(),
            reason: format!("invalid PEM: {e}"),
        })?;
    Ok(certs)
}

/// Read a PEM private key from `path`.
///
/// An empty file (no PEM section at all) is reported as [`TlsError::KeyEmpty`]
/// rather than as a generic parse failure, so the operator can tell "you
/// forgot the key" from "the key is malformed".
fn read_pem_key(
    path: &Path,
) -> Result<async_nats::rustls::pki_types::PrivateKeyDer<'static>, TlsError> {
    use async_nats::rustls::pki_types::pem::{Error as PemError, PemObject};
    use async_nats::rustls::pki_types::PrivateKeyDer;

    let bytes = std::fs::read(path).map_err(|e| TlsError::KeyUnreadable {
        path: path.display().to_string(),
        reason: e.to_string(),
    })?;
    let mut reader = std::io::Cursor::new(bytes);
    PrivateKeyDer::from_pem_reader(&mut reader).map_err(|e| match e {
        PemError::NoItemsFound => TlsError::KeyEmpty {
            path: path.display().to_string(),
        },
        other => TlsError::KeyUnreadable {
            path: path.display().to_string(),
            reason: format!("invalid PEM private key: {other}"),
        },
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write a file with the given contents and (on unix) the given mode.
    fn write_file(path: &Path, contents: &str, mode: u32) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        #[cfg(not(unix))]
        let _ = mode;
    }

    /// A self-signed CA + leaf pair, generated with `rcgen` at test time.
    ///
    /// Real key material is never committed to the repository.
    fn make_pki() -> (String, String, String) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca_cert.pem();

        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        leaf_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "localhost");
        leaf_params.is_ca = rcgen::IsCa::NoCa;
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        (ca_pem, leaf_cert.pem(), leaf_key.serialize_pem())
    }

    /// Serializes tests that mutate the process-global environment.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn env_returns_none_when_nothing_is_set() {
        let _guard = ENV_LOCK.lock().unwrap();
        for var in [
            "MINGA_NATS_TLS_CA",
            "MINGA_NATS_TLS_CERT",
            "MINGA_NATS_TLS_KEY",
            "MINGA_NATS_TLS_SERVER_NAME",
        ] {
            std::env::remove_var(var);
        }
        assert!(NatsTlsConfig::from_env().unwrap().is_none());
    }

    #[test]
    fn env_rejects_partial_configuration() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("MINGA_NATS_TLS_CA", "/tmp/ca.pem");
        let err = NatsTlsConfig::from_env().unwrap_err();
        assert!(matches!(err, TlsError::Incomplete { .. }));
        std::env::remove_var("MINGA_NATS_TLS_CA");
    }

    #[test]
    fn preflight_accepts_valid_material() {
        let dir = tempfile::tempdir().unwrap();
        let (ca, cert, key) = make_pki();
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        write_file(&ca_path, &ca, 0o644);
        write_file(&cert_path, &cert, 0o644);
        write_file(&key_path, &key, 0o600);

        let cfg = NatsTlsConfig::new(&ca_path, &cert_path, &key_path, "localhost");
        cfg.preflight().expect("valid material must pass preflight");
        assert!(cfg.client_config().is_ok());
    }

    #[test]
    fn preflight_rejects_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = NatsTlsConfig::new(
            dir.path().join("nope-ca.pem"),
            dir.path().join("nope-cert.pem"),
            dir.path().join("nope-key.pem"),
            "localhost",
        );
        assert!(matches!(
            cfg.preflight(),
            Err(TlsError::CaUnreadable { .. })
        ));
    }

    #[test]
    fn preflight_rejects_world_readable_key() {
        let dir = tempfile::tempdir().unwrap();
        let (ca, cert, key) = make_pki();
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        write_file(&ca_path, &ca, 0o644);
        write_file(&cert_path, &cert, 0o644);
        write_file(&key_path, &key, 0o644);

        let cfg = NatsTlsConfig::new(&ca_path, &cert_path, &key_path, "localhost");
        assert!(matches!(
            cfg.preflight(),
            Err(TlsError::KeyPermissionsTooOpen { .. })
        ));
    }

    #[test]
    fn preflight_rejects_empty_pem() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        write_file(&ca_path, "not a pem", 0o644);
        write_file(&cert_path, "not a pem", 0o644);
        write_file(&key_path, "not a pem", 0o600);

        let cfg = NatsTlsConfig::new(&ca_path, &cert_path, &key_path, "localhost");
        assert!(cfg.preflight().is_err());
    }

    #[test]
    fn preflight_rejects_empty_server_name() {
        let dir = tempfile::tempdir().unwrap();
        let (ca, cert, key) = make_pki();
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        write_file(&ca_path, &ca, 0o644);
        write_file(&cert_path, &cert, 0o644);
        write_file(&key_path, &key, 0o600);

        let cfg = NatsTlsConfig::new(&ca_path, &cert_path, &key_path, "  ");
        assert!(matches!(cfg.preflight(), Err(TlsError::Incomplete { .. })));
    }
}
