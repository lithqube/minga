//! NATS server sidecar lifecycle management.
//!
//! The manager spawns `nats-server` as a child process with a generated
//! config (JetStream enabled, pinned storage limits, localhost monitoring).
//! The sidecar is supervised for the lifetime of the manager process.
//!
//! Resolution order for the `nats-server` binary:
//! 1. `MINGA_NATS_SERVER_BIN` environment variable
//! 2. `nats-server` next to the `minga` executable
//! 3. `nats-server` in `PATH`
//!
//! Workers do not spawn a sidecar — they connect to the manager's NATS
//! server via `--nats-url`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;
use tokio::process::Child;

#[derive(Debug, Error)]
pub enum SidecarError {
    #[error("failed to create data directory: {0}")]
    DataDir(String),

    #[error("failed to write config file: {0}")]
    ConfigWrite(String),

    #[error("plaintext NATS sidecar may only listen on loopback")]
    InsecureListener,
    #[error(
        "NATS token, certificate mapping, and operator/JWT authentication are mutually exclusive"
    )]
    ConflictingAuth,
    #[error("invalid NATS operator/JWT configuration: {0}")]
    JwtConfig(String),
    #[error("failed to spawn nats-server: {0}")]
    Spawn(String),

    #[error("nats-server did not become ready within {0:?}")]
    ReadyTimeout(Duration),

    #[error("nats-server exited early with status: {0}")]
    EarlyExit(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// TLS material for the NATS **server** side.
///
/// Paths point at files the `nats-server` process reads itself. Minga never
/// parses the key and never copies it into the generated config — the config
/// only carries the path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTls {
    /// PEM file with the server certificate.
    pub cert_file: PathBuf,
    /// PEM file with the server private key.
    pub key_file: PathBuf,
    /// PEM file with the CA that signs client certificates.
    pub ca_file: PathBuf,
    /// Verify client certificates against `ca_file`.
    pub verify: bool,
    /// Map the client certificate identity to NATS permissions.
    pub verify_and_map: bool,
}

/// Cluster (route) TLS for a NATS server.
///
/// Separate from the client-facing TLS: routes are a different trust
/// relationship and must not silently reuse the client material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTls {
    /// Shared cluster identity; every route peer must use the same name.
    pub name: String,
    /// PEM file with the server certificate used for routes.
    pub cert_file: PathBuf,
    /// PEM file with the server private key used for routes.
    pub key_file: PathBuf,
    /// PEM file with the CA that signs peer server certificates.
    pub ca_file: PathBuf,
    /// Route listen port.
    pub port: u16,
    /// Peer routes, e.g. `nats://10.0.0.2:6222`.
    pub routes: Vec<String>,
}

/// NATS operator and account JWT resolver configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsJwtAuth {
    /// Public operator JWT file loaded by `nats-server`.
    pub operator_jwt_file: PathBuf,
    /// Full account JWT resolver directory.
    pub resolver_dir: PathBuf,
    /// Account JWT to preload into the full resolver.
    pub account_jwt_file: PathBuf,
    /// Control-plane account identity seed, held only by the manager.
    pub account_seed_file: PathBuf,
    /// Directory containing tenant-scoped signer seeds.
    pub tenant_signer_dir: PathBuf,
    /// NATS account public NKey matching the configured account JWT.
    pub account_public_key: String,
    /// SYS-account user credentials for resolver updates.
    pub system_credentials_file: PathBuf,
}

/// Options for the NATS sidecar.
#[derive(Debug, Clone)]
pub struct SidecarOptions {
    /// Operator/JWT client authentication instead of certificate mapping.
    pub jwt_auth: Option<NatsJwtAuth>,
    /// Data directory (config and JetStream store live under this).
    pub data_dir: PathBuf,
    /// Node ID (used in server_name).
    pub node_id: u64,
    /// Other manager identities in the NATS cluster.
    pub manager_node_ids: Vec<u64>,
    /// Client port (default 4222).
    pub client_port: u16,
    /// Monitor port (default 8222, localhost only).
    pub monitor_port: u16,
    /// JetStream max memory store (e.g. "64MB").
    pub max_memory_store: String,
    /// JetStream max file store (e.g. "8GB").
    pub max_file_store: String,
    /// Client authentication token. When set, every client (including the
    /// manager itself) must present it to connect.
    pub client_token: Option<String>,
    /// Address the client listener binds to.
    ///
    /// Defaults to loopback. A plaintext sidecar must never bind on a public
    /// interface; remote nodes require an explicitly configured TLS server.
    pub listen_addr: String,
    /// Client-facing TLS. `None` means plaintext (development only).
    pub tls: Option<ServerTls>,
    /// Cluster route TLS. `None` means no cluster configuration.
    pub cluster: Option<ClusterTls>,
}

impl Default for SidecarOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/minga"),
            node_id: 1,
            client_port: 4222,
            manager_node_ids: Vec::new(),
            monitor_port: 8222,
            max_memory_store: "64MB".to_string(),
            max_file_store: "8GB".to_string(),
            client_token: None,
            jwt_auth: None,
            listen_addr: "127.0.0.1".to_string(),
            tls: None,
            cluster: None,
        }
    }
}

/// A running NATS server sidecar process.
pub struct NatsSidecar {
    child: Child,
    client_url: String,
}

impl NatsSidecar {
    /// Spawn `nats-server` with the given options and wait for readiness.
    pub async fn start(opts: &SidecarOptions) -> Result<Self, SidecarError> {
        if opts.tls.is_none() && opts.listen_addr != "127.0.0.1" && opts.listen_addr != "::1" {
            return Err(SidecarError::InsecureListener);
        }
        if opts.tls.as_ref().is_some_and(|tls| tls.verify_and_map) && opts.client_token.is_some() {
            return Err(SidecarError::ConflictingAuth);
        }
        if let Some(jwt_auth) = &opts.jwt_auth {
            let Some(tls) = &opts.tls else {
                return Err(SidecarError::JwtConfig("requires client TLS".to_string()));
            };
            if opts.client_token.is_some() || tls.verify_and_map || !tls.verify {
                return Err(SidecarError::ConflictingAuth);
            }
            for path in [
                &jwt_auth.operator_jwt_file,
                &jwt_auth.account_jwt_file,
                &jwt_auth.account_seed_file,
                &jwt_auth.system_credentials_file,
            ] {
                crate::read_owner_only_file(path, 64 * 1024).map_err(SidecarError::JwtConfig)?;
            }
            if !jwt_auth.resolver_dir.is_dir() || !jwt_auth.tenant_signer_dir.is_dir() {
                return Err(SidecarError::JwtConfig(
                    "resolver and tenant signer directories must exist".to_string(),
                ));
            }
        }
        // Create data directories.
        let nats_dir = opts.data_dir.join("nats");
        let jetstream_dir = nats_dir.join("jetstream");
        // Owner-only: the config holds the client token and the stream holds
        // task specs with their environment values.
        crate::private::create_dir(&jetstream_dir)
            .and_then(|()| {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&nats_dir, std::fs::Permissions::from_mode(0o700))
            })
            .map_err(|e| SidecarError::DataDir(e.to_string()))?;
        if let Some(jwt_auth) = &opts.jwt_auth {
            let account_claim_path = jwt_auth
                .resolver_dir
                .join(format!("{}.jwt", jwt_auth.account_public_key));
            match std::fs::symlink_metadata(&account_claim_path) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    return Err(SidecarError::JwtConfig(format!(
                        "resolver claim path `{}` is not a regular file",
                        account_claim_path.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::copy(&jwt_auth.account_jwt_file, &account_claim_path).map_err(
                        |error| {
                            SidecarError::JwtConfig(format!(
                                "cannot preload account claim `{}`: {error}",
                                account_claim_path.display()
                            ))
                        },
                    )?;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(
                            &account_claim_path,
                            std::fs::Permissions::from_mode(0o600),
                        )
                        .map_err(|error| SidecarError::JwtConfig(error.to_string()))?;
                    }
                }
                Err(error) => return Err(SidecarError::JwtConfig(error.to_string())),
            }
        }

        // Generate and write config.
        let config_path = nats_dir.join("server.conf");
        let config = generate_config(opts, &jetstream_dir);
        crate::private::write_file(&config_path, config.as_bytes())
            .map_err(|e| SidecarError::ConfigWrite(e.to_string()))?;
        tracing::debug!(path = %config_path.display(), "generated nats-server config");

        // Find the binary.
        let bin = find_nats_server()?;
        tracing::info!(binary = %bin.display(), "found nats-server");

        // Spawn the process.
        let child = tokio::process::Command::new(&bin)
            .arg("-c")
            .arg(&config_path)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| SidecarError::Spawn(e.to_string()))?;

        let pid = child.id().unwrap_or(0);
        tracing::info!(pid, port = opts.client_port, "spawned nats-server sidecar");

        let scheme = if opts.tls.is_some() { "tls" } else { "nats" };
        let host = if opts.tls.is_some() {
            "localhost"
        } else {
            "127.0.0.1"
        };
        let client_url = format!("{scheme}://{host}:{}", opts.client_port);

        let mut sidecar = Self { child, client_url };

        // Wait for readiness.
        sidecar.wait_for_ready(opts.client_port).await?;

        Ok(sidecar)
    }

    /// The NATS client URL (e.g. `nats://127.0.0.1:4222`).
    pub fn client_url(&self) -> &str {
        &self.client_url
    }

    /// Wait for the NATS server to accept TCP connections on the client port.
    async fn wait_for_ready(&mut self, port: u16) -> Result<(), SidecarError> {
        let timeout = Duration::from_secs(10);
        let interval = Duration::from_millis(100);
        let start = std::time::Instant::now();

        loop {
            // Check if the process exited early.
            if let Some(status) = self.child.try_wait()? {
                return Err(SidecarError::EarlyExit(format!("exit status: {status}")));
            }

            // Try to connect.
            match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
                Ok(_) => {
                    tracing::info!(port, "nats-server is ready");
                    return Ok(());
                }
                Err(_) => {
                    if start.elapsed() > timeout {
                        return Err(SidecarError::ReadyTimeout(timeout));
                    }
                    tokio::time::sleep(interval).await;
                }
            }
        }
    }
}

impl Drop for NatsSidecar {
    fn drop(&mut self) {
        // kill_on_drop(true) handles this, but we also try explicitly.
        let _ = self.child.start_kill();
    }
}

// ---------------------------------------------------------------------------
// Config generation
// ---------------------------------------------------------------------------

/// Generate the NATS server config as a string.
fn generate_config(opts: &SidecarOptions, jetstream_dir: &Path) -> String {
    let mut out = String::new();

    out.push_str("# Minga NATS server config (generated - do not edit)\n");
    out.push_str(&format!("# Node: {}\n", opts.node_id));
    out.push_str(&format!("# Data dir: {}\n\n", opts.data_dir.display()));

    out.push_str(&format!(
        "listen: {}:{}\n",
        opts.listen_addr, opts.client_port
    ));
    out.push_str(&format!("server_name: minga-{}\n", opts.node_id));
    let verify_and_map = opts.tls.as_ref().is_some_and(|tls| tls.verify_and_map);
    if let Some(jwt_auth) = &opts.jwt_auth {
        out.push_str(&format!(
            "operator: \"{}\"\n",
            jwt_auth.operator_jwt_file.display()
        ));
        out.push_str("resolver: {\n    type: full\n");
        out.push_str(&format!(
            "    dir: \"{}\"\n}}\n",
            jwt_auth.resolver_dir.display()
        ));
    } else if !verify_and_map {
        if let Some(token) = &opts.client_token {
            out.push_str(&format!("authorization: {{ token: \"{token}\" }}\n"));
        }
    }
    out.push('\n');

    // Monitoring - localhost only. The monitoring endpoint is not protected by
    // client-certificate verification, so it must never listen on a routable
    // address.
    out.push_str(&format!("http: \"127.0.0.1:{}\"\n\n", opts.monitor_port));

    // Client-facing TLS.
    if let Some(tls) = &opts.tls {
        out.push_str("tls {\n");
        out.push_str(&format!("    cert_file: \"{}\"\n", tls.cert_file.display()));
        out.push_str(&format!("    key_file: \"{}\"\n", tls.key_file.display()));
        out.push_str(&format!("    ca_file: \"{}\"\n", tls.ca_file.display()));
        out.push_str(&format!("    verify: {}\n", tls.verify));
        if tls.verify_and_map {
            // Map the verified client certificate identity onto NATS
            // permissions, so a valid certificate is not automatically a
            // cluster administrator.
            out.push_str("    verify_and_map: true\n");
        }
        out.push_str("}\n\n");
    }

    if verify_and_map {
        out.push_str("authorization {\n  users = [\n");
        let mut manager_ids = opts.manager_node_ids.clone();
        manager_ids.push(opts.node_id);
        manager_ids.sort_unstable();
        manager_ids.dedup();
        for node_id in manager_ids {
            append_user(
                &mut out,
                &format!("minga-manager-{node_id}"),
                // $JS.FC: flow-control replies; without them an ordered
                // consumer (KV listing, watches) stalls on large payloads.
                &[
                    "$JS.API.>".into(),
                    "$JS.FC.>".into(),
                    "$KV.>".into(),
                    "minga.>".into(),
                ],
                &[
                    "$JS.API.>".into(),
                    "$KV.>".into(),
                    "minga.>".into(),
                    "_INBOX.>".into(),
                ],
            );
        }
        out.push_str("  ]\n}\n\n");
    }
    // Cluster routes - a separate trust relationship from client TLS.
    if let Some(cluster) = &opts.cluster {
        out.push_str("cluster {\n");
        out.push_str(&format!("    name: {}\n", cluster.name));
        out.push_str(&format!("    port: {}\n", cluster.port));
        if !cluster.routes.is_empty() {
            out.push_str("    routes = [\n");
            for route in &cluster.routes {
                out.push_str(&format!("        \"{route}\",\n"));
            }
            out.push_str("    ]\n");
        }
        out.push_str("    tls {\n");
        out.push_str(&format!(
            "        cert_file: \"{}\"\n",
            cluster.cert_file.display()
        ));
        out.push_str(&format!(
            "        key_file: \"{}\"\n",
            cluster.key_file.display()
        ));
        out.push_str(&format!(
            "        ca_file: \"{}\"\n",
            cluster.ca_file.display()
        ));
        out.push_str("        verify: true\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");
    }

    out.push_str("# JetStream, with storage limits pinned (not 75% defaults).\n");
    out.push_str("# The limits are only valid inside the jetstream block; at top level\n");
    out.push_str("# nats-server 2.x rejects them as unknown fields.\n");
    out.push_str("jetstream {\n");
    out.push_str(&format!("    store_dir: \"{}\"\n", jetstream_dir.display()));
    out.push_str(&format!(
        "    max_memory_store: {}\n",
        opts.max_memory_store
    ));
    out.push_str(&format!("    max_file_store: {}\n", opts.max_file_store));
    out.push_str("}\n");

    out
}

fn append_user(out: &mut String, identity: &str, publish: &[String], subscribe: &[String]) {
    out.push_str(&format!(
        "    {{\n      user: \"{identity}\"\n      permissions: {{\n"
    ));
    append_permission(out, "publish", publish);
    append_permission(out, "subscribe", subscribe);
    out.push_str("      }\n    },\n");
}

fn append_permission(out: &mut String, direction: &str, subjects: &[String]) {
    out.push_str(&format!("        {direction}: {{ allow: ["));
    for (index, subject) in subjects.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&format!("\"{subject}\""));
    }
    out.push_str("] }\n");
}

// ---------------------------------------------------------------------------
// Binary resolution
// ---------------------------------------------------------------------------

/// Find the `nats-server` binary.
///
/// Resolution order:
/// 1. `MINGA_NATS_SERVER_BIN` env var
/// 2. `nats-server` next to the current executable
/// 3. `nats-server` in `PATH` (returns just "nats-server" for PATH lookup)
fn find_nats_server() -> Result<PathBuf, SidecarError> {
    // 1. Env var.
    if let Ok(path) = std::env::var("MINGA_NATS_SERVER_BIN") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
        tracing::warn!(
            path = %path.display(),
            "MINGA_NATS_SERVER_BIN is set but the file does not exist"
        );
    }

    // 2. Next to the current executable.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("nats-server");
            if candidate.exists() {
                return Ok(candidate);
            }
        }
    }

    // 3. PATH — return bare name; the OS will resolve it.
    Ok(PathBuf::from("nats-server"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_contains_required_fields() {
        let opts = SidecarOptions {
            data_dir: PathBuf::from("/var/lib/minga"),
            node_id: 1,
            client_port: 4222,
            monitor_port: 8222,
            max_memory_store: "64MB".to_string(),
            max_file_store: "1GB".to_string(),
            ..SidecarOptions::default()
        };
        let jetstream_dir = Path::new("/var/lib/minga/nats/jetstream");
        let config = generate_config(&opts, jetstream_dir);

        assert!(config.contains("listen: 127.0.0.1:4222"));
        assert!(config.contains("server_name: minga-1"));
        assert!(config.contains("http: \"127.0.0.1:8222\""));
        assert!(config.contains("max_memory_store: 64MB"));
        assert!(config.contains("max_file_store: 1GB"));
        assert!(config.contains("jetstream {"));
        assert!(config.contains("store_dir: \"/var/lib/minga/nats/jetstream\""));
    }

    #[tokio::test]
    async fn plaintext_sidecar_rejects_public_listener() {
        let opts = SidecarOptions {
            listen_addr: "0.0.0.0".to_string(),
            ..SidecarOptions::default()
        };
        assert!(matches!(
            NatsSidecar::start(&opts).await,
            Err(SidecarError::InsecureListener)
        ));
    }

    #[test]
    fn config_uses_custom_ports() {
        let opts = SidecarOptions {
            data_dir: PathBuf::from("/tmp/minga-test"),
            node_id: 3,
            client_port: 14222,
            monitor_port: 18222,
            max_memory_store: "128MB".to_string(),
            max_file_store: "2GB".to_string(),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/tmp/minga-test/nats/jetstream"));

        assert!(config.contains("listen: 127.0.0.1:14222"));
        assert!(config.contains("server_name: minga-3"));
        assert!(config.contains("http: \"127.0.0.1:18222\""));
        assert!(config.contains("max_memory_store: 128MB"));
        assert!(config.contains("max_file_store: 2GB"));
    }

    #[test]
    fn config_without_tls_has_no_tls_block() {
        let opts = SidecarOptions::default();
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));

        assert!(!config.contains("tls {"));
        assert!(!config.contains("verify_and_map"));
        assert!(!config.contains("cluster {"));
    }

    #[test]
    fn config_with_tls_emits_client_tls_block() {
        let opts = SidecarOptions {
            tls: Some(ServerTls {
                cert_file: PathBuf::from("/etc/minga/tls/server-cert.pem"),
                key_file: PathBuf::from("/etc/minga/tls/server-key.pem"),
                ca_file: PathBuf::from("/etc/minga/tls/ca.pem"),
                verify: true,
                verify_and_map: true,
            }),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));

        assert!(config.contains("tls {"));
        assert!(config.contains("cert_file: \"/etc/minga/tls/server-cert.pem\""));
        assert!(config.contains("key_file: \"/etc/minga/tls/server-key.pem\""));
        assert!(config.contains("ca_file: \"/etc/minga/tls/ca.pem\""));
        assert!(config.contains("verify: true"));
        assert!(config.contains("verify_and_map: true"));
    }

    #[test]
    fn config_enables_operator_jwt_resolver_without_certificate_mapping() {
        let opts = SidecarOptions {
            jwt_auth: Some(NatsJwtAuth {
                operator_jwt_file: PathBuf::from("/etc/minga/nats/operator.jwt"),
                resolver_dir: PathBuf::from("/var/lib/minga/nats/resolver"),
                account_jwt_file: PathBuf::from("/etc/minga/nats/account.jwt"),
                account_seed_file: PathBuf::from("/etc/minga/nats/account.seed"),
                tenant_signer_dir: PathBuf::from("/etc/minga/nats/signers"),
                account_public_key: "A".to_string(),
                system_credentials_file: PathBuf::from("/etc/minga/nats/sys.creds"),
            }),
            tls: Some(ServerTls {
                cert_file: PathBuf::from("/etc/minga/tls/server-cert.pem"),
                key_file: PathBuf::from("/etc/minga/tls/server-key.pem"),
                ca_file: PathBuf::from("/etc/minga/tls/ca.pem"),
                verify: true,
                verify_and_map: false,
            }),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));

        assert!(config.contains("operator: \"/etc/minga/nats/operator.jwt\""));
        assert!(config.contains("resolver: {"));
        assert!(config.contains("type: full"));
        assert!(config.contains("dir: \"/var/lib/minga/nats/resolver\""));
        assert!(!config.contains("verify_and_map"));
        assert!(!config.contains("authorization:"));
    }

    #[tokio::test]
    async fn jwt_auth_rejects_missing_tls() {
        let opts = SidecarOptions {
            jwt_auth: Some(NatsJwtAuth {
                operator_jwt_file: PathBuf::from("/operator.jwt"),
                resolver_dir: PathBuf::from("/resolver"),
                account_jwt_file: PathBuf::from("/account.jwt"),
                account_seed_file: PathBuf::from("/account.seed"),
                tenant_signer_dir: PathBuf::from("/signers"),
                account_public_key: "A".to_string(),
                system_credentials_file: PathBuf::from("/sys.creds"),
            }),
            ..SidecarOptions::default()
        };
        assert!(matches!(
            NatsSidecar::start(&opts).await,
            Err(SidecarError::JwtConfig(_))
        ));
    }

    #[test]
    fn certificate_mapping_does_not_create_static_worker_users() {
        let opts = SidecarOptions {
            node_id: 1,
            manager_node_ids: vec![2],
            tls: Some(ServerTls {
                cert_file: PathBuf::from("/etc/minga/tls/server-cert.pem"),
                key_file: PathBuf::from("/etc/minga/tls/server-key.pem"),
                ca_file: PathBuf::from("/etc/minga/tls/ca.pem"),
                verify: true,
                verify_and_map: true,
            }),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));
        assert!(config.contains("user: \"minga-manager-2\""));
        assert!(!config.contains("minga-worker-"));
    }

    #[test]
    fn config_with_cluster_tls_emits_separate_route_block() {
        let opts = SidecarOptions {
            cluster: Some(ClusterTls {
                name: "minga-cluster".to_string(),
                cert_file: PathBuf::from("/etc/minga/tls/route-cert.pem"),
                key_file: PathBuf::from("/etc/minga/tls/route-key.pem"),
                ca_file: PathBuf::from("/etc/minga/tls/route-ca.pem"),
                port: 6222,
                routes: vec!["nats://10.0.0.2:6222".to_string()],
            }),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));

        assert!(config.contains("cluster {"));
        assert!(config.contains("port: 6222"));
        assert!(config.contains("\"nats://10.0.0.2:6222\""));
        // Route material is distinct from any client material.
        assert!(config.contains("cert_file: \"/etc/minga/tls/route-cert.pem\""));
        assert!(config.contains("key_file: \"/etc/minga/tls/route-key.pem\""));
        assert!(config.contains("ca_file: \"/etc/minga/tls/route-ca.pem\""));
    }

    #[test]
    fn config_never_embeds_key_material() {
        let opts = SidecarOptions {
            tls: Some(ServerTls {
                cert_file: PathBuf::from("/etc/minga/tls/server-cert.pem"),
                key_file: PathBuf::from("/etc/minga/tls/server-key.pem"),
                ca_file: PathBuf::from("/etc/minga/tls/ca.pem"),
                verify: true,
                verify_and_map: true,
            }),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));

        // Only the *path* to the key may appear - never PEM content.
        assert!(!config.contains("PRIVATE KEY"));
        assert!(!config.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn config_honours_restricted_listen_address() {
        let opts = SidecarOptions {
            listen_addr: "127.0.0.1".to_string(),
            ..SidecarOptions::default()
        };
        let config = generate_config(&opts, Path::new("/var/lib/minga/nats/jetstream"));

        assert!(config.contains("listen: 127.0.0.1:4222"));
        assert!(!config.contains("listen: 0.0.0.0"));
    }

    #[test]
    fn default_options() {
        let opts = SidecarOptions::default();
        assert_eq!(opts.client_port, 4222);
        assert_eq!(opts.monitor_port, 8222);
        assert_eq!(opts.max_memory_store, "64MB");
        assert_eq!(opts.max_file_store, "8GB");
    }

    #[test]
    fn find_nats_server_returns_path() {
        // This test just checks that find_nats_server doesn't panic.
        // It will return "nats-server" (PATH lookup) if the binary isn't found.
        let result = find_nats_server();
        assert!(result.is_ok());
    }

    #[test]
    fn find_nats_server_env_var() {
        // Create a temp file and point MINGA_NATS_SERVER_BIN at it.
        let dir = tempfile::tempdir().unwrap();
        let fake_bin = dir.path().join("nats-server");
        std::fs::write(&fake_bin, "#!/bin/sh\n").unwrap();

        // SAFETY: single-threaded test, env var is process-global.
        std::env::set_var("MINGA_NATS_SERVER_BIN", &fake_bin);
        let result = find_nats_server().unwrap();
        std::env::remove_var("MINGA_NATS_SERVER_BIN");

        assert_eq!(result, fake_bin);
    }

    #[tokio::test]
    async fn clustered_sidecars_keep_jetstream_available_after_one_manager_stops() {
        use minga_bus::{EventBus, NatsBus};
        use minga_state::{
            types::{ResourceSpec, Service, ServiceMode},
            StateStore,
        };
        use std::collections::{BTreeMap, HashMap};
        use std::net::{IpAddr, Ipv4Addr};

        // Held until each sidecar starts: a port released early can be taken
        // by a parallel test before the sidecar binds it.
        let mut listeners: Vec<_> = (0..9)
            .map(|_| Some(std::net::TcpListener::bind("127.0.0.1:0").unwrap()))
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .flatten()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();

        let raft_listeners: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let raft_addrs: Vec<String> = raft_listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        drop(raft_listeners);
        let raft_dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let raft_peers = [
            BTreeMap::from([(2, raft_addrs[1].clone()), (3, raft_addrs[2].clone())]),
            BTreeMap::from([(1, raft_addrs[0].clone()), (3, raft_addrs[2].clone())]),
            BTreeMap::from([(1, raft_addrs[0].clone()), (2, raft_addrs[1].clone())]),
        ];
        let (raft1, raft2, raft3) = tokio::try_join!(
            StateStore::open(
                1,
                &raft_addrs[0],
                raft_dirs[0].path(),
                raft_peers[0].clone()
            ),
            StateStore::open(
                2,
                &raft_addrs[1],
                raft_dirs[1].path(),
                raft_peers[1].clone()
            ),
            StateStore::open(
                3,
                &raft_addrs[2],
                raft_dirs[2].path(),
                raft_peers[2].clone()
            ),
        )
        .unwrap();
        let mut raft_stores = vec![raft1, raft2, raft3];
        let sample_service = |id: &str| Service {
            is_job: false,
            id: id.to_string(),
            tenant_id: "default".to_string(),
            name: id.to_string(),
            image: "registry.local/test:1".to_string(),
            replicas: 1,
            mode: ServiceMode::Replicated,
            constraints: Vec::new(),
            env: HashMap::new(),
            ports: Vec::new(),
            networks: Vec::new(),
            resources: ResourceSpec::default(),
            agent: None,
            egress: None,
            restart_policy: Default::default(),
            secrets: Vec::new(),
            command: Vec::new(),
            entrypoint: Vec::new(),
            labels: HashMap::new(),
            healthcheck: None,
            mounts: Vec::new(),
            user: None,
            extra_hosts: Vec::new(),
            update: Default::default(),
            rollback: None,
            previous: None,
        };

        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let mut tempdirs = Vec::new();
        let mut sides = Vec::new();
        let routes: Vec<String> = (0..3)
            .map(|i| format!("nats://127.0.0.1:{}", ports[6 + i]))
            .collect();

        for i in 0..3 {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
            params
                .subject_alt_names
                .push(rcgen::SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();

            let dir = tempfile::tempdir().unwrap();
            let cert_path = dir.path().join("route-cert.pem");
            let key_path = dir.path().join("route-key.pem");
            let ca_path = dir.path().join("route-ca.pem");
            std::fs::write(&cert_path, cert.pem()).unwrap();
            std::fs::write(&key_path, key.serialize_pem()).unwrap();
            std::fs::write(&ca_path, ca.pem()).unwrap();

            let cluster = ClusterTls {
                name: "minga-test".to_string(),
                cert_file: cert_path,
                key_file: key_path,
                ca_file: ca_path,
                port: ports[6 + i],
                routes: routes
                    .iter()
                    .enumerate()
                    .filter(|(peer, _)| *peer != i)
                    .map(|(_, route)| route.clone())
                    .collect(),
            };
            for port in [i, 3 + i, 6 + i] {
                listeners[port] = None;
            }
            let side = NatsSidecar::start(&SidecarOptions {
                data_dir: dir.path().to_path_buf(),
                node_id: i as u64 + 1,
                client_port: ports[i],
                monitor_port: ports[3 + i],
                cluster: Some(cluster),
                ..SidecarOptions::default()
            })
            .await
            .expect("start route-TLS sidecar");
            sides.push(side);
            tempdirs.push(dir);
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
        let seeds = sides
            .iter()
            .map(|side| side.client_url())
            .collect::<Vec<_>>()
            .join(",");
        let bus = NatsBus::connect(&seeds).await.expect("connect to sidecars");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if bus.ensure_replicas(3).await.is_ok() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "three route-TLS sidecars did not form a JetStream cluster"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // Stream creation and replica placement are separate from the
        // metadata quorum forming; let JetStream finish both before failure.
        tokio::time::sleep(Duration::from_secs(3)).await;

        bus.put_status("node.1.task.clustered", b"before")
            .await
            .unwrap();
        raft_stores[0]
            .upsert_service(sample_service("before"))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut stopped = sides.pop().unwrap();
        stopped.child.start_kill().unwrap();
        stopped.child.wait().await.unwrap();
        raft_stores.pop().unwrap().shutdown().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let status = bus.get_status("node.1.task.clustered").await;
            if status
                .as_ref()
                .is_ok_and(|value| value.as_deref() == Some(&b"before"[..]))
            {
                break;
            }
            let last_status = match status {
                Ok(value) => format!("unexpected value {value:?}"),
                Err(error) => error.to_string(),
            };
            assert!(
                tokio::time::Instant::now() < deadline,
                "status read did not recover after a manager sidecar stopped: {last_status}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if bus
                .put_status("node.1.task.clustered", b"after")
                .await
                .is_ok()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "status write did not recover after a manager sidecar stopped"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(
            bus.get_status("node.1.task.clustered")
                .await
                .unwrap()
                .as_deref(),
            Some(&b"after"[..])
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if raft_stores[0]
                .upsert_service(sample_service("after"))
                .await
                .is_ok()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Raft write did not recover after one manager stopped"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for store in &raft_stores {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                if store.get_service("after").await.unwrap().is_some() {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "post-failure Raft write did not replicate"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        for store in raft_stores {
            store.shutdown().await.unwrap();
        }
    }
}
