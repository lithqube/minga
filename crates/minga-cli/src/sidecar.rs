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

/// Options for the NATS sidecar.
#[derive(Debug, Clone)]
pub struct SidecarOptions {
    /// Data directory (config and JetStream store live under this).
    pub data_dir: PathBuf,
    /// Node ID (used in server_name).
    pub node_id: u64,
    /// Client port (default 4222).
    pub client_port: u16,
    /// Monitor port (default 8222, localhost only).
    pub monitor_port: u16,
    /// JetStream max memory store (e.g. "64MB").
    pub max_memory_store: String,
    /// JetStream max file store (e.g. "1GB").
    pub max_file_store: String,
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
            monitor_port: 8222,
            max_memory_store: "64MB".to_string(),
            max_file_store: "1GB".to_string(),
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
        // Create data directories.
        let nats_dir = opts.data_dir.join("nats");
        let jetstream_dir = nats_dir.join("jetstream");
        tokio::fs::create_dir_all(&jetstream_dir)
            .await
            .map_err(|e| SidecarError::DataDir(e.to_string()))?;

        // Generate and write config.
        let config_path = nats_dir.join("server.conf");
        let config = generate_config(opts, &jetstream_dir);
        tokio::fs::write(&config_path, &config)
            .await
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

        let client_url = format!("nats://127.0.0.1:{}", opts.client_port);

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
    out.push_str(&format!("server_name: minga-{}\n\n", opts.node_id));

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
        assert_eq!(opts.max_file_store, "1GB");
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
}
