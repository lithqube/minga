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

    #[error("failed to spawn nats-server: {0}")]
    Spawn(String),

    #[error("nats-server did not become ready within {0:?}")]
    ReadyTimeout(Duration),

    #[error("nats-server exited early with status: {0}")]
    EarlyExit(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
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
    format!(
        r#"# Minga NATS server config (generated — do not edit)
# Node: {}
# Data dir: {}

listen: 0.0.0.0:{}
server_name: minga-{}

# Monitoring — localhost only
http: "127.0.0.1:{}"

# JetStream storage limits (pinned, not 75% defaults)
max_memory_store: {}
max_file_store: {}

jetstream {{
    store_dir: "{}"
}}
"#,
        opts.node_id,
        opts.data_dir.display(),
        opts.client_port,
        opts.node_id,
        opts.monitor_port,
        opts.max_memory_store,
        opts.max_file_store,
        jetstream_dir.display(),
    )
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
        };
        let jetstream_dir = Path::new("/var/lib/minga/nats/jetstream");
        let config = generate_config(&opts, jetstream_dir);

        assert!(config.contains("listen: 0.0.0.0:4222"));
        assert!(config.contains("server_name: minga-1"));
        assert!(config.contains("http: \"127.0.0.1:8222\""));
        assert!(config.contains("max_memory_store: 64MB"));
        assert!(config.contains("max_file_store: 1GB"));
        assert!(config.contains("jetstream {"));
        assert!(config.contains("store_dir: \"/var/lib/minga/nats/jetstream\""));
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
        };
        let config = generate_config(&opts, Path::new("/tmp/minga-test/nats/jetstream"));

        assert!(config.contains("listen: 0.0.0.0:14222"));
        assert!(config.contains("server_name: minga-3"));
        assert!(config.contains("http: \"127.0.0.1:18222\""));
        assert!(config.contains("max_memory_store: 128MB"));
        assert!(config.contains("max_file_store: 2GB"));
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
