//! Node-local WireGuard key storage and lazy kernel interface setup.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{EventBus, KvOp};
#[cfg(target_os = "linux")]
use std::process::Command;
use tokio_stream::StreamExt;

use minga_net::KeyPair;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireGuardError {
    #[error("cannot access WireGuard key file {path}: {source}")]
    KeyFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid private key in {path}: {source}")]
    InvalidKey {
        path: PathBuf,
        #[source]
        source: minga_net::WgError,
    },
    #[error("WireGuard interface requires Linux")]
    UnsupportedPlatform,
    #[error("WireGuard is required for nodes with peers, but {0}")]
    MissingTool(String),
    #[error("WireGuard command failed: {0}")]
    Command(String),
}

/// Load this node's private key or create `<data_dir>/wg.key` with mode 0600.
pub fn load_or_generate_keypair(data_dir: &Path) -> Result<KeyPair, WireGuardError> {
    fs::create_dir_all(data_dir).map_err(|source| WireGuardError::KeyFile {
        path: data_dir.to_path_buf(),
        source,
    })?;
    let path = data_dir.join("wg.key");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    match options.open(&path) {
        Ok(mut file) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(std::fs::Permissions::from_mode(0o600))
                    .map_err(|source| WireGuardError::KeyFile {
                        path: path.clone(),
                        source,
                    })?;
            }
            let keypair = KeyPair::generate();
            file.write_all(keypair.private_key.as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|source| WireGuardError::KeyFile {
                    path: path.clone(),
                    source,
                })?;
            Ok(keypair)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read_keypair(&path),
        Err(source) => Err(WireGuardError::KeyFile { path, source }),
    }
}

fn read_keypair(path: &Path) -> Result<KeyPair, WireGuardError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(path)
        .map_err(|source| WireGuardError::KeyFile {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| WireGuardError::KeyFile {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(WireGuardError::KeyFile {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "expected a regular file",
            ),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|source| WireGuardError::KeyFile {
                    path: path.to_path_buf(),
                    source,
                })?;
        }
    }

    let mut private_key = String::new();
    file.read_to_string(&mut private_key)
        .map_err(|source| WireGuardError::KeyFile {
            path: path.to_path_buf(),
            source,
        })?;
    let private_key = private_key.trim();
    let public_key =
        KeyPair::public_from_private(private_key).map_err(|source| WireGuardError::InvalidKey {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(KeyPair {
        private_key: private_key.to_owned(),
        public_key,
    })
}

/// Create/configure the node interface when it has at least one peer.
///
/// Peer updates are applied by the mesh reconciler; this only ensures the
/// local interface, key, listen port and MTU are ready for those updates.
pub fn ensure_interface(
    key_path: &Path,
    listen_port: u16,
    mtu: u16,
    peer_count: usize,
) -> Result<(), WireGuardError> {
    if peer_count == 0 {
        return Ok(());
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (key_path, listen_port, mtu);
        Err(WireGuardError::UnsupportedPlatform)
    }
    #[cfg(target_os = "linux")]
    {
        if !Path::new("/sys/module/wireguard").exists() {
            return Err(WireGuardError::MissingTool(
                "the kernel WireGuard module is not loaded".into(),
            ));
        }
        let wg = Command::new("wg")
            .arg("--version")
            .output()
            .map_err(|error| WireGuardError::MissingTool(format!("wg is unavailable: {error}")))?;
        if !wg.status.success() {
            return Err(WireGuardError::MissingTool(
                "wg --version returned an error".into(),
            ));
        }

        let interface = minga_net::wireguard::WG_INTERFACE_NAME;
        let existing = Command::new("ip")
            .args(["link", "show", "dev", interface])
            .output()
            .map_err(|error| WireGuardError::MissingTool(format!("ip is unavailable: {error}")))?;
        if !existing.status.success() {
            run_ip(&["link", "add", "dev", interface, "type", "wireguard"])?;
        }
        run_command(Command::new("wg").args([
            "set",
            interface,
            "private-key",
            key_path.to_string_lossy().as_ref(),
            "listen-port",
            &listen_port.to_string(),
        ]))?;
        run_ip(&["link", "set", "dev", interface, "mtu", &mtu.to_string()])?;
        run_ip(&["link", "set", "dev", interface, "up"])?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn run_ip(args: &[&str]) -> Result<(), WireGuardError> {
    run_command(Command::new("ip").args(args))
}

#[cfg(target_os = "linux")]
fn run_command(command: &mut std::process::Command) -> Result<(), WireGuardError> {
    let output = command
        .output()
        .map_err(|error| WireGuardError::MissingTool(error.to_string()))?;
    if !output.status.success() {
        return Err(WireGuardError::Command(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn remove_interface() -> Result<(), WireGuardError> {
    let interface = minga_net::wireguard::WG_INTERFACE_NAME;
    let existing = Command::new("ip")
        .args(["link", "show", "dev", interface])
        .output()
        .map_err(|error| WireGuardError::MissingTool(format!("ip is unavailable: {error}")))?;
    if existing.status.success() {
        run_ip(&["link", "delete", "dev", interface])?;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn remove_interface() -> Result<(), WireGuardError> {
    Ok(())
}

/// Watches this node's peer assignment and lazily prepares its WireGuard interface.
pub async fn run_wireguard_assignments(
    bus: Arc<dyn EventBus>,
    node_id: u64,
    key_path: PathBuf,
    listen_port: u16,
    mtu: u16,
) {
    let key = format!("node.{node_id}.wg");
    let mut peer_count = 0;
    loop {
        let mut assignments = match bus.watch_assignments(&key).await {
            Ok(assignments) => assignments,
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot watch WireGuard peer assignment");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        while let Some(event) = assignments.next().await {
            if event.key != key {
                continue;
            }
            apply_peer_assignment(
                node_id,
                event.value.as_deref(),
                event.op == KvOp::Delete,
                &key_path,
                listen_port,
                mtu,
                &mut peer_count,
            );
        }
        tracing::warn!(
            node_id,
            "WireGuard peer assignment watch ended; reconnecting"
        );
    }
}

fn apply_peer_assignment(
    node_id: u64,
    value: Option<&[u8]>,
    deleted: bool,
    key_path: &Path,
    listen_port: u16,
    mtu: u16,
    peer_count: &mut usize,
) {
    let peers = match value {
        Some(value) => {
            match minga_state::codec::decode_wire::<minga_net::wireguard::WireGuardPeerSet>(value) {
                Ok(set) => set.peers.len(),
                Err(error) => {
                    tracing::warn!(node_id, error = %error, "invalid WireGuard peer assignment");
                    return;
                }
            }
        }
        None => 0,
    };

    if peers == 0 {
        if *peer_count > 0 || deleted {
            if let Err(error) = remove_interface() {
                tracing::warn!(node_id, error = %error, "cannot remove WireGuard interface");
            }
        }
        *peer_count = 0;
    } else if let Err(error) = ensure_interface(key_path, listen_port, mtu, peers) {
        tracing::warn!(node_id, error = %error, "cannot prepare WireGuard interface");
    } else {
        *peer_count = peers;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_key_is_private_and_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_generate_keypair(dir.path()).unwrap();
        let second = load_or_generate_keypair(dir.path()).unwrap();
        assert_eq!(first.public_key, second.public_key);
        assert_eq!(first.private_key, second.private_key);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.path().join("wg.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn existing_key_with_broad_permissions_is_restricted() {
        let dir = tempfile::tempdir().unwrap();
        let key = KeyPair::generate();
        let path = dir.path().join("wg.key");
        fs::write(&path, key.private_key).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        }
        let loaded = load_or_generate_keypair(dir.path()).unwrap();
        assert_eq!(
            loaded.public_key,
            KeyPair::public_from_private(&loaded.private_key).unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn no_interface_is_created_without_peers() {
        assert!(ensure_interface(Path::new("/missing/wg.key"), 51820, 1420, 0).is_ok());
    }
}
