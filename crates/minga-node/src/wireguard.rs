//! Node-local WireGuard key storage and lazy kernel interface setup.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{EventBus, KvOp};
#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(any(target_os = "linux", test))]
use std::collections::HashSet;
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
    #[error("invalid WireGuard assignment: {0}")]
    InvalidAssignment(String),
    #[error("WireGuard command failed: {0}")]
    Command(String),
    #[error("required WireGuard tool unavailable: {0}")]
    MissingTool(String),
    #[error("WireGuard mesh is supported only on Linux")]
    UnsupportedPlatform,
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

/// Watches this node's peer assignment and reconciles its WireGuard mesh.
pub async fn run_wireguard_assignments(
    bus: Arc<dyn EventBus>,
    node_id: u64,
    key_path: PathBuf,
    listen_port: u16,
    mtu: u16,
) {
    let key = format!("node.{node_id}.wg");
    let mut current = None;
    loop {
        let mut assignments = match bus.watch_assignments(&key).await {
            Ok(assignments) => assignments,
            Err(error) => {
                tracing::warn!(node_id, error = %error, "cannot watch WireGuard peer assignment");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut retry = false;
        while let Some(event) = assignments.next().await {
            if event.key != key {
                continue;
            }
            if let Err(error) = apply_peer_assignment(
                node_id,
                event.value.as_deref(),
                event.op == KvOp::Delete,
                &key_path,
                listen_port,
                mtu,
                &mut current,
            )
            .await
            {
                tracing::warn!(node_id, error = %error, "cannot apply WireGuard peer assignment");
                retry = true;
                break;
            }
        }
        tracing::warn!(
            node_id,
            "WireGuard peer assignment watch ended; reconnecting"
        );
        if retry {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(target_os = "linux")]
async fn apply_peer_assignment(
    node_id: u64,
    value: Option<&[u8]>,
    deleted: bool,
    key_path: &Path,
    listen_port: u16,
    mtu: u16,
    current: &mut Option<minga_net::wireguard::WireGuardPeerSet>,
) -> Result<(), WireGuardError> {
    let set = if deleted {
        None
    } else {
        value
            .map(|value| {
                minga_state::codec::decode_wire::<minga_net::wireguard::WireGuardPeerSet>(value)
                    .map_err(|error| WireGuardError::InvalidAssignment(error.to_string()))
            })
            .transpose()?
    };

    if let Some(set) = &set {
        validate_peer_set(set)?;
    }
    if set.as_ref() == current.as_ref() {
        match set.as_ref() {
            Some(set) if set.peers.is_empty() => return Ok(()),
            Some(_) => {
                #[cfg(target_os = "linux")]
                if interface_is_up()? {
                    return Ok(());
                }
                #[cfg(not(target_os = "linux"))]
                return Ok(());
            }
            None => {}
        }
    }

    let network_setup = minga_node_network_setup(key_path);
    let Some(set) = set else {
        #[cfg(target_os = "linux")]
        remove_interface()?;
        network_setup
            .teardown_wireguard_mesh()
            .await
            .map_err(|error| WireGuardError::Command(error.to_string()))?;
        *current = None;
        return Ok(());
    };
    if set.peers.is_empty() {
        #[cfg(target_os = "linux")]
        remove_interface()?;
        network_setup
            .setup_wireguard_mesh(&set.networks)
            .await
            .map_err(|error| WireGuardError::Command(error.to_string()))?;
        *current = Some(set);
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    ensure_interface(key_path, listen_port, mtu, set.peers.len())?;
    #[cfg(target_os = "linux")]
    {
        apply_peer_diff(&set, current.as_ref())?;
        network_setup
            .setup_wireguard_mesh(&set.networks)
            .await
            .map_err(|error| WireGuardError::Command(error.to_string()))?;
        apply_routes(&set)?;
    }
    *current = Some(set);
    tracing::info!(node_id, "WireGuard peer assignment applied");
    Ok(())
}

#[cfg(not(target_os = "linux"))]
async fn apply_peer_assignment(
    node_id: u64,
    value: Option<&[u8]>,
    deleted: bool,
    key_path: &Path,
    listen_port: u16,
    mtu: u16,
    current: &mut Option<minga_net::wireguard::WireGuardPeerSet>,
) -> Result<(), WireGuardError> {
    let _ = (node_id, value, deleted, key_path, listen_port, mtu, current);
    Err(WireGuardError::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
fn minga_node_network_setup(key_path: &Path) -> crate::netns::NetworkSetup {
    crate::netns::NetworkSetup::new(
        key_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("net"),
    )
}

#[cfg(any(target_os = "linux", test))]
fn validate_peer_set(set: &minga_net::wireguard::WireGuardPeerSet) -> Result<(), WireGuardError> {
    let mut network_ids = HashSet::new();
    let mut local_subnets = Vec::new();
    for network in &set.networks {
        if !network_ids.insert(&network.network_id) {
            return Err(WireGuardError::InvalidAssignment(format!(
                "duplicate network {}",
                network.network_id
            )));
        }
        let subnet = minga_net::Cidr::parse(&network.subnet).map_err(|error| {
            WireGuardError::InvalidAssignment(format!(
                "invalid subnet for network {}: {error}",
                network.network_id
            ))
        })?;
        if network.local {
            local_subnets.push(subnet);
        }
    }

    let mut public_keys = HashSet::new();
    for peer in &set.peers {
        if !minga_net::wireguard::validate_public_key(&peer.public_key) {
            return Err(WireGuardError::InvalidAssignment(format!(
                "invalid public key for node {}",
                peer.node_id
            )));
        }
        if !public_keys.insert(&peer.public_key) {
            return Err(WireGuardError::InvalidAssignment(format!(
                "duplicate peer key for node {}",
                peer.node_id
            )));
        }
        for allowed_ip in &peer.allowed_ips {
            let (ip, prefix) = allowed_ip.split_once('/').ok_or_else(|| {
                WireGuardError::InvalidAssignment(format!("invalid AllowedIP {allowed_ip:?}"))
            })?;
            let ip = ip.parse::<std::net::Ipv4Addr>().map_err(|_| {
                WireGuardError::InvalidAssignment(format!(
                    "AllowedIP must be an IPv4 /32: {allowed_ip:?}"
                ))
            })?;
            if prefix != "32" {
                return Err(WireGuardError::InvalidAssignment(format!(
                    "AllowedIP must be an IPv4 /32: {allowed_ip:?}"
                )));
            }
            if !local_subnets.iter().any(|subnet| subnet.contains(ip)) {
                return Err(WireGuardError::InvalidAssignment(format!(
                    "AllowedIP {allowed_ip:?} is outside local networks"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn interface_is_up() -> Result<bool, WireGuardError> {
    let output = Command::new("ip")
        .args([
            "-o",
            "link",
            "show",
            "dev",
            minga_net::wireguard::WG_INTERFACE_NAME,
        ])
        .output()
        .map_err(|error| WireGuardError::MissingTool(format!("ip is unavailable: {error}")))?;
    if !output.status.success() {
        return Ok(false);
    }
    let output = String::from_utf8_lossy(&output.stdout);
    let flags = output
        .split_once('<')
        .and_then(|(_, rest)| rest.split_once('>'))
        .map(|(flags, _)| flags)
        .unwrap_or_default();
    Ok(flags.split(',').any(|flag| flag == "UP"))
}

#[cfg(target_os = "linux")]
fn apply_peer_diff(
    set: &minga_net::wireguard::WireGuardPeerSet,
    previous: Option<&minga_net::wireguard::WireGuardPeerSet>,
) -> Result<(), WireGuardError> {
    use minga_net::wireguard::PeerConfig;

    let interface = minga_net::wireguard::WG_INTERFACE_NAME;
    let output = Command::new("wg")
        .args(["show", interface, "allowed-ips"])
        .output()
        .map_err(|error| WireGuardError::MissingTool(format!("wg is unavailable: {error}")))?;
    if !output.status.success() {
        return Err(WireGuardError::Command(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    let mut existing = HashMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((key, ips)) = line.split_once('\t') else {
            continue;
        };
        let mut ips: Vec<_> = ips
            .split(',')
            .filter(|ip| !ip.is_empty() && *ip != "(none)")
            .map(str::to_owned)
            .collect();
        ips.sort();
        existing.insert(key.to_owned(), ips);
    }
    let output = Command::new("wg")
        .args(["show", interface, "endpoints"])
        .output()
        .map_err(|error| WireGuardError::MissingTool(format!("wg is unavailable: {error}")))?;
    if !output.status.success() {
        return Err(WireGuardError::Command(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    let endpoints: HashMap<_, _> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            Some((fields.next()?.to_owned(), fields.next()?.to_owned()))
        })
        .collect();

    let desired: HashSet<_> = set
        .peers
        .iter()
        .map(|peer| peer.public_key.as_str())
        .collect();
    for key in existing
        .keys()
        .filter(|key| !desired.contains(key.as_str()))
    {
        run_command(Command::new("wg").args(["set", interface, "peer", key, "remove"]))?;
    }
    for peer in &set.peers {
        let mut allowed_ips = peer.allowed_ips.clone();
        allowed_ips.sort();
        let allowed_ips_changed = existing
            .get(&peer.public_key)
            .is_some_and(|current| current != &allowed_ips);
        let stale_endpoint = peer.endpoint.is_none()
            && endpoints
                .get(&peer.public_key)
                .is_some_and(|endpoint| endpoint != "(none)");
        let unchanged = previous
            .and_then(|previous| {
                previous
                    .peers
                    .iter()
                    .find(|old| old.public_key == peer.public_key)
            })
            .is_some_and(|old| old == peer)
            && existing
                .get(&peer.public_key)
                .is_some_and(|current| current == &allowed_ips)
            && !stale_endpoint;
        if unchanged {
            continue;
        }
        if allowed_ips_changed || stale_endpoint {
            run_command(Command::new("wg").args([
                "set",
                interface,
                "peer",
                &peer.public_key,
                "remove",
            ]))?;
        }
        let config = PeerConfig {
            public_key: peer.public_key.clone(),
            allowed_ips: peer.allowed_ips.clone(),
            endpoint: peer.endpoint.clone(),
            keepalive: peer.keepalive,
        };
        let args = config.to_wg_set_args(interface);
        let args: Vec<_> = args.iter().map(String::as_str).collect();
        run_command(Command::new("wg").args(args))?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_routes(set: &minga_net::wireguard::WireGuardPeerSet) -> Result<(), WireGuardError> {
    use std::net::Ipv4Addr;

    let interface = minga_net::wireguard::WG_INTERFACE_NAME;
    let output = Command::new("ip")
        .args(["-4", "route", "show", "dev", interface])
        .output()
        .map_err(|error| WireGuardError::MissingTool(format!("ip is unavailable: {error}")))?;
    if !output.status.success() {
        return Err(WireGuardError::Command(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    let mut current = HashSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(route) = line.split_whitespace().next() {
            if let Some(address) = route
                .split('/')
                .next()
                .and_then(|ip| ip.parse::<Ipv4Addr>().ok())
            {
                current.insert(address);
            }
        }
    }
    let desired: HashSet<Ipv4Addr> = set
        .peers
        .iter()
        .flat_map(|peer| peer.allowed_ips.iter())
        .filter_map(|ip| {
            ip.split_once('/')
                .and_then(|(address, _)| address.parse().ok())
        })
        .collect();
    for address in current.difference(&desired) {
        run_ip(&[
            "-4",
            "route",
            "del",
            &format!("{address}/32"),
            "dev",
            interface,
        ])?;
    }
    for address in desired.difference(&current) {
        run_ip(&[
            "-4",
            "route",
            "replace",
            &format!("{address}/32"),
            "dev",
            interface,
        ])?;
    }
    Ok(())
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
    #[test]
    fn peer_allowed_ips_must_belong_to_local_networks() {
        let key = KeyPair::generate();
        let set = minga_net::wireguard::WireGuardPeerSet {
            peers: vec![minga_net::wireguard::WireGuardPeerAssignment {
                node_id: 2,
                public_key: key.public_key,
                allowed_ips: vec!["10.243.0.30/32".into()],
                endpoint: None,
                keepalive: 25,
                path: minga_net::wireguard::WireGuardPeerPath::Direct,
            }],
            networks: vec![minga_net::wireguard::WireGuardNetworkAssignment {
                network_id: "shared".into(),
                subnet: "10.242.0.0/24".into(),
                local: true,
            }],
        };
        assert!(validate_peer_set(&set).is_err());
    }
}
