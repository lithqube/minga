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
    let key_file_error = |path: &Path| {
        let path = path.to_path_buf();
        move |source| WireGuardError::KeyFile { path, source }
    };
    // Persist the `wg.key` directory entry before the key is used: a concurrent
    // creator may have linked it without syncing the directory yet.
    let sync_dir = || {
        fs::File::open(data_dir)
            .and_then(|dir| dir.sync_all())
            .map_err(key_file_error(data_dir))
    };
    let path = data_dir.join("wg.key");
    if path.symlink_metadata().is_ok() {
        let keypair = read_keypair(&path)?;
        sync_dir()?;
        return Ok(keypair);
    }

    // Write the key to a private temp file, then link it into place: `wg.key`
    // only ever appears complete, and a concurrent start reads the winner's.
    static NEXT_TMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = data_dir.join(format!(
        "wg.key.tmp.{}.{}",
        std::process::id(),
        NEXT_TMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let keypair = KeyPair::generate();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // A leftover from a crashed process with a reused pid is stale.
    let _ = fs::remove_file(&tmp);
    let mut file = options.open(&tmp).map_err(key_file_error(&tmp))?;
    let linked = file
        .write_all(keypair.private_key.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(key_file_error(&tmp))
        .map(|()| fs::hard_link(&tmp, &path));
    drop(file);
    let _ = fs::remove_file(&tmp);
    let keypair = match linked? {
        Ok(()) => keypair,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read_keypair(&path)?,
        Err(source) => return Err(key_file_error(&path)(source)),
    };
    sync_dir()?;
    Ok(keypair)
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
    node_id: u64,
    peer_count: usize,
) -> Result<(), WireGuardError> {
    if peer_count == 0 {
        return Ok(());
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (key_path, listen_port, mtu, node_id);
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
        let mesh_address = minga_net::overlay::mesh_node_ip(node_id)
            .map_err(|error| WireGuardError::InvalidAssignment(error.to_string()))?;
        run_ip(&[
            "addr",
            "replace",
            &format!("{mesh_address}/32"),
            "dev",
            interface,
        ])?;
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

        while let Some(event) = assignments.next().await {
            if event.key != key {
                continue;
            }
            // `ip` and `wg` block; keep them off the runtime the scheduler
            // and supervisor share.
            let key_path = key_path.clone();
            let runtime = tokio::runtime::Handle::current();
            let mut current_state = current.clone();
            let result = tokio::task::spawn_blocking(move || {
                runtime
                    .block_on(apply_peer_assignment(
                        node_id,
                        event.value.as_deref(),
                        event.op == KvOp::Delete,
                        &key_path,
                        listen_port,
                        mtu,
                        &mut current_state,
                    ))
                    .map(|()| current_state)
            })
            .await;
            match result {
                Ok(Ok(updated)) => current = updated,
                Ok(Err(error)) => {
                    tracing::warn!(node_id, error = %error, "cannot apply WireGuard peer assignment");
                    // A failed apply may have changed the kernel partway; forget
                    // the cached set so the retry re-applies every peer.
                    current = None;
                    break;
                }
                Err(error) => {
                    tracing::warn!(node_id, error = %error, "WireGuard peer assignment task failed");
                    current = None;
                    break;
                }
            }
        }
        tracing::warn!(
            node_id,
            "WireGuard peer assignment watch ended; reconnecting"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
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
        validate_peer_set(set, node_id)?;
    }
    if set.as_ref() == current.as_ref() {
        match set.as_ref() {
            Some(set) if set.peers.is_empty() => return Ok(()),
            #[cfg(target_os = "linux")]
            Some(_) if interface_is_up()? => return Ok(()),
            #[cfg(not(target_os = "linux"))]
            Some(_) => return Ok(()),
            _ => {}
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
    ensure_interface(key_path, listen_port, mtu, node_id, set.peers.len())?;
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
fn validate_peer_set(
    set: &minga_net::wireguard::WireGuardPeerSet,
    node_id: u64,
) -> Result<(), WireGuardError> {
    let mesh_error = |error: minga_net::overlay::OverlayError| {
        WireGuardError::InvalidAssignment(error.to_string())
    };
    let mesh_subnet = minga_net::overlay::allocate_mesh_subnet().map_err(mesh_error)?;
    let own_mesh_ip = minga_net::overlay::mesh_node_ip(node_id).map_err(mesh_error)?;
    let mut network_ids = HashSet::new();
    let mut allowed_subnets = Vec::new();
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
        if network.local || network.relay || network.ingress {
            allowed_subnets.push(subnet);
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
            let peer_mesh_ip =
                minga_net::overlay::mesh_node_ip(peer.node_id).map_err(mesh_error)?;
            // A peer claiming this node's own mesh address would capture its
            // replies, so no path may carry it.
            if ip == own_mesh_ip {
                return Err(WireGuardError::InvalidAssignment(format!(
                    "AllowedIP {allowed_ip:?} is this node's mesh address"
                )));
            }
            // A relay manager forwards for the NAT workers behind it, so it
            // also carries their mesh addresses; a direct peer carries only its own.
            let relayed_mesh_ip = peer.path == minga_net::wireguard::WireGuardPeerPath::Relay
                && mesh_subnet.contains(ip);
            if !allowed_subnets.iter().any(|subnet| subnet.contains(ip))
                && ip != peer_mesh_ip
                && !relayed_mesh_ip
            {
                return Err(WireGuardError::InvalidAssignment(format!(
                    "AllowedIP {allowed_ip:?} is outside assigned task and mesh addresses"
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
        // `wg show <if> allowed-ips` separates addresses with spaces.
        let mut ips: Vec<_> = ips
            .split_whitespace()
            .filter(|ip| *ip != "(none)")
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
    fn concurrent_starts_agree_on_one_complete_key() {
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let start = Arc::new(std::sync::Barrier::new(32));
            let handles: Vec<_> = (0..32)
                .map(|_| {
                    let path = dir.path().to_path_buf();
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        load_or_generate_keypair(&path).map(|keypair| keypair.private_key)
                    })
                })
                .collect();
            let keys: Vec<_> = handles
                .into_iter()
                .map(|handle| handle.join().unwrap().expect("no start sees a partial key"))
                .collect();
            assert!(keys.iter().all(|key| key == &keys[0]));
            let leftovers: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(leftovers, ["wg.key"], "no temp files are left behind");
        }
    }

    #[test]
    fn no_interface_is_created_without_peers() {
        assert!(ensure_interface(Path::new("/missing/wg.key"), 51820, 1420, 1, 0).is_ok());
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
                relay: false,
                ingress: false,
            }],
        };
        let mut mesh_address = set.clone();
        mesh_address.networks.clear();
        mesh_address.peers[0].allowed_ips = vec!["10.200.0.2/32".into()];
        assert!(validate_peer_set(&mesh_address, 1).is_ok());
        let mut ingress_route = set.clone();
        ingress_route.networks[0].local = false;
        ingress_route.networks[0].ingress = true;
        ingress_route.networks[0].subnet = "10.243.0.0/24".into();
        assert!(validate_peer_set(&ingress_route, 1).is_ok());
        let mut relayed = set.clone();
        relayed
            .networks
            .push(minga_net::wireguard::WireGuardNetworkAssignment {
                network_id: "relay".into(),
                subnet: "10.243.0.0/24".into(),
                local: false,
                relay: true,
                ingress: false,
            });
        assert!(validate_peer_set(&relayed, 1).is_ok());
        assert!(validate_peer_set(&set, 1).is_err());
    }

    #[test]
    fn relay_manager_may_carry_relayed_workers_mesh_addresses() {
        // Worker 2's view: manager 1 relays for it and for NAT worker 3, so
        // the scheduler lists worker 3's mesh address on the manager peer.
        let relay_set = |path| minga_net::wireguard::WireGuardPeerSet {
            peers: vec![minga_net::wireguard::WireGuardPeerAssignment {
                node_id: 1,
                public_key: KeyPair::generate().public_key,
                allowed_ips: vec![
                    "10.242.0.10/32".into(),
                    "10.200.0.1/32".into(),
                    "10.200.0.3/32".into(),
                ],
                endpoint: Some("192.0.2.1:51820".into()),
                keepalive: 25,
                path,
            }],
            networks: vec![minga_net::wireguard::WireGuardNetworkAssignment {
                network_id: "shared".into(),
                subnet: "10.242.0.0/24".into(),
                local: true,
                relay: false,
                ingress: true,
            }],
        };
        let relay = relay_set(minga_net::wireguard::WireGuardPeerPath::Relay);
        assert!(validate_peer_set(&relay, 2).is_ok());
        // A direct peer may carry only its own mesh address.
        let direct = relay_set(minga_net::wireguard::WireGuardPeerPath::Direct);
        assert!(validate_peer_set(&direct, 2).is_err());
        // No peer may claim the receiving node's own mesh address.
        let mut hijack = relay;
        hijack.peers[0].allowed_ips.push("10.200.0.2/32".into());
        assert!(validate_peer_set(&hijack, 2).is_err());
    }
}
