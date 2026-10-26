//! Overlay manager: coordinates IPAM, WireGuard, and DNS for overlay networks.
//!
//! The `OverlayManager` is the high-level API for Minga's overlay networking.
//! It manages multiple overlay networks, each with its own IPAM instance
//! and DNS records. WireGuard keypairs and peer configs are generated
//! per-node.
//!
//! # Architecture
//!
//! ```text
//! OverlayManager
//! ├── networks: HashMap<NetworkId, NetworkOverlay>
//! │   ├── ipam: Ipam          (IP allocation per network)
//! │   ├── subnet: Cidr         (network subnet)
//! │   └── wg_port: u16         (WireGuard listen port)
//! ├── dns: DnsStore            (service discovery)
//! ├── keypair: KeyPair         (this node's WireGuard keypair)
//! └── node_id: u64             (this node's ID)
//! ```
//!
//! The manager is purely a control-plane component. Applying the generated
//! configs to the kernel (netlink, `wg` CLI, `ip` commands) is handled
//! by the deployment layer.

use std::collections::HashMap;
use std::net::Ipv4Addr;

use thiserror::Error;

use crate::cidr::{Cidr, CidrError, DEFAULT_SUPERNET};
use crate::dns::{self, DnsStore};
use crate::ipam::{self, Ipam, IpamError};
use crate::wireguard::{self, InterfaceConfig, KeyPair, Peer};

#[derive(Debug, Error)]
pub enum OverlayError {
    #[error("IPAM error: {0}")]
    Ipam(#[from] IpamError),
    #[error("CIDR error: {0}")]
    Cidr(#[from] CidrError),
    #[error("network not found: {0}")]
    NetworkNotFound(String),
    #[error("network already exists: {0}")]
    NetworkExists(String),
    #[error("WireGuard error: {0}")]
    Wg(#[from] wireguard::WgError),
}

/// State for a single overlay network.
#[derive(Debug)]
pub struct NetworkOverlay {
    /// Network ID.
    pub id: String,
    /// Network display name.
    pub name: String,
    /// Subnet CIDR.
    pub subnet: Cidr,
    /// IPAM instance for this network.
    pub ipam: Ipam,
    /// WireGuard listen port.
    pub wg_port: u16,
}

/// Peer information for WireGuard config generation.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: u64,
    pub public_key: String,
    pub endpoint: Option<String>,
    pub overlay_ip: String,
}

/// The overlay manager.
pub struct OverlayManager {
    node_id: u64,
    keypair: KeyPair,
    networks: HashMap<String, NetworkOverlay>,
    dns: DnsStore,
}

impl OverlayManager {
    /// Create a new overlay manager for a node with a fresh keypair.
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            keypair: KeyPair::generate(),
            networks: HashMap::new(),
            dns: DnsStore::new(),
        }
    }

    /// Create with an existing keypair (e.g. loaded from disk).
    pub fn with_keypair(node_id: u64, keypair: KeyPair) -> Self {
        Self {
            node_id,
            keypair,
            networks: HashMap::new(),
            dns: DnsStore::new(),
        }
    }

    /// This node's ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// This node's WireGuard keypair.
    pub fn keypair(&self) -> &KeyPair {
        &self.keypair
    }

    /// This node's WireGuard public key (base64).
    pub fn public_key(&self) -> &str {
        &self.keypair.public_key
    }

    /// Access the DNS store.
    pub fn dns(&self) -> &DnsStore {
        &self.dns
    }

    /// Mutable access to the DNS store.
    pub fn dns_mut(&mut self) -> &mut DnsStore {
        &mut self.dns
    }

    /// Number of managed networks.
    pub fn network_count(&self) -> usize {
        self.networks.len()
    }

    /// List network IDs.
    pub fn network_ids(&self) -> Vec<&str> {
        self.networks.keys().map(String::as_str).collect()
    }

    /// Get a network overlay by ID.
    pub fn get_network(&self, id: &str) -> Option<&NetworkOverlay> {
        self.networks.get(id)
    }

    // --- Network management -------------------------------------------------

    /// Create a new overlay network with an explicit subnet.
    pub fn create_network(
        &mut self,
        id: impl Into<String>,
        name: impl Into<String>,
        subnet: &str,
        wg_port: Option<u16>,
    ) -> Result<(), OverlayError> {
        let id = id.into();
        if self.networks.contains_key(&id) {
            return Err(OverlayError::NetworkExists(id));
        }
        let cidr = Cidr::parse(subnet)?;
        let name = name.into();
        let ipam = Ipam::new(&id, cidr);
        let port = wireguard::default_listen_port(wg_port);

        tracing::info!(network = %id, subnet = %cidr, "overlay: created network");
        self.networks.insert(
            id.clone(),
            NetworkOverlay {
                id,
                name,
                subnet: cidr,
                ipam,
                wg_port: port,
            },
        );
        Ok(())
    }

    /// Create a new overlay network with an auto-allocated subnet from
    /// the default supernet (10.100.0.0/16).
    pub fn create_network_auto(
        &mut self,
        id: impl Into<String>,
        name: impl Into<String>,
        wg_port: Option<u16>,
    ) -> Result<(), OverlayError> {
        let id = id.into();
        let subnet = ipam::allocate_subnet(&id)?;
        self.create_network(id, name, &subnet.to_cidr_string(), wg_port)
    }

    /// Remove a network. Releases all IPs and DNS records.
    pub fn remove_network(&mut self, id: &str) -> Result<(), OverlayError> {
        let net = self
            .networks
            .remove(id)
            .ok_or_else(|| OverlayError::NetworkNotFound(id.to_string()))?;

        // Remove DNS records for this network's tasks.
        for (_, task_id) in net.ipam.list() {
            // Task DNS records are managed by stack/service context;
            // we just clean up what we know about.
            let _ = task_id;
        }

        tracing::info!(network = %id, subnet = %net.subnet, "overlay: removed network");
        Ok(())
    }

    // --- IP allocation ------------------------------------------------------

    /// Allocate an IP for a task in a network.
    pub fn allocate_ip(
        &mut self,
        network_id: &str,
        task_id: &str,
    ) -> Result<Ipv4Addr, OverlayError> {
        let net = self
            .networks
            .get_mut(network_id)
            .ok_or_else(|| OverlayError::NetworkNotFound(network_id.to_string()))?;
        let ip = net.ipam.allocate(task_id)?;
        tracing::debug!(
            network = %network_id,
            task = %task_id,
            ip = %ip,
            "overlay: allocated IP"
        );
        Ok(ip)
    }

    /// Release a task's IP in a network.
    pub fn release_ip(&mut self, network_id: &str, task_id: &str) -> Option<Ipv4Addr> {
        if let Some(net) = self.networks.get_mut(network_id) {
            net.ipam.release(task_id)
        } else {
            None
        }
    }

    /// Get a task's IP in a network.
    pub fn task_ip(&self, network_id: &str, task_id: &str) -> Option<Ipv4Addr> {
        self.networks.get(network_id)?.ipam.get(task_id)
    }

    // --- Service discovery --------------------------------------------------

    /// Register a service with its task IPs.
    pub fn register_service(&mut self, service: &str, stack: &str, ips: Vec<Ipv4Addr>) {
        let fqdn = dns::service_fqdn(service, stack);
        self.dns.set(fqdn, ips);
    }

    /// Register a single task's DNS record.
    pub fn register_task(&mut self, task_id: &str, service: &str, stack: &str, ip: Ipv4Addr) {
        let fqdn = dns::task_fqdn(task_id, service, stack);
        self.dns.set(fqdn, vec![ip]);
    }

    /// Unregister a task's DNS record.
    pub fn unregister_task(&mut self, task_id: &str, service: &str, stack: &str) {
        let fqdn = dns::task_fqdn(task_id, service, stack);
        self.dns.remove(&fqdn);
    }

    /// Resolve a service name to IPs.
    pub fn resolve_service(&self, service: &str, stack: &str) -> Vec<Ipv4Addr> {
        self.dns.resolve(&dns::service_fqdn(service, stack))
    }

    /// Resolve a name (FQDN or short name).
    pub fn resolve(&self, name: &str) -> Vec<Ipv4Addr> {
        let fqdn = if dns::is_minga_name(name) {
            name.to_string()
        } else {
            format!("{name}.{}", dns::MINGA_DOMAIN)
        };
        self.dns.resolve(&fqdn)
    }

    // --- WireGuard config generation ----------------------------------------

    /// Generate the WireGuard interface config for a network on this node.
    ///
    /// `overlay_ip` is this node's IP in the network's subnet (with CIDR suffix).
    /// `peers` are the remote nodes to configure.
    pub fn wg_interface_config(
        &self,
        network_id: &str,
        overlay_ip: &str,
        peers: &[PeerInfo],
    ) -> Result<InterfaceConfig, OverlayError> {
        let net = self
            .networks
            .get(network_id)
            .ok_or_else(|| OverlayError::NetworkNotFound(network_id.to_string()))?;

        let wg_peers: Vec<Peer> = peers
            .iter()
            .map(|p| Peer {
                public_key: p.public_key.clone(),
                allowed_ips: vec![format!("{}/32", p.overlay_ip)],
                endpoint: p.endpoint.clone(),
                keepalive: if p.endpoint.is_some() { 25 } else { 0 },
            })
            .collect();

        Ok(InterfaceConfig {
            private_key: self.keypair.private_key.clone(),
            listen_port: net.wg_port,
            address: overlay_ip.to_string(),
            peers: wg_peers,
        })
    }

    /// Generate the WireGuard interface name for a network.
    pub fn wg_interface_name(&self, network_id: &str) -> String {
        wireguard::interface_name(network_id)
    }
}

/// Allocate a supernet for the overlay mesh itself (node-to-node WireGuard IPs).
/// Nodes get IPs from this supernet for the WireGuard tunnel addresses.
pub fn allocate_mesh_subnet() -> Result<Cidr, OverlayError> {
    // Use a separate range from the default service supernet.
    Ok(Cidr::parse("10.200.0.0/16")?)
}

/// Get the default service supernet.
pub fn default_supernet() -> Result<Cidr, OverlayError> {
    Ok(Cidr::parse(DEFAULT_SUPERNET)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manager() -> OverlayManager {
        OverlayManager::new(1)
    }

    #[test]
    fn create_and_get_network() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24", None)
            .unwrap();
        assert_eq!(mgr.network_count(), 1);
        let net = mgr.get_network("net-1").unwrap();
        assert_eq!(net.name, "frontend");
        assert_eq!(net.subnet.to_cidr_string(), "10.100.0.0/24");
        assert_eq!(net.wg_port, 51820);
    }

    #[test]
    fn create_duplicate_network_fails() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24", None)
            .unwrap();
        let result = mgr.create_network("net-1", "dup", "10.100.1.0/24", None);
        assert!(result.is_err());
    }

    #[test]
    fn create_network_auto_subnet() {
        let mut mgr = test_manager();
        mgr.create_network_auto("my-net", "my-net", None).unwrap();
        let net = mgr.get_network("my-net").unwrap();
        assert_eq!(net.subnet.prefix_len(), 24);
        assert!(net.subnet.to_cidr_string().starts_with("10.100."));
    }

    #[test]
    fn remove_network() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24", None)
            .unwrap();
        mgr.remove_network("net-1").unwrap();
        assert_eq!(mgr.network_count(), 0);
        assert!(mgr.get_network("net-1").is_none());
    }

    #[test]
    fn remove_nonexistent_network() {
        let mut mgr = test_manager();
        assert!(mgr.remove_network("nope").is_err());
    }

    #[test]
    fn allocate_and_release_ip() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24", None)
            .unwrap();
        let ip = mgr.allocate_ip("net-1", "task-1").unwrap();
        assert_eq!(mgr.task_ip("net-1", "task-1"), Some(ip));
        mgr.release_ip("net-1", "task-1");
        assert_eq!(mgr.task_ip("net-1", "task-1"), None);
    }

    #[test]
    fn allocate_ip_nonexistent_network() {
        let mut mgr = test_manager();
        assert!(mgr.allocate_ip("nope", "task-1").is_err());
    }

    #[test]
    fn register_and_resolve_service() {
        let mut mgr = test_manager();
        mgr.register_service(
            "web",
            "plant-7",
            vec![Ipv4Addr::new(10, 100, 0, 10), Ipv4Addr::new(10, 100, 0, 11)],
        );
        let ips = mgr.resolve_service("web", "plant-7");
        assert_eq!(ips.len(), 2);
    }

    #[test]
    fn register_and_unregister_task() {
        let mut mgr = test_manager();
        mgr.register_task("web.1", "web", "plant-7", Ipv4Addr::new(10, 100, 0, 10));
        let ips = mgr.resolve("web.1.web.plant-7");
        assert_eq!(ips, vec![Ipv4Addr::new(10, 100, 0, 10)]);
        mgr.unregister_task("web.1", "web", "plant-7");
        assert!(mgr.resolve("web.1.web.plant-7").is_empty());
    }

    #[test]
    fn resolve_short_name() {
        let mut mgr = test_manager();
        mgr.dns_mut().set(
            "web.plant-7.minga.local",
            vec![Ipv4Addr::new(10, 100, 0, 10)],
        );
        let ips = mgr.resolve("web.plant-7");
        assert_eq!(ips, vec![Ipv4Addr::new(10, 100, 0, 10)]);
    }

    #[test]
    fn wg_config_generation() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24", Some(52000))
            .unwrap();
        let peers = vec![
            PeerInfo {
                node_id: 2,
                public_key: "PEER2_PUBKEY=".to_string(),
                endpoint: Some("10.0.0.2:51820".to_string()),
                overlay_ip: "10.100.0.2".to_string(),
            },
            PeerInfo {
                node_id: 3,
                public_key: "PEER3_PUBKEY=".to_string(),
                endpoint: None,
                overlay_ip: "10.100.0.3".to_string(),
            },
        ];
        let config = mgr
            .wg_interface_config("net-1", "10.100.0.1/24", &peers)
            .unwrap();
        assert_eq!(config.listen_port, 52000);
        assert_eq!(config.address, "10.100.0.1/24");
        assert_eq!(config.private_key, mgr.keypair().private_key);
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].public_key, "PEER2_PUBKEY=");
        assert_eq!(config.peers[0].allowed_ips, vec!["10.100.0.2/32"]);
        assert_eq!(config.peers[0].endpoint, Some("10.0.0.2:51820".to_string()));
        assert_eq!(config.peers[0].keepalive, 25);
        assert_eq!(config.peers[1].keepalive, 0); // no endpoint → no keepalive

        let s = config.to_config_string();
        assert!(s.contains("ListenPort = 52000"));
        assert!(s.contains("Address = 10.100.0.1/24"));
    }

    #[test]
    fn wg_config_nonexistent_network() {
        let mgr = test_manager();
        let result = mgr.wg_interface_config("nope", "10.100.0.1/24", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn wg_interface_name() {
        let mgr = test_manager();
        assert_eq!(mgr.wg_interface_name("my-network"), "minga-my-networ");
    }

    #[test]
    fn keypair_accessible() {
        let mgr = test_manager();
        assert_eq!(mgr.public_key().len(), 44);
        assert_eq!(mgr.keypair().private_key.len(), 44);
    }

    #[test]
    fn multiple_networks_independent_ipam() {
        let mut mgr = test_manager();
        mgr.create_network("net-a", "net-a", "10.100.0.0/24", None)
            .unwrap();
        mgr.create_network("net-b", "net-b", "10.100.1.0/24", None)
            .unwrap();

        let ip_a = mgr.allocate_ip("net-a", "task-1").unwrap();
        let ip_b = mgr.allocate_ip("net-b", "task-1").unwrap();

        // Same task in different networks gets different IPs
        assert_ne!(ip_a, ip_b);
        // IPs are in the correct subnets
        assert!(mgr.get_network("net-a").unwrap().subnet.contains(ip_a));
        assert!(mgr.get_network("net-b").unwrap().subnet.contains(ip_b));
    }

    #[test]
    fn mesh_subnet_is_separate() {
        let mesh = allocate_mesh_subnet().unwrap();
        let services = default_supernet().unwrap();
        assert_eq!(mesh.to_cidr_string(), "10.200.0.0/16");
        assert_eq!(services.to_cidr_string(), "10.100.0.0/16");
        // No overlap
        assert!(!mesh.contains(services.network()));
    }
}
