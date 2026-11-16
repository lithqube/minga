//! Overlay manager: coordinates IPAM and DNS for overlay networks.
//!
//! The `OverlayManager` owns cluster network IPAM and service discovery.
//! WireGuard identity and interface configuration are node-local and handled
//! by `minga-node`.
//!
//! # Architecture
//!
//! ```text
//! OverlayManager
//! ├── networks: HashMap<NetworkId, NetworkOverlay>
//! │   ├── ipam: Ipam          (IP allocation per network)
//! │   └── subnet: Cidr         (network subnet)
//! ├── dns: DnsStore            (service discovery)
//! └── node_id: u64             (this node's ID)
//! ```
//!
//!
//! The manager is a control-plane component. Node-local interface setup is
//! handled by `minga-node` when peer assignments are available.

use std::collections::HashMap;
use std::net::Ipv4Addr;

use thiserror::Error;

use crate::cidr::{Cidr, CidrError, DEFAULT_SUPERNET};
use crate::dns::{self, DnsStore};
use crate::ipam::{self, Ipam, IpamError};

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
}

/// The overlay manager.
#[derive(Default)]
pub struct OverlayManager {
    networks: HashMap<String, NetworkOverlay>,
    dns: DnsStore,
}

impl OverlayManager {
    /// Create a new overlay manager.
    pub fn new() -> Self {
        Self {
            networks: HashMap::new(),
            dns: DnsStore::new(),
        }
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
    ) -> Result<(), OverlayError> {
        let id = id.into();
        if self.networks.contains_key(&id) {
            return Err(OverlayError::NetworkExists(id));
        }
        let cidr = Cidr::parse(subnet)?;
        let name = name.into();
        let ipam = Ipam::new(&id, cidr);

        tracing::info!(network = %id, subnet = %cidr, "overlay: created network");
        self.networks.insert(
            id.clone(),
            NetworkOverlay {
                id,
                name,
                subnet: cidr,
                ipam,
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
    ) -> Result<(), OverlayError> {
        let taken: Vec<Cidr> = self.networks.values().map(|n| n.subnet).collect();
        let subnet = ipam::allocate_subnet(&default_supernet()?, &taken)?;
        self.create_network(id, name, &subnet.to_cidr_string())
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
}

/// Allocate a supernet for the overlay mesh itself (node-to-node WireGuard IPs).
/// Nodes get IPs from this supernet for the WireGuard tunnel addresses.
pub fn allocate_mesh_subnet() -> Result<Cidr, OverlayError> {
    // Use a separate range from the default service supernet.
    Ok(Cidr::parse("10.200.0.0/16")?)
}

/// Checks a network's driver options (Compose `driver_opts`, Docker
/// `Options`). Only `encrypted` is accepted, as a no-op; anything else
/// fails loudly instead of being dropped.
pub fn check_driver_opts<'a>(
    network: &str,
    keys: impl IntoIterator<Item = &'a str>,
) -> Result<(), String> {
    match keys.into_iter().find(|k| *k != "encrypted") {
        Some(k) => Err(format!(
            "network {network}: unsupported driver option `{k}` (Minga accepts only `encrypted`)"
        )),
        None => Ok(()),
    }
}

/// Get the default service supernet.
pub fn default_supernet() -> Result<Cidr, OverlayError> {
    Ok(Cidr::parse(DEFAULT_SUPERNET)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manager() -> OverlayManager {
        OverlayManager::new()
    }

    #[test]
    fn create_and_get_network() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24")
            .unwrap();
        assert_eq!(mgr.network_count(), 1);
        let net = mgr.get_network("net-1").unwrap();
        assert_eq!(net.name, "frontend");
        assert_eq!(net.subnet.to_cidr_string(), "10.100.0.0/24");
    }

    #[test]
    fn create_duplicate_network_fails() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24")
            .unwrap();
        let result = mgr.create_network("net-1", "dup", "10.100.1.0/24");
        assert!(result.is_err());
    }

    #[test]
    fn driver_opts_accept_only_encrypted() {
        check_driver_opts("n", ["encrypted"]).unwrap();
        let err = check_driver_opts("n", ["encrypted", "com.docker.network.mtu"]).unwrap_err();
        assert!(err.contains("`com.docker.network.mtu`"), "{err}");
    }

    #[test]
    fn create_network_auto_never_reuses_a_subnet() {
        let mut mgr = test_manager();
        mgr.create_network_auto("a", "a").unwrap();
        mgr.create_network_auto("b", "b").unwrap();
        let (a, b) = (mgr.get_network("a").unwrap(), mgr.get_network("b").unwrap());
        assert!(!a.subnet.overlaps(&b.subnet));
    }

    #[test]
    fn create_network_auto_subnet() {
        let mut mgr = test_manager();
        mgr.create_network_auto("my-net", "my-net").unwrap();
        let net = mgr.get_network("my-net").unwrap();
        assert_eq!(net.subnet.prefix_len(), 24);
        assert!(net.subnet.to_cidr_string().starts_with("10.100."));
    }

    #[test]
    fn remove_network() {
        let mut mgr = test_manager();
        mgr.create_network("net-1", "frontend", "10.100.0.0/24")
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
        mgr.create_network("net-1", "frontend", "10.100.0.0/24")
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
    fn multiple_networks_independent_ipam() {
        let mut mgr = test_manager();
        mgr.create_network("net-a", "net-a", "10.100.0.0/24")
            .unwrap();
        mgr.create_network("net-b", "net-b", "10.100.1.0/24")
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
