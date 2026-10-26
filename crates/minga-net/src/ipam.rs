//! IP Address Management (IPAM) for overlay networks.
//!
//! Each overlay network gets a subnet (e.g. /24). IPs are allocated
//! deterministically from the subnet based on a task identifier, so the
//! same task always gets the same IP across restarts.
//!
//! # Allocation strategy
//!
//! 1. Hash the task ID to get a stable index into the subnet's usable hosts.
//! 2. If the IP is already allocated to a different task, probe forward
//!    (linear probing) until a free slot is found.
//! 3. Track allocations in memory; release on task removal.
//!
//! # Subnet allocation from a supernet
//!
//! When a network is created without an explicit subnet, one is allocated
//! from the default supernet (10.100.0.0/16) as a /24. The allocation is
//! deterministic based on the network ID hash.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;

use thiserror::Error;

use crate::cidr::{Cidr, CidrError, DEFAULT_SUPERNET};

#[derive(Debug, Error)]
pub enum IpamError {
    #[error("CIDR error: {0}")]
    Cidr(#[from] CidrError),
    #[error("subnet exhausted: no free IPs in {0}")]
    Exhausted(String),
    #[error("IP {0} is already allocated to task {1}")]
    AlreadyAllocated(String, String),
    #[error("task {0} has no IP allocated in network {1}")]
    NotAllocated(String, String),
    #[error("invalid subnet: {0}")]
    InvalidSubnet(String),
}

/// Manages IP allocations for a single overlay network.
#[derive(Debug)]
pub struct Ipam {
    /// The subnet for this network.
    subnet: Cidr,
    /// Allocated IPs: IP → task_id.
    allocations: HashMap<Ipv4Addr, String>,
    /// Task → allocated IP (reverse lookup).
    by_task: HashMap<String, Ipv4Addr>,
    /// Network ID (for error messages).
    network_id: String,
}

impl Ipam {
    /// Create a new IPAM for the given network and subnet.
    pub fn new(network_id: impl Into<String>, subnet: Cidr) -> Self {
        Self {
            subnet,
            allocations: HashMap::new(),
            by_task: HashMap::new(),
            network_id: network_id.into(),
        }
    }

    /// Create an IPAM, parsing the subnet from a CIDR string.
    pub fn with_subnet_str(network_id: impl Into<String>, subnet: &str) -> Result<Self, IpamError> {
        let cidr = Cidr::parse(subnet)?;
        Ok(Self::new(network_id, cidr))
    }

    /// The subnet CIDR.
    pub fn subnet(&self) -> &Cidr {
        &self.subnet
    }

    /// Number of currently allocated IPs.
    pub fn allocated_count(&self) -> usize {
        self.allocations.len()
    }

    /// Number of free IPs remaining.
    pub fn free_count(&self) -> u64 {
        self.subnet
            .usable_hosts()
            .saturating_sub(self.allocations.len() as u64)
    }

    /// Allocate an IP for a task. Deterministic: the same task_id always
    /// gets the same IP (until released).
    pub fn allocate(&mut self, task_id: &str) -> Result<Ipv4Addr, IpamError> {
        // Already allocated?
        if let Some(ip) = self.by_task.get(task_id) {
            return Ok(*ip);
        }

        // Deterministic starting index from task_id hash.
        let hash = hash_task_id(task_id);
        let capacity = self.subnet.usable_hosts();
        if capacity == 0 {
            return Err(IpamError::Exhausted(self.subnet.to_cidr_string()));
        }
        let start = hash % capacity;

        // Linear probing.
        for i in 0..capacity {
            let idx = (start + i) % capacity;
            if let Some(ip) = self.subnet.nth_host(idx) {
                if let std::collections::hash_map::Entry::Vacant(entry) = self.allocations.entry(ip)
                {
                    entry.insert(task_id.to_string());
                    self.by_task.insert(task_id.to_string(), ip);
                    tracing::debug!(
                        task = %task_id,
                        ip = %ip,
                        network = %self.network_id,
                        "IPAM: allocated IP"
                    );
                    return Ok(ip);
                }
            }
        }

        Err(IpamError::Exhausted(self.subnet.to_cidr_string()))
    }

    /// Release the IP allocated to a task.
    pub fn release(&mut self, task_id: &str) -> Option<Ipv4Addr> {
        if let Some(ip) = self.by_task.remove(task_id) {
            self.allocations.remove(&ip);
            tracing::debug!(
                task = %task_id,
                ip = %ip,
                network = %self.network_id,
                "IPAM: released IP"
            );
            Some(ip)
        } else {
            None
        }
    }

    /// Get the IP allocated to a task, if any.
    pub fn get(&self, task_id: &str) -> Option<Ipv4Addr> {
        self.by_task.get(task_id).copied()
    }

    /// Get the task that owns an IP, if any.
    pub fn get_task(&self, ip: Ipv4Addr) -> Option<&str> {
        self.allocations.get(&ip).map(String::as_str)
    }

    /// List all allocations (IP, task_id pairs), sorted by IP.
    pub fn list(&self) -> Vec<(Ipv4Addr, String)> {
        let mut entries: Vec<_> = self
            .allocations
            .iter()
            .map(|(ip, task)| (*ip, task.clone()))
            .collect();
        entries.sort_by_key(|(ip, _)| u32::from(*ip));
        entries
    }

    /// Restore allocations from a list of (task_id, IP) pairs.
    /// Used when recovering state after a restart.
    pub fn restore(&mut self, entries: &[(String, Ipv4Addr)]) {
        for (task_id, ip) in entries {
            if self.subnet.contains(*ip) {
                self.allocations.insert(*ip, task_id.clone());
                self.by_task.insert(task_id.clone(), *ip);
            }
        }
    }
}

/// Allocate a /24 subnet from the default supernet for a network.
/// Deterministic: the same network_id always gets the same subnet.
pub fn allocate_subnet(network_id: &str) -> Result<Cidr, IpamError> {
    let supernet = Cidr::parse(DEFAULT_SUPERNET)?;
    let hash = hash_task_id(network_id);
    let max_subnets = supernet.size() / 256; // /24 subnets in a /16
    let idx = hash % max_subnets;
    Ok(supernet.nth_subnet(24, idx)?)
}

/// Hash a string to a u64 (deterministic across runs).
fn hash_task_id(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ipam() -> Ipam {
        Ipam::with_subnet_str("test-net", "10.100.0.0/24").unwrap()
    }

    #[test]
    fn allocate_and_get() {
        let mut ipam = test_ipam();
        let ip = ipam.allocate("task-1").unwrap();
        assert_eq!(ipam.get("task-1"), Some(ip));
        assert_eq!(ipam.allocated_count(), 1);
    }

    #[test]
    fn allocate_is_deterministic() {
        let mut ipam1 = test_ipam();
        let mut ipam2 = test_ipam();
        let ip1 = ipam1.allocate("my-task").unwrap();
        let ip2 = ipam2.allocate("my-task").unwrap();
        assert_eq!(ip1, ip2, "same task_id should get same IP");
    }

    #[test]
    fn allocate_idempotent() {
        let mut ipam = test_ipam();
        let ip1 = ipam.allocate("task-1").unwrap();
        let ip2 = ipam.allocate("task-1").unwrap();
        assert_eq!(ip1, ip2);
        assert_eq!(ipam.allocated_count(), 1);
    }

    #[test]
    fn different_tasks_get_different_ips() {
        let mut ipam = test_ipam();
        let ip1 = ipam.allocate("task-1").unwrap();
        let ip2 = ipam.allocate("task-2").unwrap();
        assert_ne!(ip1, ip2);
        assert_eq!(ipam.allocated_count(), 2);
    }

    #[test]
    fn release_and_reallocate() {
        let mut ipam = test_ipam();
        let ip = ipam.allocate("task-1").unwrap();
        assert_eq!(ipam.release("task-1"), Some(ip));
        assert_eq!(ipam.get("task-1"), None);
        assert_eq!(ipam.allocated_count(), 0);

        // Reallocate — should get the same IP (deterministic).
        let ip2 = ipam.allocate("task-1").unwrap();
        assert_eq!(ip, ip2);
    }

    #[test]
    fn release_nonexistent() {
        let mut ipam = test_ipam();
        assert_eq!(ipam.release("nonexistent"), None);
    }

    #[test]
    fn get_task_reverse_lookup() {
        let mut ipam = test_ipam();
        let ip = ipam.allocate("task-1").unwrap();
        assert_eq!(ipam.get_task(ip), Some("task-1"));
        assert_eq!(ipam.get_task(Ipv4Addr::new(10, 100, 0, 200)), None);
    }

    #[test]
    fn list_allocations() {
        let mut ipam = test_ipam();
        ipam.allocate("task-a").unwrap();
        ipam.allocate("task-b").unwrap();
        let entries = ipam.list();
        assert_eq!(entries.len(), 2);
        // Sorted by IP
        assert!(u32::from(entries[0].0) <= u32::from(entries[1].0));
    }

    #[test]
    fn restore_allocations() {
        let mut ipam = test_ipam();
        let entries = vec![
            ("task-1".to_string(), Ipv4Addr::new(10, 100, 0, 10)),
            ("task-2".to_string(), Ipv4Addr::new(10, 100, 0, 20)),
        ];
        ipam.restore(&entries);
        assert_eq!(ipam.allocated_count(), 2);
        assert_eq!(ipam.get("task-1"), Some(Ipv4Addr::new(10, 100, 0, 10)));
        assert_eq!(ipam.get("task-2"), Some(Ipv4Addr::new(10, 100, 0, 20)));
    }

    #[test]
    fn restore_ignores_ips_outside_subnet() {
        let mut ipam = test_ipam();
        let entries = vec![
            ("task-1".to_string(), Ipv4Addr::new(10, 100, 0, 10)),
            ("task-bad".to_string(), Ipv4Addr::new(192, 168, 1, 1)), // outside subnet
        ];
        ipam.restore(&entries);
        assert_eq!(ipam.allocated_count(), 1);
        assert_eq!(ipam.get("task-bad"), None);
    }

    #[test]
    fn free_count() {
        let mut ipam = test_ipam();
        assert_eq!(ipam.free_count(), 254);
        ipam.allocate("task-1").unwrap();
        assert_eq!(ipam.free_count(), 253);
    }

    #[test]
    fn allocate_subnet_from_supernet() {
        let subnet1 = allocate_subnet("net-a").unwrap();
        let subnet2 = allocate_subnet("net-b").unwrap();
        // Both should be /24s in 10.100.0.0/16
        assert_eq!(subnet1.prefix_len(), 24);
        assert_eq!(subnet2.prefix_len(), 24);
        // Different networks get different subnets (with high probability)
        assert_ne!(subnet1.to_cidr_string(), subnet2.to_cidr_string());
    }

    #[test]
    fn allocate_subnet_is_deterministic() {
        let s1 = allocate_subnet("my-network").unwrap();
        let s2 = allocate_subnet("my-network").unwrap();
        assert_eq!(s1.to_cidr_string(), s2.to_cidr_string());
    }

    #[test]
    fn small_subnet_exhaustion() {
        let mut ipam = Ipam::with_subnet_str("tiny", "10.100.0.0/30").unwrap();
        // /30 has 2 usable hosts
        assert!(ipam.allocate("t1").is_ok());
        assert!(ipam.allocate("t2").is_ok());
        // Third allocation should exhaust (or collide and probe, but /30 only has 2)
        // With linear probing, the third will fail after checking all slots.
        let result = ipam.allocate("t3");
        assert!(result.is_err());
    }

    #[test]
    fn ip_in_subnet_range() {
        let mut ipam = test_ipam();
        let ip = ipam.allocate("task-1").unwrap();
        assert!(ipam.subnet().contains(ip));
    }
}
