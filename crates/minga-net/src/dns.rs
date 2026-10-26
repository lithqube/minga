//! Service discovery via DNS-style A records.
//!
//! Maps service names to the IP addresses of their running tasks.
//! In Minga's overlay, each service gets a DNS name like
//! `<service>.<stack>.minga.local` that resolves to the overlay IPs
//! of all running replicas.
//!
//! # Record model
//!
//! - **Service A records**: `<service>.<stack>.minga.local` → [task IPs]
//! - **Task A records**: `<task-id>.<service>.<stack>.minga.local` → [task IP]
//! - **Node A records**: `node-<id>.minga.local` → [node overlay IP]
//!
//! Records are stored in memory and can be synced from the state store.
//! A DNS server implementation (UDP/TCP :53) is a follow-up; this module
//! provides the data model and resolution logic.

use std::collections::HashMap;
use std::net::Ipv4Addr;

/// The internal DNS domain suffix.
pub const MINGA_DOMAIN: &str = "minga.local";

/// A DNS record store for the overlay.
#[derive(Debug, Default)]
pub struct DnsStore {
    /// FQDN → list of IPs.
    records: HashMap<String, Vec<Ipv4Addr>>,
}

impl DnsStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a record.
    pub fn set(&mut self, name: impl Into<String>, ips: Vec<Ipv4Addr>) {
        self.records.insert(name.into().to_lowercase(), ips);
    }

    /// Add an IP to an existing record (or create it).
    pub fn add(&mut self, name: impl Into<String>, ip: Ipv4Addr) {
        let name = name.into().to_lowercase();
        self.records.entry(name).or_default().push(ip);
    }

    /// Remove an IP from a record. Removes the record if empty.
    pub fn remove_ip(&mut self, name: &str, ip: Ipv4Addr) {
        let name = name.to_lowercase();
        if let Some(ips) = self.records.get_mut(&name) {
            ips.retain(|&i| i != ip);
            if ips.is_empty() {
                self.records.remove(&name);
            }
        }
    }

    /// Remove a record entirely.
    pub fn remove(&mut self, name: &str) {
        self.records.remove(&name.to_lowercase());
    }

    /// Resolve a name to IPs. Returns empty vec if not found.
    pub fn resolve(&self, name: &str) -> Vec<Ipv4Addr> {
        self.records
            .get(&name.to_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    /// Check if a name exists.
    pub fn exists(&self, name: &str) -> bool {
        self.records.contains_key(&name.to_lowercase())
    }

    /// List all records (name, IPs), sorted by name.
    pub fn list(&self) -> Vec<(String, Vec<Ipv4Addr>)> {
        let mut entries: Vec<_> = self
            .records
            .iter()
            .map(|(name, ips)| (name.clone(), ips.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Clear all records.
    pub fn clear(&mut self) {
        self.records.clear();
    }
}

// ---------------------------------------------------------------------------
// Name construction helpers
// ---------------------------------------------------------------------------

/// Build a service FQDN: `<service>.<stack>.minga.local`.
pub fn service_fqdn(service: &str, stack: &str) -> String {
    format!("{service}.{stack}.{MINGA_DOMAIN}")
}

/// Build a task FQDN: `<task-id>.<service>.<stack>.minga.local`.
pub fn task_fqdn(task_id: &str, service: &str, stack: &str) -> String {
    format!("{task_id}.{service}.{stack}.{MINGA_DOMAIN}")
}

/// Build a node FQDN: `node-<id>.minga.local`.
pub fn node_fqdn(node_id: u64) -> String {
    format!("node-{node_id}.{MINGA_DOMAIN}")
}

/// Shorten a FQDN by stripping the minga.local suffix.
pub fn short_name(fqdn: &str) -> &str {
    fqdn.strip_suffix(&format!(".{MINGA_DOMAIN}"))
        .unwrap_or(fqdn)
}

/// Check if a name is within the minga.local domain.
pub fn is_minga_name(name: &str) -> bool {
    name == MINGA_DOMAIN || name.ends_with(&format!(".{MINGA_DOMAIN}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(n: u8) -> Ipv4Addr {
        Ipv4Addr::new(10, 100, 0, n)
    }

    #[test]
    fn set_and_resolve() {
        let mut dns = DnsStore::new();
        dns.set("web.plant-7.minga.local", vec![ip(10), ip(11)]);
        let resolved = dns.resolve("web.plant-7.minga.local");
        assert_eq!(resolved, vec![ip(10), ip(11)]);
    }

    #[test]
    fn resolve_case_insensitive() {
        let mut dns = DnsStore::new();
        dns.set("Web.Plant-7.Minga.Local", vec![ip(10)]);
        assert_eq!(dns.resolve("web.plant-7.minga.local"), vec![ip(10)]);
        assert_eq!(dns.resolve("WEB.PLANT-7.MINGA.LOCAL"), vec![ip(10)]);
    }

    #[test]
    fn resolve_not_found() {
        let dns = DnsStore::new();
        assert!(dns.resolve("nonexistent.minga.local").is_empty());
    }

    #[test]
    fn add_ip_to_record() {
        let mut dns = DnsStore::new();
        dns.add("web.plant-7.minga.local", ip(10));
        dns.add("web.plant-7.minga.local", ip(11));
        assert_eq!(dns.resolve("web.plant-7.minga.local"), vec![ip(10), ip(11)]);
    }

    #[test]
    fn remove_ip_from_record() {
        let mut dns = DnsStore::new();
        dns.set("web.plant-7.minga.local", vec![ip(10), ip(11)]);
        dns.remove_ip("web.plant-7.minga.local", ip(10));
        assert_eq!(dns.resolve("web.plant-7.minga.local"), vec![ip(11)]);
    }

    #[test]
    fn remove_ip_removes_empty_record() {
        let mut dns = DnsStore::new();
        dns.set("web.plant-7.minga.local", vec![ip(10)]);
        dns.remove_ip("web.plant-7.minga.local", ip(10));
        assert!(!dns.exists("web.plant-7.minga.local"));
        assert_eq!(dns.len(), 0);
    }

    #[test]
    fn remove_record() {
        let mut dns = DnsStore::new();
        dns.set("web.plant-7.minga.local", vec![ip(10)]);
        dns.remove("web.plant-7.minga.local");
        assert!(dns.is_empty());
    }

    #[test]
    fn list_records_sorted() {
        let mut dns = DnsStore::new();
        dns.set("b.minga.local", vec![ip(2)]);
        dns.set("a.minga.local", vec![ip(1)]);
        let entries = dns.list();
        assert_eq!(entries[0].0, "a.minga.local");
        assert_eq!(entries[1].0, "b.minga.local");
    }

    #[test]
    fn service_fqdn_format() {
        assert_eq!(service_fqdn("web", "plant-7"), "web.plant-7.minga.local");
    }

    #[test]
    fn task_fqdn_format() {
        assert_eq!(
            task_fqdn("web.1", "web", "plant-7"),
            "web.1.web.plant-7.minga.local"
        );
    }

    #[test]
    fn node_fqdn_format() {
        assert_eq!(node_fqdn(1), "node-1.minga.local");
        assert_eq!(node_fqdn(42), "node-42.minga.local");
    }

    #[test]
    fn short_name_strips_suffix() {
        assert_eq!(short_name("web.plant-7.minga.local"), "web.plant-7");
        assert_eq!(short_name("no-suffix"), "no-suffix");
    }

    #[test]
    fn is_minga_name_check() {
        assert!(is_minga_name("minga.local"));
        assert!(is_minga_name("web.plant-7.minga.local"));
        assert!(!is_minga_name("example.com"));
        assert!(!is_minga_name("web.plant-7.minga.local.evil.com"));
    }

    #[test]
    fn clear_all() {
        let mut dns = DnsStore::new();
        dns.set("a.minga.local", vec![ip(1)]);
        dns.set("b.minga.local", vec![ip(2)]);
        dns.clear();
        assert!(dns.is_empty());
    }

    #[test]
    fn multiple_services_same_stack() {
        let mut dns = DnsStore::new();
        dns.set(service_fqdn("web", "plant-7"), vec![ip(10), ip(11)]);
        dns.set(service_fqdn("api", "plant-7"), vec![ip(20)]);
        dns.set(service_fqdn("db", "plant-7"), vec![ip(30)]);

        assert_eq!(
            dns.resolve(&service_fqdn("web", "plant-7")),
            vec![ip(10), ip(11)]
        );
        assert_eq!(dns.resolve(&service_fqdn("api", "plant-7")), vec![ip(20)]);
        assert_eq!(dns.resolve(&service_fqdn("db", "plant-7")), vec![ip(30)]);
        assert_eq!(dns.len(), 3);
    }
}
