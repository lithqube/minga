//! CIDR utilities for overlay IPAM.
//!
//! Thin wrapper around the `ipnet` crate with helpers specific to
//! Minga's overlay networking: deterministic host enumeration, subnet
//! allocation from a supernet, and validation.

use std::net::Ipv4Addr;

use ipnet::Ipv4Net;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CidrError {
    #[error("invalid CIDR: {0}")]
    Invalid(String),
    #[error("CIDR too small: {0} has only {1} usable addresses")]
    TooSmall(String, u64),
    #[error("no available subnets in {0}")]
    Exhausted(String),
    #[error("IP {0} is not in {1}")]
    NotInSubnet(String, String),
}

/// A parsed IPv4 CIDR with helper methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    net: Ipv4Net,
}

impl Cidr {
    /// Parse a CIDR string (e.g. "10.100.0.0/24").
    pub fn parse(s: &str) -> Result<Self, CidrError> {
        let net: Ipv4Net = s
            .parse()
            .map_err(|e| CidrError::Invalid(format!("{s}: {e}")))?;
        Ok(Self { net })
    }

    /// The raw `Ipv4Net`.
    pub fn net(&self) -> Ipv4Net {
        self.net
    }

    /// Network address (e.g. 10.100.0.0).
    pub fn network(&self) -> Ipv4Addr {
        self.net.network()
    }

    /// Broadcast address (e.g. 10.100.0.255).
    pub fn broadcast(&self) -> Ipv4Addr {
        self.net.broadcast()
    }

    /// Prefix length (e.g. 24).
    pub fn prefix_len(&self) -> u8 {
        self.net.prefix_len()
    }

    /// Total number of addresses in the subnet (including network/broadcast).
    pub fn size(&self) -> u64 {
        subnet_size(self.net.prefix_len())
    }

    /// Number of usable host addresses (excludes network and broadcast for /31, /32).
    pub fn usable_hosts(&self) -> u64 {
        usable_hosts(self.net)
    }

    /// Check if an IP is in this subnet.
    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        self.net.contains(&ip)
    }

    /// Iterate over usable host addresses (excludes network and broadcast).
    pub fn usable_hosts_iter(&self) -> impl Iterator<Item = Ipv4Addr> {
        usable_hosts_iter(self.net)
    }

    /// The Nth usable host address (0-indexed). Returns None if out of range.
    /// Index 0 = first usable host (network + 1).
    pub fn nth_host(&self, n: u64) -> Option<Ipv4Addr> {
        if n >= self.usable_hosts() {
            return None;
        }
        let base = u32::from(self.net.network());
        // For /31 and /32, all addresses are usable.
        let offset = match self.net.prefix_len() {
            31 | 32 => n,
            _ => n + 1, // skip network address
        };
        Some(Ipv4Addr::from(base + offset as u32))
    }

    /// Allocate a subnet of the given prefix length from this supernet.
    /// Returns the Nth such subnet (0-indexed).
    pub fn nth_subnet(&self, new_prefix: u8, n: u64) -> Result<Cidr, CidrError> {
        if new_prefix <= self.net.prefix_len() {
            return Err(CidrError::Invalid(format!(
                "subnet prefix /{new_prefix} must be larger than /{}",
                self.net.prefix_len()
            )));
        }
        let subnet_sz = 2u64.pow((32 - new_prefix) as u32);
        let supernet_size = self.size();
        let max_subnets = supernet_size / subnet_sz;
        if n >= max_subnets {
            return Err(CidrError::Exhausted(format!(
                "/{} (subnet /{new_prefix}, index {n})",
                self.net.prefix_len()
            )));
        }
        let base = u32::from(self.net.network());
        let subnet_base = base + (n * subnet_sz) as u32;
        let subnet = Ipv4Net::new(Ipv4Addr::from(subnet_base), new_prefix)
            .map_err(|e| CidrError::Invalid(e.to_string()))?;
        Ok(Cidr { net: subnet })
    }

    /// String representation (e.g. "10.100.0.0/24").
    pub fn to_cidr_string(&self) -> String {
        self.net.to_string()
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.net)
    }
}

/// Total number of addresses in a subnet of the given prefix length.
pub fn subnet_size(prefix_len: u8) -> u64 {
    2u64.pow((32 - prefix_len.min(32)) as u32)
}

/// Number of usable host addresses in a subnet.
pub fn usable_hosts(net: Ipv4Net) -> u64 {
    match net.prefix_len() {
        32 => 1,
        31 => 2, // point-to-point: both addresses usable
        p => {
            let size = subnet_size(p);
            if size >= 2 {
                size - 2
            } else {
                size
            }
        }
    }
}

/// Iterate over usable host addresses.
pub fn usable_hosts_iter(net: Ipv4Net) -> impl Iterator<Item = Ipv4Addr> {
    let network = u32::from(net.network());
    let broadcast = u32::from(net.broadcast());
    let (start, end) = match net.prefix_len() {
        32 => (network, network),
        31 => (network, broadcast),
        _ => (network + 1, broadcast.saturating_sub(1)),
    };
    (start..=end).map(Ipv4Addr::from)
}

/// Default overlay supernet: 10.100.0.0/16 (65,534 usable hosts, 256 /24 subnets).
pub const DEFAULT_SUPERNET: &str = "10.100.0.0/16";

/// Default WireGuard listen port.
pub const DEFAULT_WG_PORT: u16 = 51820;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_cidr() {
        let c = Cidr::parse("10.100.0.0/24").unwrap();
        assert_eq!(c.network(), Ipv4Addr::new(10, 100, 0, 0));
        assert_eq!(c.broadcast(), Ipv4Addr::new(10, 100, 0, 255));
        assert_eq!(c.prefix_len(), 24);
        assert_eq!(c.size(), 256);
        assert_eq!(c.usable_hosts(), 254);
    }

    #[test]
    fn parse_invalid_cidr() {
        assert!(Cidr::parse("not-a-cidr").is_err());
        assert!(Cidr::parse("10.0.0.0/33").is_err());
        assert!(Cidr::parse("10.0.0.0").is_err()); // missing prefix
    }

    #[test]
    fn contains_check() {
        let c = Cidr::parse("10.100.0.0/24").unwrap();
        assert!(c.contains(Ipv4Addr::new(10, 100, 0, 1)));
        assert!(c.contains(Ipv4Addr::new(10, 100, 0, 254)));
        assert!(!c.contains(Ipv4Addr::new(10, 100, 1, 1)));
        assert!(!c.contains(Ipv4Addr::new(10, 99, 0, 1)));
    }

    #[test]
    fn nth_host() {
        let c = Cidr::parse("10.100.0.0/24").unwrap();
        assert_eq!(c.nth_host(0), Some(Ipv4Addr::new(10, 100, 0, 1)));
        assert_eq!(c.nth_host(1), Some(Ipv4Addr::new(10, 100, 0, 2)));
        assert_eq!(c.nth_host(253), Some(Ipv4Addr::new(10, 100, 0, 254)));
        assert_eq!(c.nth_host(254), None); // out of range
    }

    #[test]
    fn nth_host_slash_30() {
        let c = Cidr::parse("10.100.0.0/30").unwrap();
        assert_eq!(c.usable_hosts(), 2);
        assert_eq!(c.nth_host(0), Some(Ipv4Addr::new(10, 100, 0, 1)));
        assert_eq!(c.nth_host(1), Some(Ipv4Addr::new(10, 100, 0, 2)));
        assert_eq!(c.nth_host(2), None);
    }

    #[test]
    fn usable_hosts_iter() {
        let c = Cidr::parse("10.100.0.0/30").unwrap();
        let hosts: Vec<_> = c.usable_hosts_iter().collect();
        assert_eq!(
            hosts,
            vec![Ipv4Addr::new(10, 100, 0, 1), Ipv4Addr::new(10, 100, 0, 2),]
        );
    }

    #[test]
    fn nth_subnet() {
        let supernet = Cidr::parse("10.100.0.0/16").unwrap();
        let s0 = supernet.nth_subnet(24, 0).unwrap();
        assert_eq!(s0.to_cidr_string(), "10.100.0.0/24");
        let s1 = supernet.nth_subnet(24, 1).unwrap();
        assert_eq!(s1.to_cidr_string(), "10.100.1.0/24");
        let s255 = supernet.nth_subnet(24, 255).unwrap();
        assert_eq!(s255.to_cidr_string(), "10.100.255.0/24");
        assert!(supernet.nth_subnet(24, 256).is_err());
    }

    #[test]
    fn nth_subnet_invalid_prefix() {
        let supernet = Cidr::parse("10.100.0.0/24").unwrap();
        assert!(supernet.nth_subnet(24, 0).is_err()); // same prefix
        assert!(supernet.nth_subnet(16, 0).is_err()); // smaller prefix
    }

    #[test]
    fn display() {
        let c = Cidr::parse("10.100.0.0/24").unwrap();
        assert_eq!(format!("{c}"), "10.100.0.0/24");
    }

    #[test]
    fn default_supernet() {
        let c = Cidr::parse(DEFAULT_SUPERNET).unwrap();
        assert_eq!(c.prefix_len(), 16);
        assert_eq!(c.size(), 65536);
        assert_eq!(c.usable_hosts(), 65534);
    }

    #[test]
    fn slash_31_both_usable() {
        let c = Cidr::parse("10.100.0.0/31").unwrap();
        assert_eq!(c.usable_hosts(), 2);
        assert_eq!(c.nth_host(0), Some(Ipv4Addr::new(10, 100, 0, 0)));
        assert_eq!(c.nth_host(1), Some(Ipv4Addr::new(10, 100, 0, 1)));
    }

    #[test]
    fn slash_32_single_host() {
        let c = Cidr::parse("10.100.0.5/32").unwrap();
        assert_eq!(c.usable_hosts(), 1);
        assert_eq!(c.nth_host(0), Some(Ipv4Addr::new(10, 100, 0, 5)));
        assert_eq!(c.nth_host(1), None);
    }
}
