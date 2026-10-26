//! Network control and data plane: WireGuard overlay, IPAM, service discovery.
//!
//! The `minga-net` crate provides the building blocks for Minga's overlay
//! networking:
//!
//! - [`cidr`] — CIDR parsing, subnet arithmetic, host enumeration
//! - [`ipam`] — Deterministic IP address allocation per network
//! - [`wireguard`] — X25519 keypair generation, `wg-quick` config generation
//! - [`dns`] — Service discovery: A records, FQDN resolution
//! - [`overlay`] — [`OverlayManager`]: coordinates IPAM + WireGuard + DNS
//!
//! # Architecture
//!
//! Each overlay network gets a /24 subnet (auto-allocated from 10.100.0.0/16
//! or specified explicitly). Tasks get deterministic IPs from their network's
//! subnet. Nodes form a WireGuard mesh (10.200.0.0/16) for encrypted
//! node-to-node traffic. Service discovery maps service names to task IPs
//! under the `minga.local` domain.
//!
//! This crate is control-plane only. Applying configs to the kernel
//! (netlink, `wg` CLI, `ip` commands) is handled by the deployment layer.
//!
//! # Example
//!
//! ```
//! use minga_net::OverlayManager;
//! use std::net::Ipv4Addr;
//!
//! let mut mgr = OverlayManager::new(1);
//! mgr.create_network_auto("frontend", "frontend", None).unwrap();
//! let ip = mgr.allocate_ip("frontend", "web.1").unwrap();
//! mgr.register_service("web", "plant-7", vec![ip]);
//! let resolved = mgr.resolve_service("web", "plant-7");
//! assert_eq!(resolved, vec![ip]);
//! ```

pub mod cidr;
pub mod dns;
pub mod ipam;
pub mod overlay;
pub mod wireguard;

pub use cidr::{Cidr, CidrError, DEFAULT_SUPERNET, DEFAULT_WG_PORT};
pub use dns::{DnsStore, MINGA_DOMAIN};
pub use ipam::{Ipam, IpamError};
pub use overlay::{OverlayError, OverlayManager, PeerInfo};
pub use wireguard::{InterfaceConfig, KeyPair, Peer, WgError};
