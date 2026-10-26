//! WireGuard keypair generation and config file generation.
//!
//! Uses X25519 (Curve25519) for keypairs, matching WireGuard's crypto.
//! Keys are base64-encoded in the standard WireGuard format (44 chars).
//!
//! # Config generation
//!
//! Generates `wg-quick` compatible INI config files for the local
//! interface and peer stanzas for remote nodes. The actual interface
//! configuration (netlink / `wg` CLI) is handled by the deployment
//! layer, not this module.

use base64::Engine;
use thiserror::Error;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::cidr::DEFAULT_WG_PORT;

#[derive(Debug, Error)]
pub enum WgError {
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
}

/// A WireGuard keypair.
#[derive(Debug, Clone)]
pub struct KeyPair {
    /// Base64-encoded private key (44 chars).
    pub private_key: String,
    /// Base64-encoded public key (44 chars).
    pub public_key: String,
}

impl KeyPair {
    /// Generate a new random keypair.
    pub fn generate() -> Self {
        let secret = StaticSecret::random_from_rng(rand::thread_rng());
        let public = PublicKey::from(&secret);
        Self {
            private_key: base64::engine::general_purpose::STANDARD.encode(secret.to_bytes()),
            public_key: base64::engine::general_purpose::STANDARD.encode(public.to_bytes()),
        }
    }

    /// Derive the public key from a private key.
    pub fn public_from_private(private_key_b64: &str) -> Result<String, WgError> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(private_key_b64)
            .map_err(|e| WgError::InvalidKey(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(WgError::InvalidKey(format!(
                "expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        let secret = StaticSecret::from(key);
        let public = PublicKey::from(&secret);
        Ok(base64::engine::general_purpose::STANDARD.encode(public.to_bytes()))
    }
}

/// A WireGuard peer configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Peer {
    /// Base64-encoded public key.
    pub public_key: String,
    /// Allowed IPs (CIDR strings, e.g. "10.100.0.5/32").
    pub allowed_ips: Vec<String>,
    /// Endpoint (host:port), optional for server-side peers.
    pub endpoint: Option<String>,
    /// Persistent keepalive in seconds (0 = disabled).
    pub keepalive: u16,
}

/// Configuration for a local WireGuard interface.
#[derive(Debug, Clone)]
pub struct InterfaceConfig {
    /// Base64-encoded private key.
    pub private_key: String,
    /// Listen port.
    pub listen_port: u16,
    /// Overlay IP address for this interface (e.g. "10.100.0.1/24").
    pub address: String,
    /// Peers to configure.
    pub peers: Vec<Peer>,
}

impl InterfaceConfig {
    /// Generate a `wg-quick` compatible config file.
    pub fn to_config_string(&self) -> String {
        let mut out = String::new();
        out.push_str("[Interface]\n");
        out.push_str(&format!("PrivateKey = {}\n", self.private_key));
        out.push_str(&format!("ListenPort = {}\n", self.listen_port));
        out.push_str(&format!("Address = {}\n", self.address));
        for peer in &self.peers {
            out.push_str("\n[Peer]\n");
            out.push_str(&format!("PublicKey = {}\n", peer.public_key));
            out.push_str(&format!("AllowedIPs = {}\n", peer.allowed_ips.join(", ")));
            if let Some(endpoint) = &peer.endpoint {
                out.push_str(&format!("Endpoint = {endpoint}\n"));
            }
            if peer.keepalive > 0 {
                out.push_str(&format!("PersistentKeepalive = {}\n", peer.keepalive));
            }
        }
        out
    }
}

/// A peer stanza for `wg set` (without the [Interface] section).
#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub public_key: String,
    pub allowed_ips: Vec<String>,
    pub endpoint: Option<String>,
    pub keepalive: u16,
}

impl PeerConfig {
    /// Generate the `wg set` arguments for adding this peer.
    pub fn to_wg_set_args(&self, interface: &str) -> Vec<String> {
        let mut args = vec!["set".to_string(), interface.to_string()];
        args.push("peer".to_string());
        args.push(self.public_key.clone());
        if let Some(endpoint) = &self.endpoint {
            args.push("endpoint".to_string());
            args.push(endpoint.clone());
        }
        if !self.allowed_ips.is_empty() {
            args.push("allowed-ips".to_string());
            args.push(self.allowed_ips.join(","));
        }
        if self.keepalive > 0 {
            args.push("persistent-keepalive".to_string());
            args.push(self.keepalive.to_string());
        }
        args
    }
}

/// Validate a base64-encoded WireGuard public key.
pub fn validate_public_key(key: &str) -> bool {
    match base64::engine::general_purpose::STANDARD.decode(key) {
        Ok(bytes) => bytes.len() == 32,
        Err(_) => false,
    }
}

/// Validate a host:port endpoint string.
pub fn validate_endpoint(endpoint: &str) -> bool {
    endpoint.parse::<std::net::SocketAddr>().is_ok() || parse_host_port(endpoint).is_some()
}

/// Parse a host:port string (supports hostnames, not just IPs).
fn parse_host_port(s: &str) -> Option<(String, u16)> {
    let (host, port) = s.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port))
}

/// Default WireGuard interface name prefix.
pub const WG_INTERFACE_PREFIX: &str = "minga";

/// Generate an interface name for a network.
pub fn interface_name(network_id: &str) -> String {
    // Sanitize: keep alphanumeric, replace others with '-'.
    let sanitized: String = network_id
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect();
    // Truncate to fit IFNAMSIZ (15 chars): "minga-" + 9 chars of network_id.
    let suffix: String = sanitized.chars().take(9).collect();
    format!("{WG_INTERFACE_PREFIX}-{suffix}")
}

/// Default listen port for a network.
pub fn default_listen_port(network_wg_port: Option<u16>) -> u16 {
    network_wg_port.unwrap_or(DEFAULT_WG_PORT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_keypair() {
        let kp = KeyPair::generate();
        assert_eq!(kp.private_key.len(), 44); // 32 bytes → 44 base64 chars
        assert_eq!(kp.public_key.len(), 44);
        assert_ne!(kp.private_key, kp.public_key);
    }

    #[test]
    fn keypairs_are_unique() {
        let kp1 = KeyPair::generate();
        let kp2 = KeyPair::generate();
        assert_ne!(kp1.private_key, kp2.private_key);
        assert_ne!(kp1.public_key, kp2.public_key);
    }

    #[test]
    fn derive_public_from_private() {
        let kp = KeyPair::generate();
        let derived = KeyPair::public_from_private(&kp.private_key).unwrap();
        assert_eq!(derived, kp.public_key);
    }

    #[test]
    fn derive_public_invalid_key() {
        assert!(KeyPair::public_from_private("not-base64!").is_err());
        assert!(KeyPair::public_from_private("dG9vLXNob3J0").is_err()); // too short
    }

    #[test]
    fn validate_public_key_valid() {
        let kp = KeyPair::generate();
        assert!(validate_public_key(&kp.public_key));
    }

    #[test]
    fn validate_public_key_invalid() {
        assert!(!validate_public_key("not-a-key"));
        assert!(!validate_public_key("dG9vLXNob3J0")); // too short
        assert!(!validate_public_key("")); // empty
    }

    #[test]
    fn validate_endpoint_valid() {
        assert!(validate_endpoint("10.0.0.1:51820"));
        assert!(validate_endpoint("192.168.1.100:51820"));
        assert!(validate_endpoint("example.com:51820"));
        assert!(validate_endpoint("[::1]:51820"));
    }

    #[test]
    fn validate_endpoint_invalid() {
        assert!(!validate_endpoint("no-port"));
        assert!(!validate_endpoint(":51820"));
        assert!(!validate_endpoint("host:not-a-port"));
        assert!(!validate_endpoint(""));
    }

    #[test]
    fn config_string_format() {
        let config = InterfaceConfig {
            private_key: "PRIVATE_KEY_B64=".to_string(),
            listen_port: 51820,
            address: "10.100.0.1/24".to_string(),
            peers: vec![
                Peer {
                    public_key: "PEER1_PUBKEY=".to_string(),
                    allowed_ips: vec!["10.100.0.2/32".to_string()],
                    endpoint: Some("10.0.0.2:51820".to_string()),
                    keepalive: 25,
                },
                Peer {
                    public_key: "PEER2_PUBKEY=".to_string(),
                    allowed_ips: vec!["10.100.0.3/32".to_string(), "10.100.1.0/24".to_string()],
                    endpoint: None,
                    keepalive: 0,
                },
            ],
        };
        let s = config.to_config_string();
        assert!(s.contains("[Interface]"));
        assert!(s.contains("PrivateKey = PRIVATE_KEY_B64="));
        assert!(s.contains("ListenPort = 51820"));
        assert!(s.contains("Address = 10.100.0.1/24"));
        assert!(s.contains("[Peer]"));
        assert!(s.contains("PublicKey = PEER1_PUBKEY="));
        assert!(s.contains("AllowedIPs = 10.100.0.2/32"));
        assert!(s.contains("Endpoint = 10.0.0.2:51820"));
        assert!(s.contains("PersistentKeepalive = 25"));
        assert!(s.contains("AllowedIPs = 10.100.0.3/32, 10.100.1.0/24"));
        // Peer without endpoint should not have Endpoint line
        let peer2_section = s.split("PEER2_PUBKEY=").nth(1).unwrap();
        assert!(!peer2_section.lines().next().unwrap().contains("Endpoint"));
    }

    #[test]
    fn peer_to_wg_set_args() {
        let peer = PeerConfig {
            public_key: "PUBKEY=".to_string(),
            allowed_ips: vec!["10.100.0.2/32".to_string()],
            endpoint: Some("10.0.0.2:51820".to_string()),
            keepalive: 25,
        };
        let args = peer.to_wg_set_args("minga-abc");
        assert_eq!(args[0], "set");
        assert_eq!(args[1], "minga-abc");
        assert_eq!(args[2], "peer");
        assert_eq!(args[3], "PUBKEY=");
        assert!(args.contains(&"endpoint".to_string()));
        assert!(args.contains(&"10.0.0.2:51820".to_string()));
        assert!(args.contains(&"allowed-ips".to_string()));
        assert!(args.contains(&"10.100.0.2/32".to_string()));
        assert!(args.contains(&"persistent-keepalive".to_string()));
        assert!(args.contains(&"25".to_string()));
    }

    #[test]
    fn interface_name_sanitized() {
        // "minga-" (6) + up to 9 chars of sanitized name = max 15 chars
        assert_eq!(interface_name("my-network"), "minga-my-networ");
        assert_eq!(interface_name("plant-7.frontend"), "minga-plant-7-f");
        assert_eq!(interface_name("a"), "minga-a");
        // Max length: "minga-" (6) + 9 = 15 chars (IFNAMSIZ-1)
        let long = interface_name("very-long-network-name-here");
        assert!(long.len() <= 15);
        assert_eq!(long, "minga-very-long");
    }

    #[test]
    fn default_listen_port_values() {
        assert_eq!(default_listen_port(None), 51820);
        assert_eq!(default_listen_port(Some(52000)), 52000);
    }
}
