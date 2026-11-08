//! Container network namespaces: bridges, veth pairs, port publishing.
//!
//! This is the kernel side of Minga networking (Linux only). The
//! control-plane pieces (IPAM, DNS records) live in `minga-net`; this
//! module applies them:
//!
//! - one bridge per overlay network (`minga-<hash>`), gateway = first
//!   usable IP of the network's subnet;
//! - one named netns per task (`minga-<task-id>`), with a veth pair per
//!   joined network and the task's IPAM-assigned address;
//! - published ports via iptables DNAT (host `PREROUTING` + `OUTPUT`);
//! - a per-task `/etc/resolv.conf` pointing at the bridge gateway, where
//!   the node's DNS server listens.
//!
//! Everything is driven through `ip`/`iptables` commands: boring,
//! inspectable with the same tools an operator would use, and loud on
//! failure. Setup writes a per-task state file so teardown can remove
//! exactly what was created, even after a node restart.
//!
//! Requires root (CAP_NET_ADMIN). iproute2 and iptables must be installed.

use std::net::Ipv4Addr;
use std::path::PathBuf;

use minga_net::Cidr;
use minga_state::types::{NetworkId, PortMapping};

/// Errors from network setup. Always name the failing command.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("{cmd} failed: {stderr}")]
    Command { cmd: String, stderr: String },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("state serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid subnet for network {0}: {1}")]
    Subnet(NetworkId, String),
    #[error("task IP {0} is not in subnet {1}")]
    IpOutsideSubnet(String, String),
}

/// Runs a command; on non-zero exit, fails with its stderr.
async fn run(program: &str, args: &[&str]) -> Result<(), NetError> {
    let output = tokio::process::Command::new(program)
        .args(args)
        .output()
        .await
        .map_err(|e| NetError::Command {
            cmd: format!("{program} {}", args.join(" ")),
            stderr: e.to_string(),
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(NetError::Command {
        cmd: format!("{program} {}", args.join(" ")),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

/// Runs a command that is allowed to fail (idempotent teardown).
async fn run_tolerated(program: &str, args: &[&str]) {
    let _ = tokio::process::Command::new(program)
        .args(args)
        .output()
        .await;
}

/// Inserts an iptables rule unless it is already present.
async fn ensure_rule(table: &str, chain: &str, rule: &[&str]) -> Result<(), NetError> {
    let exists = tokio::process::Command::new("iptables")
        .args(["-t", table, "-C", chain])
        .args(rule)
        .output()
        .await
        .is_ok_and(|o| o.status.success());
    if exists {
        return Ok(());
    }
    let args = [&["-t", table, "-I", chain], rule].concat();
    run("iptables", &args).await
}

/// A short deterministic hash for interface names (IFNAMSIZ is 16 bytes).
fn short_hash(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    format!("{:08x}", (h.finish() & 0xFFFF_FFFF) as u32)
}

/// Bridge name for a network: `minga-<8 hex>`.
fn bridge_name(network_id: &str) -> String {
    format!("minga-{}", short_hash(network_id))
}

/// veth names for a (task, network) pair: `v<6hex>h` / `v<6hex>c`.
fn veth_names(task_id: &str, network_id: &str) -> (String, String) {
    let h = &short_hash(&format!("{task_id}/{network_id}"))[..6];
    (format!("v{h}h"), format!("v{h}c"))
}

/// The subnet of a network: explicit from state, or a deterministic /24
/// derived from the network ID — the same rule the scheduler's IPAM uses
/// (`minga_scheduler::netassign`), so node and scheduler agree.
pub fn subnet_for(network_id: &str, state_subnet: Option<&str>) -> Result<Cidr, NetError> {
    match state_subnet {
        Some(raw) => {
            Cidr::parse(raw).map_err(|e| NetError::Subnet(network_id.to_string(), e.to_string()))
        }
        None => minga_net::ipam::allocate_subnet(network_id)
            .map_err(|e| NetError::Subnet(network_id.to_string(), e.to_string())),
    }
}

/// The gateway IP of a subnet: its first usable address. The bridge and
/// the DNS server both listen here.
pub fn gateway_of(subnet: &Cidr) -> Option<Ipv4Addr> {
    subnet.nth_host(0)
}

/// What a task needs on one network.
#[derive(Debug, Clone)]
pub struct TaskNetAttachment {
    pub network_id: NetworkId,
    pub ip: Ipv4Addr,
    pub subnet: Cidr,
}

/// Paths the OCI spec needs for a networked task.
#[derive(Debug, Clone)]
pub struct TaskNetns {
    /// Path to the persistent netns (for the OCI `network` namespace).
    pub netns_path: PathBuf,
    /// Per-task resolv.conf to bind-mount read-only.
    pub resolv_conf: PathBuf,
}

/// Recorded per-task state, so teardown removes exactly what setup made.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TaskNetState {
    ips: Vec<Ipv4Addr>,
    ports: Vec<PortMapping>,
}

/// The node-level network setup. One instance per node.
pub struct NetworkSetup {
    /// Node data dir for networking state (`<data_dir>/net`).
    state_dir: PathBuf,
}

/// The netns handle path. `/var/run/netns` is the one location every tool
/// (`ip`, runc, `nsenter`) agrees on; `IPNETNS_DIR` indirection proved
/// unreliable (the nsfs mount and the OCI spec path diverged).
fn netns_path(ns: &str) -> PathBuf {
    PathBuf::from(format!("/var/run/netns/{ns}"))
}

impl NetworkSetup {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
        }
    }

    /// The bridge gateway address for a network (also the DNS listen IP).
    pub fn gateway_for(&self, network_id: &str, state_subnet: Option<&str>) -> Option<Ipv4Addr> {
        subnet_for(network_id, state_subnet)
            .ok()
            .and_then(|s| gateway_of(&s))
    }

    /// Ensures the bridge for a network exists with its gateway address.
    pub async fn ensure_bridge(&self, network_id: &str, subnet: &Cidr) -> Result<(), NetError> {
        let br = bridge_name(network_id);
        let gw = gateway_of(subnet)
            .ok_or_else(|| NetError::Subnet(network_id.to_string(), "no usable hosts".into()))?;
        let prefix = subnet.prefix_len();

        // Idempotent: existing bridge is fine.
        run_tolerated("ip", &["link", "add", &br, "type", "bridge"]).await;
        run(
            "ip",
            &["addr", "replace", &format!("{gw}/{prefix}"), "dev", &br],
        )
        .await?;
        run("ip", &["link", "set", &br, "up"]).await?;

        // Forwarding between bridges and the outside world. Set once;
        // failures surface on the first container start.
        run_tolerated("sysctl", &["-w", "net.ipv4.ip_forward=1"]).await;

        // Task IPs are private to this host: without SNAT, replies from
        // anything off this bridge have no route back.
        let cidr = subnet.to_string();
        ensure_rule(
            "nat",
            "POSTROUTING",
            &["-s", &cidr, "!", "-o", &br, "-j", "MASQUERADE"],
        )
        .await?;
        // Hosts that default FORWARD to DROP (or load br_netfilter) still
        // pass task traffic and published ports.
        ensure_rule("filter", "FORWARD", &["-i", &br, "-j", "ACCEPT"]).await?;
        ensure_rule(
            "filter",
            "FORWARD",
            &[
                "-o",
                &br,
                "-m",
                "conntrack",
                "--ctstate",
                "RELATED,ESTABLISHED,DNAT",
                "-j",
                "ACCEPT",
            ],
        )
        .await?;
        Ok(())
    }

    /// Creates the task's netns, attaches it to every joined network,
    /// publishes its ports and writes its resolv.conf.
    ///
    /// The first attachment provides the default route and the resolver.
    pub async fn setup_task(
        &self,
        task_id: &str,
        attachments: &[TaskNetAttachment],
        ports: &[PortMapping],
        search_domains: &[String],
    ) -> Result<TaskNetns, NetError> {
        if attachments.is_empty() {
            return Err(NetError::Subnet(
                task_id.to_string(),
                "task has no network attachments".into(),
            ));
        }

        tokio::fs::create_dir_all(&self.state_dir).await?;
        let ns = format!("minga-{task_id}");

        // Start from a clean slate: a previous attempt must not leak.
        self.teardown_task(task_id).await?;

        run("ip", &["netns", "add", &ns]).await?;

        let result = self
            .setup_task_inner(task_id, &ns, attachments, ports, search_domains)
            .await;
        if let Err(e) = &result {
            tracing::error!(task = %task_id, error = %e, "network setup failed; tearing down");
            self.teardown_task(task_id).await?;
        }
        result
    }

    async fn setup_task_inner(
        &self,
        task_id: &str,
        ns: &str,
        attachments: &[TaskNetAttachment],
        ports: &[PortMapping],
        search_domains: &[String],
    ) -> Result<TaskNetns, NetError> {
        let ns_path = netns_path(ns);

        // Loopback up inside the netns.
        run("ip", &["-n", ns, "link", "set", "lo", "up"]).await?;

        let mut first = true;
        for att in attachments {
            if !att.subnet.contains(att.ip) {
                return Err(NetError::IpOutsideSubnet(
                    att.ip.to_string(),
                    att.subnet.to_cidr_string(),
                ));
            }
            self.ensure_bridge(&att.network_id, &att.subnet).await?;

            let br = bridge_name(&att.network_id);
            let gw = gateway_of(&att.subnet).expect("bridge ensured => gateway exists");
            let prefix = att.subnet.prefix_len();
            let (vh, vc) = veth_names(task_id, &att.network_id);

            // A stale pair from a crashed run must not fail setup.
            run_tolerated("ip", &["link", "del", &vh]).await;
            run(
                "ip",
                &["link", "add", &vh, "type", "veth", "peer", "name", &vc],
            )
            .await?;
            run("ip", ["link", "set", &vh, "master", &br, "up"].as_slice()).await?;
            run("ip", ["link", "set", &vc, "netns", ns].as_slice()).await?;
            run("ip", ["-n", ns, "link", "set", &vc, "up"].as_slice()).await?;
            run(
                "ip",
                [
                    "-n",
                    ns,
                    "addr",
                    "add",
                    &format!("{}/{prefix}", att.ip),
                    "dev",
                    &vc,
                ]
                .as_slice(),
            )
            .await?;
            if first {
                run(
                    "ip",
                    [
                        "-n",
                        ns,
                        "route",
                        "replace",
                        "default",
                        "via",
                        &gw.to_string(),
                    ]
                    .as_slice(),
                )
                .await?;
                first = false;
            }
        }

        // Published ports: DNAT host -> primary task IP (first attachment).
        let primary_ip = attachments[0].ip;
        for port in ports {
            let Some(published) = port.published else {
                continue;
            };
            let proto = port.protocol.as_str();
            let to = format!("{}:{}", primary_ip, port.target);
            let dport = published.to_string();
            run(
                "iptables",
                &[
                    "-t",
                    "nat",
                    "-A",
                    "PREROUTING",
                    "-p",
                    proto,
                    "--dport",
                    &dport,
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &to,
                ],
            )
            .await?;
            // Host-local clients (curl from the node itself).
            run(
                "iptables",
                &[
                    "-t",
                    "nat",
                    "-A",
                    "OUTPUT",
                    "-o",
                    "lo",
                    "-p",
                    proto,
                    "--dport",
                    &dport,
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &to,
                ],
            )
            .await?;
        }

        // resolv.conf: the gateway hosts the node's DNS server.
        let gw = gateway_of(&attachments[0].subnet).expect("attachment has gateway");
        let mut resolv = format!("nameserver {gw}\noptions ndots:0\n");
        if !search_domains.is_empty() {
            resolv.push_str(&format!("search {}\n", search_domains.join(" ")));
        }
        let resolv_conf = self.state_dir.join(format!("{ns}.resolv.conf"));
        tokio::fs::write(&resolv_conf, resolv).await?;

        // Record what to tear down.
        let state = TaskNetState {
            ips: attachments.iter().map(|a| a.ip).collect(),
            ports: ports.to_vec(),
        };
        let raw = serde_json::to_vec(&state)?;
        tokio::fs::write(self.state_dir.join(format!("{ns}.json")), raw).await?;

        Ok(TaskNetns {
            netns_path: ns_path,
            resolv_conf,
        })
    }

    /// Removes the task's netns, DNAT rules and state files. Idempotent.
    pub async fn teardown_task(&self, task_id: &str) -> Result<(), NetError> {
        let ns = format!("minga-{task_id}");
        let state_path = self.state_dir.join(format!("{ns}.json"));

        if let Ok(raw) = tokio::fs::read(&state_path).await {
            if let Ok(state) = serde_json::from_slice::<TaskNetState>(&raw) {
                if let (Some(ip), true) = (state.ips.first(), !state.ports.is_empty()) {
                    for port in &state.ports {
                        let Some(published) = port.published else {
                            continue;
                        };
                        let proto = port.protocol.as_str();
                        let to = format!("{}:{}", ip, port.target);
                        let dport = published.to_string();
                        run_tolerated(
                            "iptables",
                            &[
                                "-t",
                                "nat",
                                "-D",
                                "PREROUTING",
                                "-p",
                                proto,
                                "--dport",
                                &dport,
                                "-j",
                                "DNAT",
                                "--to-destination",
                                &to,
                            ],
                        )
                        .await;
                        run_tolerated(
                            "iptables",
                            &[
                                "-t",
                                "nat",
                                "-D",
                                "OUTPUT",
                                "-o",
                                "lo",
                                "-p",
                                proto,
                                "--dport",
                                &dport,
                                "-j",
                                "DNAT",
                                "--to-destination",
                                &to,
                            ],
                        )
                        .await;
                    }
                }
            }
        }

        run_tolerated("ip", &["netns", "delete", &ns]).await;

        let _ = tokio::fs::remove_file(self.state_dir.join(format!("{ns}.resolv.conf"))).await;
        let _ = tokio::fs::remove_file(&state_path).await;
        Ok(())
    }
}

/// Parses the first nameserver from /etc/resolv.conf, for upstream
/// forwarding of non-cluster queries. `None` when absent/unreadable.
pub fn system_upstream_resolver() -> Option<std::net::SocketAddr> {
    let body = std::fs::read_to_string("/etc/resolv.conf").ok()?;
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("nameserver") {
            let addr = rest.trim();
            if let Ok(ip) = addr.parse::<std::net::IpAddr>() {
                return Some(std::net::SocketAddr::new(ip, 53));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_names_fit_ifnamsiz() {
        let br = bridge_name("shop_default");
        assert!(br.len() <= 15, "bridge name {br} too long");
        assert_eq!(br, bridge_name("shop_default"));
        let (vh, vc) = veth_names("shop.web.1", "shop_default");
        assert!(vh.len() <= 15 && vc.len() <= 15);
        assert_eq!(vh, veth_names("shop.web.1", "shop_default").0);
        assert_ne!(vh, veth_names("shop.web.2", "shop_default").0);
    }

    #[test]
    fn gateway_is_first_usable_host() {
        let subnet = Cidr::parse("10.100.5.0/24").unwrap();
        assert_eq!(gateway_of(&subnet), Some(Ipv4Addr::new(10, 100, 5, 1)));
    }

    #[test]
    fn subnet_rule_matches_scheduler() {
        // Explicit wins.
        let s = subnet_for("net", Some("10.99.0.0/24")).unwrap();
        assert_eq!(s.to_cidr_string(), "10.99.0.0/24");
        // Auto is deterministic and inside the default supernet.
        let a = subnet_for("shop_default", None).unwrap();
        let b = subnet_for("shop_default", None).unwrap();
        assert_eq!(a, b);
        assert!(a.to_cidr_string().starts_with("10.100."));
    }
}
