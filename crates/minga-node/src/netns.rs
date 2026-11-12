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
//! - published ports via iptables DNAT (host `PREROUTING` for traffic addressed to
//!   the node, `OUTPUT` for clients on the node itself);
//! - a per-task `/etc/resolv.conf` pointing at the bridge gateway, where
//!   the node's DNS server listens;
//! - isolation: tasks on different networks cannot reach each other except
//!   through published ports, a task cannot reach the host's 127.0.0.0/8,
//!   and a task cannot publish a port the node itself uses.
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
    /// The node itself uses this port: a DNAT rule would capture the node's
    /// own clients (NATS, API, DNS) and hand their credentials to the task.
    #[error("published port {port}/{proto} is in use on this node ({by})")]
    PortInUse {
        port: u16,
        proto: String,
        by: String,
    },
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
        .args(["-w", "5", "-t", table, "-C", chain])
        .args(rule)
        .output()
        .await
        .is_ok_and(|o| o.status.success());
    if exists {
        return Ok(());
    }
    let args = [&["-w", "5", "-t", table, "-I", chain], rule].concat();
    run("iptables", &args).await
}

/// Which nat rule form to build for a published port.
#[derive(Clone, Copy)]
enum Chain {
    /// `PREROUTING`, only for traffic addressed to the node.
    PreroutingLocal,
    /// The 1.0.2 `PREROUTING` rule with no destination match (it also
    /// captured tasks' outbound traffic). Kept so teardown removes it.
    PreroutingAny,
    /// `OUTPUT` over loopback, for clients on the node itself.
    Output,
}

/// iptables arguments for one DNAT rule. `op` is `-A` or `-D`.
fn dnat_rule(op: &str, chain: Chain, proto: &str, published: u16, to: &str) -> Vec<String> {
    let (name, matches): (&str, &[&str]) = match chain {
        Chain::PreroutingLocal => ("PREROUTING", &["-m", "addrtype", "--dst-type", "LOCAL"]),
        Chain::PreroutingAny => ("PREROUTING", &[]),
        Chain::Output => ("OUTPUT", &["-o", "lo"]),
    };
    ["-w", "5", "-t", "nat", op, name]
        .into_iter()
        .chain(matches.iter().copied())
        .chain(["-p", proto, "--dport", &published.to_string()])
        .chain(["-j", "DNAT", "--to-destination", to])
        .map(String::from)
        .collect()
}

fn as_strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

/// `INPUT` rule: a task may not open a connection to 127.0.0.0/8 through its
/// bridge. `route_localnet` (needed for host -> 127.0.0.1:published) would
/// otherwise let a task with CAP_NET_RAW reach loopback-only host services
/// (CVE-2020-8558). Replies to host-originated connections stay allowed.
fn loopback_guard_rule(br: &str) -> Vec<String> {
    [
        "-i",
        br,
        "-d",
        "127.0.0.0/8",
        "-m",
        "conntrack",
        "!",
        "--ctstate",
        "RELATED,ESTABLISHED",
        "-j",
        "DROP",
    ]
    .map(String::from)
    .to_vec()
}

/// `FORWARD` rules isolating one bridge from every other minga bridge, in
/// insert order (`ensure_rule` inserts at the top, so the last one ends up
/// first): drop new traffic to another network unless it was DNAT'd to a
/// published port, then let same-bridge traffic (br_netfilter) through.
/// iptables allows one `-o` per rule, hence the separate same-bridge accept.
fn isolation_rules(br: &str) -> [Vec<String>; 2] {
    let drop = [
        "-i",
        br,
        "-o",
        "minga-+",
        "-m",
        "conntrack",
        "!",
        "--ctstate",
        "DNAT",
        "-j",
        "DROP",
    ];
    let same = ["-i", br, "-o", br, "-j", "ACCEPT"];
    [
        drop.map(String::from).to_vec(),
        same.map(String::from).to_vec(),
    ]
}

/// Ports with a listening TCP socket or a bound UDP socket in a
/// `/proc/net/{tcp,tcp6,udp,udp6}` table (the host's own network namespace).
fn bound_ports(table: &str, tcp: bool) -> std::collections::BTreeSet<u16> {
    table
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let local = fields.get(1)?;
            let state = fields.get(3)?;
            // 0A is TCP_LISTEN; any UDP socket in the table is bound.
            if tcp && *state != "0A" {
                return None;
            }
            u16::from_str_radix(local.rsplit_once(':')?.1, 16).ok()
        })
        .collect()
}

/// A short deterministic hash for interface names (IFNAMSIZ is 16 bytes).
fn short_hash(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    format!("{:08x}", (h.finish() & 0xFFFF_FFFF) as u32)
}

/// Bridge name for a network: `minga-<8 hex>`.
pub fn bridge_name(network_id: &str) -> String {
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
    /// Ports the node owns even when nothing listens on them yet (a sidecar
    /// that restarts after a task's rule exists): (protocol, port, owner).
    reserved: Vec<(String, u16, String)>,
}

/// The netns handle path. `/var/run/netns` is the one location every tool
/// (`ip`, runc, `nsenter`) agrees on; `IPNETNS_DIR` indirection proved
/// unreliable (the nsfs mount and the OCI spec path diverged).
fn netns_path(ns: &str) -> PathBuf {
    PathBuf::from(format!("/var/run/netns/{ns}"))
}

impl NetworkSetup {
    /// The embedded DNS server listens on every bridge gateway on port 53,
    /// so 53 is always reserved.
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            reserved: ["tcp", "udp"]
                .map(|p| (p.to_string(), 53, "embedded DNS".to_string()))
                .to_vec(),
        }
    }

    /// Adds node-owned TCP ports (Raft, NATS sidecar, metrics, egress).
    pub fn with_reserved_tcp_ports(
        mut self,
        ports: impl IntoIterator<Item = (u16, String)>,
    ) -> Self {
        self.reserved.extend(
            ports
                .into_iter()
                .map(|(port, by)| ("tcp".to_string(), port, by)),
        );
        self
    }

    /// Refuses a published port the node uses: a reserved one, or one a host
    /// process listens on (Docker fails the same way, "port is already allocated").
    fn check_port_free(&self, proto: &str, port: u16) -> Result<(), NetError> {
        let in_use = |by: &str| NetError::PortInUse {
            port,
            proto: proto.to_string(),
            by: by.to_string(),
        };
        if let Some((_, _, by)) = self
            .reserved
            .iter()
            .find(|(p, n, _)| p.eq_ignore_ascii_case(proto) && *n == port)
        {
            return Err(in_use(by.as_str()));
        }
        let tcp = !proto.eq_ignore_ascii_case("udp");
        let tables: &[&str] = if tcp {
            &["/proc/net/tcp", "/proc/net/tcp6"]
        } else {
            &["/proc/net/udp", "/proc/net/udp6"]
        };
        for table in tables {
            // Missing on non-Linux test hosts; a node always has them.
            let Ok(text) = std::fs::read_to_string(table) else {
                continue;
            };
            if bound_ports(&text, tcp).contains(&port) {
                return Err(in_use("a host process listens on it"));
            }
        }
        Ok(())
    }

    fn resolv_conf_path(&self, ns: &str) -> PathBuf {
        self.state_dir.join(format!("{ns}.resolv.conf"))
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

        // Lets the host reach published ports at 127.0.0.1: the DNAT'd
        // packet still carries a loopback source, which the kernel drops on
        // other interfaces unless route_localnet is on.
        run_tolerated(
            "sysctl",
            &["-w", &format!("net.ipv4.conf.{br}.route_localnet=1")],
        )
        .await;

        // Task IPs are private to this host: without SNAT, replies from
        // anything off this bridge have no route back.
        let cidr = subnet.to_string();
        ensure_rule(
            "nat",
            "POSTROUTING",
            &["-s", &cidr, "!", "-o", &br, "-j", "MASQUERADE"],
        )
        .await?;
        // Host-originated traffic into the bridge (127.0.0.1 or the node's
        // own address, after DNAT) needs a source the task can answer.
        ensure_rule(
            "nat",
            "POSTROUTING",
            &[
                "-o",
                &br,
                "-m",
                "addrtype",
                "--src-type",
                "LOCAL",
                "-j",
                "MASQUERADE",
            ],
        )
        .await?;
        // A task calling the node's address on a published port (itself or a
        // sibling): masquerade so the reply returns through the host.
        ensure_rule(
            "nat",
            "POSTROUTING",
            &[
                "-s",
                &cidr,
                "-o",
                &br,
                "-m",
                "conntrack",
                "--ctstate",
                "DNAT",
                "-j",
                "MASQUERADE",
            ],
        )
        .await?;
        ensure_rule("filter", "INPUT", &as_strs(&loopback_guard_rule(&br))).await?;
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
        // Networks are isolated from each other, as Docker's bridges are;
        // inserted last so they sit above the accepts.
        for rule in isolation_rules(&br) {
            ensure_rule("filter", "FORWARD", &as_strs(&rule)).await?;
        }
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

        for port in ports {
            if let Some(published) = port.published {
                self.check_port_free(&port.protocol, published)?;
            }
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
            // Lets a task reach itself through a published port on the
            // node's address (the DNAT'd packet leaves by the port it came in).
            run(
                "bridge",
                ["link", "set", "dev", &vh, "hairpin", "on"].as_slice(),
            )
            .await?;
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
            let to = format!("{}:{}", primary_ip, port.target);
            for rule in [
                dnat_rule("-A", Chain::PreroutingLocal, &port.protocol, published, &to),
                // Host-local clients (curl from the node itself).
                dnat_rule("-A", Chain::Output, &port.protocol, published, &to),
            ] {
                run("iptables", &as_strs(&rule)).await?;
            }
        }

        // resolv.conf: the gateway hosts the node's DNS server.
        let gw = gateway_of(&attachments[0].subnet).expect("attachment has gateway");
        let mut resolv = format!("nameserver {gw}\noptions ndots:0\n");
        if !search_domains.is_empty() {
            resolv.push_str(&format!("search {}\n", search_domains.join(" ")));
        }
        let resolv_conf = self.resolv_conf_path(ns);
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
                if let Some(ip) = state.ips.first() {
                    for port in &state.ports {
                        let Some(published) = port.published else {
                            continue;
                        };
                        let to = format!("{}:{}", ip, port.target);
                        // The current forms, plus the 1.0.2 PREROUTING form
                        // that rules written by an older binary may still use.
                        for chain in [Chain::PreroutingLocal, Chain::PreroutingAny, Chain::Output] {
                            let rule = dnat_rule("-D", chain, &port.protocol, published, &to);
                            run_tolerated("iptables", &as_strs(&rule)).await;
                        }
                    }
                }
            }
        }

        run_tolerated("ip", &["netns", "delete", &ns]).await;

        let resolv_conf = self.resolv_conf_path(&ns);
        // `runtime.rs` writes the task's hosts file beside the resolv.conf.
        let _ = tokio::fs::remove_file(resolv_conf.with_extension("hosts")).await;
        let _ = tokio::fs::remove_file(resolv_conf).await;
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

    #[test]
    fn prerouting_dnat_only_matches_node_addresses() {
        let to = "10.100.5.2:8080";
        let new = dnat_rule("-A", Chain::PreroutingLocal, "tcp", 443, to).join(" ");
        assert_eq!(
            new,
            "-w 5 -t nat -A PREROUTING -m addrtype --dst-type LOCAL -p tcp --dport 443 \
             -j DNAT --to-destination 10.100.5.2:8080"
        );
        // The 1.0.2 form (no destination match) is what teardown must still delete.
        let old = dnat_rule("-D", Chain::PreroutingAny, "tcp", 443, to).join(" ");
        assert_eq!(
            old,
            "-w 5 -t nat -D PREROUTING -p tcp --dport 443 -j DNAT --to-destination 10.100.5.2:8080"
        );
    }

    #[test]
    fn output_dnat_is_loopback_only() {
        let rule = dnat_rule("-A", Chain::Output, "udp", 53, "10.100.5.2:5353").join(" ");
        assert_eq!(
            rule,
            "-w 5 -t nat -A OUTPUT -o lo -p udp --dport 53 -j DNAT --to-destination 10.100.5.2:5353"
        );
    }

    #[test]
    fn bridge_guard_rules() {
        assert_eq!(
            loopback_guard_rule("minga-abc").join(" "),
            "-i minga-abc -d 127.0.0.0/8 -m conntrack ! --ctstate RELATED,ESTABLISHED -j DROP"
        );
        let [drop, same] = isolation_rules("minga-abc");
        assert_eq!(
            drop.join(" "),
            "-i minga-abc -o minga-+ -m conntrack ! --ctstate DNAT -j DROP"
        );
        assert_eq!(same.join(" "), "-i minga-abc -o minga-abc -j ACCEPT");
    }

    #[test]
    fn bound_ports_reads_listening_tcp_and_bound_udp() {
        let tcp = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:107E 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1 1 0 100 0 0 10 0
   1: 0100007F:94F2 0100007F:107E 01 00000000:00000000 00:00000000 00000000     0        0 2 1 0 20 4 30 10 -1
";
        // 0x107E = 4222 listens; 0x94F2 is the client side of an established connection.
        assert_eq!(
            bound_ports(tcp, true).into_iter().collect::<Vec<_>>(),
            [4222]
        );
        let udp = "   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  10: 0107640A:0035 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 3 2 0 0
";
        assert_eq!(
            bound_ports(udp, false).into_iter().collect::<Vec<_>>(),
            [53]
        );
    }

    #[test]
    fn node_ports_are_refused_for_publishing() {
        let setup = NetworkSetup::new("/nonexistent").with_reserved_tcp_ports([
            (7001, "Raft".to_string()),
            (4222, "NATS sidecar".to_string()),
        ]);
        let err = setup.check_port_free("tcp", 4222).unwrap_err().to_string();
        assert!(
            err.contains("4222/tcp") && err.contains("NATS sidecar"),
            "{err}"
        );
        assert!(setup.check_port_free("udp", 53).is_err(), "embedded DNS");
        assert!(setup.check_port_free("TCP", 7001).is_err());
    }
}
