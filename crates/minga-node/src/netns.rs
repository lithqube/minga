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

use std::collections::HashSet;
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
    #[error("invalid ingress assignment: {0}")]
    IngressAssignment(String),
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
async fn output(program: &str, args: &[&str]) -> Result<String, NetError> {
    let result = tokio::process::Command::new(program)
        .args(args)
        .output()
        .await
        .map_err(|e| NetError::Command {
            cmd: format!("{program} {}", args.join(" ")),
            stderr: e.to_string(),
        })?;
    if !result.status.success() {
        return Err(NetError::Command {
            cmd: format!("{program} {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&result.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&result.stdout).into_owned())
}

async fn minga_bridges() -> Result<Vec<String>, NetError> {
    Ok(output("ip", &["-o", "link", "show", "type", "bridge"])
        .await?
        .lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .map(|name| name.trim_end_matches(':'))
        .filter(|name| name.starts_with("minga-"))
        .map(str::to_owned)
        .collect())
}

const WG_FORWARD_CHAIN: &str = "MINGA_WG_FORWARD";
const TASK_SOURCE_CHAIN: &str = "MINGA_TASK_SOURCE";
const TASK_FAIL_CLOSED_IN_CHAIN: &str = "MINGA_TASK_FAIL_CLOSED_IN";
const TASK_FAIL_CLOSED_OUT_CHAIN: &str = "MINGA_TASK_FAIL_CLOSED_OUT";
const TASK_MASQUERADE_CHAIN: &str = "MINGA_MASQUERADE";
const INGRESS_CONNMARK: &str = "0x4d494e47";

async fn remove_wireguard_rules() {
    run_tolerated(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-D",
            "FORWARD",
            "-j",
            WG_FORWARD_CHAIN,
        ],
    )
    .await;
    run_tolerated(
        "iptables",
        &["-w", "5", "-t", "filter", "-F", WG_FORWARD_CHAIN],
    )
    .await;
    run_tolerated(
        "iptables",
        &["-w", "5", "-t", "filter", "-X", WG_FORWARD_CHAIN],
    )
    .await;
    run_tolerated(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-D",
            "INPUT",
            "-i",
            minga_net::wireguard::WG_INTERFACE_NAME,
            "-j",
            "DROP",
        ],
    )
    .await;
    run_tolerated(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "mangle",
            "-D",
            "FORWARD",
            "-o",
            minga_net::wireguard::WG_INTERFACE_NAME,
            "-p",
            "tcp",
            "--tcp-flags",
            "SYN,RST",
            "SYN",
            "-j",
            "TCPMSS",
            "--clamp-mss-to-pmtu",
        ],
    )
    .await;
}
/// Keeps source validation, emergency drops and mesh filtering ahead of
/// broad bridge accepts, including when a bridge is created later.
async fn keep_wireguard_forward_first() -> Result<(), NetError> {
    move_filter_jump_first("FORWARD", WG_FORWARD_CHAIN).await?;
    // Source validation runs before the mesh accept rules.
    move_filter_jump_first("FORWARD", TASK_SOURCE_CHAIN).await?;
    move_filter_jump_first("INPUT", TASK_SOURCE_CHAIN).await?;
    // Emergency drops stay ahead of both normal chains.
    move_filter_jump_first("FORWARD", TASK_FAIL_CLOSED_IN_CHAIN).await?;
    move_filter_jump_first("FORWARD", TASK_FAIL_CLOSED_OUT_CHAIN).await?;
    move_filter_jump_first("INPUT", TASK_FAIL_CLOSED_IN_CHAIN).await
}

async fn move_filter_jump_first(hook: &str, chain: &str) -> Result<(), NetError> {
    let args = ["-w", "5", "-t", "filter", "-S", chain];
    let result = tokio::process::Command::new("iptables")
        .args(args)
        .output()
        .await
        .map_err(|error| NetError::Command {
            cmd: format!("iptables {}", args.join(" ")),
            stderr: error.to_string(),
        })?;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr).trim().to_string();
        if stderr.contains("No chain/target/match by that name") {
            return Ok(());
        }
        return Err(NetError::Command {
            cmd: format!("iptables {}", args.join(" ")),
            stderr,
        });
    }
    let hooked = tokio::process::Command::new("iptables")
        .args(["-w", "5", "-t", "filter", "-C", hook, "-j", chain])
        .output()
        .await
        .is_ok_and(|o| o.status.success());
    // Delete and re-insert in one transaction so the jump (source
    // validation among them) is never missing from the hook.
    let mut batch = String::from("*filter\n");
    if hooked {
        batch.push_str(&format!("-D {hook} -j {chain}\n"));
    }
    batch.push_str(&format!("-I {hook} 1 -j {chain}\nCOMMIT\n"));
    iptables_restore(&batch).await
}

/// Applies `iptables-restore --noflush` input as one atomic commit.
async fn iptables_restore(input: &str) -> Result<(), NetError> {
    use tokio::io::AsyncWriteExt;
    let command_error = |stderr: String| NetError::Command {
        cmd: format!("iptables-restore -w 5 --noflush <<< {input:?}"),
        stderr,
    };
    let mut child = tokio::process::Command::new("iptables-restore")
        .args(["-w", "5", "--noflush"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| command_error(error.to_string()))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| command_error("stdin was not piped".into()))?;
    stdin
        .write_all(input.as_bytes())
        .await
        .map_err(|error| command_error(error.to_string()))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .await
        .map_err(|error| command_error(error.to_string()))?;
    if !output.status.success() {
        return Err(command_error(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }
    Ok(())
}

async fn ensure_task_source_chain() -> Result<(), NetError> {
    run_tolerated("modprobe", &["br_netfilter"]).await;
    run("sysctl", &["-w", "net.bridge.bridge-nf-call-iptables=1"]).await?;
    run_tolerated(
        "iptables",
        &["-w", "5", "-t", "filter", "-N", TASK_SOURCE_CHAIN],
    )
    .await;
    for hook in ["FORWARD", "INPUT"] {
        let exists = tokio::process::Command::new("iptables")
            .args([
                "-w",
                "5",
                "-t",
                "filter",
                "-C",
                hook,
                "-j",
                TASK_SOURCE_CHAIN,
            ])
            .output()
            .await
            .map_err(|error| NetError::Command {
                cmd: format!("iptables -w 5 -t filter -C {hook} -j {TASK_SOURCE_CHAIN}"),
                stderr: error.to_string(),
            })?;
        if !exists.status.success() {
            run(
                "iptables",
                &[
                    "-w",
                    "5",
                    "-t",
                    "filter",
                    "-I",
                    hook,
                    "1",
                    "-j",
                    TASK_SOURCE_CHAIN,
                ],
            )
            .await?;
        }
    }
    keep_wireguard_forward_first().await
}

// ensure_rule inserts at the chain head; install DROP first, then the
// source-specific RETURN, so only the assigned address passes.
async fn ensure_task_source_rules(veth: &str, ip: Ipv4Addr) -> Result<(), NetError> {
    ensure_task_source_chain().await?;
    let address = format!("{ip}/32");
    ensure_rule(
        "filter",
        TASK_SOURCE_CHAIN,
        &["-m", "physdev", "--physdev-in", veth, "-j", "DROP"],
    )
    .await?;
    ensure_rule(
        "filter",
        TASK_SOURCE_CHAIN,
        &[
            "-m",
            "physdev",
            "--physdev-in",
            veth,
            "-s",
            &address,
            "-j",
            "RETURN",
        ],
    )
    .await
}

async fn remove_task_source_rules(veth: &str, ip: Ipv4Addr) {
    let address = format!("{ip}/32");
    run_tolerated(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-D",
            TASK_SOURCE_CHAIN,
            "-m",
            "physdev",
            "--physdev-in",
            veth,
            "-s",
            &address,
            "-j",
            "RETURN",
        ],
    )
    .await;
    run_tolerated(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-D",
            TASK_SOURCE_CHAIN,
            "-m",
            "physdev",
            "--physdev-in",
            veth,
            "-j",
            "DROP",
        ],
    )
    .await;
}

/// Runs a command that is allowed to fail (idempotent teardown).
async fn run_tolerated(program: &str, args: &[&str]) {
    let _ = tokio::process::Command::new(program)
        .args(args)
        .output()
        .await;
}

/// Inserts an iptables rule at the chain head unless it is already present.
async fn ensure_rule(table: &str, chain: &str, rule: &[&str]) -> Result<(), NetError> {
    ensure_rule_at(table, chain, "-I", rule).await
}

/// Appends an iptables rule unless it is already present.
async fn ensure_rule_last(table: &str, chain: &str, rule: &[&str]) -> Result<(), NetError> {
    ensure_rule_at(table, chain, "-A", rule).await
}

async fn ensure_rule_at(
    table: &str,
    chain: &str,
    position: &str,
    rule: &[&str],
) -> Result<(), NetError> {
    let exists = tokio::process::Command::new("iptables")
        .args(["-w", "5", "-t", table, "-C", chain])
        .args(rule)
        .output()
        .await
        .is_ok_and(|o| o.status.success());
    if exists {
        return Ok(());
    }
    let args = [&["-w", "5", "-t", table, position, chain], rule].concat();
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

/// The subnet stored for a network in state. The node never computes one:
/// the Raft leader assigns every network a unique subnet, and a network
/// without one yet is an error.
pub fn subnet_for(network_id: &str, state_subnet: Option<&str>) -> Result<Cidr, NetError> {
    let err = |e: String| NetError::Subnet(network_id.to_string(), e);
    let raw = state_subnet.ok_or_else(|| err("no subnet stored in state yet".to_string()))?;
    Cidr::parse(raw).map_err(|e| err(e.to_string()))
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

const TASK_NET_STATE_FORMAT_VERSION: u8 = 1;

/// Recorded per-task state, so teardown removes exactly what setup made.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TaskNetState {
    #[serde(default)]
    format_version: u8,
    ips: Vec<Ipv4Addr>,
    ports: Vec<PortMapping>,
    #[serde(default)]
    veths: Vec<String>,
}
fn task_veth_for_ip(
    task_links: &str,
    task_addresses: &str,
    host_links: &str,
    ip: Ipv4Addr,
) -> Option<String> {
    let task_name = task_addresses.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        fields.next()?;
        let name = fields.next()?.split('@').next()?.trim_end_matches(':');
        fields.next()?;
        let address = fields.next()?.split('/').next()?.parse::<Ipv4Addr>().ok()?;
        (address == ip).then_some(name)
    })?;
    let host_index = task_links.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        fields.next()?;
        let name = fields.next()?.trim_end_matches(':');
        if name.split('@').next()? != task_name {
            return None;
        }
        name.rsplit_once("@if")?.1.parse::<u32>().ok()
    })?;
    host_links.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let index = fields.next()?.trim_end_matches(':').parse::<u32>().ok()?;
        if index != host_index {
            return None;
        }
        fields.next().map(|name| {
            name.split('@')
                .next()
                .unwrap_or(name)
                .trim_end_matches(':')
                .to_owned()
        })
    })
}

fn ingress_port_chain(assignment: &minga_state::types::IngressAssignment) -> String {
    let protocol = match assignment.protocol.as_str() {
        "tcp" => "T",
        "udp" => "U",
        "sctp" => "S",
        _ => unreachable!("assignment protocol validated before chain creation"),
    };
    format!("MINGA_I_{protocol}_{}", assignment.published)
}

/// The node-level network setup. One instance per node.
pub struct NetworkSetup {
    /// Node data dir for networking state (`<data_dir>/net`).
    state_dir: PathBuf,
    /// Ports the node owns even when nothing listens on them yet.
    reserved: Vec<(String, u16, String)>,
    /// Legacy v2 task DNAT is removed once during the v3 transition.
    legacy_ingress_cleaned: std::sync::Mutex<bool>,
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
            legacy_ingress_cleaned: std::sync::Mutex::new(false),
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
    /// Reconciles the node-local routing mesh with manager-authored backend sets.
    pub async fn setup_ingress(
        &self,
        assignments: &[minga_state::types::IngressAssignment],
        node_id: minga_state::types::NodeId,
    ) -> Result<(), NetError> {
        let mut desired_chains = HashSet::new();
        for assignment in assignments {
            if assignment.published == 0
                || !matches!(assignment.protocol.as_str(), "tcp" | "udp" | "sctp")
                || assignment.backends.is_empty()
            {
                return Err(NetError::IngressAssignment(format!(
                    "{} / {} has invalid port or no backends",
                    assignment.published, assignment.protocol
                )));
            }
            self.check_port_free(&assignment.protocol, assignment.published)?;
            let chain = ingress_port_chain(assignment);
            if !desired_chains.insert(chain.clone()) {
                return Err(NetError::IngressAssignment(format!(
                    "duplicate published port {} / {}",
                    assignment.published, assignment.protocol
                )));
            }
            for backend in &assignment.backends {
                backend
                    .ip
                    .parse::<Ipv4Addr>()
                    .map_err(|error| NetError::IngressAssignment(error.to_string()))?;
                if backend.target_port == 0 {
                    return Err(NetError::IngressAssignment(
                        "backend target port is zero".into(),
                    ));
                }
                if backend.node_id != node_id && assignment.source_overlay_ip.is_none() {
                    return Err(NetError::IngressAssignment(
                        "remote backend requires this node's overlay address".into(),
                    ));
                }
            }
            if let Some(source) = &assignment.source_overlay_ip {
                let source = source
                    .parse::<Ipv4Addr>()
                    .map_err(|error| NetError::IngressAssignment(error.to_string()))?;
                let expected = minga_net::overlay::mesh_node_ip(node_id)
                    .map_err(|error| NetError::IngressAssignment(error.to_string()))?;
                if source != expected {
                    return Err(NetError::IngressAssignment(format!(
                        "source address {source} does not belong to node {node_id}"
                    )));
                }
            }
        }

        let clean_legacy = !*self.legacy_ingress_cleaned.lock().unwrap();
        if clean_legacy {
            self.remove_legacy_ingress_dnat().await?;
            *self.legacy_ingress_cleaned.lock().unwrap() = true;
        }

        run_tolerated("iptables", &["-w", "5", "-t", "nat", "-N", "MINGA_INGRESS"]).await;
        run_tolerated(
            "iptables",
            &["-w", "5", "-t", "nat", "-N", "MINGA_INGRESS_SNAT"],
        )
        .await;
        run_tolerated(
            "iptables",
            &["-w", "5", "-t", "mangle", "-N", "MINGA_INGRESS_MARK"],
        )
        .await;
        ensure_rule(
            "mangle",
            "PREROUTING",
            &[
                "-m",
                "addrtype",
                "--dst-type",
                "LOCAL",
                "-j",
                "MINGA_INGRESS_MARK",
            ],
        )
        .await?;
        ensure_rule(
            "mangle",
            "OUTPUT",
            &["-o", "lo", "-j", "MINGA_INGRESS_MARK"],
        )
        .await?;
        ensure_rule(
            "nat",
            "PREROUTING",
            &[
                "-m",
                "addrtype",
                "--dst-type",
                "LOCAL",
                "-j",
                "MINGA_INGRESS",
            ],
        )
        .await?;
        ensure_rule("nat", "OUTPUT", &["-o", "lo", "-j", "MINGA_INGRESS"]).await?;
        ensure_rule("nat", "POSTROUTING", &["-j", "MINGA_INGRESS_SNAT"]).await?;

        let existing = output("iptables", &["-w", "5", "-t", "nat", "-S", "MINGA_INGRESS"]).await?;
        let old_chains: HashSet<String> = existing
            .lines()
            .filter_map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                if fields.get(0) != Some(&"-A") || fields.get(1) != Some(&"MINGA_INGRESS") {
                    return None;
                }
                let target = fields.windows(2).find_map(|pair| {
                    (pair[0] == "-j" && pair[1].starts_with("MINGA_I_")).then_some(pair[1])
                })?;
                Some(target.to_string())
            })
            .collect();

        for chain in &desired_chains {
            run_tolerated("iptables", &["-w", "5", "-t", "nat", "-N", chain.as_str()]).await;
        }
        let mut batch = String::from("*nat\n-F MINGA_INGRESS\n-F MINGA_INGRESS_SNAT\n");
        for chain in old_chains.difference(&desired_chains) {
            batch.push_str(&format!("-F {chain}\n-X {chain}\n"));
        }
        for assignment in assignments {
            let chain = ingress_port_chain(assignment);
            batch.push_str(&format!("-F {chain}\n"));
            batch.push_str(&format!(
                "-A MINGA_INGRESS -p {} --dport {} -j {chain}\n",
                assignment.protocol, assignment.published
            ));
            for (index, backend) in assignment.backends.iter().enumerate() {
                let destination = format!("{}:{}", backend.ip, backend.target_port);
                if index + 1 < assignment.backends.len() {
                    let probability = 1.0 / (assignment.backends.len() - index) as f64;
                    batch.push_str(&format!(
                        "-A {chain} -m statistic --mode random --probability {probability:.10} \
                         -j DNAT --to-destination {destination}\n"
                    ));
                } else {
                    batch.push_str(&format!(
                        "-A {chain} -j DNAT --to-destination {destination}\n"
                    ));
                }
                if backend.node_id != node_id {
                    let source = assignment
                        .source_overlay_ip
                        .as_deref()
                        .expect("remote backend validated with overlay address");
                    batch.push_str(&format!(
                        "-A MINGA_INGRESS_SNAT -p {} -d {}/32 --dport {} -m conntrack \
                         --ctstate DNAT -j SNAT --to-source {source}\n",
                        assignment.protocol, backend.ip, backend.target_port
                    ));
                }
            }
        }
        let mut mark_batch =
            String::from("*mangle\n:MINGA_INGRESS_MARK - [0:0]\n-F MINGA_INGRESS_MARK\n");
        for assignment in assignments {
            mark_batch.push_str(&format!(
                "-A MINGA_INGRESS_MARK -p {} --dport {} -m conntrack --ctstate NEW \
                 -j CONNMARK --set-mark {INGRESS_CONNMARK}\n",
                assignment.protocol, assignment.published
            ));
        }
        mark_batch.push_str("COMMIT\n");
        iptables_restore(&mark_batch).await?;
        batch.push_str("COMMIT\n");
        iptables_restore(&batch).await
    }

    async fn remove_legacy_ingress_dnat(&self) -> Result<(), NetError> {
        let mut entries = match tokio::fs::read_dir(&self.state_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.path().extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let raw = tokio::fs::read(entry.path()).await?;
            let state: TaskNetState = serde_json::from_slice(&raw)?;
            let Some(ip) = state.ips.first() else {
                continue;
            };
            for port in state.ports {
                if port.mode != minga_state::types::PortMode::Ingress {
                    continue;
                }
                let Some(published) = port.published else {
                    continue;
                };
                let destination = format!("{ip}:{}", port.target);
                for chain in [Chain::PreroutingLocal, Chain::PreroutingAny, Chain::Output] {
                    let rule = dnat_rule("-D", chain, &port.protocol, published, &destination);
                    run_tolerated("iptables", &as_strs(&rule)).await;
                }
            }
        }
        Ok(())
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
        run_tolerated("modprobe", &["br_netfilter"]).await;
        run("sysctl", &["-w", "net.bridge.bridge-nf-call-iptables=1"]).await?;

        // Lets the host reach published ports at 127.0.0.1: the DNAT'd
        // packet still carries a loopback source, which the kernel drops on
        // other interfaces unless route_localnet is on.
        run_tolerated(
            "sysctl",
            &["-w", &format!("net.ipv4.conf.{br}.route_localnet=1")],
        )
        .await;

        // Task IPs are private to this host, so external egress needs SNAT.
        let cidr = subnet.to_string();
        // Remove legacy direct rules; task egress now passes through one chain
        // so bridge and WireGuard exclusions don't require duplicate -o flags.
        for rule in [
            vec!["-s", &cidr, "!", "-o", &br, "-j", "MASQUERADE"],
            vec![
                "-s",
                &cidr,
                "!",
                "-o",
                &br,
                "!",
                "-o",
                minga_net::wireguard::WG_INTERFACE_NAME,
                "-j",
                "MASQUERADE",
            ],
        ] {
            let mut args = vec!["-w", "5", "-t", "nat", "-D", "POSTROUTING"];
            args.extend(rule);
            run_tolerated("iptables", &args).await;
        }
        run_tolerated(
            "iptables",
            &["-w", "5", "-t", "nat", "-N", TASK_MASQUERADE_CHAIN],
        )
        .await;
        ensure_rule(
            "nat",
            TASK_MASQUERADE_CHAIN,
            &["-o", "minga-+", "-j", "RETURN"],
        )
        .await?;
        // The fallback must sit after the overlay RETURN, which ensure_rule
        // put at the head.
        ensure_rule_last("nat", TASK_MASQUERADE_CHAIN, &["-j", "MASQUERADE"]).await?;
        ensure_rule(
            "nat",
            "POSTROUTING",
            &["-s", &cidr, "-j", TASK_MASQUERADE_CHAIN],
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
        keep_wireguard_forward_first().await?;
        Ok(())
    }
    /// Configures bridge proxy ARP and source-checked forwarding for the
    /// WireGuard mesh. The dedicated chain stays ahead of the broad bridge
    /// accepts installed by `ensure_bridge`.
    pub async fn setup_wireguard_mesh(
        &self,
        networks: &[minga_net::wireguard::WireGuardNetworkAssignment],
    ) -> Result<(), NetError> {
        let wg = minga_net::wireguard::WG_INTERFACE_NAME;
        let mesh_subnet = minga_net::overlay::allocate_mesh_subnet()
            .map_err(|error| NetError::IngressAssignment(error.to_string()))?
            .to_string();
        if networks
            .iter()
            .any(|network| network.relay || network.ingress)
        {
            // Linux resets per-interface IPv4 settings when ip_forward changes.
            run("sysctl", &["-w", "net.ipv4.ip_forward=1"]).await?;
        }
        for bridge in minga_bridges().await? {
            run_tolerated(
                "sysctl",
                &["-w", &format!("net.ipv4.conf.{bridge}.proxy_arp=0")],
            )
            .await;
        }

        let mut local_bridges = Vec::new();
        let mut remote_subnets = Vec::new();
        let mut relay_subnets = Vec::new();
        let mut relay_ingress_subnets = Vec::new();
        let mut rules = vec![
            vec![
                "-o".to_string(),
                wg.to_string(),
                "-m".to_string(),
                "connmark".to_string(),
                "--mark".to_string(),
                INGRESS_CONNMARK.to_string(),
                "-j".to_string(),
                "ACCEPT".to_string(),
            ],
            vec![
                "-i".to_string(),
                wg.to_string(),
                "-m".to_string(),
                "connmark".to_string(),
                "--mark".to_string(),
                INGRESS_CONNMARK.to_string(),
                "-j".to_string(),
                "ACCEPT".to_string(),
            ],
        ];
        for network in networks {
            let subnet = subnet_for(&network.network_id, Some(&network.subnet))?;
            if network.relay {
                let subnet = subnet.to_string();
                relay_subnets.push(subnet.clone());
                if network.ingress {
                    relay_ingress_subnets.push(subnet);
                }
            }
            if !network.local {
                remote_subnets.push(subnet.to_string());
                continue;
            }
            let bridge = bridge_name(&network.network_id);
            self.ensure_bridge(&network.network_id, &subnet).await?;
            run(
                "sysctl",
                &["-w", &format!("net.ipv4.conf.{bridge}.proxy_arp=1")],
            )
            .await?;
            let cidr = subnet.to_string();
            local_bridges.push(bridge.clone());
            rules.push(vec![
                "-i".to_string(),
                bridge.clone(),
                "-o".to_string(),
                wg.to_string(),
                "-s".to_string(),
                cidr.clone(),
                "-j".to_string(),
                "ACCEPT".to_string(),
            ]);
            if network.ingress {
                // Mesh-address SNAT is allowed only from authenticated peers
                // to this network's ingress-published tasks.
                rules.push(vec![
                    "-i".to_string(),
                    wg.to_string(),
                    "-o".to_string(),
                    bridge.clone(),
                    "-s".to_string(),
                    mesh_subnet.clone(),
                    "-d".to_string(),
                    cidr.clone(),
                    "-j".to_string(),
                    "ACCEPT".to_string(),
                ]);
            }
            rules.push(vec![
                "-i".to_string(),
                wg.to_string(),
                "-o".to_string(),
                bridge,
                "-s".to_string(),
                cidr,
                "-j".to_string(),
                "ACCEPT".to_string(),
            ]);
        }
        // No route or peer exists for these subnets; drop at the task bridge
        // instead of blackholing host traffic in the node's routing table.
        for bridge in &local_bridges {
            for subnet in &remote_subnets {
                rules.push(vec![
                    "-i".to_string(),
                    bridge.clone(),
                    "-d".to_string(),
                    subnet.clone(),
                    "-j".to_string(),
                    "DROP".to_string(),
                ]);
            }
        }
        for subnet in &relay_subnets {
            rules.push(vec![
                "-i".to_string(),
                wg.to_string(),
                "-o".to_string(),
                wg.to_string(),
                "-s".to_string(),
                subnet.clone(),
                "-d".to_string(),
                subnet.clone(),
                "-j".to_string(),
                "ACCEPT".to_string(),
            ]);
        }
        for subnet in &relay_ingress_subnets {
            rules.push(vec![
                "-i".to_string(),
                wg.to_string(),
                "-o".to_string(),
                wg.to_string(),
                "-s".to_string(),
                mesh_subnet.clone(),
                "-d".to_string(),
                subnet.clone(),
                "-j".to_string(),
                "ACCEPT".to_string(),
            ]);
            rules.push(vec![
                "-i".to_string(),
                wg.to_string(),
                "-o".to_string(),
                wg.to_string(),
                "-s".to_string(),
                subnet.clone(),
                "-d".to_string(),
                mesh_subnet.clone(),
                "-j".to_string(),
                "ACCEPT".to_string(),
            ]);
        }
        rules.push(["-o", wg, "-j", "DROP"].map(str::to_owned).to_vec());
        rules.push(["-i", wg, "-j", "DROP"].map(str::to_owned).to_vec());

        // Declaring the chain in a --noflush restore creates or flushes it in
        // the same commit as the new rules: the mesh policy is never empty.
        let mut batch = format!("*filter\n:{WG_FORWARD_CHAIN} - [0:0]\n");
        for rule in rules {
            batch.push_str(&format!("-A {WG_FORWARD_CHAIN} {}\n", rule.join(" ")));
        }
        batch.push_str("COMMIT\n");
        iptables_restore(&batch).await?;
        keep_wireguard_forward_first().await?;
        ensure_rule("filter", "INPUT", &["-i", wg, "-j", "DROP"]).await?;
        ensure_rule(
            "mangle",
            "FORWARD",
            &[
                "-o",
                wg,
                "-p",
                "tcp",
                "--tcp-flags",
                "SYN,RST",
                "SYN",
                "-j",
                "TCPMSS",
                "--clamp-mss-to-pmtu",
            ],
        )
        .await?;
        Ok(())
    }

    /// Removes mesh firewall and proxy-ARP state. The WireGuard link owns
    /// its routes, which the kernel removes with that link.
    pub async fn teardown_wireguard_mesh(&self) -> Result<(), NetError> {
        for bridge in minga_bridges().await? {
            run_tolerated(
                "sysctl",
                &["-w", &format!("net.ipv4.conf.{bridge}.proxy_arp=0")],
            )
            .await;
        }
        remove_wireguard_rules().await;
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
            if port.mode == minga_state::types::PortMode::Host {
                if let Some(published) = port.published {
                    self.check_port_free(&port.protocol, published)?;
                }
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
        let mut task_veths = Vec::with_capacity(attachments.len());
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
            task_veths.push(vh.clone());

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

        // Host-mode ports retain node-local DNAT. Ingress ports are routed
        // by the supervisor's cluster-wide backend assignment.
        let primary_ip = attachments[0].ip;
        for port in ports {
            if port.mode != minga_state::types::PortMode::Host {
                continue;
            }
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

        // Record what to tear down before installing source filters so a
        // partial iptables failure can still be rolled back.
        let state = TaskNetState {
            format_version: TASK_NET_STATE_FORMAT_VERSION,
            ips: attachments.iter().map(|a| a.ip).collect(),
            ports: ports.to_vec(),
            veths: task_veths,
        };
        let raw = serde_json::to_vec(&state)?;
        tokio::fs::write(self.state_dir.join(format!("{ns}.json")), raw).await?;
        for (veth, attachment) in state.veths.iter().zip(attachments) {
            ensure_task_source_rules(veth, attachment.ip).await?;
        }
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
                for (veth, ip) in state.veths.iter().zip(&state.ips) {
                    remove_task_source_rules(veth, *ip).await;
                }
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
    /// Reinstalls source-IP guards for tasks that survived a node restart.
    pub async fn reconcile_task_source_filters(&self) -> Result<(), NetError> {
        let mut entries = match tokio::fs::read_dir(&self.state_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let namespaces = output("ip", &["netns", "list"]).await?;
                if namespaces.lines().any(|line| {
                    line.split_whitespace()
                        .next()
                        .is_some_and(|namespace| namespace.starts_with("minga-"))
                }) {
                    return Err(NetError::Command {
                        cmd: format!("read task network state {}", self.state_dir.display()),
                        stderr: "active task namespaces exist without state files".into(),
                    });
                }
                self.unblock_task_networking().await?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let namespaces = output("ip", &["netns", "list"]).await?;
        let mut untracked_namespaces: HashSet<String> = namespaces
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|namespace| namespace.starts_with("minga-"))
            .map(str::to_owned)
            .collect();
        let host_links = output("ip", &["-o", "link", "show"]).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(namespace) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            if !untracked_namespaces.remove(namespace) {
                continue;
            }
            let raw = tokio::fs::read(&path).await?;
            let mut state: TaskNetState = serde_json::from_slice(&raw)?;
            if state.format_version > TASK_NET_STATE_FORMAT_VERSION {
                return Err(NetError::Command {
                    cmd: format!("read task state {}", path.display()),
                    stderr: format!(
                        "unsupported task network state version {}",
                        state.format_version
                    ),
                });
            }
            if state.veths.len() != state.ips.len() {
                let task_links = output("ip", &["-n", namespace, "-o", "link", "show"]).await?;
                let task_addresses =
                    output("ip", &["-n", namespace, "-o", "-4", "addr", "show"]).await?;
                state.veths = state
                    .ips
                    .iter()
                    .map(|ip| task_veth_for_ip(&task_links, &task_addresses, &host_links, *ip))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| NetError::Command {
                        cmd: format!("ip -n {namespace} -o -4 addr show"),
                        stderr: "could not map a task IP to its host veth".into(),
                    })?;
            }
            if state.format_version != TASK_NET_STATE_FORMAT_VERSION {
                state.format_version = TASK_NET_STATE_FORMAT_VERSION;
            }
            tokio::fs::write(&path, serde_json::to_vec(&state)?).await?;
            for (veth, ip) in state.veths.iter().zip(&state.ips) {
                ensure_task_source_rules(veth, *ip).await?;
            }
        }
        if let Some(namespace) = untracked_namespaces.into_iter().next() {
            return Err(NetError::Command {
                cmd: format!("read task network state {}", self.state_dir.display()),
                stderr: format!("active task namespace {namespace} has no state file"),
            });
        }
        self.unblock_task_networking().await?;
        Ok(())
    }

    /// Blocks all task bridge ingress and egress while source guards are
    /// being recovered or repaired.
    pub async fn block_task_networking(&self) -> Result<(), NetError> {
        run_tolerated(
            "iptables",
            &["-w", "5", "-t", "filter", "-N", TASK_FAIL_CLOSED_IN_CHAIN],
        )
        .await;
        run(
            "iptables",
            &["-w", "5", "-t", "filter", "-F", TASK_FAIL_CLOSED_IN_CHAIN],
        )
        .await?;
        run(
            "iptables",
            &[
                "-w",
                "5",
                "-t",
                "filter",
                "-A",
                TASK_FAIL_CLOSED_IN_CHAIN,
                "-i",
                "minga-+",
                "-j",
                "DROP",
            ],
        )
        .await?;
        run_tolerated(
            "iptables",
            &["-w", "5", "-t", "filter", "-N", TASK_FAIL_CLOSED_OUT_CHAIN],
        )
        .await;
        run(
            "iptables",
            &["-w", "5", "-t", "filter", "-F", TASK_FAIL_CLOSED_OUT_CHAIN],
        )
        .await?;
        run(
            "iptables",
            &[
                "-w",
                "5",
                "-t",
                "filter",
                "-A",
                TASK_FAIL_CLOSED_OUT_CHAIN,
                "-o",
                "minga-+",
                "-j",
                "DROP",
            ],
        )
        .await?;
        keep_wireguard_forward_first().await
    }

    async fn unblock_task_networking(&self) -> Result<(), NetError> {
        // Keep the empty chains and their hooks installed. A later fail-closed
        // transition can populate them without a window before the jump exists.
        for chain in [TASK_FAIL_CLOSED_IN_CHAIN, TASK_FAIL_CLOSED_OUT_CHAIN] {
            let args = ["-w", "5", "-t", "filter", "-S", chain];
            let result = tokio::process::Command::new("iptables")
                .args(args)
                .output()
                .await
                .map_err(|error| NetError::Command {
                    cmd: format!("iptables {}", args.join(" ")),
                    stderr: error.to_string(),
                })?;
            if result.status.success() {
                run("iptables", &["-w", "5", "-t", "filter", "-F", chain]).await?;
            } else {
                let stderr = String::from_utf8_lossy(&result.stderr);
                if !stderr.contains("No chain/target/match by that name") {
                    return Err(NetError::Command {
                        cmd: format!("iptables {}", args.join(" ")),
                        stderr: stderr.trim().to_string(),
                    });
                }
            }
        }
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
    fn task_ip_maps_to_host_veth_after_restart() {
        let task_links = "2: at0@if41: <BROADCAST,UP> mtu 1500";
        let task_addresses = "2: at0    inet 10.242.0.10/24 scope global at0";
        let host_links = "41: ta0@if2: <BROADCAST,UP> mtu 1500";
        assert_eq!(
            task_veth_for_ip(
                task_links,
                task_addresses,
                host_links,
                Ipv4Addr::new(10, 242, 0, 10),
            ),
            Some("ta0".into())
        );
    }
    #[test]
    fn task_state_v0_defaults_veths_for_migration() {
        let state: TaskNetState =
            serde_json::from_str(r#"{"ips":["10.242.0.10"],"ports":[]}"#).unwrap();
        assert_eq!(state.format_version, 0);
        assert!(state.veths.is_empty());
    }

    #[test]
    fn gateway_is_first_usable_host() {
        let subnet = Cidr::parse("10.100.5.0/24").unwrap();
        assert_eq!(gateway_of(&subnet), Some(Ipv4Addr::new(10, 100, 5, 1)));
    }

    #[test]
    fn subnet_comes_only_from_state() {
        // Explicit wins.
        let s = subnet_for("net", Some("10.99.0.0/24")).unwrap();
        assert_eq!(s.to_cidr_string(), "10.99.0.0/24");
        // The node never derives one itself.
        assert!(subnet_for("shop_default", None).is_err());
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
