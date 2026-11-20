//! Published-port traffic paths on a real kernel (root + Linux only).
//!
//! Builds the topology with network namespaces and checks, for a task that
//! publishes `PUBLISHED -> TARGET`:
//!
//! - host -> `127.0.0.1:PUBLISHED` reaches the task;
//! - host -> the node's own address reaches the task;
//! - a sibling task -> the node's address reaches the task;
//! - the task -> the node's address reaches itself (hairpin);
//! - a task -> a NON-local address on the published port is NOT redirected;
//! - a task on another network cannot reach the task's IP directly, but can
//!   reach its published port through the node address;
//! - publishing a node-owned port (53, the embedded DNS) is refused;
//! - a required-egress task reaches only the proxy on its gateway, over
//!   IPv4 and IPv6, and the proxy listener is closed to non-bridge
//!   interfaces (needs nft).
//!
//! The "node address" is 198.51.100.7 added to `lo` (a LOCAL address that
//! needs no real NIC); the "external host" is a namespace behind a veth at
//! 203.0.113.9. Needs root, iproute2, iptables and python3.
//!
//! Skips when it cannot run, unless `MINGA_REQUIRE_NETNS` is set, in which
//! case it fails loudly (CI sets it).
//!
//! To run: `sudo -E env "PATH=$PATH" cargo test -p minga-node --test netns -- --nocapture`
//!
//! Cleanup removes the namespaces, the node address and the bridge; the
//! bridge-level iptables rules (named after the bridge) are left behind.
//! The egress scenario uses per-run names and removes the host-wide
//! `minga_egress_listener` table only if it created it.

#![cfg(target_os = "linux")]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use minga_net::Cidr;
use minga_node::netns::{bridge_name, NetworkSetup, TaskNetAttachment};
use minga_state::types::PortMapping;

const PUBLISHED: u16 = 18080;
const TARGET: u16 = 8080;
const NODE_ADDR: &str = "198.51.100.7";
const EXT_HOST: &str = "203.0.113.1";
const INGRESS_MESH_ADDR: &str = "10.200.0.1";
const EXT_ADDR: &str = "203.0.113.9";
const NETWORK: &str = "netns_test_default";
const NETWORK2: &str = "netns_test_other";
const EXT_NS: &str = "minga-nstest-ext";

/// Args: port, word and an optional `6` for an IPv6-only listener.
const SERVER: &str = r#"
import socket, sys
port, word = int(sys.argv[1]), sys.argv[2].encode()
v6 = sys.argv[3:] == ["6"]
s = socket.socket(socket.AF_INET6 if v6 else socket.AF_INET)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
if v6:
    s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
s.bind(("::" if v6 else "0.0.0.0", port)); s.listen(8)
while True:
    c, _ = s.accept(); c.sendall(word); c.close()
"#;

const CLIENT: &str = r#"
import socket, sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=3)
print(s.recv(64).decode())
"#;

fn sh(program: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| format!("{program}: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!(
            "{program} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// Why the test cannot run here, if it cannot.
fn unavailable() -> Option<String> {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        return Some("not running as root".into());
    }
    for (tool, flag) in [
        ("ip", "-V"),
        ("iptables", "-V"),
        ("bridge", "-V"),
        ("python3", "-V"),
    ] {
        if Command::new(tool).arg(flag).output().is_err() {
            return Some(format!("{tool} is not installed"));
        }
    }
    None
}

/// Runs a python server in a namespace; killed on drop.
struct Server(Child);

impl Server {
    fn start(ns: &str, port: u16, word: &str) -> Self {
        Self::spawn(ns, port, word, "4")
    }

    fn start_v6(ns: &str, port: u16, word: &str) -> Self {
        Self::spawn(ns, port, word, "6")
    }

    fn spawn(ns: &str, port: u16, word: &str, family: &str) -> Self {
        Server(
            Command::new("ip")
                .args(["netns", "exec", ns, "python3", "-c", SERVER])
                .args([&port.to_string(), word, family])
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("spawn server"),
        )
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Connects from `ns` (or the host when `None`); returns what the peer sent.
/// Retries briefly so servers have time to bind.
fn fetch(ns: Option<&str>, addr: &str, port: u16) -> Result<String, String> {
    let port = port.to_string();
    let mut last = String::new();
    for _ in 0..10 {
        let r = match ns {
            Some(ns) => sh(
                "ip",
                &["netns", "exec", ns, "python3", "-c", CLIENT, addr, &port],
            ),
            None => sh("python3", &["-c", CLIENT, addr, &port]),
        };
        match r {
            Ok(body) if !body.is_empty() => return Ok(body),
            Ok(_) => last = "empty reply".into(),
            Err(e) => last = e,
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    Err(last)
}

fn expect(what: &str, got: Result<String, String>, want: &str) -> Result<(), String> {
    match got {
        Ok(ref body) if body == want => {
            eprintln!("ok: {what}");
            Ok(())
        }
        other => Err(format!("{what}: wanted {want:?}, got {other:?}")),
    }
}

async fn scenario(setup: &NetworkSetup) -> Result<(), String> {
    let subnet = Cidr::parse("10.231.7.0/24").unwrap();
    let attach = |ip: &str| {
        vec![TaskNetAttachment {
            network_id: NETWORK.into(),
            ip: ip.parse().unwrap(),
            subnet,
        }]
    };
    let ports = [PortMapping {
        target: TARGET,
        published: Some(PUBLISHED),
        protocol: "tcp".into(),
        mode: minga_state::types::PortMode::Host,
    }];

    // The node's own address, and an "external" host behind a veth.
    sh(
        "ip",
        &["addr", "add", &format!("{NODE_ADDR}/32"), "dev", "lo"],
    )?;
    sh("ip", &["netns", "add", EXT_NS])?;
    sh(
        "ip",
        &[
            "link", "add", "nstexth", "type", "veth", "peer", "name", "nstextc",
        ],
    )?;
    sh("ip", &["link", "set", "nstextc", "netns", EXT_NS])?;
    sh(
        "ip",
        &["addr", "add", &format!("{EXT_HOST}/24"), "dev", "nstexth"],
    )?;
    sh("ip", &["link", "set", "nstexth", "up"])?;
    sh("ip", &["-n", EXT_NS, "link", "set", "lo", "up"])?;
    sh("ip", &["-n", EXT_NS, "link", "set", "nstextc", "up"])?;
    sh(
        "ip",
        &[
            "-n",
            EXT_NS,
            "addr",
            "add",
            &format!("{EXT_ADDR}/24"),
            "dev",
            "nstextc",
        ],
    )?;
    sh(
        "ip",
        &["-n", EXT_NS, "route", "add", "default", "via", EXT_HOST],
    )?;

    setup
        .setup_task("nstest-web", &attach("10.231.7.10"), &ports, &[])
        .await
        .map_err(|e| e.to_string())?;
    setup
        .setup_task("nstest-client", &attach("10.231.7.11"), &[], &[])
        .await
        .map_err(|e| e.to_string())?;

    let _task = Server::start("minga-nstest-web", TARGET, "task");
    // The external host listens on the same port number as the published one.
    let _ext = Server::start(EXT_NS, PUBLISHED, "external");

    expect(
        "host -> 127.0.0.1:published",
        fetch(None, "127.0.0.1", PUBLISHED),
        "task",
    )?;
    expect(
        "host -> node address:published",
        fetch(None, NODE_ADDR, PUBLISHED),
        "task",
    )?;
    expect(
        "sibling task -> node address:published",
        fetch(Some("minga-nstest-client"), NODE_ADDR, PUBLISHED),
        "task",
    )?;
    expect(
        "task -> node address:published (hairpin)",
        fetch(Some("minga-nstest-web"), NODE_ADDR, PUBLISHED),
        "task",
    )?;
    expect(
        "task -> external host on the published port is not redirected",
        fetch(Some("minga-nstest-client"), EXT_ADDR, PUBLISHED),
        "external",
    )?;

    // A task on another network: isolated from the first network's tasks,
    // but published ports stay reachable through the node address.
    setup
        .setup_task(
            "nstest-other",
            &[TaskNetAttachment {
                network_id: NETWORK2.into(),
                ip: "10.231.8.10".parse().unwrap(),
                subnet: Cidr::parse("10.231.8.0/24").unwrap(),
            }],
            &[],
            &[],
        )
        .await
        .map_err(|e| e.to_string())?;
    match sh(
        "ip",
        &[
            "netns",
            "exec",
            "minga-nstest-other",
            "python3",
            "-c",
            CLIENT,
            "10.231.7.10",
            &TARGET.to_string(),
        ],
    ) {
        Err(_) => eprintln!("ok: other network -> task IP is dropped"),
        Ok(body) => return Err(format!("other network reached the task directly: {body:?}")),
    }
    expect(
        "other network -> node address:published",
        fetch(Some("minga-nstest-other"), NODE_ADDR, PUBLISHED),
        "task",
    )?;

    // Port 53 belongs to the embedded DNS server on every gateway.
    let dns = [PortMapping {
        target: 5353,
        published: Some(53),
        protocol: "udp".into(),
        mode: minga_state::types::PortMode::Host,
    }];
    match setup
        .setup_task("nstest-dns", &attach("10.231.7.12"), &dns, &[])
        .await
    {
        Err(minga_node::netns::NetError::PortInUse { .. }) => {
            eprintln!("ok: publishing 53/udp is refused")
        }
        other => {
            return Err(format!(
                "publishing 53/udp: wanted PortInUse, got {other:?}"
            ))
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn published_ports_reach_the_task_and_only_node_addresses() {
    if let Some(why) = unavailable() {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required netns test cannot run: {why}"
        );
        eprintln!("SKIP: {why}");
        return;
    }

    let state = tempfile::tempdir().unwrap();
    let setup = NetworkSetup::new(state.path());
    let result = scenario(&setup).await;

    // Always clean up, then report.
    for task in ["nstest-web", "nstest-client", "nstest-other", "nstest-dns"] {
        let _ = setup.teardown_task(task).await;
    }
    let _ = sh("ip", &["link", "del", &bridge_name(NETWORK2)]);
    let _ = sh("ip", &["netns", "delete", EXT_NS]);
    let _ = sh("ip", &["link", "del", "nstexth"]);
    let _ = sh(
        "ip",
        &["addr", "del", &format!("{NODE_ADDR}/32"), "dev", "lo"],
    );
    let _ = sh("ip", &["link", "del", &bridge_name(NETWORK)]);

    // Teardown must leave neither rule form behind.
    let nat = sh("iptables", &["-t", "nat", "-S"]).unwrap_or_default();
    let leaked = nat
        .lines()
        .any(|l| l.contains("DNAT") && l.contains(&format!("--dport {PUBLISHED}")));
    result.expect("published-port paths");
    assert!(!leaked, "DNAT rules left after teardown:\n{nat}");
}

const INGRESS_PUBLISHED: u16 = 18081;
const INGRESS_NODE_ADDR: &str = "198.51.100.8";
const INGRESS_NETWORK: &str = "netns_test_ingress";

#[tokio::test(flavor = "multi_thread")]
async fn ingress_port_reaches_each_backend_on_another_node() {
    if let Some(why) = unavailable() {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required netns test cannot run: {why}"
        );
        eprintln!("SKIP: {why}");
        return;
    }

    let state = tempfile::tempdir().unwrap();
    let setup = NetworkSetup::new(state.path());
    let subnet = Cidr::parse("10.232.7.0/24").unwrap();
    let attach = |ip: &str| {
        vec![TaskNetAttachment {
            network_id: INGRESS_NETWORK.into(),
            ip: ip.parse().unwrap(),
            subnet,
        }]
    };
    let ingress_ports = [PortMapping {
        target: TARGET,
        published: Some(INGRESS_PUBLISHED),
        protocol: "tcp".into(),
        mode: minga_state::types::PortMode::Ingress,
    }];
    let result = async {
        sh(
            "ip",
            &[
                "addr",
                "add",
                &format!("{INGRESS_NODE_ADDR}/32"),
                "dev",
                "lo",
            ],
        )?;
        sh(
            "ip",
            &[
                "addr",
                "add",
                &format!("{INGRESS_MESH_ADDR}/32"),
                "dev",
                "lo",
            ],
        )?;
        setup
            .setup_task(
                "nsingress-web-1",
                &attach("10.232.7.10"),
                &ingress_ports,
                &[],
            )
            .await
            .map_err(|error| error.to_string())?;
        setup
            .setup_task(
                "nsingress-web-2",
                &attach("10.232.7.11"),
                &ingress_ports,
                &[],
            )
            .await
            .map_err(|error| error.to_string())?;
        let assignment = minga_state::types::IngressAssignment {
            published: INGRESS_PUBLISHED,
            protocol: "tcp".into(),
            backends: vec![
                minga_state::types::IngressBackend {
                    node_id: 2,
                    ip: "10.232.7.10".into(),
                    target_port: TARGET,
                },
                minga_state::types::IngressBackend {
                    node_id: 2,
                    ip: "10.232.7.11".into(),
                    target_port: TARGET,
                },
            ],
            source_overlay_ip: Some(INGRESS_MESH_ADDR.into()),
        };
        setup
            .setup_ingress(&[assignment], 1)
            .await
            .map_err(|error| error.to_string())?;

        let _first = Server::start("minga-nsingress-web-1", TARGET, "replica-1");
        let _second = Server::start("minga-nsingress-web-2", TARGET, "replica-2");
        let mut seen = std::collections::HashSet::new();
        for _ in 0..20 {
            seen.insert(fetch(None, INGRESS_NODE_ADDR, INGRESS_PUBLISHED)?);
        }
        if seen.len() != 2 || !seen.contains("replica-1") || !seen.contains("replica-2") {
            return Err(format!(
                "20 ingress requests did not reach both backends: {seen:?}"
            ));
        }
        Ok::<_, String>(())
    }
    .await;

    for task in ["nsingress-web-1", "nsingress-web-2"] {
        let _ = setup.teardown_task(task).await;
    }
    let _ = setup.setup_ingress(&[], 1).await;
    let _ = sh(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "mangle",
            "-D",
            "PREROUTING",
            "-m",
            "addrtype",
            "--dst-type",
            "LOCAL",
            "-j",
            "MINGA_INGRESS_MARK",
        ],
    );
    let _ = sh(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "mangle",
            "-D",
            "OUTPUT",
            "-o",
            "lo",
            "-j",
            "MINGA_INGRESS_MARK",
        ],
    );
    let _ = sh(
        "iptables",
        &["-w", "5", "-t", "mangle", "-F", "MINGA_INGRESS_MARK"],
    );
    let _ = sh(
        "iptables",
        &["-w", "5", "-t", "mangle", "-X", "MINGA_INGRESS_MARK"],
    );
    let _ = sh(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "nat",
            "-D",
            "PREROUTING",
            "-m",
            "addrtype",
            "--dst-type",
            "LOCAL",
            "-j",
            "MINGA_INGRESS",
        ],
    );
    let _ = sh(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "nat",
            "-D",
            "OUTPUT",
            "-o",
            "lo",
            "-j",
            "MINGA_INGRESS",
        ],
    );
    let _ = sh(
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "nat",
            "-D",
            "POSTROUTING",
            "-j",
            "MINGA_INGRESS_SNAT",
        ],
    );
    let _ = sh("iptables", &["-w", "5", "-t", "nat", "-F", "MINGA_INGRESS"]);
    let _ = sh(
        "iptables",
        &["-w", "5", "-t", "nat", "-F", "MINGA_INGRESS_SNAT"],
    );
    let _ = sh("iptables", &["-w", "5", "-t", "nat", "-X", "MINGA_INGRESS"]);
    let _ = sh(
        "iptables",
        &["-w", "5", "-t", "filter", "-X", "MINGA_INGRESS_FWD"],
    );
    let _ = sh(
        "iptables",
        &["-w", "5", "-t", "nat", "-X", "MINGA_INGRESS_SNAT"],
    );
    let _ = sh(
        "ip",
        &[
            "addr",
            "del",
            &format!("{INGRESS_NODE_ADDR}/32"),
            "dev",
            "lo",
        ],
    );
    let _ = sh(
        "ip",
        &[
            "addr",
            "del",
            &format!("{INGRESS_MESH_ADDR}/32"),
            "dev",
            "lo",
        ],
    );
    let _ = sh("ip", &["link", "del", &bridge_name(INGRESS_NETWORK)]);

    result.expect("ingress backend routing");
}

const EGRESS_PROXY: u16 = 18090;
const EGRESS_OTHER_HOST_PORT: u16 = 18092;
const EGRESS_EXT_HOST: &str = "203.0.114.1";
const EGRESS_EXT_ADDR: &str = "203.0.114.9";
const EGRESS_GATEWAY6: &str = "fd00:233:7::1";
const EGRESS_TASK6: &str = "fd00:233:7::10";
const EGRESS_PEER6: &str = "fd00:233:7::11";

/// Prints `blocked` only when the TCP connect fails; any other failure
/// (no namespace, no python) fails the command instead.
const CONNECT: &str = r#"
import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=3).close()
except OSError:
    print("blocked")
else:
    print("connected")
"#;

/// Per-run names, so cleanup never removes another run's fixtures, plus
/// whether this run installed the host-wide listener table.
struct EgressFixture {
    network: String,
    ext_ns: String,
    host_veth: String,
    ns_veth: String,
    task: String,
    peer: String,
    owns_listener: bool,
}

impl EgressFixture {
    fn new() -> Self {
        let run = std::process::id();
        Self {
            network: format!("netns_test_egress_{run}"),
            ext_ns: format!("minga-nsegress-ext-{run}"),
            // IFNAMSIZ: at most 5 + 7 pid digits.
            host_veth: format!("nsegh{run}"),
            ns_veth: format!("nsegc{run}"),
            task: format!("nsegress-task-{run}"),
            peer: format!("nsegress-peer-{run}"),
            owns_listener: false,
        }
    }

    fn task_ns(&self) -> String {
        format!("minga-{}", self.task)
    }

    fn peer_ns(&self) -> String {
        format!("minga-{}", self.peer)
    }

    /// Removes what this run may have created; every name is unique to the
    /// run, so a missing one is the only possible miss.
    async fn cleanup(&self, setup: &NetworkSetup) {
        for task in [&self.task, &self.peer] {
            let _ = setup.teardown_task(task).await;
        }
        if self.owns_listener {
            minga_node::netns::remove_egress_listener().await;
        }
        let _ = sh("ip", &["netns", "delete", &self.ext_ns]);
        let _ = sh("ip", &["link", "del", &self.host_veth]);
        let _ = sh("ip", &["link", "del", &bridge_name(&self.network)]);
    }
}

/// Runs a python server on the host itself; killed on drop.
fn host_server(port: u16, word: &str, family: &str) -> Server {
    Server(
        Command::new("python3")
            .args(["-c", SERVER, &port.to_string(), word, family])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn host server"),
    )
}

fn refused(what: &str, ns: &str, addr: &str, port: u16) -> Result<(), String> {
    match sh(
        "ip",
        &[
            "netns",
            "exec",
            ns,
            "python3",
            "-c",
            CONNECT,
            addr,
            &port.to_string(),
        ],
    ) {
        Ok(out) if out == "blocked" => {
            eprintln!("ok: {what} is refused");
            Ok(())
        }
        other => Err(format!("{what} was not refused: {other:?}")),
    }
}

/// The task's interface inside its namespace.
fn ns_veth(ns: &str) -> Result<String, String> {
    let links = sh("ip", &["-n", ns, "-o", "link", "show", "type", "veth"])?;
    links
        .lines()
        .next()
        .and_then(|line| line.split(": ").nth(1))
        .and_then(|name| name.split('@').next())
        .map(str::to_owned)
        .ok_or_else(|| format!("no veth in {ns}: {links:?}"))
}

async fn egress_scenario(setup: &NetworkSetup, f: &mut EgressFixture) -> Result<(), String> {
    let subnet = Cidr::parse("10.233.7.0/24").unwrap();
    let gateway = "10.233.7.1";
    let attach = |ip: &str| {
        vec![TaskNetAttachment {
            network_id: f.network.clone(),
            ip: ip.parse().unwrap(),
            subnet,
        }]
    };
    sh("ip", &["netns", "add", &f.ext_ns])?;
    sh(
        "ip",
        &[
            "link",
            "add",
            &f.host_veth,
            "type",
            "veth",
            "peer",
            "name",
            &f.ns_veth,
        ],
    )?;
    sh("ip", &["link", "set", &f.ns_veth, "netns", &f.ext_ns])?;
    sh(
        "ip",
        &[
            "addr",
            "add",
            &format!("{EGRESS_EXT_HOST}/24"),
            "dev",
            &f.host_veth,
        ],
    )?;
    sh("ip", &["link", "set", &f.host_veth, "up"])?;
    sh("ip", &["-n", &f.ext_ns, "link", "set", "lo", "up"])?;
    sh("ip", &["-n", &f.ext_ns, "link", "set", &f.ns_veth, "up"])?;
    sh(
        "ip",
        &[
            "-n",
            &f.ext_ns,
            "addr",
            "add",
            &format!("{EGRESS_EXT_ADDR}/24"),
            "dev",
            &f.ns_veth,
        ],
    )?;
    sh(
        "ip",
        &[
            "-n",
            &f.ext_ns,
            "route",
            "add",
            "default",
            "via",
            EGRESS_EXT_HOST,
        ],
    )?;

    for (task, ip) in [(&f.task, "10.233.7.10"), (&f.peer, "10.233.7.11")] {
        setup
            .setup_task(task, &attach(ip), &[], &[])
            .await
            .map_err(|e| e.to_string())?;
    }
    setup
        .require_task_egress(&f.task)
        .await
        .map_err(|e| e.to_string())?;
    // The listener table is host-wide: never replace one this run did not
    // create.
    let listener_preexists = sh("nft", &["list", "table", "inet", "minga_egress_listener"]).is_ok();
    if listener_preexists {
        eprintln!("SKIP: listener check, minga_egress_listener already exists on this host");
    } else {
        f.owns_listener = true;
        minga_node::netns::restrict_egress_listener(EGRESS_PROXY)
            .await
            .map_err(|e| e.to_string())?;
    }

    let task_ns = f.task_ns();
    let peer_ns = f.peer_ns();
    let _proxy = host_server(EGRESS_PROXY, "proxy", "4");
    let _other = host_server(EGRESS_OTHER_HOST_PORT, "host", "4");
    let _task = Server::start(&task_ns, TARGET, "task");
    let _peer = Server::start(&peer_ns, TARGET, "peer");
    let _ext = Server::start(&f.ext_ns, TARGET, "external");

    expect(
        "required-egress task -> gateway proxy",
        fetch(Some(task_ns.as_str()), gateway, EGRESS_PROXY),
        "proxy",
    )?;
    expect(
        "peer -> required-egress task (reply path)",
        fetch(Some(peer_ns.as_str()), "10.233.7.10", TARGET),
        "task",
    )?;
    // Each refusal follows a control: the same target answers an allowed
    // source, so only the policy can refuse it.
    expect(
        "peer -> external host",
        fetch(Some(peer_ns.as_str()), EGRESS_EXT_ADDR, TARGET),
        "external",
    )?;
    refused(
        "required-egress task -> external host",
        &task_ns,
        EGRESS_EXT_ADDR,
        TARGET,
    )?;
    expect("host -> peer", fetch(None, "10.233.7.11", TARGET), "peer")?;
    refused(
        "required-egress task -> same-bridge peer",
        &task_ns,
        "10.233.7.11",
        TARGET,
    )?;
    expect(
        "peer -> other gateway port",
        fetch(Some(peer_ns.as_str()), gateway, EGRESS_OTHER_HOST_PORT),
        "host",
    )?;
    refused(
        "required-egress task -> other gateway port",
        &task_ns,
        gateway,
        EGRESS_OTHER_HOST_PORT,
    )?;
    if !listener_preexists {
        expect(
            "external host -> other host port",
            fetch(
                Some(f.ext_ns.as_str()),
                EGRESS_EXT_HOST,
                EGRESS_OTHER_HOST_PORT,
            ),
            "host",
        )?;
        refused(
            "external host -> egress proxy listener",
            &f.ext_ns,
            EGRESS_EXT_HOST,
            EGRESS_PROXY,
        )?;
    }
    ipv6_scenario(f).await
}

/// IPv6 is disabled in the task netns, but a container with `CAP_NET_RAW`
/// can still put IPv6 frames on its veth. Re-enabling IPv6 in the netns
/// stands in for those frames.
async fn ipv6_scenario(f: &EgressFixture) -> Result<(), String> {
    let bridge = bridge_name(&f.network);
    if let Err(e) = sh(
        "ip",
        &[
            "-6",
            "addr",
            "add",
            &format!("{EGRESS_GATEWAY6}/64"),
            "dev",
            &bridge,
            "nodad",
        ],
    ) {
        eprintln!("SKIP: IPv6 checks, the host has no IPv6 on the bridge: {e}");
        return Ok(());
    }
    let task_ns = f.task_ns();
    let peer_ns = f.peer_ns();
    for (ns, addr) in [(&task_ns, EGRESS_TASK6), (&peer_ns, EGRESS_PEER6)] {
        let veth = ns_veth(ns)?;
        for key in ["all".to_string(), veth.clone()] {
            sh(
                "ip",
                &[
                    "netns",
                    "exec",
                    ns,
                    "sysctl",
                    "-q",
                    "-w",
                    &format!("net.ipv6.conf.{key}.disable_ipv6=0"),
                ],
            )?;
        }
        sh(
            "ip",
            &[
                "-n",
                ns,
                "-6",
                "addr",
                "add",
                &format!("{addr}/64"),
                "dev",
                &veth,
                "nodad",
            ],
        )?;
    }
    let _peer6 = Server::start_v6(&peer_ns, TARGET, "peer6");
    let _host6 = host_server(EGRESS_OTHER_HOST_PORT, "host6", "6");

    expect(
        "host -> peer over IPv6",
        fetch(None, EGRESS_PEER6, TARGET),
        "peer6",
    )?;
    refused(
        "required-egress task -> same-bridge peer over IPv6",
        &task_ns,
        EGRESS_PEER6,
        TARGET,
    )?;
    expect(
        "peer -> host over IPv6",
        fetch(
            Some(peer_ns.as_str()),
            EGRESS_GATEWAY6,
            EGRESS_OTHER_HOST_PORT,
        ),
        "host6",
    )?;
    refused(
        "required-egress task -> host over IPv6",
        &task_ns,
        EGRESS_GATEWAY6,
        EGRESS_OTHER_HOST_PORT,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn required_egress_reaches_only_the_gateway_proxy() {
    let why = unavailable().or_else(|| {
        Command::new("nft")
            .arg("-v")
            .output()
            .is_err()
            .then(|| "nft is not installed".to_string())
    });
    if let Some(why) = why {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required netns test cannot run: {why}"
        );
        eprintln!("SKIP: {why}");
        return;
    }

    let state = tempfile::tempdir().unwrap();
    let setup = NetworkSetup::new(state.path()).with_egress_proxy_port(EGRESS_PROXY);
    let mut fixture = EgressFixture::new();
    let result = egress_scenario(&setup, &mut fixture).await;
    fixture.cleanup(&setup).await;

    result.expect("required-egress paths");
}

/// Concurrent bridge setups reorder the same host `FORWARD`/`INPUT` jumps.
/// Each must succeed, not fail with `RULE_DELETE` because another setup
/// moved the jump first (issue #115).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_bridge_setups_keep_the_filter_jumps() {
    if let Some(why) = unavailable() {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required netns test cannot run: {why}"
        );
        eprintln!("SKIP: {why}");
        return;
    }

    let state = tempfile::tempdir().unwrap();
    let setup = NetworkSetup::new(state.path());
    let networks: Vec<(String, Cidr)> = (0..16)
        .map(|i| {
            let subnet = Cidr::parse(&format!("10.234.{i}.0/24")).unwrap();
            (format!("netns_test_race_{i}"), subnet)
        })
        .collect();
    let mut errors = Vec::new();
    for _ in 0..3 {
        let results = futures::future::join_all(
            networks
                .iter()
                .map(|(network, subnet)| setup.ensure_bridge(network, subnet)),
        )
        .await;
        errors.extend(results.into_iter().filter_map(Result::err));
    }
    for (network, _) in &networks {
        let _ = sh("ip", &["link", "del", &bridge_name(network)]);
    }
    assert!(errors.is_empty(), "concurrent ensure_bridge failed: {errors:?}");
}
