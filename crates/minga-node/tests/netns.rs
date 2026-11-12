//! Published-port traffic paths on a real kernel (root + Linux only).
//!
//! Builds the topology with network namespaces and checks, for a task that
//! publishes `PUBLISHED -> TARGET`:
//!
//! - host -> `127.0.0.1:PUBLISHED` reaches the task;
//! - host -> the node's own address reaches the task;
//! - a sibling task -> the node's address reaches the task;
//! - the task -> the node's address reaches itself (hairpin);
//! - a task -> a NON-local address on the published port is NOT redirected.
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
const EXT_ADDR: &str = "203.0.113.9";
const NETWORK: &str = "netns_test_default";
const EXT_NS: &str = "minga-nstest-ext";

const SERVER: &str = r#"
import socket, sys
port, word = int(sys.argv[1]), sys.argv[2].encode()
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", port)); s.listen(8)
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
        Server(
            Command::new("ip")
                .args(["netns", "exec", ns, "python3", "-c", SERVER])
                .args([&port.to_string(), word])
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
    for task in ["nstest-web", "nstest-client"] {
        let _ = setup.teardown_task(task).await;
    }
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
