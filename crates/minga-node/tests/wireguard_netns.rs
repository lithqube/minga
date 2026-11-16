//! Cross-node WireGuard forwarding on a real kernel (root + Linux only).
//!
//! Three node namespaces share an underlay. Two tasks share one overlay while
//! a third is isolated on another. CI sets MINGA_REQUIRE_NETNS, so missing
//! kernel/tooling is a failure rather than a silent skip.

#![cfg(target_os = "linux")]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use minga_net::wireguard::WG_INTERFACE_NAME;
use minga_net::KeyPair;
use minga_node::netns::bridge_name;
use tempfile::TempDir;
static NETNS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DNS_SERVER: &str = r#"
import socket, struct
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("10.242.0.1", 53))
while True:
    q, peer = s.recvfrom(512)
    pos, labels = 12, []
    while q[pos]:
        size = q[pos]
        pos += 1
        labels.append(q[pos:pos+size].decode())
        pos += size
    pos += 1
    qtype = struct.unpack("!H", q[pos:pos+2])[0]
    end = pos + 4
    question = q[12:end]
    answer = b""
    count = 0
    if ".".join(labels).lower() == "web.test" and qtype == 1:
        answer = (b"\xc0\x0c\x00\x01\x00\x01" + struct.pack("!I", 30)
                  + b"\x00\x04" + socket.inet_aton("10.242.0.20"))
        count = 1
    header = q[:2] + b"\x81\x80\x00\x01" + struct.pack("!H", count) + b"\x00\x00\x00\x00"
    s.sendto(header + question + answer, peer)
"#;

const HTTP_SERVER: &str = r#"
from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"ready")
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(200); self.end_headers(); self.wfile.write(str(len(body)).encode())
    def log_message(self, *_): pass
HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
"#;

const HTTP_CLIENT: &str = r#"
import urllib.request
body = b"x" * 65536
req = urllib.request.Request("http://web.test:8080/", data=body)
print(urllib.request.urlopen(req, timeout=4).read().decode())
"#;

struct Cleanup {
    namespaces: Vec<String>,
    resolver_dirs: Vec<PathBuf>,
    underlay: Option<String>,
    bridge_nf_call_iptables: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for namespace in &self.namespaces {
            let _ = sh("ip", &["netns", "delete", namespace]);
        }
        if let Some(underlay) = &self.underlay {
            let _ = sh("ip", &["link", "delete", underlay]);
        }
        let setting = format!(
            "net.bridge.bridge-nf-call-iptables={}",
            self.bridge_nf_call_iptables.trim()
        );
        let _ = sh("sysctl", &["-w", &setting]);
        for dir in &self.resolver_dirs {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

struct Server(Child);

impl Server {
    fn start(namespace: &str, script: &str) -> Self {
        let child = Command::new("ip")
            .args(["netns", "exec", namespace, "python3", "-u", "-c", script])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start namespace server");
        Self(child)
    }
}

impl Server {
    /// Waits until the server listens on `port` (`ss` flag `-lnt` or `-lnu`),
    /// failing early if the process exits.
    async fn wait_listening(
        &mut self,
        namespace: &str,
        ss_flags: &str,
        port: u16,
    ) -> Result<(), String> {
        let suffix = format!(":{port}");
        for _ in 0..50 {
            if let Ok(Some(status)) = self.0.try_wait() {
                return Err(format!("server in {namespace} exited early: {status}"));
            }
            let listening = in_ns(namespace, "ss", &[ss_flags]).unwrap_or_default();
            if listening
                .lines()
                .filter_map(|line| line.split_whitespace().nth(3))
                .any(|local| local.ends_with(&suffix))
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "server in {namespace} did not listen on port {port} within 5s"
        ))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sh(program: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| format!("{program} {}: {e}", args.join(" ")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(format!(
            "{program} {} exited {}: {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn in_ns(namespace: &str, program: &str, args: &[&str]) -> Result<String, String> {
    let mut command = vec!["netns", "exec", namespace, program];
    command.extend_from_slice(args);
    sh("ip", &command)
}

fn ip_ns(namespace: &str, args: &[&str]) -> Result<String, String> {
    let mut command = vec!["-n", namespace];
    command.extend_from_slice(args);
    sh("ip", &command)
}

fn unavailable() -> Option<String> {
    if sh("id", &["-u"]).ok().as_deref() != Some("0") {
        return Some("requires root/CAP_NET_ADMIN".into());
    }
    for (program, args) in [
        ("ip", vec!["-V"]),
        ("iptables", vec!["-V"]),
        ("wg", vec!["--version"]),
        ("ping", vec!["-V"]),
        ("python3", vec!["-V"]),
    ] {
        if let Err(error) = sh(program, &args) {
            return Some(error);
        }
    }
    if !Path::new("/sys/module/wireguard").exists() && sh("modprobe", &["wireguard"]).is_err() {
        return Some("WireGuard kernel module is unavailable".into());
    }
    if !Path::new("/sys/module/br_netfilter").exists() && sh("modprobe", &["br_netfilter"]).is_err()
    {
        return Some("bridge netfilter module is unavailable".into());
    }
    None
}

fn add_namespace(cleanup: &mut Cleanup, name: String) -> Result<String, String> {
    sh("ip", &["netns", "add", &name])?;
    cleanup.namespaces.push(name.clone());
    Ok(name)
}

fn node_link(namespace: &str, underlay: &str, index: usize, address: &str) -> Result<(), String> {
    let host_end = format!("h{}{}", std::process::id() % 10_000, index);
    let node_end = format!("n{}{}", std::process::id() % 10_000, index);
    sh(
        "ip",
        &[
            "link", "add", &host_end, "type", "veth", "peer", "name", &node_end,
        ],
    )?;
    sh("ip", &["link", "set", &host_end, "master", underlay])?;
    sh("ip", &["link", "set", &host_end, "up"])?;
    sh("ip", &["link", "set", &node_end, "netns", namespace])?;
    ip_ns(namespace, &["addr", "add", address, "dev", &node_end])?;
    ip_ns(namespace, &["link", "set", "dev", &node_end, "up"])?;
    Ok(())
}

fn create_node_bridge(namespace: &str, network: &str, subnet: &str) -> Result<String, String> {
    let bridge = bridge_name(network);
    ip_ns(namespace, &["link", "add", &bridge, "type", "bridge"])?;
    ip_ns(namespace, &["addr", "add", subnet, "dev", &bridge])?;
    ip_ns(namespace, &["link", "set", &bridge, "up"])?;
    in_ns(namespace, "sysctl", &["-w", "net.ipv4.ip_forward=1"])?;
    in_ns(
        namespace,
        "sysctl",
        &["-w", "net.bridge.bridge-nf-call-iptables=1"],
    )?;
    in_ns(
        namespace,
        "sysctl",
        &["-w", "net.ipv4.conf.all.rp_filter=0"],
    )?;
    Ok(bridge)
}

fn enable_proxy_arp(namespace: &str, bridge: &str) -> Result<(), String> {
    in_ns(
        namespace,
        "sysctl",
        &["-w", &format!("net.ipv4.conf.{bridge}.proxy_arp=1")],
    )?;
    Ok(())
}

struct TaskNetwork<'a> {
    namespace_name: String,
    node: &'a str,
    bridge: &'a str,
    host_veth: &'a str,
    task_veth: &'a str,
    address: &'a str,
    gateway: &'a str,
}

fn create_task(cleanup: &mut Cleanup, network: TaskNetwork<'_>) -> Result<String, String> {
    let TaskNetwork {
        namespace_name,
        node,
        bridge,
        host_veth,
        task_veth,
        address,
        gateway,
    } = network;
    let task = add_namespace(cleanup, namespace_name)?;
    sh(
        "ip",
        &[
            "link", "add", host_veth, "type", "veth", "peer", "name", task_veth,
        ],
    )?;
    sh("ip", &["link", "set", host_veth, "netns", node])?;
    sh("ip", &["link", "set", task_veth, "netns", &task])?;
    ip_ns(node, &["link", "set", host_veth, "master", bridge])?;
    ip_ns(node, &["link", "set", host_veth, "up"])?;
    ip_ns(&task, &["addr", "add", address, "dev", task_veth])?;
    ip_ns(&task, &["link", "set", "lo", "up"])?;
    ip_ns(&task, &["link", "set", task_veth, "up"])?;
    ip_ns(&task, &["route", "add", "default", "via", gateway])?;
    let resolver = PathBuf::from(format!("/etc/netns/{task}/resolv.conf"));
    let resolver_dir = resolver.parent().expect("resolver parent");
    fs::create_dir_all(resolver_dir).map_err(|error| error.to_string())?;
    cleanup.resolver_dirs.push(resolver_dir.to_path_buf());
    fs::write(
        &resolver,
        format!("nameserver {gateway}\noptions ndots:0\n"),
    )
    .map_err(|error| error.to_string())?;
    Ok(task)
}

fn configure_wg(
    namespace: &str,
    listen_port: u16,
    key_path: &Path,
    mtu: u16,
) -> Result<(), String> {
    ip_ns(
        namespace,
        &["link", "add", "dev", WG_INTERFACE_NAME, "type", "wireguard"],
    )?;
    in_ns(
        namespace,
        "wg",
        &[
            "set",
            WG_INTERFACE_NAME,
            "private-key",
            key_path.to_str().expect("UTF-8 temp path"),
            "listen-port",
            &listen_port.to_string(),
        ],
    )?;
    ip_ns(
        namespace,
        &[
            "link",
            "set",
            "dev",
            WG_INTERFACE_NAME,
            "mtu",
            &mtu.to_string(),
        ],
    )?;
    ip_ns(namespace, &["link", "set", "dev", WG_INTERFACE_NAME, "up"])?;
    for interface in ["all", WG_INTERFACE_NAME] {
        in_ns(
            namespace,
            "sysctl",
            &["-w", &format!("net.ipv4.conf.{interface}.rp_filter=0")],
        )?;
    }
    Ok(())
}

fn install_mesh_rules(
    namespace: &str,
    bridge: &str,
    subnet: &str,
    isolated_subnet: &str,
) -> Result<(), String> {
    const CHAIN: &str = "MINGA_WG_FORWARD";
    in_ns(
        namespace,
        "iptables",
        &["-w", "5", "-t", "filter", "-N", CHAIN],
    )?;
    in_ns(
        namespace,
        "iptables",
        &["-w", "5", "-t", "filter", "-I", "FORWARD", "1", "-j", CHAIN],
    )?;
    for (input, output) in [(bridge, WG_INTERFACE_NAME), (WG_INTERFACE_NAME, bridge)] {
        in_ns(
            namespace,
            "iptables",
            &[
                "-w", "5", "-t", "filter", "-A", CHAIN, "-i", input, "-o", output, "-s", subnet,
                "-j", "ACCEPT",
            ],
        )?;
    }
    in_ns(
        namespace,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-A",
            CHAIN,
            "-i",
            bridge,
            "-d",
            isolated_subnet,
            "-j",
            "DROP",
        ],
    )?;
    for direction in ["-o", "-i"] {
        in_ns(
            namespace,
            "iptables",
            &[
                "-w",
                "5",
                "-t",
                "filter",
                "-A",
                CHAIN,
                direction,
                WG_INTERFACE_NAME,
                "-j",
                "DROP",
            ],
        )?;
    }
    in_ns(
        namespace,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-I",
            "INPUT",
            "1",
            "-i",
            WG_INTERFACE_NAME,
            "-j",
            "DROP",
        ],
    )?;
    in_ns(
        namespace,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "mangle",
            "-A",
            "FORWARD",
            "-o",
            WG_INTERFACE_NAME,
            "-p",
            "tcp",
            "--tcp-flags",
            "SYN,RST",
            "SYN",
            "-j",
            "TCPMSS",
            "--clamp-mss-to-pmtu",
        ],
    )?;
    Ok(())
}

fn install_task_source_rules(namespace: &str, veth: &str, address: &str) -> Result<(), String> {
    const CHAIN: &str = "MINGA_TASK_SOURCE";
    in_ns(
        namespace,
        "iptables",
        &["-w", "5", "-t", "filter", "-N", CHAIN],
    )?;
    for hook in ["FORWARD", "INPUT"] {
        in_ns(
            namespace,
            "iptables",
            &["-w", "5", "-t", "filter", "-I", hook, "1", "-j", CHAIN],
        )?;
    }
    in_ns(
        namespace,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-A",
            CHAIN,
            "-m",
            "physdev",
            "--physdev-in",
            veth,
            "-s",
            address,
            "-j",
            "RETURN",
        ],
    )?;
    in_ns(
        namespace,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-A",
            CHAIN,
            "-m",
            "physdev",
            "--physdev-in",
            veth,
            "-j",
            "DROP",
        ],
    )?;
    Ok(())
}
fn peer(namespace: &str, public_key: &str, endpoint: &str, allowed_ip: &str) -> Result<(), String> {
    in_ns(
        namespace,
        "wg",
        &[
            "set",
            WG_INTERFACE_NAME,
            "peer",
            public_key,
            "endpoint",
            endpoint,
            "allowed-ips",
            allowed_ip,
            "persistent-keepalive",
            "25",
        ],
    )?;
    Ok(())
}

fn nat_worker_link(
    router: &str,
    worker: &str,
    bridge: &str,
    index: usize,
    worker_address: &str,
) -> Result<(), String> {
    let router_end = format!("r{}{}", std::process::id() % 10_000, index);
    let worker_end = format!("w{}{}", std::process::id() % 10_000, index);
    sh(
        "ip",
        &[
            "link",
            "add",
            &router_end,
            "type",
            "veth",
            "peer",
            "name",
            &worker_end,
        ],
    )?;
    sh("ip", &["link", "set", &router_end, "netns", router])?;
    sh("ip", &["link", "set", &worker_end, "netns", worker])?;
    ip_ns(router, &["link", "set", &router_end, "master", bridge])?;
    ip_ns(router, &["link", "set", "dev", &router_end, "up"])?;
    ip_ns(worker, &["link", "set", "lo", "up"])?;
    ip_ns(worker, &["addr", "add", worker_address, "dev", &worker_end])?;
    ip_ns(worker, &["link", "set", "dev", &worker_end, "up"])?;
    Ok(())
}

fn assigned_peer(
    namespace: &str,
    public_key: &str,
    endpoint: Option<&str>,
    allowed_ips: &[&str],
    keepalive: u16,
) -> Result<(), String> {
    let mut args = vec!["set", WG_INTERFACE_NAME, "peer", public_key];
    let joined_ips = allowed_ips.join(",");
    if let Some(endpoint) = endpoint {
        args.extend(["endpoint", endpoint]);
    }
    if !allowed_ips.is_empty() {
        args.extend(["allowed-ips", &joined_ips]);
    }
    let keepalive = keepalive.to_string();
    if keepalive != "0" {
        args.extend(["persistent-keepalive", &keepalive]);
    }
    in_ns(namespace, "wg", &args)?;
    Ok(())
}

fn install_relay_rule(namespace: &str, subnet: &str) -> Result<(), String> {
    in_ns(namespace, "sysctl", &["-w", "net.ipv4.ip_forward=1"])?;
    in_ns(
        namespace,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-I",
            "MINGA_WG_FORWARD",
            "1",
            "-i",
            WG_INTERFACE_NAME,
            "-o",
            WG_INTERFACE_NAME,
            "-s",
            subnet,
            "-d",
            subnet,
            "-j",
            "ACCEPT",
        ],
    )?;
    Ok(())
}

fn ping(namespace: &str, address: &str, source: Option<&str>) -> Result<(), String> {
    let mut args = vec!["-c", "1", "-W", "2"];
    if let Some(source) = source {
        args.extend(["-I", source]);
    }
    args.push(address);
    in_ns(namespace, "ping", &args).map(|_| ())
}

#[tokio::test(flavor = "multi_thread")]
async fn wireguard_routes_shared_tasks_and_isolates_other_networks() -> Result<(), String> {
    let _lock = NETNS_TEST_LOCK.lock().await;
    if let Some(reason) = unavailable() {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required WireGuard netns test cannot run: {reason}"
        );
        eprintln!("SKIP: {reason}");
        return Ok(());
    }

    let pid = std::process::id();
    let underlay = format!("mwbr{}", pid % 100_000);
    let bridge_nf_call_iptables =
        fs::read_to_string("/proc/sys/net/bridge/bridge-nf-call-iptables")
            .map_err(|error| error.to_string())?;
    let mut cleanup = Cleanup {
        namespaces: Vec::new(),
        resolver_dirs: Vec::new(),
        underlay: None,
        bridge_nf_call_iptables,
    };
    sh("sysctl", &["-w", "net.bridge.bridge-nf-call-iptables=0"])?;
    let node_a = add_namespace(&mut cleanup, format!("mwg{pid}a"))?;
    let node_b = add_namespace(&mut cleanup, format!("mwg{pid}b"))?;
    let node_c = add_namespace(&mut cleanup, format!("mwg{pid}c"))?;
    sh("ip", &["link", "add", &underlay, "type", "bridge"])?;
    cleanup.underlay = Some(underlay.clone());
    sh("ip", &["link", "set", &underlay, "up"])?;
    node_link(&node_a, &underlay, 0, "198.18.0.1/24")?;
    node_link(&node_b, &underlay, 1, "198.18.0.2/24")?;
    node_link(&node_c, &underlay, 2, "198.18.0.3/24")?;
    ip_ns(
        &node_a,
        &["route", "replace", "10.243.0.0/24", "via", "198.18.0.3"],
    )?;
    ip_ns(
        &node_c,
        &["route", "replace", "10.242.0.0/24", "via", "198.18.0.1"],
    )?;
    for namespace in [node_a.as_str(), node_c.as_str()] {
        in_ns(
            namespace,
            "sysctl",
            &["-w", "net.ipv4.conf.all.rp_filter=0"],
        )?;
        in_ns(
            namespace,
            "iptables",
            &["-w", "5", "-P", "FORWARD", "ACCEPT"],
        )?;
    }

    let bridge_a = create_node_bridge(&node_a, "shared", "10.242.0.1/24")?;
    let bridge_b = create_node_bridge(&node_b, "shared", "10.242.0.1/24")?;
    let bridge_c = create_node_bridge(&node_c, "isolated", "10.243.0.1/24")?;
    enable_proxy_arp(&node_a, &bridge_a)?;
    enable_proxy_arp(&node_b, &bridge_b)?;
    enable_proxy_arp(&node_c, &bridge_c)?;
    let task_a = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}a"),
            node: &node_a,
            bridge: &bridge_a,
            host_veth: "ta0",
            task_veth: "at0",
            address: "10.242.0.10/24",
            gateway: "10.242.0.1",
        },
    )?;
    let task_b = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}b"),
            node: &node_b,
            bridge: &bridge_b,
            host_veth: "tb0",
            task_veth: "bt0",
            address: "10.242.0.20/24",
            gateway: "10.242.0.1",
        },
    )?;
    let task_c = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}c"),
            node: &node_c,
            bridge: &bridge_c,
            host_veth: "tc0",
            task_veth: "ct0",
            address: "10.243.0.30/24",
            gateway: "10.243.0.1",
        },
    )?;

    let keys = TempDir::new().map_err(|error| error.to_string())?;
    let key_a = KeyPair::generate();
    let key_b = KeyPair::generate();
    for (name, key) in [("a.key", &key_a), ("b.key", &key_b)] {
        let path = keys.path().join(name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| error.to_string())?;
        file.write_all(key.private_key.as_bytes())
            .map_err(|error| error.to_string())?;
    }
    configure_wg(&node_a, 51820, &keys.path().join("a.key"), 1420)?;
    configure_wg(&node_b, 51821, &keys.path().join("b.key"), 1420)?;
    peer(
        &node_a,
        &key_b.public_key,
        "198.18.0.2:51821",
        "10.242.0.20/32",
    )?;
    peer(
        &node_b,
        &key_a.public_key,
        "198.18.0.1:51820",
        "10.242.0.10/32",
    )?;
    let wg_link = ip_ns(&node_a, &["-o", "link", "show", "dev", WG_INTERFACE_NAME])?;
    assert!(wg_link.contains("mtu 1420"), "WireGuard MTU: {wg_link}");
    ip_ns(
        &node_a,
        &[
            "route",
            "replace",
            "10.242.0.20/32",
            "dev",
            WG_INTERFACE_NAME,
        ],
    )?;
    ip_ns(
        &node_b,
        &[
            "route",
            "replace",
            "10.242.0.10/32",
            "dev",
            WG_INTERFACE_NAME,
        ],
    )?;
    let initial_routes = ip_ns(&node_a, &["-4", "route", "show", "dev", WG_INTERFACE_NAME])?;
    assert!(
        initial_routes.contains("10.242.0.20"),
        "remote task /32 route missing: {initial_routes}"
    );
    let initial_allowed_ips = in_ns(&node_a, "wg", &["show", WG_INTERFACE_NAME, "allowed-ips"])?;
    assert!(
        initial_allowed_ips.contains("10.242.0.20/32"),
        "peer AllowedIP missing: {initial_allowed_ips}"
    );

    // Prime bridge FDB entries for each attached task.
    ping(&task_a, "10.242.0.1", None)?;
    ping(&task_b, "10.242.0.1", None)?;
    ping(&task_c, "10.243.0.1", None)?;
    // Prove the underlay path exists before the overlay's cross-network drop.
    ping(&task_a, "10.243.0.30", None).map_err(|error| {
        let node_underlay_ping =
            ping(&node_a, "198.18.0.3", None).map_or_else(|error| error, |_| "ok".into());
        let node_a_neighbor =
            ip_ns(&node_a, &["neigh", "show"]).unwrap_or_else(|route_error| route_error);
        let node_a_forward =
            in_ns(&node_a, "iptables", &["-S", "FORWARD"]).unwrap_or_else(|rule_error| rule_error);
        let node_c_forward =
            in_ns(&node_c, "iptables", &["-S", "FORWARD"]).unwrap_or_else(|rule_error| rule_error);
        let bridge_ping =
            ping(&node_a, "10.243.0.1", Some("10.242.0.1")).map_or_else(|error| error, |_| "ok".into());
        let node_a_routes =
            ip_ns(&node_a, &["-4", "route", "show"]).unwrap_or_else(|route_error| route_error);
        let node_c_routes =
            ip_ns(&node_c, &["-4", "route", "show"]).unwrap_or_else(|route_error| route_error);
        let task_a_routes =
            ip_ns(&task_a, &["-4", "route", "show"]).unwrap_or_else(|route_error| route_error);
        let task_c_routes =
            ip_ns(&task_c, &["-4", "route", "show"]).unwrap_or_else(|route_error| route_error);
        format!(
            "cross-network underlay path was not reachable: {error}; node A routes: {node_a_routes}; node C routes: {node_c_routes}; task A routes: {task_a_routes}; task C routes: {task_c_routes}; node A -> node C underlay ping: {node_underlay_ping}; node A -> node C bridge ping: {bridge_ping}; node A neighbors: {node_a_neighbor}; node A FORWARD: {node_a_forward}; node C FORWARD: {node_c_forward}"
        )
    })?;
    install_mesh_rules(&node_a, &bridge_a, "10.242.0.0/24", "10.243.0.0/24")?;
    install_mesh_rules(&node_b, &bridge_b, "10.242.0.0/24", "10.243.0.0/24")?;
    install_mesh_rules(&node_c, &bridge_c, "10.243.0.0/24", "10.242.0.0/24")?;
    install_task_source_rules(&node_a, "ta0", "10.242.0.10/32")?;
    install_task_source_rules(&node_b, "tb0", "10.242.0.20/32")?;
    install_task_source_rules(&node_c, "tc0", "10.243.0.30/32")?;
    let forward_rules = in_ns(&node_a, "iptables", &["-S", "FORWARD"])?;
    let jump_order: Vec<&str> = forward_rules
        .lines()
        .filter_map(|line| line.strip_prefix("-A FORWARD -j "))
        .collect();
    assert_eq!(
        jump_order.first(),
        Some(&"MINGA_TASK_SOURCE"),
        "task source validation must precede mesh acceptance: {forward_rules}"
    );
    assert_eq!(jump_order.get(1), Some(&"MINGA_WG_FORWARD"));
    for (namespace, bridge) in [
        (&node_a, &bridge_a),
        (&node_b, &bridge_b),
        (&node_c, &bridge_c),
    ] {
        assert_eq!(
            in_ns(
                namespace,
                "sysctl",
                &["-n", &format!("net.ipv4.conf.{bridge}.proxy_arp")],
            )?,
            "1"
        );
        in_ns(
            namespace,
            "iptables",
            &[
                "-w",
                "5",
                "-t",
                "mangle",
                "-C",
                "FORWARD",
                "-o",
                WG_INTERFACE_NAME,
                "-p",
                "tcp",
                "--tcp-flags",
                "SYN,RST",
                "SYN",
                "-j",
                "TCPMSS",
                "--clamp-mss-to-pmtu",
            ],
        )?;
    }
    assert!(
        ping(&task_a, "10.243.0.30", None).is_err(),
        "a task on another overlay must not be reachable"
    );
    let mut dns = Server::start(&node_a, DNS_SERVER);
    let mut web = Server::start(&task_b, HTTP_SERVER);
    let mut isolated = Server::start(&task_c, HTTP_SERVER);
    dns.wait_listening(&node_a, "-lnu", 53).await?;
    web.wait_listening(&task_b, "-lnt", 8080).await?;
    isolated.wait_listening(&task_c, "-lnt", 8080).await?;

    ping(&task_a, "10.242.0.20", None)?;
    let response = in_ns(&task_a, "python3", &["-c", HTTP_CLIENT])?;
    assert_eq!(response, "65536", "DNS + 64KiB HTTP payload over WireGuard");

    // A compromised underlay peer cannot inject a task source into the mesh.
    ip_ns(&node_c, &["link", "set", "lo", "up"])?;
    ip_ns(&node_c, &["addr", "add", "10.242.0.10/32", "dev", "lo"])?;
    assert!(
        ping(&node_c, "10.242.0.20", Some("10.242.0.10")).is_err(),
        "underlay traffic must not be forwarded into WireGuard"
    );
    let wg_drops = in_ns(
        &node_a,
        "iptables",
        &["-t", "filter", "-L", "MINGA_WG_FORWARD", "-v", "-n", "-x"],
    )?;
    let underlay_drop_count = wg_drops
        .lines()
        .find(|line| line.contains("DROP") && line.contains(WG_INTERFACE_NAME))
        .and_then(|line| line.split_whitespace().next())
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or_default();
    assert!(
        underlay_drop_count > 0,
        "the WireGuard egress drop must catch the underlay injection: {wg_drops}"
    );
    // Task A cannot emit traffic using an IP outside its IPAM assignment.
    ip_ns(&task_a, &["addr", "add", "10.242.0.11/32", "dev", "at0"])?;
    assert!(
        ping(&task_a, "10.242.0.20", Some("10.242.0.11")).is_err(),
        "a task must not emit a source IP outside its IPAM assignment"
    );
    let source_rules = in_ns(
        &node_a,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "filter",
            "-L",
            "MINGA_TASK_SOURCE",
            "-v",
            "-n",
            "-x",
        ],
    )?;
    assert!(
        source_rules.lines().any(|line| {
            line.contains("DROP")
                && line.contains("ta0")
                && line
                    .split_whitespace()
                    .next()
                    .and_then(|packets| packets.parse::<u64>().ok())
                    .is_some_and(|packets| packets > 0)
        }),
        "spoofed frame was not rejected by the task source guard: {source_rules}"
    );

    // Removing the task removes its /32 route and peer AllowedIPs.
    in_ns(
        &node_a,
        "wg",
        &[
            "set",
            WG_INTERFACE_NAME,
            "peer",
            &key_b.public_key,
            "remove",
        ],
    )?;
    in_ns(
        &node_b,
        "wg",
        &[
            "set",
            WG_INTERFACE_NAME,
            "peer",
            &key_a.public_key,
            "remove",
        ],
    )?;
    ip_ns(
        &node_a,
        &["route", "del", "10.242.0.20/32", "dev", WG_INTERFACE_NAME],
    )?;
    ip_ns(
        &node_b,
        &["route", "del", "10.242.0.10/32", "dev", WG_INTERFACE_NAME],
    )?;
    let routes = ip_ns(&node_a, &["-4", "route", "show", "dev", WG_INTERFACE_NAME])?;
    assert!(!routes.contains("10.242.0.20"), "stale /32 route: {routes}");
    let allowed_ips = in_ns(&node_b, "wg", &["show", WG_INTERFACE_NAME, "allowed-ips"])?;
    assert!(
        !allowed_ips.contains("10.242.0.10/32"),
        "stale AllowedIP: {allowed_ips}"
    );
    let routes_b = ip_ns(&node_b, &["-4", "route", "show", "dev", WG_INTERFACE_NAME])?;
    assert!(
        !routes_b.contains("10.242.0.10"),
        "stale /32 route: {routes_b}"
    );
    let allowed_ips_a = in_ns(&node_a, "wg", &["show", WG_INTERFACE_NAME, "allowed-ips"])?;
    assert!(
        !allowed_ips_a.contains("10.242.0.20/32"),
        "stale AllowedIP: {allowed_ips_a}"
    );
    assert!(ping(&task_a, "10.242.0.20", None).is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn nat_workers_relay_through_manager_and_use_direct_path_to_reachable_worker(
) -> Result<(), String> {
    let _lock = NETNS_TEST_LOCK.lock().await;
    if let Some(reason) = unavailable() {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required WireGuard netns test cannot run: {reason}"
        );
        eprintln!("SKIP: {reason}");
        return Ok(());
    }

    let pid = std::process::id();
    let underlay = format!("mwnat{}", pid % 100_000);
    let bridge_nf_call_iptables =
        fs::read_to_string("/proc/sys/net/bridge/bridge-nf-call-iptables")
            .map_err(|error| error.to_string())?;
    let mut cleanup = Cleanup {
        namespaces: Vec::new(),
        resolver_dirs: Vec::new(),
        underlay: None,
        bridge_nf_call_iptables,
    };
    sh("sysctl", &["-w", "net.bridge.bridge-nf-call-iptables=0"])?;
    let manager = add_namespace(&mut cleanup, format!("mwn{pid}m"))?;
    let standby = add_namespace(&mut cleanup, format!("mwn{pid}s"))?;
    let worker_a = add_namespace(&mut cleanup, format!("mwn{pid}a"))?;
    let worker_b = add_namespace(&mut cleanup, format!("mwn{pid}b"))?;
    let reachable = add_namespace(&mut cleanup, format!("mwn{pid}c"))?;
    let nat = add_namespace(&mut cleanup, format!("mwn{pid}r"))?;
    sh("ip", &["link", "add", &underlay, "type", "bridge"])?;
    cleanup.underlay = Some(underlay.clone());
    sh("ip", &["link", "set", &underlay, "up"])?;
    node_link(&manager, &underlay, 10, "198.18.0.1/24")?;
    node_link(&standby, &underlay, 11, "198.18.0.2/24")?;
    node_link(&reachable, &underlay, 12, "198.18.0.5/24")?;
    node_link(&nat, &underlay, 13, "198.18.0.4/24")?;
    ip_ns(&nat, &["link", "add", "brnat", "type", "bridge"])?;
    ip_ns(&nat, &["addr", "add", "10.244.0.1/24", "dev", "brnat"])?;
    ip_ns(&nat, &["link", "set", "brnat", "up"])?;
    nat_worker_link(&nat, &worker_a, "brnat", 0, "10.244.0.2/24")?;
    nat_worker_link(&nat, &worker_b, "brnat", 1, "10.244.0.3/24")?;
    let nat_underlay = format!("n{}13", pid % 10_000);
    in_ns(&nat, "sysctl", &["-w", "net.ipv4.ip_forward=1"])?;
    in_ns(&nat, "iptables", &["-w", "5", "-P", "FORWARD", "ACCEPT"])?;
    in_ns(
        &nat,
        "iptables",
        &[
            "-w",
            "5",
            "-t",
            "nat",
            "-A",
            "POSTROUTING",
            "-s",
            "10.244.0.0/24",
            "-o",
            &nat_underlay,
            "-j",
            "MASQUERADE",
        ],
    )?;
    for (worker, address) in [(&worker_a, "10.244.0.1"), (&worker_b, "10.244.0.1")] {
        ip_ns(worker, &["route", "replace", "default", "via", address])?;
    }

    let subnet = "10.242.0.0/24";
    let bridges = [
        create_node_bridge(&manager, "shared", "10.242.0.1/24")?,
        create_node_bridge(&standby, "shared", "10.242.0.1/24")?,
        create_node_bridge(&worker_a, "shared", "10.242.0.1/24")?,
        create_node_bridge(&worker_b, "shared", "10.242.0.1/24")?,
        create_node_bridge(&reachable, "shared", "10.242.0.1/24")?,
    ];
    for (namespace, bridge) in [
        (&manager, &bridges[0]),
        (&standby, &bridges[1]),
        (&worker_a, &bridges[2]),
        (&worker_b, &bridges[3]),
        (&reachable, &bridges[4]),
    ] {
        enable_proxy_arp(namespace, bridge)?;
    }
    let _task_manager = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}m"),
            node: &manager,
            bridge: &bridges[0],
            host_veth: "tm0",
            task_veth: "mt0",
            address: "10.242.0.30/24",
            gateway: "10.242.0.1",
        },
    )?;
    let _task_standby = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}s"),
            node: &standby,
            bridge: &bridges[1],
            host_veth: "ts0",
            task_veth: "st0",
            address: "10.242.0.40/24",
            gateway: "10.242.0.1",
        },
    )?;
    let task_a = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}a"),
            node: &worker_a,
            bridge: &bridges[2],
            host_veth: "ta0",
            task_veth: "at0",
            address: "10.242.0.10/24",
            gateway: "10.242.0.1",
        },
    )?;
    let task_b = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}b"),
            node: &worker_b,
            bridge: &bridges[3],
            host_veth: "tb0",
            task_veth: "bt0",
            address: "10.242.0.20/24",
            gateway: "10.242.0.1",
        },
    )?;
    let task_reachable = create_task(
        &mut cleanup,
        TaskNetwork {
            namespace_name: format!("mwt{pid}c"),
            node: &reachable,
            bridge: &bridges[4],
            host_veth: "tc0",
            task_veth: "ct0",
            address: "10.242.0.50/24",
            gateway: "10.242.0.1",
        },
    )?;

    let keys = TempDir::new().map_err(|error| error.to_string())?;
    let key_manager = KeyPair::generate();
    let key_standby = KeyPair::generate();
    let key_a = KeyPair::generate();
    let key_b = KeyPair::generate();
    let key_reachable = KeyPair::generate();
    for (name, key) in [
        ("manager.key", &key_manager),
        ("standby.key", &key_standby),
        ("a.key", &key_a),
        ("b.key", &key_b),
        ("reachable.key", &key_reachable),
    ] {
        let path = keys.path().join(name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| error.to_string())?;
        file.write_all(key.private_key.as_bytes())
            .map_err(|error| error.to_string())?;
    }
    configure_wg(&manager, 51820, &keys.path().join("manager.key"), 1420)?;
    configure_wg(&standby, 51822, &keys.path().join("standby.key"), 1420)?;
    configure_wg(&worker_a, 51821, &keys.path().join("a.key"), 1420)?;
    configure_wg(&worker_b, 51823, &keys.path().join("b.key"), 1420)?;
    configure_wg(&reachable, 51825, &keys.path().join("reachable.key"), 1420)?;

    assigned_peer(&manager, &key_a.public_key, None, &["10.242.0.10/32"], 0)?;
    assigned_peer(&manager, &key_b.public_key, None, &["10.242.0.20/32"], 0)?;
    assigned_peer(&standby, &key_a.public_key, None, &["10.242.0.10/32"], 0)?;
    assigned_peer(&standby, &key_b.public_key, None, &["10.242.0.20/32"], 0)?;
    assigned_peer(
        &worker_a,
        &key_manager.public_key,
        Some("198.18.0.1:51820"),
        &["10.242.0.20/32", "10.242.0.30/32"],
        25,
    )?;
    assigned_peer(
        &worker_a,
        &key_standby.public_key,
        Some("198.18.0.2:51822"),
        &["10.242.0.40/32"],
        25,
    )?;
    assigned_peer(
        &worker_a,
        &key_reachable.public_key,
        Some("198.18.0.5:51825"),
        &["10.242.0.50/32"],
        25,
    )?;
    assigned_peer(
        &worker_b,
        &key_manager.public_key,
        Some("198.18.0.1:51820"),
        &["10.242.0.10/32", "10.242.0.30/32"],
        25,
    )?;
    assigned_peer(
        &worker_b,
        &key_standby.public_key,
        Some("198.18.0.2:51822"),
        &["10.242.0.40/32"],
        25,
    )?;
    assigned_peer(&reachable, &key_a.public_key, None, &["10.242.0.10/32"], 0)?;

    for (namespace, addresses) in [
        (&manager, &["10.242.0.10/32", "10.242.0.20/32"][..]),
        (&standby, &["10.242.0.10/32", "10.242.0.20/32"][..]),
        (
            &worker_a,
            &[
                "10.242.0.20/32",
                "10.242.0.30/32",
                "10.242.0.40/32",
                "10.242.0.50/32",
            ][..],
        ),
        (
            &worker_b,
            &["10.242.0.10/32", "10.242.0.30/32", "10.242.0.40/32"][..],
        ),
        (&reachable, &["10.242.0.10/32"][..]),
    ] {
        for address in addresses {
            ip_ns(
                namespace,
                &["-4", "route", "replace", address, "dev", WG_INTERFACE_NAME],
            )?;
        }
    }

    install_mesh_rules(&manager, &bridges[0], subnet, "10.243.0.0/24")?;
    install_mesh_rules(&standby, &bridges[1], subnet, "10.243.0.0/24")?;
    install_mesh_rules(&worker_a, &bridges[2], subnet, "10.243.0.0/24")?;
    install_mesh_rules(&worker_b, &bridges[3], subnet, "10.243.0.0/24")?;
    install_mesh_rules(&reachable, &bridges[4], subnet, "10.243.0.0/24")?;
    install_relay_rule(&manager, subnet)?;
    assert_eq!(
        in_ns(&manager, "sysctl", &["-n", "net.ipv4.ip_forward"])?,
        "1"
    );
    for (namespace, host_veth, address) in [
        (&manager, "tm0", "10.242.0.30/32"),
        (&standby, "ts0", "10.242.0.40/32"),
        (&worker_a, "ta0", "10.242.0.10/32"),
        (&worker_b, "tb0", "10.242.0.20/32"),
        (&reachable, "tc0", "10.242.0.50/32"),
    ] {
        install_task_source_rules(namespace, host_veth, address)?;
    }

    ping(&task_a, "10.242.0.30", None)
        .map_err(|error| format!("NAT worker could not reach its manager directly: {error}"))?;
    ping(&task_b, "10.242.0.30", None)
        .map_err(|error| format!("second NAT worker could not reach its manager: {error}"))?;
    ping(&task_a, "10.242.0.40", None)?;
    ping(&task_b, "10.242.0.40", None)?;
    ping(&task_a, "10.242.0.20", None)
        .map_err(|error| format!("NAT workers could not relay through manager: {error}"))?;
    in_ns(&task_a, "ping", &["-c", "3", "-W", "2", "10.242.0.50"])
        .map(|_| ())
        .map_err(|error| {
            format!("NAT worker could not reach reachable worker directly: {error}")
        })?;
    let direct_endpoint = in_ns(&worker_a, "wg", &["show", WG_INTERFACE_NAME, "endpoints"])?;
    assert!(
        direct_endpoint.contains("198.18.0.5:51825"),
        "reachable worker endpoint was not selected directly: {direct_endpoint}"
    );
    let direct_handshakes = in_ns(
        &reachable,
        "wg",
        &["show", WG_INTERFACE_NAME, "latest-handshakes"],
    )?;
    assert!(
        direct_handshakes.lines().any(|line| {
            line.starts_with(&key_a.public_key)
                && line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|timestamp| timestamp.parse::<u64>().ok())
                    .is_some_and(|timestamp| timestamp > 0)
        }),
        "reachable worker did not handshake directly with the NAT worker: {direct_handshakes}"
    );
    ping(&task_reachable, "10.242.0.10", None).map_err(|error| {
        format!("reachable worker could not use the NAT worker's learned endpoint: {error}")
    })?;
    let relay_rules = in_ns(&manager, "iptables", &["-S", "MINGA_WG_FORWARD"])?;
    assert!(
        relay_rules
            .contains("-s 10.242.0.0/24 -d 10.242.0.0/24 -i minga-wg0 -o minga-wg0 -j ACCEPT"),
        "manager relay rule is not limited to the overlay subnet: {relay_rules}"
    );

    let failover_started = Instant::now();
    ip_ns(&manager, &["link", "set", "dev", WG_INTERFACE_NAME, "down"])?;
    install_relay_rule(&standby, subnet)?;
    for (namespace, old_manager, addresses) in [
        (
            &worker_a,
            &key_manager.public_key,
            &["10.242.0.20/32", "10.242.0.40/32"][..],
        ),
        (
            &worker_b,
            &key_manager.public_key,
            &["10.242.0.10/32", "10.242.0.40/32"][..],
        ),
    ] {
        in_ns(
            namespace,
            "wg",
            &["set", WG_INTERFACE_NAME, "peer", old_manager, "remove"],
        )?;
        assigned_peer(
            namespace,
            &key_standby.public_key,
            Some("198.18.0.2:51822"),
            addresses,
            25,
        )?;
        for address in addresses {
            if *address != "10.242.0.40/32" {
                ip_ns(
                    namespace,
                    &["-4", "route", "replace", address, "dev", WG_INTERFACE_NAME],
                )?;
            }
        }
    }
    ping(&task_a, "10.242.0.20", None)
        .map_err(|error| format!("standby manager did not take over relay: {error}"))?;
    ping(&task_b, "10.242.0.10", None)
        .map_err(|error| format!("standby manager did not relay return traffic: {error}"))?;
    assert!(
        failover_started.elapsed() < Duration::from_secs(5),
        "relay did not recover within one handshake"
    );

    Ok(())
}
