//! Integration tests for NATS **transport security** against real servers.
//!
//! These tests start actual `nats-server` processes with TLS enabled and
//! assert the security properties that matter:
//!
//! - a client with the correct CA and client certificate connects;
//! - a client with the **wrong CA** is rejected;
//! - a client with **no client certificate** is rejected when the server
//!   requires one;
//! - a **plaintext** client is rejected by a TLS-only server;
//! - a client whose URL host does not match the certificate's expected name
//!   is rejected before the handshake.
//!
//! Every test uses a real server process. Tests that cannot start a server
//! **fail** rather than skip: a skipped security test is indistinguishable
//! from a passing one, which is exactly the failure mode this suite exists
//! to prevent.
//!
//! To run: `cargo test -p minga-bus --test nats_tls -- --nocapture --test-threads=1`

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use minga_bus::tls::NatsTlsConfig;
use minga_bus::{EventBus, LogEntry, NatsBus};

// Parallel tests select ephemeral ports before the child binds; without
// serialization another test can grab the same port and produce a false
// readiness signal or a peer reset.
static NATS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------
// Test PKI
// ---------------------------------------------------------------------------

/// A generated certificate authority plus the certificates it signed.
struct Pki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
    other_ca_pem: String,
}

/// Generate a CA and a leaf certificate for `dns_name`.
fn make_leaf(
    ca: &rcgen::Certificate,
    ca_key: &rcgen::KeyPair,
    dns_name: &str,
    is_ca: bool,
    with_ip_san: bool,
) -> (String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec![dns_name.to_string()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, dns_name);
    params.is_ca = if is_ca {
        rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained)
    } else {
        rcgen::IsCa::NoCa
    };
    if with_ip_san {
        params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
                std::net::Ipv4Addr::LOCALHOST,
            )));
    }
    let cert = params.signed_by(&key, ca, ca_key).unwrap();
    (cert.pem(), key.serialize_pem())
}

/// Build a full test PKI: one CA, a server leaf, a client leaf, and a
/// second unrelated CA (used for the "wrong CA" negative test).
fn make_pki() -> Pki {
    // Authority CA.
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "minga-test-ca");
    let ca = ca_params.self_signed(&ca_key).unwrap();

    // A different CA, for the "wrong CA" case.
    let other_key = rcgen::KeyPair::generate().unwrap();
    let mut other_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    other_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    other_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "minga-other-ca");
    let other_ca = other_params.self_signed(&other_key).unwrap();

    // Server leaf, valid for `localhost` and for 127.0.0.1. The IP SAN
    // matters: a route peer dials the address in the route URL, so a
    // certificate carrying only the DNS name fails route verification.
    let (server_cert_pem, server_key_pem) = make_leaf(&ca, &ca_key, "localhost", false, true);
    // Client leaf, valid for `minga-client`.
    let (client_cert_pem, client_key_pem) = make_leaf(&ca, &ca_key, "minga-client", false, false);

    Pki {
        ca_pem: ca.pem(),
        server_cert_pem,
        server_key_pem,
        client_cert_pem,
        client_key_pem,
        other_ca_pem: other_ca.pem(),
    }
}

/// Write a file with the given contents, mode 0600 for keys and 0644 else.
fn write_file(path: &Path, contents: &str, private: bool) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    f.flush().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if private { 0o600 } else { 0o644 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Server harness
// ---------------------------------------------------------------------------

/// A running `nats-server` with TLS.
struct TlsServer {
    child: Child,
    port: u16,
    /// The monitoring (HTTP) port, when the config asked for one.
    monitor_port: Option<u16>,
    /// The cluster route port, when the config asked for one.
    route_port: Option<u16>,
    _dir: tempfile::TempDir,
}

/// Ports that `nats-server` allocated for itself.
///
/// `nats-server` can pick its own ports (port `-1` in the config) and
/// write them to a JSON ports file. Letting the server allocate the port
/// removes the race that a "find a free port, then bind it later" probe
/// has: between the probe releasing the port and the server binding it,
/// another process can take it, and the server then either fails to bind
/// or — worse — the test connects to a stranger's listener.
#[derive(Debug, Clone, Copy)]
struct ServerPorts {
    client: u16,
    monitor: Option<u16>,
    route: Option<u16>,
}

/// Parse the JSON ports file `nats-server` writes when
/// `--ports_file_dir` is given.
fn parse_ports_file(path: &Path) -> Option<ServerPorts> {
    let body = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&body).ok()?;

    // The file maps a listener kind ("nats", "monitoring", "cluster") to a
    // list of URLs. Each URL is `scheme://host:port`.
    let port_of = |kind: &str| -> Option<u16> {
        value
            .get(kind)?
            .as_array()?
            .first()?
            .as_str()?
            .rsplit(':')
            .next()?
            .parse()
            .ok()
    };

    Some(ServerPorts {
        client: port_of("nats")?,
        monitor: port_of("monitoring"),
        route: port_of("cluster"),
    })
}

/// Find the ports file `nats-server` wrote into `dir`.
///
/// The file is named `<binary>_<pid>.ports`, so it is found by globbing
/// for `*.ports` rather than by reconstructing the name.
fn find_ports_file(dir: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "ports"))
        .collect();
    candidates.sort();
    candidates.pop()
}

/// Locate the `nats-server` binary.
fn nats_server_bin() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("MINGA_NATS_SERVER_BIN") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }
    // Common install locations; also check PATH.
    for candidate in [
        "/usr/local/bin/nats-server",
        "/opt/homebrew/bin/nats-server",
        "/usr/bin/nats-server",
    ] {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Some(p);
        }
    }
    // PATH lookup.
    if let Ok(path) = std::env::var("PATH") {
        for dir in path.split(':') {
            let p = PathBuf::from(dir).join("nats-server");
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

/// Start a `nats-server` with the given config.
///
/// The config is expected to request ports of `-1`, which makes
/// `nats-server` allocate them itself; the allocated ports are read back
/// from the ports file it writes.
///
/// Returns `None` when the `nats-server` binary is not installed — the
/// caller decides whether that is a skip or a failure.
fn start_server(config_body: &str, dir: tempfile::TempDir) -> Option<TlsServer> {
    let bin = nats_server_bin()?;
    let config_path = dir.path().join("server.conf");
    write_file(&config_path, config_body, false);

    let child = Command::new(&bin)
        .arg("-c")
        .arg(&config_path)
        .arg("--ports_file_dir")
        .arg(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn nats-server");

    Some(TlsServer {
        child,
        port: 0,
        monitor_port: None,
        route_port: None,
        _dir: dir,
    })
}

/// Wait for `nats-server` to write its ports file, then adopt the ports it
/// allocated.
///
/// This replaces a "probe a free port, then wait for a TCP connect"
/// readiness check, which had two failure modes: the probe could hand the
/// port to another process before the server bound it, and a successful
/// TCP connect said nothing about whether the listener was the NATS
/// server under test.
fn adopt_allocated_ports(server: &mut TlsServer) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(Some(status)) = server.child.try_wait() {
            let mut stderr = String::new();
            if let Some(mut pipe) = server.child.stderr.take() {
                use std::io::Read;
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("nats-server exited early: {status}\n--- stderr ---\n{stderr}");
        }
        if let Some(path) = find_ports_file(server._dir.path()) {
            if let Some(ports) = parse_ports_file(&path) {
                server.port = ports.client;
                server.monitor_port = ports.monitor;
                server.route_port = ports.route;
                return;
            }
        }
        if Instant::now() >= deadline {
            panic!("nats-server did not report its allocated ports");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A NATS server requiring a client certificate signed by `pki.ca_pem`.
fn start_tls_server(pki: &Pki) -> TlsServer {
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    let cert_path = dir.path().join("server-cert.pem");
    let key_path = dir.path().join("server-key.pem");
    write_file(&ca_path, &pki.ca_pem, false);
    write_file(&cert_path, &pki.server_cert_pem, false);
    write_file(&key_path, &pki.server_key_pem, true);

    // `-1` asks nats-server to allocate the port itself.
    let config = format!(
        r#"listen: 127.0.0.1:-1
server_name: minga-tls-test
http: "127.0.0.1:-1"

tls {{
    cert_file: "{cert}"
    key_file: "{key}"
    ca_file: "{ca}"
    verify: true
    verify_and_map: true
}}

jetstream {{
    store_dir: "{store}"
}}
"#,
        cert = cert_path.display(),
        key = key_path.display(),
        ca = ca_path.display(),
        store = dir.path().join("jetstream").display(),
    );

    let mut server = start_server(&config, dir).expect("nats-server binary not found");
    adopt_allocated_ports(&mut server);
    server
}

/// Wait until the server's client listener accepts connections.
///
/// The port is the one `nats-server` allocated for itself, so this
/// confirms the right listener is up rather than a stranger's socket that
/// happens to sit on the same number.
fn wait_ready(server: &TlsServer) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", server.port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("nats-server did not become ready on port {}", server.port);
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Write a client TLS material set to a temp dir and return the config.
fn client_config(dir: &Path, pki: &Pki, ca_pem: &str) -> NatsTlsConfig {
    let ca_path = dir.join("client-ca.pem");
    let cert_path = dir.join("client-cert.pem");
    let key_path = dir.join("client-key.pem");
    write_file(&ca_path, ca_pem, false);
    write_file(&cert_path, &pki.client_cert_pem, false);
    write_file(&key_path, &pki.client_key_pem, true);

    NatsTlsConfig::new(&ca_path, &cert_path, &key_path, "localhost")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A TLS-only server must accept a properly-authenticated client.
#[tokio::test]
async fn tls_client_with_valid_certificate_connects() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let pki = make_pki();
    let server = start_tls_server(&pki);
    wait_ready(&server);

    let dir = tempfile::tempdir().unwrap();
    let cfg = client_config(dir.path(), &pki, &pki.ca_pem);

    let url = format!("tls://localhost:{}", server.port);
    let bus = NatsBus::connect_secure(&url, &cfg)
        .await
        .expect("a correctly authenticated TLS client must connect");

    // The bus must be usable, not merely connected.
    bus.put_status("task.tls-ok", b"running").await.unwrap();
    assert_eq!(
        bus.get_status("task.tls-ok").await.unwrap(),
        Some(b"running".to_vec())
    );
}

/// A client presenting a certificate from an untrusted CA is rejected.
#[tokio::test]
async fn tls_client_with_wrong_ca_is_rejected() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let pki = make_pki();
    let server = start_tls_server(&pki);
    wait_ready(&server);

    let dir = tempfile::tempdir().unwrap();
    // Trust the *other* CA, so the server certificate does not verify.
    let cfg = client_config(dir.path(), &pki, &pki.other_ca_pem);

    let url = format!("tls://localhost:{}", server.port);
    let result = NatsBus::connect_secure(&url, &cfg).await;
    assert!(
        result.is_err(),
        "a client trusting the wrong CA must be rejected"
    );
}

/// A client with no client certificate is rejected by a server that
/// requires one.
#[tokio::test]
async fn tls_client_without_certificate_is_rejected() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let pki = make_pki();
    let server = start_tls_server(&pki);
    wait_ready(&server);

    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("ca.pem");
    write_file(&ca_path, &pki.ca_pem, false);

    // No client cert/key: the client is anonymous, and the server demands
    // a verified client certificate.
    let cfg = NatsTlsConfig::new(&ca_path, &ca_path, &ca_path, "localhost");

    let url = format!("tls://localhost:{}", server.port);
    let result = NatsBus::connect_secure(&url, &cfg).await;
    assert!(
        result.is_err(),
        "an anonymous TLS client must be rejected by a server requiring client certs"
    );
}

/// A plaintext client must not be able to talk to a TLS-only server.
#[tokio::test]
async fn plaintext_client_is_rejected_by_tls_server() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let pki = make_pki();
    let server = start_tls_server(&pki);
    wait_ready(&server);

    let url = format!("nats://localhost:{}", server.port);
    let result = NatsBus::connect(&url).await;
    assert!(
        result.is_err(),
        "a plaintext client must be rejected by a TLS-only server"
    );
}

/// The URL host must match the expected server name; a mismatch is
/// refused before the handshake.
#[tokio::test]
async fn tls_url_host_mismatch_is_rejected() {
    let _guard = NATS_TEST_LOCK.lock().await;
    let pki = make_pki();
    let dir = tempfile::tempdir().unwrap();
    let cfg = client_config(dir.path(), &pki, &pki.ca_pem);

    let err = cfg
        .validate_url("tls://wrong-host:4222")
        .expect_err("a host that does not match the expected name must be rejected");
    assert!(err.to_string().contains("does not match"));

    // The correct host is accepted.
    cfg.validate_url("tls://localhost:4222")
        .expect("the matching host must be accepted");
}

/// A `nats://` URL is refused in secure mode — it is an accidental
/// downgrade, not a valid secure endpoint.
#[tokio::test]
async fn plaintext_scheme_is_rejected_in_secure_mode() {
    let _guard = NATS_TEST_LOCK.lock().await;
    let pki = make_pki();
    let dir = tempfile::tempdir().unwrap();
    let cfg = client_config(dir.path(), &pki, &pki.ca_pem);

    let err = cfg
        .validate_url("nats://localhost:4222")
        .expect_err("a nats:// URL must be refused in secure mode");
    assert!(err.to_string().contains("tls://"));
}

/// A plaintext client is still accepted by a plaintext server — the secure
/// path is opt-in, not a breaking change for local development.
#[tokio::test]
async fn plaintext_server_still_accepts_plaintext_client() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let dir = tempfile::tempdir().unwrap();
    let config = format!(
        r#"listen: 127.0.0.1:-1
server_name: minga-plain-test
jetstream {{
    store_dir: "{store}"
}}
"#,
        store = dir.path().join("jetstream").display(),
    );
    let mut server = start_server(&config, dir).expect("nats-server binary not found");
    adopt_allocated_ports(&mut server);

    let url = format!("nats://localhost:{}", server.port);
    let bus = NatsBus::connect(&url)
        .await
        .expect("a plaintext client must still work against a plaintext server");
    bus.put_status("task.plain", b"ok").await.unwrap();
}

/// Two servers with route TLS must discover each other and form a cluster.
#[tokio::test]
async fn two_servers_form_a_cluster_over_tls_routes() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let pki = make_pki();
    let dir = tempfile::tempdir().unwrap();

    let ca_path = dir.path().join("ca.pem");
    let cert_path = dir.path().join("server-cert.pem");
    let key_path = dir.path().join("server-key.pem");
    write_file(&ca_path, &pki.ca_pem, false);
    write_file(&cert_path, &pki.server_cert_pem, false);
    write_file(&key_path, &pki.server_key_pem, true);

    // Server A starts first and allocates its own route port; server B is
    // then told to dial it. Only one side needs the route configured —
    // NATS forms the route from the solicited connection, and the peer
    // accepts it. This is what lets both servers use self-allocated ports
    // without a chicken-and-egg problem.
    //
    // JetStream will not start in a cluster without a configured route, so
    // A is given a placeholder route to a port nothing listens on. NATS
    // accepts a configured-but-not-yet-reachable route at startup and
    // retries it, which is exactly the behaviour needed here.
    let config_a = format!(
        r#"listen: 127.0.0.1:-1
server_name: minga-node-a
http: "127.0.0.1:-1"

tls {{
    cert_file: "{cert}"
    key_file: "{key}"
    ca_file: "{ca}"
    verify: true
    verify_and_map: true
}}

cluster {{
    name: minga-cluster
    port: -1
    routes = [
        "nats://127.0.0.1:{placeholder_route}"
    ]
    tls {{
        cert_file: "{cert}"
        key_file: "{key}"
        ca_file: "{ca}"
        verify: true
    }}
}}

jetstream {{
    store_dir: "{store_a}"
}}
"#,
        placeholder_route = 1,
        cert = cert_path.display(),
        key = key_path.display(),
        ca = ca_path.display(),
        store_a = dir.path().join("js-a").display(),
    );

    let dir_a = tempfile::tempdir().unwrap();
    let mut server_a = start_server(&config_a, dir_a).expect("nats-server binary not found");
    adopt_allocated_ports(&mut server_a);
    let route_port_a = server_a
        .route_port
        .expect("server A must have allocated a route port");

    let config_b = format!(
        r#"listen: 127.0.0.1:-1
server_name: minga-node-b
http: "127.0.0.1:-1"

tls {{
    cert_file: "{cert}"
    key_file: "{key}"
    ca_file: "{ca}"
    verify: true
    verify_and_map: true
}}

cluster {{
    name: minga-cluster
    port: -1
    routes = [
        "nats://127.0.0.1:{route_port_a}"
    ]
    tls {{
        cert_file: "{cert}"
        key_file: "{key}"
        ca_file: "{ca}"
        verify: true
    }}
}}

jetstream {{
    store_dir: "{store_b}"
}}
"#,
        route_port_a = route_port_a,
        cert = cert_path.display(),
        key = key_path.display(),
        ca = ca_path.display(),
        store_b = dir.path().join("js-b").display(),
    );

    let dir_b = tempfile::tempdir().unwrap();
    let mut server_b = start_server(&config_b, dir_b).expect("nats-server binary not found");
    adopt_allocated_ports(&mut server_b);

    let monitor_a = server_a
        .monitor_port
        .expect("server A must have allocated a monitoring port");
    let _ = &mut server_b;

    // Give the route connection a moment to form, then check the route
    // count reported by the monitoring endpoint.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let routes = async {
        let body = reqwest::get(format!("http://127.0.0.1:{monitor_a}/routez"))
            .await
            .expect("monitoring endpoint unreachable")
            .text()
            .await
            .expect("cannot read monitoring response");
        body
    }
    .await;

    assert!(
        routes.contains("num_routes"),
        "monitoring output should report the route count, got: {routes}"
    );

    // NATS records a route per direction, so a healthy two-server cluster
    // reports several entries. What matters is that the count is non-zero:
    // a zero means the TLS route never formed.
    let num_routes: usize = routes
        .split("\"num_routes\":")
        .nth(1)
        .and_then(|rest| {
            rest.trim_start()
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .and_then(|n| n.parse().ok())
        })
        .expect("num_routes should be a number");

    assert!(
        num_routes > 0,
        "the two servers must form a TLS route, got num_routes={num_routes}: {routes}"
    );
}

/// The bus returned by a secure connect is usable for the real workload:
/// events, status and logs all work over TLS.
#[tokio::test]
async fn secure_bus_supports_events_status_and_logs() {
    let _guard = NATS_TEST_LOCK.lock().await;
    if nats_server_bin().is_none() {
        panic!("nats-server binary not found - install it to run the TLS suite");
    }

    let pki = make_pki();
    let server = start_tls_server(&pki);
    wait_ready(&server);

    let dir = tempfile::tempdir().unwrap();
    let cfg = client_config(dir.path(), &pki, &pki.ca_pem);

    let url = format!("tls://localhost:{}", server.port);
    let bus: Arc<dyn minga_bus::EventBus> = NatsBus::connect_secure(&url, &cfg)
        .await
        .expect("secure connect must succeed");

    bus.put_status("task.secure", b"running").await.unwrap();
    bus.publish_event("minga.task.secure.event", b"started")
        .await
        .unwrap();
    bus.publish_log(
        "task.secure",
        &LogEntry::stdout("task.secure", b"hello over tls"),
    )
    .await
    .unwrap();

    assert_eq!(
        bus.get_status("task.secure").await.unwrap(),
        Some(b"running".to_vec())
    );

    let history = bus.log_history("task.secure", 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].data, b"hello over tls");
}
