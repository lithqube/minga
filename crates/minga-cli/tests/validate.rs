//! `minga validate` fails loudly when a port sets `host_ip` (issue #135):
//! the host IP cannot reach desired state yet, so loading must refuse it
//! instead of silently publishing the port on every address.

use std::io::Write;
use std::process::{Command, Stdio};

fn run_validate(compose: &str) -> (bool, String) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(compose.as_bytes()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_minga"))
        .args(["validate", "-c", file.path().to_str().unwrap()])
        .stdin(Stdio::null())
        .output()
        .expect("minga binary should run");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

#[test]
fn validate_rejects_port_host_ip_short_syntax() {
    let (ok, out) = run_validate("services:\n  db: {image: db, ports: ['127.0.0.1:5432:5432']}\n");
    assert!(!ok, "validate should fail:\n{out}");
    assert!(
        out.contains(
            "services.db.ports[0]: `host_ip` is part of the Compose spec but not supported yet"
        ),
        "unexpected output:\n{out}"
    );
}

#[test]
fn validate_rejects_port_host_ip_long_syntax() {
    let (ok, out) = run_validate(
        "services:\n  db:\n    image: db\n    ports:\n      - {target: 5432, published: 5432, host_ip: 127.0.0.1}\n",
    );
    assert!(!ok, "validate should fail:\n{out}");
    assert!(
        out.contains(
            "services.db.ports[0]: `host_ip` is part of the Compose spec but not supported yet"
        ),
        "unexpected output:\n{out}"
    );
}

#[test]
fn validate_accepts_ports_without_host_ip() {
    let (ok, out) = run_validate("services:\n  db: {image: db, ports: ['5432:5432', 8080]}\n");
    assert!(ok, "validate should succeed:\n{out}");
}
