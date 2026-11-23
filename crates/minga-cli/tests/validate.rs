//! `minga validate` accepts a port that sets `host_ip` (issue #162) and refuses
//! one whose `host_ip` is not an IP address.

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
fn validate_accepts_port_host_ip_short_syntax() {
    let (ok, out) = run_validate(
        "services:\n  db: {image: db, ports: ['127.0.0.1:5432:5432', '[::1]:5433:5433']}\n",
    );
    assert!(ok, "validate should succeed:\n{out}");
}

#[test]
fn validate_accepts_port_host_ip_long_syntax() {
    let (ok, out) = run_validate(
        "services:\n  db:\n    image: db\n    ports:\n      - {target: 5432, published: 5432, host_ip: 127.0.0.1}\n",
    );
    assert!(ok, "validate should succeed:\n{out}");
}

#[test]
fn validate_rejects_port_host_ip_that_is_not_an_ip_address() {
    let (ok, out) = run_validate("services:\n  db: {image: db, ports: ['localhost:5432:5432']}\n");
    assert!(!ok, "validate should fail:\n{out}");
    assert!(
        out.contains("is not an IP address"),
        "unexpected output:\n{out}"
    );
}

#[test]
fn validate_accepts_ports_without_host_ip() {
    let (ok, out) = run_validate("services:\n  db: {image: db, ports: ['5432:5432', 8080]}\n");
    assert!(ok, "validate should succeed:\n{out}");
}
