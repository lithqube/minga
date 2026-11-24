//! A manager started with `--peer` but no `--wg-endpoint` should exit non-zero
//! with an error that names `--peer` as the trigger and says a single host
//! should start without it (issue #199, AC4).

use std::process::{Command, Stdio};

fn run_manager_with_peer() -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_minga"))
        .args([
            "manager",
            "--node-id",
            "1",
            "--runtime",
            "mock",
            "--bind",
            "127.0.0.1:59900",
            // Bind API to loopback to bypass the "no-token / no-TLS" check that
            // would otherwise fire before the --peer / --wg-endpoint check.
            "--api-bind",
            "127.0.0.1:2375",
            "--peer",
            "2=127.0.0.1:59901",
            // intentionally omit --wg-endpoint
        ])
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
fn manager_with_peer_and_no_wg_endpoint_names_peer_in_error() {
    let (ok, out) = run_manager_with_peer();
    assert!(!ok, "should exit non-zero:\n{out}");
    assert!(
        out.contains("--peer"),
        "error must name --peer as the trigger:\n{out}"
    );
    assert!(
        out.contains("single host") || out.contains("without --peer"),
        "error must say a single host should start without --peer:\n{out}"
    );
}
