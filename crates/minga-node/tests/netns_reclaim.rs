//! Task namespaces without a state file at node start (issue #204).
//!
//! Own test binary: recovery looks at every `minga-*` namespace on the host,
//! so it must not overlap the namespaces other netns tests create.
//!
//! Needs root, iproute2 and iptables. Skips when it cannot run, unless
//! `MINGA_REQUIRE_NETNS` is set (CI sets it).
//! To run: `sudo -E env "PATH=$PATH" cargo test -p minga-node --test netns_reclaim`

#![cfg(target_os = "linux")]

use std::process::{Command, Stdio};

use minga_node::netns::NetworkSetup;

fn ip(args: &[&str]) -> bool {
    Command::new("ip")
        .args(args)
        .status()
        .is_ok_and(|status| status.success())
}

fn namespace_exists(name: &str) -> bool {
    let listed = Command::new("ip").args(["netns", "list"]).output().unwrap();
    String::from_utf8_lossy(&listed.stdout)
        .lines()
        .any(|line| line.split_whitespace().next() == Some(name))
}

fn unavailable() -> Option<String> {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        return Some("not running as root".into());
    }
    for tool in ["ip", "iptables"] {
        if Command::new(tool).arg("-V").output().is_err() {
            return Some(format!("{tool} is not installed"));
        }
    }
    None
}

#[tokio::test]
async fn namespaces_without_state_are_removed_unless_a_process_runs_in_them() {
    if let Some(why) = unavailable() {
        assert!(
            std::env::var_os("MINGA_REQUIRE_NETNS").is_none(),
            "required netns test cannot run: {why}"
        );
        eprintln!("SKIP: {why}");
        return;
    }
    let (idle, busy) = ("minga-reclaim-idle", "minga-reclaim-busy");
    assert!(ip(&["netns", "add", idle]));
    assert!(ip(&["netns", "add", busy]));
    let mut holder = Command::new("ip")
        .args(["netns", "exec", busy, "sleep", "60"])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    // `ip netns pids` sees the process once it has entered the namespace.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let state = tempfile::tempdir().unwrap();
    let setup = NetworkSetup::new(state.path());
    let refused = setup.reconcile_task_source_filters().await;

    // The unused namespace is gone either way; the occupied one blocks
    // recovery and keeps its name.
    let error = refused.expect_err("a namespace in use must refuse recovery");
    assert!(error.to_string().contains(busy), "{error}");
    assert!(error.to_string().contains("ip netns delete"), "{error}");
    assert!(!namespace_exists(idle), "idle namespace was not removed");
    assert!(namespace_exists(busy), "busy namespace must be kept");

    let _ = holder.kill();
    let _ = holder.wait();
    setup
        .reconcile_task_source_filters()
        .await
        .expect("recovery succeeds once nothing runs in the namespace");
    assert!(!namespace_exists(busy), "freed namespace was not removed");

    // No state directory at all takes the other recovery path.
    let nodir = "minga-reclaim-nodir";
    assert!(ip(&["netns", "add", nodir]));
    let missing = NetworkSetup::new(state.path().join("never-created"));
    missing.reconcile_task_source_filters().await.unwrap();
    assert!(!namespace_exists(nodir), "stale namespace was not removed");
}

#[tokio::test]
async fn preflight_accepts_this_kernel_and_leaves_no_scratch_chain() {
    if unavailable().is_some() {
        return;
    }
    minga_node::netns::preflight_netfilter().await.unwrap();
    for table in ["filter", "nat"] {
        let listed = Command::new("iptables")
            .args(["-w", "5", "-t", table, "-S", "MINGA_PREFLIGHT"])
            .output()
            .unwrap();
        assert!(!listed.status.success(), "scratch chain left in {table}");
    }
}
