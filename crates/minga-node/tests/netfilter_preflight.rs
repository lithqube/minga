//! Start-time netfilter check (issue #204), against a stand-in `iptables`
//! so no root or particular kernel is needed. Linux only: the module is.

#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;

use minga_node::netns::{preflight_netfilter, NetError};

/// A fake `iptables` that logs every call and rejects `FAIL_ON`'s match the
/// way `iptables-nft` rejects one the kernel lacks.
fn install_fake_iptables(dir: &std::path::Path, fail_on: &str) {
    let script = format!(
        "#!/bin/sh\n\
         echo \"$@\" >> '{log}'\n\
         case \"$*\" in\n\
           -V) echo 'iptables v1.8.9 (nf_tables)'; exit 0;;\n\
           *'-m {fail_on} '*) echo 'iptables: Incompatible with this kernel.' >&2; exit 4;;\n\
         esac\n\
         exit 0\n",
        log = dir.join("calls").display(),
    );
    let path = dir.join("iptables");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn unsupported_match_fails_start_naming_it_and_leaves_no_scratch_chain() {
    let dir = tempfile::tempdir().unwrap();
    install_fake_iptables(dir.path(), "physdev");
    let path = format!(
        "{}:{}",
        dir.path().display(),
        std::env::var("PATH").unwrap()
    );
    // SAFETY: the only test of this binary, so no other thread reads the
    // environment while it is set.
    unsafe { std::env::set_var("PATH", path) };

    let error = preflight_netfilter()
        .await
        .expect_err("physdev is rejected");
    let NetError::Netfilter(failure) = &error else {
        panic!("wrong error: {error}");
    };
    let text = failure.to_string();
    for needle in [
        "`physdev`",
        "xt_physdev",
        "nft_compat",
        "iptables v1.8.9 (nf_tables)",
        "Incompatible with this kernel",
        "-m physdev --physdev-in",
    ] {
        assert!(text.contains(needle), "missing {needle:?} in: {text}");
    }

    // The scratch chain is flushed and deleted even though the probe failed.
    let calls = std::fs::read_to_string(dir.path().join("calls")).unwrap();
    let last: Vec<&str> = calls.lines().rev().take(2).collect();
    assert!(last[0].contains("-X MINGA_PREFLIGHT"), "{calls}");
    assert!(last[1].contains("-F MINGA_PREFLIGHT"), "{calls}");

    // A kernel with every match passes.
    install_fake_iptables(dir.path(), "no-such-match");
    preflight_netfilter().await.expect("all matches accepted");
}
