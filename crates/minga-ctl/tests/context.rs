//! End-to-end test for `minga-ctl context`.
//!
//! Drives the built binary against a temporary config directory. No API
//! call is made: the context group is local file work only.

use std::path::Path;
use std::process::Command;

/// Runs `minga-ctl` with a temporary config directory.
fn run(dir: &Path, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_minga-ctl"))
        .args(args)
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("HOME", dir.join("home"))
        .env_remove("MINGA_CTL_CONTEXT")
        .env_remove("MINGA_CTL_ENDPOINT")
        .env_remove("MINGA_CTL_TOKEN")
        .output()
        .expect("minga-ctl should run");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// The config file the binary wrote.
fn config_file(dir: &Path) -> std::path::PathBuf {
    dir.join("config").join("minga-ctl").join("config.yaml")
}

#[test]
fn adding_a_context_writes_a_private_config() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, out, err) = run(
        dir.path(),
        &[
            "context",
            "add",
            "prod",
            "--ssh",
            "alice@mgr1.example.net",
            "--remote-token-file",
            "/etc/minga/api-token",
        ],
    );
    assert!(ok, "stderr: {err}");
    assert!(out.contains("context `prod` added and selected"), "{out}");

    let path = config_file(dir.path());
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("alice@mgr1.example.net"), "{text}");

    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "config mode was {mode:04o}");
}

#[test]
fn the_config_directory_is_private() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");

    use std::os::unix::fs::PermissionsExt;
    let path = dir.path().join("config").join("minga-ctl");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700, "config dir mode was {mode:04o}");
}

#[test]
fn listing_contexts_shows_the_current_one() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(
        dir.path(),
        &["context", "add", "prod", "--ssh", "alice@mgr1.example.net"],
    );
    assert!(ok, "stderr: {err}");
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");

    let (ok, out, err) = run(dir.path(), &["context", "list"]);
    assert!(ok, "stderr: {err}");
    assert!(out.contains("CURRENT"), "{out}");
    assert!(out.contains("prod"), "{out}");
    assert!(out.contains("local"), "{out}");
    assert!(out.contains("ssh alice@mgr1.example.net"), "{out}");
    assert!(out.contains("direct"), "{out}");
}

#[test]
fn json_output_is_an_array() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");

    let (ok, out, err) = run(dir.path(), &["--json", "context", "list"]);
    assert!(ok, "stderr: {err}");
    let parsed: serde_json::Value = serde_json::from_str(&out).expect("valid JSON");
    let rows = parsed.as_array().expect("an array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "local");
    assert_eq!(rows[0]["current"], true);
    assert_eq!(rows[0]["transport"], "direct");
}

#[test]
fn a_plain_http_remote_endpoint_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(
        dir.path(),
        &[
            "context",
            "add",
            "remote",
            "--endpoint",
            "http://10.0.0.1:2375",
        ],
    );
    assert!(!ok, "a remote plain-HTTP endpoint must be refused");
    assert!(err.contains("refusing plain HTTP to 10.0.0.1"), "{err}");
}

#[test]
fn adding_a_context_twice_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(!ok);
    assert!(err.contains("already exists"), "{err}");
}

#[test]
fn deleting_the_current_context_clears_the_selection() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");
    let (ok, _, err) = run(dir.path(), &["context", "delete", "local"]);
    assert!(ok, "stderr: {err}");

    let (ok, out, err) = run(dir.path(), &["--json", "context", "list"]);
    assert!(ok, "stderr: {err}");
    let parsed: serde_json::Value = serde_json::from_str(&out).expect("valid JSON");
    assert!(parsed.as_array().expect("an array").is_empty());
}

#[test]
fn deleting_an_unknown_context_exits_three() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "delete", "nope"]);
    assert!(!ok);
    assert!(err.contains("not found"), "{err}");
}

#[test]
fn a_group_readable_config_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");

    use std::os::unix::fs::PermissionsExt;
    let path = config_file(dir.path());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

    let (ok, _, err) = run(dir.path(), &["context", "list"]);
    assert!(!ok, "a group-readable config must be refused");
    assert!(err.contains("readable by group or others"), "{err}");
    assert!(err.contains("mode 0644"), "{err}");
}

#[test]
fn an_unknown_config_key_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");

    let path = config_file(dir.path());
    let text = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{text}api_token: hunter2\n")).unwrap();

    let (ok, _, err) = run(dir.path(), &["context", "list"]);
    assert!(!ok, "an unknown config key must be refused");
    assert!(err.contains("unknown field `api_token`"), "{err}");
}

#[test]
fn quiet_output_prints_ids_only() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, _, err) = run(dir.path(), &["context", "add", "local"]);
    assert!(ok, "stderr: {err}");
    let (ok, out, err) = run(dir.path(), &["--quiet", "context", "list"]);
    assert!(ok, "stderr: {err}");
    assert_eq!(out.trim(), "local");
}

#[test]
fn quiet_changes_print_nothing() {
    let dir = tempfile::tempdir().unwrap();
    for args in [
        &["--quiet", "context", "add", "local"][..],
        &["--quiet", "context", "add", "other"][..],
        &["--quiet", "context", "use", "other"][..],
        &["--quiet", "context", "delete", "local"][..],
    ] {
        let (ok, out, err) = run(dir.path(), args);
        assert!(ok, "{args:?}: {err}");
        assert!(out.is_empty(), "{args:?} printed {out:?}");
    }
}

#[test]
fn concurrent_adds_keep_every_context() {
    let dir = tempfile::tempdir().unwrap();
    let names: Vec<String> = (0..8).map(|i| format!("ctx{i}")).collect();
    std::thread::scope(|scope| {
        for name in &names {
            let dir = dir.path();
            scope.spawn(move || {
                let (ok, _, err) = run(dir, &["context", "add", name]);
                assert!(ok, "{name}: {err}");
            });
        }
    });
    let (ok, out, err) = run(dir.path(), &["--quiet", "context", "list"]);
    assert!(ok, "stderr: {err}");
    let mut listed: Vec<&str> = out.lines().collect();
    listed.sort_unstable();
    assert_eq!(listed, names, "a concurrent add lost a context");
    let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("config").join("minga-ctl"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains(".tmp."))
        .collect();
    assert!(leftovers.is_empty(), "temp files left: {leftovers:?}");
}
