//! `registry login`, `logout` and `list` (plan sections 5.8 and 6).
//!
//! Most tests use a local context whose `registry_auth_file` is in a temp
//! dir. The ssh test puts a stub `ssh` first on `PATH` that records its
//! arguments and stdin, then runs the command the way a remote shell would
//! (`eval` of the joined arguments). No real SSH.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde_json::{json, Value};

const SECRET: &str = "s3cr3t-Pull-Token";

const STUB: &str = r#"#!/bin/sh
printf '%s\n' "$@" >> "$STUB_DIR/ssh.args"
[ "$1" = "-T" ] && [ "$2" = "--" ] || exit 255
shift 3
tee -a "$STUB_DIR/ssh.stdin" | eval "$*"
"#;

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    /// A local context `local` whose credential file is `docker/config.json`.
    fn local() -> Env {
        let env = Env {
            dir: tempfile::tempdir().unwrap(),
        };
        let file = env.file();
        let (code, _, err) = env.ctl(
            &[
                "context",
                "add",
                "local",
                "--registry-auth-file",
                file.to_str().unwrap(),
                "--use",
            ],
            "",
        );
        assert_eq!(code, 0, "context add: {err}");
        env
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn file(&self) -> PathBuf {
        self.path("docker/config.json")
    }

    /// Runs minga-ctl with `stdin` piped in (so never a terminal).
    fn ctl(&self, args: &[&str], stdin: &str) -> (i32, String, String) {
        use std::io::Write;
        let path = format!(
            "{}:{}",
            self.path("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = Command::new(env!("CARGO_BIN_EXE_minga-ctl"))
            .args(args)
            .env("PATH", path)
            .env("STUB_DIR", self.dir.path())
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env("XDG_STATE_HOME", self.path("state"))
            .env("HOME", self.path("home"))
            .env_remove("MINGA_CTL_CONTEXT")
            .env_remove("MINGA_CTL_ENDPOINT")
            .env_remove("MINGA_CTL_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // A command that never reads stdin may close it first.
        let _ = child.stdin.take().unwrap().write_all(stdin.as_bytes());
        let out = child.wait_with_output().unwrap();
        (
            out.status.code().unwrap(),
            String::from_utf8_lossy(&out.stdout).to_string(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    }

    fn login(&self, registry: &str, user: &str) -> (i32, String, String) {
        self.ctl(
            &["registry", "login", registry, "--username", user],
            &format!("{SECRET}\n"),
        )
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.file()).unwrap()).unwrap()
    }

    fn write(&self, text: &str, mode: u32) {
        std::fs::create_dir_all(self.file().parent().unwrap()).unwrap();
        std::fs::write(self.file(), text).unwrap();
        std::fs::set_permissions(self.file(), std::fs::Permissions::from_mode(mode)).unwrap();
    }

    fn mode(&self) -> u32 {
        std::fs::metadata(self.file()).unwrap().permissions().mode() & 0o777
    }

    fn audit(&self) -> String {
        std::fs::read_to_string(self.path("state/minga-ctl/audit.jsonl")).unwrap()
    }
}

fn auth(user: &str) -> String {
    BASE64.encode(format!("{user}:{SECRET}"))
}

#[test]
fn login_ghcr_creates_the_file_0600_with_the_auth_entry() {
    let env = Env::local();
    let (code, out, err) = env.login("ghcr", "acme-deploy");
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("stored"), "{out}");
    assert!(err.contains("read:packages"), "{err}");
    assert!(err.contains("note: created"), "{err}");
    assert_eq!(env.mode(), 0o600);
    assert_eq!(env.json()["auths"]["ghcr.io"]["auth"], auth("acme-deploy"));

    let audit = env.audit();
    assert!(audit.contains(r#""command":"registry login""#), "{audit}");
    assert!(audit.contains(r#""username":"acme-deploy""#), "{audit}");
    assert!(!audit.contains(SECRET) && !audit.contains(&auth("acme-deploy")));

    // The context names its file, so there is no default-path warning, and
    // the JSON result says which file was written.
    assert!(!err.contains("the default"), "{err}");
    let (code, out, err) = env.ctl(
        &[
            "--json",
            "registry",
            "login",
            "ghcr",
            "--username",
            "acme-deploy",
        ],
        &format!("{SECRET}\n"),
    );
    assert_eq!(code, 0, "{err}");
    let rows: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(rows[0]["file"], env.file().to_str().unwrap(), "{out}");
}

#[test]
fn a_failing_api_token_command_does_not_block_registry_commands() {
    let env = Env::local();
    // A second context whose token command always fails; registry commands
    // never call the API, so they must not run it.
    let file = env.file();
    let (code, _, err) = env.ctl(
        &[
            "context",
            "add",
            "broken-token",
            "--token-command",
            "false",
            "--registry-auth-file",
            file.to_str().unwrap(),
            "--use",
        ],
        "",
    );
    assert_eq!(code, 0, "context add: {err}");
    let (code, out, err) = env.login("ghcr", "acme-deploy");
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("stored"), "{out}");
    let (code, _, err) = env.ctl(&["registry", "list"], "");
    assert_eq!(code, 0, "{err}");
    // An API command in the same context still runs it, and fails.
    let (code, _, err) = env.ctl(&["stack", "ls"], "");
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("token command"), "{err}");
}

#[test]
fn other_hosts_and_unknown_keys_survive_and_logout_removes_only_its_host() {
    let env = Env::local();
    let before = json!({
        "auths": { "quay.io": { "auth": "cTp4" } },
        "credsStore": "desktop",
        "HttpHeaders": { "User-Agent": "x" }
    });
    env.write(&before.to_string(), 0o600);

    let (code, _, err) = env.login("ghcr.io", "acme-deploy");
    assert_eq!(code, 0, "{err}");
    let after = env.json();
    assert_eq!(after["auths"]["quay.io"], before["auths"]["quay.io"]);
    assert_eq!(after["credsStore"], "desktop");
    assert_eq!(after["HttpHeaders"], before["HttpHeaders"]);
    assert_eq!(after["auths"]["ghcr.io"]["auth"], auth("acme-deploy"));

    let (code, out, err) = env.ctl(&["registry", "logout", "ghcr", "--yes"], "");
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("removed"), "{out}");
    assert_eq!(env.json(), before);

    let (code, out, _) = env.ctl(&["registry", "logout", "ghcr", "--yes"], "");
    assert_eq!(code, 0);
    assert!(out.contains("not logged in"), "{out}");
    assert!(env.audit().contains(r#""command":"registry logout""#));
}

#[test]
fn logout_without_a_terminal_needs_yes() {
    let env = Env::local();
    env.login("ghcr", "acme-deploy");
    let (code, _, err) = env.ctl(&["registry", "logout", "ghcr"], "");
    assert_eq!(code, 1);
    assert!(err.contains("pass --yes"), "{err}");
    assert!(env.json()["auths"]["ghcr.io"].is_object());
}

#[test]
fn logout_dockerhub_also_removes_the_docker_login_key() {
    let env = Env::local();
    env.write(
        r#"{"auths":{"https://index.docker.io/v1/":{"auth":"aDp0"},"quay.io":{}}}"#,
        0o600,
    );
    let (code, _, err) = env.ctl(&["registry", "logout", "dockerhub", "--yes"], "");
    assert_eq!(code, 0, "{err}");
    assert_eq!(env.json(), json!({ "auths": { "quay.io": {} } }));
}

#[test]
fn a_google_registry_stores_the_json_key_user() {
    let env = Env::local();
    let key = env.path("pull-sa.json");
    std::fs::write(&key, SECRET).unwrap();
    let (code, _, err) = env.ctl(
        &[
            "registry",
            "login",
            "europe-west1-docker.pkg.dev",
            "--key-file",
            key.to_str().unwrap(),
        ],
        "",
    );
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        env.json()["auths"]["europe-west1-docker.pkg.dev"]["auth"],
        auth("_json_key")
    );
}

#[test]
fn a_file_that_is_not_json_is_left_unchanged_with_exit_2() {
    let env = Env::local();
    env.write("not json {", 0o600);
    let (code, _, err) = env.login("ghcr", "acme-deploy");
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("is not valid JSON; left unchanged"), "{err}");
    assert_eq!(std::fs::read_to_string(env.file()).unwrap(), "not json {");
}

#[test]
fn list_shows_usernames_and_never_the_secret() {
    let env = Env::local();
    env.login("ghcr", "acme-deploy");
    env.login("registry.example.com:5000", "bot");
    for args in [
        &["registry", "list"][..],
        &["registry", "list", "--json"][..],
    ] {
        let (code, out, err) = env.ctl(args, "");
        assert_eq!(code, 0, "{err}");
        assert!(out.contains("acme-deploy") && out.contains("bot"), "{out}");
        assert!(out.contains("registry.example.com:5000"), "{out}");
        assert!(
            !out.contains(SECRET) && !out.contains(&auth("bot")),
            "{out}"
        );
        assert!(!err.contains(SECRET), "{err}");
    }
}

#[test]
fn a_second_login_keeps_the_mode_of_the_existing_file() {
    let env = Env::local();
    env.write("{}", 0o640);
    let (code, _, err) = env.login("ghcr", "acme-deploy");
    assert_eq!(code, 0, "{err}");
    assert!(!err.contains("note: created"), "{err}");
    assert_eq!(env.mode(), 0o640);
    env.login("ghcr", "other");
    assert_eq!(env.mode(), 0o640);
    assert_eq!(env.json()["auths"]["ghcr.io"]["auth"], auth("other"));
}

#[test]
fn a_login_drops_world_access_and_leaves_no_temporary_file() {
    let env = Env::local();
    env.write("{}", 0o644);
    let (code, _, err) = env.login("ghcr", "acme-deploy");
    assert_eq!(code, 0, "{err}");
    assert_eq!(env.mode(), 0o640);
    let tmp = format!("{}.minga-ctl.tmp", env.file().display());
    assert!(!std::path::Path::new(&tmp).exists());
}

#[test]
fn a_docker_hub_alias_replaces_the_docker_login_entry() {
    let env = Env::local();
    let old = json!({ "auths": { "https://index.docker.io/v1/": { "auth": auth("old") } } });
    env.write(&old.to_string(), 0o600);
    let (code, _, err) = env.login("INDEX.docker.io", "new");
    assert_eq!(code, 0, "{err}");
    let auths = env.json()["auths"].clone();
    assert_eq!(auths, json!({ "docker.io": { "auth": auth("new") } }));
    let (code, out, err) = env.ctl(&["registry", "logout", "index.docker.io", "--yes"], "");
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("removed"), "{out}");
    assert_eq!(env.json()["auths"], json!({}));
}

#[test]
fn an_empty_stdin_is_exit_1_and_changes_nothing() {
    let env = Env::local();
    let (code, _, err) = env.ctl(
        &["registry", "login", "ghcr", "--username", "acme-deploy"],
        "",
    );
    assert_eq!(code, 1);
    assert!(err.contains("no credential on stdin for ghcr.io"), "{err}");
    assert!(!env.file().exists());
}

#[test]
fn over_ssh_the_credential_goes_on_stdin_never_in_argv() {
    let env = Env::local();
    let bin = env.path("bin");
    std::fs::create_dir(&bin).unwrap();
    std::fs::write(bin.join("ssh"), STUB).unwrap();
    std::fs::set_permissions(bin.join("ssh"), std::fs::Permissions::from_mode(0o755)).unwrap();

    let file = env.file();
    let (code, _, err) = env.ctl(
        &[
            "context",
            "add",
            "prod",
            "--ssh",
            "alice@mgr1.example.net",
            "--node",
            "alice@mgr1.example.net",
            "--node",
            "alice@wrk1.example.net",
            "--registry-auth-file",
            file.to_str().unwrap(),
            "--use",
        ],
        "",
    );
    assert_eq!(code, 0, "{err}");

    let (code, out, err) = env.ctl(
        &[
            "--debug",
            "registry",
            "login",
            "ghcr",
            "--username",
            "acme-deploy",
            "--password-stdin",
        ],
        &format!("{SECRET}\n"),
    );
    assert_eq!(code, 0, "{err}");
    assert_eq!(out.matches("stored").count(), 2, "{out}");
    assert!(
        err.contains("debug: ssh -T -- alice@wrk1.example.net 'sh' '-c'"),
        "{err}"
    );
    assert!(!err.contains(SECRET), "{err}");
    assert_eq!(env.json()["auths"]["ghcr.io"]["auth"], auth("acme-deploy"));

    let args = std::fs::read_to_string(env.path("ssh.args")).unwrap();
    assert!(!args.contains(SECRET) && !args.contains(&auth("acme-deploy")));
    assert!(args.contains(&format!("'{}'", file.display())), "{args}");
    // The new content went to the node on stdin.
    let stdin = std::fs::read_to_string(env.path("ssh.stdin")).unwrap();
    assert!(stdin.contains(&auth("acme-deploy")), "{stdin}");

    let (code, out, err) = env.ctl(&["registry", "list", "--ssh", "alice@wrk1.example.net"], "");
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("alice@wrk1.example.net") && out.contains("acme-deploy"));
}
