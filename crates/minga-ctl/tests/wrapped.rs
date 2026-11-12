//! The commands that run `minga` (`stack deploy`, `service logs`, `image`,
//! `secret put`) against a stub `MINGA_BIN`, a stub `ssh` first on `PATH`
//! and a scripted API. No real SSH, no real `minga`.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TARGET: &str = "alice@mgr1.example.net";

/// Stub `minga`: records arguments, environment and stdin. `logs` calls run
/// in parallel, so each records under its task ID and prints two lines.
const MINGA: &str = r#"#!/bin/sh
d="$STUB_DIR"
f="$d/minga"
[ "$1" = logs ] && f="$d/logs.$2"
printf '%s\n' "$@" > "$f.args"
env > "$f.env"
cat > "$f.stdin"
if [ "$1" = logs ]; then
  echo "line one"
  echo "warn" >&2
  [ -n "$STUB_FOLLOW" ] && exec sleep 30
fi
if [ -n "$STUB_CURL" ]; then
  url=""; prev=""
  for a in "$@"; do [ "$prev" = "--api-url" ] && url="$a"; prev="$a"; done
  curl -s -H "Authorization: Bearer $MINGA_API_TOKEN" "$url/v1.41/services" > "$d/curl.out"
fi
exit "${STUB_MINGA_EXIT:-0}"
"#;

/// Stub `ssh`: `-N` is the tunnel; `'cat'` is the token read; anything else
/// is a node command, recorded per target. Target `bad` fails.
const SSH: &str = r#"#!/bin/sh
d="$STUB_DIR"
if [ "$1" = "-N" ]; then
  printf '%s\n' "$@" > "$d/tunnel.tmp" && mv "$d/tunnel.tmp" "$d/tunnel.args"
  exec sleep 30
fi
if [ "$4" = "'cat'" ]; then
  printf '%s' "$STUB_TOKEN"
  exit 0
fi
printf '%s\n' "$@" > "$d/exec.$3.args"
cat > "$d/exec.$3.stdin"
if [ "$3" = bad ]; then
  echo "boom" >&2
  exit 1
fi
"#;

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Env {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        for (name, script) in [("minga", MINGA), ("ssh", SSH)] {
            let path = bin.join(name);
            std::fs::write(&path, script).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        Env { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn command(&self, args: &[&str], env: &[(&str, &str)]) -> Command {
        let path = format!(
            "{}:{}",
            self.path("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_minga-ctl"));
        cmd.args(args)
            .env("PATH", path)
            .env("MINGA_BIN", self.path("bin").join("minga"))
            .env("STUB_DIR", self.dir.path())
            .env("STUB_TOKEN", "s3cret-token\n")
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env("XDG_STATE_HOME", self.path("state"))
            .env("HOME", self.path("home"))
            .env("USER", "tester")
            .env_remove("MINGA_CTL_CONTEXT")
            .env_remove("MINGA_CTL_ENDPOINT")
            .env_remove("MINGA_CTL_TOKEN")
            .env_remove("MINGA_API_TOKEN");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd
    }

    /// Runs minga-ctl with `stdin`; returns (exit code, stdout, stderr).
    fn run(&self, args: &[&str], env: &[(&str, &str)], stdin: &[u8]) -> (i32, String, String) {
        let mut child = self
            .command(args, env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(stdin).unwrap();
        let out = child.wait_with_output().unwrap();
        (
            out.status.code().unwrap_or(-1),
            text(&out.stdout),
            text(&out.stderr),
        )
    }

    fn read(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.path(name)).ok()
    }

    fn lines(&self, name: &str) -> Vec<String> {
        let text = self.read(name).unwrap_or_else(|| panic!("{name} missing"));
        text.lines().map(str::to_string).collect()
    }

    fn audit(&self) -> Vec<Value> {
        self.read("state/minga-ctl/audit.jsonl")
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn revision(&self, service: &str) -> Option<Value> {
        self.read(&format!("state/minga-ctl/revisions/default/{service}.json"))
            .map(|t| serde_json::from_str(&t).unwrap())
    }

    /// A revision left by an earlier change: `id` at `image`.
    fn seed_revision(&self, id: &str, image: &str) {
        let dir = self.path("state/minga-ctl/revisions/default");
        std::fs::create_dir_all(&dir).unwrap();
        let revision = json!({
            "saved_at": "2026-01-01T00:00:00Z",
            "service_id": id,
            "spec": service(id, image)["Spec"],
        });
        std::fs::write(dir.join(format!("{id}.json")), revision.to_string()).unwrap();
    }

    fn ssh_context(&self) {
        let args = [
            "context",
            "add",
            "prod",
            "--ssh",
            TARGET,
            "--remote-token-file",
            "/etc/minga/api-token",
        ];
        let (code, _, err) = self.run(&args, &[], b"");
        assert_eq!(code, 0, "context add: {err}");
    }

    fn compose(&self, text: &str) -> String {
        let path = self.path("compose.yaml");
        std::fs::write(&path, text).unwrap();
        path.display().to_string()
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

const RELATIVE_BIND: &str =
    "services:\n  traefik:\n    image: traefik:3\n    volumes:\n      - ./conf:/etc/traefik:ro\n";
const NODE_SAFE: &str = "services:\n  web:\n    image: web:1\n    env_file: ./web.env\n    extra_hosts: ['host.docker.internal:host-gateway']\n";

/// An API that answers the given replies in order, one per connection, and
/// then stops listening. Returns its URL and the request heads it saw.
fn fake_api(replies: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let heads = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&heads);
    std::thread::spawn(move || {
        for (status, body) in replies {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 4096];
            let end = loop {
                if let Some(i) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break request.len(),
                    Ok(n) => request.extend_from_slice(&chunk[..n]),
                }
            };
            let head = text(&request[..end]);
            let length = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            while request.len() < end + length {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => request.extend_from_slice(&chunk[..n]),
                }
            }
            seen.lock().unwrap().push(text(&request));
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (url, heads)
}

fn service(id: &str, image: &str) -> Value {
    json!({
        "ID": id,
        "Version": { "Index": 1 },
        "Spec": {
            "Name": id,
            "Labels": { "com.docker.stack.namespace": "app" },
            "TaskTemplate": { "ContainerSpec": { "Image": image } },
            "Mode": { "Replicated": { "Replicas": 1 } }
        }
    })
}

fn ok(body: Value) -> (u16, String) {
    (200, body.to_string())
}

#[test]
fn deploy_passes_the_token_in_the_environment_and_saves_only_changed_revisions() {
    let env = Env::new();
    // A relative bind mount is fine here: the node is this machine.
    let file = env.compose(RELATIVE_BIND);
    let (url, heads) = fake_api(vec![
        ok(json!([
            service("app.web", "web:1"),
            service("app.worker", "worker:1")
        ])),
        ok(json!([
            service("app.web", "web:2"),
            service("app.worker", "worker:1"),
            service("app.new", "new:1")
        ])),
    ]);
    // From an earlier change: the deploy leaves app.worker unchanged, so this stays.
    env.seed_revision("app.worker", "worker:0");
    let token = "tok-deploy-5678";
    let (code, out, err) = env.run(
        &[
            "stack", "deploy", "-c", &file, "-p", "prod", "--wait", "7", "app",
        ],
        &[("MINGA_CTL_ENDPOINT", &url), ("MINGA_CTL_TOKEN", token)],
        b"",
    );
    assert_eq!(code, 0, "stderr: {err}");

    // The token is in the child's environment, never in its arguments.
    let args = env.lines("minga.args");
    assert_eq!(
        args,
        [
            "deploy",
            "-c",
            &file,
            "-p",
            "prod",
            "--wait",
            "7",
            "--api-url",
            &url,
            "app"
        ]
    );
    let child_env = env.read("minga.env").unwrap();
    assert!(
        child_env
            .lines()
            .any(|l| l == format!("MINGA_API_TOKEN={token}")),
        "{child_env}"
    );
    assert!(!child_env.contains("MINGA_CTL_TOKEN"), "{child_env}");
    assert!(!args.iter().any(|a| a.contains(token)));

    // Only the service whose spec changed gets a revision: its old spec.
    let web = env.revision("app.web").expect("app.web revision");
    assert_eq!(
        web["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "web:1"
    );
    let worker = env.revision("app.worker").expect("app.worker revision");
    assert_eq!(worker["saved_at"], "2026-01-01T00:00:00Z");
    assert_eq!(
        worker["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "worker:0"
    );
    assert_eq!(env.revision("app.new"), None);
    assert_eq!(
        out.trim(),
        "saved revision for app.web (app.worker unchanged; app.new new)"
    );
    assert_eq!(heads.lock().unwrap().len(), 2);

    let audit = env.audit();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0]["command"], "stack deploy");
    assert_eq!(audit[0]["result"], "ok");
    assert!(!audit[0].to_string().contains(token));
}

#[test]
fn deploy_exits_2_when_the_read_after_minga_fails() {
    let env = Env::new();
    let file = env.compose(NODE_SAFE);
    // One reply: the read before the deploy. The read after is refused.
    let (url, _) = fake_api(vec![ok(json!([service("app.web", "web:1")]))]);
    // From an earlier change; rolling back to it now would skip a change.
    env.seed_revision("app.web", "web:0");
    let (code, _, err) = env.run(
        &["stack", "deploy", "-c", &file, "app"],
        &[("MINGA_CTL_ENDPOINT", &url)],
        b"",
    );
    assert_eq!(code, 2, "stderr: {err}");
    assert!(env.read("minga.args").is_some(), "minga should have run");
    assert!(
        err.contains("error: deployed, but could not save revisions for app: "),
        "{err}"
    );
    assert!(
        err.contains("hint: each service's revision is its spec from before this deploy"),
        "{err}"
    );
    // Saved before minga ran, so the stale web:0 revision is gone.
    let web = env.revision("app.web").expect("app.web revision");
    assert_eq!(
        web["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "web:1"
    );
    assert_eq!(env.audit()[0]["exit_code"], 2);
}

#[test]
fn deploy_reports_a_failed_minga_as_exit_2() {
    let env = Env::new();
    let file = env.compose(NODE_SAFE);
    let (url, _) = fake_api(vec![ok(json!([])), ok(json!([]))]);
    let (code, _, err) = env.run(
        &["stack", "deploy", "-c", &file, "app"],
        &[("MINGA_CTL_ENDPOINT", &url), ("STUB_MINGA_EXIT", "3")],
        b"",
    );
    assert_eq!(code, 2, "stderr: {err}");
    assert!(err.contains("minga deploy failed"), "{err}");
}

#[test]
fn deploy_refuses_a_relative_bind_mount_when_the_node_is_another_machine() {
    let message = "bind-mounts ./conf (service `traefik`), which would resolve on this machine, not on the node";
    let hint =
        "hint: use an absolute path on the node, or run minga-ctl on the node with a local context";

    // An SSH context: refused before ssh or minga runs.
    let env = Env::new();
    env.ssh_context();
    let file = env.compose(RELATIVE_BIND);
    let (code, _, err) = env.run(&["stack", "deploy", "-c", &file, "app"], &[], b"");
    assert_eq!(code, 1, "stderr: {err}");
    assert!(err.contains(&format!("error: {file} {message}")), "{err}");
    assert!(err.contains(hint), "{err}");
    assert!(env.read("minga.args").is_none(), "minga ran");
    assert!(env.read("tunnel.args").is_none(), "ssh ran");

    // A direct https:// endpoint is another machine too.
    let env = Env::new();
    let file = env.compose(RELATIVE_BIND);
    let (code, _, err) = env.run(
        &["stack", "deploy", "-c", &file, "app"],
        &[("MINGA_CTL_ENDPOINT", "https://minga.example.net")],
        b"",
    );
    assert_eq!(code, 1, "stderr: {err}");
    assert!(err.contains(message), "{err}");
    assert!(env.read("minga.args").is_none(), "minga ran");
    assert_eq!(env.audit()[0]["exit_code"], 1);
}

#[test]
fn deploy_without_minga_is_exit_1_with_the_hint() {
    let env = Env::new();
    let file = env.compose(NODE_SAFE);
    let (url, _) = fake_api(vec![ok(json!([service("app.web", "web:1")]))]);
    env.seed_revision("app.web", "web:0");
    let (code, _, err) = env.run(
        &["stack", "deploy", "-c", &file, "app"],
        &[
            ("MINGA_CTL_ENDPOINT", &url),
            ("MINGA_BIN", "/nonexistent/minga"),
        ],
        b"",
    );
    assert_eq!(code, 1, "stderr: {err}");
    // Nothing was deployed, so the earlier rollback target is still there.
    let web = env.revision("app.web").expect("app.web revision");
    assert_eq!(
        web["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "web:0"
    );
    assert!(
        err.contains("error: `minga` not found on PATH (needed for `stack deploy`)"),
        "{err}"
    );
    assert!(
        err.contains("hint: install minga on this machine (it runs here, not on the node) or set MINGA_BIN; on macOS: cargo install --locked --path crates/minga-cli"),
        "{err}"
    );
}

/// Waits for `path` to appear.
async fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Answers `requests` requests on the tunnel socket with `[]`; returns the
/// request heads. The tunnel's readiness probe sends nothing and is skipped.
async fn serve_socket(listener: tokio::net::UnixListener, requests: usize) -> Vec<String> {
    let mut heads = Vec::new();
    while heads.len() < requests {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        if buf.is_empty() {
            continue;
        }
        let reply = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n[]";
        stream.write_all(reply.as_bytes()).await.unwrap();
        heads.push(text(&buf));
    }
    heads
}

#[tokio::test]
async fn deploy_over_ssh_relays_the_tunnel_and_passes_the_remote_token() {
    let env = Env::new();
    env.ssh_context();
    // env_file and host-gateway are allowed when the node is another machine.
    let file = env.compose(NODE_SAFE);
    let child = env
        .command(
            &["stack", "deploy", "-c", &file, "app"],
            &[("STUB_CURL", "1")],
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let run = tokio::task::spawn_blocking(move || child.wait_with_output());

    wait_for(&env.path("tunnel.args")).await;
    let args = env.lines("tunnel.args");
    let (sock, _) = args[4].split_once(':').unwrap();
    let listener = tokio::net::UnixListener::bind(sock).unwrap();
    // The read before, the stub's own request through the relay, the read after.
    let heads = serve_socket(listener, 3).await;
    let out = run.await.unwrap().unwrap();
    assert!(out.status.success(), "stderr: {}", text(&out.stderr));

    let minga_args = env.lines("minga.args");
    let at = minga_args.iter().position(|a| a == "--api-url").unwrap();
    assert!(
        minga_args[at + 1].starts_with("http://127.0.0.1:"),
        "{minga_args:?}"
    );
    assert!(!minga_args.iter().any(|a| a.contains("s3cret")));
    let child_env = env.read("minga.env").unwrap();
    assert!(
        child_env
            .lines()
            .any(|l| l == "MINGA_API_TOKEN=s3cret-token"),
        "{child_env}"
    );
    // The relay reached the API over the tunnel socket.
    assert_eq!(env.read("curl.out").as_deref(), Some("[]"));
    let relayed = heads[1].to_ascii_lowercase();
    assert!(relayed.starts_with("get /v1.41/services"), "{relayed}");
    assert!(
        relayed.contains("authorization: bearer s3cret-token"),
        "{relayed}"
    );
}

#[test]
fn secret_put_sends_the_value_on_stdin_only() {
    let env = Env::new();
    let value = b"hunter2-secret-value\n";
    let (url, heads) = fake_api(vec![(201, r#"{"ID":"app_db"}"#.to_string())]);
    let (code, out, err) = env.run(
        &["secret", "put", "app_db"],
        &[("MINGA_CTL_ENDPOINT", &url)],
        value,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert_eq!(env.lines("minga.args"), ["secret", "put", "app_db"]);
    assert_eq!(env.read("minga.stdin").unwrap().as_bytes(), value);
    let leaked = "hunter2";
    for (name, text) in [
        ("args", env.read("minga.args").unwrap()),
        ("env", env.read("minga.env").unwrap()),
        ("audit", env.read("state/minga-ctl/audit.jsonl").unwrap()),
        ("stdout", out.clone()),
        ("stderr", err.clone()),
    ] {
        assert!(!text.contains(leaked), "value in {name}: {text}");
    }
    assert!(out.contains("local  provisioned"), "{out}");
    assert!(
        out.contains("secret `app_db` declared in context `default`"),
        "{out}"
    );
    let heads = heads.lock().unwrap();
    assert!(
        heads[0].starts_with("POST /v1.41/secrets/create"),
        "{heads:?}"
    );
    assert!(heads[0].ends_with(r#"{"Name":"app_db"}"#), "{heads:?}");
    assert_eq!(env.audit()[0]["command"], "secret put");

    // Already declared: a 409 is not an error.
    let (url, _) = fake_api(vec![(409, r#"{"message":"exists"}"#.to_string())]);
    let (code, out, err) = env.run(
        &["secret", "put", "app_db"],
        &[("MINGA_CTL_ENDPOINT", &url)],
        value,
    );
    assert_eq!(code, 0, "stderr: {err}");
    assert!(out.contains("already declared"), "{out}");
}

#[test]
fn secret_put_with_empty_stdin_is_exit_1() {
    let env = Env::new();
    let (code, _, err) = env.run(&["secret", "put", "app_db"], &[], b"");
    assert_eq!(code, 1, "stderr: {err}");
    assert!(err.contains("no secret value on stdin"), "{err}");
    assert!(env.read("minga.args").is_none(), "minga ran");
}

#[test]
fn a_failed_target_is_exit_2_and_skips_the_declaration() {
    let env = Env::new();
    let value = b"hunter2-secret-value";
    let (url, heads) = fake_api(vec![(201, "{}".to_string())]);
    let (code, out, err) = env.run(
        &["secret", "put", "app_db", "--ssh", "good", "--ssh", "bad"],
        &[("MINGA_CTL_ENDPOINT", &url)],
        value,
    );
    assert_eq!(code, 2, "stderr: {err}");
    assert_eq!(
        env.lines("exec.good.args"),
        ["-T", "--", "good", "'minga'", "'secret'", "'put'", "'app_db'"]
    );
    assert_eq!(env.read("exec.good.stdin").unwrap().as_bytes(), value);
    assert!(err.contains("error: bad: boom"), "{err}");
    assert!(out.contains("good  provisioned"), "{out}");
    assert!(heads.lock().unwrap().is_empty(), "the API was called");
    assert!(!err.contains("hunter2") && !out.contains("hunter2"));
}

#[test]
fn service_logs_follow_ends_when_stdout_is_closed() {
    let env = Env::new();
    let (url, _) = fake_api(vec![
        ok(service("app.web", "web:1")),
        ok(json!([{ "ID": "app.web.1.aa" }, { "ID": "app.web.2.bb" }])),
    ]);
    let start = Instant::now();
    let mut child = env
        .command(
            &["service", "logs", "app.web", "-f"],
            &[("MINGA_CTL_ENDPOINT", &url), ("STUB_FOLLOW", "1")],
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Like `| head` exiting: the first line minga-ctl writes hits a closed pipe.
    drop(child.stdout.take());
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    // Both stubs sleep 30s; ending well before that means they were stopped.
    assert!(start.elapsed() < Duration::from_secs(20));
}

#[test]
fn service_logs_prefixes_each_line_with_its_task() {
    let env = Env::new();
    let (url, heads) = fake_api(vec![
        ok(service("app.web", "web:1")),
        ok(json!([{ "ID": "app.web.1.aa" }, { "ID": "app.web.2.bb" }])),
    ]);
    let (code, out, err) = env.run(
        &["service", "logs", "app.web", "--tail", "5"],
        &[("MINGA_CTL_ENDPOINT", &url)],
        b"",
    );
    assert_eq!(code, 0, "stderr: {err}");
    for task in ["app.web.1.aa", "app.web.2.bb"] {
        assert!(out.contains(&format!("{task} | line one\n")), "{out}");
        assert!(err.contains(&format!("{task} | warn\n")), "{err}");
        assert_eq!(
            env.lines(&format!("logs.{task}.args")),
            [
                "logs",
                task,
                "--tail",
                "5",
                "--nats-url",
                "nats://127.0.0.1:4222"
            ]
        );
    }
    let heads = heads.lock().unwrap();
    assert!(
        heads[1].starts_with("GET /v1.41/tasks?filters="),
        "{heads:?}"
    );
    assert!(env.audit().is_empty(), "logs is a read");
}

#[test]
fn image_pull_runs_on_each_node_and_rm_needs_confirmation() {
    let env = Env::new();
    let image = "ghcr.io/acme/web:1.4.2";
    let (code, out, err) = env.run(
        &["image", "pull", image, "--ssh", "n1", "--ssh", "n2"],
        &[],
        b"",
    );
    assert_eq!(code, 0, "stderr: {err}");
    for node in ["n1", "n2"] {
        assert_eq!(
            env.lines(&format!("exec.{node}.args")),
            [
                "-T",
                "--",
                node,
                "'minga'",
                "'image'",
                "'pull'",
                &format!("'{image}'")
            ]
        );
        assert!(out.contains(&format!("{node}  pulled")), "{out}");
    }
    assert_eq!(env.audit()[0]["command"], "image pull");

    let (code, _, err) = env.run(&["image", "rm", image, "--ssh", "n1"], &[], b"");
    assert_eq!(code, 1, "stderr: {err}");
    assert!(err.contains("`image rm` needs confirmation"), "{err}");
    assert!(env.lines("exec.n1.args").contains(&"'pull'".to_string()));

    let (code, _, err) = env.run(&["image", "pull", "bad;id", "--ssh", "n1"], &[], b"");
    assert_eq!(code, 1, "stderr: {err}");
}
