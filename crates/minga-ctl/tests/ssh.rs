//! The SSH transport against a stub `ssh` placed first on `PATH`.
//!
//! No test opens a real SSH connection (plan section 6). The stub records its
//! arguments; for the tunnel it then sleeps, and the test itself listens on
//! the Unix socket named in `-L` and answers as the API would.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

const TARGET: &str = "alice@mgr1.example.net";

/// Stub ssh: `-N` is the tunnel, anything else is the token read.
const STUB: &str = r#"#!/bin/sh
d="$STUB_DIR"
if [ "$1" = "-N" ]; then
  printf '%s\n' "$@" > "$d/tunnel.tmp" && mv "$d/tunnel.tmp" "$d/tunnel.args"
  echo $$ > "$d/tunnel.pid"
  if [ -n "$STUB_TUNNEL_FAIL" ]; then
    echo "debug1: noise" >&2
    echo "Permission denied (publickey)." >&2
    exit 255
  fi
  exec sleep 30
fi
printf '%s\n' "$@" > "$d/exec.args"
printf '%s' "$STUB_TOKEN"
"#;

struct Stub {
    dir: tempfile::TempDir,
}

impl Stub {
    fn new() -> Stub {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let ssh = bin.join("ssh");
        std::fs::write(&ssh, STUB).unwrap();
        std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
        let stub = Stub { dir };
        let (ok, _, err) = stub.ctl(
            &[
                "context",
                "add",
                "prod",
                "--ssh",
                TARGET,
                "--remote-token-file",
                "/etc/minga/api-token",
            ],
            &[],
        );
        assert!(ok, "context add: {err}");
        stub
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
            .env("STUB_DIR", self.dir.path())
            .env("STUB_TOKEN", "  s3cret-token \n")
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env("HOME", self.path("home"))
            .env_remove("MINGA_CTL_CONTEXT")
            .env_remove("MINGA_CTL_ENDPOINT")
            .env_remove("MINGA_CTL_TOKEN");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd
    }

    fn ctl(&self, args: &[&str], env: &[(&str, &str)]) -> (bool, String, String) {
        let out = self.command(args, env).output().unwrap();
        (out.status.success(), text(&out.stdout), text(&out.stderr))
    }

    fn lines(&self, name: &str) -> Vec<String> {
        std::fs::read_to_string(self.path(name))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

/// Waits for the stub to record the tunnel arguments.
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

/// Answers every request on `sock` with `[]` and returns the request heads.
async fn serve_api(listener: UnixListener, requests: usize) -> Vec<String> {
    let mut heads = Vec::new();
    while heads.len() < requests {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        // The readiness probe connects and closes without sending anything.
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        if buf.is_empty() {
            continue;
        }
        let body = "[]";
        let reply = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(reply.as_bytes()).await.unwrap();
        heads.push(text(&buf));
    }
    heads
}

#[tokio::test]
async fn a_command_runs_through_the_tunnel_with_the_remote_token() {
    let stub = Stub::new();
    let child = stub
        .command(&["--debug", "secret", "list"], &[])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let run = tokio::task::spawn_blocking(move || child.wait_with_output());

    wait_for(&stub.path("tunnel.args")).await;
    let args = stub.lines("tunnel.args");
    assert_eq!(args[..4], ["-N", "-o", "ExitOnForwardFailure=yes", "-L"]);
    let (sock, dest) = args[4].split_once(':').unwrap();
    assert_eq!(dest, "127.0.0.1:2375");
    assert_eq!(args[5..], ["--", TARGET]);
    assert_eq!(args.len(), 7, "{args:?}");

    let sock = PathBuf::from(sock);
    let dir = sock.parent().unwrap().to_path_buf();
    let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700, "socket dir mode {mode:04o}");
    let heads = serve_api(UnixListener::bind(&sock).unwrap(), 1).await;

    let out: Output = run.await.unwrap().unwrap();
    let stderr = text(&out.stderr);
    assert!(out.status.success(), "stderr: {stderr}");

    // The token came from the remote file, trimmed, and went only in the header.
    assert_eq!(
        stub.lines("exec.args"),
        ["-T", "--", TARGET, "'cat'", "'/etc/minga/api-token'"]
    );
    let head = heads[0].to_ascii_lowercase();
    assert!(head.starts_with("get /v1.41/secrets"), "{head}");
    assert!(
        head.contains("authorization: bearer s3cret-token\r\n"),
        "{head}"
    );
    assert!(!stderr.contains("s3cret-token"), "{stderr}");
    assert!(!args.iter().any(|a| a.contains("s3cret")));

    // --debug shows the ssh command lines.
    assert!(
        stderr.contains("debug: ssh -N -o ExitOnForwardFailure=yes -L "),
        "{stderr}"
    );
    assert!(
        stderr.contains(&format!(
            "debug: ssh -T -- {TARGET} 'cat' '/etc/minga/api-token'"
        )),
        "{stderr}"
    );

    // The ssh child is killed and the socket directory removed on exit.
    assert_cleaned_up(&stub, &dir).await;
}

/// The socket directory named in the recorded tunnel arguments.
fn socket_dir(stub: &Stub) -> PathBuf {
    let args = stub.lines("tunnel.args");
    let (sock, _) = args[4].split_once(':').unwrap();
    Path::new(sock).parent().unwrap().to_path_buf()
}

/// The stub tunnel's pid is gone and the socket directory removed.
async fn assert_cleaned_up(stub: &Stub, dir: &Path) {
    let pid = stub.lines("tunnel.pid")[0].clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Command::new("kill")
        .args(["-0", &pid])
        .status()
        .unwrap()
        .success()
    {
        assert!(Instant::now() < deadline, "ssh pid {pid} still running");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!dir.exists(), "{} was left behind", dir.display());
}

#[tokio::test]
async fn a_signal_kills_the_tunnel_and_removes_the_socket_directory() {
    for signo in [libc::SIGTERM, libc::SIGHUP, libc::SIGINT] {
        let stub = Stub::new();
        let child = stub.command(&["secret", "list"], &[]).spawn().unwrap();
        let pid = libc::pid_t::try_from(child.id()).unwrap();
        let run = tokio::task::spawn_blocking(move || child.wait_with_output());
        wait_for(&stub.path("tunnel.args")).await;
        wait_for(&stub.path("tunnel.pid")).await;
        let dir = socket_dir(&stub);
        // SAFETY: kill(2) on the minga-ctl child this test started.
        assert_eq!(unsafe { libc::kill(pid, signo) }, 0);
        let out = run.await.unwrap().unwrap();
        assert_eq!(out.status.code(), Some(128 + signo), "signal {signo}");
        assert_cleaned_up(&stub, &dir).await;
    }
}

#[tokio::test]
async fn a_hanging_ssh_without_a_terminal_times_out_after_ten_seconds() {
    let stub = Stub::new();
    let mut cmd = stub.command(&["secret", "list"], &[]);
    // A new session has no controlling terminal, as under cron or CI.
    // SAFETY: setsid is async-signal-safe.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut cmd, || {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let started = Instant::now();
    let out = tokio::task::spawn_blocking(move || cmd.output())
        .await
        .unwrap()
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(
        text(&out.stderr).trim(),
        format!("error: ssh to {TARGET} failed: the tunnel did not open within 10 s")
    );
    assert!(elapsed >= Duration::from_secs(10), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(20), "{elapsed:?}");
    let dir = socket_dir(&stub);
    assert_cleaned_up(&stub, &dir).await;
}

#[test]
fn a_failed_tunnel_is_exit_two_with_the_last_ssh_line() {
    let stub = Stub::new();
    let out = stub
        .command(&["secret", "list"], &[("STUB_TUNNEL_FAIL", "1")])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(
        text(&out.stderr).trim(),
        format!("error: ssh to {TARGET} failed: Permission denied (publickey).")
    );
}

#[test]
fn an_empty_remote_token_is_refused_before_the_tunnel() {
    let stub = Stub::new();
    let (ok, _, err) = stub.ctl(&["secret", "list"], &[("STUB_TOKEN", " \n ")]);
    assert!(!ok);
    assert!(err.contains("/etc/minga/api-token on"), "{err}");
    assert!(err.contains("is empty"), "{err}");
    assert!(!stub.path("tunnel.args").exists());
}

#[test]
fn a_target_starting_with_a_dash_is_refused_at_add_time() {
    let stub = Stub::new();
    for bad in ["-oProxyCommand=touch /tmp/x", "host;id"] {
        let flag = format!("--ssh={bad}");
        let (ok, _, err) = stub.ctl(&["context", "add", "evil", &flag], &[]);
        assert!(!ok, "{bad} was accepted");
        assert!(err.contains("invalid SSH target"), "{err}");
    }
    let (ok, _, err) = stub.ctl(
        &["context", "add", "evil", "--ssh", "mgr1", "--node=-p22"],
        &[],
    );
    assert!(!ok && err.contains("invalid SSH target"), "{err}");
    let (ok, _, err) = stub.ctl(
        &[
            "context",
            "add",
            "evil",
            "--ssh",
            "mgr1",
            "--remote-token-file",
            "/etc/$(id)",
        ],
        &[],
    );
    assert!(!ok && err.contains("invalid remote path"), "{err}");
}

#[test]
fn context_add_applies_the_tunnel_destination_rules() {
    let stub = Stub::new();
    let add = |extra: &[&str]| {
        let mut args = vec!["context", "add", "bad", "--ssh", "mgr1"];
        args.extend_from_slice(extra);
        stub.ctl(&args, &[])
    };
    for (extra, expected) in [
        (
            &["--endpoint", "https://127.0.0.1:2375"][..],
            "must be an http:// URL",
        ),
        (
            &["--endpoint", "http://10.0.0.5:2375"][..],
            "must name the SSH target's loopback",
        ),
        (
            &["--metrics-endpoint", "http://10.0.0.5:9100"][..],
            "must name the SSH target's loopback",
        ),
    ] {
        let (ok, _, err) = add(extra);
        assert!(!ok, "{extra:?} was accepted");
        assert!(err.contains(expected), "{extra:?}: {err}");
    }
    let (ok, _, err) = add(&["--metrics-endpoint", "http://127.0.0.1:9100"]);
    assert!(ok, "{err}");
    let list =
        std::fs::read_to_string(stub.path("config").join("minga-ctl").join("config.yaml")).unwrap();
    assert!(!list.contains("10.0.0.5"), "{list}");
}

#[test]
fn a_remote_token_file_without_ssh_is_refused_at_add_time() {
    let stub = Stub::new();
    let (ok, _, err) = stub.ctl(
        &[
            "context",
            "add",
            "local",
            "--remote-token-file",
            "/etc/minga/api-token",
        ],
        &[],
    );
    assert!(!ok);
    assert!(err.contains("--remote-token-file needs --ssh"), "{err}");
}
