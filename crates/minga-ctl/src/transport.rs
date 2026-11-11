//! How a command reaches the cluster (plan section 2.2).
//!
//! [`connect`] is the one place that picks the transport: direct HTTP, or an
//! SSH tunnel whose `ssh` child lives inside [`Connection`] until the command
//! returns. This module also holds the validation and quoting rules for
//! everything sent to a remote shell (plan item 8), and [`remote_exec`].
//!
//! The tunnel forwards to a Unix socket in a private 0700 directory, not to a
//! local TCP port. A released TCP port can be bound by another local user
//! before `ssh` binds it (the port is visible in `ps`), and the token would go
//! to them. minga-ctl instead keeps its own `127.0.0.1:0` listener for the
//! whole command and relays each connection to the socket.

use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{Child, ChildStderr, Command};
use tokio::task::JoinHandle;

use crate::api::ApiClient;
use crate::config::{self, Resolved, TokenSource};
use crate::error::CtlError;

/// How long to wait for the tunnel to accept a connection.
const TUNNEL_WAIT: Duration = Duration::from_secs(10);

/// A live connection for one command.
pub struct Connection {
    /// Client for the Docker Engine API.
    pub api: ApiClient,
    /// Root URL that serves `/metrics` (the API root unless the context sets
    /// `metrics_endpoint`).
    pub metrics: String,
    /// The SSH tunnel, if any. Dropping it kills `ssh`.
    _tunnel: Option<Tunnel>,
}

/// Connects to the resolved context's cluster.
pub async fn connect(resolved: &Resolved, debug: bool) -> Result<Connection, CtlError> {
    if let Some(target) = &resolved.context.ssh {
        return connect_ssh(resolved, target, debug).await;
    }
    if let TokenSource::RemoteFile(_) = resolved.token_source {
        return Err(CtlError::Config(format!(
            "context `{}` sets remote_token_file but no ssh target to read it over",
            resolved.name
        )));
    }

    // Direct transport: http:// to loopback, or https:// (plan item 4).
    config::check_endpoint_policy(None, &resolved.endpoint)?;
    let metrics = match &resolved.context.metrics_endpoint {
        Some(url) => {
            config::check_endpoint_policy(None, url)?;
            url.trim_end_matches('/').to_string()
        }
        None => resolved.endpoint.trim_end_matches('/').to_string(),
    };
    let api = ApiClient::new(
        &resolved.endpoint,
        resolved.token.clone(),
        resolved.context.tls_cert.as_deref(),
        &resolved.name,
        debug,
    )?;
    Ok(Connection {
        api,
        metrics,
        _tunnel: None,
    })
}

/// The SSH transport: read the token if it lives on the node, then tunnel.
async fn connect_ssh(
    resolved: &Resolved,
    target: &str,
    debug: bool,
) -> Result<Connection, CtlError> {
    validate_target(target)?;
    let api_dest = forward_dest(&resolved.endpoint)?;
    let metrics_dest = resolved
        .context
        .metrics_endpoint
        .as_deref()
        .map(forward_dest)
        .transpose()?;
    let token = match &resolved.token_source {
        TokenSource::RemoteFile(path) => Some(read_remote_token(target, path, debug).await?),
        _ => resolved.token.clone(),
    };
    let tunnel = Tunnel::open(target, &api_dest, metrics_dest.as_deref(), debug).await?;
    // The tunnel is plain HTTP to minga-ctl's own loopback listener.
    let api = ApiClient::new(&tunnel.api, token, None, &resolved.name, debug)?;
    let metrics = tunnel.metrics.clone().unwrap_or_else(|| tunnel.api.clone());
    Ok(Connection {
        api,
        metrics,
        _tunnel: Some(tunnel),
    })
}

/// An `ssh -N` child forwarding to the API (and metrics) on the target.
pub struct Tunnel {
    /// `http://127.0.0.1:<port>` for the API.
    pub api: String,
    /// `http://127.0.0.1:<port>` for metrics, when `metrics_endpoint` is set.
    pub metrics: Option<String>,
    child: Child,
    dir: PathBuf,
    relays: Vec<JoinHandle<()>>,
}

impl Tunnel {
    /// Starts `ssh` and waits until the forward accepts a connection
    /// ([`TUNNEL_WAIT`]) or `ssh` fails.
    pub async fn open(
        target: &str,
        api_dest: &str,
        metrics_dest: Option<&str>,
        debug: bool,
    ) -> Result<Tunnel, CtlError> {
        validate_target(target)?;
        let fail = |detail: String| CtlError::Ssh {
            target: target.to_string(),
            detail,
        };
        let dir =
            private_dir().map_err(|e| fail(format!("cannot create a socket directory: {e}")))?;
        let api_sock = dir.join("api.sock");
        let metrics_sock = metrics_dest.map(|_| dir.join("metrics.sock"));
        let args = tunnel_args(
            target,
            (&api_sock, api_dest),
            metrics_sock.as_deref().zip(metrics_dest),
        );
        let mut cmd = ssh_command(&args, debug);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&dir);
                return Err(fail(format!("cannot run ssh: {e}")));
            }
        };
        let stderr = child.stderr.take().expect("stderr is piped");
        let mut tunnel = Tunnel {
            api: String::new(),
            metrics: None,
            child,
            dir,
            relays: Vec::new(),
        };
        let mut reader = Some(tokio::spawn(drain_stderr(stderr, debug)));

        let socks: Vec<&Path> = std::iter::once(api_sock.as_path())
            .chain(metrics_sock.as_deref())
            .collect();
        let deadline = Instant::now() + TUNNEL_WAIT;
        loop {
            // A ControlMaster can hold the forward after this client exits,
            // so a socket that answers wins over the exit status.
            if all_connect(&socks).await {
                break;
            }
            let exited = tunnel
                .child
                .try_wait()
                .map_err(|e| fail(format!("cannot wait for ssh: {e}")))?;
            if let Some(status) = exited {
                let last = match reader.take() {
                    Some(handle) => tokio::time::timeout(Duration::from_secs(1), handle)
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .flatten(),
                    None => None,
                };
                return Err(fail(last.unwrap_or_else(|| {
                    format!("ssh exited with {status} before the tunnel opened")
                })));
            }
            if Instant::now() >= deadline {
                return Err(fail(format!(
                    "the tunnel did not open within {} s",
                    TUNNEL_WAIT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tunnel.api = tunnel.relay(api_sock).await?;
        if let Some(sock) = metrics_sock {
            tunnel.metrics = Some(tunnel.relay(sock).await?);
        }
        Ok(tunnel)
    }

    /// Listens on `127.0.0.1:0` and relays each connection to `sock`.
    async fn relay(&mut self, sock: PathBuf) -> Result<String, CtlError> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .and_then(|l| Ok((l.local_addr()?, l)))
            .map_err(|e| CtlError::Unreachable {
                endpoint: "127.0.0.1:0".to_string(),
                detail: e.to_string(),
            })?;
        let (addr, listener) = listener;
        self.relays.push(tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let sock = sock.clone();
                tokio::spawn(async move {
                    // A failed relay closes the connection; the client
                    // reports it as an unreachable API.
                    if let Ok(mut unix) = UnixStream::connect(&sock).await {
                        let _ = tokio::io::copy_bidirectional(&mut tcp, &mut unix).await;
                    }
                });
            }
        }));
        Ok(format!("http://{addr}"))
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        for relay in &self.relays {
            relay.abort();
        }
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// What a remote command returned. An ssh failure is an error instead.
#[derive(Debug)]
pub struct RemoteOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Runs `argv` on `target` over ssh and returns its status and output.
///
/// Validate each value with the `validate_*` function for its kind first;
/// every argument is then checked against [`remote_arg_ok`] and
/// single-quoted. Secrets go in `stdin`, never in `argv`: arguments show up
/// in process listings and the `--debug` trace. Exit status 255 is ssh's own
/// failure and becomes [`CtlError::Ssh`]; any other status is the caller's
/// to judge.
pub async fn remote_exec(
    target: &str,
    argv: &[&str],
    stdin: Option<&[u8]>,
    debug: bool,
) -> Result<RemoteOutput, CtlError> {
    let args = exec_args(target, argv)?;
    let mut cmd = ssh_command(&args, debug);
    cmd.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    })
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| CtlError::Ssh {
        target: target.to_string(),
        detail: format!("cannot run ssh: {e}"),
    })?;
    let pipe = child.stdin.take();
    let write = async move {
        match (pipe, stdin) {
            (Some(mut pipe), Some(bytes)) => pipe.write_all(bytes).await,
            _ => Ok(()),
        }
    };
    let (written, output) = tokio::join!(write, child.wait_with_output());
    let output = output.map_err(|e| CtlError::Ssh {
        target: target.to_string(),
        detail: format!("cannot wait for ssh: {e}"),
    })?;
    if output.status.code() == Some(255) {
        return Err(ssh_error(target, &output.stderr, output.status));
    }
    if let Err(e) = written {
        if output.status.success() {
            return Err(CtlError::Ssh {
                target: target.to_string(),
                detail: format!("cannot write stdin: {e}"),
            });
        }
    }
    Ok(RemoteOutput {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

/// Reads the API token from `path` on `target`; in memory only.
async fn read_remote_token(target: &str, path: &str, debug: bool) -> Result<String, CtlError> {
    validate_remote_path(path)?;
    let out = remote_exec(target, &["cat", path], None, debug).await?;
    if !out.status.success() {
        return Err(ssh_error(target, &out.stderr, out.status));
    }
    let text = String::from_utf8(out.stdout).map_err(|_| {
        CtlError::Config(format!(
            "remote_token_file {path} on {target} is not UTF-8 text"
        ))
    })?;
    let token = text.trim();
    if token.is_empty() {
        return Err(CtlError::Config(format!(
            "remote_token_file {path} on {target} is empty"
        )));
    }
    Ok(token.to_string())
}

/// The ssh arguments for the tunnel; each forward is `(local socket, host:port)`.
pub fn tunnel_args(
    target: &str,
    api: (&Path, &str),
    metrics: Option<(&Path, &str)>,
) -> Vec<String> {
    let mut args: Vec<String> = ["-N", "-o", "ExitOnForwardFailure=yes"]
        .map(String::from)
        .into();
    for (sock, dest) in std::iter::once(api).chain(metrics) {
        args.push("-L".to_string());
        args.push(format!("{}:{dest}", sock.display()));
    }
    args.push("--".to_string());
    args.push(target.to_string());
    args
}

/// The ssh arguments that run `argv` on `target`, each argument quoted.
pub fn exec_args(target: &str, argv: &[&str]) -> Result<Vec<String>, CtlError> {
    validate_target(target)?;
    if argv.is_empty() {
        return Err(CtlError::Usage(
            "a remote command needs a program to run".to_string(),
        ));
    }
    let mut args = vec!["--".to_string(), target.to_string()];
    for arg in argv {
        if !remote_arg_ok(arg) {
            return Err(CtlError::Usage(format!(
                "refusing to send `{}` to a remote shell",
                arg.escape_debug()
            )));
        }
        args.push(quote(arg));
    }
    Ok(args)
}

/// The characters any remote argument may use: the union of the plan's
/// item 8 sets plus `=`, `,` and `+` for flags. No spaces, quotes, `\` or `!`.
pub fn remote_arg_ok(arg: &str) -> bool {
    !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._:/@=,+-".contains(c))
}

/// Single-quotes `value` for a POSIX shell.
pub fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// An SSH target: `^[A-Za-z0-9][A-Za-z0-9._@:-]*$`.
pub fn validate_target(value: &str) -> Result<(), CtlError> {
    check("SSH target", value, "._@:-")
}

/// A service, task or secret ID: `^[A-Za-z0-9][A-Za-z0-9._-]*$`.
pub fn validate_id(value: &str) -> Result<(), CtlError> {
    check("ID", value, "._-")
}

/// An image reference: `^[A-Za-z0-9][A-Za-z0-9._:/@-]*$`.
pub fn validate_image(value: &str) -> Result<(), CtlError> {
    check("image reference", value, "._:/@-")
}

/// A path on the remote node: `^/[A-Za-z0-9._/-]+$`.
pub fn validate_remote_path(value: &str) -> Result<(), CtlError> {
    let ok = value.len() > 1
        && value.starts_with('/')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._/-".contains(c));
    if ok {
        return Ok(());
    }
    Err(CtlError::Usage(format!(
        "invalid remote path `{}`: it must be absolute and use only letters, digits and `._/-`",
        value.escape_debug()
    )))
}

/// Checks a value that starts with an ASCII letter or digit and continues
/// with letters, digits or `extra`.
fn check(kind: &str, value: &str, extra: &str) -> Result<(), CtlError> {
    let mut chars = value.chars();
    let ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || extra.contains(c));
    if ok {
        return Ok(());
    }
    Err(CtlError::Usage(format!(
        "invalid {kind} `{}`: it must start with a letter or digit and use only letters, digits and `{extra}`",
        value.escape_debug()
    )))
}

/// The `host:port` an `http://` URL names, as seen from the SSH target.
pub fn forward_dest(url: &str) -> Result<String, CtlError> {
    let bad = |why: &str| CtlError::Config(format!("with ssh, `{url}` {why}"));
    let rest = url
        .get(..7)
        .filter(|scheme| scheme.eq_ignore_ascii_case("http://"))
        .map(|_| &url[7..])
        .ok_or_else(|| bad("must be an http:// URL; the tunnel encrypts it"))?;
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    let shape = "must be http://HOST[:PORT] with no user, path or query";
    if authority.contains(['/', '?', '#', '@']) {
        return Err(bad(shape));
    }
    let (host, port, v6) = match authority.strip_prefix('[') {
        Some(inner) => {
            let (host, after) = inner.split_once(']').ok_or_else(|| bad(shape))?;
            let port = match after {
                "" => None,
                _ => Some(after.strip_prefix(':').ok_or_else(|| bad(shape))?),
            };
            (host, port, true)
        }
        None => match authority.split_once(':') {
            Some((host, port)) => (host, Some(port), false),
            None => (authority, None, false),
        },
    };
    let host_ok = if v6 {
        !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_hexdigit() || ":.".contains(c))
    } else {
        host.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ".-".contains(c))
    };
    if !host_ok {
        return Err(bad(shape));
    }
    let port = match port {
        None => 80,
        Some(p) => p
            .parse::<u16>()
            .ok()
            .filter(|p| *p != 0)
            .ok_or_else(|| bad(shape))?,
    };
    Ok(if v6 {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

/// `ssh` with `args`, killed when dropped; traced under `--debug`.
fn ssh_command(args: &[String], debug: bool) -> Command {
    if debug {
        eprintln!("debug: ssh {}", args.join(" "));
    }
    let mut cmd = Command::new("ssh");
    cmd.args(args).kill_on_drop(true);
    cmd
}

/// A new directory only this user can enter, for the tunnel's sockets.
fn private_dir() -> std::io::Result<PathBuf> {
    use std::os::unix::fs::DirBuilderExt;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("minga-ctl-{}-{nanos:x}", std::process::id()));
    // `create`, not `create_all`: fails if the path already exists, so a
    // directory or symlink planted there is never used.
    std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
    Ok(dir)
}

/// True when every socket accepts a connection.
async fn all_connect(socks: &[&Path]) -> bool {
    for sock in socks {
        if UnixStream::connect(sock).await.is_err() {
            return false;
        }
    }
    true
}

/// Reads ssh's stderr to the end and returns its last line. Under
/// `--debug` each line is echoed as it arrives.
async fn drain_stderr(stderr: ChildStderr, debug: bool) -> Option<String> {
    let mut lines = BufReader::new(stderr).lines();
    let mut last = None;
    while let Ok(Some(line)) = lines.next_line().await {
        let line = printable(&line);
        if debug {
            eprintln!("debug: ssh: {line}");
        }
        if !line.trim().is_empty() {
            last = Some(line.trim().to_string());
        }
    }
    last
}

/// `CtlError::Ssh` with the last line of ssh's stderr as the detail.
fn ssh_error(target: &str, stderr: &[u8], status: ExitStatus) -> CtlError {
    let text = String::from_utf8_lossy(stderr);
    let detail = text
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(printable)
        .unwrap_or_else(|| format!("ssh exited with {status}"));
    CtlError::Ssh {
        target: target.to_string(),
        detail,
    }
}

/// Replaces control characters, so remote text cannot drive the terminal.
fn printable(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tunnel_argv_forwards_the_api_to_a_socket() {
        let args = tunnel_args(
            "alice@mgr1.example.net",
            (Path::new("/tmp/d/api.sock"), "127.0.0.1:2375"),
            None,
        );
        assert_eq!(
            args,
            [
                "-N",
                "-o",
                "ExitOnForwardFailure=yes",
                "-L",
                "/tmp/d/api.sock:127.0.0.1:2375",
                "--",
                "alice@mgr1.example.net",
            ]
        );
    }

    #[test]
    fn the_tunnel_argv_adds_metrics_only_when_set() {
        let args = tunnel_args(
            "mgr1",
            (Path::new("/d/api.sock"), "127.0.0.1:2375"),
            Some((Path::new("/d/metrics.sock"), "127.0.0.1:9100")),
        );
        assert_eq!(
            args,
            [
                "-N",
                "-o",
                "ExitOnForwardFailure=yes",
                "-L",
                "/d/api.sock:127.0.0.1:2375",
                "-L",
                "/d/metrics.sock:127.0.0.1:9100",
                "--",
                "mgr1",
            ]
        );
    }

    #[test]
    fn no_argv_sets_batch_mode_or_host_key_checking() {
        let tunnel = tunnel_args("mgr1", (Path::new("/d/a"), "127.0.0.1:2375"), None);
        let exec = exec_args("mgr1", &["cat", "/etc/minga/api-token"]).unwrap();
        for arg in tunnel.iter().chain(&exec) {
            assert!(!arg.contains("BatchMode"), "{arg}");
            assert!(!arg.contains("StrictHostKeyChecking"), "{arg}");
        }
    }

    #[test]
    fn the_exec_argv_puts_the_target_after_dashes_and_quotes_arguments() {
        let args = exec_args("alice@mgr1", &["cat", "/etc/minga/api-token"]).unwrap();
        assert_eq!(
            args,
            ["--", "alice@mgr1", "'cat'", "'/etc/minga/api-token'"]
        );
    }

    #[test]
    fn the_exec_argv_refuses_shell_syntax() {
        for arg in [
            "a b", "x;rm", "$(id)", "`id`", "a'b", "a\\b", "!1", "", "a\nb",
        ] {
            assert!(exec_args("mgr1", &["echo", arg]).is_err(), "{arg:?}");
        }
        assert!(exec_args("mgr1", &[]).is_err());
    }

    #[test]
    fn quoting_survives_a_single_quote() {
        assert_eq!(quote("plain"), "'plain'");
        assert_eq!(quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn ssh_targets_follow_the_plan_rule() {
        for ok in [
            "mgr1",
            "alice@mgr1.example.net",
            "alice@10.0.0.1",
            "h:22",
            "a-b_c",
        ] {
            assert!(validate_target(ok).is_ok(), "{ok}");
        }
        for bad in [
            "-oProxyCommand=id",
            "-p22",
            "",
            "@host",
            ".host",
            "host name",
            "host;id",
            "hôst",
            "host/x",
        ] {
            assert!(validate_target(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ids_images_and_paths_follow_the_plan_rules() {
        assert!(validate_id("myapp_web.1").is_ok());
        assert!(validate_id("my-app").is_ok());
        assert!(validate_id("-x").is_err());
        assert!(validate_id("a/b").is_err());
        assert!(validate_id("a:b").is_err());

        assert!(validate_image("ghcr.io/org/app:1.2@sha256:abc").is_ok());
        assert!(validate_image("-x").is_err());
        assert!(validate_image("app latest").is_err());
        assert!(validate_image("app;id").is_err());

        assert!(validate_remote_path("/etc/minga/api-token").is_ok());
        assert!(validate_remote_path("/").is_err());
        assert!(validate_remote_path("etc/minga").is_err());
        assert!(validate_remote_path("/etc/my file").is_err());
        assert!(validate_remote_path("/etc/$(id)").is_err());
        assert!(validate_remote_path("~/token").is_err());
    }

    #[test]
    fn a_forward_destination_is_host_and_port() {
        assert_eq!(
            forward_dest("http://127.0.0.1:2375").unwrap(),
            "127.0.0.1:2375"
        );
        assert_eq!(
            forward_dest("HTTP://localhost:9100/").unwrap(),
            "localhost:9100"
        );
        assert_eq!(forward_dest("http://[::1]:2375").unwrap(), "[::1]:2375");
        assert_eq!(forward_dest("http://10.0.0.5").unwrap(), "10.0.0.5:80");
        for bad in [
            "https://127.0.0.1:2375",
            "http://127.0.0.1:2375/v1.41",
            "http://u@127.0.0.1:2375",
            "http://127.0.0.1:x",
            "http://127.0.0.1:0",
            "http://-host:1",
            "http://[::1",
            "http://h:1:2",
            "tcp://127.0.0.1:2375",
        ] {
            assert!(forward_dest(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn an_ssh_error_reports_the_last_stderr_line() {
        use std::os::unix::process::ExitStatusExt;
        let status = ExitStatus::from_raw(255 << 8);
        let err = ssh_error(
            "alice@mgr1",
            b"debug noise\nPermission denied (publickey).\n\n",
            status,
        );
        assert_eq!(
            err.to_string(),
            "ssh to alice@mgr1 failed: Permission denied (publickey)."
        );
        assert_eq!(err.exit_code(), 2);
        let err = ssh_error("h", b"\x1b[2Jboom", status);
        assert!(err.to_string().ends_with("?[2Jboom"), "{err}");
    }
}
