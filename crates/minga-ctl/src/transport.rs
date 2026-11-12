//! How a command reaches the cluster (plan section 2.2).
//!
//! [`connect`] is the one place that picks the transport: direct HTTP, or an
//! SSH tunnel whose `ssh` child lives inside [`Connection`] until the command
//! returns. This module also holds the validation and quoting rules for
//! everything sent to a remote shell (plan item 8), and [`remote_exec`].
//!
//! The tunnel forwards to Unix sockets in a fresh 0700 directory, and the
//! [`ApiClient`] connects to those sockets directly. There is no local TCP
//! port: a free port handed to `ssh` is released first and can be bound by
//! another local user who reads it from `ps`, and the token would go to them.
//! Other local users cannot reach the sockets either.

use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, ChildStderr, Command};

use crate::api::ApiClient;
use crate::config::{self, Resolved, TokenSource};
use crate::error::CtlError;

/// How long to wait for the tunnel when ssh has no terminal to prompt on.
const TUNNEL_WAIT: Duration = Duration::from_secs(10);

/// Longest socket path used. `sun_path` is 104 bytes on macOS and 108 on
/// Linux, the terminating NUL included.
const SOCKET_PATH_MAX: usize = 100;

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

impl Connection {
    /// A connection without a tunnel, for a client the caller built.
    pub fn direct(api: ApiClient, metrics: String) -> Connection {
        Connection {
            api,
            metrics,
            _tunnel: None,
        }
    }

    /// The tunnel's API socket, when the connection goes over SSH.
    pub fn api_socket(&self) -> Option<&Path> {
        self._tunnel.as_ref().map(|t| t.api_sock.as_path())
    }
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
    Ok(Connection::direct(api, metrics))
}

/// The SSH transport: read the token if it lives on the node, then tunnel.
///
/// The token read runs first and has no time limit, so a first-use host-key
/// or passphrase prompt can be answered there; with a `ControlMaster` in the
/// operator's ssh config the tunnel then reuses that connection.
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
    // URLs name the API as seen from the target; every connection goes to
    // the tunnel's socket.
    let root = resolved.endpoint.trim_end_matches('/').to_string();
    let mut api = ApiClient::over_socket(&tunnel.api_sock, &root, token, &resolved.name, debug)?;
    let metrics = match (&resolved.context.metrics_endpoint, &tunnel.metrics_sock) {
        (Some(url), Some(sock)) => {
            let metrics = url.trim_end_matches('/').to_string();
            api = api.route_text(&metrics, sock)?;
            metrics
        }
        _ => root,
    };
    Ok(Connection {
        api,
        metrics,
        _tunnel: Some(tunnel),
    })
}

/// What the signal thread kills and removes if minga-ctl is told to stop:
/// the ssh pid once it is known, and the socket directory.
type Cleanup = Arc<Mutex<Option<(Option<u32>, PathBuf)>>>;

/// An `ssh -N` child forwarding Unix sockets to the API (and metrics) on
/// the target.
pub struct Tunnel {
    /// Socket forwarded to the API on the target.
    pub api_sock: PathBuf,
    /// Socket forwarded to metrics, when `metrics_endpoint` is set.
    pub metrics_sock: Option<PathBuf>,
    child: Child,
    dir: PathBuf,
    cleanup: Cleanup,
}

impl Tunnel {
    /// Starts `ssh` and waits until every forward accepts a connection, or
    /// `ssh` fails. Without a terminal ssh cannot prompt, so the wait is
    /// limited to [`TUNNEL_WAIT`]; with one the operator may be answering a
    /// prompt and can interrupt instead.
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
        let cleanup = signal_guard().map_err(|e| fail(format!("cannot watch for signals: {e}")))?;
        let dir = private_dir().map_err(fail)?;
        *lock(&cleanup) = Some((None, dir.clone()));
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
                lock(&cleanup).take();
                let _ = std::fs::remove_dir_all(&dir);
                return Err(fail(format!("cannot run ssh: {e}")));
            }
        };
        if let Some(pending) = lock(&cleanup).as_mut() {
            pending.0 = child.id();
        }
        let stderr = child.stderr.take().expect("stderr is piped");
        let mut tunnel = Tunnel {
            api_sock,
            metrics_sock,
            child,
            dir,
            cleanup,
        };
        let mut reader = Some(tokio::spawn(drain_stderr(stderr, debug)));

        let socks: Vec<PathBuf> = std::iter::once(tunnel.api_sock.clone())
            .chain(tunnel.metrics_sock.clone())
            .collect();
        let deadline = (!has_terminal()).then(|| Instant::now() + TUNNEL_WAIT);
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
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Err(fail(format!(
                    "the tunnel did not open within {} s",
                    TUNNEL_WAIT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(tunnel)
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        // Held while killing, so the signal thread never acts on a pid that
        // has been reaped and reused.
        let mut pending = lock(&self.cleanup);
        pending.take();
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Starts a thread that, on SIGINT, SIGTERM or SIGHUP, kills the tunnel's
/// ssh, removes its socket directory and exits with 128 + the signal number.
///
/// `Drop` and `kill_on_drop` run only on a normal return, so without this a
/// signal would leave `ssh -N` and the directory behind. The thread has its
/// own runtime, so it also acts while the command blocks (a confirmation
/// prompt, for example). On Linux `ssh_command` also sets `PR_SET_PDEATHSIG`,
/// which covers SIGKILL; elsewhere a SIGKILL still orphans the tunnel.
fn signal_guard() -> std::io::Result<Cleanup> {
    use tokio::signal::unix::{signal, SignalKind};
    let cleanup: Cleanup = Arc::default();
    let shared = Arc::clone(&cleanup);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // Registered here, before ssh starts, so no signal falls through to the
    // default action.
    let (mut int, mut term, mut hup) = {
        let _entered = runtime.enter();
        (
            signal(SignalKind::interrupt())?,
            signal(SignalKind::terminate())?,
            signal(SignalKind::hangup())?,
        )
    };
    std::thread::spawn(move || {
        let signo = runtime.block_on(async {
            tokio::select! {
                _ = int.recv() => libc::SIGINT,
                _ = term.recv() => libc::SIGTERM,
                _ = hup.recv() => libc::SIGHUP,
            }
        });
        if let Some((pid, dir)) = lock(&shared).take() {
            if let Some(pid) = pid.and_then(|p| libc::pid_t::try_from(p).ok()) {
                // SAFETY: kill(2) on the pid of our own unreaped child.
                unsafe { libc::kill(pid, libc::SIGTERM) };
            }
            let _ = std::fs::remove_dir_all(dir);
        }
        std::process::exit(128 + signo);
    });
    Ok(cleanup)
}

/// What a remote command returned. An ssh failure is an error instead.
#[derive(Debug)]
pub struct RemoteOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// One argument of a remote command.
#[derive(Debug, Clone, Copy)]
pub enum RemoteArg<'a> {
    /// Text the code itself chooses, such as a program name or a flag. It
    /// may start with `-`.
    Fixed(&'static str),
    /// A value from config, flags or the server. It may not start with `-`,
    /// so it can never be read as an option.
    Value(&'a str),
    /// A fixed `sh -c` script the code chooses. It is quoted but not
    /// character-checked; values reach it only as later `Value` arguments.
    Script(&'static str),
}

/// Runs `argv` on `target` over ssh and returns its status and output.
///
/// Validate each value with the `validate_*` function for its kind first;
/// every argument is then checked against [`remote_arg_ok`] and
/// single-quoted. Secrets go in `stdin`, never in `argv`: arguments show up
/// in process listings and the `--debug` trace. Exit status 255 is ssh's own
/// failure and becomes [`CtlError::Ssh`]; any other status is the caller's
/// to judge.
///
/// There is no time limit: ssh may be waiting on a host-key, passphrase or
/// MFA prompt, and remote commands such as an image pull can take minutes.
/// A caller that needs one wraps the call in `tokio::time::timeout`; the
/// child is killed when the future is dropped.
pub async fn remote_exec(
    target: &str,
    argv: &[RemoteArg<'_>],
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

/// `ssh` running `argv` on `target`, for a caller that streams its output
/// (`service logs -f`). The same validation and quoting as [`remote_exec`].
pub fn remote_command(
    target: &str,
    argv: &[RemoteArg<'_>],
    debug: bool,
) -> Result<Command, CtlError> {
    Ok(ssh_command(&exec_args(target, argv)?, debug))
}

/// Reads the API token from `path` on `target`; in memory only.
async fn read_remote_token(target: &str, path: &str, debug: bool) -> Result<String, CtlError> {
    validate_remote_path(path)?;
    let argv = [RemoteArg::Fixed("cat"), RemoteArg::Value(path)];
    let out = remote_exec(target, &argv, None, debug).await?;
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
///
/// `-T` keeps a `RequestTTY force` in the operator's ssh config from adding
/// a terminal, whose output processing would change the command's stdout.
pub fn exec_args(target: &str, argv: &[RemoteArg<'_>]) -> Result<Vec<String>, CtlError> {
    validate_target(target)?;
    if argv.is_empty() {
        return Err(CtlError::Usage(
            "a remote command needs a program to run".to_string(),
        ));
    }
    let mut args = vec!["-T".to_string(), "--".to_string(), target.to_string()];
    for arg in argv {
        let (text, ok) = match *arg {
            RemoteArg::Fixed(text) => (text, remote_arg_ok(text)),
            RemoteArg::Value(text) => (text, remote_arg_ok(text) && !text.starts_with('-')),
            RemoteArg::Script(text) => (text, true),
        };
        if !ok {
            return Err(CtlError::Usage(format!(
                "refusing to send `{}` to a remote shell",
                text.escape_debug()
            )));
        }
        args.push(quote(text));
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
    if value.len() > 1 && value.starts_with('/') && path_chars_ok(value) {
        return Ok(());
    }
    Err(CtlError::Usage(format!(
        "invalid remote path `{}`: it must be absolute and use only letters, digits and `._/-`",
        value.escape_debug()
    )))
}

/// Letters, digits and `._/-` only.
fn path_chars_ok(value: &str) -> bool {
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._/-".contains(c))
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

/// The `host:port` an `http://` URL names, as seen from the SSH target. The
/// host must be the target's loopback interface.
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
    // ssh opens the connection to this host from the target, in plain HTTP;
    // only the target's own loopback keeps the token inside the tunnel.
    let loopback = if v6 {
        host == "::1"
    } else {
        host == "127.0.0.1" || host.eq_ignore_ascii_case("localhost")
    };
    if !loopback {
        return Err(bad(
            "must name the SSH target's loopback (127.0.0.1, localhost or [::1]); \
             the hop from the target to any other host is plain HTTP outside the tunnel",
        ));
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
        format!("[::1]:{port}")
    } else {
        format!("{}:{port}", host.to_ascii_lowercase())
    })
}

/// `ssh` with `args`, killed when dropped; traced under `--debug`.
fn ssh_command(args: &[String], debug: bool) -> Command {
    if debug {
        eprintln!("debug: ssh {}", args.join(" "));
    }
    let mut cmd = Command::new("ssh");
    cmd.args(args).kill_on_drop(true);
    #[cfg(target_os = "linux")]
    // SAFETY: prctl is async-signal-safe and touches only the child.
    unsafe {
        cmd.pre_exec(|| {
            // ssh gets SIGTERM when minga-ctl dies, even by SIGKILL.
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    cmd
}

/// True when ssh can prompt: it reads passphrases and MFA codes from
/// `/dev/tty`, not stdin.
fn has_terminal() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .is_ok()
}

/// A new directory only this user can enter, for the tunnel's sockets.
/// `$TMPDIR` is used unless its socket paths would be too long or unsafe
/// inside `-L path:host:port`; then `/tmp`.
fn private_dir() -> Result<PathBuf, String> {
    use std::os::unix::fs::DirBuilderExt;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let name = format!("minga-ctl-{}-{nanos:x}", std::process::id());
    for base in [std::env::temp_dir(), PathBuf::from("/tmp")] {
        let dir = base.join(&name);
        if !socket_dir_ok(&dir) {
            continue;
        }
        // `create`, not `create_all`: fails if the path already exists, so a
        // directory or symlink planted there is never used.
        return match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => Ok(dir),
            Err(e) => Err(format!("cannot create {}: {e}", dir.display())),
        };
    }
    Err(
        "no temporary directory gives a socket path ssh can use; set TMPDIR to a short path"
            .to_string(),
    )
}

/// Whether the longest socket path in `dir` fits `sun_path` and holds no
/// `:` or other character `-L` or a shell could misread.
fn socket_dir_ok(dir: &Path) -> bool {
    dir.join("metrics.sock")
        .to_str()
        .is_some_and(|p| p.len() <= SOCKET_PATH_MAX && path_chars_ok(p))
}

/// True when every socket accepts a connection.
async fn all_connect(socks: &[PathBuf]) -> bool {
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
        let exec = exec_args(
            "mgr1",
            &[
                RemoteArg::Fixed("cat"),
                RemoteArg::Value("/etc/minga/api-token"),
            ],
        )
        .unwrap();
        for arg in tunnel.iter().chain(&exec) {
            assert!(!arg.contains("BatchMode"), "{arg}");
            assert!(!arg.contains("StrictHostKeyChecking"), "{arg}");
        }
    }

    #[test]
    fn the_exec_argv_puts_the_target_after_dashes_and_quotes_arguments() {
        let args = exec_args(
            "alice@mgr1",
            &[
                RemoteArg::Fixed("cat"),
                RemoteArg::Value("/etc/minga/api-token"),
            ],
        )
        .unwrap();
        assert_eq!(
            args,
            ["-T", "--", "alice@mgr1", "'cat'", "'/etc/minga/api-token'"]
        );
    }

    #[test]
    fn the_exec_argv_refuses_shell_syntax() {
        for arg in [
            "a b", "x;rm", "$(id)", "`id`", "a'b", "a\\b", "!1", "", "a\nb",
        ] {
            let argv = [RemoteArg::Fixed("echo"), RemoteArg::Value(arg)];
            assert!(exec_args("mgr1", &argv).is_err(), "{arg:?}");
        }
        assert!(exec_args("mgr1", &[]).is_err());
        assert!(exec_args("mgr1", &[RemoteArg::Fixed("a b")]).is_err());
    }

    #[test]
    fn only_fixed_arguments_may_start_with_a_dash() {
        let value = [RemoteArg::Fixed("ls"), RemoteArg::Value("-la")];
        assert!(exec_args("mgr1", &value).is_err());
        let flag = [RemoteArg::Fixed("ls"), RemoteArg::Fixed("-la")];
        assert_eq!(exec_args("mgr1", &flag).unwrap()[4], "'-la'");
    }

    #[test]
    fn a_socket_directory_must_fit_sun_path_and_parse_in_a_forward() {
        assert!(socket_dir_ok(Path::new("/tmp/minga-ctl-1-ab")));
        assert!(socket_dir_ok(Path::new(
            "/var/folders/9x/abcdefghijklmnopqrstuvwxyz0000gn/T/minga-ctl-99999-3b9aca00"
        )));
        assert!(!socket_dir_ok(&Path::new("/tmp").join("x".repeat(90))));
        assert!(!socket_dir_ok(Path::new("/tmp/a:b/minga-ctl-1-ab")));
        assert!(!socket_dir_ok(Path::new("/tmp/a b/minga-ctl-1-ab")));
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
        assert_eq!(forward_dest("http://LOCALHOST").unwrap(), "localhost:80");
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
    fn a_forward_destination_must_be_the_targets_loopback() {
        for remote in [
            "http://10.0.0.5:2375",
            "http://mgr2.example.net:2375",
            "http://127.0.0.2:2375",
            "http://[fe80::1]:2375",
            "http://0.0.0.0:2375",
        ] {
            let err = forward_dest(remote).unwrap_err().to_string();
            assert!(
                err.contains("must name the SSH target's loopback"),
                "{remote}: {err}"
            );
            assert_eq!(forward_dest(remote).unwrap_err().exit_code(), 1);
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
