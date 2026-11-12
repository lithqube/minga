//! `minga-ctl image` (plan section 5.4), and the node-local execution that
//! `image`, `secret put` and `service logs` share.
//!
//! Node-local commands run on each SSH target over [`transport::remote_exec`],
//! or on this machine when the context names no target. Locally `minga` is
//! `MINGA_BIN`, else `minga` on `PATH`; on a node it is `minga` on the SSH
//! user's `PATH`.

use std::ffi::OsString;
use std::process::Stdio;

use clap::Subcommand;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::cmd::service::{confirm, report};
use crate::config::{Context, Resolved};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::{self, RemoteArg, RemoteOutput};

/// Image commands.
#[derive(Debug, Subcommand)]
pub enum ImageCommand {
    /// Pull an image into Minga's containerd namespace on each node.
    Pull {
        /// Image reference, e.g. `ghcr.io/acme/web:1.4.2`.
        image: String,
        /// Run on this SSH target; repeat for several. Default: the
        /// context's `nodes`, else its `ssh`, else this machine.
        #[arg(long = "ssh", value_name = "TARGET")]
        ssh: Vec<String>,
    },
    /// List the images in Minga's containerd namespace on each node.
    Ls {
        /// Run on this SSH target; repeat for several.
        #[arg(long = "ssh", value_name = "TARGET")]
        ssh: Vec<String>,
    },
    /// Remove an image from Minga's containerd namespace on each node.
    Rm {
        /// Image reference.
        image: String,
        /// Run on this SSH target; repeat for several.
        #[arg(long = "ssh", value_name = "TARGET")]
        ssh: Vec<String>,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
}

impl ImageCommand {
    /// The audit-log command name and arguments, for commands that change state.
    pub fn audit(&self) -> Option<(&'static str, Value)> {
        match self {
            ImageCommand::Ls { .. } => None,
            ImageCommand::Pull { image, ssh } => {
                Some(("image pull", json!({ "image": image, "ssh": ssh })))
            }
            ImageCommand::Rm { image, ssh, yes } => Some((
                "image rm",
                json!({ "image": image, "ssh": ssh, "yes": yes }),
            )),
        }
    }
}

/// Runs an image command. No API call.
pub async fn run(
    cmd: &ImageCommand,
    resolved: &Resolved,
    debug: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match cmd {
        ImageCommand::Pull { image, ssh } => {
            transport::validate_image(image)?;
            let nodes = targets(ssh, &resolved.context)?;
            let argv = [
                RemoteArg::Fixed("minga"),
                RemoteArg::Fixed("image"),
                RemoteArg::Fixed("pull"),
                RemoteArg::Value(image),
            ];
            on_each(
                "image pull",
                &nodes,
                &argv,
                None,
                "pulled",
                debug,
                format,
                quiet,
            )
            .await
        }
        ImageCommand::Ls { ssh } => {
            ls(&targets(ssh, &resolved.context)?, debug, format, quiet).await
        }
        ImageCommand::Rm { image, ssh, yes } => {
            transport::validate_image(image)?;
            let nodes = targets(ssh, &resolved.context)?;
            let names: Vec<&str> = nodes.iter().map(Node::name).collect();
            confirm(
                "image rm",
                *yes,
                &format!("This removes image {image} from:\n  {}", names.join("\n  ")),
            )?;
            let argv = [
                RemoteArg::Fixed("ctr"),
                RemoteArg::Fixed("-n"),
                RemoteArg::Fixed("minga"),
                RemoteArg::Fixed("images"),
                RemoteArg::Fixed("rm"),
                RemoteArg::Value(image),
            ];
            on_each(
                "image rm", &nodes, &argv, None, "removed", debug, format, quiet,
            )
            .await
        }
    }
}

/// `ctr -n minga images ls -q` on each node, as NODE / REF rows.
async fn ls(nodes: &[Node<'_>], debug: bool, format: Format, quiet: bool) -> Result<(), CtlError> {
    let argv = ["ctr", "-n", "minga", "images", "ls", "-q"].map(RemoteArg::Fixed);
    let mut rows = Rows::new(&["NODE", "REF"]).with_id_column(1);
    let mut failed = 0;
    for node in nodes {
        match exec(node, &argv, None, "image ls", debug).await {
            Ok(out) if out.status.success() => {
                for line in String::from_utf8_lossy(&out.stdout).lines() {
                    let image = printable(line.trim());
                    if !image.is_empty() {
                        let value = json!({ "node": node.name(), "ref": image });
                        rows.push(vec![node.name().to_string(), image], value);
                    }
                }
            }
            Err(err @ CtlError::MissingBinary { .. }) if matches!(node, Node::Local) => {
                return Err(err)
            }
            outcome => {
                failed += 1;
                eprintln!("error: {}: {}", node.name(), failure(outcome));
            }
        }
    }
    output::print(format, quiet, &rows)?;
    halted("image ls", failed, nodes.len())
}

/// Where a node-local command runs.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Node<'a> {
    Local,
    Ssh(&'a str),
}

impl Node<'_> {
    /// The name shown in output: the SSH target, or `local`.
    pub(crate) fn name(&self) -> &str {
        match self {
            Node::Local => "local",
            Node::Ssh(target) => target,
        }
    }
}

/// Targets in plan order: every `--ssh`; else the context's `nodes`; else its
/// `ssh`; else this machine. All are validated before anything runs.
pub(crate) fn targets<'a>(
    flags: &'a [String],
    context: &'a Context,
) -> Result<Vec<Node<'a>>, CtlError> {
    let list: &[String] = if !flags.is_empty() {
        flags
    } else if !context.nodes.is_empty() {
        &context.nodes
    } else {
        context.ssh.as_slice()
    };
    if list.is_empty() {
        return Ok(vec![Node::Local]);
    }
    list.iter()
        .map(|target| transport::validate_target(target).map(|()| Node::Ssh(target)))
        .collect()
}

/// Runs `argv` on `node`. Secrets go in `stdin`, never in `argv`. A missing
/// program (spawn failure locally, exit 127 from the remote shell) is
/// [`CtlError::MissingBinary`]; any other status is the caller's to judge.
pub(crate) async fn exec(
    node: &Node<'_>,
    argv: &[RemoteArg<'_>],
    stdin: Option<&[u8]>,
    command: &str,
    debug: bool,
) -> Result<RemoteOutput, CtlError> {
    let out = match node {
        Node::Ssh(target) => transport::remote_exec(target, argv, stdin, debug).await?,
        Node::Local => {
            let mut cmd = local_command(argv, command)?;
            cmd.stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
            let mut child = cmd.spawn().map_err(|e| spawn_error(e, argv, command))?;
            let pipe = child.stdin.take();
            let write = async move {
                match (pipe, stdin) {
                    (Some(mut pipe), Some(bytes)) => pipe.write_all(bytes).await,
                    _ => Ok(()),
                }
            };
            let (written, output) = tokio::join!(write, child.wait_with_output());
            let output =
                output.map_err(|e| CtlError::Usage(format!("cannot wait for `{command}`: {e}")))?;
            if let Err(e) = written {
                if output.status.success() {
                    return Err(CtlError::Usage(format!(
                        "cannot write stdin for `{command}`: {e}"
                    )));
                }
            }
            RemoteOutput {
                status: output.status,
                stdout: output.stdout,
                stderr: output.stderr,
            }
        }
    };
    if out.status.code() == Some(127) {
        return Err(missing(argv, command));
    }
    Ok(out)
}

/// The local process for `argv`: `minga` is `MINGA_BIN` when set. The
/// child never sees `MINGA_CTL_TOKEN`.
pub(crate) fn local_command(argv: &[RemoteArg<'_>], command: &str) -> Result<Command, CtlError> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| CtlError::Usage(format!("`{command}` has no program to run")))?;
    let mut cmd = Command::new(program_path(text(program)));
    cmd.args(args.iter().map(text))
        .env_remove("MINGA_CTL_TOKEN")
        .kill_on_drop(true);
    Ok(cmd)
}

/// `MINGA_BIN`, else `minga` on `PATH`.
pub(crate) fn minga_bin() -> OsString {
    std::env::var_os("MINGA_BIN")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "minga".into())
}

fn program_path(program: &str) -> OsString {
    if program == "minga" {
        minga_bin()
    } else {
        program.into()
    }
}

fn text<'a>(arg: &RemoteArg<'a>) -> &'a str {
    match *arg {
        RemoteArg::Fixed(text) => text,
        RemoteArg::Value(text) => text,
    }
}

/// A local spawn failure: not found is [`CtlError::MissingBinary`].
pub(crate) fn spawn_error(err: std::io::Error, argv: &[RemoteArg<'_>], command: &str) -> CtlError {
    if err.kind() == std::io::ErrorKind::NotFound {
        return missing(argv, command);
    }
    let program = argv.first().map_or("", text);
    CtlError::Usage(format!("cannot run `{program}` for `{command}`: {err}"))
}

fn missing(argv: &[RemoteArg<'_>], command: &str) -> CtlError {
    CtlError::MissingBinary {
        bin: argv.first().map_or("", text).to_string(),
        command: command.to_string(),
    }
}

/// Runs `argv` on every node, one after another. A failure on one node is
/// reported and the rest still run; any failure is exit 2.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn on_each(
    command: &str,
    nodes: &[Node<'_>],
    argv: &[RemoteArg<'_>],
    stdin: Option<&[u8]>,
    done: &str,
    debug: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let width = nodes.iter().map(|n| n.name().len()).max().unwrap_or(0);
    let mut lines = Vec::new();
    let mut values = Vec::new();
    let mut failed = 0;
    for node in nodes {
        match exec(node, argv, stdin, command, debug).await {
            Ok(out) if out.status.success() => {
                lines.push(format!("{:<width$}  {done}", node.name()));
                values.push(json!({ "node": node.name(), "result": "ok" }));
            }
            // This machine is the only target: `minga` missing is exit 1.
            Err(err @ CtlError::MissingBinary { .. }) if matches!(node, Node::Local) => {
                return Err(err)
            }
            outcome => {
                failed += 1;
                let detail = failure(outcome);
                eprintln!("error: {}: {detail}", node.name());
                values.push(json!({ "node": node.name(), "result": "error", "detail": detail }));
            }
        }
    }
    report(format, quiet, &lines.join("\n"), Value::Array(values))?;
    halted(command, failed, nodes.len())
}

/// Exit 2 when any node failed.
pub(crate) fn halted(command: &str, failed: usize, total: usize) -> Result<(), CtlError> {
    if failed == 0 {
        return Ok(());
    }
    Err(CtlError::Halted(format!(
        "`{command}` failed on {failed} of {total} target(s)"
    )))
}

/// Why a node command failed: the error, or the last line of its stderr.
pub(crate) fn failure(outcome: Result<RemoteOutput, CtlError>) -> String {
    match outcome {
        Err(err) => err.to_string(),
        Ok(out) => String::from_utf8_lossy(&out.stderr)
            .lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(printable)
            .unwrap_or_else(|| format!("exited with {}", out.status)),
    }
}

/// Replaces control characters, so node output cannot drive the terminal.
fn printable(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(ssh: Option<&str>, nodes: &[&str]) -> Context {
        Context {
            ssh: ssh.map(str::to_string),
            nodes: nodes.iter().map(|n| n.to_string()).collect(),
            ..Context::default()
        }
    }

    fn names(nodes: &[Node<'_>]) -> Vec<String> {
        nodes.iter().map(|n| n.name().to_string()).collect()
    }

    #[test]
    fn targets_follow_flags_then_nodes_then_ssh_then_local() {
        let ctx = context(Some("mgr1"), &["n1", "n2"]);
        let flags = vec!["f1".to_string()];
        assert_eq!(names(&targets(&flags, &ctx).unwrap()), ["f1"]);
        assert_eq!(names(&targets(&[], &ctx).unwrap()), ["n1", "n2"]);
        let ssh_only = context(Some("mgr1"), &[]);
        assert_eq!(names(&targets(&[], &ssh_only).unwrap()), ["mgr1"]);
        let local = context(None, &[]);
        assert_eq!(names(&targets(&[], &local).unwrap()), ["local"]);
    }

    #[test]
    fn a_bad_target_is_refused_before_anything_runs() {
        let flags = vec!["ok".to_string(), "-oProxyCommand=id".to_string()];
        let err = targets(&flags, &Context::default()).unwrap_err();
        assert_eq!(err.exit_code(), 1);
    }
}
