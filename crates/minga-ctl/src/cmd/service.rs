//! `minga-ctl service` (plan section 5.3).

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, IsTerminal, Write};
use std::process::Stdio;

use clap::{ArgGroup, Subcommand};
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};

use crate::api::{self, ApiClient, Found};
use crate::cmd::image;
use crate::config::Resolved;
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::state;
use crate::transport::{self, Connection, RemoteArg};
use crate::view;

/// NATS URL for `minga logs` when the context sets none.
const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

/// Service commands.
#[derive(Debug, Subcommand)]
pub enum ServiceCommand {
    /// List services with their running and desired replicas.
    Ls {
        /// Only this stack's services.
        #[arg(long)]
        stack: Option<String>,
    },
    /// List a service's tasks.
    Ps {
        /// Service ID, `<stack>.<service>`.
        service: String,
    },
    /// Show a service's image, replicas, ports, networks, update policy and
    /// healthcheck, its tasks with state, restarts, exit code and last error,
    /// and the last lines of output of each task that is not running.
    Inspect {
        /// Service ID, `<stack>.<service>`.
        service: String,
        /// Lines of output to show for each task that is not running; 0
        /// means all. Fetched with `minga logs` on the node.
        #[arg(long, default_value_t = 20)]
        tail: u64,
    },
    /// Print the logs of a service's running tasks with `minga logs`.
    Logs {
        /// Service ID, `<stack>.<service>`.
        service: String,
        /// Keep printing new lines until interrupted.
        #[arg(short = 'f', long)]
        follow: bool,
        /// Lines to show from the end of each task's log; 0 means all.
        #[arg(long, default_value_t = 100)]
        tail: u64,
    },
    /// Set the replica count of a replicated service.
    Scale {
        /// Service ID, `<stack>.<service>`.
        service: String,
        /// Desired number of replicas.
        #[arg(long)]
        replicas: u64,
    },
    /// Change a service's image, environment or replicas.
    #[command(group(
        ArgGroup::new("change")
            .required(true)
            .multiple(true)
            .args(["image", "env_add", "env_rm", "replicas"])
    ))]
    Update {
        /// Service ID, `<stack>.<service>`.
        service: String,
        /// New image reference.
        #[arg(long)]
        image: Option<String>,
        /// Set an environment variable. Values are visible in process
        /// listings; use Minga secrets for credentials.
        #[arg(long = "env-add", value_name = "KEY=VALUE")]
        env_add: Vec<String>,
        /// Remove an environment variable.
        #[arg(long = "env-rm", value_name = "KEY")]
        env_rm: Vec<String>,
        /// Desired number of replicas.
        #[arg(long)]
        replicas: Option<u64>,
    },
    /// Remove a service. Its spec is saved first, so `service rollback`
    /// can bring it back.
    Rm {
        /// Service ID, `<stack>.<service>`.
        service: String,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
    /// Return a service to the spec saved before minga-ctl last changed it,
    /// or recreate one that minga-ctl removed.
    Rollback {
        /// Service ID, `<stack>.<service>`.
        service: String,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
}

impl ServiceCommand {
    /// The audit-log command name and arguments, for commands that change
    /// state. Environment values are left out: `--env-add` logs the key.
    pub fn audit(&self) -> Option<(&'static str, Value)> {
        match self {
            ServiceCommand::Ls { .. }
            | ServiceCommand::Ps { .. }
            | ServiceCommand::Inspect { .. }
            | ServiceCommand::Logs { .. } => None,
            ServiceCommand::Scale { service, replicas } => Some((
                "service scale",
                json!({ "service": service, "replicas": replicas }),
            )),
            ServiceCommand::Update {
                service,
                image,
                env_add,
                env_rm,
                replicas,
            } => Some((
                "service update",
                json!({
                    "service": service,
                    "image": image,
                    "env_add": env_add
                        .iter()
                        .map(|e| e.split_once('=').map_or(INVALID, |(k, _)| logged_key(k)))
                        .collect::<Vec<_>>(),
                    "env_rm": env_rm.iter().map(|k| logged_key(k)).collect::<Vec<_>>(),
                    "replicas": replicas,
                }),
            )),
            ServiceCommand::Rm { service, yes } => {
                Some(("service rm", json!({ "service": service, "yes": yes })))
            }
            ServiceCommand::Rollback { service, yes } => Some((
                "service rollback",
                json!({ "service": service, "yes": yes }),
            )),
        }
    }
}

/// Runs a service command.
pub async fn run(
    cmd: &ServiceCommand,
    conn: &Connection,
    resolved: &Resolved,
    debug: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let client = &conn.api;
    match cmd {
        ServiceCommand::Ls { stack } => ls(client, stack.as_deref(), format, quiet).await,
        ServiceCommand::Ps { service } => ps(client, service, format, quiet).await,
        ServiceCommand::Inspect { service, tail } => {
            inspect(client, resolved, service, *tail, debug, format, quiet).await
        }
        ServiceCommand::Logs {
            service,
            follow,
            tail,
        } => logs(client, resolved, service, *follow, *tail, debug).await,
        ServiceCommand::Scale { service, replicas } => {
            scale(client, service, *replicas, format, quiet).await
        }
        ServiceCommand::Update {
            service,
            image,
            env_add,
            env_rm,
            replicas,
        } => {
            let change = Change {
                image: image.as_deref(),
                env_add,
                env_rm,
                replicas: *replicas,
            };
            update(client, service, &change, format, quiet).await
        }
        ServiceCommand::Rm { service, yes } => rm(client, service, *yes, format, quiet).await,
        ServiceCommand::Rollback { service, yes } => {
            rollback(client, service, format, quiet, |summary| {
                confirm("service rollback", *yes, summary)
            })
            .await
        }
    }
}

/// `minga logs` for each running task, on the context's SSH target or here.
/// Every line gets the task ID in front; stderr stays on stderr.
async fn logs(
    client: &ApiClient,
    resolved: &Resolved,
    id: &str,
    follow: bool,
    tail: u64,
    debug: bool,
) -> Result<(), CtlError> {
    read(client, id).await?;
    let filter = json!({ "service": [id], "desired-state": ["running"] });
    let tasks = client.get("tasks", Some(&filter)).await?;
    let nats = resolved
        .context
        .nats_url
        .as_deref()
        .unwrap_or(DEFAULT_NATS_URL);
    let tail = tail.to_string();
    // Set once stdout is closed (`| head`): every stream then stops.
    let (closed, _) = tokio::sync::watch::channel(false);
    let mut running = Vec::new();
    for task in view::items(&tasks) {
        let task = view::id(task);
        transport::validate_id(task)?;
        let mut argv = vec![
            RemoteArg::Fixed("minga"),
            RemoteArg::Fixed("logs"),
            RemoteArg::Value(task),
            RemoteArg::Fixed("--tail"),
            RemoteArg::Value(&tail),
        ];
        if follow {
            argv.push(RemoteArg::Fixed("-f"));
        }
        argv.extend([RemoteArg::Fixed("--nats-url"), RemoteArg::Value(nats)]);
        let ssh = resolved.context.ssh.as_deref();
        let mut cmd = match ssh {
            Some(target) => transport::remote_command(target, &argv, debug)?,
            None => image::local_command(&argv, "service logs")?,
        };
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = cmd.spawn().map_err(|e| match ssh {
            Some(target) => CtlError::Ssh {
                target: target.to_string(),
                detail: format!("cannot run ssh: {e}"),
            },
            None => image::spawn_error(e, &argv, "service logs"),
        })?;
        running.push(tokio::spawn(stream(
            task.to_string(),
            child,
            closed.clone(),
        )));
    }
    if running.is_empty() {
        eprintln!("{id} has no running tasks");
    }
    let mut failed = Vec::new();
    for handle in running {
        let (task, status) = handle
            .await
            .map_err(|e| CtlError::Halted(format!("a log stream stopped: {e}")))?;
        if *closed.borrow() {
            // The reader went away; the streams were stopped on purpose.
            continue;
        }
        match status.map(|s| s.code()) {
            Ok(Some(0)) => {}
            Ok(Some(127)) => {
                return Err(CtlError::MissingBinary {
                    bin: "minga".to_string(),
                    command: "service logs".to_string(),
                })
            }
            Ok(Some(255)) if resolved.context.ssh.is_some() => {
                failed.push(format!("{task} (ssh failed)"))
            }
            Ok(code) => failed.push(format!(
                "{task} (exit {})",
                code.map_or("signal".to_string(), |c| c.to_string())
            )),
            Err(e) => failed.push(format!("{task} ({e})")),
        }
    }
    if failed.is_empty() {
        return Ok(());
    }
    Err(CtlError::Halted(format!(
        "`minga logs` failed for {}",
        failed.join(", ")
    )))
}

/// Copies a child's stdout and stderr line by line, each with `task | ` in
/// front, and waits for it. When stdout is closed, it sets `closed`, and on
/// `closed` (from any stream) it kills the child, so `-f | head` ends.
async fn stream(
    task: String,
    mut child: tokio::process::Child,
    closed: tokio::sync::watch::Sender<bool>,
) -> (String, std::io::Result<std::process::ExitStatus>) {
    let out = child.stdout.take().map(BufReader::new);
    let err = child.stderr.take().map(BufReader::new);
    let prefix = format!("{task} | ");
    let mut stop = closed.subscribe();
    let copy_out = async {
        if !copy_lines(out, &prefix, false).await {
            closed.send_replace(true);
        }
    };
    let wait = async {
        tokio::select! {
            status = child.wait() => status,
            // The guard `wait_for` returns is dropped here, before any await.
            _ = async { drop(stop.wait_for(|closed| *closed).await) } => {
                let _ = child.kill().await;
                child.wait().await
            }
        }
    };
    let (_, _, status) = tokio::join!(copy_out, copy_lines(err, &prefix, true), wait);
    (task, status)
}

/// Returns false if the output could not be written (a closed pipe).
async fn copy_lines(
    reader: Option<impl AsyncBufRead + Unpin>,
    prefix: &str,
    to_stderr: bool,
) -> bool {
    let Some(mut reader) = reader else {
        return true;
    };
    let mut line = Vec::new();
    while matches!(reader.read_until(b'\n', &mut line).await, Ok(n) if n > 0) {
        if !line.ends_with(b"\n") {
            line.push(b'\n');
        }
        let mut bytes = prefix.as_bytes().to_vec();
        bytes.append(&mut line);
        // One write per line keeps lines from different tasks whole.
        let written = if to_stderr {
            std::io::stderr().lock().write_all(&bytes)
        } else {
            std::io::stdout().lock().write_all(&bytes)
        };
        if written.is_err() {
            return false;
        }
    }
    true
}

/// `GET /services` and `GET /tasks`; REPLICAS is running tasks over desired.
async fn ls(
    client: &ApiClient,
    stack: Option<&str>,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let filter = match stack {
        Some(stack) => {
            api::check_id("stack", stack)?;
            Some(view::stack_filter(stack))
        }
        None => None,
    };
    let services = client.get("services", filter.as_ref()).await?;
    let tasks = client.get("tasks", filter.as_ref()).await?;
    let mut running: HashMap<&str, u64> = HashMap::new();
    for task in view::items(&tasks) {
        if task.pointer("/Status/State").and_then(Value::as_str) == Some("running") {
            let service = task["ServiceID"].as_str().unwrap_or("");
            *running.entry(service).or_default() += 1;
        }
    }

    let mut services: Vec<&Value> = view::items(&services).iter().collect();
    services.sort_by(|a, b| view::id(a).cmp(view::id(b)));
    let mut rows = Rows::new(&["ID", "STACK", "MODE", "REPLICAS", "IMAGE"]);
    for service in services {
        let id = view::id(service);
        let stack = view::stack_of(service).unwrap_or("-");
        let (mode, desired) = view::mode(service);
        let up = running.get(id).copied().unwrap_or(0);
        let replicas = match desired {
            Some(desired) => format!("{up}/{desired}"),
            None => up.to_string(),
        };
        let image = view::image(service);
        let mut value = json!({
            "id": id, "stack": stack, "mode": mode, "running": up, "image": image,
        });
        if let Some(desired) = desired {
            value["desired"] = json!(desired);
        }
        rows.push(
            [id, stack, mode, &replicas, image]
                .map(str::to_string)
                .to_vec(),
            value,
        );
    }
    output::print(format, quiet, &rows)
}

/// `GET /services/{id}` (404: exit 3), then that service's tasks.
async fn ps(client: &ApiClient, id: &str, format: Format, quiet: bool) -> Result<(), CtlError> {
    let service = read(client, id).await?;
    let tasks = client
        .get("tasks", Some(&json!({ "service": [id] })))
        .await?;
    let nodes = view::node_names(&client.get("nodes", None).await?);
    let rows = view::task_rows(&tasks, std::slice::from_ref(&service), &nodes);
    output::print(format, quiet, &rows)?;
    if !quiet {
        if let Some(hint) = view::not_running_hint(&tasks) {
            eprintln!("{hint}");
        }
    }
    Ok(())
}

/// How many `minga logs` run at once for `inspect`.
static LOGS_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// How long one `minga logs` may take before its task is reported as skipped.
/// The wait for a slot is not counted.
const LOGS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// `GET /services/{id}` (404: exit 3), then that service's tasks and, for
/// each task that is not running, the end of its output.
async fn inspect(
    client: &ApiClient,
    resolved: &Resolved,
    id: &str,
    tail: u64,
    debug: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let service = read(client, id).await?;
    let tasks = client
        .get("tasks", Some(&json!({ "service": [id] })))
        .await?;
    let nodes = view::node_names(&client.get("nodes", None).await?);
    let mut rows = view::inspect_task_rows(&tasks, &nodes);
    let summary = view::spec_summary(&service);
    // Quiet prints the task IDs only, like `ps`: no node is contacted.
    if quiet {
        return match format {
            Format::Table => output::print(format, quiet, &rows),
            _ => Ok(()),
        };
    }

    let mut outputs = Vec::new(); // (row index, output)
    for (i, task) in rows.values.iter().enumerate() {
        if task["state"] != "running" {
            let task = task["id"].as_str().unwrap_or("").to_string();
            outputs.push((i, task_output(resolved, task, tail, debug)));
        }
    }
    for (i, output) in outputs {
        let output = output.await;
        rows.values[i]["logs"] = match &output {
            Ok((stdout, stderr)) => json!({ "stdout": stdout, "stderr": stderr, "error": null }),
            Err(why) => json!({ "stdout": [], "stderr": [], "error": why }),
        };
    }

    match format {
        Format::Table => {
            output::say(false, &view::spec_text(&summary))?;
            if !rows.is_empty() {
                output::say(false, "")?;
                output::print(format, quiet, &rows)?;
            }
            for task in &rows.values {
                if task["logs"].is_object() {
                    output::say(false, &logs_text(task, tail))?;
                }
            }
            Ok(())
        }
        _ => report(
            format,
            quiet,
            "",
            json!({ "service": summary, "tasks": rows.values }),
        ),
    }
}

/// One task's output as `minga logs` printed it: (stdout lines, stderr
/// lines), or why it could not be read (unreachable node, no `minga`).
type TaskOutput = Result<(Vec<String>, Vec<String>), String>;

/// Runs `minga logs <task> --tail N` on the context's SSH target or here,
/// the way `service logs` does, and waits for it.
fn task_output(
    resolved: &Resolved,
    task: String,
    tail: u64,
    debug: bool,
) -> impl std::future::Future<Output = TaskOutput> {
    let ssh = resolved.context.ssh.clone();
    let nats = resolved
        .context
        .nats_url
        .clone()
        .unwrap_or_else(|| DEFAULT_NATS_URL.to_string());
    let prepared = (|| {
        transport::validate_id(&task)?;
        let tail = tail.to_string();
        let argv = [
            RemoteArg::Fixed("minga"),
            RemoteArg::Fixed("logs"),
            RemoteArg::Value(&task),
            RemoteArg::Fixed("--tail"),
            RemoteArg::Value(&tail),
            RemoteArg::Fixed("--nats-url"),
            RemoteArg::Value(&nats),
        ];
        let mut cmd = match &ssh {
            Some(target) => transport::remote_command(target, &argv, debug)?,
            None => image::local_command(&argv, "service inspect")?,
        };
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok::<_, CtlError>(cmd)
    })();
    // Started now, so the tasks' commands run together.
    let handle = prepared.map(|mut cmd| {
        tokio::spawn(async move {
            // A few ssh logins at a time: many at once can exceed the
            // node's `MaxStartups` and look like an unreachable node.
            let _permit = LOGS_SLOTS.acquire().await;
            tokio::time::timeout(LOGS_TIMEOUT, cmd.output()).await
        })
    });
    async move {
        let out = match handle {
            Err(e) => return Err(e.to_string()),
            Ok(handle) => match handle.await {
                Ok(Ok(Ok(out))) => out,
                Ok(Ok(Err(e))) => return Err(format!("cannot run minga logs: {e}")),
                Ok(Err(_)) => return Err("timed out".to_string()),
                Err(e) => return Err(e.to_string()),
            },
        };
        match out.status.code() {
            Some(0) => {
                // `minga logs --tail` counts log entries, which may hold many
                // lines, so the last `tail` lines are taken here.
                let lines = |bytes: &[u8]| -> Vec<String> {
                    let text = String::from_utf8_lossy(bytes);
                    let all: Vec<&str> = text.lines().collect();
                    let from = if tail == 0 {
                        0
                    } else {
                        all.len().saturating_sub(tail as usize)
                    };
                    all[from..].iter().map(|l| l.to_string()).collect()
                };
                Ok((lines(&out.stdout), lines(&out.stderr)))
            }
            Some(127) => Err("`minga` not found on the node".to_string()),
            Some(255) if ssh.is_some() => Err("node unreachable (ssh failed)".to_string()),
            Some(code) => Err(format!("`minga logs` exited {code}")),
            None => Err("`minga logs` was killed".to_string()),
        }
    }
}

/// The text form of one task's `logs` field: each line labelled with its
/// stream, control characters (a container controls its own output) shown as `?`.
fn logs_text(task: &Value, tail: u64) -> String {
    let id = task["id"].as_str().unwrap_or("");
    let last = if tail == 0 {
        "output".to_string()
    } else {
        format!("last {tail} lines of output")
    };
    let logs = &task["logs"];
    let mut text = format!("\n{id} ({}): {last}", task["state"].as_str().unwrap_or("-"));
    if let Some(why) = logs["error"].as_str() {
        text.push_str(&format!("\n  skipped: {why}"));
        return text;
    }
    let mut empty = true;
    for stream in ["stdout", "stderr"] {
        for line in logs[stream].as_array().into_iter().flatten() {
            empty = false;
            let line: String = line
                .as_str()
                .unwrap_or("")
                .chars()
                .map(|c| if c.is_control() && c != '\t' { '?' } else { c })
                .collect();
            text.push_str(&format!("\n  [{stream}] {line}"));
        }
    }
    if empty {
        text.push_str("\n  (no output)");
    }
    text
}

/// `DELETE /services/{id}` after saving `spec` as the revision, so a service
/// is never gone without one. If the save fails nothing is deleted; if the
/// API refuses the delete the revision it replaced is put back; if the
/// outcome is unknown (no answer) the new revision stays.
pub(crate) async fn delete_saving(
    client: &ApiClient,
    id: &str,
    spec: &Value,
) -> Result<Found, CtlError> {
    // An unreadable revision file is not worth keeping.
    let old = state::load_revision(&client.context, id).ok().flatten();
    state::save_revision(&client.context, id, spec).map_err(|err| {
        CtlError::Halted(format!(
            "cannot save the revision of {id} ({err}); nothing was removed"
        ))
    })?;
    let result = client.delete(&format!("services/{id}")).await;
    // Put the old revision back only when the server answered that it did not
    // delete. A lost response (or an already gone service) may mean it did, and
    // then the new revision is the only way to recreate it.
    let refused = match &result {
        // A 2xx the client could not read (not JSON) is still a delete.
        Err(CtlError::Api { status, .. }) => !(200..300).contains(status),
        Err(CtlError::Auth { .. }) => true,
        _ => false,
    };
    if refused {
        // Best effort: a revision left at the current spec rolls back to itself.
        let _ = crate::cmd::stack::restore(client, id, old);
    }
    result
}

/// `rm`: read (404: exit 3), confirm, save the spec as the revision so
/// `rollback` can recreate the service, then `DELETE`.
async fn rm(
    client: &ApiClient,
    id: &str,
    yes: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let service = read(client, id).await?;
    let spec = &service["Spec"];
    let (mode, desired) = view::mode(&service);
    let replicas = desired.map_or(String::new(), |n| format!(" x{n}"));
    confirm(
        "service rm",
        yes,
        &format!(
            "This removes service {id} ({mode}{replicas}, {}) from context `{}`; \
             `service rollback` can bring it back.",
            view::image(&service),
            client.context
        ),
    )?;
    if let Found::No = delete_saving(client, id, spec).await? {
        return Err(not_found(client, "service", id));
    }
    report(
        format,
        quiet,
        &format!("{id} removed; `minga-ctl service rollback {id}` recreates it"),
        json!({ "service": id, "removed": true }),
    )
}

/// `scale`: read, set the replicas in a copy, post only if it changed.
async fn scale(
    client: &ApiClient,
    id: &str,
    replicas: u64,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let (old, _, changed) = edit(client, id, |spec| set_replicas(spec, id, replicas)).await?;
    let from = replicas_of(&old);
    let text = if !changed {
        format!("{id} already has {replicas} replicas; nothing changed")
    } else {
        let from = from.map_or("-".to_string(), |n| n.to_string());
        format!("{id} scaled from {from} to {replicas}")
    };
    let value = json!({
        "service": id,
        "replicas": { "from": from, "to": replicas },
        "changed": changed,
    });
    report(format, quiet, &text, value)
}

/// The edits `update` asks for.
struct Change<'a> {
    image: Option<&'a str>,
    env_add: &'a [String],
    env_rm: &'a [String],
    replicas: Option<u64>,
}

/// `update`: the same sequence as `scale`, for image, environment and replicas.
async fn update(
    client: &ApiClient,
    id: &str,
    change: &Change<'_>,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    // Check every flag before the first request. A bad argument may be a
    // value typed in the wrong place, so it is named by position, never echoed.
    if let Some(i) = change
        .env_add
        .iter()
        .position(|e| !e.split_once('=').is_some_and(|(k, _)| valid_key(k)))
    {
        return Err(CtlError::Usage(format!(
            "--env-add argument {} is not KEY=VALUE",
            i + 1
        )));
    }
    if let Some(i) = change.env_rm.iter().position(|k| !valid_key(k)) {
        return Err(CtlError::Usage(format!(
            "--env-rm argument {} is not a KEY",
            i + 1
        )));
    }

    let (old, new, changed) = edit(client, id, |spec| {
        if let Some(n) = change.replicas {
            set_replicas(spec, id, n)?;
        }
        let container = spec
            .pointer_mut("/TaskTemplate/ContainerSpec")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                CtlError::Usage(format!("service `{id}` has no container spec to update"))
            })?;
        if let Some(image) = change.image {
            container.insert("Image".to_string(), json!(image));
        }
        // Docker's order: removals first, then additions. Removing from a
        // service without `Env` leaves it without one.
        if let Some(env) = container.get_mut("Env").and_then(Value::as_array_mut) {
            env.retain(|e| !change.env_rm.iter().any(|k| k == env_key_of(e)));
        }
        if !change.env_add.is_empty() {
            let env = container.entry("Env").or_insert(Value::Null);
            if env.is_null() {
                *env = json!([]);
            }
            let env = env.as_array_mut().ok_or_else(|| {
                CtlError::Usage(format!("service `{id}` has an Env that is not a list"))
            })?;
            for entry in change.env_add {
                let key = env_key(entry);
                match env.iter_mut().find(|e| env_key_of(e) == key) {
                    Some(existing) => *existing = json!(entry),
                    None => env.push(json!(entry)),
                }
            }
        }
        Ok(())
    })
    .await?;

    let changes = describe(&old, &new);
    let text = if !changed {
        format!("{id} already matches; nothing changed")
    } else {
        let list: Vec<String> = changes.iter().map(|(l, c)| format!("{l} {c}")).collect();
        format!("{id} updated: {}", list.join(", "))
    };
    let value = json!({
        "service": id,
        "changed": changed,
        "changes": changes.into_iter().collect::<BTreeMap<_, _>>(),
    });
    report(format, quiet, &text, value)
}

/// `rollback`: post the saved revision after `confirm` agrees and the service
/// has not changed since it was read. On success the spec it replaced becomes
/// the new revision, so a second rollback returns to where it started. A
/// service that is gone (`service rm`, `stack deploy --prune`) is created
/// again from its revision.
pub async fn rollback(
    client: &ApiClient,
    id: &str,
    format: Format,
    quiet: bool,
    confirm: impl FnOnce(&str) -> Result<(), CtlError>,
) -> Result<(), CtlError> {
    api::check_id("service", id)?;
    let revision = state::load_revision(&client.context, id)?
        .ok_or_else(|| not_found(client, "revision", id))?;
    let first = match client.get_found(&format!("services/{id}"), None).await? {
        Found::Yes(service) => service,
        Found::No => return recreate(client, id, &revision, format, quiet, confirm).await,
    };
    let current = &first["Spec"];
    if same_spec(&revision.spec, current) {
        let text = format!("{id} already matches the saved revision; nothing changed");
        return report(
            format,
            quiet,
            &text,
            json!({ "service": id, "rolled_back": false }),
        );
    }

    let mut summary = format!(
        "Roll back {id} in context `{}` to the revision saved {}:",
        client.context, revision.saved_at
    );
    for (label, change) in describe(current, &revision.spec) {
        summary.push_str(&format!("\n  {label}: {change}"));
    }
    confirm(&summary)?;

    let second = read(client, id).await?;
    if !same_spec(&second["Spec"], current) {
        return Err(CtlError::Halted(format!(
            "service `{id}` changed while waiting for confirmation; run rollback again"
        )));
    }
    post_spec(client, id, &second, &revision.spec).await?;
    save_revision(client, id, current)?;
    report(
        format,
        quiet,
        &format!("{id} rolled back"),
        json!({ "service": id, "rolled_back": true }),
    )
}

/// `rollback` of a removed service: `POST /services/create` with the revision.
/// The revision stays, and now equals the service.
async fn recreate(
    client: &ApiClient,
    id: &str,
    revision: &state::Revision,
    format: Format,
    quiet: bool,
    confirm: impl FnOnce(&str) -> Result<(), CtlError>,
) -> Result<(), CtlError> {
    confirm(&format!(
        "Recreate {id} in context `{}` from the revision saved {}:\n  image: {}",
        client.context,
        revision.saved_at,
        revision
            .spec
            .pointer("/TaskTemplate/ContainerSpec/Image")
            .and_then(Value::as_str)
            .unwrap_or("-"),
    ))?;
    // A 409 here means the service came back while waiting for the answer.
    client.post("services/create", &revision.spec).await?;
    report(
        format,
        quiet,
        &format!("{id} recreated"),
        json!({ "service": id, "rolled_back": true, "recreated": true }),
    )
}

/// `GET /services/{id}`; a 404 is exit 3.
pub(crate) async fn read(client: &ApiClient, id: &str) -> Result<Value, CtlError> {
    api::check_id("service", id)?;
    match client.get_found(&format!("services/{id}"), None).await? {
        Found::Yes(service) => Ok(service),
        Found::No => Err(not_found(client, "service", id)),
    }
}

fn not_found(client: &ApiClient, kind: &'static str, id: &str) -> CtlError {
    CtlError::NotFound {
        kind,
        name: id.to_string(),
        context: client.context.clone(),
    }
}

/// Reads the service, applies `change` to a copy of its spec and, only if the
/// copy differs, posts it and then saves the spec as read as the revision.
/// Returns the spec as read, the edited copy and whether it changed.
async fn edit(
    client: &ApiClient,
    id: &str,
    change: impl FnOnce(&mut Value) -> Result<(), CtlError>,
) -> Result<(Value, Value, bool), CtlError> {
    let service = read(client, id).await?;
    let old = service["Spec"].clone();
    let mut new = old.clone();
    change(&mut new)?;
    let changed = !same_spec(&new, &old);
    if changed {
        post_spec(client, id, &service, &new).await?;
        save_revision(client, id, &old)?;
    }
    Ok((old, new, changed))
}

/// `POST /services/{id}/update?version=<Version.Index>` with the whole spec.
/// The server ignores `version` today; it is sent so the call stays correct
/// once the server checks it.
async fn post_spec(
    client: &ApiClient,
    id: &str,
    service: &Value,
    spec: &Value,
) -> Result<(), CtlError> {
    let version = service
        .pointer("/Version/Index")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    client
        .post(&format!("services/{id}/update?version={version}"), spec)
        .await
        .map(drop)
}

/// Saves the revision once the API has accepted the change. If that fails,
/// the old revision file is removed: it now holds a spec two changes back,
/// and a rollback to it would be wrong.
pub(crate) fn save_revision(client: &ApiClient, id: &str, spec: &Value) -> Result<(), CtlError> {
    let Err(err) = state::save_revision(&client.context, id, spec) else {
        return Ok(());
    };
    let fate = match state::remove_revision(&client.context, id) {
        Ok(()) => "the previous revision was removed, so rollback has nothing to restore".to_string(),
        Err(e) => format!(
            "the previous revision could not be removed either ({e}); delete it before any rollback, which would restore an older spec"
        ),
    };
    Err(CtlError::Halted(format!(
        "{id} was changed, but its revision could not be saved ({err}); {fate}"
    )))
}

fn set_replicas(spec: &mut Value, id: &str, replicas: u64) -> Result<(), CtlError> {
    match spec.pointer_mut("/Mode/Replicated") {
        Some(Value::Object(mode)) => {
            mode.insert("Replicas".to_string(), json!(replicas));
            Ok(())
        }
        _ => Err(CtlError::Usage(format!(
            "service `{id}` is global and cannot be scaled"
        ))),
    }
}

fn replicas_of(spec: &Value) -> Option<u64> {
    spec.pointer("/Mode/Replicated/Replicas")
        .and_then(Value::as_u64)
}

/// The key of a `KEY=VALUE` (or bare `KEY`) environment entry.
fn env_key(entry: &str) -> &str {
    entry.split_once('=').map_or(entry, |(key, _)| key)
}

fn env_key_of(entry: &Value) -> &str {
    env_key(entry.as_str().unwrap_or(""))
}

/// What the audit log records in place of an argument that is not a key.
const INVALID: &str = "<invalid>";

/// A key that is safe to print and log: no `=`, whitespace or control
/// characters, so it cannot be a `KEY=VALUE` or a stray value.
fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key
            .chars()
            .any(|c| c == '=' || c.is_whitespace() || c.is_control())
}

fn logged_key(key: &str) -> &str {
    if valid_key(key) {
        key
    } else {
        INVALID
    }
}

/// Compares specs the way the server stores them: Minga keeps the
/// environment in a map, so the order of `Env` can differ between two reads,
/// and an empty `Env` is the same as none.
pub(crate) fn same_spec(a: &Value, b: &Value) -> bool {
    let normal = |spec: &Value| {
        let mut spec = spec.clone();
        if let Some(container) = spec
            .pointer_mut("/TaskTemplate/ContainerSpec")
            .and_then(Value::as_object_mut)
        {
            match container.get_mut("Env") {
                Some(Value::Array(env)) if !env.is_empty() => {
                    env.sort_by(|x, y| x.as_str().cmp(&y.as_str()))
                }
                Some(Value::Array(_) | Value::Null) => {
                    container.remove("Env");
                }
                _ => {}
            }
        }
        spec
    };
    normal(a) == normal(b)
}

/// What differs from `old` to `new`: image, replicas and the number of
/// changed environment keys (values are not shown).
fn describe(old: &Value, new: &Value) -> Vec<(String, String)> {
    let mut changes = Vec::new();
    let image = |s: &Value| {
        s.pointer("/TaskTemplate/ContainerSpec/Image")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string()
    };
    if image(old) != image(new) {
        changes.push((
            "image".to_string(),
            format!("{} -> {}", image(old), image(new)),
        ));
    }
    let replicas = |s: &Value| replicas_of(s).map_or("-".to_string(), |n| n.to_string());
    if replicas(old) != replicas(new) {
        changes.push((
            "replicas".to_string(),
            format!("{} -> {}", replicas(old), replicas(new)),
        ));
    }
    let env = |s: &Value| -> BTreeMap<String, String> {
        s.pointer("/TaskTemplate/ContainerSpec/Env")
            .map(view::items)
            .unwrap_or_default()
            .iter()
            .filter_map(Value::as_str)
            .map(|e| (env_key(e).to_string(), e.to_string()))
            .collect()
    };
    let (before, after) = (env(old), env(new));
    let keys = before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|k| before.get(*k) != after.get(*k))
        .count();
    if keys > 0 {
        let noun = if keys == 1 { "key" } else { "keys" };
        changes.push(("environment".to_string(), format!("{keys} {noun} changed")));
    }
    if changes.is_empty() && !same_spec(old, new) {
        changes.push(("other fields".to_string(), "changed".to_string()));
    }
    changes
}

/// Asks before a command that removes or replaces things (plan 2.2 item 11).
/// `--yes` skips the question; without a terminal and without `--yes` the
/// command fails.
pub(crate) fn confirm(command: &str, yes: bool, summary: &str) -> Result<(), CtlError> {
    if yes {
        return Ok(());
    }
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Err(CtlError::Usage(format!(
            "`{command}` needs confirmation; pass --yes to run without a terminal"
        )));
    }
    eprint!("{summary}\nContinue? [y/N] ");
    let mut answer = String::new();
    stdin
        .lock()
        .read_line(&mut answer)
        .map_err(|e| CtlError::Usage(format!("cannot read the answer: {e}")))?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => Ok(()),
        _ => Err(CtlError::Usage("cancelled; nothing changed".to_string())),
    }
}

/// Prints the result of a change: `text` for a table, `value` for JSON or
/// YAML, nothing with `--quiet`.
pub(crate) fn report(
    format: Format,
    quiet: bool,
    text: &str,
    value: Value,
) -> Result<(), CtlError> {
    match format {
        _ if quiet => Ok(()),
        Format::Table => output::say(false, text),
        Format::Json => output::say(false, &serde_json::to_string_pretty(&value)?),
        Format::Yaml => {
            let yaml =
                serde_yaml::to_string(&value).map_err(|e| CtlError::Output(e.to_string()))?;
            output::say(false, yaml.trim_end())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(image: &str, replicas: u64, env: &[&str]) -> Value {
        json!({
            "Name": "app.web",
            "TaskTemplate": { "ContainerSpec": { "Image": image, "Env": env } },
            "Mode": { "Replicated": { "Replicas": replicas } }
        })
    }

    #[test]
    fn env_order_does_not_make_specs_differ() {
        assert!(same_spec(
            &spec("a", 1, &["A=1", "B=2"]),
            &spec("a", 1, &["B=2", "A=1"])
        ));
        assert!(!same_spec(&spec("a", 1, &["A=1"]), &spec("a", 1, &["A=2"])));
    }

    #[test]
    fn changes_are_described_without_values() {
        let changes = describe(
            &spec("web:1", 2, &["A=1", "B=2"]),
            &spec("web:2", 3, &["A=secret", "C=3"]),
        );
        assert_eq!(
            changes,
            [
                ("image".to_string(), "web:1 -> web:2".to_string()),
                ("replicas".to_string(), "2 -> 3".to_string()),
                ("environment".to_string(), "3 keys changed".to_string()),
            ]
        );
        assert!(!format!("{changes:?}").contains("secret"));
    }

    #[test]
    fn a_global_service_cannot_be_scaled() {
        let mut global = json!({ "Mode": { "Global": {} } });
        let err = set_replicas(&mut global, "shop.proxy", 2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "service `shop.proxy` is global and cannot be scaled"
        );
    }

    #[test]
    fn an_empty_env_is_the_same_as_none() {
        let mut none = spec("a", 1, &[]);
        none["TaskTemplate"]["ContainerSpec"]
            .as_object_mut()
            .unwrap()
            .remove("Env");
        assert!(same_spec(&none, &spec("a", 1, &[])));
        assert!(!same_spec(&none, &spec("a", 1, &["A=1"])));
    }

    #[test]
    fn only_plain_keys_are_printed_or_logged() {
        assert_eq!(logged_key("API_URL"), "API_URL");
        for bad in ["", "TOKEN=abc", "two words", "tab\there"] {
            assert_eq!(logged_key(bad), INVALID, "{bad:?}");
        }
    }

    #[test]
    fn env_keys_are_split_at_the_first_equals_sign() {
        assert_eq!(env_key("A=b=c"), "A");
        assert_eq!(env_key("BARE"), "BARE");
    }
}
