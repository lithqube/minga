//! `minga-ctl stack` (plan section 5.2).

use std::collections::BTreeMap;
use std::path::Path;

use clap::Subcommand;
use serde_json::{json, Value};
use tokio::net::{TcpListener, UnixStream};

use crate::api::{self, ApiClient};
use crate::cmd::image;
use crate::cmd::service::{self, confirm, report};
use crate::config::{self, Resolved};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::state;
use crate::transport::{Connection, RemoteArg};
use crate::view;

/// Stack commands.
#[derive(Debug, Subcommand)]
pub enum StackCommand {
    /// Deploy a Compose file with `minga deploy`, saving the previous spec
    /// of every service it changes for `service rollback`.
    Deploy {
        /// Path to the Compose file.
        #[arg(short = 'c', long = "compose-file", default_value = "compose.yaml")]
        compose_file: String,
        /// Profile to enable; repeat for several.
        #[arg(short = 'p', long = "profile", value_name = "PROFILE")]
        profiles: Vec<String>,
        /// Seconds `minga deploy` waits for tasks to be placed.
        #[arg(long, default_value_t = 30)]
        wait: u64,
        /// Name of the stack.
        stack: String,
    },
    /// List the stacks: services grouped by their stack label.
    Ls,
    /// List a stack's tasks.
    Ps {
        /// Name of the stack.
        stack: String,
    },
    /// Remove a stack's services, and the networks, configs or secrets named
    /// with `--with`.
    Rm {
        /// Name of the stack.
        stack: String,
        /// Also remove this network, config or secret. It must start with
        /// `<stack>_` and no service outside the stack may use it.
        #[arg(long = "with", value_name = "ID")]
        with: Vec<String>,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
}

impl StackCommand {
    /// The audit-log command name and arguments, for commands that change state.
    pub fn audit(&self) -> Option<(&'static str, Value)> {
        match self {
            StackCommand::Ls | StackCommand::Ps { .. } => None,
            StackCommand::Deploy {
                compose_file,
                profiles,
                wait,
                stack,
            } => Some((
                "stack deploy",
                json!({ "stack": stack, "compose_file": compose_file, "profiles": profiles, "wait": wait }),
            )),
            StackCommand::Rm { stack, with, yes } => Some((
                "stack rm",
                json!({ "stack": stack, "with": with, "yes": yes }),
            )),
        }
    }

    /// Checks that run before connecting: `deploy` step 0, the bind-mount
    /// guard, when the node is another machine.
    pub fn preflight(&self, resolved: &Resolved) -> Result<(), CtlError> {
        match self {
            StackCommand::Deploy { compose_file, .. } if node_is_remote(resolved) => {
                check_bind_mounts(compose_file)
            }
            _ => Ok(()),
        }
    }
}

/// The node is another machine with an SSH target, or with an API endpoint
/// that is not a loopback address (an `https://` proxy, for example).
fn node_is_remote(resolved: &Resolved) -> bool {
    resolved.context.ssh.is_some() || !is_loopback(&resolved.endpoint)
}

fn is_loopback(endpoint: &str) -> bool {
    let rest = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest);
    let host = config::host_of(rest.split(['/', '?', '#']).next().unwrap_or(""));
    host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "::1"
}

/// Refuses a Compose file with a relative bind mount: `minga deploy` would
/// resolve it on this machine, not on the node.
fn check_bind_mounts(file: &str) -> Result<(), CtlError> {
    let text = std::fs::read_to_string(file)
        .map_err(|e| CtlError::Usage(format!("cannot read {file}: {e}")))?;
    let doc: serde_yaml::Value = serde_yaml::from_str(&text)
        .map_err(|e| CtlError::Usage(format!("{file} is not valid YAML: {e}")))?;
    match relative_bind(&doc) {
        Some((service, source)) => Err(CtlError::Hinted {
            code: 1,
            message: format!(
                "{file} bind-mounts {source} (service `{service}`), which would resolve on this machine, not on the node"
            ),
            hint: "use an absolute path on the node, or run minga-ctl on the node with a local context"
                .to_string(),
        }),
        None => Ok(()),
    }
}

/// The first `(service, source)` whose volume source is relative.
/// shortcut: reads the plain file only; `extends`, `include` and variable
/// interpolation are not followed (plan 5.2). Upgrade if those show up in
/// remote deploys.
fn relative_bind(doc: &serde_yaml::Value) -> Option<(String, String)> {
    for (name, service) in doc.get("services")?.as_mapping()? {
        let Some(volumes) = service.get("volumes").and_then(|v| v.as_sequence()) else {
            continue;
        };
        for volume in volumes {
            let source = match volume {
                // `./conf:/etc/app[:ro]`; a bare `/data` is an anonymous volume.
                serde_yaml::Value::String(short) => short.split_once(':').map(|(s, _)| s),
                long => long.get("source").and_then(|s| s.as_str()),
            };
            if let Some(source) = source.filter(|s| is_relative(s)) {
                let name = name.as_str().unwrap_or("?").to_string();
                return Some((name, source.to_string()));
            }
        }
    }
    None
}

fn is_relative(source: &str) -> bool {
    matches!(source, "." | "..")
        || source.starts_with("./")
        || source.starts_with("../")
        || source.starts_with('~')
}

/// `deploy` steps 1 to 4 (plan 5.2).
async fn deploy(
    conn: &Connection,
    compose_file: &str,
    profiles: &[String],
    wait: u64,
    stack: &str,
    quiet: bool,
) -> Result<(), CtlError> {
    let client = &conn.api;
    api::check_id("stack", stack)?;

    // 1. The specs as they are now, saved as revisions before the deploy, so
    // a deploy that is interrupted or cannot be read back never leaves a
    // revision from an older change to roll back to.
    let filter = view::stack_filter(stack);
    let before = specs(&client.get("services", Some(&filter)).await?);
    let earlier = save_before(client, &before)?;

    // 2. `minga deploy`, the token in its environment only.
    let relay = match conn.api_socket() {
        Some(sock) => Some(relay(sock).await?),
        None => None,
    };
    let url = relay
        .as_ref()
        .map_or_else(|| client.root().to_string(), |(url, _)| url.clone());
    let mut cmd = tokio::process::Command::new(image::minga_bin());
    cmd.args(["deploy", "-c", compose_file]);
    for profile in profiles {
        cmd.args(["-p", profile]);
    }
    cmd.args(["--wait", &wait.to_string(), "--api-url", &url, stack])
        .env_remove("MINGA_CTL_TOKEN")
        .env_remove("MINGA_API_URL")
        .kill_on_drop(true);
    match client.token() {
        Some(token) => cmd.env("MINGA_API_TOKEN", token),
        None => cmd.env_remove("MINGA_API_TOKEN"),
    };
    let status = cmd.status().await;
    if let Some((_, task)) = relay {
        task.abort();
    }
    let status =
        status.map_err(|e| image::spawn_error(e, &[RemoteArg::Fixed("minga")], "stack deploy"))?;

    // 3. Read again whether or not it succeeded; services it did not change
    // get their earlier revision back.
    let problem = match settle(client, &filter, &before, earlier).await {
        Ok((changes, errors)) => {
            output::say(quiet, &changes.summary())?;
            (!errors.is_empty()).then(|| errors.join("; "))
        }
        Err(err) => Some(err.to_string()),
    };

    // 4. The exit code.
    if !status.success() {
        let mut message = format!("minga deploy failed ({status})");
        if let Some(reason) = problem {
            message.push_str(&format!("; could not save revisions for {stack}: {reason}"));
        }
        return Err(CtlError::Halted(message));
    }
    match problem {
        Some(reason) => Err(CtlError::Hinted {
            code: 2,
            message: format!("deployed, but could not save revisions for {stack}: {reason}"),
            hint: "each service's revision is its spec from before this deploy; for a \
                   service the deploy did not change, rollback restores that same spec"
                .to_string(),
        }),
        None => Ok(()),
    }
}

/// Service ID to `Spec`.
fn specs(services: &Value) -> BTreeMap<String, Value> {
    view::items(services)
        .iter()
        .map(|s| (view::id(s).to_string(), s["Spec"].clone()))
        .collect()
}

/// What a deploy did to the stack's services.
#[derive(Default)]
struct Changes {
    saved: Vec<String>,
    unchanged: Vec<String>,
    new: Vec<String>,
}

impl Changes {
    /// `saved revision for myapp.web (myapp.worker unchanged)`.
    fn summary(&self) -> String {
        let mut text = if self.saved.is_empty() {
            "no revision saved".to_string()
        } else {
            format!("saved revision for {}", self.saved.join(", "))
        };
        let mut notes = Vec::new();
        if !self.unchanged.is_empty() {
            notes.push(format!("{} unchanged", self.unchanged.join(", ")));
        }
        if !self.new.is_empty() {
            notes.push(format!("{} new", self.new.join(", ")));
        }
        if !notes.is_empty() {
            text.push_str(&format!(" ({})", notes.join("; ")));
        }
        text
    }
}

/// The revisions [`save_before`] replaced, by service id.
type Earlier = BTreeMap<String, Option<state::Revision>>;

/// Step 1: saves each service's current spec as its revision and returns the
/// revisions it replaced. If one save fails, nothing is deployed and the
/// revisions already replaced are put back.
fn save_before(client: &ApiClient, before: &BTreeMap<String, Value>) -> Result<Earlier, CtlError> {
    let mut earlier = Earlier::new();
    for (id, spec) in before {
        // An unreadable revision file is not worth keeping.
        let old = state::load_revision(&client.context, id).ok().flatten();
        earlier.insert(id.clone(), old);
        if let Err(err) = state::save_revision(&client.context, id, spec) {
            for (id, old) in earlier {
                // Best effort: a revision left at the current spec rolls back to itself.
                let _ = restore(client, &id, old);
            }
            return Err(CtlError::Halted(format!(
                "cannot save the revision of {id} before deploying ({err}); nothing was deployed"
            )));
        }
    }
    Ok(earlier)
}

fn restore(client: &ApiClient, id: &str, old: Option<state::Revision>) -> Result<(), CtlError> {
    match old {
        Some(revision) => state::put_revision(&client.context, &revision),
        None => state::remove_revision(&client.context, id),
    }
}

/// Step 3: a service whose spec changed keeps the step 1 revision; one the
/// deploy did not change (or that is gone) gets its earlier revision back.
/// Returns the changes and the revision writes that failed.
async fn settle(
    client: &ApiClient,
    filter: &Value,
    before: &BTreeMap<String, Value>,
    earlier: Earlier,
) -> Result<(Changes, Vec<String>), CtlError> {
    let after = specs(&client.get("services", Some(filter)).await?);
    let mut changes = Changes::default();
    let mut errors = Vec::new();
    for id in after.keys().filter(|id| !before.contains_key(*id)) {
        changes.new.push(id.clone());
    }
    for (id, old) in earlier {
        let now = after.get(&id);
        if now.is_some_and(|spec| !service::same_spec(&before[&id], spec)) {
            changes.saved.push(id);
            continue;
        }
        if let Err(err) = restore(client, &id, old) {
            errors.push(format!(
                "cannot put back the earlier revision of {id} ({err})"
            ));
        }
        if now.is_some() {
            changes.unchanged.push(id);
        }
    }
    Ok((changes, errors))
}

/// `minga deploy` needs an HTTP URL, but the SSH tunnel is a Unix socket.
/// This binds a loopback TCP port itself (so no other user can take the
/// port first and receive the token) and relays it to the socket while the
/// child runs.
/// shortcut: another local user can connect to the port during the deploy
/// and reach the API, which still asks them for the token. Upgrade: a
/// Unix-socket option in `minga deploy`.
async fn relay(sock: &Path) -> Result<(String, tokio::task::JoinHandle<()>), CtlError> {
    let fail = |e: std::io::Error| {
        CtlError::Usage(format!("cannot open a local port for `minga deploy`: {e}"))
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(fail)?;
    let addr = listener.local_addr().map_err(fail)?;
    let sock = sock.to_path_buf();
    let task = tokio::spawn(async move {
        while let Ok((mut tcp, _)) = listener.accept().await {
            let sock = sock.clone();
            tokio::spawn(async move {
                if let Ok(mut unix) = UnixStream::connect(&sock).await {
                    let _ = tokio::io::copy_bidirectional(&mut tcp, &mut unix).await;
                }
            });
        }
    });
    Ok((format!("http://{addr}"), task))
}

/// Runs a stack command.
pub async fn run(
    cmd: &StackCommand,
    conn: &Connection,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let client = &conn.api;
    match cmd {
        StackCommand::Deploy {
            compose_file,
            profiles,
            wait,
            stack,
        } => deploy(conn, compose_file, profiles, *wait, stack, quiet).await,
        StackCommand::Ls => ls(client, format, quiet).await,
        StackCommand::Ps { stack } => ps(client, stack, format, quiet).await,
        StackCommand::Rm { stack, with, yes } => rm(client, stack, with, *yes, format, quiet).await,
    }
}

/// What `rm --with` can name, as (kind, API collection).
const KINDS: [(&str, &str); 3] = [
    ("network", "networks"),
    ("config", "configs"),
    ("secret", "secrets"),
];

/// Where a service spec names a network, config or secret, as (list, key).
const REFERENCES: [(&str, &str); 6] = [
    ("/Spec/TaskTemplate/Networks", "Target"),
    ("/Spec/Networks", "Target"),
    ("/Spec/TaskTemplate/ContainerSpec/Secrets", "SecretID"),
    ("/Spec/TaskTemplate/ContainerSpec/Secrets", "SecretName"),
    ("/Spec/TaskTemplate/ContainerSpec/Configs", "ConfigID"),
    ("/Spec/TaskTemplate/ContainerSpec/Configs", "ConfigName"),
];

/// A network, config or secret whose ID starts with `<stack>_`.
struct Candidate {
    kind: &'static str,
    collection: &'static str,
    id: String,
}

/// `rm`: only services carry the stack label, so only services are known to
/// belong to the stack. Anything else is removed only when named with
/// `--with` and proven safe; otherwise nothing at all is removed.
async fn rm(
    client: &ApiClient,
    stack: &str,
    with: &[String],
    yes: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    api::check_id("stack", stack)?;
    for id in with {
        api::check_id("resource", id)?;
    }

    // 1. The stack's services.
    let services = client
        .get("services", Some(&view::stack_filter(stack)))
        .await?;
    let mut services: Vec<&str> = view::items(&services).iter().map(view::id).collect();
    services.sort_unstable();
    if services.is_empty() && with.is_empty() {
        return Err(CtlError::NotFound {
            kind: "stack",
            name: stack.to_string(),
            context: client.context.clone(),
        });
    }

    // 2. Candidates: IDs that start with `<stack>_` suggest, not prove, ownership.
    let prefix = format!("{stack}_");
    let mut candidates = Vec::new();
    for (kind, collection) in KINDS {
        for item in view::items(&client.get(collection, None).await?) {
            let id = item["ID"].as_str().or(item["Id"].as_str()).unwrap_or("");
            if id.starts_with(&prefix) {
                candidates.push(Candidate {
                    kind,
                    collection,
                    id: id.to_string(),
                });
            }
        }
    }

    // 3. Every --with must be a candidate no service outside the stack uses.
    let all = client.get("services", None).await?;
    let outside: Vec<&Value> = view::items(&all)
        .iter()
        .filter(|s| view::stack_of(s) != Some(stack))
        .collect();
    let mut named: Vec<&Candidate> = Vec::new();
    for id in with {
        let Some(candidate) = candidates.iter().find(|c| &c.id == id) else {
            return Err(CtlError::Usage(format!(
                "refusing --with {id}: not a network, config or secret of stack `{stack}` (its ID must start with `{prefix}`); nothing removed"
            )));
        };
        if let Some(user) = outside.iter().find(|s| refers_to(s, id)) {
            return Err(CtlError::Usage(format!(
                "refusing --with {id}: {} `{id}` is used by service `{}` outside stack `{stack}`; nothing removed",
                candidate.kind,
                view::id(user)
            )));
        }
        if !named.iter().any(|c| c.id == candidate.id) {
            named.push(candidate);
        }
    }

    // 4. Confirm.
    let mut summary = format!("This removes from context `{}`:", client.context);
    if !services.is_empty() {
        summary.push_str(&format!("\n  services: {}", services.join(", ")));
    }
    for c in &named {
        summary.push_str(&format!("\n  {:<8}{}", c.kind, c.id));
    }
    confirm("stack rm", yes, &summary)?;

    // 5. Services first, then the named resources. Every deletion is tried
    // and reported; a 409 on a resource is a warning, any other failure
    // makes the command exit 2 once everything has been reported.
    let mut removed_services = Vec::new();
    let mut failed = Vec::new(); // (kind, id, reason)
    for id in &services {
        match client.delete(&format!("services/{id}")).await {
            Ok(_) => removed_services.push(*id),
            Err(e) => failed.push(("service", id.to_string(), e.to_string())),
        }
    }
    let services_failed = !failed.is_empty();
    let mut removed = Vec::new();
    let mut in_use = Vec::new(); // (candidate, reason)
    for c in &named {
        if services_failed {
            // A service still there may use it.
            let reason = "not tried: some of the stack's services are still there";
            in_use.push((*c, reason.to_string()));
            continue;
        }
        match client.delete(&format!("{}/{}", c.collection, c.id)).await {
            Ok(_) => removed.push(*c),
            Err(CtlError::Api {
                status: 409,
                message,
            }) => {
                eprintln!("warning: {} {} was not removed: {message}", c.kind, c.id);
                in_use.push((*c, format!("in use: {message}")));
            }
            Err(e) => failed.push((c.kind, c.id.clone(), e.to_string())),
        }
    }

    let mut lines = Vec::new();
    if !removed_services.is_empty() {
        let n = removed_services.len();
        let noun = if n == 1 { "service" } else { "services" };
        lines.push(format!("removed {n} {noun}"));
    }
    for c in &removed {
        let note = if c.kind == "secret" {
            " (metadata only; files on nodes stay)"
        } else {
            ""
        };
        lines.push(format!("removed {} {}{note}", c.kind, c.id));
    }
    let not_removed: Vec<(&str, &str, &str)> = failed
        .iter()
        .map(|(kind, id, reason)| (*kind, id.as_str(), reason.as_str()))
        .chain(
            in_use
                .iter()
                .map(|(c, r)| (c.kind, c.id.as_str(), r.as_str())),
        )
        .collect();
    if !not_removed.is_empty() {
        lines.push("not removed:".to_string());
        for (kind, id, reason) in &not_removed {
            lines.push(format!("  {kind:<8}{id}: {reason}"));
        }
    }

    // 6. What was left in place or not removed, and how to remove it.
    let left: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| !with.contains(&c.id))
        .collect();
    if !left.is_empty() {
        lines.push("left in place (ownership cannot be confirmed):".to_string());
        for c in &left {
            let note = if c.kind == "secret" {
                " (metadata only)"
            } else {
                ""
            };
            lines.push(format!("  {:<8}{}{note}", c.kind, c.id));
        }
    }
    let retry: Vec<&str> = not_removed
        .iter()
        .filter(|(kind, ..)| *kind != "service")
        .map(|(_, id, _)| *id)
        .chain(left.iter().map(|c| c.id.as_str()))
        .collect();
    if !retry.is_empty() || services_failed {
        let flags: String = retry.iter().map(|id| format!(" --with {id}")).collect();
        lines.push(format!("to remove them: minga-ctl stack rm {stack}{flags}"));
    }
    let entry = |kind: &str, id: &str| json!({ "kind": kind, "id": id });
    let value = json!({
        "stack": stack,
        "removed_services": removed_services,
        "removed": removed.iter().map(|c| entry(c.kind, &c.id)).collect::<Vec<_>>(),
        "not_removed": not_removed
            .iter()
            .map(|(kind, id, reason)| json!({ "kind": kind, "id": id, "reason": reason }))
            .collect::<Vec<_>>(),
        "left_in_place": left.iter().map(|c| entry(c.kind, &c.id)).collect::<Vec<_>>(),
    });
    report(format, quiet, &lines.join("\n"), value)?;

    if failed.is_empty() {
        return Ok(());
    }
    let list: Vec<String> = failed
        .iter()
        .map(|(kind, id, reason)| format!("{kind} {id} ({reason})"))
        .collect();
    let done = if removed_services.is_empty() {
        "no services".to_string()
    } else {
        removed_services.join(", ")
    };
    Err(CtlError::Halted(format!(
        "stack rm {stack} did not finish: removed {done}; could not remove {}",
        list.join(", ")
    )))
}

/// Whether a service spec names `id` as a network, config or secret.
fn refers_to(service: &Value, id: &str) -> bool {
    REFERENCES.iter().any(|(list, key)| {
        service
            .pointer(list)
            .is_some_and(|list| view::items(list).iter().any(|r| r[*key] == id))
    })
}

/// `GET /services`, grouped by `com.docker.stack.namespace`. Services without
/// the label are not listed.
async fn ls(client: &ApiClient, format: Format, quiet: bool) -> Result<(), CtlError> {
    let services = client.get("services", None).await?;
    let mut stacks: BTreeMap<&str, usize> = BTreeMap::new();
    for service in view::items(&services) {
        if let Some(stack) = view::stack_of(service) {
            *stacks.entry(stack).or_default() += 1;
        }
    }
    let mut rows = Rows::new(&["NAME", "SERVICES"]);
    for (name, count) in stacks {
        rows.push(
            vec![name.to_string(), count.to_string()],
            json!({ "name": name, "services": count }),
        );
    }
    output::print(format, quiet, &rows)
}

/// A stack's tasks, with node names and the derived health column.
async fn ps(client: &ApiClient, stack: &str, format: Format, quiet: bool) -> Result<(), CtlError> {
    api::check_id("stack", stack)?;
    let filter = view::stack_filter(stack);
    let services = client.get("services", Some(&filter)).await?;
    if view::items(&services).is_empty() {
        return Err(CtlError::NotFound {
            kind: "stack",
            name: stack.to_string(),
            context: client.context.clone(),
        });
    }
    let tasks = client.get("tasks", Some(&filter)).await?;
    let nodes = view::node_names(&client.get("nodes", None).await?);
    let rows = view::task_rows(&tasks, view::items(&services), &nodes);
    output::print(format, quiet, &rows)?;
    if !quiet {
        if let Some(hint) = view::not_running_hint(&tasks) {
            eprintln!("{hint}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn yaml(text: &str) -> serde_yaml::Value {
        serde_yaml::from_str(text).unwrap()
    }

    #[test]
    fn relative_bind_mounts_are_found_in_short_and_long_form() {
        let short = yaml("services:\n  traefik:\n    volumes: ['./conf:/etc/traefik:ro']\n");
        assert_eq!(
            relative_bind(&short),
            Some(("traefik".to_string(), "./conf".to_string()))
        );
        let long = yaml(
            "services:\n  web:\n    volumes:\n      - type: bind\n        source: ../data\n        target: /data\n",
        );
        assert_eq!(
            relative_bind(&long),
            Some(("web".to_string(), "../data".to_string()))
        );
        for source in [".", "~/x"] {
            let doc = yaml(&format!("services:\n  a:\n    volumes: ['{source}:/x']\n"));
            assert!(relative_bind(&doc).is_some(), "{source}");
        }
    }

    #[test]
    fn absolute_and_named_volumes_and_other_keys_pass() {
        let doc = yaml(
            "services:\n  a:\n    env_file: ./a.env\n    extra_hosts: ['h:host-gateway']\n    volumes: ['/srv/conf:/etc/a', 'data:/var/lib/a', '/anon']\nvolumes:\n  data: {}\n",
        );
        assert_eq!(relative_bind(&doc), None);
    }

    #[test]
    fn only_loopback_endpoints_are_this_machine() {
        for local in [
            "http://127.0.0.1:2375",
            "http://localhost:2375",
            "https://[::1]:2376",
        ] {
            assert!(is_loopback(local), "{local}");
        }
        for remote in [
            "https://minga.example.net",
            "https://10.0.0.1:2376",
            "https://localhost.example.net",
        ] {
            assert!(!is_loopback(remote), "{remote}");
        }
    }

    #[tokio::test]
    async fn the_relay_forwards_a_tcp_connection_to_the_socket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("api.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let echo = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });
        let (url, task) = relay(&path).await.unwrap();
        let mut tcp = tokio::net::TcpStream::connect(url.trim_start_matches("http://"))
            .await
            .unwrap();
        tcp.write_all(b"ping").await.unwrap();
        let mut reply = [0u8; 4];
        tcp.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"ping");
        echo.await.unwrap();
        task.abort();
    }
}
