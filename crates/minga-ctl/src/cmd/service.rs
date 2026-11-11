//! `minga-ctl service` (plan section 5.3).
//!
//! `logs` comes in build-order step 5.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, IsTerminal};

use clap::{ArgGroup, Subcommand};
use serde_json::{json, Value};

use crate::api::{self, ApiClient, Found};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::state;
use crate::transport::Connection;
use crate::view;

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
    /// Return a service to the spec saved before minga-ctl last changed it.
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
            ServiceCommand::Ls { .. } | ServiceCommand::Ps { .. } => None,
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
                    "env_add": env_add.iter().map(|e| env_key(e)).collect::<Vec<_>>(),
                    "env_rm": env_rm,
                    "replicas": replicas,
                }),
            )),
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
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let client = &conn.api;
    match cmd {
        ServiceCommand::Ls { stack } => ls(client, stack.as_deref(), format, quiet).await,
        ServiceCommand::Ps { service } => ps(client, service, format, quiet).await,
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
        ServiceCommand::Rollback { service, yes } => {
            rollback(client, service, format, quiet, |summary| {
                confirm("service rollback", *yes, summary)
            })
            .await
        }
    }
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
    output::print(format, quiet, &rows)
}

/// `scale`: read, set the replicas in a copy, post only if it changed.
async fn scale(
    client: &ApiClient,
    id: &str,
    replicas: u64,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let (old, new) = edit(client, id, |spec| set_replicas(spec, id, replicas)).await?;
    let from = replicas_of(&old);
    let text = if old == new {
        format!("{id} already has {replicas} replicas; nothing changed")
    } else {
        let from = from.map_or("-".to_string(), |n| n.to_string());
        format!("{id} scaled from {from} to {replicas}")
    };
    let value = json!({
        "service": id,
        "replicas": { "from": from, "to": replicas },
        "changed": old != new,
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
    // Check every flag before the first request.
    for entry in change.env_add {
        if !entry.contains('=') || env_key(entry).is_empty() {
            return Err(CtlError::Usage(format!(
                "--env-add `{}` is not KEY=VALUE",
                env_key(entry)
            )));
        }
    }
    if let Some(key) = change
        .env_rm
        .iter()
        .find(|k| k.is_empty() || k.contains('='))
    {
        return Err(CtlError::Usage(format!("--env-rm `{key}` is not a KEY")));
    }

    let (old, new) = edit(client, id, |spec| {
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
        if !change.env_add.is_empty() || !change.env_rm.is_empty() {
            let env = container.entry("Env").or_insert(Value::Null);
            if env.is_null() {
                *env = json!([]);
            }
            let env = env.as_array_mut().ok_or_else(|| {
                CtlError::Usage(format!("service `{id}` has an Env that is not a list"))
            })?;
            // Docker's order: removals first, then additions.
            env.retain(|e| !change.env_rm.iter().any(|k| k == env_key_of(e)));
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
    let text = if old == new {
        format!("{id} already matches; nothing changed")
    } else {
        let list: Vec<String> = changes.iter().map(|(l, c)| format!("{l} {c}")).collect();
        format!("{id} updated: {}", list.join(", "))
    };
    let value = json!({
        "service": id,
        "changed": old != new,
        "changes": changes.into_iter().collect::<BTreeMap<_, _>>(),
    });
    report(format, quiet, &text, value)
}

/// `rollback`: post the saved revision after `confirm` agrees and the service
/// has not changed since it was read. On success the spec it replaced becomes
/// the new revision, so a second rollback returns to where it started.
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
    let first = read(client, id).await?;
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

/// `GET /services/{id}`; a 404 is exit 3.
async fn read(client: &ApiClient, id: &str) -> Result<Value, CtlError> {
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
/// Returns the spec as read and the edited copy.
async fn edit(
    client: &ApiClient,
    id: &str,
    change: impl FnOnce(&mut Value) -> Result<(), CtlError>,
) -> Result<(Value, Value), CtlError> {
    let service = read(client, id).await?;
    let old = service["Spec"].clone();
    let mut new = old.clone();
    change(&mut new)?;
    if new != old {
        post_spec(client, id, &service, &new).await?;
        save_revision(client, id, &old)?;
    }
    Ok((old, new))
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

/// Saves the revision once the API has accepted the change.
fn save_revision(client: &ApiClient, id: &str, spec: &Value) -> Result<(), CtlError> {
    state::save_revision(&client.context, id, spec).map_err(|e| {
        CtlError::Halted(format!(
            "{id} was changed, but its revision could not be saved ({e}); rollback data for this change is missing"
        ))
    })
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

/// Minga keeps the environment in a map, so the order of `Env` can differ
/// between two reads of the same service. Compare specs with `Env` sorted.
fn same_spec(a: &Value, b: &Value) -> bool {
    let sorted = |spec: &Value| {
        let mut spec = spec.clone();
        if let Some(Value::Array(env)) = spec.pointer_mut("/TaskTemplate/ContainerSpec/Env") {
            env.sort_by(|x, y| x.as_str().cmp(&y.as_str()));
        }
        spec
    };
    sorted(a) == sorted(b)
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
    fn env_keys_are_split_at_the_first_equals_sign() {
        assert_eq!(env_key("A=b=c"), "A");
        assert_eq!(env_key("BARE"), "BARE");
    }
}
