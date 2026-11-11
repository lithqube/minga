//! `minga-ctl stack` (plan section 5.2).
//!
//! `deploy` comes in build-order step 5.

use std::collections::BTreeMap;

use clap::Subcommand;
use serde_json::{json, Value};

use crate::api::{self, ApiClient};
use crate::cmd::service::{confirm, report};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::Connection;
use crate::view;

/// Stack commands.
#[derive(Debug, Subcommand)]
pub enum StackCommand {
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
            StackCommand::Rm { stack, with, yes } => Some((
                "stack rm",
                json!({ "stack": stack, "with": with, "yes": yes }),
            )),
        }
    }
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

    // 5. Services first, then the named resources; a 409 is a warning.
    for id in &services {
        client.delete(&format!("services/{id}")).await?;
    }
    let mut lines = Vec::new();
    if !services.is_empty() {
        let noun = if services.len() == 1 {
            "service"
        } else {
            "services"
        };
        lines.push(format!("removed {} {noun}", services.len()));
    }
    let mut removed = Vec::new();
    for c in &named {
        match client.delete(&format!("{}/{}", c.collection, c.id)).await {
            Ok(_) => {
                let note = if c.kind == "secret" {
                    " (metadata only; files on nodes stay)"
                } else {
                    ""
                };
                lines.push(format!("removed {} {}{note}", c.kind, c.id));
                removed.push(json!({ "kind": c.kind, "id": c.id }));
            }
            Err(CtlError::Api {
                status: 409,
                message,
            }) => eprintln!("warning: {} {} was not removed: {message}", c.kind, c.id),
            Err(e) => return Err(e),
        }
    }

    // 6. What was left in place, and how to remove it.
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
        let flags: String = left.iter().map(|c| format!(" --with {}", c.id)).collect();
        lines.push(format!("to remove them: minga-ctl stack rm {stack}{flags}"));
    }
    let value = json!({
        "stack": stack,
        "removed_services": services,
        "removed": removed,
        "left_in_place": left
            .iter()
            .map(|c| json!({ "kind": c.kind, "id": c.id }))
            .collect::<Vec<_>>(),
    });
    report(format, quiet, &lines.join("\n"), value)
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
    output::print(format, quiet, &rows)
}
