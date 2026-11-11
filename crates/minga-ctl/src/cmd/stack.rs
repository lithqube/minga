//! `minga-ctl stack` — read commands (plan section 5.2).
//!
//! `deploy` and `rm` come in later build-order steps.

use std::collections::BTreeMap;

use clap::Subcommand;
use serde_json::json;

use crate::api::{self, ApiClient};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
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
}

/// Runs a stack command.
pub async fn run(
    cmd: &StackCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match cmd {
        StackCommand::Ls => ls(client, format, quiet).await,
        StackCommand::Ps { stack } => ps(client, stack, format, quiet).await,
    }
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
