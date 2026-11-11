//! `minga-ctl service` — read commands (plan section 5.3).
//!
//! `logs`, `scale`, `update` and `rollback` come in later build-order steps.

use std::collections::HashMap;

use clap::Subcommand;
use serde_json::{json, Value};

use crate::api::{self, ApiClient, Found};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
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
    api::check_id("service", id)?;
    let service = match client.get_found(&format!("services/{id}"), None).await? {
        Found::Yes(service) => service,
        Found::No => {
            return Err(CtlError::NotFound {
                kind: "service",
                name: id.to_string(),
                context: client.context.clone(),
            })
        }
    };
    let tasks = client
        .get("tasks", Some(&json!({ "service": [id] })))
        .await?;
    let nodes = view::node_names(&client.get("nodes", None).await?);
    let rows = view::task_rows(&tasks, std::slice::from_ref(&service), &nodes);
    output::print(format, quiet, &rows)
}
