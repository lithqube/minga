//! Operator commands for cluster resources exposed by Minga's API.

use std::io::{IsTerminal, Write};

use clap::Subcommand;
use serde_json::{json, Value};

use crate::api::{self, ApiClient, Found};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};

#[derive(Clone, Debug, Subcommand)]
pub enum ManagerCommand {
    /// List the current Raft voters.
    Ls,
    /// Add a manager that was started with `minga manager --join`.
    Add {
        id: u64,
        /// The manager's reachable Raft address.
        #[arg(long)]
        raft_addr: String,
    },
    /// Remove a manager from the Raft voter set.
    Rm {
        id: u64,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum NodeCommand {
    /// List registered nodes and their scheduling state.
    Ls,
    /// Show one node's details.
    Inspect { id: String },
    /// Change a node's availability and/or labels.
    Update {
        id: String,
        /// `drain` stops new placement; `active` permits placement again.
        #[arg(long, value_parser = ["active", "drain"])]
        availability: Option<String>,
        /// Add or replace a label. Repeat for multiple labels.
        #[arg(long = "label", value_name = "KEY=VALUE")]
        labels: Vec<String>,
        /// Remove one label. Repeat for multiple labels.
        #[arg(long = "label-rm", value_name = "KEY")]
        remove_labels: Vec<String>,
        /// Remove every label before applying `--label` values.
        #[arg(long)]
        clear_labels: bool,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum NetworkCommand {
    /// List cluster networks.
    Ls,
    /// Show a network's settings and attached containers.
    Inspect { id: String },
    /// Create a cluster network.
    Create {
        name: String,
        /// Explicit subnet CIDR; omitted subnets are allocated by the leader.
        #[arg(long)]
        subnet: Option<String>,
        /// Allow standalone containers to attach.
        #[arg(long)]
        attachable: bool,
    },
    /// Remove a network.
    Rm {
        id: String,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ConfigCommand {
    /// List configs (contents are never returned by the API).
    Ls,
    /// Show config metadata.
    Inspect { id: String },
    /// Create a config from a file. The file is base64 encoded for the API.
    Create {
        name: String,
        /// File whose bytes become the config payload.
        #[arg(long)]
        file: String,
    },
    /// Remove a config.
    Rm {
        id: String,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum TaskCommand {
    /// List tasks across services.
    Ls,
    /// Show one task's status, placement and network attachments.
    Inspect { id: String },
}

#[derive(Clone, Debug)]
pub enum ClusterCommand {
    Manager(ManagerCommand),
    Node(NodeCommand),
    Network(NetworkCommand),
    Config(ConfigCommand),
    Task(TaskCommand),
}

impl ClusterCommand {
    /// Audit metadata for a write command; payloads and secret values are excluded.
    pub fn audit(&self) -> Option<(&'static str, Value)> {
        match self {
            Self::Manager(ManagerCommand::Ls)
            | Self::Node(NodeCommand::Ls | NodeCommand::Inspect { .. })
            | Self::Network(NetworkCommand::Ls | NetworkCommand::Inspect { .. })
            | Self::Config(ConfigCommand::Ls | ConfigCommand::Inspect { .. })
            | Self::Task(_) => None,
            Self::Manager(ManagerCommand::Add { id, raft_addr }) => {
                Some(("manager add", json!({"id": id, "raft_addr": raft_addr})))
            }
            Self::Manager(ManagerCommand::Rm { id, yes }) => {
                Some(("manager rm", json!({"id": id, "yes": yes})))
            }
            Self::Node(NodeCommand::Update {
                id,
                availability,
                labels,
                remove_labels,
                clear_labels,
            }) => Some((
                "node update",
                json!({
                    "id": id,
                    "availability": availability,
                    "label_keys": labels.iter().filter_map(|s| s.split_once('=').map(|(k, _)| k)).collect::<Vec<_>>(),
                    "remove_labels": remove_labels,
                    "clear_labels": clear_labels,
                }),
            )),
            Self::Network(NetworkCommand::Create {
                name,
                subnet,
                attachable,
            }) => Some((
                "network create",
                json!({"name": name, "subnet": subnet, "attachable": attachable}),
            )),
            Self::Network(NetworkCommand::Rm { id, yes }) => {
                Some(("network rm", json!({"id": id, "yes": yes})))
            }
            Self::Config(ConfigCommand::Create { name, .. }) => {
                Some(("config create", json!({"name": name})))
            }
            Self::Config(ConfigCommand::Rm { id, yes }) => {
                Some(("config rm", json!({"id": id, "yes": yes})))
            }
        }
    }
}

pub async fn run(
    command: &ClusterCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        ClusterCommand::Manager(command) => run_manager(command, client, format, quiet).await,
        ClusterCommand::Node(command) => run_node(command, client, format, quiet).await,
        ClusterCommand::Network(command) => run_network(command, client, format, quiet).await,
        ClusterCommand::Config(command) => run_config(command, client, format, quiet).await,
        ClusterCommand::Task(command) => run_task(command, client, format, quiet).await,
    }
}

async fn run_manager(
    command: &ManagerCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        ManagerCommand::Ls => {
            let managers = client.get_unversioned("minga/managers").await?;
            let mut rows = Rows::new(&["ID"]);
            for id in managers.as_array().into_iter().flatten() {
                let id = id
                    .as_u64()
                    .map_or_else(|| id.to_string(), |id| id.to_string());
                rows.push(vec![id.clone()], json!({"id": id}));
            }
            output::print(format, quiet, &rows)
        }
        ManagerCommand::Add { id, raft_addr } => {
            let result = client
                .post_unversioned("minga/managers", &json!({"id": id, "raft_addr": raft_addr}))
                .await?;
            output::say(quiet, &format!("added manager {id}: {result}"))
        }
        ManagerCommand::Rm { id, yes } => {
            confirm(
                *yes,
                &format!("remove manager {id} from the Raft voter set?"),
            )?;
            delete(
                client
                    .delete_unversioned(&format!("minga/managers/{id}"))
                    .await?,
                "manager",
                &id.to_string(),
            )?;
            output::say(quiet, &format!("removed manager {id}"))
        }
    }
}

async fn run_node(
    command: &NodeCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        NodeCommand::Ls => {
            let nodes = client.get("nodes", None).await?;
            let mut rows = Rows::new(&["ID", "HOST", "ROLE", "AVAILABILITY", "STATE", "ADDRESS"]);
            for node in items(&nodes) {
                let id = str_at(node, "/ID");
                let host = str_at(node, "/Description/Hostname");
                let role = str_at(node, "/Spec/Role");
                let availability = str_at(node, "/Spec/Availability");
                let state = str_at(node, "/Status/State");
                let address = str_at(node, "/Status/Addr");
                rows.push(
                    vec![
                        id.into(),
                        host.into(),
                        role.into(),
                        availability.into(),
                        state.into(),
                        address.into(),
                    ],
                    node.clone(),
                );
            }
            output::print(format, quiet, &rows)
        }
        NodeCommand::Inspect { id } => {
            api::check_id("node", id)?;
            let node = client.get(&format!("nodes/{id}"), None).await?;
            output::print_value(format, quiet, &node)
        }
        NodeCommand::Update {
            id,
            availability,
            labels,
            remove_labels,
            clear_labels,
        } => {
            api::check_id("node", id)?;
            if availability.is_none()
                && labels.is_empty()
                && remove_labels.is_empty()
                && !clear_labels
            {
                return Err(CtlError::Usage(
                    "node update requires at least one change option".into(),
                ));
            }
            let current = client.get(&format!("nodes/{id}"), None).await?;
            let mut current_labels = current
                .pointer("/Spec/Labels")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if *clear_labels {
                current_labels.clear();
            }
            for raw in labels {
                let (key, value) = raw.split_once('=').ok_or_else(|| {
                    CtlError::Usage(format!("invalid label {raw:?}; expected KEY=VALUE"))
                })?;
                if key.is_empty() {
                    return Err(CtlError::Usage("label key must not be empty".into()));
                }
                current_labels.insert(key.to_string(), Value::String(value.to_string()));
            }
            for key in remove_labels {
                current_labels.remove(key);
            }
            let role = str_at(&current, "/Spec/Role");
            let name = current
                .pointer("/Spec/Name")
                .cloned()
                .unwrap_or(Value::Null);
            let availability = match availability.as_deref() {
                Some(availability) => availability,
                None if str_at(&current, "/Status/State") == "ready" => "active",
                None => {
                    return Err(CtlError::Usage(
                        "node is not ready; specify --availability active or drain explicitly"
                            .into(),
                    ));
                }
            };
            let updated = client
                .post(
                    &format!("nodes/{id}/update"),
                    &json!({"Name": name, "Role": role, "Availability": availability, "Labels": current_labels}),
                )
                .await?;
            output::print_value(format, quiet, &updated)
        }
    }
}

async fn run_network(
    command: &NetworkCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        NetworkCommand::Ls => {
            let networks = client.get("networks", None).await?;
            let mut rows = Rows::new(&["ID", "NAME", "DRIVER", "SCOPE", "SUBNET"]);
            for network in items(&networks) {
                let id = str_at(network, "/ID");
                let name = str_at(network, "/Name");
                let driver = str_at(network, "/Driver");
                let scope = str_at(network, "/Scope");
                let subnet = network
                    .pointer("/IPAM/Config/0/Subnet")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                rows.push(
                    vec![
                        id.into(),
                        name.into(),
                        driver.into(),
                        scope.into(),
                        subnet.into(),
                    ],
                    network.clone(),
                );
            }
            output::print(format, quiet, &rows)
        }
        NetworkCommand::Inspect { id } => {
            api::check_id("network", id)?;
            let network = client.get(&format!("networks/{id}"), None).await?;
            output::print_value(format, quiet, &network)
        }
        NetworkCommand::Create {
            name,
            subnet,
            attachable,
        } => {
            api::check_id("network", name)?;
            let mut body = json!({
                "Name": name,
                "Attachable": attachable,
            });
            if let Some(subnet) = subnet {
                body["IPAM"] = json!({"Config": [{"Subnet": subnet}]});
            }
            let response = client.post("networks/create", &body).await?;
            output::say(
                quiet,
                &format!("created network {}", str_at(&response, "/ID")),
            )
        }
        NetworkCommand::Rm { id, yes } => {
            api::check_id("network", id)?;
            confirm(*yes, &format!("remove network {id}?"))?;
            delete(
                client.delete(&format!("networks/{id}")).await?,
                "network",
                id,
            )?;
            output::say(quiet, &format!("removed network {id}"))
        }
    }
}

async fn run_config(
    command: &ConfigCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        ConfigCommand::Ls => {
            let configs = client.get("configs", None).await?;
            let mut rows = Rows::new(&["ID", "NAME", "CREATED"]);
            for config in items(&configs) {
                rows.push(
                    vec![
                        str_at(config, "/ID").into(),
                        str_at(config, "/Spec/Name").into(),
                        str_at(config, "/CreatedAt").into(),
                    ],
                    config.clone(),
                );
            }
            output::print(format, quiet, &rows)
        }
        ConfigCommand::Inspect { id } => {
            api::check_id("config", id)?;
            let config = client.get(&format!("configs/{id}"), None).await?;
            output::print_value(format, quiet, &config)
        }
        ConfigCommand::Create { name, file } => {
            api::check_id("config", name)?;
            let data = std::fs::read(file).map_err(|err| {
                CtlError::Usage(format!("cannot read config file {}: {err}", file))
            })?;
            use base64::Engine as _;
            let encoded = base64::engine::general_purpose::STANDARD.encode(data);
            let response = client
                .post("configs/create", &json!({"Name": name, "Data": encoded}))
                .await?;
            output::say(
                quiet,
                &format!("created config {}", str_at(&response, "/ID")),
            )
        }
        ConfigCommand::Rm { id, yes } => {
            api::check_id("config", id)?;
            confirm(*yes, &format!("remove config {id}?"))?;
            delete(client.delete(&format!("configs/{id}")).await?, "config", id)?;
            output::say(quiet, &format!("removed config {id}"))
        }
    }
}

async fn run_task(
    command: &TaskCommand,
    client: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        TaskCommand::Ls => {
            let tasks = client.get("tasks", None).await?;
            let mut rows = Rows::new(&["ID", "SERVICE", "NODE", "DESIRED", "STATE", "ERROR"]);
            for task in items(&tasks) {
                rows.push(
                    vec![
                        str_at(task, "/ID").into(),
                        str_at(task, "/ServiceID").into(),
                        str_at(task, "/NodeID").into(),
                        str_at(task, "/DesiredState").into(),
                        str_at(task, "/Status/State").into(),
                        str_at(task, "/Status/Err").into(),
                    ],
                    task.clone(),
                );
            }
            output::print(format, quiet, &rows)
        }
        TaskCommand::Inspect { id } => {
            api::check_id("task", id)?;
            let task = client.get(&format!("tasks/{id}"), None).await?;
            output::print_value(format, quiet, &task)
        }
    }
}

/// Displays the API's system info or version response as one JSON/YAML record.
pub async fn system_info(
    client: &ApiClient,
    version: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let path = if version { "version" } else { "info" };
    let response = client.get(path, None).await?;
    output::print_value(format, quiet, &response)
}

fn items(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or_default()
}

fn str_at<'a>(value: &'a Value, path: &str) -> &'a str {
    value.pointer(path).and_then(Value::as_str).unwrap_or("")
}

fn delete(found: Found, kind: &str, id: &str) -> Result<(), CtlError> {
    match found {
        Found::Yes(_) => Ok(()),
        Found::No => Err(CtlError::Api {
            status: 404,
            message: format!("{kind} `{id}` was not found"),
        }),
    }
}

fn confirm(yes: bool, prompt: &str) -> Result<(), CtlError> {
    if yes {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(CtlError::Usage(format!(
            "{prompt} pass --yes in non-interactive use"
        )));
    }
    eprint!("{prompt} [y/N] ");
    std::io::stderr()
        .flush()
        .map_err(|err| CtlError::Output(format!("write confirmation prompt: {err}")))?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .map_err(|err| CtlError::Usage(format!("cannot read confirmation: {err}")))?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        Err(CtlError::Usage("cancelled; nothing changed".into()))
    }
}
