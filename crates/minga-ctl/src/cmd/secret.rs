//! `minga-ctl secret` — `put` and `list` (plan section 5.7).

use std::io::Read;

use clap::Subcommand;
use serde_json::{json, Value};

use crate::api::ApiClient;
use crate::cmd::image::{self, Node};
use crate::config::Resolved;
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::{self, Connection, RemoteArg};
use crate::view;

/// Secret commands.
#[derive(Debug, Subcommand)]
pub enum SecretCommand {
    /// Provision a secret value on each node and declare it in the API. The
    /// value is read from stdin and never appears in arguments or logs.
    Put {
        /// Secret ID: `<stack>_<compose secret key>`.
        id: String,
        /// Run on this SSH target; repeat for several. Default: the
        /// context's `nodes`, else its `ssh`, else this machine.
        #[arg(long = "ssh", value_name = "TARGET")]
        ssh: Vec<String>,
        /// Directory of the node-local secret store (`minga secret put`'s
        /// default when not given).
        #[arg(long, value_name = "DIR")]
        secret_store: Option<String>,
    },
    /// List secret metadata. Values never leave the nodes.
    List,
}

impl SecretCommand {
    /// The audit-log command name and arguments. The value is never logged.
    pub fn audit(&self) -> Option<(&'static str, Value)> {
        match self {
            SecretCommand::List => None,
            SecretCommand::Put {
                id,
                ssh,
                secret_store,
            } => Some((
                "secret put",
                json!({ "id": id, "ssh": ssh, "secret_store": secret_store }),
            )),
        }
    }
}

/// Runs a secret command.
pub async fn run(
    cmd: &SecretCommand,
    conn: &Connection,
    resolved: &Resolved,
    debug: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let client = &conn.api;
    match cmd {
        SecretCommand::List => list(client, format, quiet).await,
        SecretCommand::Put {
            id,
            ssh,
            secret_store,
        } => {
            transport::validate_id(id)?;
            if let Some(dir) = secret_store {
                transport::validate_remote_path(dir)?;
            }
            let nodes = image::targets(ssh, &resolved.context)?;
            let mut value = Vec::new();
            std::io::stdin()
                .read_to_end(&mut value)
                .map_err(|e| CtlError::Usage(format!("cannot read the secret value: {e}")))?;
            if value.is_empty() {
                return Err(CtlError::Usage(format!(
                    "no secret value on stdin for `{id}`; pipe it in, e.g. `pass show x | minga-ctl secret put {id}`"
                )));
            }
            let store = secret_store.as_deref();
            put(client, id, store, &nodes, &value, debug, format, quiet).await
        }
    }
}

/// 1. `minga secret put <id>` on each node with the value on stdin.
/// 2. Only if every node succeeded, `POST /secrets/create`; a 409 means it is
///    already declared.
#[allow(clippy::too_many_arguments)]
async fn put(
    client: &ApiClient,
    id: &str,
    secret_store: Option<&str>,
    nodes: &[Node<'_>],
    value: &[u8],
    debug: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let mut argv = vec![
        RemoteArg::Fixed("minga"),
        RemoteArg::Fixed("secret"),
        RemoteArg::Fixed("put"),
        RemoteArg::Value(id),
    ];
    if let Some(dir) = secret_store {
        argv.extend([RemoteArg::Fixed("--secret-store"), RemoteArg::Value(dir)]);
    }
    let provisioned = image::on_each(
        "secret put",
        nodes,
        &argv,
        Some(value),
        "provisioned",
        debug,
        format,
        quiet,
    )
    .await;
    if let Err(err) = provisioned {
        return Err(CtlError::Halted(format!(
            "{err}; secret `{id}` was not declared in the API"
        )));
    }
    let declared = match client.post("secrets/create", &json!({ "Name": id })).await {
        Ok(_) => "declared",
        Err(CtlError::Api { status: 409, .. }) => "already declared",
        Err(err) => return Err(err),
    };
    output::say(
        quiet,
        &format!("secret `{id}` {declared} in context `{}`", client.context),
    )
}

/// `GET /secrets`. It does not show which nodes hold the value.
async fn list(client: &ApiClient, format: Format, quiet: bool) -> Result<(), CtlError> {
    let secrets = client.get("secrets", None).await?;
    let mut secrets: Vec<&Value> = view::items(&secrets).iter().collect();
    secrets.sort_by(|a, b| view::id(a).cmp(view::id(b)));
    let mut rows = Rows::new(&["ID", "NAME", "CREATED"]);
    for secret in secrets {
        let id = view::id(secret);
        let name = secret
            .pointer("/Spec/Name")
            .and_then(Value::as_str)
            .unwrap_or(id);
        let created = secret["CreatedAt"].as_str().unwrap_or("-");
        rows.push(
            [id, name, created].map(str::to_string).to_vec(),
            json!({ "id": id, "name": name, "created": created }),
        );
    }
    output::print(format, quiet, &rows)
}
