//! `minga-ctl secret` — `list` (plan section 5.7).
//!
//! `put` wraps `minga secret put` and comes with the wrapped commands.

use clap::Subcommand;
use serde_json::{json, Value};

use crate::api::ApiClient;
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::Connection;
use crate::view;

/// Secret commands.
#[derive(Debug, Subcommand)]
pub enum SecretCommand {
    /// List secret metadata. Values never leave the nodes.
    List,
}

/// Runs a secret command.
pub async fn run(
    cmd: &SecretCommand,
    conn: &Connection,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let client = &conn.api;
    match cmd {
        SecretCommand::List => list(client, format, quiet).await,
    }
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
