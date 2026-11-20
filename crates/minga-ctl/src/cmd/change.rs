//! `minga-ctl change`: inspect and decide tenant-scoped agent proposals.

use clap::{Args, Subcommand};
use serde_json::Value;

use crate::api::ApiClient;
use crate::error::CtlError;
use crate::output::{self, Format, Rows};

#[derive(Debug, Subcommand)]
pub enum ChangeCommand {
    /// List change requests visible to the tenant in the client certificate.
    List(ListArgs),
    /// Approve and atomically apply a pending change request.
    Approve(DecisionArgs),
    /// Reject a pending change request.
    Reject(DecisionArgs),
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// Maximum number of recent requests.
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u8).range(1..=100))]
    limit: u8,
}

#[derive(Debug, Args)]
pub struct DecisionArgs {
    /// Change request ID, as returned by `minga mcp` or `change list`.
    id: String,
}

pub async fn run(
    command: &ChangeCommand,
    api: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    match command {
        ChangeCommand::List(args) => {
            let requests = api.tenant_change_requests(args.limit).await?;
            let rows = requests.as_array().ok_or_else(|| CtlError::Api {
                status: 502,
                message: "change request list has an invalid shape".into(),
            })?;
            let mut output =
                Rows::new(&["ID", "SERVICE", "ACTION", "STATUS", "EXPIRES"]).with_id_column(0);
            for request in rows {
                let id = string_field(request, "id");
                let service = string_field(request, "service_id");
                let action = string_field(request, "kind");
                let status = string_field(request, "status");
                let expires = request
                    .get("expires_at_unix")
                    .and_then(Value::as_u64)
                    .map(|value| value.to_string())
                    .unwrap_or_default();
                output.push(
                    vec![
                        id.clone(),
                        service.clone(),
                        action.clone(),
                        status.clone(),
                        expires,
                    ],
                    request.clone(),
                );
            }
            output::print(format, quiet, &output)?;
        }
        ChangeCommand::Approve(args) | ChangeCommand::Reject(args) => {
            let approve = matches!(command, ChangeCommand::Approve(_));
            let request = api.decide_tenant_change_request(&args.id, approve).await?;
            let status = string_field(&request, "status");
            let verb = if approve { "approved" } else { "rejected" };
            if format == Format::Json {
                let mut row = Rows::new(&["ID"]).with_id_column(0);
                row.push(vec![args.id.clone()], request);
                output::print(format, quiet, &row)?;
            } else {
                output::say(
                    quiet,
                    &format!("change request {} {verb} ({status})", args.id),
                )?;
            }
        }
    }
    Ok(())
}

fn string_field(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}
