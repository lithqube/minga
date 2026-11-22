//! `minga-ctl change`: inspect and decide tenant-scoped agent proposals.

use std::fmt::Write as _;
use std::io::Write as _;

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
    Approve(ApproveArgs),
    /// Reject a pending change request.
    Reject(DecisionArgs),
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// Maximum number of recent requests.
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u8).range(1..=100))]
    limit: u8,
    /// Only requests still waiting for a decision.
    #[arg(long)]
    pending: bool,
}

#[derive(Debug, Args)]
pub struct ApproveArgs {
    #[command(flatten)]
    decision: DecisionArgs,
    /// Do not ask for confirmation (the diff is still printed to stderr).
    #[arg(long)]
    yes: bool,
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
            let requests = api.tenant_change_requests(args.limit, args.pending).await?;
            let rows = requests.as_array().ok_or_else(|| CtlError::Api {
                status: 502,
                message: "change request list has an invalid shape".into(),
            })?;
            let mut output =
                Rows::new(&["ID", "SERVICE", "ACTION", "STATUS", "EXPIRES", "CHANGES"])
                    .with_id_column(0);
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
                let changes = review_diff(request)?;
                output.push(
                    vec![
                        id.clone(),
                        service.clone(),
                        action.clone(),
                        status.clone(),
                        expires,
                        changes,
                    ],
                    request.clone(),
                );
            }
            if format == Format::Json {
                print_json(&serde_json::to_value(&output.values)?)?;
            } else {
                output::print(format, quiet, &output)?;
            }
        }
        ChangeCommand::Approve(args) => {
            let request = api.tenant_change_request(&args.decision.id).await?;
            let summary = format!(
                "change request {}: {}",
                args.decision.id,
                review_diff(&request)?
            );
            // Always show review evidence, including with --quiet or --yes.
            writeln!(std::io::stderr().lock(), "{summary}")
                .map_err(|error| CtlError::Output(error.to_string()))?;
            super::service::confirm("change approve", args.yes, "Approve this change?")?;
            decide(api, &args.decision.id, true, format, quiet).await?;
        }
        ChangeCommand::Reject(args) => {
            decide(api, &args.id, false, format, quiet).await?;
        }
    }
    Ok(())
}

fn string_field(value: &Value, field: &str) -> String {
    let text = value.get(field).and_then(Value::as_str).unwrap_or_default();
    let quoted = ascii_json(&Value::String(text.to_owned()).to_string());
    quoted[1..quoted.len() - 1].to_owned()
}

async fn decide(
    api: &ApiClient,
    id: &str,
    approve: bool,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let request = api.decide_tenant_change_request(id, approve).await?;
    let status = string_field(&request, "status");
    let verb = if approve { "approved" } else { "rejected" };
    match format {
        Format::Json => print_json(&request),
        Format::Yaml => output::print_value(format, quiet, &request),
        Format::Table => output::say(quiet, &format!("change request {id} {verb} ({status})")),
    }
}

fn review_diff(request: &Value) -> Result<String, CtlError> {
    let changes = request
        .get("changes")
        .and_then(Value::as_array)
        .ok_or_else(|| CtlError::Api {
            status: 502,
            message: "change request has no reviewable diff; upgrade the server".into(),
        })?;
    changes
        .iter()
        .map(|change| {
            Ok(format!(
                "{}: {} -> {}",
                string_field(change, "field"),
                ascii_json(&serde_json::to_string(&change["before"])?),
                ascii_json(&serde_json::to_string(&change["after"])?)
            ))
        })
        .collect::<Result<Vec<_>, CtlError>>()
        .map(|lines| lines.join("; "))
}

fn print_json(value: &Value) -> Result<(), CtlError> {
    output::say(false, &ascii_json(&serde_json::to_string_pretty(value)?))
}

// Escape every non-ASCII code point, including all Unicode format characters.
// JSON UTF-16 escapes preserve the exact values for structured-output consumers.
fn ascii_json(text: &str) -> String {
    let mut escaped = String::new();
    for unit in text.encode_utf16() {
        if unit < 128 {
            escaped.push(char::from(unit as u8));
        } else {
            let _ = write!(escaped, "\\u{unit:04x}");
        }
    }
    escaped
}
