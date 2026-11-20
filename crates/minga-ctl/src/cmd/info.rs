//! `minga-ctl info`: distinguish this client build from the server and show
//! persisted Raft membership alongside observations made by the local member.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::ApiClient;
use crate::error::CtlError;
use crate::output::{self, Format, Rows};

#[derive(Debug, Deserialize)]
struct ClusterResponse {
    server_version: String,
    build_revision: Option<String>,
    api_version: String,
    state_format_version: u8,
    raft: RaftInfo,
}

#[derive(Debug, Serialize, Deserialize)]
struct RaftInfo {
    node_id: u64,
    state: String,
    leader_id: Option<u64>,
    ready: bool,
    quorum_ack_age_ms: Option<u64>,
    quorum_ack_recent: Option<bool>,
    members: Vec<RaftMember>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RaftMember {
    id: u64,
    address: String,
    voter: bool,
    replication_acknowledged: Option<bool>,
    transport_reachable: Option<bool>,
}

#[derive(Debug, Serialize)]
struct Info {
    client_version: &'static str,
    server_version: String,
    build_revision: Option<String>,
    api_version: String,
    state_format_version: u8,
    raft: RaftInfo,
}

/// Reads server build metadata and Raft observations without changing state.
pub async fn run(api: &ApiClient, format: Format, quiet: bool) -> Result<(), CtlError> {
    let response = api.get("minga/cluster", None).await?;
    let response: ClusterResponse = serde_json::from_value(response)?;
    let info = Info {
        client_version: env!("CARGO_PKG_VERSION"),
        server_version: response.server_version,
        build_revision: response.build_revision,
        api_version: response.api_version,
        state_format_version: response.state_format_version,
        raft: response.raft,
    };

    if quiet {
        return output::say(false, &info.raft.node_id.to_string());
    }
    if format == Format::Table {
        let mut rows = Rows::new(&["FACT", "OBSERVED"]);
        push(&mut rows, "client version", json!(info.client_version));
        push(&mut rows, "server version", json!(info.server_version));
        push(
            &mut rows,
            "server build revision",
            json!(info.build_revision),
        );
        push(&mut rows, "Docker API version", json!(info.api_version));
        push(
            &mut rows,
            "persisted state format version",
            json!(info.state_format_version),
        );
        push(&mut rows, "Raft node ID", json!(info.raft.node_id));
        push(&mut rows, "Raft state", json!(info.raft.state));
        push(&mut rows, "Raft ready", json!(info.raft.ready));
        push(&mut rows, "Raft leader ID", json!(info.raft.leader_id));
        push(
            &mut rows,
            "quorum acknowledgment age (ms)",
            json!(info.raft.quorum_ack_age_ms),
        );
        push(
            &mut rows,
            "recent quorum acknowledgment",
            json!(info.raft.quorum_ack_recent),
        );
        for member in &info.raft.members {
            push(
                &mut rows,
                &format!("Raft member {}", member.id),
                json!({
                    "address": member.address,
                    "voter": member.voter,
                    "replication_acknowledged": member.replication_acknowledged,
                    "transport_reachable": member.transport_reachable,
                }),
            );
        }
        return output::print(Format::Table, false, &rows);
    }

    let text = match format {
        Format::Json => serde_json::to_string_pretty(&info)?,
        Format::Yaml => {
            serde_yaml::to_string(&info).map_err(|error| CtlError::Output(error.to_string()))?
        }
        Format::Table => return Ok(()),
    };
    let mut stdout = std::io::stdout().lock();
    use std::io::Write as _;
    writeln!(stdout, "{text}")
        .and_then(|()| stdout.flush())
        .map_err(|error| CtlError::Output(format!("write to stdout: {error}")))
}

fn push(rows: &mut Rows, field: &str, value: Value) {
    let observed = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    rows.push(
        vec![field.to_owned(), observed],
        json!({"field": field, "value": value}),
    );
}
