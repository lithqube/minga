//! `minga-ctl doctor`: bounded, read-only diagnostics and a sanitized bundle.

use std::fs::OpenOptions;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{Resolved, TokenSource};
use crate::error::CtlError;
use crate::output::{self, Format};
use crate::transport::Connection;

use super::report::{self, Finding, Severity, Status};

const BUNDLE_INCLUDED: &[&str] = &[
    "client and server version/build metadata",
    "API liveness and readiness status",
    "persisted state format version",
    "Raft readiness and membership counts; member addresses omitted",
    "tenant, node and task counts; resource identifiers omitted",
    "authentication and TLS configuration booleans",
    "findings with sanitized evidence and bounded next actions",
];
const BUNDLE_OMITTED: &[&str] = &[
    "API tokens and token command arguments or file paths",
    "NATS URLs and credentials",
    "TLS certificate and private-key contents or paths",
    "service environment, labels, secrets and task error text",
    "tenant, service and task names or IDs; node addresses",
    "context names, endpoint hosts and configuration file contents",
];

/// Doctor arguments.
#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Write a sanitized support bundle to a new local file (never overwrites).
    #[arg(long)]
    bundle: Option<PathBuf>,
    /// Seconds each API or readiness probe may take.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=60))]
    timeout: u64,
}

#[derive(Debug, Serialize)]
struct ConnectionSummary {
    endpoint_scheme: &'static str,
    ssh_tunnel_configured: bool,
    token_configured: bool,
    token_source: &'static str,
    custom_tls_ca_configured: bool,
    separate_metrics_endpoint_configured: bool,
}

impl ConnectionSummary {
    fn from_resolved(resolved: &Resolved, conn: &Connection) -> Self {
        let token_source = match &resolved.token_source {
            TokenSource::Env => "environment",
            TokenSource::Command(_) => "command",
            TokenSource::RemoteFile(_) => "remote_file",
            TokenSource::None => "none",
        };
        Self {
            endpoint_scheme: if resolved.endpoint.starts_with("https://") {
                "https"
            } else {
                "http"
            },
            ssh_tunnel_configured: resolved.context.ssh.is_some(),
            token_configured: resolved.token.is_some(),
            token_source,
            custom_tls_ca_configured: resolved.context.tls_cert.is_some(),
            separate_metrics_endpoint_configured: conn.metrics != conn.api.root(),
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct Inventory {
    tenant_count: Option<usize>,
    node_count: Option<usize>,
    task_counts: Option<TaskCounts>,
}

#[derive(Debug, Default, Serialize)]
struct TaskCounts {
    total: usize,
    running: usize,
    failed: usize,
    other: usize,
}

#[derive(Debug, Serialize)]
struct ServerBuild {
    version: Option<String>,
    build_revision: Option<String>,
    api_version: Option<String>,
    state_format_version: Option<u8>,
}

#[derive(Debug, Serialize)]
struct RaftSummary {
    local_state: String,
    ready: bool,
    quorum_ack_recent: Option<bool>,
    member_count: usize,
    voter_count: usize,
    unreachable_voter_count: usize,
}

#[derive(Debug, Serialize)]
struct BundlePolicy {
    included: &'static [&'static str],
    omitted: &'static [&'static str],
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    schema_version: u8,
    client_version: &'static str,
    server: ServerBuild,
    raft: Option<RaftSummary>,
    connection: ConnectionSummary,
    inventory: Inventory,
    findings: Vec<Finding>,
    support_bundle: BundlePolicy,
}

#[derive(Debug, Deserialize)]
struct ClusterResponse {
    server_version: String,
    build_revision: Option<String>,
    api_version: String,
    #[serde(default)]
    state_format_version: Option<u8>,
    raft: ClusterRaft,
}

#[derive(Debug, Deserialize)]
struct ClusterRaft {
    state: String,
    ready: bool,
    quorum_ack_recent: Option<bool>,
    members: Vec<ClusterMember>,
}

#[derive(Debug, Deserialize)]
struct ClusterMember {
    voter: bool,
    transport_reachable: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadinessReason {
    Raft,
    Bus,
}

fn readiness_reason(status: u16, body: &str) -> Option<ReadinessReason> {
    if status != 503 {
        return None;
    }
    if body.contains("raft not ready") {
        Some(ReadinessReason::Raft)
    } else if body.contains("bus not connected") {
        Some(ReadinessReason::Bus)
    } else {
        None
    }
}

fn bus_unavailable_finding(source: &'static str) -> Finding {
    Finding::new(
        "nats-connectivity",
        Severity::Error,
        Status::Failed,
        "message bus is disconnected",
        format!("{source} explicitly reported `bus not connected`"),
        "Check NATS reachability, credentials and server health.",
    )
}
#[derive(Debug)]
enum ProbeError {
    Timeout,
    Request(CtlError),
}

async fn probe<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, CtlError>>,
) -> Result<T, ProbeError> {
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ProbeError::Request(error)),
        Err(_) => Err(ProbeError::Timeout),
    }
}

/// Collects read-only observations, emits a report and optionally writes a private bundle.
pub async fn run(
    args: &DoctorArgs,
    conn: &Connection,
    resolved: &Resolved,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let timeout = Duration::from_secs(args.timeout);
    let mut report = DoctorReport {
        schema_version: 1,
        client_version: env!("CARGO_PKG_VERSION"),
        server: ServerBuild {
            version: None,
            build_revision: None,
            api_version: None,
            state_format_version: None,
        },
        raft: None,
        connection: ConnectionSummary::from_resolved(resolved, conn),
        inventory: Inventory::default(),
        findings: Vec::new(),
        support_bundle: BundlePolicy {
            included: BUNDLE_INCLUDED,
            omitted: BUNDLE_OMITTED,
        },
    };

    let health_url = format!("{}/healthz", conn.api.root());
    match probe(timeout, conn.api.text(&health_url)).await {
        Ok((200, _)) => report.findings.push(Finding::new(
            "api-liveness",
            Severity::Info,
            Status::Ok,
            "API liveness probe passed",
            "GET /healthz returned HTTP 200",
            "No action required.",
        )),
        Ok((status, _)) => report.findings.push(Finding::new(
            "api-liveness",
            Severity::Error,
            Status::Failed,
            "API liveness probe failed",
            format!("GET /healthz returned HTTP {status}; response body omitted"),
            "Check the Minga API process and its logs.",
        )),
        Err(error) => report.findings.push(probe_failed_finding(
            "api-liveness",
            "API liveness probe failed",
            probe_error_evidence(&error),
            "Check API reachability and the Minga API process.",
        )),
    }

    let ready_url = format!("{}/readyz", conn.api.root());
    let api_readiness = probe(timeout, conn.api.text(&ready_url)).await;
    let bus_down = matches!(
        &api_readiness,
        Ok((status, body)) if readiness_reason(*status, body) == Some(ReadinessReason::Bus)
    );
    match &api_readiness {
        Ok((200, _)) => report.findings.push(Finding::new(
            "api-readiness",
            Severity::Info,
            Status::Ok,
            "API readiness probe passed",
            "GET /readyz returned HTTP 200",
            "No action required.",
        )),
        Ok((status, body)) if readiness_reason(*status, body) == Some(ReadinessReason::Raft) => {
            report.findings.push(Finding::new(
                "api-readiness",
                Severity::Error,
                Status::Failed,
                "API is not ready because Raft is not ready",
                "GET /readyz returned HTTP 503: raft not ready",
                "Check manager membership, quorum and peer connectivity.",
            ))
        }
        Ok((status, body)) if readiness_reason(*status, body) == Some(ReadinessReason::Bus) => {
            report.findings.push(Finding::new(
                "api-readiness",
                Severity::Error,
                Status::Failed,
                "API is not ready because the message bus is disconnected",
                "GET /readyz returned HTTP 503: bus not connected",
                "Check the configured NATS endpoint, credentials and server health.",
            ))
        }
        Ok((status, _)) => report.findings.push(Finding::new(
            "api-readiness",
            Severity::Error,
            Status::Failed,
            "API readiness probe failed",
            format!("GET /readyz returned HTTP {status}; response body omitted"),
            "Check the Minga API readiness conditions and logs.",
        )),
        Err(error) => report.findings.push(probe_failed_finding(
            "api-readiness",
            "API readiness probe failed",
            probe_error_evidence(error),
            "Check API reachability and the Minga API readiness conditions.",
        )),
    }

    let separate_metrics = conn.metrics != conn.api.root();
    let metrics_readiness = if separate_metrics {
        let metrics_url = format!("{}/readyz", conn.metrics);
        Some(probe(timeout, conn.api.text(&metrics_url)).await)
    } else {
        None
    };
    if bus_down {
        report
            .findings
            .push(bus_unavailable_finding("The API readiness endpoint"));
    } else {
        match metrics_readiness {
            Some(Ok((200, _))) => report.findings.push(Finding::new(
                "nats-connectivity",
                Severity::Info,
                Status::Ok,
                "message bus readiness passed",
                "The configured metrics readiness endpoint returned HTTP 200",
                "No action required.",
            )),
            Some(Ok((status, body)))
                if readiness_reason(status, &body) == Some(ReadinessReason::Bus) =>
            {
                report
                    .findings
                    .push(bus_unavailable_finding("The metrics readiness endpoint"));
            }
            Some(Ok((status, _))) => report.findings.push(Finding::new(
                "nats-connectivity",
                Severity::Warning,
                Status::Unknown,
                "message bus connectivity could not be isolated",
                format!(
                    "Metrics readiness returned HTTP {status} without a recognized bus status; response body omitted"
                ),
                "Inspect Minga manager and NATS logs for the readiness failure.",
            )),
            Some(Err(error)) => report.findings.push(probe_finding(
                "nats-connectivity",
                "message bus connectivity could not be checked",
                probe_error_evidence(&error),
                "Check the configured metrics endpoint and manager readiness.",
            )),
            None => report.findings.push(Finding::new(
                "nats-connectivity",
                Severity::Info,
                Status::Unknown,
                "message bus connectivity is not independently exposed",
                "No separate metrics readiness endpoint is configured; API readiness does not prove NATS connectivity",
                "Configure a metrics endpoint for independent bus readiness, or verify NATS from manager logs.",
            )),
        }
    }

    match probe(timeout, conn.api.get("version", None)).await {
        Ok(version) => {
            report.server.version = value_string(&version, "Version");
            report.server.api_version = value_string(&version, "ApiVersion");
            report.server.build_revision = value_string(&version, "GitCommit");
            report.findings.push(Finding::new(
                "server-build",
                Severity::Info,
                Status::Ok,
                "server build metadata retrieved",
                format!(
                    "server version {}; build revision {}",
                    report.server.version.as_deref().unwrap_or("unknown"),
                    report.server.build_revision.as_deref().unwrap_or("unknown")
                ),
                "Compare the server build with the intended release.",
            ));
        }
        Err(error) => report.findings.push(probe_finding(
            "server-build",
            "server build metadata unavailable",
            probe_error_evidence(&error),
            "Check API authorization and server version compatibility.",
        )),
    }

    match probe(timeout, conn.api.get("minga/cluster", None)).await {
        Ok(value) => match serde_json::from_value::<ClusterResponse>(value) {
            Ok(cluster) => {
                let voter_count = cluster.raft.members.iter().filter(|member| member.voter).count();
                let unreachable_voter_count = cluster
                    .raft
                    .members
                    .iter()
                    .filter(|member| member.voter && member.transport_reachable == Some(false))
                    .count();
                let raft = RaftSummary {
                    local_state: cluster.raft.state,
                    ready: cluster.raft.ready,
                    quorum_ack_recent: cluster.raft.quorum_ack_recent,
                    member_count: cluster.raft.members.len(),
                    voter_count,
                    unreachable_voter_count,
                };
                let (severity, status, summary) = raft_assessment(
                    raft.ready,
                    raft.quorum_ack_recent,
                    raft.unreachable_voter_count,
                );
                let evidence = format!(
                    "state {}; ready {}; recent quorum acknowledgment {}; {} voters of {} members; {} unreachable voters",
                    raft.local_state,
                    raft.ready,
                    raft.quorum_ack_recent.map_or("unknown".to_owned(), |v| v.to_string()),
                    raft.voter_count,
                    raft.member_count,
                    raft.unreachable_voter_count
                );
                report.findings.push(Finding::new(
                    "raft-quorum",
                    severity,
                    status,
                    summary,
                    evidence,
                    "Check voter reachability and manager membership before changing cluster state.",
                ));
                report.server.version = Some(cluster.server_version);
                report.server.build_revision = cluster.build_revision;
                report.server.api_version = Some(cluster.api_version);
                report.server.state_format_version = cluster.state_format_version;
                report.raft = Some(raft);
            }
            Err(_) => report.findings.push(Finding::new(
                "raft-quorum",
                Severity::Warning,
                Status::Unknown,
                "Raft diagnostics could not be decoded",
                "The cluster diagnostics response did not match the supported schema; response body omitted",
                "Upgrade minga-ctl or compare its version with the server release.",
            )),
        },
        Err(error) => report.findings.push(probe_finding(
            "raft-quorum",
            "Raft diagnostics unavailable",
            probe_error_evidence(&error),
            "Check administrator authorization and server version compatibility.",
        )),
    }

    collect_inventory(&mut report, conn, timeout).await;
    report.findings.push(Finding::new(
        "auth-tls-configuration",
        Severity::Info,
        Status::Ok,
        "local authentication and transport settings summarized",
        format!(
            "scheme {}; token configured {}; token source {}; SSH tunnel {}; custom CA {}; separate metrics endpoint {}",
            report.connection.endpoint_scheme,
            report.connection.token_configured,
            report.connection.token_source,
            report.connection.ssh_tunnel_configured,
            report.connection.custom_tls_ca_configured,
            report.connection.separate_metrics_endpoint_configured
        ),
        "No credentials or configuration paths are included in this report.",
    ));

    if let Some(path) = &args.bundle {
        write_bundle(path, &report)?;
        eprintln!("wrote sanitized support bundle to {}", path.display());
    }
    print_report(&report, format, quiet)?;
    report::exit_status(&report.findings)
}

async fn collect_inventory(report: &mut DoctorReport, conn: &Connection, timeout: Duration) {
    match probe(timeout, conn.api.get("nodes", None)).await {
        Ok(value) => match count_collection(&value, "nodes") {
            Some(count) => {
                report.inventory.node_count = Some(count);
                report.findings.push(Finding::new(
                    "node-inventory",
                    Severity::Info,
                    Status::Ok,
                    "node inventory retrieved",
                    format!("{count} nodes"),
                    "No action required.",
                ));
            }
            None => report.findings.push(Finding::new(
                "node-inventory",
                Severity::Warning,
                Status::Unknown,
                "node inventory response was not a recognized collection",
                "Response structure omitted",
                "Upgrade minga-ctl or compare its version with the server release.",
            )),
        },
        Err(error) => report.findings.push(probe_finding(
            "node-inventory",
            "node inventory unavailable",
            probe_error_evidence(&error),
            "Check cluster-operator authorization and API reachability.",
        )),
    }

    let tenants_url = format!("{}/minga/tenants", conn.api.root());
    match probe(timeout, conn.api.text(&tenants_url)).await {
        Ok((200, body)) => match serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| count_collection(&value, "tenants"))
        {
            Some(count) => {
                report.inventory.tenant_count = Some(count);
                report.findings.push(Finding::new(
                    "tenant-inventory",
                    Severity::Info,
                    Status::Ok,
                    "tenant inventory retrieved",
                    format!("{count} tenants"),
                    "No action required.",
                ));
            }
            None => report.findings.push(Finding::new(
                "tenant-inventory",
                Severity::Warning,
                Status::Unknown,
                "tenant inventory response was not a recognized collection",
                "Response structure omitted",
                "Upgrade minga-ctl or compare its version with the server release.",
            )),
        },
        Ok((status, _)) => report.findings.push(Finding::new(
            "tenant-inventory",
            Severity::Warning,
            Status::Unknown,
            "tenant inventory unavailable",
            format!("GET /minga/tenants returned HTTP {status}; response body omitted"),
            "Check administrator authorization for tenant inventory.",
        )),
        Err(error) => report.findings.push(probe_finding(
            "tenant-inventory",
            "tenant inventory unavailable",
            probe_error_evidence(&error),
            "Check administrator authorization and API reachability.",
        )),
    }

    match probe(timeout, conn.api.get("tasks", None)).await {
        Ok(value) => match task_counts(&value) {
            Some(counts) => {
                report.findings.push(task_health_finding(&counts));
                report.inventory.task_counts = Some(counts);
            }
            None => report.findings.push(Finding::new(
                "task-health",
                Severity::Warning,
                Status::Unknown,
                "task inventory response was not a recognized collection",
                "Response structure omitted",
                "Upgrade minga-ctl or compare its version with the server release.",
            )),
        },
        Err(error) => report.findings.push(probe_finding(
            "task-health",
            "task health unavailable for this identity",
            probe_error_evidence(&error),
            "Use a tenant-scoped identity to inspect task health.",
        )),
    }
}

fn count_collection(value: &Value, key: &str) -> Option<usize> {
    value
        .as_array()
        .or_else(|| value.get(key).and_then(Value::as_array))
        .map(|items| items.len())
}

fn task_counts(value: &Value) -> Option<TaskCounts> {
    let tasks = value.as_array()?;
    let mut counts = TaskCounts {
        total: tasks.len(),
        ..TaskCounts::default()
    };
    for task in tasks {
        match task.pointer("/Status/State").and_then(Value::as_str) {
            Some("running") => counts.running += 1,
            Some("failed" | "rejected") => counts.failed += 1,
            _ => counts.other += 1,
        }
    }
    Some(counts)
}
fn task_health_finding(counts: &TaskCounts) -> Finding {
    let failed = counts.failed > 0;
    Finding::new(
        "task-health",
        if failed {
            Severity::Error
        } else {
            Severity::Info
        },
        if failed { Status::Failed } else { Status::Ok },
        if failed {
            "failed tasks reported"
        } else {
            "no failed tasks reported"
        },
        format!(
            "{} tasks: {} running, {} failed, {} other; task error text omitted",
            counts.total, counts.running, counts.failed, counts.other
        ),
        "Inspect failed tasks through an authorized tenant-scoped view.",
    )
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}
fn raft_assessment(
    ready: bool,
    quorum_ack_recent: Option<bool>,
    unreachable_voters: usize,
) -> (Severity, Status, &'static str) {
    if !ready || quorum_ack_recent == Some(false) {
        (Severity::Error, Status::Failed, "Raft quorum is not ready")
    } else if quorum_ack_recent.is_none() {
        (
            Severity::Warning,
            Status::Unknown,
            "Raft quorum freshness is not reported",
        )
    } else if unreachable_voters > 0 {
        (
            Severity::Warning,
            Status::Degraded,
            "Raft quorum is available but voters are unreachable",
        )
    } else {
        (
            Severity::Info,
            Status::Ok,
            "Raft reports a recent quorum acknowledgment",
        )
    }
}

fn probe_finding(
    id: &'static str,
    summary: &'static str,
    evidence: impl Into<String>,
    next_action: &'static str,
) -> Finding {
    Finding::new(
        id,
        Severity::Warning,
        Status::Unknown,
        summary,
        evidence,
        next_action,
    )
}

fn probe_failed_finding(
    id: &'static str,
    summary: &'static str,
    evidence: impl Into<String>,
    next_action: &'static str,
) -> Finding {
    Finding::new(
        id,
        Severity::Error,
        Status::Failed,
        summary,
        evidence,
        next_action,
    )
}
fn probe_error_evidence(error: &ProbeError) -> String {
    match error {
        ProbeError::Timeout => "probe timed out; endpoint and response omitted".to_owned(),
        ProbeError::Request(CtlError::Auth { .. }) => "HTTP 401: credentials rejected".to_owned(),
        ProbeError::Request(CtlError::Api { status: 403, .. }) => {
            "HTTP 403: permission denied".to_owned()
        }
        ProbeError::Request(CtlError::Api { status: 404, .. }) => {
            "HTTP 404: endpoint or scope unavailable".to_owned()
        }
        ProbeError::Request(CtlError::Api { status, .. }) => {
            format!("HTTP {status}; response body omitted")
        }
        ProbeError::Request(CtlError::Unreachable { .. }) => {
            "API endpoint unreachable; address and details omitted".to_owned()
        }
        ProbeError::Request(_) => "request failed; details omitted".to_owned(),
    }
}

fn print_report(report: &DoctorReport, format: Format, quiet: bool) -> Result<(), CtlError> {
    if quiet {
        return report::print_quiet(&report.findings);
    }
    if format == Format::Table {
        return output::print(
            Format::Table,
            false,
            &report::finding_rows(&report.findings),
        );
    }
    report::print_structured(report, format)
}

fn write_bundle(path: &Path, report: &DoctorReport) -> Result<(), CtlError> {
    let bytes = serde_json::to_vec_pretty(report)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let fail = |error: std::io::Error| CtlError::ConfigInvalid {
        path: path.display().to_string(),
        detail: error.to_string(),
    };
    let mut file = options.open(path).map_err(fail)?;
    if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(fail(error));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ApiClient;
    use crate::config::{Context, Resolved};
    use crate::transport::Connection;
    use serde_json::json;

    #[test]
    fn support_bundle_redacts_secrets_paths_and_endpoints() {
        let resolved = Resolved {
            name: "SENTINEL_CONTEXT".to_owned(),
            context: Context {
                ssh: Some("SENTINEL_SSH_TARGET".to_owned()),
                api_endpoint: Some("https://SENTINEL_ENDPOINT".to_owned()),
                remote_token_file: Some("SENTINEL_TOKEN_PATH".to_owned()),
                token_command: Some(vec!["SENTINEL_COMMAND_SECRET".to_owned()]),
                tls_cert: Some(PathBuf::from("SENTINEL_CERT_PATH")),
                client_cert: Some(PathBuf::from("SENTINEL_CLIENT_CERT_PATH")),
                client_key: Some(PathBuf::from("SENTINEL_CLIENT_KEY_PATH")),
                metrics_endpoint: Some("https://SENTINEL_METRICS_ENDPOINT".to_owned()),
                nats_url: Some("nats://user:SENTINEL_NATS_SECRET@host".to_owned()),
                nodes: Vec::new(),
                registry_auth_file: Some("SENTINEL_REGISTRY_PATH".to_owned()),
            },
            endpoint: "https://SENTINEL_ENDPOINT".to_owned(),
            token_source: TokenSource::Command(vec!["SENTINEL_TOKEN_COMMAND".to_owned()]),
            token: Some("SENTINEL_BEARER_TOKEN".to_owned()),
        };
        let api = ApiClient::new("https://SENTINEL_ENDPOINT", None, None, "test", false).unwrap();
        let conn = Connection::direct(api, "https://SENTINEL_METRICS_ENDPOINT".to_owned());
        let report = DoctorReport {
            schema_version: 1,
            client_version: "test",
            server: ServerBuild {
                version: Some("1.2.3".to_owned()),
                build_revision: None,
                api_version: Some("1.41".to_owned()),
                state_format_version: None,
            },
            raft: None,
            connection: ConnectionSummary::from_resolved(&resolved, &conn),
            inventory: Inventory::default(),
            findings: Vec::new(),
            support_bundle: BundlePolicy {
                included: BUNDLE_INCLUDED,
                omitted: BUNDLE_OMITTED,
            },
        };
        let bundle = serde_json::to_string_pretty(&report).unwrap();
        for secret in [
            "SENTINEL_CONTEXT",
            "SENTINEL_SSH_TARGET",
            "SENTINEL_ENDPOINT",
            "SENTINEL_TOKEN_PATH",
            "SENTINEL_COMMAND_SECRET",
            "SENTINEL_CERT_PATH",
            "SENTINEL_CLIENT_CERT_PATH",
            "SENTINEL_CLIENT_KEY_PATH",
            "SENTINEL_METRICS_ENDPOINT",
            "SENTINEL_NATS_SECRET",
            "SENTINEL_REGISTRY_PATH",
            "SENTINEL_TOKEN_COMMAND",
            "SENTINEL_BEARER_TOKEN",
        ] {
            assert!(!bundle.contains(secret), "bundle exposed {secret}");
        }
        assert!(bundle.contains("API tokens and token command arguments or file paths"));
        assert!(bundle.contains("TLS certificate and private-key contents or paths"));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("doctor.json");
        write_bundle(&path, &report).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), bundle);
        assert!(write_bundle(&path, &report).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    #[test]
    fn api_error_body_is_redacted_from_evidence() {
        let api_error = ProbeError::Request(CtlError::Api {
            status: 500,
            message: "SENTINEL_BEARER_TOKEN in response".to_owned(),
        });
        assert!(!probe_error_evidence(&api_error).contains("SENTINEL_BEARER_TOKEN"));
    }

    #[test]
    fn task_counts_aggregate_failures_without_reading_error_text() {
        let tasks = json!([
            {"Status": {"State": "running", "Err": "SENTINEL_TASK_ERROR"}},
            {"Status": {"State": "failed", "Err": "SENTINEL_TASK_ERROR"}},
            {"Status": {"State": "shutdown"}}
        ]);
        let counts = task_counts(&tasks).unwrap();
        assert_eq!(counts.total, 3);
        assert_eq!(counts.running, 1);
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.other, 1);
        assert!(!serde_json::to_string(&counts)
            .unwrap()
            .contains("SENTINEL_TASK_ERROR"));
        let finding = task_health_finding(&counts);
        assert_eq!(finding.id, "task-health");
        assert_eq!(finding.severity, Severity::Error);
        assert_eq!(finding.status, Status::Failed);
        assert!(!serde_json::to_string(&finding)
            .unwrap()
            .contains("SENTINEL_TASK_ERROR"));
    }

    #[test]
    fn raft_assessment_marks_stale_quorum_failed_and_unreachable_voters_degraded() {
        assert_eq!(
            raft_assessment(true, Some(true), 0),
            (
                Severity::Info,
                Status::Ok,
                "Raft reports a recent quorum acknowledgment"
            )
        );
        assert_eq!(
            raft_assessment(true, Some(true), 1),
            (
                Severity::Warning,
                Status::Degraded,
                "Raft quorum is available but voters are unreachable"
            )
        );
        assert_eq!(
            raft_assessment(true, Some(false), 0),
            (Severity::Error, Status::Failed, "Raft quorum is not ready")
        );
        assert_eq!(
            raft_assessment(true, None, 0),
            (
                Severity::Warning,
                Status::Unknown,
                "Raft quorum freshness is not reported"
            )
        );
    }
    #[test]
    fn readiness_reasons_distinguish_raft_bus_and_unrecognized_failures() {
        assert_eq!(
            readiness_reason(503, "not ready: raft not ready"),
            Some(ReadinessReason::Raft)
        );
        assert_eq!(
            readiness_reason(503, "not ready: bus not connected"),
            Some(ReadinessReason::Bus)
        );
        let bus_finding = bus_unavailable_finding("The metrics readiness endpoint");
        assert_eq!(bus_finding.id, "nats-connectivity");
        assert_eq!(bus_finding.severity, Severity::Error);
        assert_eq!(bus_finding.status, Status::Failed);
        assert_eq!(readiness_reason(503, "SENTINEL_RESPONSE"), None);
        assert_eq!(readiness_reason(200, "not ready: bus not connected"), None);
    }
}
