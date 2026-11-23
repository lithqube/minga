//! `minga-ctl upgrade preflight`: read-only compatibility and maintenance planning.

use std::time::Duration;

use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::Resolved;
use crate::error::CtlError;
use crate::output::{self, Format};
use crate::transport::Connection;

use super::report::{self, Finding, Severity, Status};

const TARGET_RELEASE: ReleaseMetadata = ReleaseMetadata {
    version: "1.3.0",
    state_format: 12,
    bundled_nats: "2.15.3",
    external_nats_tested: "2.14.10 and 2.15.3",
    min_jetstream_api_level: 4,
    release_status: "planned; candidate verification is pending",
};
const CURRENT_RELEASES: &[ReleaseMetadata] = &[
    ReleaseMetadata {
        version: "1.1.0",
        state_format: 2,
        bundled_nats: "2.10.22",
        external_nats_tested: "not recorded",
        min_jetstream_api_level: 0,
        release_status: "released",
    },
    ReleaseMetadata {
        version: "1.2.0",
        state_format: 10,
        bundled_nats: "2.10.22",
        external_nats_tested: "not recorded",
        min_jetstream_api_level: 0,
        release_status: "released",
    },
    ReleaseMetadata {
        version: "1.2.0-rc.1",
        state_format: 10,
        bundled_nats: "2.10.22",
        external_nats_tested: "not recorded",
        min_jetstream_api_level: 0,
        release_status: "release candidate",
    },
];

#[derive(Debug, Clone, Copy)]
struct ReleaseMetadata {
    version: &'static str,
    state_format: u8,
    bundled_nats: &'static str,
    external_nats_tested: &'static str,
    min_jetstream_api_level: usize,
    release_status: &'static str,
}

/// Upgrade command group.
#[derive(Debug, Args)]
pub struct UpgradeArgs {
    #[command(subcommand)]
    command: UpgradeCommand,
}

#[derive(Debug, Subcommand)]
enum UpgradeCommand {
    /// Compare the running cluster with a known target; makes no changes.
    Preflight(PreflightArgs),
}

#[derive(Debug, Args)]
struct PreflightArgs {
    /// Target Minga release. Preflight metadata is currently available for v1.3.0.
    #[arg(long)]
    to: String,
    /// Seconds each API request may take.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=60))]
    timeout: u64,
}

#[derive(Debug, Serialize)]
struct CurrentCluster {
    version: String,
    build_revision: Option<String>,
    state_format: Option<u8>,
    bundled_nats_version: Option<&'static str>,
    raft: Option<RaftSummary>,
    api_auth_configured: bool,
    api_transport_secure: bool,
    tenant_count: Option<usize>,
    node_count: Option<usize>,
}

#[derive(Debug, Serialize, Clone)]
struct RaftSummary {
    node_id: u64,
    state: String,
    leader_id: Option<u64>,
    ready: bool,
    quorum_ack_recent: Option<bool>,
    member_count: usize,
    voter_count: usize,
    unreachable_voter_count: usize,
}

#[derive(Debug, Serialize)]
struct TargetCluster {
    version: &'static str,
    release_status: &'static str,
    state_format: u8,
    bundled_nats_version: &'static str,
    external_nats_tested: &'static str,
    minimum_jetstream_api_level: usize,
}

#[derive(Debug, Serialize)]
struct UpgradeReport {
    schema_version: u8,
    client_version: &'static str,
    current: CurrentCluster,
    target: TargetCluster,
    findings: Vec<Finding>,
    ordered_steps: Vec<&'static str>,
    disruption: &'static str,
    rollback_boundary: &'static str,
    read_only: bool,
}

#[derive(Debug, Deserialize)]
struct ClusterDiagnostics {
    build_revision: Option<String>,
    #[serde(default)]
    state_format_version: Option<u8>,
    raft: ClusterRaft,
}

#[derive(Debug, Deserialize)]
struct ClusterRaft {
    node_id: u64,
    state: String,
    leader_id: Option<u64>,
    ready: bool,
    quorum_ack_recent: Option<bool>,
    members: Vec<ClusterMember>,
}

#[derive(Debug, Deserialize)]
struct ClusterMember {
    voter: bool,
    transport_reachable: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct VersionInfo {
    #[serde(rename = "Version")]
    version: String,
    #[serde(rename = "GitCommit")]
    build_revision: Option<String>,
}

/// Runs a read-only preflight against the selected cluster.
pub async fn run(
    args: &UpgradeArgs,
    conn: &Connection,
    resolved: &Resolved,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let UpgradeCommand::Preflight(args) = &args.command;
    let target = known_target(&args.to).ok_or_else(|| {
        CtlError::Usage(format!(
            "no upgrade metadata for target `{}`; known target: v{}",
            args.to, TARGET_RELEASE.version
        ))
    })?;
    let timeout = Duration::from_secs(args.timeout);
    // Keep the client's own error: a 401/403 must reach the user as an auth failure.
    let version_value = tokio::time::timeout(timeout, conn.api.get("version", None))
        .await
        .map_err(|_| {
            CtlError::Usage("timed out reading the running server version".to_owned())
        })??;
    let version: VersionInfo = serde_json::from_value(version_value)?;
    let current_metadata = known_current(&version.version);
    let cluster = request(timeout, conn.api.get("minga/cluster", None))
        .await
        .and_then(|value| {
            serde_json::from_value::<ClusterDiagnostics>(value)
                .map_err(|error| format!("unexpected cluster diagnostics: {error}"))
        });
    let reported_format = cluster
        .as_ref()
        .ok()
        .and_then(|diagnostics| diagnostics.state_format_version);
    let state_format =
        reported_format.or_else(|| current_metadata.map(|release| release.state_format));
    let node_count = collection_count(request(timeout, conn.api.get("nodes", None)).await.ok());
    let tenants_url = format!("{}/minga/tenants", conn.api.root());
    let tenant_count = request(timeout, conn.api.text(&tenants_url))
        .await
        .ok()
        .and_then(|(status, body)| (status == 200).then_some(body))
        .and_then(|body| serde_json::from_str::<Value>(&body).ok())
        .and_then(|value| collection_count(Some(value)));

    let raft = cluster.as_ref().ok().map(|diagnostics| {
        let members = &diagnostics.raft.members;
        RaftSummary {
            node_id: diagnostics.raft.node_id,
            state: diagnostics.raft.state.clone(),
            leader_id: diagnostics.raft.leader_id,
            ready: diagnostics.raft.ready,
            quorum_ack_recent: diagnostics.raft.quorum_ack_recent,
            member_count: members.len(),
            voter_count: members.iter().filter(|member| member.voter).count(),
            unreachable_voter_count: members
                .iter()
                .filter(|member| member.voter && member.transport_reachable == Some(false))
                .count(),
        }
    });
    let current = CurrentCluster {
        version: version.version,
        build_revision: cluster
            .as_ref()
            .ok()
            .and_then(|diagnostics| diagnostics.build_revision.clone())
            .or(version.build_revision),
        state_format,
        bundled_nats_version: current_metadata.map(|release| release.bundled_nats),
        raft,
        // A token or an mTLS client certificate both authenticate the context.
        api_auth_configured: resolved.token.is_some() || resolved.context.client_cert.is_some(),
        api_transport_secure: resolved.context.ssh.is_some()
            || resolved.endpoint.starts_with("https://"),
        tenant_count,
        node_count,
    };
    let mut report = UpgradeReport {
        schema_version: 1,
        client_version: env!("CARGO_PKG_VERSION"),
        current,
        target: TargetCluster {
            version: target.version,
            release_status: target.release_status,
            state_format: target.state_format,
            bundled_nats_version: target.bundled_nats,
            external_nats_tested: target.external_nats_tested,
            minimum_jetstream_api_level: target.min_jetstream_api_level,
        },
        findings: Vec::new(),
        ordered_steps: vec![
            "Before the maintenance window, create and protect a `minga state export`; it contains service environment values and is sensitive.",
            "For external NATS, verify JetStream API level 4 or newer. The target bundle is NATS 2.15.3; current external NATS is not exposed by the management API.",
            "Stop all Minga managers and workers.",
            "With every role stopped, take a cold backup of node data directories, volume data, secret values, TLS files and NATS data, and verify it restores; state export omits these and an online copy is not a restorable backup.",
            "Install the same candidate on every node, start the manager cohort together, then start workers; do not mix wire-format versions.",
            "After the candidate is ready, verify doctor readiness, Raft quorum, tenant ownership, task health and restored workload behavior.",
        ],
        disruption: "Plan a coordinated Minga control-plane outage. The API and scheduler are unavailable during replacement; workload continuity depends on replica placement and is not guaranteed.",
        rollback_boundary: "Once the candidate opens the data it is migrated to state format v12, which the previous binary cannot read; do not downgrade in place. Rollback requires restoring the complete pre-upgrade backup with the previous binary across the cluster; NATS downgrade is not a supported rollback path.",
        read_only: true,
    };
    let mut state_finding =
        state_format_finding(report.current.state_format, report.target.state_format);
    if reported_format.is_none() && report.current.state_format.is_some() {
        state_finding.evidence.push_str(
            "; inferred from release metadata for the reported server version, not reported by the server",
        );
    }
    report.findings.push(state_finding);
    report.findings.push(Finding {
        id: "current-binary",
        severity: Severity::Info,
        status: Status::Ok,
        summary: "current server identity observed".into(),
        evidence: format!(
            "server {}; build {}; state format {}",
            report.current.version,
            report
                .current
                .build_revision
                .as_deref()
                .unwrap_or("not reported"),
            report
                .current
                .state_format
                .map_or("unknown".to_owned(), |version| format!("v{version}"))
        ),
        next_action: "Compare the observed server build with the intended starting release.",
    });
    report.findings.push(raft_finding(
        report
            .current
            .raft
            .as_ref()
            .ok_or_else(|| cluster.as_ref().err().map_or("", String::as_str)),
    ));
    report.findings.push(if report.current.api_auth_configured
        && report.current.api_transport_secure
    {
        Finding {
            id: "api-security",
            severity: Severity::Info,
            status: Status::Ok,
            summary: "selected API context has authentication and protected transport configured".into(),
            evidence: format!(
                "API token or client certificate configured {}; transport protected by {}",
                report.current.api_auth_configured,
                if resolved.context.ssh.is_some() {
                    "SSH tunnel"
                } else {
                    "HTTPS"
                }
            ),
            next_action: "Verify manager Raft TLS and NATS TLS settings separately on every node.",
        }
    } else {
        Finding {
            id: "api-security",
            severity: Severity::Warning,
            status: Status::Unknown,
            summary: "selected API context does not prove authenticated protected transport".into(),
            evidence: format!(
                "API token or client certificate configured {}; protected transport configured {}",
                report.current.api_auth_configured, report.current.api_transport_secure
            ),
            next_action: "Use an authenticated HTTPS or SSH context, then verify Raft and NATS TLS independently on every node.",
        }
    });
    report.findings.push(Finding {
        id: "nats-compatibility",
        severity: Severity::Warning,
        status: Status::Unknown,
        summary: "target NATS compatibility is known; current external bus is not independently identified".into(),
        evidence: format!(
            "target bundle NATS {}; external NATS tested on {}; require JetStream API level {}; current bundled version {}; current external API level not reported",
            target.bundled_nats,
            target.external_nats_tested,
            target.min_jetstream_api_level,
            report.current.bundled_nats_version.unwrap_or("unknown")
        ),
        next_action: "Before upgrading, verify the external server's JetStream API level or use the supported bundled NATS line.",
    });
    let tenants_known = report.current.tenant_count.is_some();
    report.findings.push(Finding {
        id: "tenant-impact",
        severity: if tenants_known {
            Severity::Info
        } else {
            Severity::Warning
        },
        status: if tenants_known {
            Status::Ok
        } else {
            Status::Unknown
        },
        summary: "legacy resources migrate into the default tenant".into(),
        evidence: format!(
            "state migration defaults legacy resource ownership to `default` and ensures the default tenant; current tenant count {}",
            report.current.tenant_count.map_or("unavailable".to_owned(), |n| n.to_string())
        ),
        next_action: "After upgrade, confirm the tenant inventory and assign nodes to their intended tenants before placing tenant workloads.",
    });
    report.findings.push(Finding {
        id: "release-gate",
        severity: Severity::Warning,
        status: Status::Unknown,
        summary: "target release is not yet approved for production upgrade".into(),
        evidence: format!("v{}: {}", target.version, target.release_status),
        next_action: "Wait for the v1.3.0 release candidate and complete the documented candidate gate before any VM or staging upgrade.",
    });
    report.findings.push(Finding {
        id: "cluster-inventory",
        severity: if report.current.node_count.is_some() && tenants_known {
            Severity::Info
        } else {
            Severity::Warning
        },
        status: if report.current.node_count.is_some() && tenants_known {
            Status::Ok
        } else {
            Status::Unknown
        },
        summary: if report.current.node_count.is_some() && tenants_known {
            "read-only cluster inventory collected".into()
        } else {
            "cluster inventory is incomplete".into()
        },
        evidence: format!(
            "nodes visible to this identity {}; tenant count {}; service names and IDs omitted",
            report
                .current
                .node_count
                .map_or("unavailable".to_owned(), |n| n.to_string()),
            report
                .current
                .tenant_count
                .map_or("unavailable".to_owned(), |n| n.to_string())
        ),
        next_action: "Use an operator-admin identity if tenant inventory is unavailable.",
    });

    print_report(&report, format, quiet)?;
    report::exit_status(&report.findings)
}

async fn request<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = Result<T, CtlError>>,
) -> Result<T, String> {
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("timed out after {}s", timeout.as_secs())),
    }
}

fn known_target(version: &str) -> Option<ReleaseMetadata> {
    (version.strip_prefix('v').unwrap_or(version) == TARGET_RELEASE.version)
        .then_some(TARGET_RELEASE)
}

fn known_current(version: &str) -> Option<&'static ReleaseMetadata> {
    let version = version.strip_prefix('v').unwrap_or(version);
    CURRENT_RELEASES
        .iter()
        .find(|release| release.version == version)
}

/// `Err` carries why the diagnostics could not be read.
fn raft_finding(raft: Result<&RaftSummary, &str>) -> Finding {
    match raft {
        Ok(raft) if !raft.ready || raft.quorum_ack_recent == Some(false) => Finding {
            id: "raft-membership",
            severity: Severity::Error,
            status: Status::Failed,
            summary: "Raft is not ready for an upgrade".into(),
            evidence: raft_evidence(raft),
            next_action: "Restore leader readiness and recent quorum acknowledgement before planning a coordinated upgrade.",
        },
        Ok(raft)
            if raft.unreachable_voter_count > 0 || raft.quorum_ack_recent.is_none() =>
        {
            Finding {
                id: "raft-membership",
                severity: Severity::Warning,
                status: Status::Unknown,
                summary: "Raft membership is observed but quorum is not fully confirmed".into(),
                evidence: raft_evidence(raft),
                next_action: "Confirm all manager processes and quorum on the leader before the maintenance window.",
            }
        }
        Ok(raft) => Finding {
            id: "raft-membership",
            severity: Severity::Info,
            status: Status::Ok,
            summary: "Raft membership and recent quorum are observed".into(),
            evidence: raft_evidence(raft),
            next_action: "Keep the membership stable during the coordinated manager upgrade.",
        },
        Err(reason) => Finding {
            id: "raft-membership",
            severity: Severity::Error,
            status: Status::Unknown,
            summary: "Raft membership could not be read".into(),
            evidence: format!("The server did not return cluster diagnostics: {reason}"),
            next_action: "Use an authenticated operator context and confirm the membership with `minga-ctl info` before the upgrade.",
        },
    }
}

fn raft_evidence(raft: &RaftSummary) -> String {
    format!(
        "local node {}; state {}; leader {}; ready {}; recent quorum {}; members {}; voters {}; unreachable voters {}",
        raft.node_id,
        raft.state,
        raft.leader_id.map_or("unknown".to_owned(), |id| id.to_string()),
        raft.ready,
        raft.quorum_ack_recent
            .map_or("unknown".to_owned(), |recent| recent.to_string()),
        raft.member_count,
        raft.voter_count,
        raft.unreachable_voter_count
    )
}
fn collection_count(value: Option<Value>) -> Option<usize> {
    let value = value?;
    value
        .as_array()
        .or_else(|| value.get("nodes").and_then(Value::as_array))
        .or_else(|| value.get("tenants").and_then(Value::as_array))
        .map(Vec::len)
}

fn state_format_finding(current: Option<u8>, target: u8) -> Finding {
    match current {
        Some(version) if version == target => Finding {
            id: "state-format",
            severity: Severity::Info,
            status: Status::Ok,
            summary: "persisted state format already matches target".into(),
            evidence: format!("current and target format are v{target}"),
            next_action: "No state-format migration is required.",
        },
        Some(version) if (2..target).contains(&version) => Finding {
            id: "state-format",
            severity: Severity::Info,
            status: Status::Ok,
            summary: "direct persisted state migration is implemented".into(),
            evidence: format!(
                "format v{version} migrates directly to v{target} on open; Raft peer and worker wire frames require the target format on every node"
            ),
            next_action: "Back up first and upgrade every Minga role in one coordinated maintenance window.",
        },
        Some(version) if version < 2 => Finding {
            id: "state-format",
            severity: Severity::Error,
            status: Status::Failed,
            summary: "current state format has no migration to target".into(),
            evidence: format!(
                "no migration from format v{version} to v{target}; this build migrates formats v2 through v11"
            ),
            next_action: "Stop before upgrading; use a documented intermediate migration path or stay on the current release.",
        },
        Some(version) if version > target => Finding {
            id: "state-format",
            severity: Severity::Error,
            status: Status::Failed,
            summary: "current state format is newer than target".into(),
            evidence: format!(
                "current format v{version} is newer than target format v{target}; this target cannot read it"
            ),
            next_action: "Use a target build that supports the current state format.",
        },
        Some(version) => Finding {
            id: "state-format",
            severity: Severity::Error,
            status: Status::Failed,
            summary: "no supported direct state migration is known".into(),
            evidence: format!(
                "no direct migration from format v{version} to v{target}; supported migration sources are v2 through v11"
            ),
            next_action: "Stop before upgrading; use a documented intermediate migration path or stay on the current release.",
        },
        None => Finding {
            id: "state-format",
            severity: Severity::Error,
            status: Status::Unknown,
            summary: "current persisted state format could not be established".into(),
            evidence: "The running server did not report its format and its version has no trusted release metadata.".to_owned(),
            next_action: "Do not upgrade until the current state format is confirmed from trusted release metadata or a supported diagnostics endpoint.",
        },
    }
}

fn print_report(report: &UpgradeReport, format: Format, quiet: bool) -> Result<(), CtlError> {
    if quiet {
        return report::print_quiet(&report.findings);
    }
    if format == Format::Table {
        let mut rows = report::finding_rows(&report.findings);
        for (index, step) in report.ordered_steps.iter().enumerate() {
            let id = format!("plan-step-{}", index + 1);
            rows.push(
                vec![id.clone(), "info".to_owned(), "info".to_owned(), "ordered action".to_owned(), (*step).to_owned(), "Follow in sequence; no step is executed by this command.".to_owned()],
                json!({"check": id, "status": "info", "severity": "info", "summary": "ordered action", "evidence": step, "next_action": "No action performed."}),
            );
        }
        rows.push(
            vec![
                "disruption".to_owned(),
                "warning".to_owned(),
                "warning".to_owned(),
                "maintenance window".to_owned(),
                report.disruption.to_owned(),
                "Schedule the change with service owners.".to_owned(),
            ],
            json!({"check": "disruption", "summary": "maintenance window", "evidence": report.disruption}),
        );
        rows.push(
            vec![
                "rollback-boundary".to_owned(),
                "warning".to_owned(),
                "warning".to_owned(),
                "restore from backup".to_owned(),
                report.rollback_boundary.to_owned(),
                "Keep the pre-upgrade backup until the candidate is accepted.".to_owned(),
            ],
            json!({"check": "rollback-boundary", "summary": "restore from backup", "evidence": report.rollback_boundary}),
        );
        return output::print(Format::Table, false, &rows);
    }
    report::print_structured(report, format)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_format_assessment_uses_only_supported_migrations() {
        let from_v2 = state_format_finding(Some(2), 12);
        assert_eq!(from_v2.id, "state-format");
        assert_eq!(from_v2.severity, Severity::Info);
        assert!(from_v2
            .evidence
            .contains("format v2 migrates directly to v12"));

        let current = state_format_finding(Some(12), 12);
        assert_eq!(current.status, Status::Ok);

        let unsupported = state_format_finding(Some(1), 12);
        assert_eq!(unsupported.status, Status::Failed);
        assert_eq!(unsupported.severity, Severity::Error);
        assert!(unsupported
            .evidence
            .contains("this build migrates formats v2 through v11"));
        let newer = state_format_finding(Some(13), 12);
        assert!(newer.evidence.contains("target cannot read it"));

        let unknown = state_format_finding(None, 12);
        assert_eq!(unknown.status, Status::Unknown);
        assert_eq!(unknown.severity, Severity::Error);
    }

    #[test]
    fn target_metadata_fails_closed_for_unknown_versions() {
        assert_eq!(known_target("v1.3.0").unwrap().state_format, 12);
        assert!(known_target("9.9.9").is_none());
        assert_eq!(known_current("v1.2.0").unwrap().state_format, 10);
        assert!(known_current("1.0.2").is_none());
    }

    #[test]
    fn raft_finding_fails_closed_and_omits_peer_addresses() {
        let healthy = RaftSummary {
            node_id: 1,
            state: "Leader".to_owned(),
            leader_id: Some(1),
            ready: true,
            quorum_ack_recent: Some(true),
            member_count: 3,
            voter_count: 3,
            unreachable_voter_count: 0,
        };
        let finding = raft_finding(Ok(&healthy));
        assert_eq!(finding.status, Status::Ok);
        assert_eq!(finding.severity, Severity::Info);
        assert!(finding.evidence.contains("members 3; voters 3"));
        assert!(!finding.evidence.contains("10.0.0."));

        let degraded = RaftSummary {
            quorum_ack_recent: Some(false),
            unreachable_voter_count: 1,
            ..healthy.clone()
        };
        let finding = raft_finding(Ok(&degraded));
        assert_eq!(finding.status, Status::Failed);
        assert_eq!(finding.severity, Severity::Error);

        let follower = RaftSummary {
            quorum_ack_recent: None,
            ..healthy
        };
        assert_eq!(raft_finding(Ok(&follower)).status, Status::Unknown);
        let unread = raft_finding(Err("API 403: forbidden"));
        assert_eq!(unread.status, Status::Unknown);
        assert_eq!(unread.severity, Severity::Error);
        assert!(unread.evidence.contains("API 403"));
    }

    #[test]
    fn a_check_that_could_not_run_blocks_the_upgrade() {
        let unknown_format = state_format_finding(None, 12);
        let unread_raft = raft_finding(Err("timed out after 10s"));
        let warning = Finding {
            id: "release-gate",
            severity: Severity::Warning,
            status: Status::Unknown,
            summary: "".into(),
            evidence: String::new(),
            next_action: "",
        };
        assert!(report::exit_status(&[unknown_format]).is_err());
        assert!(report::exit_status(&[unread_raft]).is_err());
        assert!(report::exit_status(&[warning]).is_ok());
        assert!(report::exit_status(&[state_format_finding(Some(10), 11)]).is_ok());
    }
}
