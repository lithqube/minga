//! Findings shared by `doctor` and `upgrade preflight`: one severity scale,
//! one exit rule and one output shape.

use std::io::Write;

use serde::Serialize;
use serde_json::json;

use crate::error::CtlError;
use crate::output::{self, Format, Rows};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    Degraded,
    Failed,
    Unknown,
}

#[derive(Debug, Serialize)]
pub(crate) struct Finding {
    pub(crate) id: &'static str,
    pub(crate) severity: Severity,
    pub(crate) status: Status,
    pub(crate) summary: String,
    pub(crate) evidence: String,
    pub(crate) next_action: &'static str,
}

impl Finding {
    pub(crate) fn new(
        id: &'static str,
        severity: Severity,
        status: Status,
        summary: impl Into<String>,
        evidence: impl Into<String>,
        next_action: &'static str,
    ) -> Self {
        Self {
            id,
            severity,
            status,
            summary: summary.into(),
            evidence: evidence.into(),
            next_action,
        }
    }
}

/// Every `Error` finding fails the command, including one whose check could
/// not run: a check that cannot confirm a precondition must not exit 0.
pub(crate) fn exit_status(findings: &[Finding]) -> Result<(), CtlError> {
    let failed = findings
        .iter()
        .filter(|finding| finding.severity == Severity::Error)
        .count();
    if failed > 0 {
        return Err(CtlError::HealthFailed { failed });
    }
    Ok(())
}

/// `--quiet`: the IDs of findings that are not ok, one per line.
pub(crate) fn print_quiet(findings: &[Finding]) -> Result<(), CtlError> {
    let ids = findings
        .iter()
        .filter(|finding| finding.status != Status::Ok)
        .map(|finding| finding.id)
        .collect::<Vec<_>>()
        .join("\n");
    if ids.is_empty() {
        return Ok(());
    }
    output::say(false, &ids)
}

/// The findings as table rows; callers may push more rows before printing.
pub(crate) fn finding_rows(findings: &[Finding]) -> Rows {
    let mut rows = Rows::new(&[
        "CHECK",
        "STATUS",
        "SEVERITY",
        "SUMMARY",
        "EVIDENCE",
        "NEXT ACTION",
    ]);
    for finding in findings {
        let status = format!("{:?}", finding.status).to_lowercase();
        let severity = format!("{:?}", finding.severity).to_lowercase();
        rows.push(
            vec![
                finding.id.to_owned(),
                status.clone(),
                severity.clone(),
                finding.summary.clone(),
                finding.evidence.clone(),
                finding.next_action.to_owned(),
            ],
            json!({
                "check": finding.id,
                "status": status,
                "severity": severity,
                "summary": finding.summary,
                "evidence": finding.evidence,
                "next_action": finding.next_action,
            }),
        );
    }
    rows
}

/// Writes the whole report as JSON or YAML to stdout.
pub(crate) fn print_structured<T: Serialize>(report: &T, format: Format) -> Result<(), CtlError> {
    let text = match format {
        Format::Json => serde_json::to_string_pretty(report)?,
        Format::Yaml => {
            serde_yaml::to_string(report).map_err(|error| CtlError::Output(error.to_string()))?
        }
        Format::Table => unreachable!("tables print through `finding_rows`"),
    };
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{text}")
        .and_then(|()| stdout.flush())
        .map_err(|error| CtlError::Output(format!("write to stdout: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(severity: Severity, status: Status) -> Finding {
        Finding::new("check", severity, status, "", "", "")
    }

    #[test]
    fn only_error_findings_fail_the_command() {
        for status in [Status::Failed, Status::Unknown, Status::Degraded] {
            assert!(matches!(
                exit_status(&[finding(Severity::Error, status)]),
                Err(CtlError::HealthFailed { failed: 1 })
            ));
        }
        assert!(exit_status(&[
            finding(Severity::Warning, Status::Unknown),
            finding(Severity::Info, Status::Ok),
        ])
        .is_ok());
        assert!(matches!(
            exit_status(&[
                finding(Severity::Error, Status::Failed),
                finding(Severity::Error, Status::Unknown),
            ]),
            Err(CtlError::HealthFailed { failed: 2 })
        ));
    }
}
