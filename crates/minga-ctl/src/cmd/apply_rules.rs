//! The Raft apply-rule level as the operator sees it (ADR 0009): what the API
//! reports, and `upgrade apply-rules`, the one-way raise.

use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::ApiClient;
use crate::error::CtlError;
use crate::output::Format;

use super::service;

const PATH: &str = "minga/apply-rules";

/// One manager and the highest level its build implements; `None` when it did
/// not report one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManagerLevel {
    pub(crate) id: u64,
    pub(crate) implemented: Option<u32>,
}

/// The level in force and what each manager implements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Status {
    pub(crate) level: u32,
    /// The highest level the manager that answered implements.
    pub(crate) implemented: u32,
    pub(crate) managers: Vec<ManagerLevel>,
}

impl Status {
    pub(crate) async fn fetch(api: &ApiClient) -> Result<Self, CtlError> {
        Ok(serde_json::from_value(api.get_unversioned(PATH).await?)?)
    }

    /// Why the level cannot be raised to `level` yet, or `None` when every
    /// manager is known to implement it. A manager that did not report is
    /// treated like one that implements less: the raise cannot be undone.
    pub(crate) fn blockers(&self, level: u32) -> Option<String> {
        let behind = self
            .managers
            .iter()
            .filter_map(|m| match m.implemented {
                Some(implemented) if implemented < level => {
                    Some(format!("manager {} (level {implemented})", m.id))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let unknown = self
            .managers
            .iter()
            .filter(|m| m.implemented.is_none())
            .map(|m| format!("manager {}", m.id))
            .collect::<Vec<_>>();
        let mut reasons = Vec::new();
        if !behind.is_empty() {
            reasons.push(format!("implements a lower level: {}", behind.join(", ")));
        }
        if !unknown.is_empty() {
            reasons.push(format!("did not report a level: {}", unknown.join(", ")));
        }
        (!reasons.is_empty())
            .then(|| format!("upgrade every manager first ({})", reasons.join("; ")))
    }

    /// `manager 1: 1, manager 2: unknown`.
    pub(crate) fn managers_summary(&self) -> String {
        self.managers
            .iter()
            .map(|m| {
                let level = m
                    .implemented
                    .map_or("unknown".to_owned(), |l| l.to_string());
                format!("manager {}: {level}", m.id)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug, Args)]
pub struct ApplyRulesArgs {
    /// The level to raise to. Defaults to the highest level the manager you
    /// are talking to implements. Every manager must implement it.
    #[arg(long, value_name = "N")]
    to: Option<u32>,
    /// Do not ask for confirmation.
    #[arg(long)]
    yes: bool,
}

impl ApplyRulesArgs {
    /// Audit metadata for the write.
    pub(crate) fn audit(&self) -> (&'static str, Value) {
        (
            "upgrade apply-rules",
            json!({"to": self.to, "yes": self.yes}),
        )
    }
}

/// Raises the apply-rule level once every manager implements it. The API
/// checks again, so a manager that was added or downgraded after this read is
/// still refused.
pub(crate) async fn run(
    args: &ApplyRulesArgs,
    api: &ApiClient,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let status = Status::fetch(api).await?;
    let target = args.to.unwrap_or(status.implemented);
    if target < status.level {
        return Err(CtlError::Usage(format!(
            "the apply-rule level is {} and only goes up; it cannot be lowered to {target}",
            status.level
        )));
    }
    if target == status.level {
        return service::report(
            format,
            quiet,
            &format!("apply rules already at level {target}; nothing to do"),
            json!(status),
        );
    }
    if let Some(why) = status.blockers(target) {
        return Err(CtlError::Usage(format!(
            "cannot raise the apply rules to level {target}: {why}"
        )));
    }
    service::confirm(
        "upgrade apply-rules",
        args.yes,
        &format!(
            "Raise the Raft apply-rule level from {} to {target} on every manager.\n\
             This cannot be undone: a manager that implements fewer levels stops applying at this entry.",
            status.level
        ),
    )?;
    let raised: Status = serde_json::from_value(
        api.post_unversioned(PATH, &json!({"level": target}))
            .await?,
    )?;
    service::report(
        format,
        quiet,
        &format!(
            "raised the apply-rule level from {} to {}",
            status.level, raised.level
        ),
        json!(raised),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(managers: &[(u64, Option<u32>)]) -> Status {
        Status {
            level: 1,
            implemented: 2,
            managers: managers
                .iter()
                .map(|(id, implemented)| ManagerLevel {
                    id: *id,
                    implemented: *implemented,
                })
                .collect(),
        }
    }

    #[test]
    fn a_manager_on_a_lower_level_blocks_the_raise() {
        let why = status(&[(1, Some(2)), (2, Some(1))]).blockers(2).unwrap();
        assert!(why.contains("manager 2 (level 1)"), "{why}");
        assert!(!why.contains("manager 1"), "{why}");
    }

    #[test]
    fn a_manager_that_did_not_report_blocks_the_raise() {
        let why = status(&[(1, Some(2)), (2, None)]).blockers(2).unwrap();
        assert!(why.contains("did not report a level: manager 2"), "{why}");
    }

    #[test]
    fn every_manager_on_the_level_leaves_nothing_in_the_way() {
        assert_eq!(status(&[(1, Some(2)), (2, Some(3))]).blockers(2), None);
    }

    #[test]
    fn the_summary_names_each_manager() {
        assert_eq!(
            status(&[(1, Some(2)), (2, None)]).managers_summary(),
            "manager 1: 2, manager 2: unknown"
        );
    }
}
