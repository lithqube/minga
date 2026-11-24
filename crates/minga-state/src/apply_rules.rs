//! What an operator needs to raise the apply-rule level safely (ADR 0009):
//! the level in force, the level each manager's build implements, and the
//! check that a raise is only taken when every manager implements it.

use serde::{Deserialize, Serialize};

use crate::error::StateError;
use crate::types::NodeId;

/// One manager and the highest apply-rule level its build implements.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagerApplyRules {
    pub id: NodeId,
    /// `None` when the manager did not answer, or answered in a way this build
    /// cannot read (a build from before the level existed): the level is unknown.
    pub implemented: Option<u32>,
}

/// The apply-rule level in force and what each manager implements.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyRulesStatus {
    /// The level every replica applies entries under.
    pub level: u32,
    /// The highest level the answering manager's build implements.
    pub implemented: u32,
    /// Every member of the Raft membership, voter or learner.
    pub managers: Vec<ManagerApplyRules>,
}

/// Refuses a raise to `level` unless every manager is known to implement it.
///
/// A manager that implements fewer levels stops applying at the raise entry and
/// the raise cannot be undone, so an unknown level is refused the same way a
/// lower one is. Not raising at all (`level <= current`) needs no check: the
/// state machine answers `Ok` for a repeat and `Conflict` for a lower level.
pub(crate) fn check_raise(
    level: u32,
    current: u32,
    managers: &[ManagerApplyRules],
) -> Result<(), StateError> {
    if level <= current {
        return Ok(());
    }
    let mut behind = Vec::new();
    let mut unknown = Vec::new();
    for manager in managers {
        match manager.implemented {
            Some(implemented) if implemented >= level => {}
            Some(implemented) => {
                behind.push(format!("manager {} (level {implemented})", manager.id));
            }
            None => unknown.push(format!("manager {}", manager.id)),
        }
    }
    if behind.is_empty() && unknown.is_empty() {
        return Ok(());
    }
    let mut reasons = Vec::new();
    if !behind.is_empty() {
        reasons.push(format!("implements a lower level: {}", behind.join(", ")));
    }
    if !unknown.is_empty() {
        reasons.push(format!("did not report a level: {}", unknown.join(", ")));
    }
    Err(StateError::ApplyRules(format!(
        "cannot raise the apply rules to level {level}; upgrade every manager first ({})",
        reasons.join("; ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(id: NodeId, implemented: Option<u32>) -> ManagerApplyRules {
        ManagerApplyRules { id, implemented }
    }

    fn refusal(result: Result<(), StateError>) -> String {
        match result {
            Err(StateError::ApplyRules(message)) => message,
            other => panic!("expected an apply-rules refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_raise_is_refused_while_one_manager_implements_a_lower_level() {
        let managers = [
            manager(1, Some(2)),
            manager(2, Some(1)),
            manager(3, Some(2)),
        ];
        let message = refusal(check_raise(2, 1, &managers));
        assert!(message.contains("manager 2 (level 1)"), "{message}");
        assert!(!message.contains("manager 1"), "{message}");
    }

    #[test]
    fn a_raise_is_refused_while_a_manager_has_not_reported_a_level() {
        let managers = [manager(1, Some(2)), manager(2, None)];
        let message = refusal(check_raise(2, 1, &managers));
        assert!(
            message.contains("did not report a level: manager 2"),
            "{message}"
        );
    }

    #[test]
    fn a_raise_is_taken_when_every_manager_implements_the_level() {
        let managers = [manager(1, Some(3)), manager(2, Some(2))];
        assert!(check_raise(2, 1, &managers).is_ok());
    }

    #[test]
    fn a_repeat_or_lower_level_needs_no_manager_check() {
        let managers = [manager(1, Some(1)), manager(2, None)];
        assert!(check_raise(2, 2, &managers).is_ok());
        assert!(check_raise(1, 2, &managers).is_ok());
    }
}
