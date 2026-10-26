//! Placement constraint parsing and evaluation.
//!
//! Compose placement constraints look like `node.labels.gpu == true`.
//! This module parses them into a typed form and evaluates them against
//! a [`ClusterNode`]. Unknown fields fail loudly — a constraint we cannot
//! evaluate must never silently pass.

use minga_state::types::ClusterNode;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ConstraintError {
    #[error("invalid constraint {0:?}: expected `<field> <op> <value>`")]
    Syntax(String),
    #[error("invalid constraint {0:?}: unsupported field `{1}`")]
    UnsupportedField(String, String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintOp {
    Eq,
    Ne,
}

/// A parsed placement constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    /// Dotted field path, e.g. `node.labels.gpu` or `node.id`.
    pub field: String,
    pub op: ConstraintOp,
    pub value: String,
}

/// Parses a constraint string like `node.labels.gpu == true`.
///
/// Phase 0 supports `node.id` and `node.labels.<key>`. Anything else is
/// an error — we fail loudly rather than silently ignoring a constraint
/// the user thinks is being enforced.
pub fn parse_constraint(s: &str) -> Result<Constraint, ConstraintError> {
    let s = s.trim();
    let (field, op, value) = if let Some((l, r)) = s.split_once("==") {
        (l.trim(), ConstraintOp::Eq, r.trim())
    } else if let Some((l, r)) = s.split_once("!=") {
        (l.trim(), ConstraintOp::Ne, r.trim())
    } else {
        return Err(ConstraintError::Syntax(s.to_string()));
    };

    if field.is_empty() || value.is_empty() {
        return Err(ConstraintError::Syntax(s.to_string()));
    }

    if field != "node.id" && !field.starts_with("node.labels.") {
        return Err(ConstraintError::UnsupportedField(
            s.to_string(),
            field.to_string(),
        ));
    }
    if field.starts_with("node.labels.") && field.len() == "node.labels.".len() {
        return Err(ConstraintError::UnsupportedField(
            s.to_string(),
            field.to_string(),
        ));
    }

    Ok(Constraint {
        field: field.to_string(),
        op,
        value: value.to_string(),
    })
}

/// Evaluates a parsed constraint against a node.
///
/// A missing label with `==` is a non-match; with `!=` it is a match
/// (the label is absent, so it is "not equal" to the expected value).
pub fn evaluate(c: &Constraint, node: &ClusterNode) -> bool {
    let actual = resolve_field(&c.field, node);
    match c.op {
        ConstraintOp::Eq => actual.as_deref() == Some(c.value.as_str()),
        ConstraintOp::Ne => actual.as_deref() != Some(c.value.as_str()),
    }
}

/// Evaluates a raw constraint string against a node. Returns `false` on
/// parse errors — callers that need to distinguish "doesn't match" from
/// "invalid constraint" should use [`parse_constraint`] directly.
pub fn matches_str(raw: &str, node: &ClusterNode) -> bool {
    match parse_constraint(raw) {
        Ok(c) => evaluate(&c, node),
        Err(_) => false,
    }
}

/// Resolves a dotted field path to the node's value.
fn resolve_field(field: &str, node: &ClusterNode) -> Option<String> {
    if field == "node.id" {
        return Some(node.id.to_string());
    }
    if let Some(key) = field.strip_prefix("node.labels.") {
        return node.labels.get(key).cloned();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::{NodeStatus, ResourceSpec};
    use std::collections::HashMap;

    fn node(id: u64, labels: &[(&str, &str)]) -> ClusterNode {
        ClusterNode {
            id,
            address: format!("10.0.0.{id}:50051"),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            status: NodeStatus::Active,
            capacity: ResourceSpec::default(),
        }
    }

    #[test]
    fn parse_eq() {
        let c = parse_constraint("node.labels.gpu == true").unwrap();
        assert_eq!(c.field, "node.labels.gpu");
        assert_eq!(c.op, ConstraintOp::Eq);
        assert_eq!(c.value, "true");
    }

    #[test]
    fn parse_ne() {
        let c = parse_constraint("node.labels.zone != a").unwrap();
        assert_eq!(c.op, ConstraintOp::Ne);
        assert_eq!(c.value, "a");
    }

    #[test]
    fn parse_node_id() {
        let c = parse_constraint("node.id == 3").unwrap();
        assert_eq!(c.field, "node.id");
        assert_eq!(c.value, "3");
    }

    #[test]
    fn parse_errors() {
        assert!(parse_constraint("node.labels.gpu").is_err());
        assert!(parse_constraint("== true").is_err());
        assert!(parse_constraint("node.labels.gpu ==").is_err());
        assert!(parse_constraint("node.labels.gpu ~= true").is_err());
        assert!(parse_constraint("engine.labels.x == y").is_err());
        assert!(parse_constraint("node.platform.os == linux").is_err());
        assert!(parse_constraint("node.labels. == x").is_err());
    }

    #[test]
    fn match_label_eq() {
        let n = node(1, &[("gpu", "true"), ("zone", "a")]);
        let c = parse_constraint("node.labels.gpu == true").unwrap();
        assert!(evaluate(&c, &n));
        let c = parse_constraint("node.labels.gpu == false").unwrap();
        assert!(!evaluate(&c, &n));
    }

    #[test]
    fn match_label_ne() {
        let n = node(1, &[("zone", "a")]);
        let c = parse_constraint("node.labels.zone != b").unwrap();
        assert!(evaluate(&c, &n));
        let c = parse_constraint("node.labels.zone != a").unwrap();
        assert!(!evaluate(&c, &n));
    }

    #[test]
    fn missing_label_eq_is_no_match() {
        let n = node(1, &[]);
        let c = parse_constraint("node.labels.gpu == true").unwrap();
        assert!(!evaluate(&c, &n));
    }

    #[test]
    fn missing_label_ne_is_match() {
        let n = node(1, &[]);
        let c = parse_constraint("node.labels.gpu != true").unwrap();
        assert!(evaluate(&c, &n));
    }

    #[test]
    fn match_node_id() {
        let n = node(3, &[]);
        let c = parse_constraint("node.id == 3").unwrap();
        assert!(evaluate(&c, &n));
        let c = parse_constraint("node.id != 3").unwrap();
        assert!(!evaluate(&c, &n));
    }

    #[test]
    fn matches_str_handles_bad_input() {
        let n = node(1, &[("gpu", "true")]);
        assert!(matches_str("node.labels.gpu == true", &n));
        assert!(!matches_str("garbage", &n));
    }
}
