//! Errors from the state store.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("raft fatal error: {0}")]
    Fatal(String),

    #[error("raft API error: {0}")]
    Raft(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("this manager is not the Raft leader; retry against {}", leader_hint(*.0))]
    NotLeader(Option<u64>),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(String),

    /// A manager membership operation was refused or conflicted.
    #[error("{0}")]
    Membership(String),
    /// An import would replace services that already exist.
    #[error("services already exist: {0} (import with overwrite to replace them)")]
    Conflict(String),
    /// An import file this build cannot read.
    #[error("{0}")]
    BadSnapshot(String),
}

fn leader_hint(leader: Option<u64>) -> String {
    match leader {
        Some(id) => format!("manager {id}"),
        None => "the leader once one is elected".into(),
    }
}

impl StateError {
    pub(crate) fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }
}
