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

    #[error("not the leader (current leader: {0:?})")]
    NotLeader(Option<u64>),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(String),
}

impl StateError {
    pub(crate) fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }
}
