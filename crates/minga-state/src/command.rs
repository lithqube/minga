//! Commands proposed to Raft and the responses returned by the state machine.

use serde::{Deserialize, Serialize};

use crate::types::{
    ClusterNode, Config, ConfigId, Network, NetworkId, NodeId, Secret, SecretId, Service,
    ServiceId, Task, TaskId, Volume, VolumeId,
};

/// A mutation proposed to the Raft log. This is the `D` (app data) type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateCommand {
    UpsertNode(ClusterNode),
    RemoveNode(NodeId),

    UpsertService(Service),
    RemoveService(ServiceId),

    UpsertTask(Task),
    RemoveTask(TaskId),

    UpsertNetwork(Network),
    RemoveNetwork(NetworkId),

    UpsertVolume(Volume),
    RemoveVolume(VolumeId),

    UpsertSecret(Secret),
    RemoveSecret(SecretId),

    UpsertConfig(Config),
    RemoveConfig(ConfigId),

    /// Refreshes a node's lease in one log entry: sets `last_seen_unix`
    /// and revives the node unless it is draining. A missing record stays
    /// missing (`NotFound`): a removed node is not recreated. Read-modify-
    /// write from a client would race an operator's drain. Appended last:
    /// bincode encodes the variant index, so existing log entries keep
    /// decoding.
    RenewNodeLease {
        id: NodeId,
        last_seen_unix: u64,
    },
}

/// The response returned by the state machine after applying a command.
/// This is the `R` (app data response) type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateResponse {
    Ok,
    NotFound,
    Error(String),
}
