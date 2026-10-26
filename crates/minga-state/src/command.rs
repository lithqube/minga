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
}

/// The response returned by the state machine after applying a command.
/// This is the `R` (app data response) type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateResponse {
    Ok,
    NotFound,
    Error(String),
}
