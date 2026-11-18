//! Commands proposed to Raft and the responses returned by the state machine.

use serde::{Deserialize, Serialize};

use crate::types::{
    ClusterNode, Config, ConfigId, Network, NetworkId, NodeId, Secret, SecretId, Service,
    ServiceId, Task, TaskId, Tenant, TenantId, TenantQuotas, Volume, VolumeId,
};

/// A mutation proposed to the Raft log. This is the `D` (app data) type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateCommand {
    UpsertNode(ClusterNode),
    RemoveNode(NodeId),
    SetNodeNatsUserPublicKey {
        id: NodeId,
        public_key: Option<String>,
    },

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

    UpsertTenant(Tenant),
    CreateTenant(Tenant),
    UpdateTenant {
        id: TenantId,
        name: String,
        quotas: TenantQuotas,
    },
    RemoveTenant(TenantId),
    RecordNatsUserRevocation {
        user_public_key: String,
    },

    /// Refreshes a node's lease in one log entry: sets `last_seen_unix`
    /// and revives the node unless it is draining. A missing record stays
    /// missing (`NotFound`): a removed node is not recreated. Read-modify-
    /// write from a client would race an operator's drain.
    RenewNodeLease {
        id: NodeId,
        last_seen_unix: u64,
    },

    /// Removes an object only while it still belongs to `tenant_id`, so a
    /// delayed removal cannot delete another tenant's object that reused
    /// the ID. The untenanted `Remove*` variants stay for log replay.
    RemoveOwned {
        kind: ObjectKind,
        id: String,
        tenant_id: TenantId,
    },
}

/// The tenant-owned tables [`StateCommand::RemoveOwned`] can delete from.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectKind {
    Service,
    Task,
    Network,
    Volume,
    Secret,
    Config,
}

/// The response returned by the state machine after applying a command.
/// This is the `R` (app data response) type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateResponse {
    Ok,
    NotFound,
    Conflict(String),
    Error(String),
}
