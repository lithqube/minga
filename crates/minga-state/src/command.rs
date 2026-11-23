//! Commands proposed to Raft and the responses returned by the state machine.

use serde::{Deserialize, Serialize};

use crate::types::{
    ChangeRequest, ClusterNode, Config, ConfigId, Network, NetworkId, NodeId, Secret, SecretId,
    Service, ServiceId, Task, TaskId, Tenant, TenantId, TenantQuotas, Volume, VolumeId,
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
    /// An [`StateCommand::UpsertService`] from a snapshot import whose byte
    /// quota check discounts `byte_allowance`: how much this import's records
    /// for the tenant grew when re-encoded from an older format. It is scoped
    /// to the one write, so nothing outlives the import.
    ImportService {
        service: Service,
        byte_allowance: u64,
    },
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
    /// Boxed: a request holds two full services. The encoding is unchanged.
    CreateChangeRequest(Box<ChangeRequest>),
    DecideChangeRequest {
        id: String,
        tenant_id: TenantId,
        actor: String,
        approve: bool,
        now_unix: u64,
    },
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

    /// Raises the apply-rule level every replica uses from this log index on
    /// (ADR 0009). It only goes up. A build that implements fewer levels stops
    /// applying instead of applying later entries under older rules. Propose
    /// it only once every manager runs a build that implements `level`.
    SetApplyRules {
        level: u32,
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
    /// The change request as this write committed it, so a follower can
    /// answer from the write itself instead of its possibly-lagging replica.
    ChangeRequest(Box<ChangeRequest>),
}
