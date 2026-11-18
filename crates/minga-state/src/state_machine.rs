//! redb-backed Raft state machine.
//!
//! Stores the cluster's desired state (nodes, services, tasks, networks,
//! volumes, secrets, configs) in a redb database. Commands from the Raft log
//! are applied here once they are committed.

use std::fmt::Debug;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};

use openraft::entry::{EntryPayload, RaftPayload};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    AnyError, BasicNode, Entry, ErrorSubject, ErrorVerb, LogId, OptionalSend, StorageError,
    StorageIOError, StoredMembership,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::codec::{
    check_format, FORMAT_KEY, FORMAT_VERSION, V2_FORMAT_VERSION, V3_FORMAT_VERSION,
    V4_FORMAT_VERSION, V5_FORMAT_VERSION, V6_FORMAT_VERSION, V7_FORMAT_VERSION,
};
use crate::command::{StateCommand, StateResponse};
use crate::types::*;
use crate::TypeConfig;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};

// --- redb table definitions --------------------------------------------------

const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const SERVICES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("services");
const TASKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("tasks");
const NETWORKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("networks");
const VOLUMES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");
const SECRETS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("secrets");
const CONFIGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("configs");
const TENANTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("tenants");
const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sm_meta");

/// All entity tables, used for clearing on snapshot install.
const ENTITY_TABLES: [TableDefinition<&str, &[u8]>; 8] = [
    NODES_TABLE,
    SERVICES_TABLE,
    TASKS_TABLE,
    NETWORKS_TABLE,
    VOLUMES_TABLE,
    SECRETS_TABLE,
    CONFIGS_TABLE,
    TENANTS_TABLE,
];

/// Re-encodes entity records and advances the marker during the v8 migration.
///
/// The caller advances the marker in the same redb transaction, so a failure
/// leaves both the old values and the old marker intact.
#[allow(clippy::result_large_err)]
fn migrate_state(tx: &redb::WriteTransaction) -> Result<(), StorageIOError<u64>> {
    // Serde defaults fields introduced in later formats; re-encoding all
    // records applies the new optional NATS identity shape.
    rewrite_state_table::<ClusterNode>(tx, NODES_TABLE)?;
    rewrite_state_table::<Tenant>(tx, TENANTS_TABLE)?;
    rewrite_state_table::<Service>(tx, SERVICES_TABLE)?;
    rewrite_state_table::<Task>(tx, TASKS_TABLE)?;
    rewrite_state_table::<Network>(tx, NETWORKS_TABLE)?;
    rewrite_state_table::<Volume>(tx, VOLUMES_TABLE)?;
    rewrite_state_table::<Secret>(tx, SECRETS_TABLE)?;
    rewrite_state_table::<Config>(tx, CONFIGS_TABLE)?;
    let mut meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
    meta.insert(FORMAT_KEY, [FORMAT_VERSION].as_slice())
        .map_err(sm_io_err)?;
    Ok(())
}

#[allow(clippy::result_large_err)]
fn rewrite_state_table<T: DeserializeOwned + Serialize>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&str, &[u8]>,
) -> Result<(), StorageIOError<u64>> {
    let records = {
        let table = tx.open_table(definition).map_err(sm_io_err)?;
        let mut records = Vec::new();
        for row in table.iter().map_err(sm_io_err)? {
            let (key, value) = row.map_err(sm_io_err)?;
            records.push((key.value().to_owned(), value.value().to_vec()));
        }
        records
    };

    let mut table = tx.open_table(definition).map_err(sm_io_err)?;
    for (key, value) in records {
        let decoded: T = crate::codec::decode(&value)
            .map_err(|e| sm_io_err_any(ErrorSubject::Store, ErrorVerb::Read, e))?;
        let encoded = crate::codec::encode(&decoded)
            .map_err(|e| sm_io_err_any(ErrorSubject::Store, ErrorVerb::Write, e))?;
        table
            .insert(key.as_str(), encoded.as_slice())
            .map_err(sm_io_err)?;
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn tenant_has_records<T: DeserializeOwned>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&str, &[u8]>,
    tenant_id: &str,
    matches: impl Fn(&T, &str) -> bool,
) -> Result<bool, StorageError<u64>> {
    let table = tx.open_table(definition).map_err(sm_io_err)?;
    for row in table.iter().map_err(sm_io_err)? {
        let (_, value) = row.map_err(sm_io_err)?;
        let record: T = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        if matches(&record, tenant_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether `id` already exists under a tenant other than `tenant_id`. Called
/// inside the write transaction, before the table is opened for writing, so
/// two API writes that both passed admission cannot take over each other's
/// object.
#[allow(clippy::result_large_err)]
fn owned_by_other_tenant<T: DeserializeOwned>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&str, &[u8]>,
    id: &str,
    tenant_id: &str,
    tenant_of: impl Fn(&T) -> &str,
) -> Result<bool, StorageError<u64>> {
    let table = tx.open_table(definition).map_err(sm_io_err)?;
    let Some(raw) = table.get(id).map_err(sm_io_err)? else {
        return Ok(false);
    };
    let record: T = crate::codec::decode(raw.value())
        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
    Ok(tenant_of(&record) != tenant_id)
}

const NAME_IN_USE: &str = "name is already in use";

#[allow(clippy::result_large_err)]
fn tenant_quota(
    tx: &redb::WriteTransaction,
    tenant_id: &str,
) -> Result<TenantQuotas, StorageError<u64>> {
    let table = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
    let quotas = table
        .get(tenant_id)
        .map_err(sm_io_err)?
        .map(|value| crate::codec::decode(value.value()))
        .transpose()
        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?
        .map_or_else(TenantQuotas::default, |tenant: Tenant| tenant.quotas);
    Ok(quotas)
}

#[allow(clippy::result_large_err)]
fn enforce_count_quota<T: DeserializeOwned>(
    tx: &redb::WriteTransaction,
    table_definition: TableDefinition<&str, &[u8]>,
    tenant_id: &str,
    id: &str,
    quota_name: &str,
    limit: Option<u64>,
    tenant_of: impl Fn(&T) -> &str,
) -> Result<Option<String>, StorageError<u64>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let table = tx.open_table(table_definition).map_err(sm_io_err)?;
    let mut count = 0u64;
    for row in table.iter().map_err(sm_io_err)? {
        let (key, value) = row.map_err(sm_io_err)?;
        let record: T = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        if key.value() != id && tenant_of(&record) == tenant_id {
            count += 1;
        }
    }
    if count.saturating_add(1) > limit {
        return Ok(Some(format!(
            "tenant quota exceeded: {quota_name} limit {limit}"
        )));
    }
    Ok(None)
}
#[allow(clippy::result_large_err)]
fn enforce_job_quota(
    tx: &redb::WriteTransaction,
    tenant_id: &str,
    id: &str,
    is_job: bool,
    limit: Option<u64>,
) -> Result<Option<String>, StorageError<u64>> {
    let (Some(limit), true) = (limit, is_job) else {
        return Ok(None);
    };
    let table = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
    let mut count = 0u64;
    for row in table.iter().map_err(sm_io_err)? {
        let (key, value) = row.map_err(sm_io_err)?;
        let service: Service = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        if key.value() != id && service.tenant_id == tenant_id && service.is_job {
            count += 1;
        }
    }
    if count.saturating_add(1) > limit {
        return Ok(Some(format!("tenant quota exceeded: jobs limit {limit}")));
    }
    Ok(None)
}

#[allow(clippy::result_large_err)]
fn enforce_desired_state_bytes(
    tx: &redb::WriteTransaction,
    tenant_id: &str,
    replaced_table: &str,
    replaced_id: &str,
    candidate_len: usize,
) -> Result<Option<String>, StorageError<u64>> {
    let limit = tenant_quota(tx, tenant_id)?.desired_state_bytes;
    let Some(limit) = limit else {
        return Ok(None);
    };
    let mut total = 0u64;
    for (table_name, definition) in [
        ("services", SERVICES_TABLE),
        ("tasks", TASKS_TABLE),
        ("networks", NETWORKS_TABLE),
        ("volumes", VOLUMES_TABLE),
        ("secrets", SECRETS_TABLE),
        ("configs", CONFIGS_TABLE),
    ] {
        let table = tx.open_table(definition).map_err(sm_io_err)?;
        for row in table.iter().map_err(sm_io_err)? {
            let (key, value) = row.map_err(sm_io_err)?;
            let bytes = value.value();
            let belongs = match table_name {
                "services" => crate::codec::decode::<Service>(bytes)
                    .map(|record| record.tenant_id == tenant_id),
                "tasks" => {
                    crate::codec::decode::<Task>(bytes).map(|record| record.tenant_id == tenant_id)
                }
                "networks" => crate::codec::decode::<Network>(bytes)
                    .map(|record| record.tenant_id == tenant_id),
                "volumes" => crate::codec::decode::<Volume>(bytes)
                    .map(|record| record.tenant_id == tenant_id),
                "secrets" => crate::codec::decode::<Secret>(bytes)
                    .map(|record| record.tenant_id == tenant_id),
                "configs" => crate::codec::decode::<Config>(bytes)
                    .map(|record| record.tenant_id == tenant_id),
                _ => unreachable!(),
            }
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
            if belongs && !(table_name == replaced_table && key.value() == replaced_id) {
                total = total.saturating_add(bytes.len() as u64);
            }
        }
    }
    total = total.saturating_add(candidate_len as u64);
    if total > limit {
        return Ok(Some(format!(
            "tenant quota exceeded: desired_state_bytes limit {limit}"
        )));
    }
    Ok(None)
}

#[allow(clippy::result_large_err)]
fn tenant_has_resources(
    tx: &redb::WriteTransaction,
    tenant_id: &str,
) -> Result<bool, StorageError<u64>> {
    Ok(
        // Nodes too: a recreated tenant ID must not inherit the old tenant's nodes.
        tenant_has_records::<ClusterNode>(tx, NODES_TABLE, tenant_id, |r, id| r.tenant_id == id)?
            || tenant_has_records::<Service>(tx, SERVICES_TABLE, tenant_id, |r, id| {
                r.tenant_id == id
            })?
            || tenant_has_records::<Task>(tx, TASKS_TABLE, tenant_id, |r, id| r.tenant_id == id)?
            || tenant_has_records::<Network>(tx, NETWORKS_TABLE, tenant_id, |r, id| {
                r.tenant_id == id
            })?
            || tenant_has_records::<Volume>(tx, VOLUMES_TABLE, tenant_id, |r, id| {
                r.tenant_id == id
            })?
            || tenant_has_records::<Secret>(tx, SECRETS_TABLE, tenant_id, |r, id| {
                r.tenant_id == id
            })?
            || tenant_has_records::<Config>(tx, CONFIGS_TABLE, tenant_id, |r, id| {
                r.tenant_id == id
            })?,
    )
}

// --- snapshot -----------------------------------------------------------------

/// A serializable snapshot of the entire state machine.
#[derive(Serialize, Deserialize)]
struct StateSnapshot {
    nodes: Vec<ClusterNode>,
    services: Vec<Service>,
    tasks: Vec<Task>,
    networks: Vec<Network>,
    volumes: Vec<Volume>,
    secrets: Vec<Secret>,
    configs: Vec<Config>,
    #[serde(default)]
    tenants: Vec<Tenant>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

// --- state machine ------------------------------------------------------------

/// A redb-backed Raft state machine holding the cluster's desired state.
///
/// Cloneable: clones share the same underlying redb database via `Arc<Mutex<..>>`.
/// The `last_applied` / `last_membership` fields are only authoritative on the
/// instance owned by Raft; clones are used for reads and ignore them.
#[derive(Clone)]
pub struct RedbStateMachine {
    db: Arc<Mutex<Database>>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

#[allow(clippy::result_large_err)]
impl RedbStateMachine {
    /// Opens or creates a state machine at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageIOError<u64>> {
        let db = Database::create(path).map_err(sm_io_err)?;
        // Ensure tables exist, and that the file was written in the format this
        // build reads. On an error the transaction is dropped and nothing changes.
        let tx = db.begin_write().map_err(sm_io_err)?;
        let mut empty = true;
        for table in ENTITY_TABLES {
            let table = tx.open_table(table).map_err(sm_io_err)?;
            empty &= table.is_empty().map_err(sm_io_err)?;
        }
        let stored = {
            let meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
            let stored = meta
                .get(FORMAT_KEY)
                .map_err(sm_io_err)?
                .map(|v| v.value().to_vec());
            empty &= meta.is_empty().map_err(sm_io_err)?;
            stored
        };

        if stored.as_deref() == Some(&[V2_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V3_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V4_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V5_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V6_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V7_FORMAT_VERSION])
        {
            migrate_state(&tx)?;
        } else {
            let mut meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
            if check_format(stored.as_deref(), empty).map_err(sm_io_err)? {
                meta.insert(FORMAT_KEY, [FORMAT_VERSION].as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        tx.commit().map_err(sm_io_err)?;

        let mut sm = Self {
            db: Arc::new(Mutex::new(db)),
            last_applied: None,
            last_membership: StoredMembership::default(),
        };
        sm.load_meta()?;
        Ok(sm)
    }

    /// Loads last_applied and last_membership from disk.
    fn load_meta(&mut self) -> Result<(), StorageIOError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(sm_io_err)?;
        let meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;

        if let Some(v) = meta.get("last_applied").map_err(sm_io_err)? {
            let log_id: LogId<u64> = crate::codec::decode(v.value())
                .map_err(|e| sm_io_err_any(ErrorSubject::Store, ErrorVerb::Read, e))?;
            self.last_applied = Some(log_id);
        }
        if let Some(v) = meta.get("last_membership").map_err(sm_io_err)? {
            let membership: StoredMembership<u64, BasicNode> = crate::codec::decode(v.value())
                .map_err(|e| sm_io_err_any(ErrorSubject::Store, ErrorVerb::Read, e))?;
            self.last_membership = membership;
        }
        Ok(())
    }

    /// Persists last_applied and last_membership.
    fn save_meta(
        tx: &redb::WriteTransaction,
        last_applied: Option<LogId<u64>>,
        last_membership: &StoredMembership<u64, BasicNode>,
    ) -> Result<(), StorageError<u64>> {
        let mut meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
        match last_applied {
            Some(log_id) => {
                let bytes = crate::codec::encode(&log_id)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                meta.insert("last_applied", bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
            None => {
                meta.remove("last_applied").map_err(sm_io_err)?;
            }
        }
        let bytes = crate::codec::encode(last_membership)
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        meta.insert("last_membership", bytes.as_slice())
            .map_err(sm_io_err)?;
        Ok(())
    }

    /// Applies a single command to the state machine.
    fn apply_command(
        tx: &redb::WriteTransaction,
        cmd: &StateCommand,
    ) -> Result<StateResponse, StorageError<u64>> {
        match cmd {
            StateCommand::UpsertNode(node) => {
                let mut t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
                let stored_key = t
                    .get(node.id.to_string().as_str())
                    .map_err(sm_io_err)?
                    .map(|v| crate::codec::decode::<ClusterNode>(v.value()))
                    .transpose()
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?
                    .and_then(|stored| stored.nats_user_public_key);
                // Only SetNodeNatsUserPublicKey changes the key: a full record
                // read before the key was set must not erase the handle that
                // revokes the node's NATS credentials.
                let mut node = node.clone();
                if stored_key.is_some() {
                    node.nats_user_public_key = stored_key;
                }
                let bytes = crate::codec::encode(&node)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(node.id.to_string().as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveNode(id) => {
                let mut t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
                t.remove(id.to_string().as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RenewNodeLease { id, last_seen_unix } => {
                let mut t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
                let key = id.to_string();
                let existing: Option<ClusterNode> = t
                    .get(key.as_str())
                    .map_err(sm_io_err)?
                    .map(|v| crate::codec::decode::<ClusterNode>(v.value()))
                    .transpose()
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                let Some(mut record) = existing else {
                    return Ok(StateResponse::NotFound);
                };
                record.last_seen_unix = Some(*last_seen_unix);
                if record.status != NodeStatus::Draining {
                    record.status = NodeStatus::Active;
                }
                let bytes = crate::codec::encode(&record)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(key.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::SetNodeNatsUserPublicKey { id, public_key } => {
                let mut t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
                let key = id.to_string();
                let existing: Option<ClusterNode> = t
                    .get(key.as_str())
                    .map_err(sm_io_err)?
                    .map(|v| crate::codec::decode::<ClusterNode>(v.value()))
                    .transpose()
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                let Some(mut record) = existing else {
                    return Ok(StateResponse::NotFound);
                };
                record.nats_user_public_key = public_key.clone();
                let bytes = crate::codec::encode(&record)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(key.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertService(svc) => {
                // Two API writes can both pass admission before either
                // commits; this check, inside the write, decides exclusivity.
                let t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
                let mut stored = None;
                let mut others = Vec::new();
                for row in t.iter().map_err(sm_io_err)? {
                    let (key, value) = row.map_err(sm_io_err)?;
                    let other: Service = crate::codec::decode(value.value())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                    if key.value() == svc.id {
                        stored = Some(other);
                    } else {
                        others.push(other);
                    }
                }
                if stored
                    .as_ref()
                    .is_some_and(|stored| stored.tenant_id != svc.tenant_id)
                {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                // A write that keeps the stored ports (a rollout or rollback
                // step) passes, so state from before this rule never wedges.
                let ports_unchanged = stored.is_some_and(|stored| stored.ports == svc.ports);
                if !ports_unchanged {
                    if let Some(conflict) = published_port_conflict(svc, &others) {
                        return Ok(StateResponse::Error(conflict));
                    }
                }
                drop(t);
                let quotas = tenant_quota(tx, &svc.tenant_id)?;
                if let Some(error) = enforce_count_quota::<Service>(
                    tx,
                    SERVICES_TABLE,
                    &svc.tenant_id,
                    &svc.id,
                    "services",
                    quotas.services,
                    |record| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                if let Some(error) =
                    enforce_job_quota(tx, &svc.tenant_id, &svc.id, svc.is_job, quotas.jobs)?
                {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(svc)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &svc.tenant_id,
                    "services",
                    &svc.id,
                    bytes.len(),
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let mut t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
                t.insert(svc.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveService(id) => {
                let mut t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertTask(task) => {
                if owned_by_other_tenant(tx, TASKS_TABLE, &task.id, &task.tenant_id, |r: &Task| {
                    &r.tenant_id
                })? {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                let quotas = tenant_quota(tx, &task.tenant_id)?;
                if let Some(error) = enforce_count_quota::<Task>(
                    tx,
                    TASKS_TABLE,
                    &task.tenant_id,
                    &task.id,
                    "tasks",
                    quotas.tasks,
                    |record| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(task)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &task.tenant_id,
                    "tasks",
                    &task.id,
                    bytes.len(),
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let mut t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
                t.insert(task.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveTask(id) => {
                let mut t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertNetwork(net) => {
                if owned_by_other_tenant(
                    tx,
                    NETWORKS_TABLE,
                    &net.id,
                    &net.tenant_id,
                    |n: &Network| &n.tenant_id,
                )? {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                let quotas = tenant_quota(tx, &net.tenant_id)?;
                if let Some(error) = enforce_count_quota::<Network>(
                    tx,
                    NETWORKS_TABLE,
                    &net.tenant_id,
                    &net.id,
                    "networks",
                    quotas.networks,
                    |record| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(net)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &net.tenant_id,
                    "networks",
                    &net.id,
                    bytes.len(),
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let mut t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
                t.insert(net.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveNetwork(id) => {
                let mut t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertVolume(vol) => {
                if owned_by_other_tenant(
                    tx,
                    VOLUMES_TABLE,
                    &vol.id,
                    &vol.tenant_id,
                    |record: &Volume| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                let quotas = tenant_quota(tx, &vol.tenant_id)?;
                if let Some(error) = enforce_count_quota::<Volume>(
                    tx,
                    VOLUMES_TABLE,
                    &vol.tenant_id,
                    &vol.id,
                    "volumes",
                    quotas.volumes,
                    |record| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(vol)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &vol.tenant_id,
                    "volumes",
                    &vol.id,
                    bytes.len(),
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let mut t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
                t.insert(vol.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveVolume(id) => {
                let mut t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertSecret(s) => {
                if owned_by_other_tenant(tx, SECRETS_TABLE, &s.id, &s.tenant_id, |r: &Secret| {
                    &r.tenant_id
                })? {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                let quotas = tenant_quota(tx, &s.tenant_id)?;
                if let Some(error) = enforce_count_quota::<Secret>(
                    tx,
                    SECRETS_TABLE,
                    &s.tenant_id,
                    &s.id,
                    "secrets",
                    quotas.secrets,
                    |record| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(s)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) =
                    enforce_desired_state_bytes(tx, &s.tenant_id, "secrets", &s.id, bytes.len())?
                {
                    return Ok(StateResponse::Error(error));
                }
                let mut t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
                t.insert(s.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveSecret(id) => {
                let mut t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertConfig(c) => {
                if owned_by_other_tenant(tx, CONFIGS_TABLE, &c.id, &c.tenant_id, |r: &Config| {
                    &r.tenant_id
                })? {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                let quotas = tenant_quota(tx, &c.tenant_id)?;
                if let Some(error) = enforce_count_quota::<Config>(
                    tx,
                    CONFIGS_TABLE,
                    &c.tenant_id,
                    &c.id,
                    "configs",
                    quotas.configs,
                    |record| &record.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(c)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) =
                    enforce_desired_state_bytes(tx, &c.tenant_id, "configs", &c.id, bytes.len())?
                {
                    return Ok(StateResponse::Error(error));
                }
                let mut t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
                t.insert(c.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveConfig(id) => {
                let mut t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpsertTenant(tenant) => {
                let mut tenant = tenant.clone();
                let existing = {
                    let t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                    let value = t.get(tenant.id.as_str()).map_err(sm_io_err)?;
                    match value {
                        Some(value) => {
                            Some(crate::codec::decode::<Tenant>(value.value()).map_err(|e| {
                                sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e)
                            })?)
                        }
                        None => None,
                    }
                };
                if let Some(existing) = existing {
                    tenant.nats_revoked_user_public_keys = existing.nats_revoked_user_public_keys;
                }
                let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                let bytes = crate::codec::encode(&tenant)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(tenant.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::CreateTenant(tenant) => {
                let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                if t.get(tenant.id.as_str()).map_err(sm_io_err)?.is_some() {
                    return Ok(StateResponse::Conflict(format!(
                        "tenant {} already exists",
                        tenant.id
                    )));
                }
                let bytes = crate::codec::encode(tenant)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(tenant.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::UpdateTenant { id, name, quotas } => {
                let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                let existing: Option<Tenant> = t
                    .get(id.as_str())
                    .map_err(sm_io_err)?
                    .map(|value| crate::codec::decode(value.value()))
                    .transpose()
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                let Some(mut tenant) = existing else {
                    return Ok(StateResponse::NotFound);
                };
                tenant.name.clone_from(name);
                tenant.quotas.clone_from(quotas);
                let bytes = crate::codec::encode(&tenant)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(id.as_str(), bytes.as_slice()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RecordNatsUserRevocation { user_public_key } => {
                let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                let mut tenant = {
                    let Some(raw) = t.get(DEFAULT_TENANT_ID).map_err(sm_io_err)? else {
                        return Ok(StateResponse::NotFound);
                    };
                    crate::codec::decode::<Tenant>(raw.value())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?
                };
                if !tenant
                    .nats_revoked_user_public_keys
                    .contains(user_public_key)
                {
                    tenant
                        .nats_revoked_user_public_keys
                        .push(user_public_key.clone());
                    tenant.nats_revoked_user_public_keys.sort_unstable();
                    let bytes = crate::codec::encode(&tenant)
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                    t.insert(DEFAULT_TENANT_ID, bytes.as_slice())
                        .map_err(sm_io_err)?;
                }
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveTenant(id) => {
                if id == DEFAULT_TENANT_ID {
                    return Ok(StateResponse::Conflict(
                        "the default tenant cannot be removed".into(),
                    ));
                }
                let exists = {
                    let t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                    let exists = t.get(id.as_str()).map_err(sm_io_err)?.is_some();
                    exists
                };
                if !exists {
                    return Ok(StateResponse::NotFound);
                }
                if tenant_has_resources(tx, id)? {
                    return Ok(StateResponse::Conflict(format!(
                        "tenant {id} still owns resources"
                    )));
                }
                let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
        }
    }

    // --- read helpers (used by StateStore for local reads) --------------------
    //
    // These return `Result<T, String>` instead of `StorageError` to keep the
    // Result small (clippy::result_large_err). StateStore wraps them in
    // StateError::storage().

    pub async fn get_node(&self, id: NodeId) -> Result<Option<ClusterNode>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(NODES_TABLE).map_err(|e| e.to_string())?;
        match t.get(id.to_string().as_str()).map_err(|e| e.to_string())? {
            Some(v) => {
                let node: ClusterNode =
                    crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    pub async fn list_nodes(&self) -> Result<Vec<ClusterNode>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(NODES_TABLE).map_err(|e| e.to_string())?;
        let mut nodes = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let node: ClusterNode = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    pub async fn get_service(&self, id: &str) -> Result<Option<Service>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(SERVICES_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(v) => {
                let svc: Service = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(svc))
            }
            None => Ok(None),
        }
    }

    pub async fn list_services(&self) -> Result<Vec<Service>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(SERVICES_TABLE).map_err(|e| e.to_string())?;
        let mut services = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let svc: Service = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            services.push(svc);
        }
        Ok(services)
    }

    pub async fn get_task(&self, id: &str) -> Result<Option<Task>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(TASKS_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(v) => {
                let task: Task = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(task))
            }
            None => Ok(None),
        }
    }

    pub async fn list_tasks(&self) -> Result<Vec<Task>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(TASKS_TABLE).map_err(|e| e.to_string())?;
        let mut tasks = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let task: Task = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            tasks.push(task);
        }
        Ok(tasks)
    }

    pub async fn get_network(&self, id: &str) -> Result<Option<Network>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(NETWORKS_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(v) => {
                let net: Network = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(net))
            }
            None => Ok(None),
        }
    }

    pub async fn list_networks(&self) -> Result<Vec<Network>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(NETWORKS_TABLE).map_err(|e| e.to_string())?;
        let mut networks = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let net: Network = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            networks.push(net);
        }
        Ok(networks)
    }

    pub async fn get_volume(&self, id: &str) -> Result<Option<Volume>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(VOLUMES_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(v) => {
                let vol: Volume = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(vol))
            }
            None => Ok(None),
        }
    }

    pub async fn list_volumes(&self) -> Result<Vec<Volume>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(VOLUMES_TABLE).map_err(|e| e.to_string())?;
        let mut volumes = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let vol: Volume = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            volumes.push(vol);
        }
        Ok(volumes)
    }

    pub async fn get_secret(&self, id: &str) -> Result<Option<Secret>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(SECRETS_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(v) => {
                let s: Secret = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    pub async fn list_secrets(&self) -> Result<Vec<Secret>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(SECRETS_TABLE).map_err(|e| e.to_string())?;
        let mut secrets = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let s: Secret = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            secrets.push(s);
        }
        Ok(secrets)
    }

    pub async fn get_config(&self, id: &str) -> Result<Option<Config>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(CONFIGS_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(v) => {
                let c: Config = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
                Ok(Some(c))
            }
            None => Ok(None),
        }
    }

    pub async fn list_configs(&self) -> Result<Vec<Config>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(CONFIGS_TABLE).map_err(|e| e.to_string())?;
        let mut configs = Vec::new();
        for r in t.iter().map_err(|e| e.to_string())? {
            let (_k, v) = r.map_err(|e| e.to_string())?;
            let c: Config = crate::codec::decode(v.value()).map_err(|e| e.to_string())?;
            configs.push(c);
        }
        Ok(configs)
    }
    pub async fn get_tenant(&self, id: &str) -> Result<Option<Tenant>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(TENANTS_TABLE).map_err(|e| e.to_string())?;
        match t.get(id).map_err(|e| e.to_string())? {
            Some(value) => {
                let tenant: Tenant =
                    crate::codec::decode(value.value()).map_err(|e| e.to_string())?;
                Ok(Some(tenant))
            }
            None => Ok(None),
        }
    }

    pub async fn list_tenants(&self) -> Result<Vec<Tenant>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let t = tx.open_table(TENANTS_TABLE).map_err(|e| e.to_string())?;
        let mut tenants = Vec::new();
        for row in t.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            let tenant: Tenant = crate::codec::decode(value.value()).map_err(|e| e.to_string())?;
            tenants.push(tenant);
        }
        Ok(tenants)
    }
}

impl Debug for RedbStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbStateMachine")
            .field("last_applied", &self.last_applied)
            .finish()
    }
}

impl RaftStateMachine<TypeConfig> for RedbStateMachine {
    type SnapshotBuilder = RedbSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<StateResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(sm_io_err)?;
        let mut responses = Vec::new();

        for entry in entries {
            // Update membership if this is a membership entry.
            if let Some(membership) = entry.payload.get_membership() {
                self.last_membership =
                    StoredMembership::new(Some(entry.log_id), membership.clone());
            }

            let response = match &entry.payload {
                EntryPayload::Blank => StateResponse::Ok,
                EntryPayload::Normal(cmd) => Self::apply_command(&tx, cmd)?,
                EntryPayload::Membership(_) => StateResponse::Ok,
            };

            self.last_applied = Some(entry.log_id);
            responses.push(response);
        }

        // Persist metadata.
        Self::save_meta(&tx, self.last_applied, &self.last_membership)?;

        tx.commit().map_err(sm_io_err)?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RedbSnapshotBuilder {
            db: Arc::clone(&self.db),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let snap: StateSnapshot = crate::codec::decode(&bytes)
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;

        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(sm_io_err)?;

        // Clear all entity tables.
        for table_def in ENTITY_TABLES {
            let mut t = tx.open_table(table_def).map_err(sm_io_err)?;
            let keys: Vec<String> = t
                .iter()
                .map_err(sm_io_err)?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in keys {
                t.remove(k.as_str()).map_err(sm_io_err)?;
            }
        }

        // Insert snapshot data.
        {
            let mut t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
            for node in &snap.nodes {
                let bytes = crate::codec::encode(node)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(node.id.to_string().as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
            for svc in &snap.services {
                let bytes = crate::codec::encode(svc)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(svc.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
            for task in &snap.tasks {
                let bytes = crate::codec::encode(task)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(task.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
            for net in &snap.networks {
                let bytes = crate::codec::encode(net)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(net.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
            for vol in &snap.volumes {
                let bytes = crate::codec::encode(vol)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(vol.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
            for s in &snap.secrets {
                let bytes = crate::codec::encode(s)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(s.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
            for c in &snap.configs {
                let bytes = crate::codec::encode(c)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(c.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
            for tenant in &snap.tenants {
                let bytes = crate::codec::encode(tenant)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(tenant.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }

        // Save snapshot metadata.
        Self::save_meta(&tx, snap.last_applied, &snap.last_membership)?;

        tx.commit().map_err(sm_io_err)?;

        self.last_applied = snap.last_applied;
        self.last_membership = snap.last_membership;
        let _ = meta; // meta is used implicitly via snap.last_applied / snap.last_membership
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        // The state machine itself is persistent (redb). Snapshots are built
        // on demand by the snapshot builder and not stored separately.
        Ok(None)
    }
}

// --- snapshot builder ---------------------------------------------------------

/// Builds a snapshot by serializing the entire state machine.
pub struct RedbSnapshotBuilder {
    db: Arc<Mutex<Database>>,
}

impl RaftSnapshotBuilder<TypeConfig> for RedbSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(sm_io_err)?;

        let mut snap = StateSnapshot {
            nodes: Vec::new(),
            services: Vec::new(),
            tasks: Vec::new(),
            networks: Vec::new(),
            volumes: Vec::new(),
            secrets: Vec::new(),
            configs: Vec::new(),
            tenants: Vec::new(),
            last_applied: None,
            last_membership: StoredMembership::default(),
        };

        // Read all entities.
        {
            let t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let node: ClusterNode = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.nodes.push(node);
            }
        }
        {
            let t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let svc: Service = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.services.push(svc);
            }
        }
        {
            let t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let task: Task = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.tasks.push(task);
            }
        }
        {
            let t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let net: Network = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.networks.push(net);
            }
        }
        {
            let t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let vol: Volume = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.volumes.push(vol);
            }
        }
        {
            let t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let s: Secret = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.secrets.push(s);
            }
        }
        {
            let t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let c: Config = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.configs.push(c);
            }
        }
        {
            let t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
            for row in t.iter().map_err(sm_io_err)? {
                let (_, value) = row.map_err(sm_io_err)?;
                let tenant: Tenant = crate::codec::decode(value.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.tenants.push(tenant);
            }
        }
        // Read metadata.
        {
            let meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
            if let Some(v) = meta.get("last_applied").map_err(sm_io_err)? {
                let log_id: LogId<u64> = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.last_applied = Some(log_id);
            }
            if let Some(v) = meta.get("last_membership").map_err(sm_io_err)? {
                let m: StoredMembership<u64, BasicNode> = crate::codec::decode(v.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.last_membership = m;
            }
        }

        let bytes = crate::codec::encode(&snap)
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;

        let last_index = snap.last_applied.map(|l| l.index).unwrap_or(0);
        let snapshot_meta = SnapshotMeta {
            last_log_id: snap.last_applied,
            last_membership: snap.last_membership.clone(),
            snapshot_id: format!("snapshot-{last_index}"),
        };

        Ok(Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

// --- error helpers ------------------------------------------------------------

fn sm_io_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageIOError<u64> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e))
}

fn sm_io_err_any<E: std::error::Error + Send + Sync + 'static>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: E,
) -> StorageIOError<u64> {
    StorageIOError::new(subject, verb, AnyError::new(&e))
}

fn sm_storage_err<E: std::error::Error + Send + Sync + 'static>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: E,
) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(&e)),
    }
}
