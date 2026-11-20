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
    check_format, FORMAT_KEY, FORMAT_VERSION, V10_FORMAT_VERSION, V2_FORMAT_VERSION,
    V3_FORMAT_VERSION, V4_FORMAT_VERSION, V5_FORMAT_VERSION, V6_FORMAT_VERSION, V7_FORMAT_VERSION,
    V8_FORMAT_VERSION, V9_FORMAT_VERSION,
};
use crate::command::{ObjectKind, StateCommand, StateResponse};
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
const CHANGE_REQUESTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("change_requests");
const TENANTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("tenants");
const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sm_meta");

/// All entity tables, used for clearing on snapshot install.
const ENTITY_TABLES: [TableDefinition<&str, &[u8]>; 9] = [
    NODES_TABLE,
    SERVICES_TABLE,
    TASKS_TABLE,
    NETWORKS_TABLE,
    VOLUMES_TABLE,
    SECRETS_TABLE,
    CONFIGS_TABLE,
    TENANTS_TABLE,
    CHANGE_REQUESTS_TABLE,
];

/// Re-encodes entity records and advances the marker during format migration.
///
/// The caller advances the marker in the same redb transaction, so a failure
/// leaves both the old values and the old marker intact.
#[allow(clippy::result_large_err)]
fn migrate_state(tx: &redb::WriteTransaction) -> Result<(), StorageIOError<u64>> {
    // Serde defaults fields introduced in later formats; re-encoding applies
    // prior migrations. The v11 table starts empty on upgraded state.
    rewrite_state_table::<ClusterNode>(tx, NODES_TABLE)?;
    rewrite_state_table::<Tenant>(tx, TENANTS_TABLE)?;
    rewrite_state_table::<Service>(tx, SERVICES_TABLE)?;
    rewrite_state_table::<Task>(tx, TASKS_TABLE)?;
    rewrite_state_table::<Network>(tx, NETWORKS_TABLE)?;
    rewrite_state_table::<Volume>(tx, VOLUMES_TABLE)?;
    rewrite_state_table::<Secret>(tx, SECRETS_TABLE)?;
    // The v11 table starts empty on upgraded state.
    let _ = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
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

/// Whether any service, or a rollback revision it keeps, names network `id`.
#[allow(clippy::result_large_err)]
fn network_in_use(tx: &redb::WriteTransaction, id: &str) -> Result<bool, StorageError<u64>> {
    let table = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
    for row in table.iter().map_err(sm_io_err)? {
        let (_, value) = row.map_err(sm_io_err)?;
        let service: Service = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        let mut revision = Some(&service);
        while let Some(spec) = revision {
            if spec.networks.iter().any(|network| network == id) {
                return Ok(true);
            }
            revision = spec.previous.as_deref();
        }
    }
    Ok(false)
}

/// Whether every network in `ids` exists and belongs to `tenant_id`.
#[allow(clippy::result_large_err)]
fn networks_owned_by(
    tx: &redb::WriteTransaction,
    ids: &[NetworkId],
    tenant_id: &str,
) -> Result<bool, StorageError<u64>> {
    let table = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
    for id in ids {
        let Some(value) = table.get(id.as_str()).map_err(sm_io_err)? else {
            return Ok(false);
        };
        let network: Network = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        if network.tenant_id != tenant_id {
            return Ok(false);
        }
    }
    Ok(true)
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

/// The tenant field every tenant-owned record carries; JSON ignores the rest.
#[derive(serde::Deserialize)]
struct Owner {
    tenant_id: String,
}

/// The tenant's quotas, or `None` when the tenant does not exist: a write
/// under a removed tenant would otherwise land with no limits and no owner.
/// `default` is permanent and may not be seeded yet while a cluster bootstraps.
#[allow(clippy::result_large_err)]
fn tenant_quota(
    tx: &redb::WriteTransaction,
    tenant_id: &str,
) -> Result<Option<TenantQuotas>, StorageError<u64>> {
    let table = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
    let quotas = table
        .get(tenant_id)
        .map_err(sm_io_err)?
        .map(|value| crate::codec::decode(value.value()))
        .transpose()
        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?
        .map(|tenant: Tenant| tenant.quotas);
    Ok(quotas.or_else(|| (tenant_id == DEFAULT_TENANT_ID).then(TenantQuotas::default)))
}

#[allow(clippy::result_large_err)]
/// Rejects a new object past the tenant's count limit. Rewriting an object
/// that already exists never grows the count, so it always passes: a lowered
/// quota must not block status and rollout writes.
fn enforce_count_quota<T: DeserializeOwned>(
    tx: &redb::WriteTransaction,
    table_definition: TableDefinition<&str, &[u8]>,
    id: &str,
    quota_name: &str,
    limit: Option<u64>,
    counted: impl Fn(&T) -> bool,
) -> Result<Option<String>, StorageError<u64>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let table = tx.open_table(table_definition).map_err(sm_io_err)?;
    if table.get(id).map_err(sm_io_err)?.is_some() {
        return Ok(None);
    }
    let mut count = 0u64;
    for row in table.iter().map_err(sm_io_err)? {
        let (_, value) = row.map_err(sm_io_err)?;
        let record: T = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        if counted(&record) {
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
    limit: Option<u64>,
    replaced_table: &str,
    replaced_id: &str,
    candidate_len: usize,
    allowance: u64,
) -> Result<Option<String>, StorageError<u64>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let mut total = 0u64;
    let mut replaced_len = None;
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
            if table_name == replaced_table && key.value() == replaced_id {
                replaced_len = Some(bytes.len() as u64);
            } else if belongs {
                total = total.saturating_add(bytes.len() as u64);
            }
        }
    }
    // A rewrite that does not grow the record passes even over the limit.
    if replaced_len.is_some_and(|old| candidate_len as u64 <= old) {
        return Ok(None);
    }
    // `allowance` discounts the whole total, not just this record: it covers
    // growth of every record an older-format import has written so far.
    total = total
        .saturating_add(candidate_len as u64)
        .saturating_sub(allowance);
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

/// Most change requests one tenant may keep, decided or not. Past it the
/// oldest closed ones are dropped, so an agent cannot grow Raft state forever.
const MAX_CHANGE_REQUESTS_PER_TENANT: usize = 256;
/// Most open (pending, unexpired) change requests one tenant may hold.
const MAX_OPEN_CHANGE_REQUESTS_PER_TENANT: usize = 32;

/// Change requests are keyed `{tenant}/{id}`, so one tenant's rows are one
/// key range and a lookup can never land on another tenant's request.
fn change_request_key(tenant_id: &str, id: &str) -> String {
    format!("{tenant_id}/{id}")
}

/// The key range holding every change request of `tenant_id`: `/` sorts
/// directly before `0`.
fn change_request_range(tenant_id: &str) -> (String, String) {
    (format!("{tenant_id}/"), format!("{tenant_id}0"))
}

/// The tenant's change requests as `(key, open at now_unix, created_at_unix)`.
#[allow(clippy::result_large_err)]
fn tenant_change_requests(
    tx: &redb::WriteTransaction,
    tenant_id: &str,
    now_unix: u64,
) -> Result<Vec<(String, bool, u64)>, StorageError<u64>> {
    let table = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
    let (start, end) = change_request_range(tenant_id);
    let mut rows = Vec::new();
    for row in table
        .range::<&str>(start.as_str()..end.as_str())
        .map_err(sm_io_err)?
    {
        let (key, value) = row.map_err(sm_io_err)?;
        let request: ChangeRequest = crate::codec::decode(value.value())
            .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        if request.tenant_id == tenant_id {
            let open = request.status == ChangeRequestStatus::Pending
                && now_unix < request.expires_at_unix;
            rows.push((key.value().to_owned(), open, request.created_at_unix));
        }
    }
    Ok(rows)
}

fn valid_change_request_after(request: &ChangeRequest) -> bool {
    match request.kind {
        ChangeKind::Deploy => {
            let mut expected = request.before.clone();
            let mut previous = request.before.clone();
            previous.previous = None;
            expected.previous = Some(Box::new(previous));
            expected.image = request.after.image.clone();
            request.before.agent.is_none()
                && !request.after.image.trim().is_empty()
                && request.after.image.len() <= 512
                && !request.after.image.chars().any(char::is_control)
                && expected == request.after
        }
        ChangeKind::Scale => {
            let mut expected = request.before.clone();
            expected.replicas = request.after.replicas;
            (1..=1000).contains(&request.after.replicas) && expected == request.after
        }
        ChangeKind::Rollback => {
            let Some(mut expected) = request.before.previous.as_deref().cloned() else {
                return false;
            };
            let mut previous = request.before.clone();
            previous.previous = None;
            expected.previous = Some(Box::new(previous));
            expected == request.after
        }
        ChangeKind::Stop => {
            let mut expected = request.before.clone();
            expected.replicas = 0;
            expected == request.after
        }
    }
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
    #[serde(default)]
    change_requests: Vec<ChangeRequest>,
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
            || stored.as_deref() == Some(&[V8_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V9_FORMAT_VERSION])
            || stored.as_deref() == Some(&[V10_FORMAT_VERSION])
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
                if tenant_quota(tx, &node.tenant_id)?.is_none() {
                    return Ok(StateResponse::NotFound);
                }
                let mut t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
                let stored = t
                    .get(node.id.to_string().as_str())
                    .map_err(sm_io_err)?
                    .map(|v| crate::codec::decode::<ClusterNode>(v.value()))
                    .transpose()
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                // Two creates of one node ID can both pass the API check; the
                // node keeps its first tenant instead of moving without a drain.
                if stored
                    .as_ref()
                    .is_some_and(|stored| stored.tenant_id != node.tenant_id)
                {
                    return Ok(StateResponse::Conflict(NAME_IN_USE.into()));
                }
                let stored_key = stored.and_then(|stored| stored.nats_user_public_key);
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
            StateCommand::UpsertService(svc) | StateCommand::ImportService { service: svc, .. } => {
                let byte_allowance = match cmd {
                    StateCommand::ImportService { byte_allowance, .. } => *byte_allowance,
                    _ => 0,
                };
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
                let Some(quotas) = tenant_quota(tx, &svc.tenant_id)? else {
                    return Ok(StateResponse::NotFound);
                };
                if let Some(error) = enforce_count_quota::<Service>(
                    tx,
                    SERVICES_TABLE,
                    &svc.id,
                    "services",
                    quotas.services,
                    |record| record.tenant_id == svc.tenant_id,
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
                    quotas.desired_state_bytes,
                    "services",
                    &svc.id,
                    bytes.len(),
                    byte_allowance,
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
                let Some(quotas) = tenant_quota(tx, &task.tenant_id)? else {
                    return Ok(StateResponse::NotFound);
                };
                if let Some(error) = enforce_count_quota::<Task>(
                    tx,
                    TASKS_TABLE,
                    &task.id,
                    "tasks",
                    quotas.tasks,
                    |record| record.tenant_id == task.tenant_id && !record.state.is_terminal(),
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(task)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &task.tenant_id,
                    quotas.desired_state_bytes,
                    "tasks",
                    &task.id,
                    bytes.len(),
                    0,
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
                let Some(quotas) = tenant_quota(tx, &net.tenant_id)? else {
                    return Ok(StateResponse::NotFound);
                };
                if let Some(error) = enforce_count_quota::<Network>(
                    tx,
                    NETWORKS_TABLE,
                    &net.id,
                    "networks",
                    quotas.networks,
                    |record| record.tenant_id == net.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(net)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &net.tenant_id,
                    quotas.desired_state_bytes,
                    "networks",
                    &net.id,
                    bytes.len(),
                    0,
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
                let Some(quotas) = tenant_quota(tx, &vol.tenant_id)? else {
                    return Ok(StateResponse::NotFound);
                };
                if let Some(error) = enforce_count_quota::<Volume>(
                    tx,
                    VOLUMES_TABLE,
                    &vol.id,
                    "volumes",
                    quotas.volumes,
                    |record| record.tenant_id == vol.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(vol)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &vol.tenant_id,
                    quotas.desired_state_bytes,
                    "volumes",
                    &vol.id,
                    bytes.len(),
                    0,
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
                let Some(quotas) = tenant_quota(tx, &s.tenant_id)? else {
                    return Ok(StateResponse::NotFound);
                };
                if let Some(error) = enforce_count_quota::<Secret>(
                    tx,
                    SECRETS_TABLE,
                    &s.id,
                    "secrets",
                    quotas.secrets,
                    |record| record.tenant_id == s.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(s)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &s.tenant_id,
                    quotas.desired_state_bytes,
                    "secrets",
                    &s.id,
                    bytes.len(),
                    0,
                )? {
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
                let Some(quotas) = tenant_quota(tx, &c.tenant_id)? else {
                    return Ok(StateResponse::NotFound);
                };
                if let Some(error) = enforce_count_quota::<Config>(
                    tx,
                    CONFIGS_TABLE,
                    &c.id,
                    "configs",
                    quotas.configs,
                    |record| record.tenant_id == c.tenant_id,
                )? {
                    return Ok(StateResponse::Error(error));
                }
                let bytes = crate::codec::encode(c)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                if let Some(error) = enforce_desired_state_bytes(
                    tx,
                    &c.tenant_id,
                    quotas.desired_state_bytes,
                    "configs",
                    &c.id,
                    bytes.len(),
                    0,
                )? {
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
            StateCommand::RemoveOwned {
                kind,
                id,
                tenant_id,
            } => {
                let definition = match kind {
                    ObjectKind::Service => SERVICES_TABLE,
                    ObjectKind::Task => TASKS_TABLE,
                    ObjectKind::Network => NETWORKS_TABLE,
                    ObjectKind::Volume => VOLUMES_TABLE,
                    ObjectKind::Secret => SECRETS_TABLE,
                    ObjectKind::Config => CONFIGS_TABLE,
                };
                // Like Docker, a network in use stays: otherwise a service (or
                // its rollback revision) keeps an ID another tenant can take.
                if *kind == ObjectKind::Network && network_in_use(tx, id)? {
                    return Ok(StateResponse::Conflict(format!(
                        "network {id} is in use by a service"
                    )));
                }
                let mut t = tx.open_table(definition).map_err(sm_io_err)?;
                let owner = t
                    .get(id.as_str())
                    .map_err(sm_io_err)?
                    .map(|raw| crate::codec::decode::<Owner>(raw.value()))
                    .transpose()
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                match owner {
                    Some(owner) if owner.tenant_id == *tenant_id => {
                        t.remove(id.as_str()).map_err(sm_io_err)?;
                        Ok(StateResponse::Ok)
                    }
                    _ => Ok(StateResponse::NotFound),
                }
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
                // Change requests are history, not resources: with the tenant's
                // services gone none can apply, so they go with the tenant.
                let requests = tenant_change_requests(tx, id, 0)?;
                let mut table = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
                for (request_id, _, _) in &requests {
                    table.remove(request_id.as_str()).map_err(sm_io_err)?;
                }
                drop(table);
                let mut t = tx.open_table(TENANTS_TABLE).map_err(sm_io_err)?;
                t.remove(id.as_str()).map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::CreateChangeRequest(request) => {
                if request.id.is_empty()
                    || request.id.len() > 128
                    || request.id.contains('/')
                    || request.expires_at_unix <= request.created_at_unix
                    || request.before.id != request.service_id
                    || request.after.id != request.service_id
                    || request.before.tenant_id != request.tenant_id
                    || request.after.tenant_id != request.tenant_id
                    || !valid_change_request_after(request)
                {
                    return Ok(StateResponse::Error("invalid change request".into()));
                }
                if tenant_quota(tx, &request.tenant_id)?.is_none() {
                    return Ok(StateResponse::NotFound);
                }
                let current = {
                    let services = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
                    let current = services
                        .get(request.service_id.as_str())
                        .map_err(sm_io_err)?
                        .map(|value| crate::codec::decode::<Service>(value.value()))
                        .transpose()
                        .map_err(|error| {
                            sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, error)
                        })?;
                    current
                };
                if current.as_ref() != Some(&request.before) {
                    return Ok(StateResponse::Conflict(
                        "service changed while the request was being created".into(),
                    ));
                }
                let mut own =
                    tenant_change_requests(tx, &request.tenant_id, request.created_at_unix)?;
                let open = own.iter().filter(|(_, open, _)| *open).count();
                if open >= MAX_OPEN_CHANGE_REQUESTS_PER_TENANT {
                    return Ok(StateResponse::Conflict(format!(
                        "tenant has {open} open change requests; decide or let some expire first"
                    )));
                }
                own.retain(|(_, open, _)| !*open);
                own.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| left.0.cmp(&right.0)));
                let excess = (own.len() + open + 1).saturating_sub(MAX_CHANGE_REQUESTS_PER_TENANT);
                let mut request = request.clone();
                request.status = ChangeRequestStatus::Pending;
                request.reviewer = None;
                request.decided_at_unix = None;
                request.audit = vec![ChangeRequestAudit {
                    action: "created".into(),
                    actor: request.requester.clone(),
                    occurred_at_unix: request.created_at_unix,
                }];
                let bytes = crate::codec::encode(&request).map_err(|error| {
                    sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, error)
                })?;
                let key = change_request_key(&request.tenant_id, &request.id);
                let mut requests = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
                if requests.get(key.as_str()).map_err(sm_io_err)?.is_some() {
                    return Ok(StateResponse::Conflict(
                        "change request ID already exists".into(),
                    ));
                }
                for (closed_key, _, _) in own.iter().take(excess) {
                    requests.remove(closed_key.as_str()).map_err(sm_io_err)?;
                }
                requests
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::ChangeRequest(request))
            }
            StateCommand::DecideChangeRequest {
                id,
                tenant_id,
                actor,
                approve,
                now_unix,
            } => {
                let key = change_request_key(tenant_id, id);
                let Some(mut request) = ({
                    let requests = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
                    let request = requests
                        .get(key.as_str())
                        .map_err(sm_io_err)?
                        .map(|value| crate::codec::decode::<ChangeRequest>(value.value()))
                        .transpose()
                        .map_err(|error| {
                            sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, error)
                        })?;
                    request
                }) else {
                    return Ok(StateResponse::NotFound);
                };
                if request.tenant_id != *tenant_id {
                    return Ok(StateResponse::NotFound);
                }
                if request.status != ChangeRequestStatus::Pending {
                    return Ok(StateResponse::Conflict(
                        "change request is no longer pending".into(),
                    ));
                }
                if request.requester == *actor {
                    return Ok(StateResponse::Conflict(
                        "the requester cannot decide their own change request".into(),
                    ));
                }
                let action = if *now_unix >= request.expires_at_unix {
                    request.status = ChangeRequestStatus::Expired;
                    "expired"
                } else if !approve {
                    request.status = ChangeRequestStatus::Rejected;
                    "rejected"
                } else {
                    let current = {
                        let services = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
                        let current = services
                            .get(request.service_id.as_str())
                            .map_err(sm_io_err)?
                            .map(|value| crate::codec::decode::<Service>(value.value()))
                            .transpose()
                            .map_err(|error| {
                                sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, error)
                            })?;
                        current
                    };
                    if current.as_ref() != Some(&request.before) {
                        request.status = ChangeRequestStatus::Superseded;
                        request.reviewer = Some(actor.clone());
                        request.decided_at_unix = Some(*now_unix);
                        request.audit.push(ChangeRequestAudit {
                            action: "superseded".into(),
                            actor: actor.clone(),
                            occurred_at_unix: *now_unix,
                        });
                        let bytes = crate::codec::encode(&request).map_err(|error| {
                            sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, error)
                        })?;
                        let mut requests =
                            tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
                        requests
                            .insert(key.as_str(), bytes.as_slice())
                            .map_err(sm_io_err)?;
                        return Ok(StateResponse::ChangeRequest(Box::new(request)));
                    }
                    // A rollback can restore a network ID the tenant has since
                    // removed and another tenant has taken: every network the
                    // reviewed spec adds must still be the tenant's.
                    let added: Vec<NetworkId> = request
                        .after
                        .networks
                        .iter()
                        .filter(|network| !request.before.networks.contains(network))
                        .cloned()
                        .collect();
                    if !networks_owned_by(tx, &added, &request.tenant_id)? {
                        return Ok(StateResponse::Conflict(
                            "the change refers to a network the tenant does not own".into(),
                        ));
                    }
                    match Self::apply_command(
                        tx,
                        &StateCommand::UpsertService(request.after.clone()),
                    )? {
                        StateResponse::Ok => {}
                        response => return Ok(response),
                    }
                    request.status = ChangeRequestStatus::Approved;
                    "approved_and_applied"
                };
                request.reviewer = Some(actor.clone());
                request.decided_at_unix = Some(*now_unix);
                request.audit.push(ChangeRequestAudit {
                    action: action.into(),
                    actor: actor.clone(),
                    occurred_at_unix: *now_unix,
                });
                let bytes = crate::codec::encode(&request).map_err(|error| {
                    sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, error)
                })?;
                let mut requests = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
                requests
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                // The caller reads the outcome (applied, rejected, expired or
                // superseded) from the committed record's status.
                Ok(StateResponse::ChangeRequest(Box::new(request)))
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

    pub async fn get_change_request(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<ChangeRequest>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let table = tx
            .open_table(CHANGE_REQUESTS_TABLE)
            .map_err(|e| e.to_string())?;
        table
            .get(change_request_key(tenant_id, id).as_str())
            .map_err(|e| e.to_string())?
            .map(|value| crate::codec::decode(value.value()).map_err(|e| e.to_string()))
            .transpose()
    }

    /// One tenant's change requests, read from its key range only.
    pub async fn list_change_requests(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<ChangeRequest>, String> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(|e| e.to_string())?;
        let table = tx
            .open_table(CHANGE_REQUESTS_TABLE)
            .map_err(|e| e.to_string())?;
        let (start, end) = change_request_range(tenant_id);
        let mut requests = Vec::new();
        for row in table
            .range::<&str>(start.as_str()..end.as_str())
            .map_err(|e| e.to_string())?
        {
            let (_, value) = row.map_err(|e| e.to_string())?;
            requests.push(crate::codec::decode(value.value()).map_err(|e| e.to_string())?);
        }
        Ok(requests)
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
        {
            let mut t = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
            for request in &snap.change_requests {
                let bytes = crate::codec::encode(request)
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                let key = change_request_key(&request.tenant_id, &request.id);
                t.insert(key.as_str(), bytes.as_slice())
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
            change_requests: Vec::new(),
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
        {
            let t = tx.open_table(CHANGE_REQUESTS_TABLE).map_err(sm_io_err)?;
            for row in t.iter().map_err(sm_io_err)? {
                let (_, value) = row.map_err(sm_io_err)?;
                let request: ChangeRequest = crate::codec::decode(value.value())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.change_requests.push(request);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(sm: &RedbStateMachine, command: StateCommand) -> StateResponse {
        let db = sm.db.lock().unwrap();
        let tx = db.begin_write().unwrap();
        let response = RedbStateMachine::apply_command(&tx, &command).unwrap();
        tx.commit().unwrap();
        response
    }

    fn snapshot_of(sm: &mut RedbStateMachine) -> Snapshot<TypeConfig> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            sm.get_snapshot_builder()
                .await
                .build_snapshot()
                .await
                .unwrap()
        })
    }

    #[test]
    fn a_snapshot_keeps_change_requests_tenant_keyed() {
        let dir = tempfile::tempdir().unwrap();
        let mut leader = RedbStateMachine::open(dir.path().join("leader.redb")).unwrap();
        let tenant: Tenant =
            serde_json::from_str(include_str!("../tests/fixtures/v11/tenant.json")).unwrap();
        let mut request: ChangeRequest =
            serde_json::from_str(include_str!("../tests/fixtures/v11/change_request.json"))
                .unwrap();
        request.tenant_id = tenant.id.clone();
        request.before.tenant_id = tenant.id.clone();
        request.after.tenant_id = tenant.id.clone();
        assert_eq!(
            apply(&leader, StateCommand::CreateTenant(tenant.clone())),
            StateResponse::Ok
        );
        assert_eq!(
            apply(&leader, StateCommand::UpsertService(request.before.clone())),
            StateResponse::Ok
        );
        assert!(matches!(
            apply(
                &leader,
                StateCommand::CreateChangeRequest(Box::new(request.clone()))
            ),
            StateResponse::ChangeRequest(_)
        ));
        apply(
            &leader,
            StateCommand::RemoveService(request.service_id.clone()),
        );

        // A follower that installs the leader's snapshot finds the request
        // under its tenant, exactly as the leader does.
        let snapshot = snapshot_of(&mut leader);
        let mut follower = RedbStateMachine::open(dir.path().join("follower.redb")).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            follower
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
                .unwrap();
            assert!(follower
                .get_change_request(&tenant.id, &request.id)
                .await
                .unwrap()
                .is_some());
            assert_eq!(
                follower
                    .list_change_requests(&tenant.id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        });

        // Removing the tenant there drops the request, so a recreated tenant
        // and a later snapshot carry none of it.
        assert_eq!(
            apply(&follower, StateCommand::RemoveTenant(tenant.id.clone())),
            StateResponse::Ok
        );
        assert_eq!(
            apply(&follower, StateCommand::CreateTenant(tenant.clone())),
            StateResponse::Ok
        );
        runtime.block_on(async {
            assert!(follower
                .list_change_requests(&tenant.id)
                .await
                .unwrap()
                .is_empty());
        });
        let snapshot = snapshot_of(&mut follower);
        let snap: StateSnapshot = crate::codec::decode(snapshot.snapshot.get_ref()).unwrap();
        assert!(snap.change_requests.is_empty());
    }
}
