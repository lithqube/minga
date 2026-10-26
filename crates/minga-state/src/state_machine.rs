//! redb-backed Raft state machine.
//!
//! Stores the cluster's desired state (nodes, services, tasks, networks,
//! volumes, secrets, configs) in a redb database. Commands from the Raft log
//! are applied here once they are committed.

use std::fmt::Debug;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use openraft::entry::{EntryPayload, RaftPayload};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    AnyError, BasicNode, Entry, ErrorSubject, ErrorVerb, LogId, OptionalSend, StorageError,
    StorageIOError, StoredMembership,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::command::{StateCommand, StateResponse};
use crate::types::*;
use crate::TypeConfig;

// --- redb table definitions --------------------------------------------------

const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const SERVICES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("services");
const TASKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("tasks");
const NETWORKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("networks");
const VOLUMES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");
const SECRETS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("secrets");
const CONFIGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("configs");
const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sm_meta");

/// All entity tables, used for clearing on snapshot install.
const ENTITY_TABLES: [TableDefinition<&str, &[u8]>; 7] = [
    NODES_TABLE,
    SERVICES_TABLE,
    TASKS_TABLE,
    NETWORKS_TABLE,
    VOLUMES_TABLE,
    SECRETS_TABLE,
    CONFIGS_TABLE,
];

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
        let tx = db.begin_write().map_err(sm_io_err)?;
        for table in ENTITY_TABLES {
            tx.open_table(table).map_err(sm_io_err)?;
        }
        tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
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
            let (log_id, _): (LogId<u64>, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| sm_io_err_any(ErrorSubject::Store, ErrorVerb::Read, e))?;
            self.last_applied = Some(log_id);
        }
        if let Some(v) = meta.get("last_membership").map_err(sm_io_err)? {
            let (membership, _): (StoredMembership<u64, BasicNode>, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
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
                let bytes = bincode::serde::encode_to_vec(log_id, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                meta.insert("last_applied", bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
            None => {
                meta.remove("last_applied").map_err(sm_io_err)?;
            }
        }
        let bytes = bincode::serde::encode_to_vec(last_membership, bincode::config::standard())
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
                let bytes = bincode::serde::encode_to_vec(node, bincode::config::standard())
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
            StateCommand::UpsertService(svc) => {
                let mut t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
                let bytes = bincode::serde::encode_to_vec(svc, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
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
                let mut t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
                let bytes = bincode::serde::encode_to_vec(task, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
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
                let mut t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
                let bytes = bincode::serde::encode_to_vec(net, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
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
                let mut t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
                let bytes = bincode::serde::encode_to_vec(vol, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
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
                let mut t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
                let bytes = bincode::serde::encode_to_vec(s, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
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
                let mut t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
                let bytes = bincode::serde::encode_to_vec(c, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(c.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
                Ok(StateResponse::Ok)
            }
            StateCommand::RemoveConfig(id) => {
                let mut t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
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
                let (node, _): (ClusterNode, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (node, _): (ClusterNode, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
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
                let (svc, _): (Service, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (svc, _): (Service, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
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
                let (task, _): (Task, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (task, _): (Task, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
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
                let (net, _): (Network, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (net, _): (Network, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
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
                let (vol, _): (Volume, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (vol, _): (Volume, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
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
                let (s, _): (Secret, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (s, _): (Secret, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
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
                let (c, _): (Config, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| e.to_string())?;
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
            let (c, _): (Config, usize) =
                bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                    .map_err(|e| e.to_string())?;
            configs.push(c);
        }
        Ok(configs)
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
        let (snap, _): (StateSnapshot, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
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
                let bytes = bincode::serde::encode_to_vec(node, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(node.id.to_string().as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
            for svc in &snap.services {
                let bytes = bincode::serde::encode_to_vec(svc, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(svc.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
            for task in &snap.tasks {
                let bytes = bincode::serde::encode_to_vec(task, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(task.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
            for net in &snap.networks {
                let bytes = bincode::serde::encode_to_vec(net, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(net.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
            for vol in &snap.volumes {
                let bytes = bincode::serde::encode_to_vec(vol, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(vol.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
            for s in &snap.secrets {
                let bytes = bincode::serde::encode_to_vec(s, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(s.id.as_str(), bytes.as_slice())
                    .map_err(sm_io_err)?;
            }
        }
        {
            let mut t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
            for c in &snap.configs {
                let bytes = bincode::serde::encode_to_vec(c, bincode::config::standard())
                    .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
                t.insert(c.id.as_str(), bytes.as_slice())
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
            last_applied: None,
            last_membership: StoredMembership::default(),
        };

        // Read all entities.
        {
            let t = tx.open_table(NODES_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (node, _): (ClusterNode, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.nodes.push(node);
            }
        }
        {
            let t = tx.open_table(SERVICES_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (svc, _): (Service, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.services.push(svc);
            }
        }
        {
            let t = tx.open_table(TASKS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (task, _): (Task, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.tasks.push(task);
            }
        }
        {
            let t = tx.open_table(NETWORKS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (net, _): (Network, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.networks.push(net);
            }
        }
        {
            let t = tx.open_table(VOLUMES_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (vol, _): (Volume, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.volumes.push(vol);
            }
        }
        {
            let t = tx.open_table(SECRETS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (s, _): (Secret, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.secrets.push(s);
            }
        }
        {
            let t = tx.open_table(CONFIGS_TABLE).map_err(sm_io_err)?;
            for r in t.iter().map_err(sm_io_err)? {
                let (_k, v) = r.map_err(sm_io_err)?;
                let (c, _): (Config, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.configs.push(c);
            }
        }
        // Read metadata.
        {
            let meta = tx.open_table(SM_META_TABLE).map_err(sm_io_err)?;
            if let Some(v) = meta.get("last_applied").map_err(sm_io_err)? {
                let (log_id, _): (LogId<u64>, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.last_applied = Some(log_id);
            }
            if let Some(v) = meta.get("last_membership").map_err(sm_io_err)? {
                let (m, _): (StoredMembership<u64, BasicNode>, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| sm_storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                snap.last_membership = m;
            }
        }

        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard())
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
