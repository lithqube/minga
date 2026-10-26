//! redb-backed Raft log storage.
//!
//! Stores log entries, votes and the committed log id in a redb database.
//! Each log entry is serialized with bincode and stored under its log index.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{
    AnyError, Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, StorageError,
    StorageIOError, Vote,
};
use redb::{Database, ReadableTable, TableDefinition};
use std::sync::Mutex;

use crate::TypeConfig;

/// Log entries: log index → serialized entry.
const LOGS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
/// Metadata: key → serialized value. Keys: `vote`, `committed`, `last_purged_log_id`.
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

/// A redb-backed Raft log store.
pub struct RedbLogStore {
    db: Arc<Mutex<Database>>,
}

impl RedbLogStore {
    /// Opens or creates a log store at the given path.
    #[allow(clippy::result_large_err)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageIOError<u64>> {
        let db = Database::create(path).map_err(io_err)?;
        // Ensure tables exist.
        let tx = db.begin_write().map_err(io_err)?;
        tx.open_table(LOGS_TABLE).map_err(io_err)?;
        tx.open_table(META_TABLE).map_err(io_err)?;
        tx.commit().map_err(io_err)?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }
}

impl Debug for RedbLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbLogStore").finish()
    }
}

impl Clone for RedbLogStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

impl RaftLogReader<TypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(io_err)?;
        let table = tx.open_table(LOGS_TABLE).map_err(io_err)?;

        let start = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut entries = Vec::new();
        for result in table.range(start..end).map_err(io_err)? {
            let (_key, value) = result.map_err(io_err)?;
            let entry: Entry<TypeConfig> =
                bincode::serde::decode_from_slice(value.value(), bincode::config::standard())
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Logs,
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    })?
                    .0;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(io_err)?;

        let mut last_purged_log_id: Option<LogId<u64>> = None;
        let mut last_log_id: Option<LogId<u64>> = None;

        // Check for purged log id in metadata.
        {
            let meta = tx.open_table(META_TABLE).map_err(io_err)?;
            if let Some(purged) = meta.get("last_purged_log_id").map_err(io_err)? {
                let (log_id, _): (LogId<u64>, usize) =
                    bincode::serde::decode_from_slice(purged.value(), bincode::config::standard())
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Logs,
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })?;
                last_purged_log_id = Some(log_id);
            }
        }

        // Find the last log entry.
        {
            let table = tx.open_table(LOGS_TABLE).map_err(io_err)?;
            if let Some(result) = table.range(0..u64::MAX).map_err(io_err)?.last() {
                let (_key, value) = result.map_err(io_err)?;
                let entry: Entry<TypeConfig> =
                    bincode::serde::decode_from_slice(value.value(), bincode::config::standard())
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Logs,
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })?
                        .0;
                last_log_id = Some(entry.log_id);
            }
        }

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(io_err)?;
        {
            let mut table = tx.open_table(META_TABLE).map_err(io_err)?;
            let bytes =
                bincode::serde::encode_to_vec(vote, bincode::config::standard()).map_err(|e| {
                    StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Vote,
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    }
                })?;
            table.insert("vote", bytes.as_slice()).map_err(io_err)?;
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(io_err)?;
        let table = tx.open_table(META_TABLE).map_err(io_err)?;
        match table.get("vote").map_err(io_err)? {
            Some(v) => {
                let (vote, _): (Vote<u64>, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Vote,
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })?;
                Ok(Some(vote))
            }
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(io_err)?;
        {
            let mut table = tx.open_table(META_TABLE).map_err(io_err)?;
            match committed {
                Some(log_id) => {
                    let bytes = bincode::serde::encode_to_vec(log_id, bincode::config::standard())
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Logs,
                                ErrorVerb::Write,
                                AnyError::new(&e),
                            ),
                        })?;
                    table
                        .insert("committed", bytes.as_slice())
                        .map_err(io_err)?;
                }
                None => {
                    table.remove("committed").map_err(io_err)?;
                }
            }
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_read().map_err(io_err)?;
        let table = tx.open_table(META_TABLE).map_err(io_err)?;
        match table.get("committed").map_err(io_err)? {
            Some(v) => {
                let (log_id, _): (LogId<u64>, usize) =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())
                        .map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Logs,
                                ErrorVerb::Read,
                                AnyError::new(&e),
                            ),
                        })?;
                Ok(Some(log_id))
            }
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(io_err)?;
        {
            let mut table = tx.open_table(LOGS_TABLE).map_err(io_err)?;
            for entry in entries {
                let index = entry.log_id.index;
                let bytes = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::LogIndex(index),
                            ErrorVerb::Write,
                            AnyError::new(&e),
                        ),
                    })?;
                table.insert(index, bytes.as_slice()).map_err(io_err)?;
            }
        }
        tx.commit().map_err(io_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(io_err)?;
        {
            let mut table = tx.open_table(LOGS_TABLE).map_err(io_err)?;
            // Remove all entries with index >= log_id.index.
            let start = log_id.index;
            let to_remove: Vec<u64> = table
                .range(start..)
                .map_err(io_err)?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .collect();
            for key in to_remove {
                table.remove(key).map_err(io_err)?;
            }
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let db = self.db.lock().unwrap();
        let tx = db.begin_write().map_err(io_err)?;
        {
            let mut table = tx.open_table(LOGS_TABLE).map_err(io_err)?;
            // Remove all entries with index <= log_id.index.
            let end = log_id.index + 1;
            let to_remove: Vec<u64> = table
                .range(..end)
                .map_err(io_err)?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .collect();
            for key in to_remove {
                table.remove(key).map_err(io_err)?;
            }
        }
        {
            // Record the purged log id.
            let mut meta = tx.open_table(META_TABLE).map_err(io_err)?;
            let bytes = bincode::serde::encode_to_vec(log_id, bincode::config::standard())
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        AnyError::new(&e),
                    ),
                })?;
            meta.insert("last_purged_log_id", bytes.as_slice())
                .map_err(io_err)?;
        }
        tx.commit().map_err(io_err)?;
        Ok(())
    }
}

fn io_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageIOError<u64> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e))
}
