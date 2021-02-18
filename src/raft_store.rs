use crate::db_utils::{DBError, SimpleDb};
use bincode::{deserialize, serialize, Error as BincodeError};
use protobuf::Message;
use raft::prelude::*;
use raft::storage::MemStorage;
use raft::{Result as RaftResult, StorageError};
use tracing::{error, info};

const HARDSTATE_KEY: &str = "HardStateKey";
const SNAPSHOT_KEY: &str = "SnaphotKey";
const ENTRY_KEY: &str = "EntryKey";
const LAST_ENTRY_KEY: &str = "LastEntryKey";

pub struct RaftStore {
    /// In memory storage used during normal operation
    in_memory: MemStorage,
    /// Persistent storage used when restarting
    presistent: SimpleDb,
    /// First valid entry index in persistent storage
    persistent_first_entry: u64,
}

impl RaftStore {
    // Create a persistent Storage for raft
    pub fn new(presistent: SimpleDb) -> Self {
        Self {
            in_memory: MemStorage::new(),
            presistent,
            persistent_first_entry: 0,
        }
    }

    /// Consume store and return peristent storage
    pub fn take_persistent(&mut self) -> SimpleDb {
        std::mem::replace(&mut self.presistent, SimpleDb::new_in_memory())
    }

    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) -> RaftResult<()> {
        let bytes = hs.write_to_bytes()?;

        self.in_memory.wl().set_hardstate(hs);
        set_persistent_hardstate(&mut self.presistent, &bytes)?;

        Ok(())
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> RaftResult<()> {
        let bytes = snapshot.write_to_bytes()?;
        let index = snapshot.get_metadata().get_index();

        self.in_memory.wl().apply_snapshot(snapshot)?;
        set_persistent_snapshot(&mut self.presistent, &bytes)?;
        set_last_persistent_entry(&mut self.presistent, index)?;
        self.discard_persistent_entries_before_snapshot(index);
        Ok(())
    }

    /// Create and store snapshot
    pub fn create_snapshot(
        &mut self,
        idx: u64,
        cs: Option<ConfState>,
        pending_membership_change: Option<ConfChange>,
        data: Vec<u8>,
    ) -> RaftResult<()> {
        let bytes = self
            .in_memory
            .wl()
            .create_snapshot(idx, cs, pending_membership_change, data)?
            .write_to_bytes()?;

        set_persistent_snapshot(&mut self.presistent, &bytes)?;
        self.discard_persistent_entries_before_snapshot(idx);
        Ok(())
    }

    /// Discards all log entries prior to compact_index.
    /// Only apply to in memory storage: persistent storage always match the last snapshot
    pub fn compact(&mut self, compact_index: u64) -> RaftResult<()> {
        self.in_memory.wl().compact(compact_index)?;
        Ok(())
    }

    /// Append the new entries to storage.
    pub fn append(&mut self, ents: &[Entry]) -> RaftResult<()> {
        self.in_memory.wl().append(ents)?;

        let persistent_first_entry = self.persistent_first_entry;
        let entries_to_write = ents
            .iter()
            .map(|e| (e, e.get_index()))
            .filter(|(_, i)| *i >= persistent_first_entry);

        for (ent, index) in entries_to_write {
            set_persistent_entry(&mut self.presistent, index, ent)?;
            set_last_persistent_entry(&mut self.presistent, index)?;
        }
        Ok(())
    }

    /// Discard entries we are not using anymore
    fn discard_persistent_entries_before_snapshot(&mut self, snap_index: u64) {
        let old = std::mem::replace(&mut self.persistent_first_entry, snap_index);

        for index in old..snap_index {
            discard_persistent_entry(&mut self.presistent, index);
        }
    }

    /// Load in_memory from persistent DB if data is available.
    /// Otherwise only use configuration.
    pub fn load_in_memory_or_default(mut self, init_cs: ConfState) -> RaftResult<Self> {
        let in_memory = &self.in_memory;
        let presistent = &self.presistent;
        in_memory.wl().set_conf_state(init_cs, None);

        if let Some(snapshot) = get_persistent_snapshot(presistent)? {
            let snap_index = snapshot.get_metadata().get_index();
            info!("load snapshot idx={}", snap_index);

            in_memory.wl().apply_snapshot(snapshot)?;
            self.persistent_first_entry = snap_index;
        }

        if let Some(last_index) = get_last_persistent_entry(presistent)? {
            info!("load entries last entry={}", last_index);
            // If no snapshot, entry 0 does not existd.
            // If snapshot, skip snapshot entry as already applied.
            let end_index = last_index + 1;
            let start_index = std::cmp::min(self.persistent_first_entry + 1, end_index);

            let mut entries = Vec::new();
            let mut missing_entries = false;
            for index in start_index..end_index {
                let entry = match get_persistent_entry(presistent, index)? {
                    Some(e) => e,
                    None => {
                        error!(
                            "Entry unabailable {} in [{}..{}]",
                            index, start_index, end_index
                        );
                        missing_entries = true;
                        continue;
                    }
                };
                entries.push(entry);
            }
            if missing_entries {
                return Err(StorageError::Unavailable.into());
            }
            in_memory.wl().append(&entries)?;
        }

        if let Some(hs) = get_persistent_hardstate(presistent)? {
            info!("load hard_state commit={}", hs.get_commit());
            in_memory.wl().set_hardstate(hs);
        }

        Ok(self)
    }
}

impl Storage for RaftStore {
    /// Implements the Storage trait.
    fn initial_state(&self) -> RaftResult<RaftState> {
        self.in_memory.initial_state()
    }

    /// Implements the Storage trait.
    fn entries(&self, low: u64, high: u64, max_size: u64) -> RaftResult<Vec<Entry>> {
        self.in_memory.entries(low, high, max_size)
    }

    /// Implements the Storage trait.
    fn term(&self, idx: u64) -> RaftResult<u64> {
        self.in_memory.term(idx)
    }

    /// Implements the Storage trait.
    fn first_index(&self) -> RaftResult<u64> {
        self.in_memory.first_index()
    }

    /// Implements the Storage trait.
    fn last_index(&self) -> RaftResult<u64> {
        self.in_memory.last_index()
    }

    /// Implements the Storage trait.
    fn snapshot(&self) -> RaftResult<Snapshot> {
        self.in_memory.snapshot()
    }
}

/// Create a generic StorageError from DBError
fn from_db_err(err: DBError) -> StorageError {
    StorageError::Other(Box::new(err))
}

/// Create a generic StorageError from BincodeError
fn from_ser_err(err: BincodeError) -> StorageError {
    StorageError::Other(err)
}

/// Format entry key for db
fn format_entry_key(index: u64) -> String {
    format!("{}_{}", ENTRY_KEY, index)
}

fn get_persistent_entry(presistent: &SimpleDb, index: u64) -> RaftResult<Option<Entry>> {
    let key = format_entry_key(index);
    if let Some(bytes) = presistent.get(&key).map_err(from_db_err)? {
        Ok(Some(protobuf::parse_from_bytes::<Entry>(&bytes)?))
    } else {
        Ok(None)
    }
}

fn set_persistent_entry(presistent: &mut SimpleDb, index: u64, ent: &Entry) -> RaftResult<()> {
    let key = format_entry_key(index);
    let bytes = ent.write_to_bytes()?;
    presistent.put(&key, &bytes).map_err(from_db_err)?;
    Ok(())
}

fn discard_persistent_entry(presistent: &mut SimpleDb, index: u64) {
    let key = format_entry_key(index);
    if let Err(e) = presistent.delete(&key).map_err(from_db_err) {
        error!("Could not delete entry key ({}): {:?}", key, e);
    }
}

fn get_last_persistent_entry(presistent: &SimpleDb) -> RaftResult<Option<u64>> {
    if let Some(bytes) = presistent.get(LAST_ENTRY_KEY).map_err(from_db_err)? {
        Ok(Some(deserialize::<u64>(&bytes).map_err(from_ser_err)?))
    } else {
        Ok(None)
    }
}

fn set_last_persistent_entry(presistent: &mut SimpleDb, index: u64) -> RaftResult<()> {
    let bytes = serialize(&index).map_err(from_ser_err)?;
    presistent
        .put(LAST_ENTRY_KEY, &bytes)
        .map_err(from_db_err)?;
    Ok(())
}

fn get_persistent_snapshot(presistent: &SimpleDb) -> RaftResult<Option<Snapshot>> {
    if let Some(bytes) = presistent.get(SNAPSHOT_KEY).map_err(from_db_err)? {
        Ok(Some(protobuf::parse_from_bytes::<Snapshot>(&bytes)?))
    } else {
        Ok(None)
    }
}

fn set_persistent_snapshot(presistent: &mut SimpleDb, bytes: &[u8]) -> RaftResult<()> {
    presistent.put(SNAPSHOT_KEY, bytes).map_err(from_db_err)?;
    Ok(())
}

fn get_persistent_hardstate(presistent: &SimpleDb) -> RaftResult<Option<HardState>> {
    if let Some(bytes) = presistent.get(HARDSTATE_KEY).map_err(from_db_err)? {
        Ok(Some(protobuf::parse_from_bytes::<HardState>(&bytes)?))
    } else {
        Ok(None)
    }
}

fn set_persistent_hardstate(presistent: &mut SimpleDb, bytes: &[u8]) -> RaftResult<()> {
    presistent.put(HARDSTATE_KEY, bytes).map_err(from_db_err)?;
    Ok(())
}
