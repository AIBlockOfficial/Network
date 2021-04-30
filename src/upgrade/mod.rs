//! This module provide the upgrade functionality.
//! All necessary data structure/deserialization utilities need to
//! be stored in the frozen_last_version module.

mod frozen_last_version;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_last_version_db;

use crate::configurations::{DbMode, ExtraNodeParams};
use crate::constants::{
    DB_PATH, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED, TX_PREPEND, WALLET_PATH,
};
use crate::db_utils::{
    new_db_no_check_version, new_db_with_version, SimpleDb, SimpleDbError, SimpleDbSpec,
    SimpleDbWriteBatch, DB_COL_DEFAULT,
};
use crate::interfaces::BlockStoredInfo;
use crate::{compute, compute_raft, raft_store, storage, wallet};
use bincode::{deserialize, serialize};
use frozen_last_version as old;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use tracing::{error, trace};

pub const DB_SPEC_INFOS: &[DbSpecInfo] = &[
    DbSpecInfo {
        node_type: "compute",
        db_path: DB_PATH,
        suffix: ".compute",
    },
    DbSpecInfo {
        node_type: "compute",
        db_path: DB_PATH,
        suffix: ".compute_raft",
    },
    DbSpecInfo {
        node_type: "storage",
        db_path: DB_PATH,
        suffix: ".storage",
    },
    DbSpecInfo {
        node_type: "storage",
        db_path: DB_PATH,
        suffix: ".storage_raft",
    },
    DbSpecInfo {
        node_type: "miner",
        db_path: WALLET_PATH,
        suffix: "",
    },
    DbSpecInfo {
        node_type: "user",
        db_path: WALLET_PATH,
        suffix: "",
    },
];

/// Result wrapper for upgrade errors
pub type Result<T> = std::result::Result<T, UpgradeError>;

#[derive(Debug)]
pub struct DbSpecInfo {
    pub node_type: &'static str,
    pub db_path: &'static str,
    pub suffix: &'static str,
}

#[derive(Debug)]
pub enum UpgradeError {
    ConfigError(&'static str),
    DbError(SimpleDbError),
    Serialization(bincode::Error),
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::DbError(err) => write!(f, "DB error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for UpgradeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::DbError(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
        }
    }
}

impl From<SimpleDbError> for UpgradeError {
    fn from(other: SimpleDbError) -> Self {
        Self::DbError(other)
    }
}

impl From<bincode::Error> for UpgradeError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

/// Upgrade DB: New column are added at begining of upgrade and old one removed at the end.
pub fn get_upgrade_compute_db(
    db_mode: DbMode,
    old_dbs: ExtraNodeParams,
) -> Result<ExtraNodeParams> {
    let spec = &old::compute::DB_SPEC;
    let raft_spec = &old::compute_raft::DB_SPEC;
    let version = old::constants::NETWORK_VERSION_SERIALIZED;
    let mut db = new_db_with_version(db_mode, spec, version, old_dbs.db)?;
    let raft_db = new_db_with_version(db_mode, raft_spec, version, old_dbs.raft_db)?;

    db.upgrade_create_missing_cf(compute::DB_COL_INTERNAL)?;
    db.upgrade_create_missing_cf(compute::DB_COL_LOCAL_TXS)?;
    Ok(ExtraNodeParams {
        db: Some(db),
        raft_db: Some(raft_db),
        ..Default::default()
    })
}

/// Upgrade DB: upgrade ready given db  .
pub fn upgrade_compute_db(mut dbs: ExtraNodeParams) -> Result<ExtraNodeParams> {
    let db = dbs.db.as_mut().unwrap();
    let raft_db = dbs.raft_db.as_mut().unwrap();

    let (batch, raft_batch) =
        upgrade_compute_db_batch((&db, &raft_db), (db.batch_writer(), raft_db.batch_writer()))?;
    let (batch, raft_batch) = (batch.done(), raft_batch.done());

    db.write(batch)?;
    raft_db.write(raft_batch)?;
    Ok(dbs)
}

/// Upgrade DB: all columns new and old are expected to be opened
pub fn upgrade_compute_db_batch<'a>(
    (db, raft_db): (&SimpleDb, &SimpleDb),
    (mut batch, mut raft_batch): (SimpleDbWriteBatch<'a>, SimpleDbWriteBatch<'a>),
) -> Result<(SimpleDbWriteBatch<'a>, SimpleDbWriteBatch<'a>)> {
    batch.put_cf(DB_COL_DEFAULT, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED);
    raft_batch.put_cf(DB_COL_DEFAULT, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED);

    if let Some(value) = db.get_cf(DB_COL_DEFAULT, old::compute::REQUEST_LIST_KEY)? {
        batch.delete_cf(DB_COL_DEFAULT, old::compute::REQUEST_LIST_KEY);

        deserialize::<BTreeSet<SocketAddr>>(&value)?;
        batch.put_cf(compute::DB_COL_INTERNAL, compute::REQUEST_LIST_KEY, value);
    }

    clean_raft_db(raft_db, &mut raft_batch, |k, v| {
        let mut consensus: old::compute_raft::ComputeConsensused =
            tracked_deserialize("ComputeConsensused", &k, &v)?;
        if let Some(v) = &mut consensus.tx_current_block_num {
            // Force re-process block
            *v -= 1;
        }
        let consensus = old::convert_compute_consensused(
            consensus,
            Some(compute_raft::SpecialHandling::FirstUpgradeBlock),
        );
        Ok(serialize(&consensus)?)
    })?;

    Ok((batch, raft_batch))
}

/// Upgrade DB: New column are added at begining of upgrade and old one removed at the end.
pub fn get_upgrade_storage_db(
    db_mode: DbMode,
    old_dbs: ExtraNodeParams,
) -> Result<ExtraNodeParams> {
    let spec = &old::storage::DB_SPEC;
    let raft_spec = &old::storage_raft::DB_SPEC;
    let version = old::constants::NETWORK_VERSION_SERIALIZED;
    let mut db = new_db_with_version(db_mode, spec, version, old_dbs.db)?;
    let raft_db = new_db_with_version(db_mode, raft_spec, version, old_dbs.raft_db)?;

    db.upgrade_create_missing_cf(storage::DB_COL_INTERNAL)?;
    db.upgrade_create_missing_cf(storage::DB_COL_BC_ALL)?;
    db.upgrade_create_missing_cf(storage::DB_COL_BC_NOW)?;
    db.upgrade_create_missing_cf(storage::DB_COL_BC_V0_2_0)?;
    Ok(ExtraNodeParams {
        db: Some(db),
        raft_db: Some(raft_db),
        ..Default::default()
    })
}

/// Upgrade DB: upgrade ready given db  .
pub fn upgrade_storage_db(mut dbs: ExtraNodeParams) -> Result<ExtraNodeParams> {
    let db = dbs.db.as_mut().unwrap();
    let raft_db = dbs.raft_db.as_mut().unwrap();

    let (batch, raft_batch) =
        upgrade_storage_db_batch((&db, &raft_db), (db.batch_writer(), raft_db.batch_writer()))?;
    let (batch, raft_batch) = (batch.done(), raft_batch.done());

    db.write(batch)?;
    raft_db.write(raft_batch)?;
    Ok(dbs)
}

/// Upgrade DB: all columns new and old are expected to be opened
pub fn upgrade_storage_db_batch<'a>(
    (db, raft_db): (&SimpleDb, &SimpleDb),
    (mut batch, mut raft_batch): (SimpleDbWriteBatch<'a>, SimpleDbWriteBatch<'a>),
) -> Result<(SimpleDbWriteBatch<'a>, SimpleDbWriteBatch<'a>)> {
    batch.put_cf(DB_COL_DEFAULT, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED);
    raft_batch.put_cf(DB_COL_DEFAULT, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED);

    let mut max_block = None;
    for (key, value) in db.iter_cf_clone(DB_COL_DEFAULT) {
        batch.delete_cf(DB_COL_DEFAULT, &key);

        if is_transaction_key(&key) {
            let _: old::naom::Transaction = tracked_deserialize("Tx deserialize", &key, &value)?;
            storage::put_to_block_chain_at(&mut batch, storage::DB_COL_BC_V0_2_0, &key, &value);
        } else if is_block_key(&key) {
            let stored_block: old::naom::StoredSerializingBlock =
                tracked_deserialize("Block deserialize", &key, &value)?;
            let b_num = stored_block.block.header.b_num;

            max_block = std::cmp::max(max_block, Some((b_num, key.clone())));
            storage::put_to_block_chain_at(&mut batch, storage::DB_COL_BC_V0_2_0, &key, &value);
        } else {
            let e = UpgradeError::ConfigError("Unexpected key");
            return Err(log_key_value_error(e, "Unexpected key", &key, &value));
        }
    }

    let last_block_stored = if let Some((_, key)) = max_block {
        let value = db.get_cf(DB_COL_DEFAULT, &key)?;
        let value = value.ok_or(UpgradeError::ConfigError("Missing last block"))?;
        let stored_block: old::naom::StoredSerializingBlock =
            tracked_deserialize("Block deserialize", &key, &value)?;

        let mut mining_transactions = BTreeMap::new();
        for (_, (tx_hash, _)) in stored_block.mining_tx_hash_and_nonces {
            let value = db.get_cf(DB_COL_DEFAULT, &tx_hash)?;
            let value = value.ok_or(UpgradeError::ConfigError("Missing mining tx"))?;
            let tx: old::naom::Transaction = tracked_deserialize("Tx deserialize", &key, &value)?;

            let tx = old::convert_transaction(tx);
            mining_transactions.insert(tx_hash, tx);
        }

        let block_hash =
            String::from_utf8(key).map_err(|_| UpgradeError::ConfigError("Non UTF-8 block key"))?;

        let header = stored_block.block.header;

        BlockStoredInfo {
            block_hash,
            block_num: header.b_num,
            merkle_hash: header.merkle_root_hash,
            nonce: header.nonce,
            mining_transactions,
            shutdown: false,
        }
    } else {
        return Err(UpgradeError::ConfigError("No last block"));
    };

    clean_raft_db(raft_db, &mut raft_batch, |k, v| {
        let consensus: old::storage_raft::StorageConsensused =
            tracked_deserialize("StorageConsensused", &k, &v)?;
        let consensus =
            old::convert_storage_consensused(consensus, Some(last_block_stored.clone()));
        Ok(serialize(&consensus)?)
    })?;

    Ok((batch, raft_batch))
}

fn clean_raft_db(
    raft_db: &SimpleDb,
    raft_batch: &mut SimpleDbWriteBatch,
    convert: impl Fn(&[u8], Vec<u8>) -> Result<Vec<u8>>,
) -> Result<()> {
    for (key, value) in raft_db.iter_cf_clone(DB_COL_DEFAULT) {
        raft_batch.delete_cf(DB_COL_DEFAULT, &key);
        if key == old::raft_store::SNAPSHOT_KEY.as_bytes() {
            let (data, meta) = get_old_persistent_snapshot_data_and_metadata(&value)?;
            let data = convert(&key, data)?;

            raft_batch.put_cf(DB_COL_DEFAULT, raft_store::SNAPSHOT_META_KEY, &meta);
            raft_batch.put_cf(DB_COL_DEFAULT, raft_store::SNAPSHOT_DATA_KEY, data);
        } else if !(key.starts_with(old::raft_store::ENTRY_KEY.as_bytes())
            || key == old::raft_store::HARDSTATE_KEY.as_bytes()
            || key == old::raft_store::LAST_ENTRY_KEY.as_bytes())
        {
            let e = UpgradeError::ConfigError("Unexpected raft key");
            return Err(log_key_value_error(e, "Unexpected raft key", &key, &value));
        }
    }
    Ok(())
}

/// Upgrade DB: New column are added at begining of upgrade and old one removed at the end.
pub fn get_upgrade_wallet_db(db_mode: DbMode, old_dbs: ExtraNodeParams) -> Result<ExtraNodeParams> {
    let spec = &old::wallet::DB_SPEC;
    let version = old::constants::NETWORK_VERSION_SERIALIZED;
    let db = new_db_with_version(db_mode, spec, version, old_dbs.wallet_db)?;
    Ok(ExtraNodeParams {
        wallet_db: Some(db),
        ..Default::default()
    })
}

/// Upgrade DB: upgrade ready given db  .
pub fn upgrade_wallet_db(mut dbs: ExtraNodeParams, passphrase: &str) -> Result<ExtraNodeParams> {
    let db = dbs.wallet_db.as_mut().unwrap();
    let batch = upgrade_wallet_db_batch(&db, db.batch_writer(), passphrase)?.done();
    db.write(batch)?;
    Ok(dbs)
}

/// Upgrade DB: all columns new and old are expected to be opened
pub fn upgrade_wallet_db_batch<'a>(
    db: &SimpleDb,
    mut batch: SimpleDbWriteBatch<'a>,
    passphrase: &str,
) -> Result<SimpleDbWriteBatch<'a>> {
    batch.put_cf(DB_COL_DEFAULT, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED);

    let masterkey = wallet::get_or_save_master_key_store(&db, &mut batch, passphrase.as_bytes());

    for (key, value) in db.iter_cf_clone(DB_COL_DEFAULT) {
        if key == old::wallet::FUND_KEY.as_bytes() {
            // Keep as is
            let f: old::wallet::FundStore =
                tracked_deserialize("FundStore deserialize", &key, &value)?;
            trace!("FundStore: {:?}", f);
        } else if key == old::wallet::KNOWN_ADDRESS_KEY.as_bytes() {
            // Keep as is
            let _: old::wallet::KnownAddresses =
                tracked_deserialize("Known Addresses deserialize", &key, &value)?;
        } else if is_wallet_transaction_store_key(&key) {
            // Keep as is
            let _: old::wallet::TransactionStore =
                tracked_deserialize("Tx Store deserialize", &key, &value)?;
        } else if is_wallet_address_store_key(&key) {
            let addr: old::wallet::AddressStore =
                tracked_deserialize("Addr Store deserialize", &key, &value)?;

            let key =
                String::from_utf8(key).map_err(|_| UpgradeError::ConfigError("Non UTF-8 key"))?;
            let addr = old::convert_address_store(addr);

            batch.delete_cf(DB_COL_DEFAULT, &key);
            wallet::save_address_store_to_wallet(&mut batch, &key, addr, &masterkey);
        } else {
            let e = UpgradeError::ConfigError("Key not recognized");
            return Err(log_key_value_error(e, "", &key, &value));
        }
    }

    Ok(batch)
}

/// whether it is a transaction key
pub fn is_transaction_key(key: &[u8]) -> bool {
    // special genesis block transactions had 6 digits and missed prefix
    key.get(0) == Some(&TX_PREPEND) || key.len() == 6
}

/// Wallet AddressStore key
fn is_wallet_address_store_key(key: &[u8]) -> bool {
    key.len() == 32
}

/// Wallet TransactionStore key
fn is_wallet_transaction_store_key(key: &[u8]) -> bool {
    // special genesis block transactions had 6 digits and missed prefix
    key.len() == 44 || key.len() == 18
}

/// whether it is a block key
pub fn is_block_key(key: &[u8]) -> bool {
    key.len() == 64
}

/// Open a database for dump doing no checks on validity
pub fn get_db_to_dump_no_checks(
    db_mode: DbMode,
    db_info: &DbSpecInfo,
    old_db: Option<SimpleDb>,
) -> Result<SimpleDb> {
    let spec = SimpleDbSpec {
        db_path: db_info.db_path,
        suffix: db_info.suffix,
        columns: &[],
    };
    Ok(new_db_no_check_version(db_mode, &spec, old_db)?)
}

/// Dump the database as string
pub fn dump_db(db: &'_ SimpleDb) -> impl Iterator<Item = String> + '_ {
    db.iter_all_cf_clone()
        .into_iter()
        .flat_map(|(c, it)| it.map(move |(k, v)| (c.clone(), k, v)))
        .map(|(c, k, v)| (c, to_u8_array_literal(&k), v))
        .map(|(c, k, v)| (c, k, to_u8_array_literal(&v)))
        .map(|(c, k, v)| format!("b\"{}\", b\"{}\", b\"{}\"", c, k, v))
}

/// Convert to a valid array literal displaying ASCII nicely
fn to_u8_array_literal(value: &[u8]) -> String {
    let mut result = String::with_capacity(value.len());
    for v in value {
        if v.is_ascii_alphanumeric() || v == &b'_' {
            result.push(*v as char);
        } else {
            result.push_str(&format!("\\x{:02X}", v));
        }
    }
    result
}

fn log_key_value_error<E: fmt::Debug>(e: E, info: &str, key: &[u8], value: &[u8]) -> E {
    let log_key = to_u8_array_literal(key);
    let log_value = to_u8_array_literal(value);
    error!("{}: {:?} \"{}\" -> \"{}\"", info, e, log_key, log_value);
    e
}

fn tracked_deserialize<'a, T: serde::Deserialize<'a>>(
    tag: &str,
    key: &[u8],
    value: &'a [u8],
) -> Result<T> {
    Ok(match deserialize(value) {
        Ok(v) => Ok(v),
        Err(e) => Err(log_key_value_error(e, tag, key, value)),
    }?)
}

fn get_old_persistent_snapshot_data_and_metadata(snapshot: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    use protobuf::Message;
    use raft::prelude::Snapshot;
    let mut snapshot = protobuf::parse_from_bytes::<Snapshot>(snapshot)
        .map_err(|e| UpgradeError::Serialization(serde::de::Error::custom(e)))?;
    let data = snapshot.take_data();
    let meta = snapshot
        .get_metadata()
        .write_to_bytes()
        .map_err(|e| UpgradeError::Serialization(serde::ser::Error::custom(e)))?;
    Ok((data, meta))
}
