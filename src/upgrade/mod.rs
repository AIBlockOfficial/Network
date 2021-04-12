//! This module provide the upgrade functionality.
//! All necessary data structure/deserialization utilities need to
//! be stored in the frozen_last_version module.

mod frozen_last_version;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_last_version_db;

use crate::compute;
use crate::configurations::DbMode;
use crate::constants::{DB_PATH, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED, WALLET_PATH};
use crate::db_utils::{
    new_db_no_check_version, new_db_with_version, SimpleDb, SimpleDbError, SimpleDbSpec,
    SimpleDbWriteBatch, DB_COL_DEFAULT,
};
use bincode::deserialize;
use frozen_last_version as old;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;

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
pub fn get_upgrade_compute_db(db_mode: DbMode, old_db: Option<SimpleDb>) -> Result<SimpleDb> {
    let spec = &old::compute::DB_SPEC;
    let version = old::constants::NETWORK_VERSION_SERIALIZED;
    let mut db = new_db_with_version(db_mode, spec, version, old_db)?;

    db.upgrade_create_missing_cf(compute::DB_COL_INTERNAL)?;
    db.upgrade_create_missing_cf(compute::DB_COL_LOCAL_TXS)?;
    Ok(db)
}

/// Upgrade DB: upgrade ready given db  .
pub fn upgrade_compute_db(mut db: SimpleDb) -> Result<SimpleDb> {
    let batch = upgrade_compute_db_batch(&db, db.batch_writer())?.done();
    db.write(batch)?;
    Ok(db)
}

/// Upgrade DB: all columns new and old are expected to be opened
pub fn upgrade_compute_db_batch<'a>(
    db: &SimpleDb,
    mut batch: SimpleDbWriteBatch<'a>,
) -> Result<SimpleDbWriteBatch<'a>> {
    batch.put_cf(DB_COL_DEFAULT, DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED);

    if let Some(value) = db.get_cf(DB_COL_DEFAULT, old::compute::REQUEST_LIST_KEY)? {
        batch.delete_cf(DB_COL_DEFAULT, old::compute::REQUEST_LIST_KEY);

        deserialize::<BTreeSet<SocketAddr>>(&value)?;
        batch.put_cf(compute::DB_COL_INTERNAL, compute::REQUEST_LIST_KEY, value);
    }
    Ok(batch)
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
