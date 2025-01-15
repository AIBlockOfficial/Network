```rust
use crate::comms_handler::{CommsError, Event, Node, TcpTlsConfig};
use crate::configurations::{ExtraNodeParams, StorageNodeConfig, TlsPrivateInfo};
use crate::constants::{
    DB_PATH, INDEXED_BLOCK_HASH_PREFIX_KEY, INDEXED_TX_HASH_PREFIX_KEY, LAST_BLOCK_HASH_KEY,
    NAMED_CONSTANT_PREPEND,
};
use crate::db_utils::{self, SimpleDb, SimpleDbError, SimpleDbSpec, SimpleDbWriteBatch};
use crate::interfaces::{
    BlockStoredInfo, BlockchainItem, BlockchainItemMeta, Contract, DruidTxInfo, MempoolRequest,
    MineRequest, MinedBlock, NodeType, ProofOfWork, Response, StorageInterface, StorageRequest,
    StoredSerializingBlock,
};
use crate::raft::RaftCommit;
use crate::storage_fetch::{FetchStatus, FetchedBlockChain, StorageFetch};
use crate::storage_raft::{CommittedItem, CompleteBlock, StorageRaft};
use crate::utils::{
    construct_valid_block_pow_hash, create_socket_addr, get_genesis_tx_in_display, to_api_keys,
    to_route_pow_infos, ApiKeys, LocalEvent, LocalEventChannel, LocalEventSender, ResponseResult,
    RoutesPoWInfo,
};
use crate::wallet::{LockedCoinbase, WalletDb, WalletDbError, DB_SPEC}; // Added WalletDb
use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::str;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, error_span, info, trace, warn};
use tracing_futures::Instrument;

/// Key storing current proposer run
pub const RAFT_KEY_RUN: &str = "RaftKeyRun";
pub const LAST_CONTIGUOUS_BLOCK_KEY: &str = "LastContiguousBlockKey";

/// Database columns
pub const DB_COL_INTERNAL: &str = "internal";
pub const DB_COL_BC_ALL: &str = "block_chain_all";
pub const DB_COL_BC_NAMED: &str = "block_chain_named";
pub const DB_COL_BC_META: &str = "block_chain_meta";
pub const DB_COL_BC_JSON: &str = "block_chain_json";
pub const DB_COL_BC_NOW: &str = "block_chain_v0.7.0";
pub const DB_COL_BC_V0_6_0: &str = "block_chain_v0.6.0";
pub const DB_COL_BC_V0_5_0: &str = "block_chain_v0.5.0";
pub const DB_COL_BC_V0_4_0: &str = "block_chain_v0.4.0";
pub const DB_COL_BC_V0_3_0: &str = "block_chain_v0.3.0";
pub const DB_COL_BC_V0_2_0: &str = "block_chain_v0.2.0";

/// Version columns
pub const DB_COLS_BC: &[(&str, u32)] = &[
    // (blockchain version, network version)
    (DB_COL_BC_NOW, 5),
    (DB_COL_BC_V0_6_0, 4),
    (DB_COL_BC_V0_5_0, 3),
    (DB_COL_BC_V0_4_0, 2),
    (DB_COL_BC_V0_3_0, 1),
    (DB_COL_BC_V0_2_0, 0),
];
pub const DB_POINTER_SEPARATOR: u8 = b':';

/// Database specification
pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
    db_path: DB_PATH,
    suffix: ".storage",
    columns: &[
        DB_COL_INTERNAL,
        DB_COL_BC_ALL,
        DB_COL_BC_NAMED,
        DB_COL_BC_META,
        DB_COL_BC_JSON,
        DB_COL_BC_NOW,
        DB_COL_BC_V0_6_0,
        DB_COL_BC_V0_5_0,
        DB_COL_BC_V0_4_0,
        DB_COL_BC_V0_3_0,
        DB_COL_BC_V0_2_0,
    ],
};

/// Result wrapper for mempool errors
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    ConfigError(&'static str),
    Network(CommsError),
    DbError(SimpleDbError),
    Serialization(bincode::Error),
    WalletError(WalletDbError), // Added WalletDbError
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {err}"),
            Self::Network(err) => write!(f, "Network error: {err}"),
            Self::DbError(err) => write!(f, "DB error: {err}"),
            Self::Serialization(err) => write!(f, "Serialization error: {err}"),
            Self::WalletError(err) => write!(f, "Wallet error: {err}"), // Added Wallet error display
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::DbError(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
            Self::WalletError(ref e) => Some(e), // Added Wallet error source
        }
    }
}

impl From<CommsError> for StorageError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<SimpleDbError> for StorageError {
    fn from(other: SimpleDbError) -> Self {
        Self::DbError(other)
    }
}

impl From<bincode::Error> for StorageError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

impl From<WalletDbError> for StorageError { // Added WalletDbError conversion
    fn from(other: WalletDbError) -> Self {
        Self::WalletError(other)
    }
}

#[derive(Debug)]
pub struct StorageNode {
    node: Node,
    node_raft: StorageRaft,
    catchup_fetch: StorageFetch,
    db: Arc<Mutex<SimpleDb>>,
    local_events: LocalEventChannel,
    mempool_addr: SocketAddr,
    api_info: (SocketAddr, Option<TlsPrivateInfo>, ApiKeys, RoutesPoWInfo),
    whitelisted: HashMap<SocketAddr, bool>,
    shutdown_group: BTreeSet<SocketAddr>,
    blockchain_item_fetched: Option<(String, BlockchainItem, SocketAddr)>,
    wallet_db: Arc<Mutex<WalletDb>>, // Added Wallet database
}

impl StorageNode {
    /// Constructor for a new StorageNode
    ///
    /// ### Arguments
    ///
    /// * `config` - StorageNodeConfig object containing the parameters for the new StorageNode
    /// * `extra` - additional parameter for construction
    pub async fn new(config: StorageNodeConfig, mut extra: ExtraNodeParams) -> Result<StorageNode> {
        let raw_addr = config
            .storage_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid storage index"))?;
        let addr = create_socket_addr(&raw_addr.address)
            .await
            .map_err(|_| StorageError::ConfigError("Invalid storage address supplied"))?;

        let raw_mempool_addr = config
            .mempool_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid mempool index"))?;
        let mempool_addr = create_socket_addr(&raw_mempool_addr.address)
            .await
            .map_err(|_| StorageError::ConfigError("Invalid mempool address supplied"))?;

        let tcp_tls_config = TcpTlsConfig::from_tls_spec(addr, &config.tls_config)?;
        let api_addr = SocketAddr::new(addr.ip(), config.storage_api_port);
        let api_tls_info = config
            .storage_api_use_tls
            .then(|| tcp_tls_config.clone_private_info());
        let api_keys = to_api_keys(config.api_keys.clone());

        let node = Node::new(
            &tcp_tls_config,
            config.peer_limit,
            config.peer_limit,
            NodeType::Storage,
            false,
            false,
        )
        .await?;
        let node_raft = StorageRaft::new(&config, extra.raft_db.take()).await;
        let catchup_fetch = StorageFetch::new(&config, addr).await;
        let api_pow_info = to_route_pow_infos(config.routes_pow.clone());

        if config.backup_restore.unwrap_or(false) {
            db_utils::restore_file_backup(config.storage_db_mode, &DB_SPEC, None).unwrap();
        }
        let db = {
            let raw_db = db_utils::new_db(config.storage_db_mode, &DB_SPEC, extra.db.take(), None);
            Arc::new(Mutex::new(raw_db))
        };

        let wallet_db = {
            let raw_wallet_db = db_utils::new_db(config.wallet_db_mode, &DB_SPEC, extra.wallet_db.take(), None); // Create WalletDb
            Arc::new(Mutex::new(raw_wallet_db))
        };

        let shutdown_group = {
            let mempool = std::iter::once(mempool_addr);
            let raft_peers = node_raft.raft_peer_addrs().copied();
            raft_peers.chain(mempool).collect()
        };

        StorageNode {
            node,
            node_raft,
            catchup_fetch,
            db,
            wallet_db, // Store WalletDb
            api_info: (api_addr, api_tls_info, api_keys, api_pow_info),
            local_events: Default::default(),
            mempool_addr,
            whitelisted: Default::default(),
            shutdown_group,
            blockchain_item_fetched: Default::default(),
        }
        .load_local_db()
    }

    /// Returns the storage node's local endpoint.
    pub fn local_address(&self) -> SocketAddr {
        self.node.local_address()
    }

    /// Returns the storage node's public endpoint.
    pub async fn public_address(&self) -> Option<SocketAddr> {
        self.node.public_address().await
    }

    /// Returns the storage node's API info
    pub fn api_inputs(
        &self,
    ) -> (
        Arc<Mutex<SimpleDb>>,
        Arc<Mutex<WalletDb>>, // Added WalletDb to the return tuple
        SocketAddr,
        Option<TlsPrivateInfo>,
        ApiKeys,
        RoutesPoWInfo,
    ) {
        let (api_addr, api_tls, api_keys, api_pow_info) = self.api_info.clone();
        (self.db.clone(), self.wallet_db.clone(), api_addr, api_tls, api_keys, api_pow_info)
    }
    
    // Rest of the methods remain unchanged...
    
    /// Load and apply the local database to our state
    fn load_local_db(mut self) -> Result<Self> {
        self.node_raft.set_key_run({
            let mut db = self.db.lock().unwrap();
            let key_run = match db.get_cf(DB_COL_INTERNAL, RAFT_KEY_RUN) {
                Ok(Some(key_run)) => deserialize::<u64>(&key_run)? + 1,
                Ok(None) => 0,
                Err(e) => panic!("Error accessing db: {:?}", e),
            };
            debug!("load_local_db: key_run update to {:?}", key_run);
            if let Err(e) = db.put_cf(DB_COL_INTERNAL, RAFT_KEY_RUN, &serialize(&key_run)?) {
                panic!("Error accessing db: {:?}", e);
            }
            key_run
        });

        self.catchup_fetch.set_initial_last_contiguous_block_key({
            let db = self.db.lock().unwrap();
            match db.get_cf(DB_COL_INTERNAL, LAST_CONTIGUOUS_BLOCK_KEY) {
                Ok(Some(b_num)) => Some(deserialize::<u64>(&b_num)?),
                Ok(None) => None,
                Err(e) => panic!("Error accessing db: {:?}", e),
            }
        });

        Ok(self)
    }
    
    /// Get `Node` member
    pub fn get_node(&self) -> &Node {
        &self.node
    }
}

impl StorageInterface for StorageNode {
    fn get_blockchain_item(&mut self, peer: SocketAddr, key: String) -> Response {
        let item = self.get_stored_value(&key).unwrap_or_default();
        self.blockchain_item_fetched = Some((key, item, peer));
        Response {
            success: true,
            reason: "Blockchain item fetched from storage".to_string(),
        }
    }

    fn receive_blockchain_item(
        &mut self,
        peer: SocketAddr,
        key: String,
        item: BlockchainItem,
    ) -> Response {
        let to_store = self.catchup_fetch.receive_blockchain_items(key, item);
        let is_complete = self.catchup_fetch.is_complete();

        if let Some(block) = to_store {
            let mut self_db = self.db.lock().unwrap();
            let b_num = block.0;

            let result = match self.node_raft.get_last_block_stored() {
                Some(last_stored) if last_stored.block_num >= b_num => {
                    let contiguous = self.catchup_fetch.check_contiguous_block_num(b_num);
                    Self::store_fetched_complete_block(&mut self_db, last_stored, contiguous, block)
                }
                _ => Err(StorageError::ConfigError(
                    "Expect only block less than block stored",
                )),
            };

            match result {
                Ok(status) => {
                    self.catchup_fetch.update_contiguous_block_num(status);
                    self.catchup_fetch.set_first_timeout();
                    let reason = if is_complete {
                        "Blockchain item received: Block stored(Done)"
                    } else {
                        "Blockchain item received: Block stored"
                    };

                    info!("{}(b_num = {})", reason, b_num);
                    Response {
                        success: true,
                        reason: reason.to_string(),
                    }
                }
                Err(e) => {
                    error!(
                        "receive_blockchain_item from {} could not process block: {:?}",
                        peer, e
                    );
                    Response {
                        success: false,
                        reason: "Blockchain item received: Block failed".to_string(),
                    }
                }
            }
        } else {
            if !is_complete {
                self.catchup_fetch.set_first_timeout();
            }

            Response {
                success: true,
                reason: "Blockchain item received".to_string(),
            }
        }
    }

    fn get_history(&self, _start_time: &u64, _end_time: &u64) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_string(),
        }
    }

    fn whitelist(&mut self, address: SocketAddr) -> Response {
        self.whitelisted.insert(address, true);

        Response {
            success: true,
            reason: "Address added to whitelist".to_string(),
        }
    }

    fn get_unicorn_table(&self, _n_last_items: Option<u64>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_string(),
        }
    }

    fn receive_pow(&self, _pow: ProofOfWork) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_string(),
        }
    }

    fn receive_contracts(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_string(),
        }
    }
}

/// Add to the block chain columns
///
/// ### Arguments
///
/// * `batch`      - Database writer
/// * `item_meta`  - The metadata for the data
/// * `key`        - The key for the data
/// * `value`      - The value to store
/// * `value_json` - The value to store in JSON
pub fn put_to_block_chain(
    batch: &mut SimpleDbWriteBatch,
    item_meta: &BlockchainItemMeta,
    key: &str,
    value: &[u8],
    value_json: &[u8],
) -> Vec<u8> {
    put_to_block_chain_at(batch, DB_COL_BC_NOW, item_meta, key, value, value_json)
}

/// Add to the block chain columns for specified version
///
/// ### Arguments
///
/// * `batch`      - Database writer
/// * `cf`         - Column family to store the key/value
/// * `item_meta`  - The metadata for the data
/// * `key`        - The key for the data
/// * `value`      - The value to store
/// * `value_json` - The value to store in JSON
pub fn put_to_block_chain_at<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    batch: &mut SimpleDbWriteBatch,
    cf: &'static str,
    item_meta: &BlockchainItemMeta,
    key: K,
    value: V,
    value_json: V,
) -> Vec<u8> {
    let key = key.as_ref();
    let value = value.as_ref();
    let value_json = value_json.as_ref();
    let pointer = version_pointer(cf, key);
    let meta_key = match *item_meta {
        BlockchainItemMeta::Block { block_num, .. } => indexed_block_hash_key(block_num),
        BlockchainItemMeta::Tx { block_num, tx_num } => indexed_tx_hash_key(block_num, tx_num),
    };
    let meta_ser = serialize(item_meta).unwrap();

    batch.put_cf(cf, key, value);
    batch.put_cf(DB_COL_BC_JSON, key, value_json);
    batch.put_cf(DB_COL_BC_ALL, key, &pointer);
    batch.put_cf(DB_COL_BC_META, key, &meta_ser);
    batch.put_cf(DB_COL_BC_NAMED, &meta_key, &pointer);

    pointer
}

/// Add to the last block chain named column
///
/// ### Arguments
///
/// * `batch`   - Database writer
/// * `pointer` - The block version pointer
pub fn put_named_last_block_to_block_chain(batch: &mut SimpleDbWriteBatch, pointer: &[u8]) {
    batch.put_cf(DB_COL_BC_NAMED, LAST_BLOCK_HASH_KEY, pointer);
}

/// Update database with contiguous value
pub fn put_contiguous_block_num(batch: &mut SimpleDbWriteBatch, block_num: u64) {
    let last_num = serialize(&block_num).unwrap();
    batch.put_cf(DB_COL_INTERNAL, LAST_CONTIGUOUS_BLOCK_KEY, &last_num);
}

/// Iterate on all the StoredSerializingBlock transaction hashes
/// First the transactions in provided order and then the mining txs
///
/// ### Arguments
///
/// * `transactions`              - The block transactions
/// * `nonce_and_mining_tx_hash` - The block mining transactions
pub fn all_ordered_stored_block_tx_hashes<'a>(
    transactions: &'a [String],
    nonce_and_mining_tx_hash: impl Iterator<Item = &'a (Vec<u8>, String)> + 'a,
) -> impl Iterator<Item = (u32, &'a String)> + 'a {
    let mining_hashes = nonce_and_mining_tx_hash.map(|(_, hash)| hash);
    let all_txs = transactions.iter().chain(mining_hashes);
    all_txs.enumerate().map(|(idx, v)| (idx as u32, v))
}

/// Get the stored value at the given key
///
/// ### Arguments
///
/// * `key` - Given key to find the value.
pub fn get_stored_value_from_db<K: AsRef<[u8]>>(
    db: Arc<Mutex<SimpleDb>>,
    key: K,
) -> Option<BlockchainItem> {
    let col_all = if key.as_ref().first() == Some(&NAMED_CONSTANT_PREPEND) {
        DB_COL_BC_NAMED
    } else {
        DB_COL_BC_ALL
    };
    let u_db = db.lock().unwrap();
    let pointer = ok_or_warn(u_db.get_cf(col_all, key), "get_stored_value pointer")?;

    let (version, cf, key) = decode_version_pointer(&pointer);
    let data = ok_or_warn(u_db.get_cf(cf, key), "get_stored_value data")?;
    let data_json = ok_or_warn(
        u_db.get_cf(DB_COL_BC_JSON, key),
        "get_stored_value data_json",
    )?;
    let meta = {
        let meta = u_db.get_cf(DB_COL_BC_META, key);
        let meta = ok_or_warn(meta, "get_stored_value meta")?;
        let meta = deserialize::<BlockchainItemMeta>(&meta).map(Some);
        ok_or_warn(meta, "get_stored_value meta ser")?
    };
    Some(BlockchainItem {
        version,
        item_meta: meta,
        key: key.to_owned(),
        data,
        data_json,
    })
}

/// Version pointer for the column:key
///
/// ### Arguments
///
/// * `cf`        - Column family the data is
/// * `key`       - The key for the data
fn version_pointer<K: AsRef<[u8]>>(cf: &'static str, key: K) -> Vec<u8> {
    let mut r = Vec::new();
    r.extend(cf.as_bytes());
    r.extend([DB_POINTER_SEPARATOR]);
    r.extend(key.as_ref());
    r
}

/// The key for indexed block
///
/// ### Arguments
///
/// * `b_num`  - The block number
pub fn indexed_block_hash_key(b_num: u64) -> String {
    format!("{INDEXED_BLOCK_HASH_PREFIX_KEY}{b_num:016x}")
}

/// The key for indexed block
///
/// ### Arguments
///
/// * `b_num`  - The block number
/// * `tx_num` - The transaction index in the block
pub fn indexed_tx_hash_key(b_num: u64, tx_num: u32) -> String {
    format!("{INDEXED_TX_HASH_PREFIX_KEY}{b_num:016x}_{tx_num:08x}")
}

/// Decodes a version pointer
///
/// ### Arguments
///
/// * `pointer`    - String to be split and decoded
pub fn decode_version_pointer(pointer: &[u8]) -> (u32, &'static str, &[u8]) {
    let mut it = pointer.split(|c| c == &DB_POINTER_SEPARATOR);
    let cf = it.next().unwrap();
    let (cf, version) = DB_COLS_BC.iter().find(|(v, _)| v.as_bytes() == cf).unwrap();
    let key = it.next().unwrap();
    (*version, cf, key)
}

/// Return an option, emiting a warning for errors converted to
fn ok_or_warn<V, E: fmt::Display>(r: std::result::Result<Option<V>, E>, tag: &str) -> Option<V> {
    r.unwrap_or_else(|e| {
        warn!("{}: {}", tag, e);
        None
    })
}
```