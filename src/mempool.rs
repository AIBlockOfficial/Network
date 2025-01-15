```rust
use crate::block_pipeline::{MiningPipelineItem, MiningPipelineStatus, Participants};
use crate::comms_handler::{CommsError, Event, Node, TcpTlsConfig};
use crate::configurations::{
    ExtraNodeParams, MempoolNodeConfig, MempoolNodeSharedConfig, TlsPrivateInfo,
};
use crate::constants::{DB_PATH, RESEND_TRIGGER_MESSAGES_COMPUTE_LIMIT};
use crate::db_utils::{self, SimpleDb, SimpleDbError, SimpleDbSpec};
use crate::interfaces::{
    BlockStoredInfo, CommonBlockInfo, Contract, DruidDroplet, DruidPool, InitialIssuance,
    MempoolApi, MempoolApiRequest, MempoolInterface, MempoolRequest, MineRequest, MinedBlock,
    MinedBlockExtraInfo, NodeType, PowInfo, ProofOfWork, Response, StorageRequest, TxStatus,
    TxStatusType, UserRequest, UtxoFetchType, UtxoSet, WinningPoWInfo,
};
use crate::mempool_raft::{
    CommittedItem, CoordinatedCommand, MempoolConsensusedRuntimeData, MempoolRaft,
    MempoolRuntimeItem,
};
use crate::raft::RaftCommit;
use crate::threaded_call::{ThreadedCallChannel, ThreadedCallSender};
use crate::tracked_utxo::TrackedUtxoSet;
use crate::utils::{
    apply_mining_tx, check_druid_participants, create_item_asset_tx_from_sig, create_socket_addr,
    format_parition_pow_address, generate_pow_random_num, get_timestamp_now,
    is_timestamp_difference_greater, to_api_keys, to_route_pow_infos, validate_pow_block,
    validate_pow_for_address, ApiKeys, LocalEvent, LocalEventChannel, LocalEventSender,
    ResponseResult, RoutesPoWInfo, StringError,
};
use crate::wallet::{WalletDb, WalletDbError, DB_SPEC};  // Added WalletDb import
use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::{
    error::Error,
    fmt,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::sync::RwLock;
use tokio::task;
use tracing::{debug, error, error_span, info, trace, warn};
use tracing_futures::Instrument;
use tw_chain::primitives::asset::TokenAmount;
use tw_chain::primitives::block::Block;
use tw_chain::primitives::transaction::{GenesisTxHashSpec, Transaction};
use tw_chain::utils::druid_utils::druid_expectations_are_met;
use tw_chain::utils::script_utils::{tx_has_valid_create_script, tx_is_valid};
use tw_chain::utils::transaction_utils::construct_tx_hash;

/// Key for local miner list
pub const REQUEST_LIST_KEY: &str = "RequestListKey";
pub const USER_NOTIFY_LIST_KEY: &str = "UserNotifyListKey";
pub const POW_RANDOM_NUM_KEY: &str = "PowRandomNumKey";
pub const POW_PREV_RANDOM_NUM_KEY: &str = "PowPreviousRandomNumKey";
pub const RAFT_KEY_RUN: &str = "RaftKeyRun";

/// Database columns
pub const DB_COL_INTERNAL: &str = "internal";
pub const DB_COL_LOCAL_TXS: &str = "local_transactions";

pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
    db_path: DB_PATH,
    suffix: ".mempool",
    columns: &[DB_COL_INTERNAL, DB_COL_LOCAL_TXS],
};

/// Result wrapper for mempool errors
pub type Result<T> = std::result::Result<T, MempoolError>;

#[derive(Debug)]
pub enum MempoolError {
    ConfigError(&'static str),
    Network(CommsError),
    DbError(SimpleDbError),
    Serialization(bincode::Error),
    AsyncTask(task::JoinError),
    GenericError(StringError),
    WalletError(WalletDbError),  // Added WalletError variant
}

impl fmt::Display for MempoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {err}"),
            Self::Network(err) => write!(f, "Network error: {err}"),
            Self::DbError(err) => write!(f, "DB error: {err}"),
            Self::AsyncTask(err) => write!(f, "Async task error: {err}"),
            Self::Serialization(err) => write!(f, "Serialization error: {err}"),
            Self::GenericError(err) => write!(f, "Generic error: {err}"),
            Self::WalletError(err) => write!(f, "Wallet error: {err}"), // Added WalletError display implementation
        }
    }
}

impl Error for MempoolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::DbError(ref e) => Some(e),
            Self::AsyncTask(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
            Self::GenericError(ref e) => Some(e),
            Self::WalletError(ref e) => Some(e), // Added WalletError source implementation
        }
    }
}

impl From<CommsError> for MempoolError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<SimpleDbError> for MempoolError {
    fn from(other: SimpleDbError) -> Self {
        Self::DbError(other)
    }
}

impl From<bincode::Error> for MempoolError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

impl From<task::JoinError> for MempoolError {
    fn from(other: task::JoinError) -> Self {
        Self::AsyncTask(other)
    }
}

impl From<StringError> for MempoolError {
    fn from(other: StringError) -> Self {
        Self::GenericError(other)
    }
}

#[derive(Debug)]
pub struct MempoolNode {
    shared_config: MempoolNodeSharedConfig,
    received_shared_config: Option<MempoolNodeSharedConfig>,
    received_runtime_data: Option<MempoolConsensusedRuntimeData>,
    node: Node,
    node_raft: MempoolRaft,
    db: SimpleDb,
    local_events: LocalEventChannel,
    b_num_to_pause: Option<u64>,
    pause_node: Arc<RwLock<bool>>,
    disable_trigger_messages: Arc<RwLock<bool>>,
    threaded_calls: ThreadedCallChannel<dyn MempoolApi>,
    jurisdiction: String,
    current_mined_block: Option<MinedBlock>,
    druid_pool: DruidPool,
    previous_random_num: Vec<u8>,
    current_random_num: Vec<u8>,
    current_trigger_messages_count: usize,
    enable_trigger_messages_pipeline_reset: bool,
    miners_changed: bool,
    partition_full_size: usize,
    request_list: BTreeSet<SocketAddr>,
    request_list_first_flood: Option<usize>,
    miner_removal_list: Arc<RwLock<BTreeSet<SocketAddr>>>,
    storage_addr: SocketAddr,
    sanction_list: Vec<String>,
    user_notification_list: BTreeSet<SocketAddr>,
    coordinated_shutdown: u64,
    shutdown_group: BTreeSet<SocketAddr>,
    fetched_utxo_set: Option<(SocketAddr, NodeType, UtxoSet)>,
    tx_status_list: BTreeMap<String, TxStatus>,
    tx_status_lifetime: i64,
    api_info: (
        SocketAddr,
        Option<TlsPrivateInfo>,
        ApiKeys,
        RoutesPoWInfo,
        Node,
    ),
    init_issuances: Vec<InitialIssuance>,
    wallet_db: WalletDb,  // Added WalletDb member
}

impl MempoolNode {
    /// Generates a new mempool node instance
    /// ### Arguments
    /// * `config` - MempoolNodeConfig for the current mempool node containing mempool nodes and storage nodes
    /// * `extra`  - additional parameter for construction
    pub async fn new(config: MempoolNodeConfig, mut extra: ExtraNodeParams) -> Result<Self> {
        let raw_addr = config
            .mempool_nodes
            .get(config.mempool_node_idx)
            .ok_or(MempoolError::ConfigError("Invalid mempool index"))?;
        let addr = create_socket_addr(&raw_addr.address).await.map_err(|_| {
            MempoolError::ConfigError("Invalid mempool node address in config file")
        })?;

        let init_issuances = config.initial_issuances.clone();
        let raw_storage_addr = config
            .storage_nodes
            .get(config.mempool_node_idx)
            .ok_or(MempoolError::ConfigError("Invalid storage index"))?;
        let storage_addr = create_socket_addr(&raw_storage_addr.address)
            .await
            .map_err(|_| {
                MempoolError::ConfigError("Invalid storage node address in config file")
            })?;

        let tcp_tls_config = TcpTlsConfig::from_tls_spec(addr, &config.tls_config)?;
        let api_addr = SocketAddr::new(addr.ip(), config.mempool_api_port);
        let api_tls_info = config
            .mempool_api_use_tls
            .then(|| tcp_tls_config.clone_private_info());

        let node = Node::new(
            &tcp_tls_config,
            config.peer_limit,
            config.sub_peer_limit,
            NodeType::Mempool,
            false,
            true,
        )
        .await?;
        let node_raft = MempoolRaft::new(&config, extra.raft_db.take()).await;

        if config.backup_restore.unwrap_or(false) {
            db_utils::restore_file_backup(config.mempool_db_mode, &DB_SPEC, None).unwrap();
        }
        let db = db_utils::new_db(config.mempool_db_mode, &DB_SPEC, extra.db.take(), None);
        let shutdown_group = {
            let storage = std::iter::once(storage_addr);
            let raft_peers = node_raft.raft_peer_addrs().copied();
            raft_peers.chain(storage).collect()
        };

        // Initialize WalletDb
        let wallet_db = WalletDb::new(DB_SPEC, extra.wallet_db.take()).await.map_err(MempoolError::WalletError)?;  // Added WalletDb initialization

        let api_pow_info = to_route_pow_infos(config.routes_pow.clone());
        let api_keys = to_api_keys(config.api_keys.clone());
        let enable_trigger_messages_pipeline_reset = config
            .enable_trigger_messages_pipeline_reset
            .unwrap_or(false);
        let api_info = (api_addr, api_tls_info, api_keys, api_pow_info, node.clone());

        let shared_config = MempoolNodeSharedConfig {
            mempool_mining_event_timeout: config.mempool_mining_event_timeout,
            mempool_partition_full_size: config.mempool_partition_full_size,
            mempool_miner_whitelist: config.mempool_miner_whitelist,
        };

        if config.sub_peer_limit > config.peer_limit {
            return Err(MempoolError::ConfigError(
                "Sub peer limit cannot be greater than peer limit",
            ));
        }

        MempoolNode {
            node,
            node_raft,
            db,
            shared_config,
            received_shared_config: Default::default(),
            received_runtime_data: Default::default(),
            local_events: Default::default(),
            pause_node: Default::default(),
            b_num_to_pause: Default::default(),
            disable_trigger_messages: Default::default(),
            threaded_calls: Default::default(),
            current_mined_block: None,
            druid_pool: Default::default(),
            current_trigger_messages_count: Default::default(),
            enable_trigger_messages_pipeline_reset,
            previous_random_num: Default::default(),
            current_random_num: Default::default(),
            miner_removal_list: Default::default(),
            miners_changed: false,
            request_list: Default::default(),
            sanction_list: config.sanction_list,
            jurisdiction: config.jurisdiction,
            request_list_first_flood: Some(config.mempool_minimum_miner_pool_len),
            partition_full_size: config.mempool_partition_full_size,
            storage_addr,
            user_notification_list: Default::default(),
            coordinated_shutdown: u64::MAX,
            shutdown_group,
            api_info,
            fetched_utxo_set: None,
            init_issuances,
            tx_status_list: Default::default(),
            tx_status_lifetime: config.tx_status_lifetime,
            wallet_db,  // Added WalletDb to the constructor
        }
        .load_local_db()
    }

    // ... rest of the code remains unchanged ...
}
```