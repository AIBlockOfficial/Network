use crate::block_pipeline::{MiningPipelineItem, MiningPipelineStatus, Participants};
use crate::comms_handler::{CommsError, Event, Node, TcpTlsConfig};
use crate::interfaces::{
    NodeType, Response, BlockStoredInfo, CommonBlockInfo, Contract, DruidDroplet, DruidPool, InitialIssuance,
    MempoolApi, MempoolApiRequest, MempoolInterface, MempoolRequest, MineRequest, MinedBlock,
    MinedBlockExtraInfo, UtxoSet, UtxoFetchType,
    WinningPoWInfo, PowInfo, ProofOfWork, TxStatus, TxStatusType, UserRequest, StorageRequest,
};
use crate::key_creation::PeerInfo;
use crate::configurations::{
    ExtraNodeParams, MempoolNodeConfig, MempoolNodeSharedConfig, TlsPrivateInfo,
};
use crate::constants::{DB_PATH, RESEND_TRIGGER_MESSAGES_COMPUTE_LIMIT};
use crate::db_utils::{self, SimpleDb, SimpleDbError, SimpleDbSpec};
use crate::mempool_raft::{
    CommittedItem, CoordinatedCommand, MempoolConsensusedRuntimeData, MempoolRaft,
    MempoolRuntimeItem, MempoolRaftItem,
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
use rand::Rng;

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
        }
        .load_local_db()
    }

    /// Get all connected miners
    pub async fn get_connected_miners(&self) -> Vec<SocketAddr> {
        self.node
            .get_peers()
            .await
            .into_iter()
            .filter_map(|(address, node_type)| {
                if node_type == Some(NodeType::Miner) {
                    Some(address)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Determine whether or not this mempool node's trigger messages are disabled
    pub async fn trigger_messages_disabled(&self) -> bool {
        *self.disable_trigger_messages.read().await
    }

    /// Determine whether or not this mempool node is paused
    pub async fn is_paused(&self) -> bool {
        *self.pause_node.read().await
    }

    /// Propose a coordinated pause of all mempool nodes
    pub async fn propose_pause_nodes(&mut self, b_num: u64) {
        self.node_raft.propose_pause_nodes(b_num).await;
    }

    /// Propose a coordinated resume of all mempool nodes
    pub async fn propose_resume_nodes(&mut self) {
        self.node_raft.propose_resume_nodes().await;
    }

    /// Propose a coordinated pause/resume of all mempool nodes
    pub async fn propose_apply_shared_config(&mut self) {
        self.node_raft.propose_apply_shared_config().await;
    }

    /// Returns the mempool node's local endpoint.
    pub fn local_address(&self) -> SocketAddr {
        self.node.local_address()
    }

    /// Returns the mempool node's public endpoint.
    pub async fn public_address(&self) -> Option<SocketAddr> {
        self.node.public_address().await
    }

    /// Get the node's mined block if any
    pub fn get_current_mined_block(&self) -> &Option<MinedBlock> {
        &self.current_mined_block
    }

    /// Injects a new event into mempool node
    pub fn inject_next_event(
        &self,
        from_peer_addr: SocketAddr,
        data: impl Serialize,
    ) -> Result<()> {
        Ok(self.node.inject_next_event(from_peer_addr, data)?)
    }

    /// Connect info for peers on the network.
    pub fn connect_info_peers(&self) -> (Node, Vec<SocketAddr>, Vec<SocketAddr>) {
        let storage = Some(self.storage_addr);
        let to_connect = self.node_raft.raft_peer_to_connect().chain(storage.iter());
        let expect_connect = self.node_raft.raft_peer_addrs().chain(storage.iter());
        (
            self.node.clone(),
            to_connect.copied().collect(),
            expect_connect.copied().collect(),
        )
    }

    /// Send initial requests:
    /// - Fetch runtime data from peers
    pub async fn send_startup_requests(&mut self) -> Result<()> {
        let mempool_peers = self.node_raft.raft_peer_addrs().copied();
        if let Err(e) = self
            .node
            .send_to_all(mempool_peers, MempoolRequest::RequestRuntimeData)
            .await
        {
            error!("Failed to send RequestRuntimeData to mempool peers: {}", e);
        }
        Ok(())
    }

    /// Local event channel.
    pub fn local_event_tx(&self) -> &LocalEventSender {
        &self.local_events.tx
    }

    /// Threaded call channel.
    pub fn threaded_call_tx(&self) -> &ThreadedCallSender<dyn MempoolApi> {
        &self.threaded_calls.tx
    }

    /// The current utxo_set including block being mined and previous block mining txs.
    pub fn get_committed_utxo_set(&self) -> &UtxoSet {
        self.node_raft.get_committed_utxo_set()
    }

    pub fn get_committed_utxo_set_to_send(&self) -> UtxoSet {
        self.node_raft.get_committed_utxo_set().clone()
    }

    pub fn get_committed_utxo_tracked_set_to_send(&self) -> TrackedUtxoSet {
        self.node_raft.get_committed_utxo_tracked_set().clone()
    }

    /// The current tx_pool that will be used to generate next block
    pub fn get_committed_tx_pool(&self) -> &BTreeMap<String, Transaction> {
        self.node_raft.get_committed_tx_pool()
    }

    /// The current tx_druid_pool that will be used to generate next block
    pub fn get_committed_tx_druid_pool(&self) -> &Vec<BTreeMap<String, Transaction>> {
        self.node_raft.get_committed_tx_druid_pool()
    }

    /// The current local DRUID transactions present in the `local_tx_druid_pool`
    pub fn get_local_druid_pool(&self) -> &Vec<BTreeMap<String, Transaction>> {
        self.node_raft.get_local_tx_druid_pool()
    }

    /// The current druid pool of pending DDE transactions
    pub fn get_pending_druid_pool(&self) -> &DruidPool {
        &self.druid_pool
    }

    /// Get request list
    pub fn get_request_list(&self) -> &BTreeSet<SocketAddr> {
        &self.request_list
    }

    /// Get a clone of `pk_cache` element of `TrackedUtxoSet`
    ///
    /// ## NOTE
    ///
    /// Only used during tests
    #[cfg(test)]
    pub fn get_pk_cache(
        &self,
    ) -> std::collections::HashMap<String, BTreeSet<tw_chain::primitives::transaction::OutPoint>>
    {
        self.node_raft.get_committed_utxo_tracked_pk_cache()
    }

    /// Remove a `pk_cache` entry from `TrackedUtxoSet`
    ///
    /// ## Arguments
    ///
    /// * `entry` - The entry to remove
    ///
    /// ## NOTE
    ///
    /// Only used during tests
    #[cfg(test)]
    pub fn remove_pk_cache_entry(&mut self, entry: &str) {
        self.node_raft.committed_utxo_remove_pk_cache(entry);
    }

    /// Return the raft loop to spawn in it own task.
    pub fn raft_loop(&self) -> impl Future<Output = ()> {
        self.node_raft.raft_loop()
    }

    /// Signal to the raft loop to complete
    pub async fn close_raft_loop(&mut self) {
        self.node_raft.close_raft_loop().await
    }

    /// Extract persistent dbs
    pub async fn take_closed_extra_params(&mut self) -> ExtraNodeParams {
        let raft_db = self.node_raft.take_closed_persistent_store().await;
        ExtraNodeParams {
            db: self.db.take().in_memory(),
            raft_db: raft_db.in_memory(),
            ..Default::default()
        }
    }

    /// Backup persistent dbs
    pub async fn backup_persistent_dbs(&mut self) {
        if self.node_raft.need_backup() {
            if let Err(e) = self.db.file_backup() {
                error!("Error bakup up main db: {:?}", e);
            }
        }
    }

    /// Info needed to run the API point.
    pub fn api_inputs(
        &self,
    ) -> (
        SocketAddr,
        Option<TlsPrivateInfo>,
        ApiKeys,
        RoutesPoWInfo,
        Node,
    ) {
        self.api_info.clone()
    }

    /// Validate and get DDE transactions that are ready to be added to the RAFT
    ///
    /// ### Arguments
    ///
    /// * `transactions` - DDE transactions to process
    pub fn validate_dde_txs(
        &mut self,
        transactions: BTreeMap<String, Transaction>,
    ) -> Vec<(bool, BTreeMap<String, Transaction>)> {
        let mut ready_txs = Vec::new();

        for (tx_hash, tx) in transactions {
            if let Some(druid_info) = tx.druid_info.clone() {
                let druid = druid_info.druid;
                let participants = druid_info.participants;

                let droplet = self
                    .druid_pool
                    .entry(druid.clone())
                    .or_insert_with(|| DruidDroplet::new(participants));

                droplet.txs.insert(tx_hash.clone(), tx);

                // Execute the DDE tx if it's ready
                if droplet.txs.len() == droplet.participants {
                    let valid = druid_expectations_are_met(&druid, droplet.txs.values())
                        && check_druid_participants(droplet);
                    ready_txs.push((valid, droplet.txs.clone()));

                    // TODO: Implement time-based removal?
                    self.druid_pool.remove(&druid);
                }
            }
        }
        ready_txs
    }

    /// Returns the mining block from the node_raft
    pub fn get_mining_block(&self) -> &Option<Block> {
        self.node_raft.get_mining_block()
    }

    /// Get mining participants selected
    pub fn get_mining_participants(&self) -> &Participants {
        self.node_raft.get_mining_participants()
    }

    ///Returns last generated block number from the node_raft
    pub fn get_committed_current_block_num(&self) -> Option<u64> {
        self.node_raft.get_committed_current_block_num()
    }

    /// Process block generation in single step (Test only)
    /// ### Arguments
    /// * `block`    - Block to be set to commited mining block
    /// * `block_tx` - BTreeMap of the block transactions
    pub fn test_skip_block_gen(&mut self, block: Block, block_tx: BTreeMap<String, Transaction>) {
        self.node_raft.test_skip_block_gen(block, block_tx)
    }

    /// Process all the mining phase in a single step (Test only)
    /// ### Arguments
    /// * `winning_pow`    - Address nad PoW for the winner to use
    pub fn test_skip_mining(&mut self, winning_pow: (SocketAddr, WinningPoWInfo), seed: Vec<u8>) {
        self.node_raft.test_skip_mining(winning_pow, seed);
        self.mining_block_mined();
    }

    /// Return closure use to validate a transaction
    fn transactions_validator(&mut self) -> impl Fn(&Transaction) -> (bool, String) + '_ {
        let utxo_set = self.node_raft.get_committed_utxo_set();
        let lock_expired = self
            .node_raft
            .get_committed_current_block_num()
            .unwrap_or_default();
        //let sanction_list = &self.sanction_list;
        let b_num = self
            .node_raft
            .get_committed_current_block_num()
            .unwrap_or_default();

        move |tx| {
            if tx.is_create_tx() {
                let is_valid = tx_has_valid_create_script(
                    &tx.inputs[0].script_signature,
                    &tx.outputs[0].value,
                );

                return match is_valid {
                    true => (true, "Create script is valid".to_string()),
                    false => (false, "Create script is invalid".to_string()),
                };
            }

            let (is_valid, validity_info) = tx_is_valid(tx, b_num, |v| {
                utxo_set
                    .get(v)
                    // .filter(|_| !sanction_list.contains(&v.t_hash))
                    .filter(|tx_out| lock_expired >= tx_out.locktime)
            });

            (!tx.is_coinbase() && is_valid, validity_info)
        }
    }

    /// Sends the latest block to storage
    pub async fn send_block_to_storage(&mut self) -> Result<()> {
        let mined_block = self.current_mined_block.clone();
        
        // Check if we have a mined block to send
        if mined_block.is_none() {
            info!("No mined block available to send to storage");
            return Err(MempoolError::GenericError(StringError(
                "No mined block available to send to storage".to_string()
            )));
        }
        
        // Log block details
        if let Some(block) = &mined_block {
            info!("Preparing to send mined block: block_num={}, hash={}", 
                  block.common.block.header.b_num,
                  block.common.block.header.previous_hash.clone().unwrap_or("".to_string()));
        }

        // Flush stale tx status
        self.flush_stale_tx_status();

        // Check if storage node is connected
        if !self.is_storage_connected().await {
            error!("Storage node is not connected, cannot send block to storage");
            return Err(MempoolError::GenericError(StringError(
                format!("Storage node at {} is not connected", self.storage_addr)
            )));
        }

        info!("");
        info!("Proposing timestamp next");
        info!("");
        self.node_raft.propose_timestamp().await;
        
        info!("Sending block to storage at {}", self.storage_addr);
        
        // Log the current peers to see if the storage is connected
        let peers = self.node.get_peers().await;
        info!("Current connected peers: {:?}", peers.keys().collect::<Vec<_>>());
        
        if !peers.contains_key(&self.storage_addr) {
            warn!("Storage at {} is not in the connected peers list!", self.storage_addr);
        }
        
        match self.node
            .send(self.storage_addr, StorageRequest::SendBlock { mined_block })
            .await {
                Ok(_) => info!("Successfully sent block to storage at {}", self.storage_addr),
                Err(e) => {
                    error!("Failed to send block to storage at {}: {:?}", self.storage_addr, e);
                    return Err(e.into());
                }
            }
        Ok(())
    }

    /// Check if the storage node is connected
    pub async fn is_storage_connected(&self) -> bool {
        let peers = self.node.get_peers().await;
        
        // Check if the storage address is in the peers list
        if !peers.contains_key(&self.storage_addr) {
            error!("Storage node at {} is not in the peers list", self.storage_addr);
            return false;
        }
        
        true
    }

    /// Floods all peers with a PoW for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_pow_to_peers(&self, _address: SocketAddr, _pow: &[u8]) {
        error!("Flooding PoW to peers not implemented");
    }

    /// Floods all peers with a PoW commit for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `_commit` - POW reference (&ProofOfWork)
    pub fn flood_commit_to_peers(&self, _address: SocketAddr, _commit: &ProofOfWork) {
        error!("Flooding commit to peers not implemented");
    }

    /// Sends the requested UTXO set to the peer that requested it.
    pub async fn send_fetched_utxo_set(&mut self) -> Result<()> {
        if let Some((peer, node_type, utxo_set)) = self.fetched_utxo_set.take() {
            match node_type {
                NodeType::Miner => {
                    self.node
                        .send(peer, MineRequest::SendUtxoSet { utxo_set })
                        .await?
                }
                NodeType::User => {
                    let b_num = self
                        .node_raft
                        .get_committed_current_block_num()
                        .unwrap_or_default();

                    self.node
                        .send(peer, UserRequest::SendUtxoSet { utxo_set, b_num })
                        .await?
                }
                _ => {
                    return Err(MempoolError::GenericError(StringError(
                        "Invalid UTXO requester".to_string(),
                    )))
                }
            }
        }
        Ok(())
    }

    /// Listens for new events from peers and handles them, processing any errors.
    pub async fn handle_next_event_response(
        &mut self,
        response: Result<Response>,
    ) -> ResponseResult {
        // debug!("Response: {:?}", response);

        match response {
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received UTXO fetch request" => {
                if let Err(e) = self.send_fetched_utxo_set().await {
                    error!("Requested UTXO set not sent {:?}", e);
                }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received coordinated pause request" => {
                debug!("Received coordinated pause request");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Node pause configuration set" => {
                debug!("Node pause configuration set");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received coordinated resume request" => {
                debug!("Received coordinated resume request");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Node resumed" => {
                warn!("NODE RESUMED");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received shared config" => {
                debug!("Shared config received");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Shared config applied" => {
                debug!("Shared config applied");
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "No shared config to apply" => {
                warn!("No shared config to apply");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Miner removal request received" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Shutdown" => {
                warn!("Shutdown now");
                return ResponseResult::Exit;
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Shutdown pending" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Start coordinated shutdown" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received partition request successfully" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received first full partition request" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Removing unauthorized miner" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received PoW successfully" => {
                debug!("Proposing winning PoW entry");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Winning PoW intake open" => {
                debug!(
                    "Block and participants ready to mine: {:?}",
                    self.get_mining_block()
                );
                info!(
                    "No. of connected miners: {:?}",
                    self.get_connected_miners().await.len()
                );
                self.flood_rand_and_block_to_partition().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Pipeline halted" => {
                info!("Send Block to storage");
                debug!("CURRENT MINED BLOCK: {:?}", self.current_mined_block);
                if let Err(e) = self.send_block_to_storage().await {
                    error!("Block not sent to storage {:?}", e);
                }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Pipeline reset" => {
                warn!(
                    "Pipeline reset to :{:?}",
                    self.node_raft.get_mining_pipeline_status()
                );
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Sent runtime data to peer" => {}
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Failed to send runtime data to peer" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received runtime data from peer" => {
                debug!("Received runtime data from peer");
                if let Some(runtime_data) = self.received_runtime_data.take() {
                    let mining_api_keys = runtime_data.mining_api_keys.into_iter().collect();
                    self.node_raft
                        .propose_runtime_item(MempoolRuntimeItem::AddMiningApiKeys(mining_api_keys))
                        .await;
                } else {
                    warn!("Runtime data received from peer is empty");
                }
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Received runtime data from unknown peer" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Transactions added to tx pool" => {
                debug!("Transactions received and processed successfully");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "First Block committed" => {
                // Only continue with the mining process if the node is not paused
                debug!("First block committed, mining process will proceed based on state transitions.");
                // if !self.is_paused().await {
                //     debug!("First block ready to mine: {:?}", self.get_mining_block());
                //     self.flood_block_to_users().await.unwrap();
                //     self.flood_rand_and_block_to_partition().await.unwrap()
                // } else {
                //     warn!("NODE PAUSED");
                //     // Disable trigger messages to ensure mining cannot take place
                //     *self.disable_trigger_messages.write().await = true;
                // }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Block committed" => {
                // Only continue with the mining process if the node is not paused
                debug!("Block committed, mining process will proceed based on state transitions.");
                // if !self.is_paused().await {
                //     debug!("Block ready to mine: {:?}", self.get_mining_block());
                //     self.flood_block_to_users().await.unwrap();
                //     self.flood_rand_and_block_to_partition().await.unwrap()
                // } else {
                //     warn!("NODE PAUSED");
                //     // Disable trigger messages to ensure mining cannot take place
                //     *self.disable_trigger_messages.write().await = true;
                // }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Block shutdown" => {
                debug!("Block shutdown (not ready to mine)");
                self.flood_closing_events().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Transactions committed" => {
                debug!("Transactions ready to be used in next block");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received block stored" => {
                info!("Block info received from storage: ready to generate block");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Snapshot applied" => {
                warn!("Snapshot applied");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received block notification" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Partition PoW received successfully" => {}
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Sent startup requests on reconnection" => {
                debug!("Sent startup requests on reconnection")
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Failed to send startup requests on reconnection" => {
                error!("Failed to send startup requests on reconnection")
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Partition list complete" => {}
            Ok(Response {
                success: false,
                reason,
            }) if reason == "PoW received is invalid" => {}
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Not block currently mined" => {}
            Ok(Response {
                success: true,
                reason,
            }) => {
                error!("UNHANDLED RESPONSE TYPE: {:?}", reason);
            }
            Ok(Response {
                success: false,
                reason,
            }) => {
                error!("WARNING: UNHANDLED RESPONSE TYPE FAILURE: {:?}", reason);
            }
            Err(error) => {
                error!("ERROR HANDLING RESPONSE: {:?}", error);
            }
        };

        ResponseResult::Continue
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event<E: Future<Output = &'static str> + Unpin>(
        &mut self,
        exit: &mut E,
    ) -> Option<Result<Response>> {
        loop {
            let ready = !self.node_raft.need_initial_state();
            let shutdown = self.node_raft.is_shutdown_commit_processed();

            // State machines are not keept between iterations or calls.
            // All selection calls (between = and =>), need to be dropable
            // i.e they should only await a channel.
            tokio::select! {
                event = self.node.next_event(), if ready => {
                    trace!("handle_next_event evt {:?}", event);
                    if let res @ Some(_) = self.handle_event(event?).await.transpose() {
                        return res;
                    }
                }
                Some(commit_data) = self.node_raft.next_commit(), if !shutdown => {
                    trace!("handle_next_event commit {:?}", commit_data);
                    if let res @ Some(_) = self.handle_committed_data(commit_data).await {
                        return res;
                    }
                }
                Some((addr, msg)) = self.node_raft.next_msg(), if ready => {
                    trace!("handle_next_event msg {:?}: {:?}", addr, msg);
                    match self.node.send(
                        addr,
                        MempoolRequest::SendRaftCmd(msg)).await {
                            Err(e) => info!("Msg not sent to {}, from {}: {:?}", addr, self.local_address(), e),
                            Ok(()) => trace!("Msg sent to {}, from {}", addr, self.local_address()),
                        };
                }
                _ = self.node_raft.timeout_propose_transactions(), if ready && !shutdown => {
                    trace!("handle_next_event timeout transactions");
                    self.node_raft.propose_local_transactions_at_timeout().await;
                    self.node_raft.propose_local_druid_transactions().await;
                }
                _ = self.node_raft.timeout_propose_mining_event(), if ready && !shutdown => {
                    trace!("handle_next_event timeout mining pipeline");
                    if !self.node_raft.propose_mining_event_at_timeout().await
                        && !self.trigger_messages_disabled().await {
                        self.node_raft.re_propose_uncommitted_current_b_num().await;
                        self.resend_trigger_message().await;
                    } else {
                        // Reset trigger messages count
                        self.current_trigger_messages_count = Default::default();
                    }
                }
                Some(event) = self.local_events.rx.recv(), if ready => {
                    if let Some(res) = self.handle_local_event(event).await {
                        return Some(Ok(res));
                    }
                }
                Some(f) = self.threaded_calls.rx.recv(), if ready => {
                    f(self);
                }
                reason = &mut *exit => return Some(Ok(Response {
                    success: true,
                    reason: reason.to_string(),
                }))
            }
        }
    }

    /// Handle commit data
    ///
    /// ### Arguments
    ///
    /// * `commit_data` - Commit to process.
    async fn handle_committed_data(&mut self, commit_data: RaftCommit) -> Option<Result<Response>> {
        let committed_item = self.node_raft.received_commit(commit_data).await;

        // Process the committed item returned by the raft layer
        let response = match committed_item {
            Some(CommittedItem::FirstBlock) => {
                self.backup_persistent_dbs().await;
                info!("FirstBlock committed. Triggering participant proposal.");
                // ---> ADDED: Propose waiting miners after FirstBlock commit <--- Corrected approach
                let current_block_num = 0; // First block is always 0
                let request_list_clone = self.request_list.clone(); // Clone to avoid borrow issues
                if !request_list_clone.is_empty() {
                    info!("Processing request list for Block {}", current_block_num);
                    self.propose_miners_from_request_list(current_block_num, &request_list_clone).await;
                }
                // ---> END ADDED <--- Corrected approach
                Some(Ok(Response {
                    success: true,
                    reason: "First Block committed".to_owned(),
                }))
            }
            Some(CommittedItem::Block) => {
                let b_num = self.node_raft.get_committed_current_block_num().unwrap_or(0); // Get the committed block number
                self.backup_persistent_dbs().await;
                info!("Block {} committed. Triggering participant proposal.", b_num);
                 // ---> ADDED: Propose waiting miners after Block commit <--- Corrected approach
                 let next_block_num = b_num + 1; // Corrected: Propose for the *next* block number
                 let request_list_clone = self.request_list.clone(); // Clone to avoid borrow issues
                 if !request_list_clone.is_empty() {
                     info!("Processing request list for Block {}", next_block_num);
                     self.propose_miners_from_request_list(next_block_num, &request_list_clone).await;
                 }
                 // ---> END ADDED <--- Corrected approach

                // Reset the process AFTER handling the Block commit itself.
                self.reset_mining_block_process().await;

                Some(Ok(Response {
                    success: true,
                    reason: "Block committed".to_owned(),
                }))
            }
            Some(CommittedItem::BlockShutdown) => {
                self.reset_mining_block_process().await;
                self.backup_persistent_dbs().await;
                Some(Ok(Response {
                    success: true,
                    reason: "Block shutdown".to_owned(),
                }))
            }
            Some(CommittedItem::StartPhasePowIntake) => {
                // RESTORED IMMEDIATE FLOOD CALL: Trigger flood *after* state transition is committed.
                info!("StartPhasePowIntake commit received, state transitioned. Triggering flood.");
                if let Err(e) = self.flood_rand_and_block_to_partition().await {
                    error!("Failed to flood block/rand on StartPhasePowIntake commit: {:?}", e);
                    // Log error, but allow pipeline to proceed. Flood function logs internally.
                }
                // Return the standard response indicating the phase transition occurred.
                Some(Ok(Response {
                    success: true,
                    reason: "Winning PoW intake open".to_owned(), // Ensure this line is correct and only one reason field exists
                }))
            }
            Some(CommittedItem::StartPhaseHalted) => {
                self.mining_block_mined(); // RESTORED: Populate self.current_mined_block before attempting to send/reset.
                // Reset is handled later in receive_block_stored after storage confirmation.
                // self.reset_mining_block_process().await; // Keep this removed
                Some(Ok(Response {
                    success: true,
                    reason: "Pipeline halted".to_owned(),
                }))
            }
            Some(CommittedItem::ResetPipeline) => Some(Ok(Response {
                success: true,
                reason: "Pipeline reset".to_owned(),
            })),
            Some(CommittedItem::Transactions) => {
                delete_local_transactions(
                    &mut self.db,
                    &self.node_raft.take_local_tx_hash_last_commited(),
                );
                Some(Ok(Response {
                    success: true,
                    reason: "Transactions committed".to_owned(),
                }))
            }
            Some(CommittedItem::Snapshot) => {
                // Override values updated by the snapshot with values from the config
                #[cfg(feature = "config_override")]
                self.apply_shared_config(self.shared_config.clone()).await;

                Some(Ok(Response {
                    success: true,
                    reason: "Snapshot applied".to_owned(),
                }))
            }
            // Handle the new variant
            Some(CommittedItem::PipelineProgress) => {
                debug!("Pipeline progress commit received (no phase change).");
                None // No specific response needed for the main loop
            }
            Some(CommittedItem::CoordinatedCmd(cmd)) => self.handle_coordinated_cmd(cmd).await,
            None => None,
        };

        response
    }

    ///Handle a local event
    ///
    /// ### Arguments
    ///
    /// * `event` - Event to process.
    async fn handle_local_event(&mut self, event: LocalEvent) -> Option<Response> {
        match event {
            LocalEvent::Exit(reason) => Some(Response {
                success: true,
                reason: reason.to_string(),
            }),
            LocalEvent::ReconnectionComplete => {
                if let Err(err) = self.send_startup_requests().await {
                    error!("Failed to send startup requests on reconnect: {}", err);
                    return Some(Response {
                        success: false,
                        reason: "Failed to send startup requests on reconnection".to_owned(),
                    });
                }
                Some(Response {
                    success: true,
                    reason: "Sent startup requests on reconnection".to_owned(),
                })
            }
            LocalEvent::SendBlockToStorage => {
                info!("Handling SendBlockToStorage event");
                match self.send_block_to_storage().await {
                    Ok(_) => {
                        // Check if we actually had a block to send
                        if self.current_mined_block.is_some() {
                            info!("Successfully sent block to storage");
                            Some(Response {
                                success: true,
                                reason: "Block sent to storage".to_owned(),
                            })
                        } else {
                            // No block was available to send
                            info!("No block was available to send to storage");
                            Some(Response {
                                success: true,
                                reason: "No block available to send to storage".to_owned(),
                            })
                        }
                    },
                    Err(e) => {
                        error!("Failed to send block to storage: {:?}", e);
                        Some(Response {
                            success: false,
                            reason: "Failed to send block to storage".to_owned(),
                        })
                    }
                }
            }
            LocalEvent::CoordinatedShutdown(shutdown) => {
                self.coordinated_shutdown = shutdown;
                Some(Response {
                    success: true,
                    reason: "Start coordinated shutdown".to_owned(),
                })
            }
            LocalEvent::Ignore => None,
        }
    }

    /// Haddles errors or events that are passed
    ///
    /// ### Arguments
    ///
    /// * `event` - Event holding the frame to be handled
    async fn handle_event(&mut self, event: Event) -> Result<Option<Response>> {
        match event {
            Event::NewFrame { peer, frame } => {
                let peer_span = error_span!("peer", ?peer);
                self.handle_new_frame(peer, frame)
                    .instrument(peer_span)
                    .await
            }
        }
    }

    /// Hanldes a new incoming message from a peer.
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    /// * 'frame' - Bytes representing the new frame.
    async fn handle_new_frame(
        &mut self,
        peer: SocketAddr,
        frame: Bytes,
    ) -> Result<Option<Response>> {
        let req = deserialize::<MempoolRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        debug!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a mempool request.
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    /// * 'req' - MempoolRequest object holding the request
    async fn handle_request(&mut self, peer: SocketAddr, req: MempoolRequest) -> Option<Response> {
        use MempoolRequest::*;
        trace!("handle_request");

        match req {
            MempoolApi(req) => self.handle_api_request(peer, req).await,
            SendUtxoRequest {
                address_list,
                requester_node_type,
            } => Some(self.fetch_utxo_set(peer, address_list, requester_node_type)),
            SendBlockStored(info) => self.receive_block_stored(peer, info).await,
            SendPoW {
                block_num,
                nonce,
                coinbase,
            } => self.receive_pow(peer, block_num, nonce, coinbase).await,
            SendPartitionEntry {
                pow_info,
                partition_entry,
            } => {
                self.receive_partition_entry(peer, pow_info, partition_entry)
                    .await
            }
            SendTransactions { transactions } => Some(self.receive_transactions(transactions)),
            SendUserBlockNotificationRequest => {
                Some(self.receive_block_user_notification_request(peer))
            }
            SendPartitionRequest { mining_api_key } => {
                Some(self.receive_partition_request(peer, mining_api_key).await)
            }
            SendSharedConfig { shared_config } => {
                match peer != self.local_address() && !self.node_raft.get_peers().contains(&peer) {
                    true => None,
                    false => self.handle_shared_config(peer, shared_config).await,
                }
            }
            Closing => self.receive_closing(peer),
            CoordinatedPause { b_num } => {
                match peer != self.local_address() && !self.node_raft.get_peers().contains(&peer) {
                    true => None,
                    false => self.handle_coordinated_pause(peer, b_num).await,
                }
            }
            CoordinatedResume => {
                match peer != self.local_address() && !self.node_raft.get_peers().contains(&peer) {
                    true => None,
                    false => self.handle_coordinated_resume(peer).await,
                }
            }
            RequestRemoveMiner => self.handle_request_remove_miner(peer).await,
            RequestRuntimeData => self.handle_receive_request_runtime_data(peer).await,
            SendRuntimeData { runtime_data } => {
                match peer != self.local_address() && !self.node_raft.get_peers().contains(&peer) {
                    true => None,
                    false => self.handle_receive_runtime_data(peer, runtime_data).await,
                }
            }
            SendRaftCmd(msg) => {
                match peer != self.local_address() && !self.node_raft.get_peers().contains(&peer) {
                    true => None,
                    false => {
                        self.node_raft.received_message(msg).await;
                        None
                    }
                }
            }
            SyncAfterMining { block_num } => {
                // Temporary implementation until handle_sync_after_mining is implemented
                info!("Received sync after mining request for block {}", block_num);
                Some(Response {
                    success: true,
                    reason: "Sync after mining acknowledged".to_owned(),
                })
            }
        }
    }

    async fn handle_receive_runtime_data(
        &mut self,
        peer: SocketAddr,
        runtime_data: MempoolConsensusedRuntimeData,
    ) -> Option<Response> {
        if !self
            .node_raft
            .raft_peer_addrs()
            .collect::<Vec<_>>()
            .contains(&&peer)
        {
            return Some(Response {
                success: false,
                reason: "Received runtime data from unknown peer".to_owned(),
            });
        }

        self.received_runtime_data = Some(runtime_data);
        Some(Response {
            success: true,
            reason: "Received runtime data from peer".to_owned(),
        })
    }

    /// Handles a request to send current runtime data
    async fn handle_receive_request_runtime_data(&mut self, peer: SocketAddr) -> Option<Response> {
        let runtime_data = self.node_raft.get_runtime_data();
        let response = MempoolRequest::SendRuntimeData { runtime_data };
        if let Err(e) = self.node.send(peer, response).await {
            error!("Failed to send runtime data to peer: {}", e);
            return Some(Response {
                success: false,
                reason: "Failed to send runtime data to peer".to_owned(),
            });
        }

        Some(Response {
            success: true,
            reason: "Sent runtime data to peer".to_owned(),
        })
    }

    /// Handles a request to remove a miner
    ///
    /// NOTE: This request is received from a Miner node
    /// who no longer wishes to participate in mining
    async fn handle_request_remove_miner(&mut self, peer: SocketAddr) -> Option<Response> {
        self.miner_removal_list.write().await.insert(peer);
        Some(Response {
            success: true,
            reason: "Miner removal request received".to_owned(),
        })
    }

    /// Handles a coordinated command
    ///
    /// ### Arguments
    ///
    /// *`cmd` - Command to execute
    async fn handle_coordinated_cmd(
        &mut self,
        cmd: CoordinatedCommand,
    ) -> Option<Result<Response>> {
        match cmd {
            CoordinatedCommand::PauseNodes { b_num } => {
                self.b_num_to_pause = Some(b_num);
                warn!("Pausing node at b_num: {b_num}");
                Some(Ok(Response {
                    success: true,
                    reason: "Node pause configuration set".to_owned(),
                }))
            }
            CoordinatedCommand::ResumeNodes => {
                // No longer needed to pause on a specific block
                self.b_num_to_pause = None;
                // Set the pause flag to false
                *self.pause_node.write().await = false;
                // Let the trigger messages be sent again to continue the mining process
                *self.disable_trigger_messages.write().await = false;
                Some(Ok(Response {
                    success: true,
                    reason: "Node resumed".to_owned(),
                }))
            }
            CoordinatedCommand::ApplySharedConfig => {
                if let Some(received_shared_config) = self.received_shared_config.take() {
                    self.apply_shared_config(received_shared_config).await;
                    return Some(Ok(Response {
                        success: true,
                        reason: "Shared config applied".to_owned(),
                    }));
                }
                Some(Ok(Response {
                    success: false,
                    reason: "No shared config to apply".to_owned(),
                }))
            }
        }
    }

    /// Handles an API internal request.
    ///
    /// ### Arguments
    ///
    /// * `peer` - Peer sending the request.
    /// * `req`  - Request to execute
    async fn handle_api_request(
        &mut self,
        peer: SocketAddr,
        req: MempoolApiRequest,
    ) -> Option<Response> {
        use MempoolApiRequest::*;

        if peer != self.local_address() {
            // Do not process if not internal request
            return None;
        }

        match req {
            SendCreateItemRequest {
                item_amount,
                script_public_key,
                public_key,
                signature,
                genesis_hash_spec,
                metadata,
            } => match self.create_item_asset_tx(
                item_amount,
                script_public_key,
                public_key,
                signature,
                genesis_hash_spec,
                metadata,
            ) {
                Ok((tx, _)) => Some(self.receive_transactions(vec![tx])),
                Err(e) => {
                    error!("Error creating item asset transaction: {:?}", e);
                    None
                }
            },
            SendTransactions { transactions } => Some(self.receive_transactions(transactions)),
            PauseNodes { b_num } => Some(self.pause_nodes(b_num)),
            ResumeNodes => Some(self.resume_nodes()),
            SendSharedConfig { shared_config } => Some(self.send_shared_config(shared_config)),
        }
    }

    /// Handles the item of closing event
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    fn receive_closing(&mut self, peer: SocketAddr) -> Option<Response> {
        if !self.shutdown_group.remove(&peer) {
            return None;
        }

        if !self.shutdown_group.is_empty() {
            return Some(Response {
                success: true,
                reason: "Shutdown pending".to_owned(),
            });
        }

        Some(Response {
            success: true,
            reason: "Shutdown".to_owned(),
        })
    }

    /// Determine if the node should pause
    pub fn should_pause(&self) -> bool {
        if let Some(b_num) = self.b_num_to_pause {
            return self.node_raft.get_current_block_num() >= b_num;
        }
        false
    }

    /// Apply a received config to the node
    pub async fn apply_shared_config(&mut self, received_shared_config: MempoolNodeSharedConfig) {
        warn!("Applying shared config: {:?}", received_shared_config);

        let MempoolNodeSharedConfig {
            mempool_mining_event_timeout,
            mempool_partition_full_size,
            mempool_miner_whitelist,
        } = received_shared_config.clone();

        self.node_raft
            .update_mining_event_timeout_duration(mempool_mining_event_timeout);
        self.node_raft
            .update_partition_full_size(mempool_partition_full_size);
        self.node_raft
            .update_mempool_miner_whitelist_active(mempool_miner_whitelist.active);
        self.node_raft.update_mempool_miner_whitelist_api_keys(
            mempool_miner_whitelist.miner_api_keys.clone(),
        );
        self.node_raft
            .update_mempool_miner_whitelist_addresses(mempool_miner_whitelist.miner_addresses);

        if let Some(unauthorized) = self.flush_unauthorized_miners().await {
            self.node_raft
                .propose_runtime_item(MempoolRuntimeItem::RemoveMiningApiKeys(unauthorized))
                .await;
        }

        self.shared_config = received_shared_config;
        self.received_shared_config = None;
    }

    /// Handle a coordinated pause request or initialization
    ///
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    async fn handle_coordinated_pause(&mut self, peer: SocketAddr, b_num: u64) -> Option<Response> {
        // We are initiation the coordinated pause
        if self.local_address() == peer {
            let current_b_num = self.node_raft.get_current_block_num();
            let b_num_to_pause = current_b_num + b_num;
            info!("Initiating coordinated pause for b_num {}", b_num_to_pause);
            self.propose_pause_nodes(b_num_to_pause).await;
            self.initiate_pause_nodes(b_num_to_pause).await.unwrap();
        } else if self.node_raft.get_peers().contains(&peer) {
            // We are receiving the coordinated pause so we just need b_num here
            // - the original coordinator of the pause event has already added current b_num
            //
            // Note: See conditional statement above
            self.propose_pause_nodes(b_num).await;
        }
        Some(Response {
            success: true,
            reason: "Received coordinated pause request".to_owned(),
        })
    }

    /// Handle a coordinated resume request or initialization
    ///
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    async fn handle_coordinated_resume(&mut self, peer: SocketAddr) -> Option<Response> {
        self.propose_resume_nodes().await;
        // We are initiating the coordinated resume
        if self.local_address() == peer {
            info!("Initiating coordinated resume");
            self.initiate_resume_nodes().await.unwrap();
        }
        Some(Response {
            success: true,
            reason: "Received coordinated resume request".to_owned(),
        })
    }

    /// Handle a shared config request
    ///
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    /// * `shared_config` - Shared config that was sent or should get sent
    async fn handle_shared_config(
        &mut self,
        peer: SocketAddr,
        shared_config: MempoolNodeSharedConfig,
    ) -> Option<Response> {
        self.propose_apply_shared_config().await;
        // We are initiating the shared config sending process
        if self.local_address() == peer {
            info!("Initiating shared config");
            self.initiate_send_shared_config(shared_config.clone())
                .await
                .unwrap();
        }
        // The sharing of a shared config was initiated by a peer
        debug!("Received shared config {:?} from {}", &shared_config, peer);
        self.received_shared_config = Some(shared_config);
        Some(Response {
            success: true,
            reason: "Received shared config".to_owned(),
        })
    }

    /// Receive a block notification request from a user node
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    fn receive_block_user_notification_request(&mut self, peer: SocketAddr) -> Response {
        self.user_notification_list.insert(peer);
        self.db
            .put_cf(
                DB_COL_INTERNAL,
                USER_NOTIFY_LIST_KEY,
                &serialize(&self.user_notification_list).unwrap(),
            )
            .unwrap();

        Response {
            success: true,
            reason: "Received block notification".to_owned(),
        }
    }

    /// Check if a miner is whitelisted
    ///
    /// ### Arguments
    ///
    /// * `miner_address` - Address of the miner
    /// * `miner_api_key` - API key of the miner
    pub fn check_miner_whitelisted(
        &self,
        miner_address: SocketAddr,
        miner_api_key: Option<String>,
    ) -> bool {
        if !self.node_raft.get_mempool_whitelisting_active() {
            // No whitelisting active, all miners are allowed
            return true;
        }
        self.node_raft
            .get_mempool_miner_whitelist_api_keys()
            .unwrap_or_default()
            .contains(&miner_api_key.unwrap_or_default())
            || self
                .node_raft
                .get_mempool_miner_whitelist_addresses()
                .unwrap_or_default()
                .iter()
                // Only check IP address, since we do not know ephemeral port beforehand
                .any(|addr| addr.ip() == miner_address.ip())
    }

    /// Receive a partition request from a miner node
    /// TODO: This may need to be part of the MempoolInterface depending on key agreement
    /// ### Arguments
    ///
    /// * `peer`            - Sending peer's socket address
    /// * `mining_api_key`  - Sending peer's API key
    async fn receive_partition_request(
        &mut self,
        peer: SocketAddr,
        mining_api_key: Option<String>,
    ) -> Response {
        info!("Received partition request from {peer:?}");

        // We either kick it if it is unauthorized, or add it to the partition.
        self.miners_changed = true;

        // TODO: Change structure to be more efficient
        let white_listing_active = self.node_raft.get_mempool_whitelisting_active();
        // Only check whitelist if activated
        if white_listing_active && !self.check_miner_whitelisted(peer, mining_api_key.clone()) {
            debug!("Removing unauthorized miner: {peer:?}");
            if let Err(e) = self.node.send(peer, MineRequest::MinerNotAuthorized).await {
                error!("Error sending request to {peer:?}: {e}");
            }
            let disconnect_handles = self.node.disconnect_all(Some(&[peer])).await;
            for handle in disconnect_handles {
                if let Err(e) = handle.await {
                    error!("Error disconnecting from {peer:?}: {e}");
                }
            }
            return Response {
                success: true,
                reason: "Removing unauthorized miner".to_owned(),
            };
        }

        // If the miner node provided an API key, store in memory
        if let Some(api_key) = mining_api_key.clone() {
            self.node_raft
                .propose_runtime_item(MempoolRuntimeItem::AddMiningApiKeys(vec![(peer, api_key)]))
                .await;
        }

        // Check if the miner is already in the request list
        if self.request_list.contains(&peer) {
            info!("Miner {} is already in request list, skipping", peer);
            return Response {
                success: true,
                reason: "Miner already in request list".to_owned(),
            };
        }

        // Add the miner to the request list (in-memory only)
        self.request_list.insert(peer);
        // Remove DB save for request_list
        // self.db
        //     .put_cf(
        //         DB_COL_INTERNAL,
        //         REQUEST_LIST_KEY,
        //         &serialize(&self.request_list).unwrap(),
        //     )
        //     .unwrap();

        // Log status
        if *self.node_raft.get_mining_pipeline_status() == MiningPipelineStatus::Halted {
            info!("Mining pipeline is Halted. Miner {} added to request list. Waiting for next block cycle.", peer);
        }

        // Always return a consistent success message after adding the miner
        // 
        // ---> ADDED: Trigger flood if pipeline is ready when miner requests <---
        if *self.node_raft.get_mining_pipeline_status() == MiningPipelineStatus::AllItemsIntake {
            info!("Pipeline is in AllItemsIntake state. Triggering flood for newly requesting miner: {}", peer);
            // It's safe to ignore the result here, as errors are logged within the flood function.
            // The flood function will handle sending only to connected participants.
            let _ = self.flood_rand_and_block_to_partition().await;
        }
        // ---> END ADDED <---

        Response {
            success: true,
            reason: "Received partition request successfully".to_owned(),
        }
    }

    /// Receives the light POW for partition inclusion
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    /// * 'partition_entry' - ProofOfWork object for the partition entry being recieved.
    async fn receive_partition_entry(
        &mut self,
        peer: SocketAddr,
        pow_info: PowInfo,
        partition_entry: ProofOfWork,
    ) -> Option<Response> {
        if pow_info.b_num != self.node_raft.get_current_block_num() {
            // Out of date entry
            return None;
        }

        let status = self.node_raft.get_mining_pipeline_status().clone();
        let random_number = match (&status, pow_info.participant_only) {
            (MiningPipelineStatus::ParticipantOnlyIntake, true) => &self.previous_random_num,
            (MiningPipelineStatus::AllItemsIntake, false) => &self.current_random_num,
            (MiningPipelineStatus::Halted, _) => {
                return Some(Response {
                    success: false,
                    reason: "Partition list complete".to_owned(),
                });
            }
            _ => return None,
        };

        let valid_pow = format_parition_pow_address(peer) == partition_entry.address
            && validate_pow_for_address(&partition_entry, &Some(random_number));

        if !valid_pow {
            return Some(Response {
                success: false,
                reason: "PoW received is invalid".to_owned(),
            });
        }

        if !self
            .node_raft
            .propose_mining_pipeline_item(MiningPipelineItem::MiningParticipant(peer, status))
            .await
        {
            return None;
        }

        Some(Response {
            success: true,
            reason: "Partition PoW received successfully".to_owned(),
        })
    }

    /// Floods the closing event to everyone
    pub async fn flood_closing_events(&mut self) -> Result<()> {
        let _ = self
            .node
            .send_to_all(Some(self.storage_addr).into_iter(), StorageRequest::Closing)
            .await
            .unwrap();

        let _ = self
            .node
            .send_to_all(
                self.node_raft.raft_peer_addrs().copied(),
                MempoolRequest::Closing,
            )
            .await
            .unwrap();

        let _ = self
            .node
            .send_to_all(self.request_list.iter().copied(), MineRequest::Closing)
            .await
            .unwrap();

        let _ = self
            .node
            .send_to_all(
                self.user_notification_list.iter().copied(),
                UserRequest::Closing,
            )
            .await
            .unwrap();

        Ok(())
    }

    pub fn get_current_mining_reward(&self) -> TokenAmount {
        *self.node_raft.get_current_reward()
    }

    /// Floods the current block to participants for mining
    pub async fn flood_rand_and_block_to_partition(&mut self) -> Result<()> {
        info!("[FLOOD_LOG] Entered flood_rand_and_block_to_partition");

        // ---> ADDED CHECK: Ensure there are participants before flooding <---
        let all_participants = self.node_raft.get_mining_participants();
        if all_participants.unsorted.len() == 0 {
            info!("[FLOOD_LOG] No target participants registered in the current mining pipeline state. Skipping flood.");
            return Ok(()); // Exit early if no participants
        }
        // ---> END ADDED CHECK <---

        let (rnum, participant_only) = match self.node_raft.get_mining_pipeline_status() {
            MiningPipelineStatus::ParticipantOnlyIntake => (self.previous_random_num.clone(), true),
            MiningPipelineStatus::AllItemsIntake => (self.current_random_num.clone(), false),
            MiningPipelineStatus::Halted => {
                warn!("[FLOOD_LOG] Status is Halted, exiting flood early."); // Log exit case
                return Ok(());
            },
        };
        info!("[FLOOD_LOG] Determined phase: participant_only={}, status={:?}", 
               participant_only, self.node_raft.get_mining_pipeline_status());

        let win_coinbases = self.node_raft.get_last_mining_transaction_hashes().clone();
        
        // Ensure mining block exists before proceeding
        // Replace guard! macro with standard if let
        let block = if let Some(b) = self.node_raft.get_mining_block() {
            b
        } else {
            error!("[FLOOD_LOG] Mining block is None, cannot flood.");
            return Err(MempoolError::GenericError("Mining block not available for flooding".into()));
        };
        
        // let block: &Block = self.node_raft.get_mining_block().as_ref().unwrap(); // Original potentially panicking line

        info!(
            "RANDOM NUMBER IN COMPUTE: {:?}, (mined:{})",
            rnum,
            win_coinbases.len()
        );
        debug!("BLOCK TO SEND: {:?}", block);

        let header = block.header.clone();
        let b_num = header.b_num;
        let reward = *self.node_raft.get_current_reward();
        let pow_info = PowInfo {
            participant_only,
            b_num,
        };

        let miner_removal_list = self.miner_removal_list.read().await.clone();
        let all_participants = self.node_raft.get_mining_participants().clone();
        
        // Get currently connected peers
        let connected_peers = self.node.get_peers().await;
        info!("[FLOOD_LOG] Connected peers: {:?}", connected_peers.keys());
        info!("[FLOOD_LOG] Target participants: {:?}", all_participants.unsorted);

        // Filter participants to only include those currently connected and not in removal list
        let connected_participants: Vec<SocketAddr> = all_participants
            .iter()
            .filter(|participant| connected_peers.contains_key(participant) && !miner_removal_list.contains(participant))
            .copied()
            .collect();
        info!("[FLOOD_LOG] Sending block to connected participants: {:?}", connected_participants);
            
        // Identify participants who were selected but are not connected (or are being removed)
        let mut unsent_miners: Vec<SocketAddr> = all_participants
            .iter()
            .filter(|participant| !connected_peers.contains_key(participant) || miner_removal_list.contains(participant))
            .copied()
            .collect();
        info!("[FLOOD_LOG] Initial unsent/removed miners: {:?}", unsent_miners);

        // Filter non-participants to only include those currently connected
        let request_list = self.request_list.clone();
        let connected_non_participants: Vec<SocketAddr> = request_list
            .difference(all_participants.lookup())
            .filter(|non_participant| connected_peers.contains_key(non_participant))
            .copied()
            .collect();
        info!("[FLOOD_LOG] Sending block (header only) to connected non-participants: {:?}", connected_non_participants);

        // Introduce a small delay to allow connections to fully establish
        // tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // info!("[FLOOD_LOG] Delay complete, attempting sends...");

        let _ = self
            .node
            .send_to_all(
                miner_removal_list.iter().copied(),
                MineRequest::MinerRemovedAck,
            )
            .await;

        // Send full block to CONNECTED participants
        let send_participants_result = self
            .node
            .send_to_all(
                connected_participants.into_iter(), // Use filtered list
                MineRequest::SendBlock {
                    pow_info,
                    rnum: rnum.clone(),
                    win_coinbases: win_coinbases.clone(),
                    block: Some(header.clone()),
                    reward,
                    b_num: header.b_num,
                },
            )
            .await;
        // Handle the Ok case which contains the list of unsent peers
        if let Ok(unsent_participants) = send_participants_result {
            if !unsent_participants.is_empty() {
                 info!("[FLOOD_LOG] Failed to send to some participants: {:?}", unsent_participants);
                 unsent_miners.extend(unsent_participants);
            }
        } else if let Err(e) = send_participants_result {
            // Log if the entire send_to_all operation failed
            error!("[FLOOD_LOG] Error during send_to_all for participants: {:?}", e);
        }

        // Send header only to CONNECTED non-participants
        let send_non_participants_result = self
            .node
            .send_to_all(
                connected_non_participants.into_iter(), // Use filtered list
                MineRequest::SendBlock {
                    pow_info,
                    rnum,
                    win_coinbases,
                    block: None,
                    reward,
                    b_num: header.b_num,
                },
            )
            .await;
        // Handle the Ok case which contains the list of unsent peers
        if let Ok(unsent_non_participants) = send_non_participants_result {
             if !unsent_non_participants.is_empty() {
                 info!("[FLOOD_LOG] Failed to send to some non-participants: {:?}", unsent_non_participants);
                 unsent_miners.extend(unsent_non_participants);
            }
        } else if let Err(e) = send_non_participants_result {
             // Log if the entire send_to_all operation failed
            error!("[FLOOD_LOG] Error during send_to_all for non-participants: {:?}", e);
        }

        info!("[FLOOD_LOG] Final list of unsent/removed miners to potentially flush: {:?}", unsent_miners);
        // REMOVED: Do not immediately flush miners just because they were not connected
        // during this specific flood attempt. The retry mechanism in resend_trigger_message
        // should handle eventually delivering the message once the miner connects.
        // A separate mechanism should handle truly stale/inactive miners.
        /*
        if !unsent_miners.is_empty() || !miner_removal_list.is_empty() {
            unsent_miners.extend(miner_removal_list.iter().copied()); // Corrected extend usage
            self.flush_stale_miners(unsent_miners.clone()); // This flushes pipeline state
            self.miner_removal_list.write().await.clear();
            // This proposes removal from consensus runtime data
            self.node_raft
                .propose_runtime_item(MempoolRuntimeItem::RemoveMiningApiKeys(unsent_miners))
                .await;
        }
        */ // Ensure comment is terminated

        // Explicitly handle the miner_removal_list if it's not empty,
        // but don't combine it with temporarily unsent miners.
        let current_miner_removal_list = miner_removal_list; // Use the value read earlier
        if !current_miner_removal_list.is_empty() {
            info!("[FLOOD_LOG] Flushing explicitly removed miners: {:?}", current_miner_removal_list);
            let miners_to_flush: Vec<SocketAddr> = current_miner_removal_list.into_iter().collect();
            self.flush_stale_miners(miners_to_flush.clone()); // Flush pipeline state
            self.miner_removal_list.write().await.clear(); // Clear the shared state
             // Propose removal from consensus runtime data
            self.node_raft
                .propose_runtime_item(MempoolRuntimeItem::RemoveMiningApiKeys(miners_to_flush))
                .await;
        }

        info!(
            "Number of connected miners: {:?}",
            self.get_connected_miners().await.len()
        );

        info!("[FLOOD_LOG] Exiting flood_rand_and_block_to_partition successfully."); // Added exit log
        Ok(())
    }

    /// If whitelisting is active, this function will remove all miners that are not whitelisted
    pub async fn flush_unauthorized_miners(&mut self) -> Option<Vec<SocketAddr>> {
        // Determine if whitelisting is active
        let whitelisting_active = self.node_raft.get_mempool_whitelisting_active();

        if !whitelisting_active {
            // Return empty array
            return None;
        }

        // Whitelisting is active, remove all miner nodes not whitelisted
        let mut miners_to_remove: Vec<SocketAddr> = Vec::new();
        let connected_miners: Vec<SocketAddr> = self.get_connected_miners().await;
        for peer in connected_miners {
            let mining_api_key = self.node_raft.get_mining_api_key_entry(peer);
            if whitelisting_active && !self.check_miner_whitelisted(peer, mining_api_key) {
                debug!("Removing unauthorized miner: {peer:?}");
                if let Err(e) = self.node.send(peer, MineRequest::MinerNotAuthorized).await {
                    error!("Error sending request to {peer:?}: {e}");
                }
                miners_to_remove.push(peer);
            }
        }

        for handle in self.node.disconnect_all(Some(&miners_to_remove)).await {
            if let Err(e) = handle.await {
                error!("Error disconnecting from peer: {:?}", e);
            }
        }

        Some(miners_to_remove)
    }

    pub fn flush_stale_miners(&mut self, stale_miners: Vec<SocketAddr>) {
        trace!("Flushing stale miners from mining pipeline and request list {stale_miners:?}");

        // Flush stale miners
        self.request_list
            .retain(|addr| !stale_miners.contains(addr));

        // Update DB
        if let Err(e) = self.db.put_cf(
            DB_COL_INTERNAL,
            REQUEST_LIST_KEY,
            &serialize(&self.request_list).unwrap(),
        ) {
            error!("Error writing updated miner request list to disk: {e:?}");
        }

        // Cleanup miners from block pipeline
        self.node_raft.flush_stale_miners(&stale_miners);
        self.miners_changed = true;
    }

    /// Floods the current block to participants for mining
    pub async fn flood_transactions_to_partition(&mut self) -> Result<()> {
        let block: &Block = self.node_raft.get_mining_block().as_ref().unwrap();
        let tx_merkle_verification = block.transactions.clone();

        self.node
            .send_to_all(
                self.node_raft.get_mining_participants_iter(),
                MineRequest::SendTransactions {
                    tx_merkle_verification,
                },
            )
            .await
            .unwrap();

        Ok(())
    }

    /// Floods the current block to user listening for updates
    pub async fn flood_block_to_users(&mut self) -> Result<()> {
        let block: Block = self.node_raft.get_mining_block().clone().unwrap();

        let unsent = self
            .node
            .send_to_all(
                self.user_notification_list.iter().copied(),
                UserRequest::BlockMining { block },
            )
            .await?;

        if !unsent.is_empty() {
            warn!("Purging users: {:?}", unsent);
            self.user_notification_list.retain(|v| !unsent.contains(v));
            self.db
                .put_cf(
                    DB_COL_INTERNAL,
                    USER_NOTIFY_LIST_KEY,
                    &serialize(&self.user_notification_list).unwrap(),
                )
                .unwrap();
        }

        Ok(())
    }

    /// Logs the winner of the block and changes the current block to a new block to be mined
    pub fn mining_block_mined(&mut self) {
        info!("Mining block mined"); // Added info log
        if let Some((mut block, mut block_txs)) = self.node_raft.take_mining_block() {
            let (_, winning_pow) = self.node_raft.get_winning_miner().clone().unwrap();
            let unicorn = self.node_raft.get_current_unicorn().clone();

            let mining_tx = winning_pow.mining_tx;
            let nonce = winning_pow.nonce;
            block.header = apply_mining_tx(block.header, nonce, mining_tx.0.clone());
            block_txs.insert(mining_tx.0, mining_tx.1);

            let extra_info = MinedBlockExtraInfo {
                shutdown: self.coordinated_shutdown <= block.header.b_num,
            };
            let common = CommonBlockInfo {
                block,
                block_txs,
                pow_p_value: winning_pow.p_value,
                pow_d_value: winning_pow.d_value,
                unicorn: unicorn.unicorn,
                unicorn_witness: unicorn.witness,
            };
            let current_mined_block = MinedBlock { common, extra_info }; // Assign to a local variable first
            self.current_mined_block = Some(current_mined_block.clone()); // Clone for the Option

            info!(
                "Current mined block set for block {}",
                self.current_mined_block.as_ref().unwrap().common.block.header.b_num.clone()
            );

            // Call send_block_to_storage asynchronously
            let mut node_clone = self.node.clone(); // Clone node for the async task
            let storage_addr_clone = self.storage_addr;
            // Clone the block data directly from the local variable
            let current_mined_block_clone = current_mined_block;

            tokio::spawn(async move {
                info!("Spawning task to send block {} to storage {}", current_mined_block_clone.common.block.header.b_num.clone(), storage_addr_clone);
                // Construct the CORRECT message type for storage
                let message = StorageRequest::SendBlock { 
                    mined_block: Some(current_mined_block_clone.clone()) // Send Option<MinedBlock>
                }; 
                match node_clone.send(storage_addr_clone, message).await {
                    Ok(_) => info!("Successfully sent block {} to storage.", current_mined_block_clone.common.block.header.b_num),
                    Err(e) => error!("Failed to send block {} to storage {}: {:?}", current_mined_block_clone.common.block.header.b_num, storage_addr_clone, e),
                }
            });

            // self.node_raft.propose_block_with_last_info(block_info).await; // Keep this commented out

        } else { // Handle the case where take_mining_block returns None
             warn!("mining_block_mined called but no mining block was available to take.");
        }
    }

    /// Reset the mining block processing to allow a new block.
    async fn reset_mining_block_process(&mut self) {
        // REMOVED: Backup/clear/restore logic for request_list
        // let request_list_backup = self.request_list.clone();
        // let has_miners = !request_list_backup.is_empty();
        // info!("[RESET_LOG] Starting reset_mining_block_process. has_miners_backup = {}", has_miners);
        // info!("[RESET_LOG] Request list BEFORE clear: {:?}", self.request_list);

        info!("Starting reset of mining block process");
        info!("Current mining pipeline status: {:?}", self.node_raft.get_mining_pipeline_status());
        
        // Handle random numbers
        info!("Updating random numbers");
        self.previous_random_num = Some(std::mem::take(&mut self.current_random_num))
            .filter(|v| !v.is_empty())
            .unwrap_or_else(generate_pow_random_num);

        self.current_random_num = generate_pow_random_num();
        
        info!("Storing random numbers in database");
        if let Err(e) = self.db
            .put_cf(
                DB_COL_INTERNAL,
                POW_PREV_RANDOM_NUM_KEY,
                &self.previous_random_num,
            ) {
            error!("Failed to store previous random number: {:?}", e);
        }
        
        if let Err(e) = self.db
            .put_cf(
                DB_COL_INTERNAL,
                POW_RANDOM_NUM_KEY,
                &self.current_random_num,
            ) {
            error!("Failed to store current random number: {:?}", e);
        }
        
        // Clear the current mined block
        if self.current_mined_block.is_some() {
            info!("Clearing current mined block");
            self.current_mined_block = None;
        } else {
            info!("No current mined block to clear");
        }

        // Reset the trigger messages count
        info!("Resetting trigger messages count");
        self.current_trigger_messages_count = 0;

        // --- REMOVED Unconditional Clear ---
        // info!("Clearing request list (backup captured: {} miners)", if has_miners { request_list_backup.len() } else { 0 });
        // self.request_list = Default::default();
        // info!("[RESET_LOG] Request list AFTER clear: {:?}", self.request_list);

        // Reset the request list first flood
        if self.request_list_first_flood.is_some() {
            info!("Resetting request list first flood");
            self.request_list_first_flood = None;
        } else {
            info!("Request list first flood is already None");
        }

        // Clear the user notification list
        if !self.user_notification_list.is_empty() {
            info!("Clearing user notification list with {} entries", self.user_notification_list.len());
            self.user_notification_list = Default::default();
        } else {
            info!("User notification list is already empty");
        }

        // Log the proposed keys before clearing them
        let proposed_keys = self.node_raft.get_block_pipeline_proposed_keys();
        info!("Current proposed keys in block pipeline: {:?}", proposed_keys);
        
        // Clear the proposed keys in the block pipeline
        info!("Clearing proposed keys in block pipeline");
        self.node_raft.clear_block_pipeline_proposed_keys();

        // If the node should pause, set the pause node flag to true
        if self.should_pause() {
            info!("Node should pause, setting pause flag to true");
            *self.pause_node.write().await = true;
        }

        // Flush stale miners
        info!("Flushing stale miners");
        if let Some(stale_miners) = self.flush_unauthorized_miners().await {
            if !stale_miners.is_empty() {
                info!("Flushed {} stale miners", stale_miners.len());
                self.node_raft.flush_stale_miners(&stale_miners);
            } else {
                info!("No stale miners to flush");
            }
        } else {
            info!("No stale miners to flush (None returned)");
        }
        
        // --- REMOVED Restore Block ---
        // info!("[RESET_LOG] Checking if miners should be restored (has_miners = {})", has_miners);
        // if has_miners { ... }

        info!("[RESET_LOG] Request list after reset (should be unchanged): {:?}", self.request_list);
        info!("Mining block process reset complete");
        info!("Current mining pipeline status after reset: {:?}", self.node_raft.get_mining_pipeline_status());
    }

    /// Load and apply the local database to our state
    fn load_local_db(mut self) -> Result<Self> {
        // Remove loading of request_list from DB
        // self.request_list = match self.db.get_cf(DB_COL_INTERNAL, REQUEST_LIST_KEY) {
        //     Ok(Some(list)) => {
        //         let list = deserialize::<BTreeSet<SocketAddr>>(&list)?;
        //         debug!("load_local_db: request_list {:?}", list);
        //         list
        //     }
        //     Ok(None) => self.request_list, // Keep default if not found
        //     Err(e) => panic!("Error accessing db: {:?}", e),
        // };
        // // Check related first_flood logic - potentially remove if request_list isn't loaded
        // if let Some(first) = self.request_list_first_flood {
        //     if first <= self.request_list.len() {
        //         self.request_list_first_flood = None;
        //         self.node_raft.set_initial_proposal_done();
        //     }
        // }

        self.user_notification_list = match self.db.get_cf(DB_COL_INTERNAL, USER_NOTIFY_LIST_KEY) {
            Ok(Some(list)) => {
                let list = deserialize::<BTreeSet<SocketAddr>>(&list)?;
                debug!("load_local_db: user_notification_list {:?}", list);
                list
            }
            Ok(None) => self.user_notification_list,
            Err(e) => panic!("Error accessing db: {:?}", e),
        };

        for (num, key) in [
            (&mut self.current_random_num, POW_RANDOM_NUM_KEY),
            (&mut self.previous_random_num, POW_PREV_RANDOM_NUM_KEY),
        ] {
            *num = {
                let current_random_num = match self.db.get_cf(DB_COL_INTERNAL, key) {
                    Ok(Some(num)) => num,
                    Ok(None) => generate_pow_random_num(),
                    Err(e) => panic!("Error accessing db: {:?}", e),
                };
                debug!("load_local_db: {} {:?}", key, current_random_num);
                if let Err(e) = self.db.put_cf(DB_COL_INTERNAL, key, &current_random_num) {
                    panic!("Error accessing db: {:?}", e);
                }
                current_random_num
            };
        }

        self.node_raft.set_key_run({
            let key_run = match self.db.get_cf(DB_COL_INTERNAL, RAFT_KEY_RUN) {
                Ok(Some(key_run)) => deserialize::<u64>(&key_run)? + 1,
                Ok(None) => 0,
                Err(e) => panic!("Error accessing db: {:?}", e),
            };
            debug!("load_local_db: key_run update to {:?}", key_run);
            if let Err(e) = self
                .db
                .put_cf(DB_COL_INTERNAL, RAFT_KEY_RUN, &serialize(&key_run)?)
            {
                panic!("Error accessing db: {:?}", e);
            }
            key_run
        });

            self.node_raft
            .append_to_tx_pool(get_local_transactions(&self.db));

        Ok(self)
    }

    /// Recieves a ProofOfWork from miner
    ///
    /// ### Arguments
    ///
    /// * `address`    - Address of miner
    /// * `block_num`  - Block number the PoW is for
    /// * `nonce`      - Sequenc number of the block held in a Vec<u8>
    /// * 'coinbase'   - The transaction object  of the mining
    async fn receive_pow(
        &mut self,
        address: SocketAddr,
        block_num: u64,
        nonce: Vec<u8>,
        coinbase: Transaction,
    ) -> Option<Response> {
        let pow_mining_block = (self.node_raft.get_mining_block().as_ref())
            .filter(|b| block_num == b.header.b_num)
            .filter(|_| self.node_raft.get_mining_participants().contains(&address));

        // Check if expected block
        let block_to_check = if let Some(mining_block) = pow_mining_block {
            info!(?address, "Received expected PoW");
            mining_block.header.clone()
        } else {
            trace!(?address, "Received outdated PoW");
            return Some(Response {
                success: false,
                reason: "Not block currently mined".to_owned(),
            });
        };

        // Check coinbase amount and structure
        let coinbase_amount = self.node_raft.get_current_reward();
        if !coinbase.is_coinbase() || coinbase.outputs[0].value.token_amount() != *coinbase_amount {
            return Some(Response {
                success: false,
                reason: "Coinbase transaction invalid".to_owned(),
            });
        }

        // Perform validation
        let coinbase_hash = construct_tx_hash(&coinbase);
        let block_to_check = apply_mining_tx(block_to_check, nonce, coinbase_hash);
        if let Err(err) = validate_pow_block(&block_to_check) {
            return Some(Response {
                success: false,
                reason: format!("Invalid PoW for block: {}", err),
            });
        }

        // TODO: D and P will need to change with keccak prime intro
        let (nonce, coinbase_hash) = block_to_check.nonce_and_mining_tx_hash;
        let pow_info = WinningPoWInfo {
            nonce,
            mining_tx: (coinbase_hash, coinbase),
            d_value: 0,
            p_value: 0,
        };

        // Propose the received PoW to the block pipeline
        if !self
            .node_raft
            .propose_mining_pipeline_item(MiningPipelineItem::WinningPoW(
                address,
                Box::new(pow_info),
            ))
            .await
        {
            return None;
        }

        Some(Response {
            success: true,
            reason: "Received PoW successfully".to_owned(),
        })
    }

    /// Receives block info from its storage node
    ///
    /// ### Arguments
    ///
    /// * `peer` - Address of the storage peer sending the block
    /// * `BlockStoredInfo` - Infomation about the recieved block
    async fn receive_block_stored(
        &mut self,
        peer: SocketAddr,
        previous_block_info: BlockStoredInfo,
    ) -> Option<Response> {
        info!(peer = ?peer, "Received block stored from peer: {}", peer);
        // Verify the sender is the expected storage node
        if peer != self.storage_addr {
            warn!(
                peer = ?peer,
                "Received block stored from unexpected peer {}. Expected {}",
                peer, self.storage_addr
            );
            // Decide if we should ignore or handle this case differently
            // For now, let's ignore it to prevent potential issues.
            return None;
        }
        info!(peer = ?peer, "Expected storage address: {}", self.storage_addr);
        info!(peer = ?peer, "Are they the same? {}", peer == self.storage_addr);

        let b_num = previous_block_info.block_num;
        let b_hash = previous_block_info.block_hash.clone();
        info!(peer = ?peer, "Received block stored info from storage: block_num={}, hash={}", b_num, b_hash);

        let current_block_num = self.node_raft.get_current_block_num(); // This returns u64
        info!(peer = ?peer, "Current mining block number: {}", current_block_num); // Log the u64 directly

        // Correct comparison: compare u64 with u64
        if current_block_num != b_num {
            warn!(peer = ?peer, "Received stale block stored info for block {}, current is {}", b_num, current_block_num);
            // Maybe request catch-up if we fall behind?
            return None;
        }

        // If this node is the leader (or Raft is disabled), propose the next block info
        // The proposal will contain the hash and number of the block just stored (b_num)
        // The Raft layer will increment this to b_num + 1 when generating the *next* block
        info!(peer = ?peer, "Current mining pipeline status before proposing: {:?}", self.node_raft.get_mining_pipeline_status());
        info!(peer = ?peer, "Proposing block with last info to Raft");
        let proposed = self
            .node_raft
            .propose_block_with_last_info(BlockStoredInfo {
                block_hash: b_hash.clone(), // Hash of the block JUST stored
                block_num: b_num, // Block number JUST stored
                nonce: vec![], // Nonce not needed here, just block id
                mining_transactions: BTreeMap::new(), // Mining txs not needed here
                shutdown: self.coordinated_shutdown == b_num + 1, // Check if NEXT block is shutdown block
            })
            .await;

        if proposed {
            info!(peer = ?peer, "Successfully proposed block with last info, transitioning to next mining phase");

            // Block proposal accepted by raft, clear the locally held mined block
            info!(peer = ?peer, "Clearing current mined block as it has been successfully processed");
            self.current_mined_block = None;

            // Generate and store the next random number pair
            info!(peer = ?peer, "Generating new random number for next mining round");
            self.previous_random_num = self.current_random_num.clone();
            // Correct function name
            self.current_random_num = generate_pow_random_num();
            info!(peer = ?peer, "Storing random numbers in database");
            // Correct method name put_cf
            if let Err(e) = self.db.put_cf(
                DB_COL_INTERNAL,
                POW_PREV_RANDOM_NUM_KEY,
                &self.previous_random_num,
            ) {
                error!("Failed to store previous random number: {:?}", e);
            }
            // Correct method name put_cf
            if let Err(e) = self.db.put_cf(
                DB_COL_INTERNAL,
                POW_RANDOM_NUM_KEY,
                &self.current_random_num,
            ) {
                error!("Failed to store current random number: {:?}", e);
            }

            // Reset trigger message counter for the new block cycle
            info!(peer = ?peer, "Resetting trigger messages count");
            self.current_trigger_messages_count = 0;
            self.request_list_first_flood = None; // Allow flooding again for the new block

            info!(peer = ?peer, "Block stored processing complete, returning success response");
            // Correct Response structure
            Some(Response {
                success: true,
                reason: "Received block stored".to_owned(),
            })
        } else {
            warn!(peer = ?peer, "Failed to propose block info to Raft (maybe duplicate or not leader?). Will retry on trigger.");
            // Don't clear current_mined_block if proposal failed
            None
        }

        // REMOVED: This reset happened too early, before the Raft commit for the next block.
        // info!(peer = ?peer, "Resetting mining block process after storage confirmation.");
        // self.reset_mining_block_process().await;

    }

    /// Re-sends messages triggering the next step in flow
    pub async fn resend_trigger_message(&mut self) {
        info!("Current mining pipeline status: {:?}", self.node_raft.get_mining_pipeline_status());
        
        match self.node_raft.get_mining_pipeline_status().clone() {
            MiningPipelineStatus::Halted => {
                info!("Mining pipeline is Halted, checking status...");
                
                // Check 2: Is the *next* block ready in the pipeline (i.e., has BlockFromStorage been committed)?
                // This check implicitly assumes the previous block cycle is complete (current_mined_block is None).
                if self.node_raft.get_mining_block().is_none() {
                    info!("Next mining block is not yet available in the pipeline.");
                    // We are Halted, no block sent (previous cycle complete), and no block ready for the *next* round.
                    // This means we are waiting for storage confirmation OR it's the start/idle state.
                    if self.request_list.is_empty() {
                        info!("Request list is empty. Proposing pipeline reset.");
                        // If no miners waiting, propose reset.
                        let reset_block_num = self.node_raft.get_committed_current_block_num().unwrap_or(0);
                        if !self.node_raft.propose_pipeline_item_with_block_num(MiningPipelineItem::ResetPipeline, reset_block_num).await {
                            error!("Failed to propose reset pipeline item when Halted/NoBlock/NoMiners.");
                        }
                    } else {
                        // Miners are waiting, but the block isn't ready. Initiate proposal if needed.
                        
                        // --- MODIFIED LOGIC START ---
                        if self.node_raft.is_first_block() {
                            // Handle proposal for the VERY FIRST block (Block 0)
                            info!("Handling proposal for initial block (Block 0)");
                            // No need to get genesis UTXOs here, MempoolRaft handles it.

                            // Use the new check specific to the initial proposal state
                            if self.node_raft.is_initial_proposal_in_flight() {
                                info!("Initial FirstBlock proposal is already in flight/handled, waiting for commit.");
                            } else {
                                info!("Proposing initial FirstBlock item via propose_initial_item().");
                                // Call the correct public method in MempoolRaft which holds the FirstBlock item
                                self.node_raft.propose_initial_item().await;
                                info!("Successfully called propose_initial_item(). Waiting for commit.");
                            }
                        } else {
                            // Handle proposal for SUBSEQUENT blocks (Block 1+)
                            let current_block_num = self.node_raft.get_committed_current_block_num().unwrap_or(0);
                            let next_block_num = current_block_num + 1;
                            info!("Initiating next block proposal for Block {}.", next_block_num);

                            let block_info = BlockStoredInfo {
                                block_hash: "".to_string(),
                                block_num: next_block_num,
                                nonce: vec![],
                                mining_transactions: BTreeMap::new(),
                                shutdown: false,
                            };
                            let item_to_propose = MempoolRaftItem::Block(block_info.clone());

                            // Check if this Block proposal is already in flight
                            if self.node_raft.is_proposal_in_flight(&item_to_propose, next_block_num) {
                                info!("Block proposal for block {} is already in flight, waiting for commit.", next_block_num);
                            } else {
                                if !self.node_raft.propose_block_with_last_info(block_info).await {
                                    error!("Failed to propose BlockFromStorage for block {}", next_block_num);
                                } else {
                                    info!("Successfully proposed BlockFromStorage for block {}. Waiting for commit.", next_block_num);
                                }
                            }
                        }
                        // --- MODIFIED LOGIC END ---
                    }
                } else {
                    // The next block *is* available in the pipeline, but we are still Halted.
                    // This implies BlockFromStorage was committed, but maybe participants haven't been handled.
                    info!("Next mining block is available, but state is still Halted. Proceeding with participant intake.");

                    // Check 3: Do we have miners waiting in the request list?
                    if !self.request_list.is_empty() {
                        info!("Found {} miners in request list. Processing participant proposals.", self.request_list.len());
                        
                        let current_participants = self.node_raft.get_mining_participants().clone();
                        let current_block_num = self.node_raft.get_current_block_num(); // Use current, not committed
                        info!("Using block number {} for participant proposals.", current_block_num);
                        
                        let mut added_any_miners = false;
                        for &miner in &self.request_list {
                            if current_participants.lookup.contains(&miner) {
                                info!("Miner {} is already in participants list, skipping proposal.", miner);
                                continue;
                            }
                            
                            let proposal_item = MiningPipelineItem::MiningParticipant(miner, MiningPipelineStatus::ParticipantOnlyIntake);
                            if self.node_raft.is_proposal_in_flight(&MempoolRaftItem::PipelineItem(proposal_item.clone(), current_block_num), current_block_num) {
                                info!("Proposal for miner {} (block {}) is already in flight, skipping.", miner, current_block_num);
                                continue;
                            }
                            
                            info!("Proposing addition of miner {} for block {}.", miner, current_block_num);
                            if self.node_raft.propose_pipeline_item_with_block_num(
                                proposal_item,
                                current_block_num
                            ).await {
                                info!("Successfully proposed adding miner {}.", miner);
                                added_any_miners = true;
                            } else {
                                warn!("Failed unexpectedly when trying to propose adding miner {}.", miner);
                            }
                        }
                        
                        // Always attempt CompleteParticipant if list isn't empty (and block is ready)
                        info!("Attempting CompleteParticipant proposal for block {}.", current_block_num);
                        let complete_proposal_item = MiningPipelineItem::CompleteParticipant;
                        if self.node_raft.propose_pipeline_item_with_block_num(
                            complete_proposal_item,
                            current_block_num
                        ).await {
                            info!("Successfully proposed CompleteParticipant (or deduplicated) for block {}.", current_block_num);
                        } else {
                            warn!("Failed to propose CompleteParticipant for block {}, maybe already handled?", current_block_num);
                        }
                        
                    } else {
                        // Halted, block ready, but no miners waiting. This seems like an idle state.
                        info!("Halted with block ready, but no miners in request list. Pipeline remains idle.");
                    }
                }
            }
            // MiningPipelineStatus::ParticipantOnlyIntake => { ... }
            MiningPipelineStatus::AllItemsIntake => {
                info!("Resend trigger: Status is AllItemsIntake, re-flooding block/rand to partition.");
                if let Err(e) = self.flood_rand_and_block_to_partition().await {
                    error!("Resend trigger: Failed to flood block/rand to partition miners in AllItemsIntake: {:?}", e);
                }
            }
            _ => {
                // Optional: Log that no action is taken for other states
                // trace!("Resend trigger: No specific action for status {:?}", self.node_raft.get_mining_pipeline_status());
            }
        }

        // Increment the trigger messages count
        self.current_trigger_messages_count += 1;
        info!("Current trigger messages count: {}", self.current_trigger_messages_count);

        // If we've sent too many trigger messages and the pipeline reset is enabled,
        // try to reset the pipeline to break out of any potential loops
        if self.enable_trigger_messages_pipeline_reset && self.current_trigger_messages_count > RESEND_TRIGGER_MESSAGES_COMPUTE_LIMIT {
            info!("Resend trigger messages limit reached, attempting pipeline reset");

            // Perform local reset first
            self.reset_mining_block_process().await;
            self.current_trigger_messages_count = 0; // Reset counter after local reset

            // Determine the correct block number context for the Raft reset proposal
            let reset_block_num = self.node_raft.get_committed_current_block_num().unwrap_or(0);
            info!(b_num=reset_block_num, "Proposing Raft pipeline reset for block context {}", reset_block_num);

            // Propose the reset pipeline item to Raft
            if self.node_raft.propose_pipeline_item_with_block_num(MiningPipelineItem::ResetPipeline, reset_block_num).await {
                info!(b_num=reset_block_num, "Successfully proposed Raft reset pipeline item for block context {}", reset_block_num);
            } else {
                error!(b_num=reset_block_num, "Failed to propose Raft reset pipeline item for block context {}", reset_block_num);
            }

            // --- REMOVED OLD LOGIC that re-added miners for block 0 ---
            // The ResetPipeline commit handler in Raft should now correctly reset the state.
            return; // Exit after initiating reset
        }
    }

    /// Create a item asset transaction from data received
    ///
    /// ### Arguments
    ///
    /// * `item_amount`      - Amount of item assets
    /// * `script_public_key`   - Public address key
    /// * `public_key`          - Public key
    /// * `signature`           - Signature
    pub fn create_item_asset_tx(
        &mut self,
        item_amount: u64,
        script_public_key: String,
        public_key: String,
        signature: String,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    ) -> Result<(Transaction, String)> {
        let b_num = self.node_raft.get_current_block_num();
        Ok(create_item_asset_tx_from_sig(
            b_num,
            item_amount,
            script_public_key,
            public_key,
            signature,
            genesis_hash_spec,
            metadata,
        )?)
    }

    /// Get `Node` member
    pub fn get_node(&self) -> &Node {
        &self.node
    }

    /// Updates the status of a particular transaction
    ///
    /// ### Arguments
    ///
    /// * `tx` - Transaction to update status for
    /// * `status` - The transaction status
    /// * `additional_info` - Additional information about the transaction
    pub fn update_tx_status(
        &mut self,
        tx: &Transaction,
        status: TxStatusType,
        additional_info: String,
    ) {
        let tx_hash = construct_tx_hash(tx);
        let current_entry = self.tx_status_list.get(&tx_hash);
        let timestamp = if let Some(entry) = current_entry {
            entry.timestamp
        } else {
            get_timestamp_now()
        };
        let tx_status = TxStatus {
            additional_info,
            status,
            timestamp,
        };

        self.tx_status_list.insert(tx_hash, tx_status);
    }

    /// Constructs a transaction status with validation information
    ///
    /// ### Arguments
    ///
    /// * `tx` - Transaction to construct status for
    pub fn construct_tx_status(&mut self, tx: &Transaction) -> (TxStatusType, String) {
        let tx_validator = self.transactions_validator();
        let (is_valid, mut validation_info) = tx_validator(&tx);
        let mut status = TxStatusType::Confirmed;

        if !is_valid {
            status = TxStatusType::Rejected;
        }

        if tx.druid_info.is_some() {
            status = TxStatusType::Pending;
            validation_info = "DRUID transaction valid. Awaiting settlement".to_owned();
        }

        (status, validation_info)
    }

    /// Flushes transaction statuses if their lifetimes have expired
    pub fn flush_stale_tx_status(&mut self) {
        let stale_txs = self
            .tx_status_list
            .iter()
            .filter(|(_, status)| {
                is_timestamp_difference_greater(
                    status.timestamp as u64,
                    get_timestamp_now() as u64,
                    self.tx_status_lifetime as u64,
                )
            })
            .map(|(tx_hash, _)| tx_hash.clone())
            .collect::<Vec<String>>();

        debug!("Flushing stale transaction statuses: {:?}", stale_txs);

        for tx_hash in stale_txs {
            self.tx_status_list.remove(&tx_hash);
        }
    }

    /// Retrieves the status for a list of transactions
    ///
    /// ### Arguments
    ///
    /// * `tx_hashes` - List of transaction hashes to retrieve status for
    pub fn get_transaction_status(&self, tx_hashes: Vec<String>) -> BTreeMap<String, TxStatus> {
        let mut tx_status = BTreeMap::new();

        for tx_hash in tx_hashes {
            if let Some(status) = self.tx_status_list.get(&tx_hash) {
                tx_status.insert(tx_hash, status.clone());
            }
        }

        tx_status
    }

    /// Receive incoming transactions
    ///
    /// ### Arguments
    ///
    /// * `transactions` - Transactions to be processed
    pub fn receive_transactions(&mut self, transactions: Vec<Transaction>) -> Response {
        let transactions_len = transactions.len();
        if !self.node_raft.tx_pool_can_accept(transactions_len) {
            let reason = "Transaction pool for this mempool node is full".to_owned();

            // Update transaction status
            for tx in transactions {
                self.update_tx_status(&tx, TxStatusType::Rejected, reason.clone());
            }

            return Response {
                success: false,
                reason,
            };
        }

        let (valid_dde_txs, valid_txs): (BTreeMap<_, _>, BTreeMap<_, _>) = {
            let tx_validator = self.transactions_validator();
            transactions
                .clone()
                .into_iter()
                .filter(|tx| tx_validator(tx).0)
                .map(|tx| (construct_tx_hash(&tx), tx))
                .partition(|tx| tx.1.druid_info.is_some())
        };

        let total_valid_txs_len = valid_txs.len() + valid_dde_txs.len();

        // Update transaction status after initial validation
        for tx in transactions {
            let (status, validation_info) = self.construct_tx_status(&tx);
            self.update_tx_status(&tx, status, validation_info);
        }

        // No valid transactions (normal or DDE) provided
        if total_valid_txs_len == 0 {
            return Response {
                success: false,
                reason: "No valid transactions provided".to_owned(),
            };
        }

        // `Normal` transactions
        store_local_transactions(&mut self.db, &valid_txs);
        self.node_raft.append_to_tx_pool(valid_txs);

        // `DDE` transactions
        // TODO: Save DDE transactions to local DB storage
        let ready_dde_txs = self.validate_dde_txs(valid_dde_txs);
        let mut invalid_dde_txs_len = 0;
        for (valid, ready) in ready_dde_txs {
            let mut status = TxStatusType::Confirmed;
            let mut validation_info = Default::default();

            if !valid {
                invalid_dde_txs_len += 1;
                status = TxStatusType::Rejected;
                validation_info = "DRUID trade expectations not met".to_owned();
            } else {
                self.node_raft.append_to_tx_druid_pool(ready.clone());
            }

            // Update transaction status for each transaction
            for tx in ready.iter() {
                self.update_tx_status(&tx.1, status.clone(), validation_info.clone());
            }
        }

        // Some txs are invalid or some DDE txs are ready to execute but fail to validate
        // TODO: Should provide better feedback on DDE transactions that fail
        if (total_valid_txs_len < transactions_len) || invalid_dde_txs_len != 0 {
            return Response {
                success: true,
                reason: "Some transactions invalid. Adding valid transactions only".to_owned(),
            };
        }

        Response {
            success: true,
            reason: "Transactions added to tx pool".to_owned(),
        }
    }

    /// Execute the initialization of a coordinated pause by invoking peers
    ///
    /// NOTE: Current block number has already been added to b_num from the coordinator
    pub async fn initiate_pause_nodes(&mut self, b_num: u64) -> Result<()> {
        self.node
            .send_to_all(
                self.node_raft.raft_peer_addrs().copied(),
                MempoolRequest::CoordinatedPause { b_num },
            )
            .await?;
        Ok(())
    }

    /// Execute the initialization of a coordinated resume by invoking peers
    pub async fn initiate_resume_nodes(&mut self) -> Result<()> {
        self.node
            .send_to_all(
                self.node_raft.raft_peer_addrs().copied(),
                MempoolRequest::CoordinatedResume,
            )
            .await?;
        Ok(())
    }

    /// Execute the initialization of a shared config application
    /// by sending the shared config to peers
    pub async fn initiate_send_shared_config(
        &mut self,
        shared_config: MempoolNodeSharedConfig,
    ) -> Result<()> {
        self.node
            .send_to_all(
                self.node_raft.raft_peer_addrs().copied(),
                MempoolRequest::SendSharedConfig { shared_config },
            )
            .await?;
        Ok(())
    }

    /// Proposes miners from the request list to the Raft layer for the given block number.
    async fn propose_miners_from_request_list(&mut self, block_num: u64, request_list: &BTreeSet<SocketAddr>) {
        for &miner in request_list {
            let proposal_item = MiningPipelineItem::MiningParticipant(miner, MiningPipelineStatus::ParticipantOnlyIntake);
            let raft_item_to_check = MempoolRaftItem::PipelineItem(proposal_item.clone(), block_num);

            // Check if this specific proposal is already in flight for this block number
            if self.node_raft.is_proposal_in_flight(&raft_item_to_check, block_num) {
                trace!("Proposal for miner {} (block {}) already in flight, skipping.", miner, block_num);
                continue;
            }

            info!("Proposing MiningParticipant for {} (block {})", miner, block_num);
            // Use propose_pipeline_item_with_block_num as mining block might not be set synchronously
            if !self.node_raft.propose_pipeline_item_with_block_num(
                proposal_item,
                block_num
            ).await {
                warn!("Failed to propose miner {} for block {} (unexpected)", miner, block_num);
            }
        }
    }
}

impl MempoolInterface for MempoolNode {
    fn fetch_utxo_set(
        &mut self,
        peer: SocketAddr,
        address_list: UtxoFetchType,
        node_type: NodeType,
    ) -> Response {
        self.fetched_utxo_set = match address_list {
            UtxoFetchType::AnyOf(addresses) => {
                let utxo_set = self.get_committed_utxo_set();
                let utxo_tracked_set = self.get_committed_utxo_tracked_set_to_send();
                let utxo_subset: UtxoSet = addresses
                    .iter()
                    .filter_map(|v| utxo_tracked_set.get_pk_cache_vec(v))
                    .flatten()
                    .filter_map(|op| {
                        utxo_set
                            .get_key_value(op)
                            .map(|(k, v)| (k.clone(), v.clone()))
                    })
                    .collect();
                Some((peer, node_type, utxo_subset))
            }
            _ => None,
        };
        Response {
            success: true,
            reason: "Received UTXO fetch request".to_owned(),
        }
    }

    fn partition(&self, _uuids: Vec<&'static str>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_owned(),
        }
    }

    fn get_service_levels(&self) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_owned(),
        }
    }

    fn execute_contract(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet".to_owned(),
        }
    }

    fn get_next_block_reward(&self) -> f64 {
        0.0
    }
}

impl MempoolApi for MempoolNode {
    fn get_shared_config(&self) -> MempoolNodeSharedConfig {
        MempoolNodeSharedConfig {
            mempool_mining_event_timeout: self.node_raft.get_mempool_mining_event_timeout(),
            mempool_partition_full_size: self.node_raft.get_mempool_partition_full_size(),
            mempool_miner_whitelist: self.node_raft.get_mempool_miner_whitelist(),
        }
    }

    fn get_transaction_status(&self, tx_hashes: Vec<String>) -> BTreeMap<String, TxStatus> {
        self.get_transaction_status(tx_hashes)
    }

    fn get_committed_utxo_tracked_set(&self) -> &TrackedUtxoSet {
        self.node_raft.get_committed_utxo_tracked_set()
    }

    fn get_pending_druid_pool(&self) -> &DruidPool {
        self.get_pending_druid_pool()
    }

    fn get_issued_supply(&self) -> TokenAmount {
        *self.node_raft.get_current_issuance()
    }

    fn receive_transactions(&mut self, transactions: Vec<Transaction>) -> Response {
        self.receive_transactions(transactions)
    }

    fn create_item_asset_tx(
        &mut self,
        item_amount: u64,
        script_public_key: String,
        public_key: String,
        signature: String,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    ) -> Result<(Transaction, String)> {
        self.create_item_asset_tx(
            item_amount,
            script_public_key,
            public_key,
            signature,
            genesis_hash_spec,
            metadata,
        )
    }

    fn pause_nodes(&mut self, b_num: u64) -> Response {
        if self
            .inject_next_event(
                self.local_address(),
                MempoolRequest::CoordinatedPause { b_num },
            )
            .is_err()
        {
            return Response {
                success: false,
                reason: "Failed to initiate coordinated pause".to_owned(),
            };
        }
        Response {
            success: true,
            reason: "Attempt coordinated node pause".to_owned(),
        }
    }

    fn resume_nodes(&mut self) -> Response {
        if self
            .inject_next_event(self.local_address(), MempoolRequest::CoordinatedResume)
            .is_err()
        {
            return Response {
                success: false,
                reason: "Failed to initiate coordinated resume".to_owned(),
            };
        }
        Response {
            success: true,
            reason: "Attempt coordinated node resume".to_owned(),
        }
    }

    fn send_shared_config(
        &mut self,
        shared_config: crate::configurations::MempoolNodeSharedConfig,
    ) -> Response {
        if self
            .inject_next_event(
                self.local_address(),
                MempoolRequest::SendSharedConfig { shared_config },
            )
            .is_err()
        {
            return Response {
                success: false,
                reason: "Failed to initiate sharing of config".to_owned(),
            };
        }
        Response {
            success: true,
            reason: "Attempt send shared config".to_owned(),
        }
    }
}

/// Get pending transactions
///
/// ### Arguments
///
/// * `db`             - Database
fn get_local_transactions(db: &SimpleDb) -> BTreeMap<String, Transaction> {
    db.iter_cf_clone(DB_COL_LOCAL_TXS)
        .map(|(k, v)| (String::from_utf8(k), deserialize(&v)))
        .map(|(k, v)| (k.unwrap(), v.unwrap()))
        .collect()
}

/// Add pending transactions
///
/// ### Arguments
///
/// * `db`             - Database
/// * `transactions`   - Transactions to store
fn store_local_transactions(db: &mut SimpleDb, transactions: &BTreeMap<String, Transaction>) {
    let mut batch = db.batch_writer();
    for (key, value) in transactions {
        let value = serialize(value).unwrap();
        batch.put_cf(DB_COL_LOCAL_TXS, key, &value);
    }
    let batch = batch.done();
    db.write(batch).unwrap();
}

/// Delete no longer relevant transaction
///
/// ### Arguments
///
/// * `db`     - Database
/// * `keys`   - Keys to delete
fn delete_local_transactions(db: &mut SimpleDb, keys: &[String]) {
    let mut batch = db.batch_writer();
    for key in keys {
        batch.delete_cf(DB_COL_LOCAL_TXS, key);
    }
    let batch = batch.done();
    db.write(batch).unwrap();
}

