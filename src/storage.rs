use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::{ExtraNodeParams, StorageNodeConfig};
use crate::constants::{
    BLOCK_PREPEND, DB_PATH, INDEXED_BLOCK_HASH_PREFIX_KEY, INDEXED_TX_HASH_PREFIX_KEY,
    LAST_BLOCK_HASH_KEY, NAMED_CONSTANT_PREPEND, PEER_LIMIT,
};
use crate::db_utils::{self, SimpleDb, SimpleDbSpec, SimpleDbWriteBatch};
use crate::interfaces::{
    BlockStoredInfo, CommonBlockInfo, ComputeRequest, Contract, MineRequest, MinedBlockExtraInfo,
    NodeType, ProofOfWork, Response, StorageInterface, StorageRequest, StoredSerializingBlock,
};
use crate::raft::RaftCommit;
use crate::storage_raft::{CommittedItem, CompleteBlock, StorageRaft};
use crate::utils::{
    concat_merkle_coinbase, get_genesis_tx_in_display, validate_pow_block, LocalEvent,
    LocalEventChannel, LocalEventSender, ResponseResult,
};
use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::Serialize;
use sha3::Digest;
use sha3::Sha3_256;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, error_span, info, trace, warn};
use tracing_futures::Instrument;

/// Key storing current proposer run
pub const RAFT_KEY_RUN: &str = "RaftKeyRun";

/// Database columns
pub const DB_COL_INTERNAL: &str = "internal";
pub const DB_COL_BC_ALL: &str = "block_chain_all";
pub const DB_COL_BC_NAMED: &str = "block_chain_named";
pub const DB_COL_BC_NOW: &str = "block_chain_v0.3.0";
pub const DB_COL_BC_V0_2_0: &str = "block_chain_v0.2.0";

pub const DB_COLS_BC: &[&str] = &[DB_COL_BC_NOW, DB_COL_BC_V0_2_0];
pub const DB_POINTER_SEPARATOR: u8 = b':';

/// Database specification
pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
    db_path: DB_PATH,
    suffix: ".storage",
    columns: &[
        DB_COL_INTERNAL,
        DB_COL_BC_ALL,
        DB_COL_BC_NAMED,
        DB_COL_BC_NOW,
        DB_COL_BC_V0_2_0,
    ],
};

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    ConfigError(&'static str),
    Network(CommsError),
    Serialization(bincode::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
        }
    }
}

impl From<CommsError> for StorageError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<bincode::Error> for StorageError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

#[derive(Debug)]
pub struct StorageNode {
    node: Node,
    node_raft: StorageRaft,
    db: Arc<Mutex<SimpleDb>>,
    local_events: LocalEventChannel,
    compute_addr: SocketAddr,
    api_addr: SocketAddr,
    whitelisted: HashMap<SocketAddr, bool>,
    shutdown_group: BTreeSet<SocketAddr>,
    specified_block_fetched: Option<(Vec<u8>, SocketAddr)>,
}

impl StorageNode {
    ///Constructor for a new StorageNode
    ///
    /// ### Arguments
    ///
    /// * `config` - StorageNodeConfig object containing the parameters for the new StorageNode
    /// * `extra`  - additional parameter for construction
    pub async fn new(config: StorageNodeConfig, mut extra: ExtraNodeParams) -> Result<StorageNode> {
        let addr = config
            .storage_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid storage index"))?
            .address;

        let api_addr = SocketAddr::new(addr.ip(), config.storage_api_port);
        let compute_addr = config
            .compute_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid compute index"))?
            .address;

        let node = Node::new(addr, PEER_LIMIT, NodeType::Storage).await?;
        let node_raft = StorageRaft::new(&config, extra.raft_db.take());
        let raw_db = db_utils::new_db(config.storage_db_mode, &DB_SPEC, extra.db.take());
        let db = Arc::new(Mutex::new(raw_db));

        let shutdown_group = {
            let compute = std::iter::once(compute_addr);
            let raft_peers = node_raft.raft_peer_addrs().copied();
            raft_peers.chain(compute).collect()
        };

        Ok(StorageNode {
            node,
            node_raft,
            db,
            api_addr,
            local_events: Default::default(),
            compute_addr,
            whitelisted: Default::default(),
            shutdown_group,
            specified_block_fetched: Default::default(),
        }
        .load_local_db()?)
    }

    /// Returns the storage node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Returns the storage node's API info
    pub fn api_inputs(&self) -> (Arc<Mutex<SimpleDb>>, SocketAddr) {
        (self.db.clone(), self.api_addr)
    }

    ///Adds a uses data as the payload to create a frame, from the peer address, in the node object of this class.
    ///
    /// ### Arguments
    ///
    /// * `from_peer_addr` - Socket address that the data was sent from.
    /// * `data` - payload used to create a new frame in the node.
    pub fn inject_next_event(
        &self,
        from_peer_addr: SocketAddr,
        data: impl Serialize,
    ) -> Result<()> {
        Ok(self.node.inject_next_event(from_peer_addr, data)?)
    }

    /// Connect info for peers on the network.
    pub fn connect_info_peers(&self) -> (Node, Vec<SocketAddr>, Vec<SocketAddr>) {
        let to_connect = self.node_raft.raft_peer_to_connect();
        let expect_connect = self.node_raft.raft_peer_addrs();
        (
            self.node.clone(),
            to_connect.copied().collect(),
            expect_connect.copied().collect(),
        )
    }

    /// Send initial requests:
    /// - None
    pub async fn send_startup_requests(&mut self) -> Result<()> {
        Ok(())
    }

    /// Local event channel.
    pub fn local_event_tx(&self) -> &LocalEventSender {
        &self.local_events.tx
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
        let mut self_db = self.db.lock().unwrap();

        ExtraNodeParams {
            db: self_db.take().in_memory(),
            raft_db: raft_db.in_memory(),
            ..Default::default()
        }
    }

    /// Listens for new events from peers and handles them, processing any errors.
    pub async fn handle_next_event_response(
        &mut self,
        response: Result<Response>,
    ) -> ResponseResult {
        debug!("Response: {:?}", response);

        match response {
            Ok(Response {
                success: true,
                reason: "Specified block fetched from storage",
            }) => {
                if let Err(e) = self.send_specified_block().await {
                    error!("Specified block not sent {:?}", e);
                }
            }
            Ok(Response {
                success: true,
                reason: "Shutdown",
            }) => {
                warn!("Shutdown now");
                return ResponseResult::Exit;
            }
            Ok(Response {
                success: true,
                reason: "Compute Shutdown",
            }) => {
                debug!("Compute shutdown");
                if self.flood_closing_events().await.unwrap() {
                    warn!("Flood closing event shutdown");
                    return ResponseResult::Exit;
                }
            }
            Ok(Response {
                success: true,
                reason: "Shutdown pending",
            }) => {}
            Ok(Response {
                success: true,
                reason: "Block received to be added",
            }) => {}
            Ok(Response {
                success: true,
                reason: "Block complete stored",
            }) => {
                info!("Block stored: Send to compute");
                if let Err(e) = self.send_stored_block().await {
                    error!("Block stored not sent {:?}", e);
                }
            }
            Ok(Response {
                success: true,
                reason: "Snapshot applied",
            }) => {
                warn!("Snapshot applied");
            }
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
                panic!("ERROR HANDLING RESPONSE: {:?}", error);
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
                Some(commit_data) = self.node_raft.next_commit() => {
                    trace!("handle_next_event commit {:?}", commit_data);
                    if let res @ Some(_) = self.handle_committed_data(commit_data).await {
                        return res;
                    }
                }
                Some((addr, msg)) = self.node_raft.next_msg(), if ready => {
                    trace!("handle_next_event msg {:?}: {:?}", addr, msg);
                    match self.node.send(
                        addr,
                        StorageRequest::SendRaftCmd(msg)).await {
                            Err(e) => info!("Msg not sent to {}, from {}: {:?}", addr, self.address(), e),
                            Ok(()) => trace!("Msg sent to {}, from {}", addr, self.address()),
                        };

                }
                Some(_) = self.node_raft.timeout_propose_block(), if ready => {
                    trace!("handle_next_event timeout block");
                    if !self.node_raft.propose_block_at_timeout().await {
                        self.node_raft.re_propose_uncommitted_current_b_num().await;
                        self.resend_trigger_message().await;
                    }
                }
                Some(event) = self.local_events.rx.recv(), if ready => {
                    if let Some(res) = self.handle_local_event(event) {
                        return Some(Ok(res));
                    }
                }
                reason = &mut *exit => return Some(Ok(Response {
                    success: true,
                    reason,
                }))
            }
        }
    }

    ///Handle a local event
    ///
    /// ### Arguments
    ///
    /// * `event` - Event to process.
    fn handle_local_event(&mut self, event: LocalEvent) -> Option<Response> {
        match event {
            LocalEvent::Exit(reason) => Some(Response {
                success: true,
                reason,
            }),
            LocalEvent::CoordinatedShutdown(_) => None,
            LocalEvent::Ignore => None,
        }
    }

    ///Handle commit data
    ///
    /// ### Arguments
    ///
    /// * `commit_data` - Commit to process.
    async fn handle_committed_data(&mut self, commit_data: RaftCommit) -> Option<Result<Response>> {
        match self.node_raft.received_commit(commit_data).await {
            Some(CommittedItem::Block) => {
                let block = self.node_raft.generate_complete_block();
                let block_stored = Self::store_complete_block(&mut self.db.lock().unwrap(), block);
                self.node_raft
                    .event_processed_generate_snapshot(block_stored);
                Some(Ok(Response {
                    success: true,
                    reason: "Block complete stored",
                }))
            }
            Some(CommittedItem::Snapshot) => Some(Ok(Response {
                success: true,
                reason: "Snapshot applied",
            })),
            None => None,
        }
    }

    /// Takes message from the event and passes it to handle_new_frame to handle the message
    ///
    /// ### Arguments
    ///
    /// * `event` - Event object containing a message from a peer.
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
    ///
    /// ### Arguments
    ///
    /// * `peer` - Socket address of the sender.
    /// * `frame` - Bytes object holding the message from the sender.
    async fn handle_new_frame(
        &mut self,
        peer: SocketAddr,
        frame: Bytes,
    ) -> Result<Option<Response>> {
        let req = deserialize::<StorageRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        trace!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a compute request.
    ///
    /// ### Arguments
    ///
    /// * `peer` - Socket address for the peer that the compute request came from.
    /// * `req` - StorageRequest object holding the compute request.
    async fn handle_request(&mut self, peer: SocketAddr, req: StorageRequest) -> Option<Response> {
        use StorageRequest::*;
        match req {
            GetSpecifiedBlock { key } => Some(self.get_specified_block(peer, &key)),
            GetHistory {
                start_time,
                end_time,
            } => Some(self.get_history(&start_time, &end_time)),
            GetUnicornTable { n_last_items } => Some(self.get_unicorn_table(n_last_items)),
            SendPow { pow } => Some(self.receive_pow(pow)),
            SendBlock { common, mined_info } => self.receive_block(peer, common, mined_info).await,
            Store { incoming_contract } => Some(self.receive_contracts(incoming_contract)),
            Closing => self.receive_closing(peer),
            SendRaftCmd(msg) => {
                self.node_raft.received_message(msg).await;
                None
            }
        }
    }

    ///Sends the latest block fetched from storage.
    pub async fn send_specified_block(&mut self) -> Result<()> {
        if let Some((block, peer)) = self.specified_block_fetched.take() {
            self.node
                .send(peer, MineRequest::SendSpecifiedBlock { block })
                .await?;
        }
        Ok(())
    }

    ///Stores a completed block including transactions and mining transactions.
    ///
    /// ### Arguments
    ///
    /// * `complete` - CompleteBlock object to be stored.
    fn store_complete_block(self_db: &mut SimpleDb, complete: CompleteBlock) -> BlockStoredInfo {
        // TODO: Makes the DB save process async
        // TODO: only accept whitelisted blocks

        // Save the complete block
        trace!("Store complete block: {:?}", complete);

        let ((stored_block, all_block_txs), (block_num, merkle_hash, nonce, shutdown)) = {
            let CompleteBlock { common, per_node } = complete;

            let header = common.block.header.clone();
            let block_num = header.b_num;
            let merkle_hash = header.merkle_root_hash;
            let nonce = header.nonce;
            let shutdown = per_node.values().all(|v| v.shutdown);
            let stored_block = StoredSerializingBlock {
                block: common.block,
                mining_tx_hash_and_nonces: per_node
                    .iter()
                    .map(|(idx, v)| (*idx, (v.mining_tx.0.clone(), v.nonce.clone())))
                    .collect(),
            };

            let mut all_block_txs = common.block_txs;
            all_block_txs.extend(per_node.into_iter().map(|(_, v)| v.mining_tx));

            let to_store = (stored_block, all_block_txs);
            let store_extra_info = (block_num, merkle_hash, nonce, shutdown);
            (to_store, store_extra_info)
        };

        let block_input = serialize(&stored_block).unwrap();
        let block_hash = {
            let hash_digest = Sha3_256::digest(&block_input);
            let mut hash_digest = hex::encode(hash_digest);
            hash_digest.insert(0, BLOCK_PREPEND as char);
            hash_digest
        };

        let last_block_stored_info = BlockStoredInfo {
            block_hash: block_hash.clone(),
            block_num,
            merkle_hash,
            nonce,
            mining_transactions: stored_block
                .mining_tx_hash_and_nonces
                .values()
                .filter_map(|(tx_hash, _)| all_block_txs.get_key_value(tx_hash))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            shutdown,
        };

        info!(
            "Store complete block summary: b_num={}, txs={}, mining={}, hash={}",
            block_num,
            stored_block.block.transactions.len(),
            last_block_stored_info.mining_transactions.len(),
            block_hash
        );

        //
        // Store to database
        //
        let mut batch = self_db.batch_writer();
        let pointer = put_to_block_chain(&mut batch, &block_hash, &block_input);
        put_named_block_to_block_chain(&mut batch, &pointer, block_num);

        let all_txs = all_ordered_stored_block_tx_hashes(
            &stored_block.block.transactions,
            &stored_block.mining_tx_hash_and_nonces,
        );
        for (tx_num, tx_hash) in all_txs {
            if let Some(tx_value) = all_block_txs.get(tx_hash) {
                let tx_input = serialize(tx_value).unwrap();
                let pointer = put_to_block_chain(&mut batch, tx_hash, &tx_input);
                put_named_tx_to_block_chain(&mut batch, &pointer, block_num, tx_num);
            } else {
                error!(
                    "Missing block {} transaction {}: \"{}\"",
                    block_num, tx_num, tx_hash
                );
            }
        }

        let batch = batch.done();
        self_db.write(batch).unwrap();

        //
        // Celebrate genesis block:
        //
        if block_num == 0 {
            info!("!!! Stored Genesis Block !!!");
            for hash in &stored_block.block.transactions {
                let tx = all_block_txs.get(hash).unwrap();
                let tx_in = get_genesis_tx_in_display(tx);
                info!("Genesis Transaction: Hash:{} -> TxIn:{}", hash, tx_in);

                for (idx, tx_out) in tx.outputs.iter().enumerate() {
                    if let Some(key) = &tx_out.script_public_key {
                        info!(
                            "Genesis entry: Index:{}, Key:{} -> Tokens:{}",
                            idx,
                            key,
                            tx_out.value.token_amount()
                        );
                    }
                }
            }
        }

        last_block_stored_info
    }

    /// Gets a value from the stored blockchain via key
    ///
    /// ### Arguments
    ///
    /// * `key` - Key for the value to retrieve
    pub fn get_stored_value<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
        get_stored_value_from_db(self.db.clone(), key)
    }

    /// Get the last block stored info to send to the compute nodes
    pub fn get_last_block_stored(&self) -> &Option<BlockStoredInfo> {
        self.node_raft.get_last_block_stored()
    }

    /// Get count of all the stored values
    pub fn get_stored_values_count(&self) -> usize {
        let db = self.db.lock().unwrap();
        db.count_cf(DB_COL_BC_ALL)
    }

    /// Sends the latest block to storage
    pub async fn send_stored_block(&mut self) -> Result<()> {
        // Only the first call will send to storage.
        let block = self.get_last_block_stored().as_ref().unwrap().clone();

        self.node
            .send(self.compute_addr, ComputeRequest::SendBlockStored(block))
            .await?;

        Ok(())
    }

    /// Re-Sends Message triggering the next step in flow
    pub async fn resend_trigger_message(&mut self) {
        if self.get_last_block_stored().is_some() {
            info!("Resend block stored");
            if let Err(e) = self.send_stored_block().await {
                error!("Resend block stored failed {:?}", e);
            }
        }
    }

    /// Floods the closing event to everyone
    pub async fn flood_closing_events(&mut self) -> Result<bool> {
        self.node
            .send_to_all(Some(self.compute_addr).into_iter(), ComputeRequest::Closing)
            .await
            .unwrap();

        self.node
            .send_to_all(
                self.node_raft.raft_peer_addrs().copied(),
                StorageRequest::Closing,
            )
            .await
            .unwrap();

        Ok(self.shutdown_group.is_empty())
    }

    /// Handles the receipt of closing event
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    fn receive_closing(&mut self, peer: SocketAddr) -> Option<Response> {
        if !self.shutdown_group.remove(&peer) {
            return None;
        }

        if peer == self.compute_addr {
            return Some(Response {
                success: true,
                reason: "Compute Shutdown",
            });
        }

        if !self.shutdown_group.is_empty() {
            return Some(Response {
                success: true,
                reason: "Shutdown pending",
            });
        }

        Some(Response {
            success: true,
            reason: "Shutdown",
        })
    }

    /// Receives the new block from the miner with permissions to write
    ///
    /// ### Arguments
    ///
    /// * `peer`       - Peer that the block is received from
    /// * `common`     - The block to be stored and checked
    /// * `mined_info` - The mining info for the block
    async fn receive_block(
        &mut self,
        peer: SocketAddr,
        common: CommonBlockInfo,
        mined_info: MinedBlockExtraInfo,
    ) -> Option<Response> {
        let valid = {
            let prev_hash = {
                let prev_hash = &common.block.header.previous_hash;
                prev_hash.as_deref().unwrap_or(&"")
            };
            let merkle_for_pow = {
                let merkle_root = &common.block.header.merkle_root_hash;
                let (mining_tx, _) = &mined_info.mining_tx;
                concat_merkle_coinbase(&merkle_root, mining_tx).await
            };
            let nonce = &mined_info.nonce;

            validate_pow_block(&prev_hash, &merkle_for_pow, nonce)
        };

        if !valid {
            return Some(Response {
                success: false,
                reason: "Block received not added. PoW invalid",
            });
        }

        if !self
            .node_raft
            .propose_received_part_block(peer, common, mined_info)
            .await
        {
            return None;
        }

        Some(Response {
            success: true,
            reason: "Block received to be added",
        })
    }

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

        Ok(self)
    }
}

impl StorageInterface for StorageNode {
    fn get_specified_block(&mut self, peer: SocketAddr, key: &str) -> Response {
        self.specified_block_fetched = Some((self.get_stored_value(key).unwrap_or_default(), peer));
        Response {
            success: true,
            reason: "Specified block fetched from storage",
        }
    }

    fn get_history(&self, _start_time: &u64, _end_time: &u64) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn whitelist(&mut self, address: SocketAddr) -> Response {
        self.whitelisted.insert(address, true);

        Response {
            success: true,
            reason: "Address added to whitelist",
        }
    }

    fn get_unicorn_table(&self, _n_last_items: Option<u64>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_pow(&self, _pow: ProofOfWork) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_contracts(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }
}

/// Add to the block chain columns
///
/// ### Arguments
///
/// * `batch` - Database writer
/// * `key`   - The key for the data
/// * `value` - The value to store
pub fn put_to_block_chain(batch: &mut SimpleDbWriteBatch, key: &str, value: &[u8]) -> Vec<u8> {
    put_to_block_chain_at(batch, DB_COL_BC_NOW, key, value)
}

/// Add to the block chain columns for specified version
///
/// ### Arguments
///
/// * `batch` - Database writer
/// * `cf`    - Column family to store the key/value
/// * `key`   - The key for the data
/// * `value` - The value to store
pub fn put_to_block_chain_at<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    batch: &mut SimpleDbWriteBatch,
    cf: &'static str,
    key: K,
    value: V,
) -> Vec<u8> {
    let key = key.as_ref();
    let value = value.as_ref();
    let pointer = version_pointer(cf, key);
    batch.put_cf(cf, key, value);
    batch.put_cf(DB_COL_BC_ALL, key, &pointer);
    pointer
}

/// Add to the block chain named column
///
/// ### Arguments
///
/// * `batch`   - Database writer
/// * `pointer` - The block version pointer
/// * `b_num`   - The block number
pub fn put_named_block_to_block_chain(batch: &mut SimpleDbWriteBatch, pointer: &[u8], b_num: u64) {
    let indexed_key = indexed_block_hash_key(b_num);
    batch.put_cf(DB_COL_BC_NAMED, LAST_BLOCK_HASH_KEY, &pointer);
    batch.put_cf(DB_COL_BC_NAMED, &indexed_key, &pointer);
}

/// Add to the block chain named column
///
/// ### Arguments
///
/// * `batch`   - Database writer
/// * `pointer` - The transaction version pointer
/// * `b_num`   - The block number
/// * `tx_num`  - The transaction index in the block
pub fn put_named_tx_to_block_chain(
    batch: &mut SimpleDbWriteBatch,
    pointer: &[u8],
    b_num: u64,
    tx_num: u32,
) {
    let indexed_key = indexed_tx_hash_key(b_num, tx_num);
    batch.put_cf(DB_COL_BC_NAMED, &indexed_key, &pointer);
}

/// Iterate on all the StoredSerializingBlock transaction hashes
/// First the transactions in provided order and then the mining txs
///
/// ### Arguments
///
/// * `transactions`              - The block transactions
/// * `mining_tx_hash_and_nonces` - The block mining transactions
pub fn all_ordered_stored_block_tx_hashes<'a>(
    transactions: &'a [String],
    mining_tx_hash_and_nonces: &'a BTreeMap<u64, (String, Vec<u8>)>,
) -> impl Iterator<Item = (u32, &'a String)> + 'a {
    let mining_hashes = mining_tx_hash_and_nonces.values().map(|(hash, _)| hash);
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
) -> Option<Vec<u8>> {
    let col_all = if key.as_ref().get(0) == Some(&NAMED_CONSTANT_PREPEND) {
        DB_COL_BC_NAMED
    } else {
        DB_COL_BC_ALL
    };
    let u_db = db.lock().unwrap();
    let pointer = u_db.get_cf(col_all, key).unwrap_or_else(|e| {
        warn!("get_stored_value error: {}", e);
        None
    })?;
    let (cf, key) = decode_version_pointer(&pointer);
    u_db.get_cf(cf, key).unwrap_or_else(|e| {
        warn!("get_stored_value error: {}", e);
        None
    })
}

/// Fetches blocks by their block numbers. Blocks which for whatever reason are
/// unretrievable will be replaced with a default (best handling?)
///
/// ### Arguments
///
/// * `nums`    - Numbers of the blocks to fetch
pub fn get_blocks_by_num(db: Arc<Mutex<SimpleDb>>, nums: Vec<u64>) -> Vec<StoredSerializingBlock> {
    nums.iter()
        .map(|num| {
            let key = indexed_block_hash_key(*num);
            let block = get_stored_value_from_db(db.clone(), key).unwrap_or_default();

            match deserialize(&block) {
                Ok(b) => b,
                Err(_) => StoredSerializingBlock::default(),
            }
        })
        .collect::<Vec<StoredSerializingBlock>>()
}

/// Version pointer for the column:key
///
/// ### Arguments
///
/// * `cf`  - Column family the data is
/// * `key` - The key for the data
fn version_pointer<K: AsRef<[u8]>>(cf: &'static str, key: K) -> Vec<u8> {
    let mut r = Vec::new();
    r.extend(cf.as_bytes());
    r.extend(&[DB_POINTER_SEPARATOR]);
    r.extend(key.as_ref());
    r
}

/// The key for indexed block
///
/// ### Arguments
///
/// * `b_num`  - The block number
fn indexed_block_hash_key(b_num: u64) -> String {
    format!("{}{:016x}", INDEXED_BLOCK_HASH_PREFIX_KEY, b_num)
}

/// The key for indexed block
///
/// ### Arguments
///
/// * `b_num`  - The block number
/// * `tx_num` - The transaction index in the block
fn indexed_tx_hash_key(b_num: u64, tx_num: u32) -> String {
    format!(
        "{}{:016x}_{:08x}",
        INDEXED_TX_HASH_PREFIX_KEY, b_num, tx_num
    )
}

/// Decodes a version pointer
///
/// ### Arguments
///
/// * `pointer`    - String to be split and decoded
pub fn decode_version_pointer(pointer: &[u8]) -> (&'static str, &[u8]) {
    let mut it = pointer.split(|c| c == &DB_POINTER_SEPARATOR);
    let cf = it.next().unwrap();
    let cf = DB_COLS_BC.iter().find(|v| v.as_bytes() == cf).unwrap();
    let key = it.next().unwrap();
    (cf, key)
}
