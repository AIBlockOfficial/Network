use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::{ExtraNodeParams, StorageNodeConfig};
use crate::constants::{DB_PATH, PEER_LIMIT};
use crate::db_utils::{self, DbIteratorItem, SimpleDb};
use crate::interfaces::{
    BlockStoredInfo, CommonBlockInfo, ComputeRequest, Contract, MinedBlockExtraInfo, NodeType,
    ProofOfWork, Response, StorageInterface, StorageRequest,
};
use crate::storage_raft::{CommittedItem, CompleteBlock, StorageRaft};
use crate::utils::{concat_merkle_coinbase, get_genesis_tx_in_display, validate_pow_block};
use bincode::{deserialize, serialize};
use bytes::Bytes;
use naom::primitives::{block::Block, transaction::Transaction};
use serde::{Deserialize, Serialize};
use sha3::Digest;
use sha3::Sha3_256;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use tokio::task;
use tracing::{debug, error, error_span, info, trace, warn};
use tracing_futures::Instrument;

/// Key storing current proposer run
pub const RAFT_KEY_RUN: &str = "RaftKeyRun";

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

/// Mined block as stored in DB.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredSerializingBlock {
    pub block: Block,
    pub mining_tx_hash_and_nonces: BTreeMap<u64, (String, Vec<u8>)>,
}

#[derive(Debug)]
pub struct StorageNode {
    node: Node,
    node_raft: StorageRaft,
    db: SimpleDb,
    compute_addr: SocketAddr,
    whitelisted: HashMap<SocketAddr, bool>,
    last_block_stored: Option<BlockStoredInfo>,
}

impl StorageNode {
    ///Constructor for a new StorageNode
    ///
    /// ### Arguments
    ///
    /// * `config` - StorageNodeConfig object containing the parameters for the new StorageNode
    pub async fn new(config: StorageNodeConfig, mut extra: ExtraNodeParams) -> Result<StorageNode> {
        let addr = config
            .storage_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid storage index"))?
            .address;
        let compute_addr = config
            .compute_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid compute index"))?
            .address;

        let node = Node::new(addr, PEER_LIMIT, NodeType::Storage).await?;
        let node_raft = StorageRaft::new(&config, extra.raft_db.take());
        let db = extra
            .db
            .take()
            .unwrap_or_else(|| db_utils::new_db(config.storage_db_mode, DB_PATH, ".storage"));

        Ok(StorageNode {
            node,
            node_raft,
            db,
            compute_addr,
            whitelisted: HashMap::new(),
            last_block_stored: None,
        }
        .load_local_db()?)
    }

    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
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

    /// Return the raft loop to spawn in it own task.
    pub fn raft_loop(&self) -> impl Future<Output = ()> {
        self.node_raft.raft_loop()
    }

    /// Signal to the raft loop to complete
    pub async fn close_raft_loop(&mut self) {
        self.node_raft.close_raft_loop().await
    }

    /// Signal to the node listening loop to complete
    pub async fn stop_listening_loop(&mut self) -> Vec<task::JoinHandle<()>> {
        self.node.stop_listening().await
    }

    /// Extract persistent dbs
    pub async fn take_closed_extra_params(&mut self) -> ExtraNodeParams {
        ExtraNodeParams {
            db: Some(std::mem::replace(&mut self.db, SimpleDb::new_in_memory())),
            raft_db: Some(self.node_raft.take_closed_persistent_store().await),
        }
    }

    /// Listens for new events from peers and handles them, processing any errors.
    /// Return true when a block was stored.
    pub async fn handle_next_event_response(&mut self, response: Result<Response>) -> bool {
        debug!("Response: {:?}", response);

        match response {
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
                return true;
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

        false
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event<E: Future<Output = &'static str> + Unpin>(
        &mut self,
        exit: &mut E,
    ) -> Option<Result<Response>> {
        loop {
            // State machines are not keept between iterations or calls.
            // All selection calls (between = and =>), need to be dropable
            // i.e they should only await a channel.
            tokio::select! {
                event = self.node.next_event() => {
                    trace!("handle_next_event evt {:?}", event);
                    if let res @ Some(_) = self.handle_event(event?).await.transpose() {
                        return res;
                    }
                }
                Some(commit_data) = self.node_raft.next_commit() => {
                    trace!("handle_next_event commit {:?}", commit_data);
                    match self.node_raft.received_commit(commit_data).await {
                        Some(CommittedItem::Block) => {
                            let block = self.node_raft.generate_complete_block();
                            self.store_complete_block(block);
                            self.node_raft.event_processed_generate_snapshot();
                            return Some(Ok(Response{
                                success: true,
                                reason: "Block complete stored",
                            }));
                        }
                        Some(CommittedItem::Snapshot) => {
                            return Some(Ok(Response{
                                success: true,
                                reason: "Snapshot applied",
                            }));
                        }
                        None => (),
                    }
                }
                Some((addr, msg)) = self.node_raft.next_msg() => {
                    trace!("handle_next_event msg {:?}: {:?}", addr, msg);
                    match self.node.send(
                        addr,
                        StorageRequest::SendRaftCmd(msg)).await {
                            Err(e) => info!("Msg not sent to {}, from {}: {:?}", addr, self.address(), e),
                            Ok(()) => trace!("Msg sent to {}, from {}", addr, self.address()),
                        };

                }
                Some(_) = self.node_raft.timeout_propose_block() => {
                    trace!("handle_next_event timeout block");
                    if !self.node_raft.propose_block_at_timeout().await {
                        self.node_raft.re_propose_uncommitted_current_b_num().await;
                        self.resend_trigger_message().await;
                    }
                }
                reason = &mut *exit => {
                    return Some(Ok(Response {
                        success: true,
                        reason,
                    }));
                }
            }
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
            GetHistory {
                start_time,
                end_time,
            } => Some(self.get_history(&start_time, &end_time)),
            GetUnicornTable { n_last_items } => Some(self.get_unicorn_table(n_last_items)),
            SendPow { pow } => Some(self.receive_pow(pow)),
            SendBlock { common, mined_info } => self.receive_block(peer, common, mined_info).await,
            Store { incoming_contract } => Some(self.receive_contracts(incoming_contract)),
            SendRaftCmd(msg) => {
                self.node_raft.received_message(msg).await;
                None
            }
        }
    }

    ///Stores a completed block including transactions and mining transactions.
    ///
    /// ### Arguments
    ///
    /// * `complete` - CompleteBlock object to be stored.
    fn store_complete_block(&mut self, complete: CompleteBlock) {
        // TODO: Makes the DB save process async
        // TODO: only accept whitelisted blocks

        // Save the complete block
        trace!("Store complete block: {:?}", complete);

        let (stored_block, block_txs, mining_transactions, block_num, merkle_hash, nonce) = {
            let CompleteBlock { common, per_node } = complete;

            let header = common.block.header.clone();
            let block_num = header.b_num;
            let merkle_hash = header.merkle_root_hash;
            let nonce = header.nonce;
            let stored_block = StoredSerializingBlock {
                block: common.block,
                mining_tx_hash_and_nonces: per_node
                    .iter()
                    .map(|(idx, v)| (*idx, (v.mining_tx.0.clone(), v.nonce.clone())))
                    .collect(),
            };
            let block_txs = common.block_txs;
            let mining_transactions: BTreeMap<_, _> =
                per_node.into_iter().map(|(_, v)| v.mining_tx).collect();

            (
                stored_block,
                block_txs,
                mining_transactions,
                block_num,
                merkle_hash,
                nonce,
            )
        };

        let block_input = serialize(&stored_block).unwrap();
        let block_hash = {
            let hash_digest = Sha3_256::digest(&block_input);
            hex::encode(hash_digest)
        };

        info!(
            "Store complete block summary: b_num={}, txs={}, mining={}, hash={}",
            block_num,
            block_txs.len(),
            mining_transactions.len(),
            block_hash
        );

        // Save Block
        self.db.put(&block_hash, &block_input).unwrap();

        // Save each transaction and mining transactions
        let all_txs = block_txs.iter().chain(&mining_transactions);
        for (tx_hash, tx_value) in all_txs {
            let tx_input = serialize(tx_value).unwrap();
            self.db.put(tx_hash, &tx_input).unwrap();
        }

        if block_num == 0 {
            // Celebrate genesis block:
            info!("!!! Stored Genesis Block !!!");
            for (hash, tx) in block_txs.iter() {
                let tx_in = get_genesis_tx_in_display(tx);
                info!("Genesis Transaction: Hash:{} -> TxIn:{}", hash, tx_in);

                for (idx, tx_out) in tx.outputs.iter().enumerate() {
                    if let Some(key) = &tx_out.script_public_key {
                        info!(
                            "Genesis entry: Index:{}, Key:{} -> Tokens:{}",
                            idx, key, tx_out.amount
                        );
                    }
                }
            }
        }

        let stored_info = BlockStoredInfo {
            block_hash,
            block_num,
            merkle_hash,
            nonce,
            mining_transactions,
        };
        self.last_block_stored = Some(stored_info);
    }

    /// Get the last block stored info to send to the compute nodes
    pub fn get_last_block_stored(&self) -> &Option<BlockStoredInfo> {
        &self.last_block_stored
    }

    /// Get the stored value at the given key
    ///
    /// ### Arguments
    ///
    /// * `key` - Given key to find the value.
    pub fn get_stored_value<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
        self.db.get(key).unwrap_or_else(|e| {
            warn!("get_stored_value error: {}", e);
            None
        })
    }

    /// Get the stored block at the given key
    ///
    /// ### Arguments
    ///
    /// * `key` - Given key to find the block.
    pub fn get_stored_block<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<StoredSerializingBlock>> {
        Ok(self
            .get_stored_value(key)
            .map(|v| deserialize::<StoredSerializingBlock>(&v))
            .transpose()?)
    }

    /// Get the stored Transaction at the given key
    ///
    /// ### Arguments
    ///
    /// * `key` - Given key used to find the transaction.
    pub fn get_stored_tx<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Transaction>> {
        Ok(self
            .get_stored_value(key)
            .map(|v| deserialize::<Transaction>(&v))
            .transpose()?)
    }

    /// Get count of all the stored values
    pub fn get_stored_values_count(&self) -> usize {
        self.db.count()
    }

    /// Get all the stored (key, values)
    pub fn get_stored_cloned_key_values(&self) -> impl Iterator<Item = DbIteratorItem> + '_ {
        self.db.iter_clone()
    }

    /// Sends the latest block to storage
    pub async fn send_stored_block(&mut self) -> Result<()> {
        // Only the first call will send to storage.
        let block = self.last_block_stored.as_ref().unwrap().clone();

        self.node
            .send(self.compute_addr, ComputeRequest::SendBlockStored(block))
            .await?;

        Ok(())
    }

    /// Re-Sends Message triggering the next step in flow
    pub async fn resend_trigger_message(&mut self) {
        if self.last_block_stored.is_some() {
            info!("Resend block stored");
            if let Err(e) = self.send_stored_block().await {
                error!("Resend block stored failed {:?}", e);
            }
        }
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
            let key_run = match self.db.get(RAFT_KEY_RUN) {
                Ok(Some(key_run)) => deserialize::<u64>(&key_run)? + 1,
                Ok(None) => 0,
                Err(e) => panic!("Error accessing db: {:?}", e),
            };
            debug!("load_local_db: key_run update to {:?}", key_run);
            if let Err(e) = self.db.put(RAFT_KEY_RUN, &serialize(&key_run)?) {
                panic!("Error accessing db: {:?}", e);
            }
            key_run
        });

        Ok(self)
    }
}

impl StorageInterface for StorageNode {
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
