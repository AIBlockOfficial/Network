use crate::comms_handler::{CommsError, Event};
use crate::compute_raft::{CommittedItem, ComputeRaft};
use crate::configurations::{ComputeNodeConfig, ExtraNodeParams};
use crate::constants::{DB_PATH, PEER_LIMIT, UNICORN_LIMIT};
use crate::db_utils::{self, SimpleDb};
use crate::hash_block::HashBlock;
use crate::interfaces::{
    BlockStoredInfo, CommonBlockInfo, ComputeInterface, ComputeRequest, Contract, MineRequest,
    MinedBlockExtraInfo, NodeType, ProofOfWork, Response, StorageRequest, UserRequest, UtxoSet,
};
use crate::unicorn::UnicornShard;
use crate::utils::{
    concat_merkle_coinbase, format_parition_pow_address, get_partition_entry_key,
    serialize_hashblock_for_pow, validate_pow_block, validate_pow_for_address,
};
use crate::Node;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use naom::primitives::asset::TokenAmount;
use naom::primitives::block::Block;
use naom::primitives::transaction::{OutPoint, Transaction};
use naom::primitives::transaction_utils::construct_tx_hash;
use naom::script::utils::tx_is_valid;
use rand::{self, Rng};
use serde::Serialize;
use sodiumoxide::crypto::secretbox::Key;
use std::collections::{BTreeMap, BTreeSet};
use std::{
    collections::HashMap,
    error::Error,
    fmt,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::task;
use tracing::{debug, error, error_span, info, trace, warn};
use tracing_futures::Instrument;

/// Key for local miner list
pub const REQUEST_LIST_KEY: &str = "RequestListKey";

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, ComputeError>;

#[derive(Debug)]
pub enum ComputeError {
    ConfigError(&'static str),
    Network(CommsError),
    Serialization(bincode::Error),
    AsyncTask(task::JoinError),
}

impl fmt::Display for ComputeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::AsyncTask(err) => write!(f, "Async task error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for ComputeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::AsyncTask(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
        }
    }
}

impl From<CommsError> for ComputeError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<bincode::Error> for ComputeError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

impl From<task::JoinError> for ComputeError {
    fn from(other: task::JoinError) -> Self {
        Self::AsyncTask(other)
    }
}

/// Druid pool structure for checking and holding participants
#[derive(Debug, Clone)]
pub struct MinedBlock {
    pub nonce: Vec<u8>,
    pub block: Block,
    pub block_tx: BTreeMap<String, Transaction>,
    pub mining_transaction: (String, Transaction),
}

/// Druid pool structure for checking and holding participants
#[derive(Debug, Clone)]
pub struct DruidDroplet {
    participants: usize,
    tx: BTreeMap<String, Transaction>,
}

#[derive(Debug)]
pub struct ComputeNode {
    node: Node,
    node_raft: ComputeRaft,
    db: SimpleDb,
    pub jurisdiction: String,
    pub current_mined_block: Option<MinedBlock>,
    pub druid_pool: BTreeMap<String, DruidDroplet>,
    pub unicorn_limit: usize,
    pub current_random_num: Vec<u8>,
    pub last_coinbase_hash: Option<(SocketAddr, String, bool)>,
    pub partition_key: Option<Key>,
    pub partition_list: (Vec<ProofOfWork>, Vec<SocketAddr>),
    pub partition_full_size: usize,
    pub request_list: BTreeSet<SocketAddr>,
    pub request_list_first_flood: Option<usize>,
    pub storage_addr: SocketAddr,
    pub unicorn_list: HashMap<SocketAddr, UnicornShard>,
    pub sanction_list: Vec<String>,
    pub user_notification_list: BTreeSet<SocketAddr>,
}

impl ComputeNode {
    /// Generates a new compute node instance
    /// ### Arguments
    /// * `config` - ComputeNodeConfig for the current compute node containing compute nodes and storage nodes
    pub async fn new(config: ComputeNodeConfig, mut extra: ExtraNodeParams) -> Result<Self> {
        let addr = config
            .compute_nodes
            .get(config.compute_node_idx)
            .ok_or(ComputeError::ConfigError("Invalid compute index"))?
            .address;
        let storage_addr = config
            .storage_nodes
            .get(config.compute_node_idx)
            .ok_or(ComputeError::ConfigError("Invalid storage index"))?
            .address;

        let node = Node::new(addr, PEER_LIMIT, NodeType::Compute).await?;
        let node_raft = ComputeRaft::new(&config, extra.raft_db.take()).await;

        let db = extra
            .db
            .take()
            .unwrap_or_else(|| db_utils::new_db(config.compute_db_mode, DB_PATH, ".compute"));

        Ok(ComputeNode {
            node,
            node_raft,
            db,
            unicorn_list: HashMap::new(),
            unicorn_limit: UNICORN_LIMIT,
            current_mined_block: None,
            druid_pool: BTreeMap::new(),
            current_random_num: Self::generate_random_num(),
            last_coinbase_hash: None,
            request_list: Default::default(),
            sanction_list: config.sanction_list,
            jurisdiction: config.jurisdiction,
            request_list_first_flood: Some(config.compute_minimum_miner_pool_len),
            partition_full_size: config.compute_partition_full_size,
            partition_list: (Vec::new(), Vec::new()),
            partition_key: None,
            storage_addr,
            user_notification_list: Default::default(),
        }
        .load_local_db()?)
    }

    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Check if the node has a current block.
    pub fn has_current_mined_block(&self) -> bool {
        self.current_mined_block.is_some()
    }

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

    /// Propose initial block when ready
    pub async fn propose_initial_uxto_set(&mut self) {
        self.node_raft.propose_initial_uxto_set().await;
    }

    /// The current utxo_set including block being mined and previous block mining txs.
    pub fn get_committed_utxo_set(&self) -> &UtxoSet {
        &self.node_raft.get_committed_utxo_set()
    }

    /// The current tx_pool that will be used to generate next block
    pub fn get_committed_tx_pool(&self) -> &BTreeMap<String, Transaction> {
        self.node_raft.get_committed_tx_pool()
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

    /// Processes a dual double entry transaction
    /// ### Arguments
    /// * `transaction` - Transaction to process
    pub async fn process_dde_tx(&mut self, transaction: Transaction) -> Response {
        if let Some(druid) = transaction.clone().druid {
            // If this transaction is meant to join others
            #[allow(clippy::map_entry)]
            if self.druid_pool.contains_key(&druid) {
                self.process_tx_druid(druid, transaction);
                self.node_raft.propose_local_druid_transactions().await;

                return Response {
                    success: true,
                    reason: "Transaction added to corresponding DRUID droplets",
                };

            // If we haven't seen this DRUID yet
            } else {
                let mut droplet = DruidDroplet {
                    participants: transaction.druid_participants.unwrap(),
                    tx: BTreeMap::new(),
                };

                let tx_hash = construct_tx_hash(&transaction);
                droplet.tx.insert(tx_hash, transaction);

                self.druid_pool.insert(druid, droplet);
                return Response {
                    success: true,
                    reason: "Transaction added to DRUID pool. Awaiting other parties",
                };
            }
        }

        Response {
            success: false,
            reason: "Dual double entry transaction doesn't contain a DRUID",
        }
    }

    /// Processes a dual double entry transaction's DRUID with the current pool
    /// ### Arguments
    /// * `druid`       - DRUID to match on
    /// * `transaction` - Transaction to process
    pub fn process_tx_druid(&mut self, druid: String, transaction: Transaction) {
        let mut current_droplet = self.druid_pool.get(&druid).unwrap().clone();
        let tx_hash = construct_tx_hash(&transaction);
        current_droplet.tx.insert(tx_hash, transaction);

        // Execute the tx if it's ready
        if current_droplet.tx.len() == current_droplet.participants {
            self.execute_dde_tx(current_droplet);
            let _removal = self.druid_pool.remove(&druid);
        }
    }

    /// Executes a waiting dual double entry transaction that is ready to execute
    /// ### Arguments
    /// * `droplet`  - DRUID droplet of transactions to execute
    pub fn execute_dde_tx(&mut self, droplet: DruidDroplet) {
        let mut txs_valid = true;

        for tx in droplet.tx.values() {
            let utxo_set = self.node_raft.get_committed_utxo_set();

            if !tx_is_valid(&tx, |v| utxo_set.get(&v)) {
                txs_valid = false;
                break;
            }
        }

        if txs_valid {
            self.node_raft.append_to_tx_druid_pool(droplet.tx);

            trace!(
                "Transactions for dual double entry execution are valid. Adding to pending block"
            );
        } else {
            debug!("Transactions for dual double entry execution are invalid");
        }
    }

    ///Returns the mining block from the node_raft
    pub fn get_mining_block(&self) -> &Option<Block> {
        self.node_raft.get_mining_block()
    }

    /// Sets the commited mining block to the given block and transaction BTreeMap
    /// ### Arguments
    /// * `block`  - Block to be set to commited mining block
    /// * `block_tx` - BTreeMap of the block transactions
    pub fn set_committed_mining_block(
        &mut self,
        block: Block,
        block_tx: BTreeMap<String, Transaction>,
    ) {
        self.node_raft.set_committed_mining_block(block, block_tx)
    }

    /// Generates a garbage random num for use in network testing
    fn generate_random_num() -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..10).map(|_| rng.gen_range(1, 200)).collect()
    }

    /// Gets a decremented socket address of peer for storage
    /// ### Arguments
    /// * `address`    - Address to decrement
    fn get_storage_address(&self, address: SocketAddr) -> SocketAddr {
        let mut storage_address = address;
        storage_address.set_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
        storage_address.set_port(address.port() - 1);

        storage_address
    }

    /// Util function to get a socket address for PID table checks
    /// ### Arguments
    /// * `address`    - Peer's address
    fn get_comms_address(&self, address: SocketAddr) -> SocketAddr {
        let comparison_port = address.port() + 1;
        let mut comparison_addr = address;

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Sends notification of a block found and stored to the winner.
    pub async fn send_bf_notification(&mut self) -> Result<bool> {
        let last_winer = self.last_coinbase_hash.take();
        info!("send_bf_notification {:?}", last_winer);

        if let Some((peer, win_coinbase, true)) = last_winer {
            // TODO: Allow resend winner notification if error.
            self.node
                .send(peer, MineRequest::NotifyBlockFound { win_coinbase })
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Sends the latest block to storage
    pub async fn send_block_to_storage(&mut self) -> Result<()> {
        // Only the first call will send to storage.
        let mined_block = self.current_mined_block.take().unwrap();
        let block = mined_block.block;
        let block_txs = mined_block.block_tx;
        let nonce = mined_block.nonce;
        let mining_tx = mined_block.mining_transaction;

        let request = StorageRequest::SendBlock {
            common: CommonBlockInfo { block, block_txs },
            mined_info: MinedBlockExtraInfo { nonce, mining_tx },
        };
        self.node.send(self.storage_addr, request).await?;

        Ok(())
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

    /// Listens for new events from peers and handles them, processing any errors.
    pub async fn handle_next_event_response(&mut self, response: Result<Response>) {
        debug!("Response: {:?}", response);

        match response {
            Ok(Response {
                success: true,
                reason: "Received partition request successfully",
            }) => {}
            Ok(Response {
                success: true,
                reason: "Received first full partition request",
            }) => {
                self.propose_initial_uxto_set().await;
            }
            Ok(Response {
                success: true,
                reason: "Partition list is full",
            }) => {
                self.flood_list_to_partition().await.unwrap();
                self.flood_block_to_partition().await.unwrap();
                self.flood_block_to_users().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason: "Received PoW successfully",
            }) => {
                info!("Send Block to storage");
                debug!("CURRENT MINED BLOCK: {:?}", self.current_mined_block);
                if let Err(e) = self.send_block_to_storage().await {
                    error!("Block not sent to storage {:?}", e);
                }
            }
            Ok(Response {
                success: true,
                reason: "Transactions added to tx pool",
            }) => {
                debug!("Transactions received and processed successfully");
            }
            Ok(Response {
                success: true,
                reason: "First Block committed",
            }) => {
                debug!("First Block ready to mine: {:?}", self.get_mining_block());
                self.flood_rand_num_to_requesters().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason: "Block committed",
            }) => {
                debug!("Block ready to mine: {:?}", self.get_mining_block());
                if let Err(e) = self.send_bf_notification().await {
                    error!("Could not send block found notification to winner {:?}", e);
                }
                self.flood_rand_num_to_requesters().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason: "Transactions committed",
            }) => {
                debug!("Transactions ready to be used in next block");
            }
            Ok(Response {
                success: true,
                reason: "Received block stored",
            }) => {
                info!("Block info received from storage: ready to generate block");
            }
            Ok(Response {
                success: true,
                reason: "Snapshot applied",
            }) => {
                warn!("Snapshot applied");
            }
            Ok(Response {
                success: true,
                reason: "Received block notification",
            }) => {}
            Ok(Response {
                success: true,
                reason: "Partition PoW received successfully",
            }) => {}
            Ok(Response {
                success: false,
                reason: "Partition list is already full",
            }) => {}
            Ok(Response {
                success: false,
                reason: "No block to mine currently",
            }) => {}
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
        }
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event(&mut self) -> Option<Result<Response>> {
        loop {
            // Process pending submission.
            self.node_raft.propose_block_with_last_info().await;

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
                        Some(CommittedItem::FirstBlock) => {
                            self.node_raft.generate_first_block();
                            self.node_raft.event_processed_generate_snapshot();
                            return Some(Ok(Response{
                                success: true,
                                reason: "First Block committed",
                            }));
                        }
                        Some(CommittedItem::Block) => {
                            self.validate_wining_miner_tx();
                            self.node_raft.generate_block().await;
                            self.node_raft.event_processed_generate_snapshot();
                            return Some(Ok(Response{
                                success: true,
                                reason: "Block committed",
                            }));
                        }
                        Some(CommittedItem::Snapshot) => {
                            return Some(Ok(Response{
                                success: true,
                                reason: "Snapshot applied",
                            }));
                        }
                        Some(CommittedItem::Transactions) => {
                            return Some(Ok(Response{
                                success: true,
                                reason: "Transactions committed",
                            }));
                        }
                        None => {}
                    }
                }
                Some((addr, msg)) = self.node_raft.next_msg() => {
                    trace!("handle_next_event msg {:?}: {:?}", addr, msg);
                    match self.node.send(
                        addr,
                        ComputeRequest::SendRaftCmd(msg)).await {
                            Err(e) => info!("Msg not sent to {}, from {}: {:?}", addr, self.address(), e),
                            Ok(()) => trace!("Msg sent to {}, from {}", addr, self.address()),
                        };
                }
                _ = self.node_raft.timeout_propose_transactions() => {
                    trace!("handle_next_event timeout transactions");
                    self.node_raft.propose_local_transactions_at_timeout().await;
                    self.node_raft.propose_local_druid_transactions().await;
                }
            }
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
        let req = deserialize::<ComputeRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        debug!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a compute request.
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    /// * 'req' - ComputeRequest object holding the request
    async fn handle_request(&mut self, peer: SocketAddr, req: ComputeRequest) -> Option<Response> {
        use ComputeRequest::*;
        trace!("handle_request");

        match req {
            SendBlockStored(info) => Some(self.receive_block_stored(peer, info)),
            SendPoW { nonce, coinbase } => Some(self.receive_pow(peer, nonce, coinbase)),
            SendPartitionEntry { partition_entry } => {
                Some(self.receive_partition_entry(peer, partition_entry))
            }
            SendTransactions { transactions } => Some(self.receive_transactions(transactions)),
            SendUserBlockNotificationRequest => {
                Some(self.receive_block_user_notification_request(peer))
            }
            SendPartitionRequest => Some(self.receive_partition_request(peer)),
            SendRaftCmd(msg) => {
                self.node_raft.received_message(msg).await;
                None
            }
        }
    }

    /// Receive a block notification request from a user node
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    fn receive_block_user_notification_request(&mut self, peer: SocketAddr) -> Response {
        self.user_notification_list.insert(peer);
        Response {
            success: true,
            reason: "Received block notification",
        }
    }

    /// Receive a partition request from a miner node
    /// TODO: This may need to be part of the ComputeInterface depending on key agreement
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    fn receive_partition_request(&mut self, peer: SocketAddr) -> Response {
        self.request_list.insert(peer);
        self.db
            .put(REQUEST_LIST_KEY, &serialize(&self.request_list).unwrap())
            .unwrap();
        if self.request_list_first_flood == Some(self.request_list.len()) {
            self.request_list_first_flood = None;
            Response {
                success: true,
                reason: "Received first full partition request",
            }
        } else {
            Response {
                success: true,
                reason: "Received partition request successfully",
            }
        }
    }

    /// Receives the light POW for partition inclusion
    /// ### Arguments
    ///
    /// * `peer` - Sending peer's socket address
    /// * 'partition_entry' - ProofOfWork object for the partition entry being recieved.
    fn receive_partition_entry(
        &mut self,
        peer: SocketAddr,
        partition_entry: ProofOfWork,
    ) -> Response {
        if self.partition_list.0.len() >= self.partition_full_size {
            return Response {
                success: false,
                reason: "Partition list is already full",
            };
        }

        if format_parition_pow_address(peer) != partition_entry.address
            || !validate_pow_for_address(&partition_entry, &Some(&self.current_random_num))
        {
            return Response {
                success: false,
                reason: "PoW received is invalid",
            };
        }

        self.partition_list.0.push(partition_entry);
        self.partition_list.1.push(peer);

        if self.partition_list.0.len() < self.partition_full_size {
            return Response {
                success: true,
                reason: "Partition PoW received successfully",
            };
        }

        self.partition_key = Some(get_partition_entry_key(&self.partition_list.0));

        Response {
            success: true,
            reason: "Partition list is full",
        }
    }

    /// Floods the random number to everyone who requested
    pub async fn flood_rand_num_to_requesters(&mut self) -> Result<()> {
        let rnum = self.current_random_num.clone();
        info!("RANDOM NUMBER IN COMPUTE: {:?}", rnum);

        self.node
            .send_to_all(
                self.request_list.iter().copied(),
                MineRequest::SendRandomNum { rnum },
            )
            .await
            .unwrap();

        Ok(())
    }

    /// Floods the current block to participants for mining
    pub async fn flood_block_to_partition(&mut self) -> Result<()> {
        info!("BLOCK TO SEND: {:?}", self.node_raft.get_mining_block());
        let block: &Block = self.node_raft.get_mining_block().as_ref().unwrap();
        let header = block.header.clone();

        let unicorn = if let Some(h) = header.previous_hash {
            h
        } else {
            "".to_string()
        };
        let hashblock = HashBlock::new_for_mining(unicorn, header.merkle_root_hash, header.b_num);
        let block = serialize_hashblock_for_pow(&hashblock);

        self.node
            .send_to_all(
                self.partition_list.1.iter().copied(),
                MineRequest::SendBlock { block },
            )
            .await
            .unwrap();

        Ok(())
    }

    /// Floods the current block to user listening for updates
    pub async fn flood_block_to_users(&mut self) -> Result<()> {
        let block: Block = self.node_raft.get_mining_block().clone().unwrap();

        self.node
            .send_to_all(
                self.user_notification_list.iter().copied(),
                UserRequest::BlockMining { block },
            )
            .await
            .unwrap();

        Ok(())
    }

    /// Floods all peers with the full partition list
    pub async fn flood_list_to_partition(&mut self) -> Result<()> {
        self.node
            .send_to_all(
                self.partition_list.1.iter().copied(),
                MineRequest::SendPartitionList {
                    p_list: self.partition_list.0.clone(),
                },
            )
            .await
            .unwrap();
        Ok(())
    }

    /// Logs the winner of the block and changes the current block to a new block to be mined
    /// ### Arguments
    ///
    /// * `nonce` - Sequence number in a Vec<u8>
    /// * `miner` - address of the miner who won the block
    /// * `mining_transaction` - String and transaction to be put into a BTreeMap    
    pub fn mining_block_mined(
        &mut self,
        nonce: Vec<u8>,
        miner: SocketAddr,
        mining_transaction: (String, Transaction),
    ) {
        // Take mining block info: no more mining for it.
        let (block, block_tx) = self.node_raft.take_mining_block();
        self.current_random_num = Self::generate_random_num();
        self.partition_list = (Vec::new(), Vec::new());
        self.partition_key = None;

        // Update latest coinbase to notify winner
        self.last_coinbase_hash = Some((miner, mining_transaction.0.clone(), false));

        // Set mined block
        self.current_mined_block = Some(MinedBlock {
            nonce,
            block,
            block_tx,
            mining_transaction,
        });
    }

    ///Validates the miner who won as the winner of the block.
    fn validate_wining_miner_tx(&mut self) {
        if let Some((miner, tx_hash, false)) = self.last_coinbase_hash.take() {
            let utxo_set = self.node_raft.get_committed_utxo_set();
            let out_point = OutPoint::new(tx_hash, 0);
            if utxo_set.contains_key(&out_point) {
                self.last_coinbase_hash = Some((miner, out_point.t_hash, true));
            }
        }
    }

    /// Checks whether a transaction's locktime has expired
    ///
    /// ### Arguments
    ///
    /// * `utxo_set`    - UTXO set
    /// * `tx`          - Transaction to check
    fn lock_expired(&self, utxo_set: &UtxoSet, tx: &OutPoint) -> bool {
        let mut lock_expiry = false;

        if let Some(txout) = utxo_set.get(tx) {
            if txout.locktime == 0 {
                lock_expiry = true;
            } else if self.current_mined_block.is_some() {
                let block_height = self
                    .current_mined_block
                    .as_ref()
                    .unwrap()
                    .block
                    .header
                    .b_num;
                if block_height >= txout.locktime {
                    lock_expiry = true;
                }
            }
        }

        lock_expiry
    }

    /// Load and apply the local database to our state
    fn load_local_db(mut self) -> Result<Self> {
        self.request_list = match self.db.get(REQUEST_LIST_KEY) {
            Ok(Some(list)) => {
                let list = deserialize::<BTreeSet<SocketAddr>>(&list).unwrap();
                debug!("load_local_db: request_list {:?}", list);
                list
            }
            Ok(None) => self.request_list,
            Err(e) => panic!("Error accessing db: {:?}", e),
        };
        if let Some(first) = self.request_list_first_flood {
            if first < self.request_list.len() {
                self.request_list_first_flood = None;
            }
        }
        Ok(self)
    }
}

impl ComputeInterface for ComputeNode {
    /// recieves commit of ProofOfWork
    ///
    /// ### Arguments
    ///
    /// * `address`    - Address of the node commiting the proof of work
    /// * `commit`     - ProofOfWork for a block coming from address address
    fn receive_commit(&mut self, address: SocketAddr, commit: ProofOfWork) -> Response {
        if let Some(entry) = self.unicorn_list.get_mut(&address) {
            if entry.is_valid(commit.clone()) {
                self.flood_commit_to_peers(address, &commit);
                return Response {
                    success: true,
                    reason: "Commit received successfully",
                };
            }

            return Response {
                success: false,
                reason: "Commit not valid. Rejecting...",
            };
        }

        Response {
            success: false,
            reason: "The node submitting a commit never submitted a promise",
        }
    }

    /// Recieves a ProofOfWork from miner
    ///
    /// ### Arguments
    ///
    /// * `address`    - Address of miner
    /// * `nonce`          - Sequenc number of the block held in a Vec<u8>
    /// * 'coinbase' - The transaction object  of the mining
    fn receive_pow(
        &mut self,
        address: SocketAddr,
        nonce: Vec<u8>,
        coinbase: Transaction,
    ) -> Response {
        info!(?address, "Received PoW");
        let coinbase_hash = construct_tx_hash(&coinbase);
        let mut block_to_check = HashBlock::new();

        if let Some(mining_block) = self.node_raft.get_mining_block() {
            let prev_hash = if let Some(h) = &mining_block.header.previous_hash {
                h.clone()
            } else {
                "".to_string()
            };

            // Update block to check
            block_to_check.merkle_hash = mining_block.header.merkle_root_hash.clone();
            block_to_check.unicorn = prev_hash;
            block_to_check.nonce = nonce.clone();
            block_to_check.b_num = mining_block.header.b_num;
        } else {
            return Response {
                success: false,
                reason: "No block to mine currently",
            };
        };

        // Check coinbase amount and structure
        let coinbase_amount = TokenAmount(12000);
        if !coinbase.is_coinbase() || coinbase.outputs[0].amount != coinbase_amount {
            return Response {
                success: false,
                reason: "Coinbase transaction invalid",
            };
        }

        // Perform validation
        let merkle_for_pow = concat_merkle_coinbase(&block_to_check.merkle_hash, &coinbase_hash);
        if !validate_pow_block(&block_to_check.unicorn, &merkle_for_pow, &nonce) {
            return Response {
                success: false,
                reason: "Invalid PoW for block",
            };
        }

        self.mining_block_mined(nonce, address, (coinbase_hash, coinbase));

        Response {
            success: true,
            reason: "Received PoW successfully",
        }
    }

    /// Returns the internal unicorn table in a Vector of the UnicornShards from unicorn_list.
    /// The unicorn_list is a HashMap of the socketAddress and the UnicornShard
    fn get_unicorn_table(&self) -> Vec<UnicornShard> {
        let mut unicorn_table = Vec::with_capacity(self.unicorn_list.len());

        for unicorn in self.unicorn_list.values() {
            unicorn_table.push(unicorn.clone());
        }

        unicorn_table
    }

    fn partition(&self, _uuids: Vec<&'static str>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn get_service_levels(&self) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_block_stored(
        &mut self,
        peer: SocketAddr,
        previous_block_info: BlockStoredInfo,
    ) -> Response {
        if peer != self.storage_addr {
            return Response {
                success: false,
                reason: "Received block stored not from our storage peer",
            };
        }

        self.node_raft.append_block_stored_info(previous_block_info);

        Response {
            success: true,
            reason: "Received block stored",
        }
    }

    fn receive_transactions(&mut self, transactions: BTreeMap<String, Transaction>) -> Response {
        if !self.node_raft.tx_pool_can_accept(transactions.len()) {
            return Response {
                success: false,
                reason: "Transaction pool for this compute node is full",
            };
        }

        let utxo_set = self.node_raft.get_committed_utxo_set();
        let valid_tx: BTreeMap<_, _> = transactions
            .iter()
            .filter(|(_, tx)| !tx.is_coinbase())
            .filter(|(_, tx)| {
                tx_is_valid(&tx, |v| {
                    utxo_set
                        .get(&v)
                        .filter(|_| !self.sanction_list.contains(&v.t_hash))
                        .filter(|_| self.lock_expired(&utxo_set, &v))
                })
            })
            .map(|(hash, tx)| (hash.clone(), tx.clone()))
            .collect();

        // At this point the tx's are considered valid
        let valid_tx_len = valid_tx.len();
        self.node_raft.append_to_tx_pool(valid_tx);

        if valid_tx_len == 0 {
            return Response {
                success: false,
                reason: "No valid transactions provided",
            };
        }

        if valid_tx_len < transactions.len() {
            return Response {
                success: true,
                reason: "Some transactions invalid. Adding valid transactions only",
            };
        }

        Response {
            success: true,
            reason: "Transactions added to tx pool",
        }
    }

    fn execute_contract(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn get_next_block_reward(&self) -> f64 {
        0.0
    }
}
