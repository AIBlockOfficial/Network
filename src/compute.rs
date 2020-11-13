use crate::comms_handler::{CommsError, Event};
use crate::constants::{
    BLOCK_SIZE, BLOCK_SIZE_IN_TX, MINING_DIFFICULTY, PARTITION_LIMIT, PEER_LIMIT, TX_POOL_LIMIT,
    UNICORN_LIMIT,
};
use crate::interfaces::{
    ComputeInterface, ComputeMessage, Contract, MineRequest, NodeType, ProofOfWork,
    ProofOfWorkBlock, Response, StorageRequest,
};
use crate::unicorn::UnicornShard;
use crate::utils::get_partition_entry_key;
use crate::Node;

use bincode::{deserialize, serialize};
use bytes::Bytes;
use sha3::{Digest, Sha3_256};

use sodiumoxide::crypto::secretbox::{gen_key, Key};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task;
use tokio::time::delay_for;

use std::{
    collections::HashMap,
    convert::TryInto,
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use naom::primitives::transaction_utils::{construct_tx_hash, update_utxo_set};
use naom::script::utils::tx_ins_are_valid;

use tracing::{debug, info, info_span, warn};

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, ComputeError>;

#[derive(Debug)]
pub enum ComputeError {
    Network(CommsError),
    Serialization(bincode::Error),
    AsyncTask(task::JoinError),
}

impl fmt::Display for ComputeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::AsyncTask(err) => write!(f, "Async task error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for ComputeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
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
pub struct DruidDroplet {
    participants: usize,
    tx: BTreeMap<String, Transaction>,
}

#[derive(Debug, Clone)]
pub struct ComputeNode {
    node: Node,
    pub current_block: Option<Block>,
    pub druid_pool: BTreeMap<String, DruidDroplet>,
    pub current_block_tx: BTreeMap<String, Transaction>,
    pub unicorn_limit: usize,
    pub current_random_num: Vec<u8>,
    pub last_block_hash: String,
    pub last_coinbase_hash: Option<String>,
    pub partition_key: Key,
    pub tx_pool: BTreeMap<String, Transaction>,
    pub utxo_set: Arc<Mutex<BTreeMap<String, Transaction>>>,
    pub partition_list: Vec<ProofOfWork>,
    pub request_list: BTreeMap<String, bool>,
    pub storage_addr: SocketAddr,
    pub unicorn_list: HashMap<SocketAddr, UnicornShard>,
}

impl ComputeNode {
    /// Generates a new compute node instance
    ///
    /// ### Arguments
    ///
    /// * `address` - Address for the current compute node
    pub async fn new(address: SocketAddr) -> Result<ComputeNode> {
        Ok(ComputeNode {
            node: Node::new(address, PEER_LIMIT, NodeType::Compute).await?,
            unicorn_list: HashMap::new(),
            unicorn_limit: UNICORN_LIMIT,
            current_block: None,
            current_block_tx: BTreeMap::new(),
            druid_pool: BTreeMap::new(),
            current_random_num: Vec::new(),
            tx_pool: BTreeMap::new(),
            last_block_hash: "".to_string(),
            last_coinbase_hash: None,
            utxo_set: Arc::new(Mutex::new(BTreeMap::new())),
            request_list: BTreeMap::new(),
            partition_list: Vec::new(),
            partition_key: gen_key(),
            storage_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        })
    }

    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Connect to a peer on the network.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
        Ok(())
    }

    /// Processes a dual double entry transaction
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to process
    pub fn process_dde_tx(&mut self, transaction: Transaction) -> Response {
        if let Some(druid) = transaction.clone().druid {
            // If this transaction is meant to join others
            if self.druid_pool.contains_key(&druid) {
                self.process_tx_druid(druid, transaction);

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
    ///
    /// ### Arguments
    ///
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
    ///
    /// ### Arguments
    ///
    /// * `droplet`  - DRUID droplet of transactions to execute
    pub fn execute_dde_tx(&mut self, droplet: DruidDroplet) {
        let mut txs_valid = true;

        for tx in droplet.tx.values() {
            if !tx_ins_are_valid(tx.inputs.clone(), &self.utxo_set.lock().unwrap()) {
                txs_valid = false;
                break;
            }
        }

        if txs_valid {
            for (h, tx) in &mut droplet.tx.clone().iter() {
                self.current_block
                    .as_mut()
                    .unwrap()
                    .transactions
                    .push(h.to_string());
                self.current_block_tx.insert(h.to_string(), tx.clone());
            }

            println!(
                "Transactions for dual double entry execution are valid. Adding to pending block"
            );
        } else {
            println!("Transactions for dual double entry execution are invalid");
        }
    }

    /// Processes the next batch of transactions from the floating tx pool
    /// to create the next block
    ///
    /// TODO: Label previous block time
    ///
    /// ### Arguments
    pub fn generate_block(&mut self) {
        let mut next_block = Block::new();

        // Update current_block_tx from tx pool if needed
        self.update_current_block_tx();

        let mut tx_hashes: Vec<String> = self
            .current_block_tx
            .keys()
            .into_iter()
            .map(|x| x.clone())
            .collect();

        next_block.header.time = 1;
        next_block.header.previous_hash = Some(self.last_block_hash.clone());

        if tx_hashes.len() > 0 {
            // If there are more transactions than block can handle
            if self.current_block_tx.len() > BLOCK_SIZE_IN_TX {
                while !next_block.is_full() {
                    let next_hash = tx_hashes.pop().unwrap();
                    next_block.transactions.push(next_hash);
                }

            // If there are fewer transactions than a full block
            } else {
                while tx_hashes.len() > 0 {
                    let next_hash = tx_hashes.pop().unwrap();
                    next_block.transactions.push(next_hash);
                }
            }

            self.current_block = Some(next_block);
        }
    }

    /// Updates the internal state of an empty tx list for the current block
    fn update_current_block_tx(&mut self) {
        if self.current_block_tx.len() == 0 {
            while self.current_block_tx.len() < BLOCK_SIZE_IN_TX && self.tx_pool.len() > 0 {
                let tx_hashes: Vec<String> =
                    self.tx_pool.keys().into_iter().map(|x| x.clone()).collect();

                let new_entry = self.tx_pool.get(&tx_hashes[0]).unwrap();
                self.current_block_tx
                    .insert(tx_hashes[0].clone(), new_entry.clone());
                self.tx_pool.remove(&tx_hashes[0]);
            }
        }
    }

    /// Validates PoW for a full block
    ///
    /// ### Arguments
    ///
    /// * `pow` - Proof of work, including the block
    pub fn validate_pow_block(pow: &mut ProofOfWorkBlock) -> bool {
        let mut pow_body = Bytes::from(serialize(&pow.block).unwrap()).to_vec();
        pow_body.append(&mut pow.nonce.clone());

        let pow_hash = Sha3_256::digest(&pow_body).to_vec();

        for entry in pow_hash[0..MINING_DIFFICULTY].to_vec() {
            if entry != 0 {
                return false;
            }
        }

        true
    }

    /// Generates a garbage file for use in network testing. Will save the file
    /// as the current block internally
    ///
    /// TODO: Fill with random tx hashes
    ///
    /// ### Arguments
    ///
    /// * `size`    - Size of the file in bytes
    pub fn generate_garbage_block(&mut self, size: usize) {
        let garbage_block = vec![0; size];
        // self.current_block = garbage_block;
    }

    /// Generates a garbage random num for use in network testing
    ///
    /// ### Arguments
    ///
    /// * `size`    - Size of the file in bytes
    pub fn generate_random_num(&mut self, size: usize) {
        let random_num = vec![0; size];
        self.current_random_num = random_num;
    }

    /// Gets a decremented socket address of peer for storage
    ///
    /// ### Arguments
    ///
    /// * `address`    - Address to decrement
    fn get_storage_address(&self, address: SocketAddr) -> SocketAddr {
        let mut storage_address = address.clone();
        storage_address.set_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
        storage_address.set_port(address.port() - 1);

        storage_address
    }

    /// Util function to get a socket address for PID table checks
    ///
    /// ### Arguments
    ///
    /// * `address`    - Peer's address
    fn get_comms_address(&self, address: SocketAddr) -> SocketAddr {
        let comparison_port = address.port() + 1;
        let mut comparison_addr = address.clone();

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Sends block to a mining node.
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Address to send to
    pub async fn send_block(&mut self, peer: SocketAddr) -> Result<()> {
        println!("BLOCK TO SEND: {:?}", self.current_block);
        println!("");
        let block_to_send = Bytes::from(serialize(&self.current_block).unwrap()).to_vec();

        self.node
            .send(
                peer,
                MineRequest::SendBlock {
                    block: block_to_send,
                },
            )
            .await?;
        Ok(())
    }

    /// Sends the full partition list to the participants
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Address to send to
    pub async fn send_partition_list(&mut self, peer: SocketAddr) -> Result<()> {
        self.node
            .send(
                peer,
                MineRequest::SendPartitionList {
                    p_list: self.partition_list.clone(),
                },
            )
            .await?;
        Ok(())
    }

    /// Sends notification of a block find to partition participants
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Address to send to
    pub async fn send_bf_notification(&mut self, peer: SocketAddr) -> Result<()> {
        self.node
            .send(
                peer,
                MineRequest::NotifyBlockFound {
                    win_coinbase: self.last_coinbase_hash.clone(),
                },
            )
            .await?;
        Ok(())
    }

    /// Sends the latest block to storage
    pub async fn send_block_to_storage(&mut self) -> Result<()> {
        self.node
            .send(
                self.storage_addr,
                StorageRequest::SendBlock {
                    block: self.current_block.as_ref().unwrap().clone(),
                    tx: self.current_block_tx.clone(),
                },
            )
            .await?;

        // Clear current block
        self.current_block = None;

        // Clear current block tx
        self.current_block_tx.clear();

        Ok(())
    }

    /// Sends random number to a mining node.
    pub async fn send_random_number(&mut self, peer: SocketAddr) -> Result<()> {
        let random_num = self.current_random_num.clone();
        println!("RANDOM NUMBER IN COMPUTE: {:?}", random_num);

        self.node
            .send(peer, MineRequest::SendRandomNum { rnum: random_num })
            .await?;
        Ok(())
    }

    /// Floods all peers with a PoW for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_pow_to_peers(&self, _address: SocketAddr, _pow: &Vec<u8>) {
        println!("Flooding PoW to peers not implemented");
    }

    /// Floods all peers with a PoW commit for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_commit_to_peers(&self, _address: SocketAddr, _commit: &ProofOfWork) {
        println!("Flooding commit to peers not implemented");
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event(&mut self) -> Option<Result<Response>> {
        let event = self.node.next_event().await?;
        self.handle_event(event).await.into()
    }

    async fn handle_event(&mut self, event: Event) -> Result<Response> {
        match event {
            Event::NewFrame { peer, frame } => Ok(self.handle_new_frame(peer, frame).await?),
        }
    }

    /// Hanldes a new incoming message from a peer.
    async fn handle_new_frame(&mut self, peer: SocketAddr, frame: Bytes) -> Result<Response> {
        info_span!("peer", ?peer).in_scope(|| {
            let req = deserialize::<ComputeMessage>(&frame).map_err(|error| {
                warn!(?error, "frame-deserialize");
                error
            })?;

            info_span!("request", ?req).in_scope(|| {
                let response = self.handle_request(peer, req);
                debug!(?response, ?peer, "response");

                Ok(response)
            })
        })
    }

    /// Handles a compute request.
    fn handle_request(&mut self, peer: SocketAddr, req: ComputeMessage) -> Response {
        use ComputeMessage::*;

        match req {
            SendPoW { pow, coinbase } => self.receive_pow(peer, pow, coinbase),
            SendPartitionEntry { partition_entry } => {
                self.receive_partition_entry(peer, partition_entry)
            }
            SendTransactions { transactions } => self.receive_transactions(transactions),
            SendPartitionRequest => self.receive_partition_request(peer),
        }
    }

    /// Receive a partition request from a miner node
    /// TODO: This may need to be part of the ComputeInterface depending on key agreement
    fn receive_partition_request(&mut self, peer: SocketAddr) -> Response {
        if self.request_list.is_empty() {
            self.generate_random_num(5);
        }

        self.request_list.insert(peer.to_string(), false);

        Response {
            success: true,
            reason: "Partition request received successfully",
        }
    }

    /// Receives the light POW for partition inclusion
    fn receive_partition_entry(
        &mut self,
        peer: SocketAddr,
        partition_entry: ProofOfWork,
    ) -> Response {
        let mut pow_mut = partition_entry.clone();

        if self.partition_list.len() < PARTITION_LIMIT && Self::validate_pow(&mut pow_mut) {
            self.partition_list.push(partition_entry.clone());

            if self.partition_list.len() == PARTITION_LIMIT {
                let key = get_partition_entry_key(self.partition_list.clone());
                let hashed_key = Sha3_256::digest(&key).to_vec();
                let key_slice: [u8; 32] = hashed_key[..].try_into().unwrap();
                self.partition_key = Key(key_slice);

                return Response {
                    success: true,
                    reason: "Partition list is full",
                };
            }

            return Response {
                success: true,
                reason: "Partition PoW received successfully",
            };
        }

        if self.partition_list.len() >= PARTITION_LIMIT {
            return Response {
                success: false,
                reason: "Partition list is full",
            };
        }

        Response {
            success: false,
            reason: "PoW received is invalid",
        }
    }

    /// Floods the random number to everyone who requested
    pub async fn flood_rand_num_to_requesters(&mut self) -> Result<()> {
        let mut exclusions = Vec::new();

        for (peer, peer_sent) in self.request_list.clone() {
            if !peer_sent {
                exclusions.push(peer.clone());
                let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");

                println!("PEER ADDRESS: {:?}", peer_addr);

                let _result = self.send_random_number(peer_addr).await.unwrap();
            }
        }

        for exclusion in exclusions {
            self.request_list.insert(exclusion, true);
        }

        Ok(())
    }

    /// Floods the current block to participants for mining
    pub async fn flood_block_to_partition(&mut self) -> Result<()> {
        self.generate_garbage_block(BLOCK_SIZE);

        for (peer, _) in self.request_list.clone() {
            let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");
            let _result = self.send_block(peer_addr).await.unwrap();
        }

        Ok(())
    }

    /// Floods all peers with the full partition list
    pub async fn flood_list_to_partition(&mut self) -> Result<()> {
        for (peer, _) in self.request_list.clone() {
            let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");
            let _result = self.send_partition_list(peer_addr).await.unwrap();
        }

        Ok(())
    }

    /// Floods all partition participants with block find notification
    pub async fn flood_block_found_notification(&mut self) -> Result<()> {
        for (peer, _) in self.request_list.clone() {
            let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");
            let _result = self.send_bf_notification(peer_addr).await.unwrap();
        }

        Ok(())
    }
}

impl ComputeInterface for ComputeNode {
    fn validate_pow(pow: &mut ProofOfWork) -> bool {
        let mut pow_body = pow.address.as_bytes().to_vec();
        pow_body.append(&mut pow.nonce.clone());

        let pow_hash = Sha3_256::digest(&pow_body).to_vec();

        for entry in pow_hash[0..MINING_DIFFICULTY].to_vec() {
            if entry != 0 {
                return false;
            }
        }

        true
    }

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

    fn receive_pow(
        &mut self,
        address: SocketAddr,
        pow: ProofOfWorkBlock,
        coinbase: Transaction,
    ) -> Response {
        info!(?address, "Received PoW");
        let mut pow_block = pow.clone();

        if !coinbase.is_coinbase() || coinbase.outputs[0].amount != 12 {
            return Response {
                success: false,
                reason: "Coinbase transaction invalid",
            };
        }

        if !Self::validate_pow_block(&mut pow_block) {
            return Response {
                success: false,
                reason: "Invalid PoW for block",
            };
        }

        // Update latest coinbase to notify winner
        self.last_coinbase_hash = coinbase.outputs[0].script_public_key.clone();

        // Update latest block hash
        let block_s = Bytes::from(serialize(&pow_block).unwrap()).to_vec();
        let latest_block_h = Sha3_256::digest(&block_s).to_vec();
        self.last_block_hash = hex::encode(latest_block_h);

        // Update internal UTXO
        self.utxo_set
            .lock()
            .unwrap()
            .append(&mut self.current_block_tx.clone());
        update_utxo_set(&mut self.utxo_set);

        Response {
            success: true,
            reason: "Received PoW successfully",
        }
    }

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

    fn receive_transactions(&mut self, transactions: BTreeMap<String, Transaction>) -> Response {
        if self.tx_pool.len() + transactions.len() > TX_POOL_LIMIT {
            return Response {
                success: false,
                reason: "Transaction pool for this compute node is full",
            };
        }

        let mut valid_tx = BTreeMap::new();

        for (hash, tx) in &transactions {
            if !tx.is_coinbase()
                && tx_ins_are_valid(tx.clone().inputs, &self.utxo_set.lock().unwrap())
            {
                valid_tx.insert(hash.clone(), tx.clone());

                // Only add if there is space
                if self.current_block_tx.len() + 1 < BLOCK_SIZE_IN_TX {
                    self.current_block_tx.insert(hash.clone(), tx.clone());
                }
            }
        }

        // At this point the tx's are considered valid
        self.tx_pool.append(&mut valid_tx.clone());

        if valid_tx.len() == 0 {
            return Response {
                success: false,
                reason: "No valid transactions provided",
            };
        }

        // Create new block if needed
        if self.current_block.is_none() {
            self.current_block = Some(Block::new());
        }

        if valid_tx.len() < transactions.len() {
            return Response {
                success: true,
                reason: "Some transactions invalid. Adding valid transactions only",
            };
        }

        Response {
            success: true,
            reason: "All transactions successfully added to tx pool",
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
