use crate::configurations::ComputeNodeConfig;
use crate::constants::{BLOCK_SIZE_IN_TX, TX_POOL_LIMIT};
use crate::raft::{
    CommitReceiver, RaftCmd, RaftCmdSender, RaftData, RaftMessageWrapper, RaftMsgReceiver, RaftNode,
};
use bincode::{deserialize, serialize};
use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::{self, Future};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{timeout_at, Instant};
use tracing::{debug, warn};

/// Item serialized into RaftData and process by Raft.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeRaftItem {
    Block((String, u32)),
    Transactions(BTreeMap<String, Transaction>),
    DruidTransactions(Vec<BTreeMap<String, Transaction>>),
}

/// All fields that are consensused between the RAFT group.
/// These fields need to be written and read from a committed log event.
#[derive(Clone, Debug, Default)]
pub struct ComputeConsensused {
    /// Committed transaction pool.
    tx_pool: BTreeMap<String, Transaction>,
    /// Committed DRUID transactions.
    tx_druid_pool: Vec<BTreeMap<String, Transaction>>,
    /// Header to use for next block if ready to generate.
    tx_previous_hash: Option<String>,
    /// Index of the last block,
    tx_previous_block_idx: u32,
    /// Current block ready to mine (consensused).
    current_block: Option<Block>,
    /// All transactions present in current_block (consensused).
    current_block_tx: BTreeMap<String, Transaction>,
}

/// Consensused Compute fields and consensus managment.
pub struct ComputeRaft {
    // false if RAFT is bypassed.
    use_raft: bool,
    // false if RAFT is bypassed.
    first_raft_peer: bool,
    /// Raft node used for running loop: only use for run_raft_loop.
    raft_node: Arc<Mutex<RaftNode>>,
    /// Channel to send command to the running RaftNode.
    cmd_tx: RaftCmdSender,
    /// Channel to receive messages from the running RaftNode to pass arround.
    msg_out_rx: Arc<Mutex<RaftMsgReceiver>>,
    /// Channel to receive commited entries from the running RaftNode to process.
    /// and extra data not processed yet.
    committed_rx: Arc<Mutex<(CommitReceiver, Vec<RaftData>)>>,
    /// Map to the address of the peers.
    peer_addr: HashMap<u64, SocketAddr>,
    /// Collection of the peer this node is responsible to connect to.
    compute_peers_to_connect: Vec<SocketAddr>,
    /// Consensused fields.
    consensused: ComputeConsensused,
    /// Local transaction pool.
    local_tx_pool: BTreeMap<String, Transaction>,
    /// Local DRUID transaction pool.
    local_tx_druid_pool: Vec<BTreeMap<String, Transaction>>,
    /// Block to propose: should contain all needed information.
    local_last_block_hash_and_time: Option<(String, u32)>,
    /// Min duration between each block poposal.
    propose_block_timeout_duration: Duration,
    /// Timeout expiration time for block poposal.
    propose_block_timeout_at: Instant,
    /// Min duration between each transaction poposal.
    propose_transactions_timeout_duration: Duration,
    /// Timeout expiration time for transactions poposal.
    propose_transactions_timeout_at: Instant,
}

impl fmt::Debug for ComputeRaft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ComputeRaft()")
    }
}

impl ComputeRaft {
    /// Create a ComputeRaft, need to spawn the raft loop to use raft.
    pub fn new(config: &ComputeNodeConfig) -> Self {
        let peers: Vec<u64> = (1..config.compute_nodes.len() + 1)
            .map(|idx| idx as u64 + 1)
            .collect();

        let peer_addr_vec: Vec<(u64, SocketAddr)> = peers
            .iter()
            .zip(config.compute_nodes.iter())
            .map(|(idx, spec)| (*idx, spec.address))
            .collect();
        let peer_addr: HashMap<u64, SocketAddr> = peer_addr_vec.iter().cloned().collect();
        let peer_id = peers[config.compute_node_idx];

        let (raft_config, raft_channels) = RaftNode::init_config(
            raft::Config {
                id: peer_id,
                peers,
                tag: format!("[id={}]", peer_id),
                ..Default::default()
            },
            Duration::from_millis(config.compute_raft_tick_timeout as u64),
        );

        let use_raft = config.compute_raft != 0;

        // TODO: Connect to all other peers once connection can succeed from both sides.
        let compute_peers_to_connect = if use_raft {
            peer_addr_vec
                .iter()
                .filter(|(idx, _)| *idx > peer_id)
                .map(|(_, addr)| addr.clone())
                .collect()
        } else {
            Vec::new()
        };

        let propose_block_timeout_duration =
            Duration::from_millis(config.compute_block_timeout as u64);
        let propose_block_timeout_at = Instant::now() + propose_block_timeout_duration;

        let propose_transactions_timeout_duration =
            Duration::from_millis(config.compute_transaction_timeout as u64);
        let propose_transactions_timeout_at =
            Instant::now() + propose_transactions_timeout_duration;

        ComputeRaft {
            use_raft,
            first_raft_peer: config.compute_node_idx == 0 || !use_raft,
            raft_node: Arc::new(Mutex::new(RaftNode::new(raft_config))),
            cmd_tx: raft_channels.cmd_tx,
            msg_out_rx: Arc::new(Mutex::new(raft_channels.msg_out_rx)),
            committed_rx: Arc::new(Mutex::new((raft_channels.committed_rx, Vec::new()))),
            peer_addr,
            compute_peers_to_connect,
            consensused: ComputeConsensused::default(),
            local_tx_pool: BTreeMap::new(),
            local_tx_druid_pool: Vec::new(),
            local_last_block_hash_and_time: Some((String::new(), 1)),
            propose_block_timeout_duration,
            propose_block_timeout_at,
            propose_transactions_timeout_duration,
            propose_transactions_timeout_at,
        }
    }

    /// All the peers to connect to when using raft.
    pub fn compute_peer_to_connect(&self) -> impl Iterator<Item = &SocketAddr> {
        self.compute_peers_to_connect.iter()
    }

    /// Blocks & waits for a next event from a peer.
    pub fn raft_loop(&self) -> impl Future<Output = ()> {
        let raft_node = self.raft_node.clone();
        let use_raft = self.use_raft;
        async move {
            if use_raft {
                raft_node.lock().await.run_raft_loop().await;
            }
        }
    }

    /// Blocks & waits for a next commit from a peer.
    pub async fn next_commit(&self) -> Option<Vec<RaftData>> {
        let mut committed_rx = self.committed_rx.lock().await;

        if !committed_rx.1.is_empty() {
            return Some(std::mem::take(&mut committed_rx.1));
        }

        committed_rx.0.recv().await
    }

    /// Process result from next_commit.
    /// Return Some if block to mine is ready to generate.
    pub async fn received_commit(&mut self, mut raft_data: Vec<RaftData>) -> Option<()> {
        let mut committed_rx = self.committed_rx.lock().await;
        for data in raft_data.drain(..) {
            if self.consensused.tx_previous_hash.is_some() {
                committed_rx.1.push(data);
                continue;
            }

            match deserialize::<ComputeRaftItem>(&data) {
                Ok(ComputeRaftItem::Transactions(mut transactions)) => {
                    self.consensused.tx_pool.append(&mut transactions);
                }
                Ok(ComputeRaftItem::DruidTransactions(mut transactions)) => {
                    self.consensused.tx_druid_pool.append(&mut transactions);
                }
                Ok(ComputeRaftItem::Block((previous_hash, previous_idx))) => {
                    if self.consensused.tx_previous_block_idx >= previous_idx {
                        // Ignore already known blocks
                        continue;
                    }

                    if self.consensused.tx_pool.is_empty()
                        && self.consensused.tx_druid_pool.is_empty()
                    {
                        // Not ready for a new block, re-propose on timeout.
                        self.local_last_block_hash_and_time = Some((previous_hash, previous_idx));
                        continue;
                    }

                    // New block:
                    // Must not populate further tx_pool & tx_druid_pool
                    // before generating block.
                    self.consensused.tx_previous_hash = Some(previous_hash);
                    self.consensused.tx_previous_block_idx = previous_idx;
                }
                Err(error) => warn!(?error, "ComputeRaftItem-deserialize"),
            }
        }

        self.consensused.tx_previous_hash.as_ref().map(|_| ())
    }

    /// Blocks & waits for a next message to dispatch from a peer.
    /// Message needs to be sent to given peer address.
    pub async fn next_msg(&self) -> Option<(SocketAddr, RaftMessageWrapper)> {
        let msg = self.msg_out_rx.lock().await.recv().await?;
        let addr = self.peer_addr.get(&msg.to).unwrap().clone();
        Some((addr, RaftMessageWrapper(msg)))
    }

    /// Process a raft message: send to spawned raft loop.
    pub async fn received_message(&mut self, msg: RaftMessageWrapper) {
        self.cmd_tx.send(RaftCmd::Raft(msg)).await.unwrap();
    }

    /// Blocks & waits for a timeout to propose a block.
    pub async fn timeout_propose_block(&self) {
        Self::timeout_at(self.propose_block_timeout_at).await;
    }

    /// Blocks & waits for a timeout to propose transactions.
    pub async fn timeout_propose_transactions(&self) {
        Self::timeout_at(self.propose_transactions_timeout_at).await;
    }

    /// Blocks & waits for timeout.
    async fn timeout_at(timeout: Instant) {
        match timeout_at(timeout, future::pending::<()>()).await {
            Ok(()) => panic!("pending completed"),
            Err(_) => (),
        }
    }

    /// Process as a result of timeout_propose_block.
    /// Reset timeout, and propose block if previous completed.
    pub async fn propose_block_at_timeout(&mut self) {
        self.propose_block_timeout_at = Instant::now() + self.propose_block_timeout_duration;

        if let Some(block) = std::mem::take(&mut self.local_last_block_hash_and_time) {
            if self.first_raft_peer {
                self.propose_item(&ComputeRaftItem::Block(block)).await;
            }
        }
    }

    /// Process as a result of timeout_propose_transactions.
    /// Reset timeout, and propose local transactions if available.
    pub async fn propose_local_transactions_at_timeout(&mut self) {
        self.propose_transactions_timeout_at =
            Instant::now() + self.propose_transactions_timeout_duration;

        let tx = std::mem::take(&mut self.local_tx_pool);
        if !tx.is_empty() {
            self.propose_item(&ComputeRaftItem::Transactions(tx)).await;
        }
    }

    /// Process as a result of timeout_propose_transactions.
    /// Propose druid transactions if available.
    pub async fn propose_local_druid_transactions(&mut self) {
        let tx = std::mem::take(&mut self.local_tx_druid_pool);
        if !tx.is_empty() {
            self.propose_item(&ComputeRaftItem::DruidTransactions(tx))
                .await;
        }
    }

    /// Propose an item to raft if use_raft, or commit it otherwise.
    async fn propose_item(&mut self, item: &ComputeRaftItem) {
        debug!("propose_item: {:?}", item);
        let data = serialize(item).unwrap();
        if self.use_raft {
            self.cmd_tx.send(RaftCmd::Propose { data }).await.unwrap();
        } else {
            self.committed_rx.lock().await.1.push(data);
        }
    }

    /// Whether adding these will grow our pool within the limit.
    pub fn tx_pool_can_accept(&self, extra_len: usize) -> bool {
        let combined_len = self.consensused.tx_pool.len() + self.local_tx_pool.len();
        combined_len + extra_len <= TX_POOL_LIMIT
    }

    /// Append new transaction to our local pool from which to propose
    /// consensused transactions.
    pub fn append_to_tx_pool(&mut self, mut transactions: BTreeMap<String, Transaction>) {
        self.local_tx_pool.append(&mut transactions);
    }

    /// Set result of mined block necessary for new block to be generated.
    pub fn set_local_last_block_hash_and_time(&mut self, hash: String, time: u32) {
        self.local_last_block_hash_and_time = Some((hash, time));
    }

    /// Append new transaction to our local pool from which to propose
    /// consensused transactions.
    pub fn append_to_tx_druid_pool(&mut self, transactions: BTreeMap<String, Transaction>) {
        self.local_tx_druid_pool.push(transactions);
    }

    /// Set consensused committed block to mine.
    /// Internal call, public for test only.
    pub fn set_committed_mining_block(
        &mut self,
        block: Block,
        block_tx: BTreeMap<String, Transaction>,
    ) {
        self.consensused.current_block = Some(block);
        self.consensused.current_block_tx = block_tx;
    }

    /// Current block to mine or being mined.
    pub fn get_mining_block(&self) -> &Option<Block> {
        &self.consensused.current_block
    }

    /// Take mining block when mining is completed, use to populate mined block.
    pub fn take_mining_block(&mut self) -> (Block, BTreeMap<String, Transaction>) {
        let block = std::mem::take(&mut self.consensused.current_block).unwrap();
        let block_tx = std::mem::take(&mut self.consensused.current_block_tx);
        (block, block_tx)
    }

    /// Processes the next batch of transactions from the floating tx pool
    /// to create the next block
    ///
    /// TODO: Label previous block time
    pub fn generate_block(&mut self) {
        let mut next_block = Block::new();
        let mut next_block_tx = BTreeMap::new();

        self.update_committed_dde_tx(&mut next_block, &mut next_block_tx);
        self.update_current_block_tx(&mut next_block, &mut next_block_tx);
        self.update_block_header(&mut next_block);

        self.set_committed_mining_block(next_block, next_block_tx)
    }

    /// Apply all consensused transactions to the block
    fn update_committed_dde_tx(
        &mut self,
        block: &mut Block,
        block_tx: &mut BTreeMap<String, Transaction>,
    ) {
        for mut txs in self.consensused.tx_druid_pool.drain(..) {
            // Process a set of transactions from a single DRUID droplet.
            block.transactions.extend(txs.keys().cloned());
            block_tx.append(&mut txs);
        }
    }

    /// Apply all consensused transactions to the block until BLOCK_SIZE_IN_TX
    fn update_current_block_tx(
        &mut self,
        block: &mut Block,
        block_tx: &mut BTreeMap<String, Transaction>,
    ) {
        let mut txs = {
            let mut txs = std::mem::take(&mut self.consensused.tx_pool);
            if let Some(max_key) = txs.keys().nth(BLOCK_SIZE_IN_TX).cloned() {
                // Set back overflowing transactions.
                self.consensused.tx_pool = txs.split_off(&max_key);
            }
            txs
        };

        block.transactions.extend(txs.keys().cloned());
        block_tx.append(&mut txs);
    }

    /// Apply the consensused information for the header.
    fn update_block_header(&mut self, block: &mut Block) {
        let previous_hash = std::mem::take(&mut self.consensused.tx_previous_hash).unwrap();

        block.header.time = self.consensused.tx_previous_block_idx + 1;
        block.header.previous_hash = Some(previous_hash);
    }
}
