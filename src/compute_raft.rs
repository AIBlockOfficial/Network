use crate::configurations::ComputeNodeConfig;
use crate::raft::{
    CommitReceiver, RaftCmd, RaftCmdSender, RaftData, RaftMessageWrapper, RaftMsgReceiver, RaftNode,
};
use bincode::{deserialize, serialize};
use naom::primitives::transaction::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeRaftItem {
    Block(String),
    Transactions(BTreeMap<String, Transaction>),
    DruidTransactions(Vec<BTreeMap<String, Transaction>>),
}

/// Consensused Compute fields and consensus managment.
pub struct ComputeRaft {
    // false if RAFT is bypassed.
    use_raft: bool,
    /// Raft node used for running loop: only use for run_raft_loop.
    raft_node: Arc<Mutex<RaftNode>>,
    /// Channel to send command to the running RaftNode.
    cmd_tx: RaftCmdSender,
    /// Channel to receive messages from the running RaftNode to pass arround.
    msg_out_rx: Arc<Mutex<RaftMsgReceiver>>,
    /// Channel to receive commited entries from the running RaftNode to process.
    committed_rx: Arc<Mutex<CommitReceiver>>,
    /// Map to the address of the peers.
    peer_addr: HashMap<u64, SocketAddr>,
    /// Collection of the peer this node is responsible to connect to.
    compute_peers_to_connect: Vec<SocketAddr>,
    /// Committed transaction pool.
    tx_pool: BTreeMap<String, Transaction>,
    /// Committed DRUID transactions.
    tx_druid_pool: Vec<BTreeMap<String, Transaction>>,
    /// Local transaction pool.
    local_tx_pool: BTreeMap<String, Transaction>,
    /// Local DRUID transaction pool.
    local_tx_druid_pool: Vec<BTreeMap<String, Transaction>>,
    /// Block to propose: should contain all needed information.
    local_last_block_hash: String,
}

impl fmt::Debug for ComputeRaft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ComputeRaft()")
    }
}

impl ComputeRaft {
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
            Duration::from_millis(10),
        );

        // TODO: Connect to all other peers once connection can succeed from both sides.
        let compute_peers_to_connect = peer_addr_vec
            .iter()
            .filter(|(idx, _)| *idx > peer_id)
            .map(|(_, addr)| addr.clone())
            .collect();

        ComputeRaft {
            use_raft: config.compute_raft != 0,
            raft_node: Arc::new(Mutex::new(RaftNode::new(raft_config))),
            cmd_tx: raft_channels.cmd_tx,
            msg_out_rx: Arc::new(Mutex::new(raft_channels.msg_out_rx)),
            committed_rx: Arc::new(Mutex::new(raft_channels.committed_rx)),
            peer_addr,
            compute_peers_to_connect,
            tx_pool: BTreeMap::new(),
            tx_druid_pool: Vec::new(),
            local_tx_pool: BTreeMap::new(),
            local_tx_druid_pool: Vec::new(),
            local_last_block_hash: "".to_string(),
        }
    }

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
        self.committed_rx.lock().await.recv().await
    }

    pub fn received_commit(&mut self, mut raft_data: Vec<RaftData>) -> Option<String> {
        let mut last_commit_block = None;
        for data in raft_data.drain(..) {
            match deserialize::<ComputeRaftItem>(&data) {
                Ok(ComputeRaftItem::Transactions(mut transactions)) => {
                    self.tx_pool.append(&mut transactions);
                }
                Ok(ComputeRaftItem::DruidTransactions(mut transactions)) => {
                    self.tx_druid_pool.append(&mut transactions);
                }
                Ok(ComputeRaftItem::Block(block)) => {
                    last_commit_block = Some(block);
                }
                Err(error) => warn!(?error, "ComputeRaftItem-deserialize"),
            }
        }
        last_commit_block
    }

    /// Blocks & waits for a next message to dispatch from a peer.
    pub async fn next_msg(&self) -> Option<(SocketAddr, RaftMessageWrapper)> {
        let msg = self.msg_out_rx.lock().await.recv().await?;
        let addr = self.peer_addr.get(&msg.to).unwrap().clone();
        Some((addr, RaftMessageWrapper(msg)))
    }

    pub async fn received_message(&mut self, msg: RaftMessageWrapper) {
        self.cmd_tx.send(RaftCmd::Raft(msg)).await.unwrap();
    }

    pub async fn propose_block(&mut self, block: String) {
        self.propose_item(&ComputeRaftItem::Block(block)).await;
    }

    pub async fn propose_local_transactions(&mut self) {
        let tx = std::mem::take(&mut self.local_tx_pool);
        self.propose_item(&ComputeRaftItem::Transactions(tx)).await;
    }

    pub async fn propose_local_druid_transactions(&mut self) {
        let tx = std::mem::take(&mut self.local_tx_druid_pool);
        self.propose_item(&ComputeRaftItem::DruidTransactions(tx))
            .await;
    }

    async fn propose_item(&mut self, item: &ComputeRaftItem) {
        let data = serialize(item).unwrap();
        self.cmd_tx.send(RaftCmd::Propose { data }).await.unwrap();
    }

    pub fn commited_tx_pool(&mut self) -> &mut BTreeMap<String, Transaction> {
        &mut self.tx_pool
    }

    pub fn tx_pool_len(&self) -> usize {
        self.tx_pool.len() + self.local_tx_pool.len()
    }

    pub fn append_to_tx_pool(&mut self, mut transactions: BTreeMap<String, Transaction>) {
        if self.use_raft {
            self.local_tx_pool.append(&mut transactions);
        } else {
            self.tx_pool.append(&mut transactions);
        }
    }

    pub fn set_local_last_block_hash(&mut self, value: String) {
        self.local_last_block_hash = value;
    }

    pub fn get_last_block_hash(&self) -> &String {
        &self.local_last_block_hash
    }

    pub fn append_to_tx_druid_pool(&mut self, transactions: BTreeMap<String, Transaction>) {
        if self.use_raft {
            self.local_tx_druid_pool.push(transactions);
        } else {
            self.tx_druid_pool.push(transactions);
        }
    }

    pub fn commited_tx_druid_pool(&mut self) -> &mut Vec<BTreeMap<String, Transaction>> {
        &mut self.tx_druid_pool
    }
}
