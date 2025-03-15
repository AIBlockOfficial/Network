use crate::db_utils::{SimpleDb, SimpleDbError};
use crate::raft::{
    CommitReceiver, RaftCmd, RaftCmdSender, RaftCommit, RaftCommitData, RaftData,
    RaftMessageWrapper, RaftMsgReceiver, RaftNode,
};
use raft::prelude::MessageType;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Provide RAFT loop and in/out channels to interact with it.
///
pub struct ActiveRaft {
    /// false if RAFT is bypassed.
    use_raft: bool,
    /// The raft peer id.
    peer_id: u64,
    /// Raft node used for running loop: only use for run_raft_loop.
    raft_node: Arc<Mutex<RaftNode>>,
    /// Channel to send command to the running RaftNode.
    cmd_tx: RaftCmdSender,
    /// Channel to receive messages from the running RaftNode to pass arround.
    msg_out_rx: Arc<Mutex<RaftMsgReceiver>>,
    /// Channel to receive commited entries from the running RaftNode to process.
    /// and extra data not processed yet.
    committed_rx: Arc<Mutex<(CommitReceiver, VecDeque<RaftCommit>)>>,
    /// Map to the address of the peers.
    peer_addr: HashMap<u64, SocketAddr>,
    /// Collection of the peer this node is responsible to connect to.
    raft_peers_to_connect: Vec<SocketAddr>,
    /// Collection of the peer expected to be connected.
    raft_peer_addrs: Vec<SocketAddr>,
    /// Track if this node is the leader
    is_leader: Arc<Mutex<bool>>,
}

impl ActiveRaft {
    /// Create ActiveRaft, need to spawn the raft loop to use raft.
    pub fn new(
        node_idx: usize,
        node_specs: &[SocketAddr],
        use_raft: bool,
        tick_timeout_duration: Duration,
        raft_db: SimpleDb,
    ) -> Self {
        let peers: Vec<u64> = (0..node_specs.len()).map(|idx| idx as u64 + 1).collect();
        let peer_id = peers[node_idx];

        let peer_addr_vec: Vec<(u64, SocketAddr)> = peers
            .iter()
            .zip(node_specs.iter())
            .map(|(idx, spec)| (*idx, *spec))
            .filter(|(idx, _)| use_raft || *idx == peer_id)
            .collect();

        let (raft_config, raft_channels) = RaftNode::init_config(
            raft::Config {
                id: peer_id,
                peers,
                max_size_per_msg: 4096,
                max_inflight_msgs: 256,
                tag: format!("[id={peer_id}]"),
                ..Default::default()
            },
            raft_db,
            tick_timeout_duration,
        );

        let peer_addr: HashMap<u64, SocketAddr> = peer_addr_vec.iter().cloned().collect();

        // TODO: Connect to all other peers once connection can succeed from both sides.
        let raft_peers_to_connect = peer_addr_vec
            .iter()
            .filter(|(idx, _)| *idx < peer_id)
            .map(|(_, addr)| *addr)
            .collect();

        let raft_peer_addrs = peer_addr_vec
            .iter()
            .filter(|(idx, _)| *idx != peer_id)
            .map(|(_, addr)| *addr)
            .collect();

        // First peer is initially the leader
        let is_leader = Arc::new(Mutex::new(node_idx == 0));

        Self {
            use_raft,
            peer_id,
            raft_node: Arc::new(Mutex::new(RaftNode::new(raft_config))),
            cmd_tx: raft_channels.cmd_tx,
            msg_out_rx: Arc::new(Mutex::new(raft_channels.msg_out_rx)),
            committed_rx: Arc::new(Mutex::new((raft_channels.committed_rx, VecDeque::new()))),
            peer_addr,
            raft_peers_to_connect,
            raft_peer_addrs,
            is_leader,
        }
    }

    /// Returns a boolean of whether or not the raft is bypassed. False for bypassed. True for not bypassed.
    pub fn use_raft(&self) -> bool {
        self.use_raft
    }

    /// Returns the peer ID of this raft
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    /// Returns a map to the addresses of this raft's peers
    pub fn peers_len(&self) -> usize {
        self.peer_addr.len()
    }

    /// All the peers to connect to when using raft.
    /// Returns an iterator that iterates over the addresses of the peers
    pub fn raft_peer_to_connect(&self) -> impl Iterator<Item = &SocketAddr> {
        self.raft_peers_to_connect.iter()
    }

    /// All the peers expected to be connected when raft is running.
    pub fn raft_peer_addrs(&self) -> impl Iterator<Item = &SocketAddr> {
        self.raft_peer_addrs.iter()
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

    /// Signal to the raft loop to complete
    pub async fn close_raft_loop(&mut self) {
        // Ensure the loop is not stalled:
        self.msg_out_rx.lock().await.close();
        self.committed_rx.lock().await.0.close();

        // Close the loop
        self.cmd_tx.send(RaftCmd::Close).unwrap();
    }

    /// Extract persistent storage of a closed raft
    pub async fn take_closed_persistent_store(&mut self) -> SimpleDb {
        self.raft_node.lock().await.take_closed_persistent_store()
    }

    /// Backup persistent storage
    pub async fn backup_persistent_store(&self) -> Result<(), SimpleDbError> {
        self.raft_node.lock().await.backup_persistent_store()
    }

    /// Blocks & waits for a next commit from a peer.
    pub async fn next_commit(&self) -> Option<RaftCommit> {
        let mut committed_rx = self.committed_rx.lock().await;

        loop {
            if let Some(commit) = committed_rx.1.pop_front() {
                return Some(commit);
            } else if let Some(commits) = committed_rx.0.recv().await {
                committed_rx.1.extend(commits.into_iter());
            }
        }
    }

    /// Blocks & waits for a next message to dispatch from a peer.
    /// Message needs to be sent to given peer address.
    pub async fn next_msg(&self) -> Option<(SocketAddr, RaftMessageWrapper)> {
        let msg = self.msg_out_rx.lock().await.recv().await?;
        let addr = *self.peer_addr.get(&msg.to).unwrap();
        Some((addr, RaftMessageWrapper(msg)))
    }

    /// Process a raft message: send to spawned raft loop.
    pub async fn received_message(&mut self, msg: RaftMessageWrapper) {
        // Check if this is a message that indicates leadership changes
        if let RaftMessageWrapper(ref message) = msg {
            match message.get_msg_type() {
                MessageType::MsgRequestVoteResponse => {
                    // If we get a vote response and we're the candidate, we might become leader
                    if message.get_to() == self.peer_id && !message.get_reject() {
                        // A successful vote for us - we might become leader soon
                        // But we'll wait for explicit leadership confirmation
                    }
                },
                MessageType::MsgHeartbeatResponse => {
                    // If we're receiving heartbeat responses, we're likely the leader
                    if message.get_to() == self.peer_id {
                        *self.is_leader.lock().await = true;
                    }
                },
                MessageType::MsgHeartbeat => {
                    // If we're receiving heartbeats, someone else is the leader
                    if message.get_to() == self.peer_id {
                        *self.is_leader.lock().await = false;
                    }
                },
                MessageType::MsgAppend => {
                    // If we're receiving append entries, someone else is the leader
                    if message.get_to() == self.peer_id {
                        *self.is_leader.lock().await = false;
                    }
                },
                MessageType::MsgAppendResponse => {
                    // If we're receiving append responses, we're likely the leader
                    if message.get_to() == self.peer_id {
                        *self.is_leader.lock().await = true;
                    }
                },
                _ => {}
            }
        }
        
        self.cmd_tx.send(RaftCmd::Raft(msg)).unwrap();
    }

    /// Propose RaftData to raft if use_raft, or commit it otherwise.
    pub async fn propose_data(&mut self, data: RaftData, context: RaftData) {
        if self.use_raft {
            self.cmd_tx
                .send(RaftCmd::Propose { data, context })
                .unwrap();
        } else {
            self.committed_rx.lock().await.1.push_back(RaftCommit {
                data: RaftCommitData::Proposed(data, context),
                ..RaftCommit::default()
            });
        }
    }

    /// Create a snapshot at the given idx with the given data.
    ///
    /// ## Arguments
    /// * `idx` - The index of the snapshot
    /// * `data` - The data to snapshot
    /// * `backup` - Whether or not to backup the DB
    pub fn create_snapshot(&mut self, idx: u64, data: RaftData, backup: bool) {
        if self.use_raft {
            self.cmd_tx
                .send(RaftCmd::Snapshot { idx, data, backup })
                .unwrap();
        }
    }

    /// Check if this node is the leader in the Raft consensus
    pub fn is_leader(&self) -> bool {
        if !self.use_raft {
            // If we're not using Raft, consider this node as the leader
            return true;
        }
        
        // Get the leadership status directly from the RaftNode
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft_node = self.raft_node.lock().await;
                raft_node.node.raft.leader_id == raft_node.node.raft.id
            })
        })
    }
}
