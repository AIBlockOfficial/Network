use crate::configurations::NodeSpec;
use crate::raft::{
    CommitReceiver, RaftCmd, RaftCmdSender, RaftData, RaftMessageWrapper, RaftMsgReceiver, RaftNode,
};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Provide RAFT loop and in/out channels to interact with it.
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
    committed_rx: Arc<Mutex<(CommitReceiver, Vec<RaftData>)>>,
    /// Map to the address of the peers.
    peer_addr: HashMap<u64, SocketAddr>,
    /// Collection of the peer this node is responsible to connect to.
    compute_peers_to_connect: Vec<SocketAddr>,
}

impl ActiveRaft {
    /// Create ActiveRaft, need to spawn the raft loop to use raft.
    pub fn new(
        node_idx: usize,
        node_specs: &[NodeSpec],
        use_raft: bool,
        tick_timeout_duration: Duration,
    ) -> Self {
        let peers: Vec<u64> = (0..node_specs.len()).map(|idx| idx as u64 + 1).collect();

        let peer_addr_vec: Vec<(u64, SocketAddr)> = peers
            .iter()
            .zip(node_specs.iter())
            .map(|(idx, spec)| (*idx, spec.address))
            .collect();
        let peer_addr: HashMap<u64, SocketAddr> = peer_addr_vec.iter().cloned().collect();
        let peer_id = peers[node_idx];

        let (raft_config, raft_channels) = RaftNode::init_config(
            raft::Config {
                id: peer_id,
                peers,
                tag: format!("[id={}]", peer_id),
                ..Default::default()
            },
            tick_timeout_duration,
        );

        // TODO: Connect to all other peers once connection can succeed from both sides.
        let compute_peers_to_connect = if use_raft {
            peer_addr_vec
                .iter()
                .filter(|(idx, _)| *idx > peer_id)
                .map(|(_, addr)| *addr)
                .collect()
        } else {
            Vec::new()
        };

        Self {
            use_raft,
            peer_id,
            raft_node: Arc::new(Mutex::new(RaftNode::new(raft_config))),
            cmd_tx: raft_channels.cmd_tx,
            msg_out_rx: Arc::new(Mutex::new(raft_channels.msg_out_rx)),
            committed_rx: Arc::new(Mutex::new((raft_channels.committed_rx, Vec::new()))),
            peer_addr,
            compute_peers_to_connect,
        }
    }

    pub fn use_raft(&self) -> bool {
        self.use_raft
    }

    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    pub fn peers_len(&self) -> usize {
        self.peer_addr.len()
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

    pub async fn append_commited_overflow(&mut self, mut raft_data: Vec<RaftData>) {
        self.committed_rx.lock().await.1.append(&mut raft_data);
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
        self.cmd_tx.send(RaftCmd::Raft(msg)).await.unwrap();
    }

    /// Propose RaftData to raft if use_raft, or commit it otherwise.
    pub async fn propose_data(&mut self, data: RaftData) {
        if self.use_raft {
            self.cmd_tx.send(RaftCmd::Propose { data }).await.unwrap();
        } else {
            self.committed_rx.lock().await.1.push(data);
        }
    }
}
