use crate::configurations::ComputeNodeConfig;
use crate::raft::{
    CommitReceiver, RaftCmd, RaftCmdSender, RaftData, RaftMessageWrapper, RaftMsgReceiver, RaftNode,
};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ComputeRaft {
    raft_node: Arc<Mutex<RaftNode>>,
    cmd_tx: RaftCmdSender,
    msg_out_rx: Arc<Mutex<RaftMsgReceiver>>,
    committed_rx: Arc<Mutex<CommitReceiver>>,
    peer_addr: HashMap<u64, SocketAddr>,
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

        let peer_addr: HashMap<u64, SocketAddr> = peers
            .iter()
            .zip(config.compute_nodes.iter())
            .map(|(idx, spec)| (*idx, spec.address))
            .collect();

        let (raft_config, raft_channels) = RaftNode::init_config(
            raft::Config {
                id: peers[config.compute_node_idx],
                peers,
                ..Default::default()
            },
            Duration::from_millis(10),
        );

        ComputeRaft {
            raft_node: Arc::new(Mutex::new(RaftNode::new(raft_config))),
            cmd_tx: raft_channels.cmd_tx,
            msg_out_rx: Arc::new(Mutex::new(raft_channels.msg_out_rx)),
            committed_rx: Arc::new(Mutex::new(raft_channels.committed_rx)),
            peer_addr,
        }
    }

    /// Blocks & waits for a next event from a peer.
    pub async fn run_raft_loop(&mut self) {
        self.raft_node.lock().await.run_raft_loop().await;
    }

    /// Blocks & waits for a next commit from a peer.
    pub async fn next_commit(&self) -> Option<Vec<RaftData>> {
        self.committed_rx.lock().await.recv().await
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

    pub async fn propose(&mut self, data: RaftData) {
        self.cmd_tx.send(RaftCmd::Propose { data }).await.unwrap();
    }
}
