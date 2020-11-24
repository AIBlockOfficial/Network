use crate::raft::{CommitReceiver, RaftCmdSender, RaftMsgReceiver, RaftNode};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ComputeRaft {
    raft_node: Arc<Mutex<RaftNode>>,
    cmd_tx: RaftCmdSender,
    msg_out_rx: Arc<Mutex<RaftMsgReceiver>>,
    committed_rx: Arc<Mutex<CommitReceiver>>,
}

impl fmt::Debug for ComputeRaft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ComputeRaft()")
    }
}

impl ComputeRaft {
    pub fn new() -> Self {
        let (raft_config, raft_channels) = RaftNode::init_config(
            raft::Config {
                id: 1,
                peers: vec![1],
                ..Default::default()
            },
            Duration::from_millis(1),
        );

        ComputeRaft {
            raft_node: Arc::new(Mutex::new(RaftNode::new(raft_config))),
            cmd_tx: raft_channels.cmd_tx,
            msg_out_rx: Arc::new(Mutex::new(raft_channels.msg_out_rx)),
            committed_rx: Arc::new(Mutex::new(raft_channels.committed_rx)),
        }
    }

    /// Blocks & waits for a next event from a peer.
    pub async fn next_event(&mut self) -> Option<()> {
        self.raft_node.lock().await.next_event().await
    }
}
