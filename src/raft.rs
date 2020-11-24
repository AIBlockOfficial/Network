use raft::prelude::*;
use raft::storage::MemStorage;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};

pub type RaftData = Vec<u8>;
pub type CommitSender = mpsc::Sender<Vec<RaftData>>;
pub type CommitReceiver = mpsc::Receiver<Vec<RaftData>>;
pub type RaftCmdSender = mpsc::Sender<RaftCmd>;
pub type RaftCmdReceiver = mpsc::Receiver<RaftCmd>;
pub type RaftMsgSender = mpsc::Sender<Message>;
pub type RaftMsgReceiver = mpsc::Receiver<Message>;

// struct RaftNode {
//     pub msg_out_rx: RaftMsgReceiver,
//     pub cmd_tx: RaftCmdSender,
//     pub committed_rx: CommitReceiver,
// }

/// Fields necessary for launching a Raft loop.
pub struct RaftConfig {
    /// Raft RawNode config.
    cfg: Config,
    /// Input command and messages.
    cmd_rx: RaftCmdReceiver,
    /// Output commited data.
    committed_tx: CommitSender,
    /// Ouput raft messages that need to be dispatched to appropriate peers.
    msg_out_tx: RaftMsgSender,
    /// Tick timeout duration.
    tick_timeout_duration: Duration,
}

/// Wrapper for raft Messages enabling Serialize/Deserialize
#[derive(Clone, Debug, PartialEq)]
pub struct RaftMessageWrapper(Message);

impl Serialize for RaftMessageWrapper {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use protobuf::Message;
        let bytes = self.0.write_to_bytes().map_err(serde::ser::Error::custom)?;
        bytes.serialize(s)
    }
}

impl<'a> Deserialize<'a> for RaftMessageWrapper {
    fn deserialize<D: Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: &[u8] = Deserialize::deserialize(deserializer)?;
        Ok(RaftMessageWrapper(
            protobuf::parse_from_bytes::<Message>(&bytes).map_err(serde::de::Error::custom)?,
        ))
    }
}

/// Input command/messages to the raft loop.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RaftCmd {
    Propose { data: RaftData },
    Raft(RaftMessageWrapper),
    Close,
}

/// Async RAFT loop processing inputs and populating output channels.
pub async fn run_raft_loop(raft_config: RaftConfig) {
    let RaftConfig {
        cfg,
        mut cmd_rx,
        mut committed_tx,
        mut msg_out_tx,
        tick_timeout_duration,
    } = raft_config;
    let peers = vec![];

    let storage = MemStorage::new();
    let mut node = RawNode::new(&cfg, storage, peers).unwrap();
    let mut timeout_at_time = Instant::now() + tick_timeout_duration;
    let mut propose_data_backlog = Vec::new();

    loop {
        match timeout_at(timeout_at_time, cmd_rx.recv()).await {
            Ok(Some(RaftCmd::Propose { data })) => {
                propose_data_backlog.push(data);
            }
            Ok(Some(RaftCmd::Raft(RaftMessageWrapper(m)))) => node.step(m).unwrap(),
            Err(_) => {
                // Timeout
                timeout_at_time = Instant::now() + tick_timeout_duration;
                node.tick();
            }
            Ok(Some(RaftCmd::Close)) | Ok(None) => {
                // Disconnected
                return;
            }
        }

        if node.raft.leader_id != raft::INVALID_ID {
            for data in propose_data_backlog.drain(..) {
                let context = Vec::new();
                node.propose(context, data).unwrap();
            }
        }

        process_ready(&mut node, &mut committed_tx, &mut msg_out_tx).await;
    }
}

async fn process_ready(
    node: &mut RawNode<MemStorage>,
    committed_tx: &mut CommitSender,
    msg_out_tx: &mut RaftMsgSender,
) {
    if !node.has_ready() {
        return;
    }

    let mut ready = node.ready();

    let is_leader = node.raft.leader_id == node.raft.id;
    if is_leader {
        send_messages_to_peers(msg_out_tx, &mut ready).await;
        update_ready_mut_store(node, &mut ready);
    } else {
        update_ready_mut_store(node, &mut ready);
        send_messages_to_peers(msg_out_tx, &mut ready).await;
    }

    apply_committed_entries(&mut ready, committed_tx).await;

    node.advance(ready);
}

async fn send_messages_to_peers(msg_out_tx: &mut RaftMsgSender, ready: &mut Ready) {
    for msg in ready.messages.drain(..) {
        msg_out_tx.send(msg).await.unwrap();
    }
}

fn update_ready_mut_store(node: &mut RawNode<MemStorage>, ready: &mut Ready) {
    if !raft::is_empty_snap(ready.snapshot()) {
        node.mut_store()
            .wl()
            .apply_snapshot(ready.snapshot().clone())
            .unwrap();
    }

    if !ready.entries().is_empty() {
        node.mut_store().wl().append(ready.entries()).unwrap();
    }

    if let Some(hs) = ready.hs() {
        node.mut_store().wl().set_hardstate(hs.clone());
    }
}

async fn apply_committed_entries(ready: &mut Ready, committed_tx: &mut CommitSender) {
    if let Some(mut committed_entries) = ready.committed_entries.take() {
        let committed: Vec<_> = committed_entries
            .drain(..)
            // Skip emtpy entry sent when the peer becomes Leader.
            .filter(|entry| !entry.get_data().is_empty())
            .filter(|entry| entry.get_entry_type() == EntryType::EntryNormal)
            .map(|mut entry| entry.take_data())
            .collect();

        if !committed.is_empty() {
            committed_tx.send(committed).await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use std::collections::HashMap;

    struct TestNode {
        pub raft_config: Option<RaftConfig>,
        pub msg_out_rx: Option<RaftMsgReceiver>,
        pub cmd_tx: RaftCmdSender,
        pub committed_rx: CommitReceiver,
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_send_proposal_is_commited_1_node() {
        send_proposal_check_commited(1).await;
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_send_proposal_is_commited_2_nodes() {
        send_proposal_check_commited(2).await;
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_send_proposal_is_commited_20_nodes() {
        send_proposal_check_commited(20).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Verify proposal are committed by all peers regardless of the proposer.
    async fn send_proposal_check_commited(num_peers: u64) {
        let mut join_handles = Vec::new();
        let (peer_indexes, mut test_nodes) = test_configs(num_peers);
        let msg_txs: Vec<_> = test_nodes.iter().map(|node| node.cmd_tx.clone()).collect();

        // Setup RAFT: Raft loops and Raft message dispatching loops.
        for test_node in &mut test_nodes {
            let raft_config = test_node.raft_config.take().unwrap();
            join_handles.push(tokio::spawn(async move {
                run_raft_loop(raft_config).await;
            }));

            let msg_out_rx = test_node.msg_out_rx.take().unwrap();
            let peer_indexes = peer_indexes.clone();
            let msg_txs = msg_txs.clone();
            join_handles.push(tokio::spawn(async move {
                dispatch_messages_loop(msg_out_rx, peer_indexes, msg_txs).await;
            }));
        }

        // Send a proposal and wait for it to be commited.
        {
            let proposed_data = vec![17];
            send_proposal(&mut test_nodes[0], proposed_data.clone()).await;

            let commited_data = recv_commited(&mut test_nodes).await;
            let expected = expected_commited(&test_nodes, &vec![proposed_data]);
            assert_eq!(commited_data, expected);
        }

        // Send a proposal to other nodeand wait for it to be commited.
        if num_peers > 1 {
            let proposed_data = vec![33];
            send_proposal(&mut test_nodes[1], proposed_data.clone()).await;

            let commited_data = recv_commited(&mut test_nodes).await;
            let expected = expected_commited(&test_nodes, &vec![proposed_data]);
            assert_eq!(commited_data, expected);
        }

        // Close raft loop so spawned task can complete and wait for completion.
        for test_node in &mut test_nodes {
            test_node.cmd_tx.send(RaftCmd::Close).await.unwrap();
        }
        join_all(join_handles).await;
    }

    async fn dispatch_messages_loop(
        mut msg_out_rx: RaftMsgReceiver,
        peer_indexes: HashMap<u64, usize>,
        mut msg_txs: Vec<RaftCmdSender>,
    ) {
        loop {
            match msg_out_rx.recv().await {
                Some(msg) => {
                    let to_index = peer_indexes[&msg.to];
                    msg_txs[to_index]
                        .send(RaftCmd::Raft(RaftMessageWrapper(msg)))
                        .await
                        .unwrap();
                }
                None => {
                    // Disconnected
                    return;
                }
            }
        }
    }

    async fn send_proposal(test_node: &mut TestNode, data: RaftData) {
        test_node
            .cmd_tx
            .send(RaftCmd::Propose { data })
            .await
            .unwrap();
    }

    async fn recv_commited(test_nodes: &mut Vec<TestNode>) -> Vec<Vec<RaftData>> {
        let mut received = Vec::new();
        for test_node in test_nodes {
            received.push(test_node.committed_rx.recv().await.unwrap());
        }
        received
    }

    fn expected_commited(
        test_nodes: &Vec<TestNode>,
        expected: &Vec<RaftData>,
    ) -> Vec<Vec<RaftData>> {
        test_nodes.iter().map(|_| expected.clone()).collect()
    }

    fn test_configs(num_peers: u64) -> (HashMap<u64, usize>, Vec<TestNode>) {
        let peers: Vec<u64> = (1..num_peers + 1).collect();
        let test_nodes: Vec<_> = peers
            .iter()
            .map(|peer_id| test_config(*peer_id, &peers))
            .collect();
        let peer_to_indexes = peers
            .iter()
            .enumerate()
            .map(|(idx, id)| (*id, idx))
            .collect();
        (peer_to_indexes, test_nodes)
    }

    fn test_config(peer_id: u64, peers: &Vec<u64>) -> TestNode {
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let (committed_tx, committed_rx) = mpsc::channel(100);
        let (msg_out_tx, msg_out_rx) = mpsc::channel(100);

        let raft_config = RaftConfig {
            cfg: Config {
                id: peer_id,
                peers: peers.clone(),
                ..Default::default()
            },
            cmd_rx,
            committed_tx,
            msg_out_tx,
            tick_timeout_duration: Duration::from_millis(1),
        };

        TestNode {
            raft_config: Some(raft_config),
            cmd_tx,
            committed_rx,
            msg_out_rx: Some(msg_out_rx),
        }
    }
}
