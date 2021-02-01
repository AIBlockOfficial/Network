use crate::utils::MpscTracingSender;
use raft::prelude::*;
use raft::storage::MemStorage;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};
use tracing::{info, trace};

pub type RaftData = Vec<u8>;
pub type CommitSender = MpscTracingSender<Vec<RaftCommit>>;
pub type CommitReceiver = mpsc::Receiver<Vec<RaftCommit>>;
pub type RaftCmdSender = mpsc::UnboundedSender<RaftCmd>;
pub type RaftCmdReceiver = mpsc::UnboundedReceiver<RaftCmd>;
pub type RaftMsgSender = MpscTracingSender<Message>;
pub type RaftMsgReceiver = mpsc::Receiver<Message>;

/// Raft Commit entry
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct RaftCommit {
    pub term: u64,
    pub index: u64,
    pub data: RaftCommitData,
}

/// Raft Commit data
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftCommitData {
    Proposed(RaftData),
    Snapshot(RaftData),
}

impl RaftCommitData {
    fn take_data(self) -> RaftData {
        match self {
            Self::Proposed(data) => data,
            Self::Snapshot(data) => data,
        }
    }
}

impl Default for RaftCommitData {
    fn default() -> Self {
        Self::Proposed(Default::default())
    }
}

/// Channels needed to interact with the running raft instance.
pub struct RaftNodeChannels {
    pub msg_out_rx: RaftMsgReceiver,
    pub cmd_tx: RaftCmdSender,
    pub committed_rx: CommitReceiver,
}

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
pub struct RaftMessageWrapper(pub Message);

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
    Snapshot { idx: u64, data: RaftData },
    Raft(RaftMessageWrapper),
    Close,
}

pub struct RaftNode {
    /// Runing raft node.
    node: RawNode<MemStorage>,
    /// Backlog of raft proposal while leader unavailable.
    propose_data_backlog: Vec<RaftData>,
    /// Input command and messages.
    cmd_rx: RaftCmdReceiver,
    /// Output commited data.
    committed_tx: CommitSender,
    /// Ouput raft messages that need to be dispatched to appropriate peers.
    msg_out_tx: RaftMsgSender,
    /// Tick timeout duration.
    tick_timeout_duration: Duration,
    /// Tick timeout expiration time.
    tick_timeout_at: Instant,
    /// Committed entries/group count.
    committed_entries_and_groups_count: (usize, usize),
    /// Outgoing messages/group count.
    outgoing_msgs_and_groups_count: (usize, usize),
    /// Incomming messages count.
    incoming_msgs_count: usize,
    /// Total tick count.
    total_tick_count: usize,
    /// Last snapshot index, and whether it need compacting.
    previous_snapshot_idx: (u64, bool),
}

impl RaftNode {
    pub fn new(raft_config: RaftConfig) -> Self {
        let storage = MemStorage::new();
        let tick_timeout_at = Instant::now() + raft_config.tick_timeout_duration;

        let node = {
            let mut node = RawNode::new(&raft_config.cfg, storage, vec![]).unwrap();
            if raft_config.cfg.id == 1 {
                // Make first peer start election immediatly on start up to avoid unecessary wait.
                node.raft.election_elapsed = node.raft.get_randomized_election_timeout();
            }

            // Set snapshot initial info
            let mut cs = ConfState::new();
            cs.set_nodes(node.raft.prs().voter_ids().iter().cloned().collect());
            node.mut_store().wl().set_conf_state(cs, None);

            node
        };

        Self {
            node,
            propose_data_backlog: Vec::new(),
            cmd_rx: raft_config.cmd_rx,
            committed_tx: raft_config.committed_tx,
            msg_out_tx: raft_config.msg_out_tx,
            tick_timeout_duration: raft_config.tick_timeout_duration,
            tick_timeout_at,
            committed_entries_and_groups_count: (0, 0),
            outgoing_msgs_and_groups_count: (0, 0),
            incoming_msgs_count: 0,
            total_tick_count: 0,
            previous_snapshot_idx: (0, false),
        }
    }

    /// Create the RaftConfig and needed channels to run the loop.
    pub fn init_config(
        node_cfg: Config,
        tick_timeout_duration: Duration,
    ) -> (RaftConfig, RaftNodeChannels) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (committed_tx, committed_rx) = mpsc::channel(100);
        let (msg_out_tx, msg_out_rx) = mpsc::channel(100);

        (
            RaftConfig {
                cfg: node_cfg,
                cmd_rx,
                committed_tx: committed_tx.into(),
                msg_out_tx: msg_out_tx.into(),
                tick_timeout_duration,
            },
            RaftNodeChannels {
                cmd_tx,
                committed_rx,
                msg_out_rx,
            },
        )
    }

    /// Async RAFT loop processing inputs and populating output channels.
    pub async fn run_raft_loop(&mut self) {
        loop {
            match self.next_event().await {
                Some(_) => (),
                None => {
                    // complete
                    return;
                }
            }
        }
    }

    /// Async RAFT loop processing inputs and populating output channels.
    async fn next_event(&mut self) -> Option<()> {
        match timeout_at(self.tick_timeout_at, self.cmd_rx.recv()).await {
            Ok(Some(RaftCmd::Propose { data })) => {
                self.propose_data_backlog.push(data);
            }
            Ok(Some(RaftCmd::Snapshot { idx, data })) => {
                let mut wl = self.node.mut_store().wl();
                let (prev_idx, need_compact) = self.previous_snapshot_idx;
                if idx != prev_idx {
                    wl.create_snapshot(idx, None, None, data).unwrap();
                }
                if need_compact {
                    wl.compact(prev_idx).unwrap();
                }
                self.previous_snapshot_idx = (idx, idx != prev_idx);
            }
            Ok(Some(RaftCmd::Raft(RaftMessageWrapper(m)))) => {
                trace!("next_event receive message({}, {:?})", self.node.raft.id, m);
                self.incoming_msgs_count += 1;
                self.node.step(m).unwrap()
            }
            Err(_) => {
                // Timeout
                self.tick_timeout_at = Instant::now() + self.tick_timeout_duration;
                self.total_tick_count += 1;
                self.node.tick();
            }
            Ok(Some(RaftCmd::Close)) | Ok(None) => {
                // Disconnected
                info!(
                    peer_id = self.node.raft.id,
                    leader_id = self.node.raft.leader_id,
                    term = self.node.raft.term,
                    ?self.total_tick_count,
                    ?self.incoming_msgs_count,
                    ?self.committed_entries_and_groups_count,
                    ?self.outgoing_msgs_and_groups_count,
                    "Closing Raft: Summary"
                );
                return None;
            }
        }

        if self.node.raft.leader_id != raft::INVALID_ID {
            for data in self.propose_data_backlog.drain(..) {
                let context = Vec::new();
                self.node.propose(context, data).unwrap();
            }
        }

        self.process_ready().await;
        Some(())
    }

    async fn process_ready(&mut self) {
        if !self.node.has_ready() {
            return;
        }

        let mut ready = self.node.ready();

        let is_leader = self.node.raft.leader_id == self.node.raft.id;
        if is_leader {
            self.send_messages_to_peers(&mut ready).await;
            self.update_ready_mut_store(&mut ready);
        } else {
            self.update_ready_mut_store(&mut ready);
            self.send_messages_to_peers(&mut ready).await;
        }

        self.apply_committed_entries(&mut ready).await;
        self.node.advance(ready);
    }

    async fn send_messages_to_peers(&mut self, ready: &mut Ready) {
        self.outgoing_msgs_and_groups_count.1 += if ready.messages.is_empty() { 0 } else { 1 };
        for msg in ready.messages.drain(..) {
            trace!("send_messages_to_peers({}, {:?})", self.node.raft.id, msg);
            self.outgoing_msgs_and_groups_count.0 += 1;
            let _ok_or_closed = self.msg_out_tx.send(msg, "msg_out").await;
        }
    }

    fn update_ready_mut_store(&mut self, ready: &mut Ready) {
        if !raft::is_empty_snap(ready.snapshot()) {
            self.node
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())
                .unwrap();
        }

        if !ready.entries().is_empty() {
            self.node.mut_store().wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            self.node.mut_store().wl().set_hardstate(hs.clone());
        }
    }

    async fn apply_committed_entries(&mut self, ready: &mut Ready) {
        let mut committed = Vec::new();

        if !raft::is_empty_snap(ready.snapshot()) {
            let snapshot = ready.snapshot();
            committed.push(RaftCommit {
                term: snapshot.get_metadata().term,
                index: snapshot.get_metadata().index,
                data: RaftCommitData::Snapshot(ready.snapshot().data.clone()),
            });
        }

        if let Some(mut committed_entries) = ready.committed_entries.take() {
            committed.extend(
                committed_entries
                    .drain(..)
                    // Skip emtpy entry sent when the peer becomes Leader.
                    .filter(|entry| !entry.get_data().is_empty())
                    .filter(|entry| entry.get_entry_type() == EntryType::EntryNormal)
                    .map(|mut entry| RaftCommit {
                        term: entry.get_term(),
                        index: entry.get_index(),
                        data: RaftCommitData::Proposed(entry.take_data()),
                    }),
            );
        }

        if !committed.is_empty() {
            self.committed_entries_and_groups_count.1 += 1;
            self.committed_entries_and_groups_count.0 += committed.len();
            let _ok_or_closed = self.committed_tx.send(committed, "committed").await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use std::cmp;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use tracing::error_span;
    use tracing_futures::Instrument;

    struct TestNode {
        pub raft_config: Option<RaftConfig>,
        pub msg_out_rx: Option<RaftMsgReceiver>,
        pub cmd_tx: RaftCmdSender,
        pub committed_rx: CommitReceiver,
        pub last_committed: Option<RaftCommit>,
    }

    #[tokio::test(basic_scheduler)]
    async fn test_send_proposal_is_commited_1_node() {
        test_send_proposal_check_commited(1).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_send_proposal_is_commited_2_nodes() {
        test_send_proposal_check_commited(2).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_send_proposal_is_commited_3_nodes() {
        test_send_proposal_check_commited(3).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_send_proposal_is_commited_20_nodes() {
        test_send_proposal_check_commited(20).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Verify proposal are committed by all peers regardless of the proposer.
    async fn test_send_proposal_check_commited(num_peers: u64) {
        let _ = tracing_subscriber::fmt::try_init();
        let (peer_indexes, mut test_nodes) = test_configs(num_peers);
        let peer_msg_lost = Arc::new(Mutex::new(HashSet::new()));
        let join_handles = spawn_nodes_loops(&peer_indexes, &mut test_nodes, &peer_msg_lost);
        let idx_1 = cmp::min(1, num_peers - 1) as usize;

        all_recv_send_proposed_data(&mut test_nodes, 0, vec![17]).await;
        all_recv_send_proposed_data(&mut test_nodes, idx_1, vec![33]).await;

        close_nodes_loops(test_nodes, join_handles).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_snapshot_1_node() {
        test_snapshot(1).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_snapshot_2_nodes() {
        test_snapshot(2).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_snapshot_3_nodes() {
        test_snapshot(3).await;
    }

    #[tokio::test(basic_scheduler)]
    async fn test_snapshot_20_nodes() {
        test_snapshot(20).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Verify snapshot does not interfer with later proposal.
    async fn test_snapshot(num_peers: u64) {
        let _ = tracing_subscriber::fmt::try_init();
        let (peer_indexes, mut test_nodes) = test_configs(num_peers);
        let peer_msg_lost = Arc::new(Mutex::new(HashSet::new()));
        let join_handles = spawn_nodes_loops(&peer_indexes, &mut test_nodes, &peer_msg_lost);

        all_recv_send_proposed_data(&mut test_nodes, 0, vec![17]).await;
        all_snapshot(&mut test_nodes, vec![12]).await;
        all_recv_send_proposed_data(&mut test_nodes, 0, vec![33]).await;

        close_nodes_loops(test_nodes, join_handles).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Node not receiving message can catch up.
    #[tokio::test(basic_scheduler)]
    async fn test_catch_up_3() {
        let _ = tracing_subscriber::fmt::try_init();
        let (peer_indexes, mut test_nodes) = test_configs(3);
        let peer_msg_lost = Arc::new(Mutex::new(Some(3).into_iter().collect::<HashSet<u64>>()));
        let join_handles = spawn_nodes_loops(&peer_indexes, &mut test_nodes, &peer_msg_lost);
        let (last_test_node, majority_test_nodes) = test_nodes.split_last_mut().unwrap();

        info!("Process with majority ignoring unresponsive node");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![17]).await;
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![33]).await;

        info!("Unresponsive node back catching up");
        peer_msg_lost.lock().await.clear();
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![vec![17]]);
        let commit1 = one_recv_commited(last_test_node).await;
        assert_eq!(commit1, vec![vec![33]]);

        info!("Complete test");
        close_nodes_loops(test_nodes, join_handles).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Node not receiving message can catch up, snapshot exist but record still there.
    #[tokio::test(basic_scheduler)]
    async fn test_skip_snapshot_catch_up_3() {
        let _ = tracing_subscriber::fmt::try_init();
        let (peer_indexes, mut test_nodes) = test_configs(3);
        let peer_msg_lost = Arc::new(Mutex::new(Some(3).into_iter().collect::<HashSet<u64>>()));
        let join_handles = spawn_nodes_loops(&peer_indexes, &mut test_nodes, &peer_msg_lost);
        let (last_test_node, majority_test_nodes) = test_nodes.split_last_mut().unwrap();
        let snapshot = vec![12];

        info!("Process with majority ignoring unresponsive node");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![17]).await;
        all_snapshot(majority_test_nodes, snapshot.clone()).await;
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![33]).await;

        info!("Unresponsive node back catching up");
        peer_msg_lost.lock().await.clear();
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![vec![17]]);
        let commit1 = one_recv_commited(last_test_node).await;
        assert_eq!(commit1, vec![vec![33]]);

        info!("Complete test");
        close_nodes_loops(test_nodes, join_handles).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Node not receiving message can catch up, need to use snapshot as record is gone.
    #[tokio::test(basic_scheduler)]
    async fn test_snap_catch_up_3() {
        let _ = tracing_subscriber::fmt::try_init();
        let (peer_indexes, mut test_nodes) = test_configs(3);
        let peer_msg_lost = Arc::new(Mutex::new(Some(3).into_iter().collect::<HashSet<u64>>()));
        let join_handles = spawn_nodes_loops(&peer_indexes, &mut test_nodes, &peer_msg_lost);
        let (last_test_node, majority_test_nodes) = test_nodes.split_last_mut().unwrap();
        let (snapshot1, snapshot2) = (vec![12], vec![13]);

        info!("Process with majority ignoring unresponsive node");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![17]).await;
        info!("Two Snapshot so record compacted up to first one");
        all_snapshot(majority_test_nodes, snapshot1.clone()).await;
        all_snapshot(majority_test_nodes, snapshot2).await;
        info!("Proposal after snapshot");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![33]).await;

        info!("Unresponsive node back catching up");
        peer_msg_lost.lock().await.clear();
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![snapshot1]);
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![vec![33]]);

        info!("Complete test");
        close_nodes_loops(test_nodes, join_handles).await;
    }

    // Setup a peer group running all raft loops and dispatching messages.
    // Node not receiving message can catch up, and then need to use snapshot as record is gone.
    #[tokio::test(basic_scheduler)]
    async fn test_snap_catch_up_after_start_3() {
        let _ = tracing_subscriber::fmt::try_init();
        let (peer_indexes, mut test_nodes) = test_configs(3);
        let peer_msg_lost_set: HashSet<u64> = Some(3).into_iter().collect();
        let peer_msg_lost = Arc::new(Mutex::new(peer_msg_lost_set.clone()));
        let join_handles = spawn_nodes_loops(&peer_indexes, &mut test_nodes, &peer_msg_lost);
        let (last_test_node, majority_test_nodes) = test_nodes.split_last_mut().unwrap();
        let (snapshot1, snapshot2) = (vec![12], vec![13]);

        info!("Process with majority ignoring unresponsive node and catch up");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![17]).await;
        peer_msg_lost.lock().await.clear();
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![vec![17]]);
        *peer_msg_lost.lock().await = peer_msg_lost_set;

        info!("Process with majority ignoring unresponsive node");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![18]).await;
        info!("Two Snapshot so record compacted up to first one");
        all_snapshot(majority_test_nodes, snapshot1.clone()).await;
        all_snapshot(majority_test_nodes, snapshot2).await;
        info!("Proposal after snapshot");
        all_recv_send_proposed_data(majority_test_nodes, 0, vec![33]).await;

        info!("Unresponsive node back catching up");
        peer_msg_lost.lock().await.clear();
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![snapshot1]);
        let commit0 = one_recv_commited(last_test_node).await;
        assert_eq!(commit0, vec![vec![33]]);

        info!("Complete test");
        close_nodes_loops(test_nodes, join_handles).await;
    }

    // Setup RAFT: Raft loops and Raft message dispatching loops.
    fn spawn_nodes_loops(
        peer_indexes: &HashMap<u64, usize>,
        test_nodes: &mut [TestNode],
        peer_msg_lost: &Arc<Mutex<HashSet<u64>>>,
    ) -> Vec<JoinHandle<()>> {
        let msg_txs: Vec<_> = test_nodes.iter().map(|node| node.cmd_tx.clone()).collect();

        let mut join_handles = Vec::new();
        for test_node in test_nodes {
            let raft_config = test_node.raft_config.take().unwrap();
            let peer_span = error_span!("", node_id = ?raft_config.cfg.id);
            join_handles.push(tokio::spawn(
                async move {
                    run_raft_loop(raft_config).await;
                }
                .instrument(peer_span.clone()),
            ));

            let msg_out_rx = test_node.msg_out_rx.take().unwrap();
            let peer_indexes = peer_indexes.clone();
            let msg_txs = msg_txs.clone();
            let peer_msg_lost = peer_msg_lost.clone();
            join_handles.push(tokio::spawn(
                async move {
                    dispatch_messages_loop(msg_out_rx, peer_indexes, msg_txs, peer_msg_lost).await;
                }
                .instrument(peer_span.clone()),
            ));
        }
        join_handles
    }

    // Close raft loop so spawned task can complete and wait for completion.
    async fn close_nodes_loops(mut test_nodes: Vec<TestNode>, join_handles: Vec<JoinHandle<()>>) {
        for test_node in &mut test_nodes {
            test_node.cmd_tx.send(RaftCmd::Close).unwrap();
        }
        join_all(join_handles).await;
    }

    // Send a proposal and wait for it to be commited.
    async fn all_recv_send_proposed_data(
        test_nodes: &mut [TestNode],
        from_node_idx: usize,
        proposed_data: RaftData,
    ) {
        send_proposal(&mut test_nodes[from_node_idx], proposed_data.clone()).await;

        let commited_data = recv_commited(test_nodes).await;
        let expected = expected_commited(test_nodes, &[proposed_data]);
        assert_eq!(commited_data, expected);
    }

    async fn run_raft_loop(raft_config: RaftConfig) {
        let mut raft_node = RaftNode::new(raft_config);
        raft_node.run_raft_loop().await;
    }

    async fn dispatch_messages_loop(
        mut msg_out_rx: RaftMsgReceiver,
        peer_indexes: HashMap<u64, usize>,
        msg_txs: Vec<RaftCmdSender>,
        peer_msg_lost: Arc<Mutex<HashSet<u64>>>,
    ) {
        loop {
            match msg_out_rx.recv().await {
                Some(msg) => {
                    let peer_msg_lost = peer_msg_lost.lock().await;
                    if peer_msg_lost.contains(&msg.to) || peer_msg_lost.contains(&msg.from) {
                        trace!("Drop message: {:?}", &msg);
                        continue;
                    }
                    let to_index = peer_indexes[&msg.to];
                    let _ = msg_txs[to_index].send(RaftCmd::Raft(RaftMessageWrapper(msg)));
                }
                None => {
                    // Disconnected
                    return;
                }
            }
        }
    }

    async fn send_proposal(test_node: &mut TestNode, data: RaftData) {
        test_node.cmd_tx.send(RaftCmd::Propose { data }).unwrap();
    }

    async fn all_snapshot(test_nodes: &mut [TestNode], data: RaftData) {
        for test_node in test_nodes {
            let idx = test_node.last_committed.as_ref().unwrap().index;
            let data = data.clone();

            let cmd = RaftCmd::Snapshot { idx, data };
            test_node.cmd_tx.send(cmd).unwrap();
        }
    }

    async fn recv_commited(test_nodes: &mut [TestNode]) -> Vec<Vec<RaftData>> {
        let mut received = Vec::new();
        for test_node in test_nodes {
            received.push(one_recv_commited(test_node).await)
        }
        received
    }

    async fn one_recv_commited(test_node: &mut TestNode) -> Vec<RaftData> {
        let commits = test_node.committed_rx.recv().await.unwrap();
        test_node.last_committed = Some(commits.last().cloned().unwrap());
        commits.into_iter().map(|e| e.data.take_data()).collect()
    }

    fn expected_commited(test_nodes: &[TestNode], expected: &[RaftData]) -> Vec<Vec<RaftData>> {
        test_nodes.iter().map(|_| expected.to_owned()).collect()
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

    fn test_config(peer_id: u64, peers: &[u64]) -> TestNode {
        let (raft_config, node_channels) = RaftNode::init_config(
            Config {
                id: peer_id,
                peers: peers.to_owned(),
                ..Default::default()
            },
            Duration::from_millis(1),
        );

        TestNode {
            raft_config: Some(raft_config),
            cmd_tx: node_channels.cmd_tx,
            committed_rx: node_channels.committed_rx,
            msg_out_rx: Some(node_channels.msg_out_rx),
            last_committed: None,
        }
    }
}
