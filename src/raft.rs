use std::fmt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};

use raft::prelude::*;
use raft::storage::MemStorage;

type RaftData = Vec<u8>;
type CommitedSender = mpsc::Sender<Vec<RaftData>>;
type CommitedReceiver = mpsc::Receiver<Vec<RaftData>>;
type MsgSender = mpsc::Sender<Msg>;
type MsgReceiver = mpsc::Receiver<Msg>;

struct RaftConfig {
    msg_rx: MsgReceiver,
    committed_tx: CommitedSender,
    timeout_duration: Duration,
}

enum Msg {
    Propose { data: RaftData },
    Raft(Message),
}

impl fmt::Debug for Msg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Propose { ref data, .. } => write!(f, "Propose {:?}", data),
            Self::Raft(ref msg) => write!(f, "Raft {:?}", msg),
        }
    }
}

async fn run_raft_loop(raft_config: RaftConfig) {
    let RaftConfig {
        mut msg_rx,
        mut committed_tx,
        timeout_duration,
    } = raft_config;
    let cfg = Config {
        id: 1,
        peers: vec![1],
        ..Default::default()
    };
    let peers = vec![];

    let storage = MemStorage::new();
    let mut node = RawNode::new(&cfg, storage, peers).unwrap();
    let mut timeout_at_time = Instant::now() + timeout_duration;
    let mut propose_data_backlog = Vec::new();

    loop {
        match timeout_at(timeout_at_time, msg_rx.recv()).await {
            Ok(Some(Msg::Propose { data })) => {
                propose_data_backlog.push(data);
            }
            Ok(Some(Msg::Raft(m))) => node.step(m).unwrap(),
            Err(_) => {
                // Timeout
                timeout_at_time = Instant::now() + timeout_duration;
                node.tick();
            }
            Ok(None) => {
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

        process_ready(&mut node, &mut committed_tx).await;
    }
}

async fn process_ready(node: &mut RawNode<MemStorage>, committed_tx: &mut CommitedSender) {
    if !node.has_ready() {
        return;
    }

    let mut ready = node.ready();

    let is_leader = node.raft.leader_id == node.raft.id;
    if is_leader {
        send_messages_to_followers(node, &mut ready);
        update_ready_mut_store(node, &mut ready);
    } else {
        update_ready_mut_store(node, &mut ready);
        send_messages_to_leader(node, &mut ready);
    }

    apply_committed_entries(&mut ready, committed_tx).await;

    node.advance(ready);
}

fn send_messages_to_followers(_node: &mut RawNode<MemStorage>, ready: &mut Ready) {
    for _msg in ready.messages.drain(..) {
        // TODO: Send messages to followers.
    }
}

fn send_messages_to_leader(_node: &mut RawNode<MemStorage>, ready: &mut Ready) {
    for _msg in ready.messages.drain(..) {
        // TODO: Send the messages to the leader.
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

async fn apply_committed_entries(ready: &mut Ready, committed_tx: &mut CommitedSender) {
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

#[tokio::test(threaded_scheduler)]
async fn test_send_proposal_is_commited() {
    let (mut msg_tx, msg_rx) = mpsc::channel(100);
    let (committed_tx, mut committed_rx) = mpsc::channel(100);

    let main_handle = tokio::spawn(async move {
        run_raft_loop(RaftConfig {
            msg_rx,
            committed_tx,
            timeout_duration: Duration::from_millis(1),
        })
        .await;
    });

    // Send a proposal and wait for it to be commited
    let proposed_data = vec![17];
    msg_tx
        .send(Msg::Propose {
            data: proposed_data.clone(),
        })
        .await
        .unwrap();
    let commited_data = committed_rx.recv().await.unwrap();
    assert_eq!(commited_data, vec![proposed_data]);

    // Close raft loop
    drop(msg_tx);

    // Wait for the process to complete i.e dropping the msg_tx.
    let (result,) = tokio::join!(main_handle);
    result.unwrap();
}
