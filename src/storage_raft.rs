use crate::active_raft::ActiveRaft;
use crate::configurations::StorageNodeConfig;
use crate::interfaces::{CommonBlockInfo, MinedBlockExtraInfo};
use crate::raft::{RaftData, RaftMessageWrapper};
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::{self, Instant};
use tracing::{debug, warn};

/// Item serialized into RaftData and process by Raft.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageRaftItem {
    PartBlock(ReceivedBlock),
    CompleteBlock(u64),
}

/// Key serialized into RaftData and process by Raft.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageRaftKey {
    pub proposer_id: u64,
    pub proposal_id: u64,
}

/// Mined block received.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedBlock {
    pub peer: SocketAddr,
    pub common: CommonBlockInfo,
    pub per_node: MinedBlockExtraInfo,
}

/// Complete block info with all mining transactions and proof of work.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteBlock {
    pub common: CommonBlockInfo,
    pub per_node: BTreeMap<u64, MinedBlockExtraInfo>,
}

/// Different state for timeout
#[derive(Clone, Debug)]
pub enum ProposeBlockTimeout {
    First,
    None,
    Some(Instant),
}

/// All fields that are consensused between the RAFT group.
/// These fields need to be written and read from a committed log event.
#[derive(Clone, Debug, Default)]
pub struct StorageConsensused {
    /// Sufficient majority
    sufficient_majority: usize,
    /// Index of the last completed block.
    current_block_num: u64,
    /// Peer ids that have voted to complete the block.
    current_block_complete_timeout_peer_ids: BTreeSet<u64>,
    /// Part block completed by Peer ids.
    current_block_completed_parts: BTreeMap<Vec<u8>, CompleteBlock>,
}

/// Consensused Compute fields and consensus managment.
pub struct StorageRaft {
    /// True if first peer (leader).
    first_raft_peer: bool,
    /// The raft instance to interact with.
    raft_active: ActiveRaft,
    /// Consensused fields.
    consensused: StorageConsensused,
    /// Min duration between each block poposal.
    propose_block_timeout_duration: Duration,
    /// Timeout expiration time for block poposal.
    propose_block_timeout_at: ProposeBlockTimeout,
    /// The last id of a proposed item.
    proposed_last_id: u64,
    /// Received blocks
    local_blocks: Vec<ReceivedBlock>,
}

impl fmt::Debug for StorageRaft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageRaft()")
    }
}

impl StorageRaft {
    /// Create a StorageRaft, need to spawn the raft loop to use raft.
    pub fn new(config: &StorageNodeConfig) -> Self {
        let raft_active = ActiveRaft::new(
            config.storage_node_idx,
            &config.storage_nodes,
            config.storage_raft != 0,
            Duration::from_millis(config.storage_raft_tick_timeout as u64),
        );

        let propose_block_timeout_duration =
            Duration::from_millis(config.storage_block_timeout as u64);
        let propose_block_timeout_at = ProposeBlockTimeout::First;

        let first_raft_peer = config.storage_node_idx == 0 || !raft_active.use_raft();
        let peers_len = raft_active.peers_len();

        let consensused = StorageConsensused {
            sufficient_majority: peers_len / 2 + 1,
            ..StorageConsensused::default()
        };

        Self {
            first_raft_peer,
            raft_active,
            consensused,
            propose_block_timeout_duration,
            propose_block_timeout_at,
            proposed_last_id: 0,
            local_blocks: Vec::new(),
        }
    }

    /// All the peers to connect to when using raft.
    pub fn raft_peer_to_connect(&self) -> impl Iterator<Item = &SocketAddr> {
        self.raft_active.raft_peer_to_connect()
    }

    /// Blocks & waits for a next event from a peer.
    pub fn raft_loop(&self) -> impl Future<Output = ()> {
        self.raft_active.raft_loop()
    }

    /// Signal to the raft loop to complete
    pub async fn close_raft_loop(&mut self) {
        self.raft_active.close_raft_loop().await
    }

    /// Blocks & waits for a next commit from a peer.
    pub async fn next_commit(&self) -> Option<RaftData> {
        self.raft_active.next_commit().await
    }

    /// Process result from next_commit.
    /// Return Some if block to mine is ready to generate.
    pub async fn received_commit(&mut self, raft_data: RaftData) -> Option<()> {
        let (key, item) = match deserialize::<(StorageRaftKey, StorageRaftItem)>(&raft_data) {
            Ok((key, item)) => (key, item),
            Err(error) => {
                warn!(?error, "StorageRaftItem-deserialize");
                return None;
            }
        };

        match item {
            StorageRaftItem::PartBlock(block) => {
                if self
                    .consensused
                    .is_current_block(block.common.block.header.b_num)
                {
                    debug!("PartBlock appened {:?}", key);
                    self.consensused.append_received_block(key, block);
                }
            }
            StorageRaftItem::CompleteBlock(idx) => {
                if self.consensused.is_current_block(idx) {
                    debug!("CompleteBlock appened ({},{:?})", idx, key);
                    self.consensused.append_received_block_timeout(key);
                }
            }
        }

        if self.consensused.has_block_ready_to_store() {
            Some(())
        } else {
            None
        }
    }

    /// Blocks & waits for a next message to dispatch from a peer.
    /// Message needs to be sent to given peer address.
    pub async fn next_msg(&self) -> Option<(SocketAddr, RaftMessageWrapper)> {
        self.raft_active.next_msg().await
    }

    /// Process a raft message: send to spawned raft loop.
    pub async fn received_message(&mut self, msg: RaftMessageWrapper) {
        self.raft_active.received_message(msg).await
    }

    /// Blocks & waits for a timeout to propose a block.
    pub async fn timeout_propose_block(&self) -> Option<()> {
        if let ProposeBlockTimeout::Some(time) = self.propose_block_timeout_at {
            time::delay_until(time).await;
            Some(())
        } else {
            None
        }
    }

    /// Process as a result of timeout_propose_block.
    /// Signal that the current block should complete.
    /// Reset timeout, Only restart it when complete block is generated.
    pub async fn propose_block_at_timeout(&mut self) {
        self.propose_block_timeout_at = ProposeBlockTimeout::None;
        self.propose_item(&StorageRaftItem::CompleteBlock(
            self.consensused.current_block_num,
        ))
        .await;
    }

    /// Add any ready local blocks.
    pub async fn propose_received_part_block(&mut self) {
        let local_blocks = std::mem::take(&mut self.local_blocks);
        for block in local_blocks.into_iter() {
            self.propose_item(&StorageRaftItem::PartBlock(block)).await;
        }
    }

    /// Propose an item to raft if use_raft, or commit it otherwise.
    async fn propose_item(&mut self, item: &StorageRaftItem) {
        self.proposed_last_id += 1;
        let key = StorageRaftKey {
            proposer_id: self.raft_active.peer_id(),
            proposal_id: self.proposed_last_id,
        };

        debug!("propose_item: {:?} -> {:?}", key, item);
        let data = serialize(&(&key, item)).unwrap();

        self.raft_active.propose_data(data).await
    }

    /// Append block to our local pool from which to propose
    /// consensused blocks.
    pub fn append_to_our_blocks(
        &mut self,
        peer: SocketAddr,
        common: CommonBlockInfo,
        mined_info: MinedBlockExtraInfo,
    ) {
        if let ProposeBlockTimeout::First = &self.propose_block_timeout_at {
            // Wait for compute and other nodes to be ready so we do not unecessarily
            // timeout early on first block.
            self.propose_block_timeout_at = self.next_propose_block_timeout_at();
        }

        self.local_blocks.push(ReceivedBlock {
            peer,
            common,
            per_node: mined_info,
        });
    }

    pub fn generate_complete_block(&mut self) -> CompleteBlock {
        self.propose_block_timeout_at = self.next_propose_block_timeout_at();
        self.consensused.generate_complete_block()
    }

    fn next_propose_block_timeout_at(&mut self) -> ProposeBlockTimeout {
        ProposeBlockTimeout::Some(Instant::now() + self.propose_block_timeout_duration)
    }
}

impl StorageConsensused {
    pub fn is_current_block(&self, block_num: u64) -> bool {
        block_num == self.current_block_num
    }

    pub fn has_block_ready_to_store(&self) -> bool {
        if self.current_block_complete_timeout_peer_ids.len() < self.sufficient_majority {
            return false;
        }

        let completed_blocks_len = self
            .current_block_completed_parts
            .values()
            .map(|v| v.per_node.len())
            .max()
            .unwrap_or(0);

        completed_blocks_len >= self.sufficient_majority
    }

    pub fn append_received_block_timeout(&mut self, key: StorageRaftKey) {
        self.current_block_complete_timeout_peer_ids
            .insert(key.proposer_id);
    }

    pub fn append_received_block(&mut self, key: StorageRaftKey, block: ReceivedBlock) {
        let block_ser = serialize(&block.common).unwrap();
        let block_hash = Sha3_256::digest(&block_ser).to_vec();

        let common = block.common;
        let node_info = block.per_node;
        let per_node = BTreeMap::new();
        self.current_block_completed_parts
            .entry(block_hash)
            .or_insert(CompleteBlock { common, per_node })
            .per_node
            .insert(key.proposer_id, node_info);
    }

    pub fn generate_complete_block(&mut self) -> CompleteBlock {
        self.current_block_num += 1;
        let _timeouts = std::mem::take(&mut self.current_block_complete_timeout_peer_ids);
        let completed_parts = std::mem::take(&mut self.current_block_completed_parts);

        let (_, complete_block) = completed_parts
            .into_iter()
            .max_by_key(|(_, v)| v.per_node.len())
            .unwrap();

        complete_block
    }
}

#[cfg(test)]
mod test {
    //use super::*;

    #[tokio::test]
    async fn test_storage_raft() {}
}
