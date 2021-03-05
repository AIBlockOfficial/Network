use std::collections::BTreeMap;
//use crate::raft::{RaftCommit, RaftCommitData, RaftData, RaftMessageWrapper};
use crate::active_raft::ActiveRaft;
use crate::raft::RaftData;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{debug, warn};

/// Key serialized into RaftData and process by Raft.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RaftContextKey {
    pub proposer_id: u64,
    pub proposal_id: u64,
}

#[derive(Clone, Default)]
pub struct RaftInFlightProposals {
    /// Proposed items in flight.
    proposed_in_flight: BTreeMap<RaftContextKey, (RaftData, RaftData)>,
    /// The last id of a proposed item.
    proposed_last_id: u64,
}

impl RaftInFlightProposals {
    /// Checks a commit of the RaftData for validity
    /// Apply commited proposal
    ///
    /// ### Arguments
    ///
    /// * `raft_data` - Data for the commit
    /// * `raft_ctx`  - Context for the commit
    pub async fn received_commit_poposal<'a, Item: Deserialize<'a>>(
        &mut self,
        raft_data: &'a [u8],
        raft_ctx: &'a [u8],
    ) -> Option<(RaftContextKey, Item, bool)> {
        match (
            deserialize::<Item>(raft_data),
            deserialize::<RaftContextKey>(raft_ctx),
        ) {
            (Ok(item), Ok(key)) => {
                let removed = self.proposed_in_flight.remove(&key).is_some();
                Some((key, item, removed))
            }
            (Err(error), Ok(key)) => {
                warn!("RaftItem-deserialize key {:?} -> error: {:?}", key, error);
                None
            }
            (_, Err(error)) => {
                warn!("RaftContextKey-deserialize error {:?}", error);
                None
            }
        }
    }

    /// Propose an item to raft if use_raft, or commit it otherwise.
    ///
    /// ### Arguments
    ///
    ///  * `raft_active` - The raft instance to propose to.
    ///  * `item`        - The item to be proposed to a raft.
    pub async fn propose_item<Item: Serialize + Debug>(
        &mut self,
        raft_active: &mut ActiveRaft,
        item: &Item,
    ) -> RaftContextKey {
        self.proposed_last_id += 1;
        let key = RaftContextKey {
            proposer_id: raft_active.peer_id(),
            proposal_id: self.proposed_last_id,
        };

        debug!("propose_item: {:?} -> {:?}", key, item);
        let data = serialize(item).unwrap();
        let context = serialize(&key).unwrap();
        self.proposed_in_flight
            .insert(key, (data.clone(), context.clone()));

        raft_active.propose_data(data, context).await;
        key
    }

    /// Re-Propose an item in flight to raft.
    ///
    /// ### Arguments
    ///
    ///  * `raft_active` - The raft instance to propose to.
    ///  * `key`         - The item key to be proposed to a raft.
    pub async fn re_propose_item(&mut self, raft_active: &mut ActiveRaft, key: RaftContextKey) {
        if let Some((data, context)) = self.proposed_in_flight.get(&key) {
            raft_active
                .propose_data(data.clone(), context.clone())
                .await;
        }
    }

    /// Re-Propose all items in flight to raft.
    ///
    /// ### Arguments
    ///
    ///  * `raft_active` - The raft instance to propose to.
    pub async fn re_propose_all_items(&mut self, raft_active: &mut ActiveRaft) {
        debug!(
            "Re-propose all non committed items: {}",
            self.proposed_in_flight.len()
        );
        for (data, context) in self.proposed_in_flight.values() {
            raft_active
                .propose_data(data.clone(), context.clone())
                .await;
        }
    }
}
