use std::collections::BTreeMap;
use crate::interfaces::ProofOfWork;
use crate::unicorn::UnicornShard;
use crate::comms_handler::CommsHandler;
use crate::interfaces::{ ComputeInterface, Contract, Tx, Response };

/// Limit for the number of peers a compute node may have
const PEER_LIMIT: usize = 6;

/// Limit for the number of PoWs a compute node may have for UnicornShard creation
const UNICORN_LIMIT: usize = 5;


#[derive(Debug, Clone)]
pub struct ComputeNode {
    address: &'static str,
    pub unicorn_list: BTreeMap<&'static str, UnicornShard>,
    peers: Vec<ComputeNode>,
    comms_handler: CommsHandler,
    pub peer_limit: usize,
    pub unicorn_limit: usize
}

impl ComputeNode {
    /// Adds a new peer to the compute node's list of peers.
    /// TODO: Could make peer_list a LRU cache later.
    /// 
    /// ### Arguments
    /// 
    /// * `address`     - Address of the new peer
    /// * `force_add`   - If true and the peer limit is reached, an old peer will be ejected to make space
    pub fn add_peer(&mut self, address: &'static str, force_add: bool) -> Response {
        let is_full = self.peers.len() >= self.peer_limit;

        if force_add && is_full  {
            self.peers.truncate(self.peer_limit);
        }

        if !is_full {
            self.peers.push(ComputeNode::new(address));
            return Response { success: true, reason: "Peer added successfully" };
        }

        Response { success: false, reason: "Peer list is full. Unable to add new peer" } 
    }

    /// Floods all peers with a PoW for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    /// 
    /// ### Arguments
    /// 
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_pow_to_peers(&self, address: &'static str, pow: &Vec<u8>) {
        println!("Flooding PoW to peers not implemented");
    }

    /// Floods all peers with a PoW commit for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    /// 
    /// ### Arguments
    /// 
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_commit_to_peers(&self, address: &'static str, commit: &ProofOfWork) {
        println!("Flooding commit to peers not implemented");
    }
}


impl ComputeInterface for ComputeNode {
    fn new(address: &'static str) -> ComputeNode {
        ComputeNode {
            address: address,
            unicorn_list: BTreeMap::new(),
            peers: Vec::with_capacity(PEER_LIMIT),
            peer_limit: PEER_LIMIT,
            unicorn_limit: UNICORN_LIMIT,
            comms_handler: CommsHandler
        }
    }

    fn receive_commit(&mut self, address: &'static str, commit: ProofOfWork) -> Response {
        if let Some(entry) = self.unicorn_list.get_mut(address) {
            if entry.is_valid(commit.clone()) {
                self.flood_commit_to_peers(address, &commit);
                return Response { success: true, reason: "Commit received successfully" }
            }

            return Response { success: false, reason: "Commit not valid. Rejecting..." };
        }
        
        Response { success: false, reason: "The node submitting a commit never submitted a promise" }
    }

    fn receive_pow(&mut self, address: &'static str, pow: Vec<u8>) -> Response {
        if self.unicorn_list.len() < self.unicorn_limit {
            let mut unicorn_value = UnicornShard::new();
            unicorn_value.promise = pow.clone();

            self.unicorn_list.insert(address, unicorn_value);
            self.flood_pow_to_peers(address, &pow);

            return Response { success: true, reason: "Received PoW successfully" };
        }

        Response { success: false, reason: "UnicornShard limit reached. Unable to receive PoW" }
    }

    fn get_unicorn_table(&self) -> Vec<UnicornShard> {
        let mut unicorn_table = Vec::with_capacity(self.unicorn_list.len());

        for unicorn in self.unicorn_list.values() {
            unicorn_table.push(unicorn.clone());
        }

        unicorn_table
    }

    fn partition(&self, uuids: Vec<&'static str>) -> Response {
        Response { success: false, reason: "Not implemented yet" }
    }

    fn get_service_levels(&self) -> Response {
        Response { success: false, reason: "Not implemented yet" }
    }

    fn receive_transactions(&self, transactions: Vec<Tx>) -> Response {
        Response { success: false, reason: "Not implemented yet" }
    }

    fn execute_contract(&self, contract: Contract) -> Response {
        Response { success: false, reason: "Not implemented yet" }
    }

    fn get_next_block_reward(&self) -> f64 {
        0.0
    }
}
