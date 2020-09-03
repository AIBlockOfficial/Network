use crate::comms_handler::{CommsError, Event};
use crate::constants::{BLOCK_SIZE, MINING_DIFFICULTY, PARTITION_LIMIT, PEER_LIMIT, UNICORN_LIMIT};
use crate::interfaces::{
    ComputeInterface, ComputeMessage, Contract, MineRequest, NodeType, ProofOfWork,
    ProofOfWorkBlock, Response, Tx,
};
use crate::unicorn::UnicornShard;
use crate::utils::get_partition_entry_key;
use crate::Node;

use bincode::deserialize;
use bytes::Bytes;
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::secretbox::{gen_key, Key};
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryInto,
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tracing::{debug, info, info_span, warn};

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, ComputeError>;

#[derive(Debug)]
pub enum ComputeError {
    Network(CommsError),
    Serialization(bincode::Error),
}

impl fmt::Display for ComputeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for ComputeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Network(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
        }
    }
}

impl From<CommsError> for ComputeError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<bincode::Error> for ComputeError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

#[derive(Debug, Clone)]
pub struct ComputeNode {
    node: Node,
    pub current_block: Vec<u8>,
    pub unicorn_limit: usize,
    current_random_num: Vec<u8>,
    pub partition_key: Key,
    pub partition_list: Vec<ProofOfWork>,
    pub request_list: BTreeMap<String, bool>,
    pub unicorn_list: HashMap<SocketAddr, UnicornShard>,
}

impl ComputeNode {
    /// Generates a new compute node instance
    ///
    /// ### Arguments
    ///
    /// * `address` - Address for the current compute node
    pub async fn new(address: SocketAddr) -> Result<ComputeNode> {
        Ok(ComputeNode {
            node: Node::new(address, PEER_LIMIT, NodeType::Compute).await?,
            unicorn_list: HashMap::new(),
            unicorn_limit: UNICORN_LIMIT,
            current_block: Vec::new(),
            current_random_num: Vec::new(),
            request_list: BTreeMap::new(),
            partition_list: Vec::new(),
            partition_key: gen_key(),
        })
    }

    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// I'm lazy, so just making another verifier for now
    pub fn validate_pow_block(pow: &mut ProofOfWorkBlock) -> bool {
        let mut pow_body = pow.address.as_bytes().to_vec();
        pow_body.append(&mut pow.nonce.clone());
        pow_body.append(&mut pow.block.clone());
        pow_body.append(&mut pow.coinbase.clone());

        let pow_hash = Sha3_256::digest(&pow_body).to_vec();

        for entry in pow_hash[0..MINING_DIFFICULTY].to_vec() {
            if entry != 0 {
                return false;
            }
        }

        true
    }

    /// Generates a garbage file for use in network testing. Will save the file
    /// as the current block internally
    ///
    /// ### Arguments
    ///
    /// * `size`    - Size of the file in bytes
    pub fn generate_garbage_block(&mut self, size: usize) {
        let garbage_block = vec![0; size];
        self.current_block = garbage_block;
    }

    /// Generates a garbage random num for use in network testing
    ///
    /// ### Arguments
    ///
    /// * `size`    - Size of the file in bytes
    pub fn generate_random_num(&mut self, size: usize) {
        let random_num = vec![0; size];
        self.current_random_num = random_num;
    }

    /// Gets a decremented socket address of peer for storage
    fn get_storage_address(&self, address: SocketAddr) -> SocketAddr {
        let mut storage_address = address.clone();
        storage_address.set_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
        storage_address.set_port(address.port() - 1);

        storage_address
    }

    /// Util function to get a socket address for PID table checks
    fn get_comms_address(&self, address: SocketAddr) -> SocketAddr {
        let comparison_port = address.port() + 1;
        let mut comparison_addr = address.clone();

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Sends block to a mining node.
    pub async fn send_block(&mut self, peer: SocketAddr) -> Result<()> {
        let block_to_send = self.current_block.clone();

        self.node
            .send(
                peer,
                MineRequest::SendBlock {
                    block: block_to_send,
                },
            )
            .await?;
        Ok(())
    }

    /// Sends the full partition list to the participants
    pub async fn send_partition_list(&mut self, peer: SocketAddr) -> Result<()> {
        self.node
            .send(
                peer,
                MineRequest::SendPartitionList {
                    p_list: self.partition_list.clone(),
                },
            )
            .await?;
        Ok(())
    }

    /// Sends random number to a mining node.
    pub async fn send_random_number(&mut self, peer: SocketAddr) -> Result<()> {
        let random_num = self.current_random_num.clone();
        println!("RANDOM NUMBER IN COMPUTE: {:?}", random_num);

        self.node
            .send(peer, MineRequest::SendRandomNum { rnum: random_num })
            .await?;
        Ok(())
    }

    /// Floods all peers with a PoW for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_pow_to_peers(&self, _address: SocketAddr, _pow: &Vec<u8>) {
        println!("Flooding PoW to peers not implemented");
    }

    /// Floods all peers with a PoW commit for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_commit_to_peers(&self, _address: SocketAddr, _commit: &ProofOfWork) {
        println!("Flooding commit to peers not implemented");
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event(&mut self) -> Option<Result<Response>> {
        let event = self.node.next_event().await?;
        self.handle_event(event).await.into()
    }

    async fn handle_event(&mut self, event: Event) -> Result<Response> {
        match event {
            Event::NewFrame { peer, frame } => Ok(self.handle_new_frame(peer, frame).await?),
        }
    }

    /// Hanldes a new incoming message from a peer.
    async fn handle_new_frame(&mut self, peer: SocketAddr, frame: Bytes) -> Result<Response> {
        info_span!("peer", ?peer).in_scope(|| {
            let req = deserialize::<ComputeMessage>(&frame).map_err(|error| {
                warn!(?error, "frame-deserialize");
                error
            })?;

            info_span!("request", ?req).in_scope(|| {
                let response = self.handle_request(peer, req);
                debug!(?response, ?peer, "response");

                Ok(response)
            })
        })
    }

    /// Handles a compute request.
    fn handle_request(&mut self, peer: SocketAddr, req: ComputeMessage) -> Response {
        use ComputeMessage::*;

        match req {
            SendPoW { pow } => self.receive_pow(peer, pow),
            SendPartitionEntry { partition_entry } => {
                self.receive_partition_entry(peer, partition_entry)
            }
            SendPartitionRequest => self.receive_partition_request(peer),
        }
    }

    /// Receive a partition request from a miner node
    /// TODO: This may need to be part of the ComputeInterface depending on key agreement
    fn receive_partition_request(&mut self, peer: SocketAddr) -> Response {
        if self.request_list.is_empty() {
            self.generate_random_num(5);
        }

        self.request_list.insert(peer.to_string(), false);

        Response {
            success: true,
            reason: "Partition request received successfully",
        }
    }

    /// Receives the light POW for partition inclusion
    fn receive_partition_entry(
        &mut self,
        peer: SocketAddr,
        partition_entry: ProofOfWork,
    ) -> Response {
        let mut pow_mut = partition_entry.clone();

        if self.partition_list.len() < PARTITION_LIMIT && Self::validate_pow(&mut pow_mut) {
            self.partition_list.push(partition_entry.clone());

            if self.partition_list.len() == PARTITION_LIMIT {
                let key = get_partition_entry_key(self.partition_list.clone());
                let hashed_key = Sha3_256::digest(&key).to_vec();
                let key_slice: [u8; 32] = hashed_key[..].try_into().unwrap();
                self.partition_key = Key(key_slice);

                return Response {
                    success: true,
                    reason: "Partition list is full",
                };
            }

            return Response {
                success: true,
                reason: "Partition PoW received successfully",
            };
        }

        if self.partition_list.len() >= PARTITION_LIMIT {
            return Response {
                success: false,
                reason: "Partition list is full",
            };
        }

        Response {
            success: false,
            reason: "PoW received is invalid",
        }
    }

    /// Floods the random number to everyone who requested
    pub async fn flood_rand_num_to_requesters(&mut self) -> Result<()> {
        let mut exclusions = Vec::new();

        for (peer, peer_sent) in self.request_list.clone() {
            if !peer_sent {
                exclusions.push(peer.clone());
                let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");

                println!("PEER ADDRESS: {:?}", peer_addr);

                let _result = self.send_random_number(peer_addr).await.unwrap();
            }
        }

        for exclusion in exclusions {
            self.request_list.insert(exclusion, true);
        }

        Ok(())
    }

    /// Floods the full partition list to participants
    pub async fn flood_block_to_partition(&mut self) -> Result<()> {
        self.generate_garbage_block(BLOCK_SIZE);

        for (peer, _) in self.request_list.clone() {
            let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");
            let _result = self.send_block(peer_addr).await.unwrap();
        }

        Ok(())
    }

    /// Floods all peers with the full partition list
    pub async fn flood_list_to_partition(&mut self) -> Result<()> {
        for (peer, _) in self.request_list.clone() {
            let peer_addr: SocketAddr = peer.parse().expect("Unable to parse socket address");
            let _result = self.send_partition_list(peer_addr).await.unwrap();
        }

        Ok(())
    }
}

impl ComputeInterface for ComputeNode {
    fn validate_pow(pow: &mut ProofOfWork) -> bool {
        let mut pow_body = pow.address.as_bytes().to_vec();
        pow_body.append(&mut pow.nonce.clone());

        let pow_hash = Sha3_256::digest(&pow_body).to_vec();

        for entry in pow_hash[0..MINING_DIFFICULTY].to_vec() {
            if entry != 0 {
                return false;
            }
        }

        true
    }

    fn receive_commit(&mut self, address: SocketAddr, commit: ProofOfWork) -> Response {
        if let Some(entry) = self.unicorn_list.get_mut(&address) {
            if entry.is_valid(commit.clone()) {
                self.flood_commit_to_peers(address, &commit);
                return Response {
                    success: true,
                    reason: "Commit received successfully",
                };
            }

            return Response {
                success: false,
                reason: "Commit not valid. Rejecting...",
            };
        }

        Response {
            success: false,
            reason: "The node submitting a commit never submitted a promise",
        }
    }

    fn receive_pow(&mut self, address: SocketAddr, pow: ProofOfWorkBlock) -> Response {
        info!(?address, "Received PoW");
        let mut pow_block = pow.clone();

        if Self::validate_pow_block(&mut pow_block) {
            return Response {
                success: true,
                reason: "Received PoW successfully",
            };
        }

        // if self.unicorn_list.len() < self.unicorn_limit {
        //     let mut unicorn_value = UnicornShard::new();
        //     unicorn_value.promise = pow.clone();

        //     self.unicorn_list.insert(address, unicorn_value);

        //     return Response {
        //         success: true,
        //         reason: "Received PoW successfully",
        //     };
        // }

        Response {
            success: false,
            reason: "UnicornShard limit reached. Unable to receive PoW",
        }
    }

    fn get_unicorn_table(&self) -> Vec<UnicornShard> {
        let mut unicorn_table = Vec::with_capacity(self.unicorn_list.len());

        for unicorn in self.unicorn_list.values() {
            unicorn_table.push(unicorn.clone());
        }

        unicorn_table
    }

    fn partition(&self, _uuids: Vec<&'static str>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn get_service_levels(&self) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_transactions(&self, _transactions: Vec<Tx>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn execute_contract(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn get_next_block_reward(&self) -> f64 {
        0.0
    }
}
