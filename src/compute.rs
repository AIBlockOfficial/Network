use crate::comms_handler::{CommsError, Event};
use crate::constants::{BLOCK_SIZE, MINING_DIFFICULTY, PARTITION_LIMIT, PEER_LIMIT, UNICORN_LIMIT};
use crate::interfaces::{
    ComputeInterface, ComputeRequest, Contract, MineRequest, ProofOfWork, ProofOfWorkBlock,
    Response, Tx,
};
use crate::unicorn::UnicornShard;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
use sha3::{Digest, Sha3_256};
use std::{collections::HashMap, error::Error, fmt, net::SocketAddr};
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
    pub partition_list: Vec<SocketAddr>,
    pub request_list: Vec<SocketAddr>,
    pub unicorn_list: HashMap<SocketAddr, UnicornShard>,
}

impl ComputeNode {
    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
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

    /// Start the compute node on the network.
    pub async fn start(&mut self) -> Result<()> {
        self.node.listen().await?;
        Ok(())
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
            let req = deserialize::<ComputeRequest>(&frame).map_err(|error| {
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
    fn handle_request(&mut self, peer: SocketAddr, req: ComputeRequest) -> Response {
        use ComputeRequest::*;

        match req {
            SendPoW { pow } => self.receive_pow(peer, pow),
            SendPartitionPoW { pow_components } => self.receive_partition_pow(peer, pow_components),
            SendPartitionRequest => self.receive_partition_request(peer),
        }
    }

    /// Receive a partition request from a miner node
    /// TODO: This may need to be part of the ComputeInterface depending on key agreement
    fn receive_partition_request(&mut self, peer: SocketAddr) -> Response {
        self.request_list.push(peer);

        Response {
            success: true,
            reason: "Partition request received successfully",
        }
    }

    /// Receives the light POW for partition inclusion
    fn receive_partition_pow(&mut self, peer: SocketAddr, pow_components: ProofOfWork) -> Response {
        let mut pow_mut = pow_components.clone();

        if self.partition_list.len() < PARTITION_LIMIT && Self::validate_pow(&mut pow_mut) {
            self.partition_list.push(peer);

            if self.partition_list.len() == PARTITION_LIMIT {
                return Response {
                    success: false,
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
        for entry in self.request_list.clone() {
            let _result = self.send_random_number(entry).await.unwrap();
        }

        Ok(())
    }

    /// Floods the full partition list to participants
    pub async fn flood_block_to_partition(&mut self) -> Result<()> {
        self.generate_garbage_block(BLOCK_SIZE);

        for entry in self.partition_list.clone() {
            let _result = self.send_block(entry).await.unwrap();
        }

        Ok(())
    }
}

impl ComputeInterface for ComputeNode {
    fn new(address: SocketAddr) -> ComputeNode {
        ComputeNode {
            node: Node::new(address, PEER_LIMIT),
            unicorn_list: HashMap::new(),
            unicorn_limit: UNICORN_LIMIT,
            current_block: Vec::new(),
            current_random_num: Vec::new(),
            request_list: Vec::new(),
            partition_list: Vec::new(),
        }
    }

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

        if self.unicorn_list.len() < self.unicorn_limit {
            let mut unicorn_value = UnicornShard::new();
            unicorn_value.promise = pow.clone();

            self.unicorn_list.insert(address, unicorn_value);
            //self.flood_pow_to_peers(address, &pow);

            return Response {
                success: true,
                reason: "Received PoW successfully",
            };
        }

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
