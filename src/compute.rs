use crate::comms_handler::{CommsError, Event};
use crate::constants::{PARTITION_LIMIT, PEER_LIMIT, UNICORN_LIMIT};
use crate::interfaces::{
    ComputeInterface, ComputeRequest, Contract, MineRequest, ProofOfWork, Response, Tx,
};
use crate::unicorn::UnicornShard;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
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
    current_block: Vec<u8>,
    pub unicorn_limit: usize,
    current_random_num: Vec<u8>,
    pub partition_list: Vec<SocketAddr>,
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

        self.node
            .send(peer, MineRequest::SendRandomNum { rnum: random_num })
            .await?;
        Ok(())
    }

    /// Sends the partition list of winners to a mining node
    pub async fn send_partition_list(&mut self, peer: SocketAddr) -> Result<()> {
        let partition_list = self.partition_list.clone();

        self.node
            .send(
                peer,
                MineRequest::SendPartitionList {
                    p_list: partition_list,
                },
            )
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
            SendPartitionRequest => self.receive_partition_request(peer),
        }
    }

    /// Receive a partition request from a miner node
    /// TODO: This may need to be part of the ComputeInterface depending on key agreement
    fn receive_partition_request(&mut self, peer: SocketAddr) -> Response {
        if self.partition_list.len() < PARTITION_LIMIT {
            self.partition_list.push(peer);

            return Response {
                success: true,
                reason: "Partition request received successfully",
            };
        }

        Response {
            success: false,
            reason: "Partition list is full",
        }
    }

    /// Floods the full partition list to participants
    pub async fn flood_partition_list(&mut self) -> Result<()> {
        for entry in self.partition_list.clone() {
            let _result = self.send_partition_list(entry).await.unwrap();
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
            partition_list: Vec::new(),
        }
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

    fn receive_pow(&mut self, address: SocketAddr, pow: Vec<u8>) -> Response {
        info!(?address, "Received PoW");

        if self.unicorn_list.len() < self.unicorn_limit {
            let mut unicorn_value = UnicornShard::new();
            unicorn_value.promise = pow.clone();

            self.unicorn_list.insert(address, unicorn_value);
            self.flood_pow_to_peers(address, &pow);

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
