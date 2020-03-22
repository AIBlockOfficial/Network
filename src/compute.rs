use crate::comms_handler::{CommsError, Event};
use crate::interfaces::ProofOfWork;
use crate::interfaces::{ComputeInterface, ComputeRequest, Contract, Response, Tx};
use crate::unicorn::UnicornShard;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::{debug, info, warn};

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, ComputeError>;

pub enum ComputeError {
    Network(CommsError),
}

impl From<CommsError> for ComputeError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

/// Limit for the number of peers a compute node may have
const PEER_LIMIT: usize = 6;

/// Limit for the number of PoWs a compute node may have for UnicornShard creation
const UNICORN_LIMIT: usize = 5;

#[derive(Debug)]
pub struct ComputeNode {
    node: Node,
    pub unicorn_list: HashMap<SocketAddr, UnicornShard>,
    pub unicorn_limit: usize,
}

impl ComputeNode {
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
        self.handle_events().await;
        Ok(())
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime.
    pub async fn handle_events(&mut self) {
        while let Some(event) = self.node.next_event().await {
            match event {
                Event::NewFrame { peer, frame } => self.handle_new_frame(peer, frame).await,
            }
        }
    }

    /// Hanldes a new incoming message from a peer.
    async fn handle_new_frame(&mut self, peer: SocketAddr, frame: Bytes) {
        match deserialize::<ComputeRequest>(&frame) {
            Ok(req) => {
                // TODO: enter request span here
                let response = self.handle_request(peer, req);
                debug!(?response, ?peer, "Sending response");
            }
            Err(error) => {
                warn!(?error, ?peer, "Failed to decode a frame");
            }
        }
    }

    /// Handles a compute request.
    fn handle_request(&mut self, peer: SocketAddr, req: ComputeRequest) -> Response {
        use ComputeRequest::*;
        match req {
            SendPoW { pow } => self.receive_pow(peer, pow),
        }
    }
}

impl ComputeInterface for ComputeNode {
    fn new(address: SocketAddr) -> ComputeNode {
        ComputeNode {
            node: Node::new(address, PEER_LIMIT),
            unicorn_list: HashMap::new(),
            unicorn_limit: UNICORN_LIMIT,
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
        info!(?address, "Receieved PoW");

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
