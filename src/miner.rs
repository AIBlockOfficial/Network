use crate::comms_handler::{CommsError, Event};
use crate::interfaces::{
    Block, ComputeRequest, HandshakeRequest, MineRequest, MinerInterface, NodeType, ProofOfWork,
    Response,
};
use crate::key_creation::{KeyAgreement, PeerInfo};
use crate::rand::Rng;
use crate::sha3::Digest;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
use tracing::{debug, info, info_span, warn};

use rand;
use sha3::Sha3_256;
use std::{fmt, net::SocketAddr, sync::Arc};
use tokio::{sync::RwLock, task};

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, MinerError>;

#[derive(Debug)]
pub enum MinerError {
    Network(CommsError),
    Serialization(bincode::Error),
    AsyncTask(task::JoinError),
}

impl fmt::Display for MinerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MinerError::Network(err) => write!(f, "Network error: {}", err),
            MinerError::AsyncTask(err) => write!(f, "Async task error: {}", err),
            MinerError::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl From<bincode::Error> for MinerError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

impl From<CommsError> for MinerError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<task::JoinError> for MinerError {
    fn from(other: task::JoinError) -> Self {
        Self::AsyncTask(other)
    }
}

/// Limit for the number of peers a compute node may have
const PEER_LIMIT: usize = 6;

/// Set the mining difficulty by number of required zeroes
const MINING_DIFFICULTY: usize = 2;

/// An instance of a MinerNode
#[derive(Debug, Clone)]
pub struct MinerNode {
    node: Node,
    rand_num: Vec<u8>,
    current_block: Vec<u8>,
    partition_list: Vec<SocketAddr>,
    key_creator: KeyAgreement,
    last_pow: Arc<RwLock<ProofOfWork>>,
}

impl MinerNode {
    /// Returns the miner node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Start the compute node on the network.
    pub async fn start(&mut self) -> Result<()> {
        Ok(self.node.listen().await?)
    }

    /// Generates a garbage coinbase tx for network testing
    fn generate_garbage_coinbase(&self) -> Vec<u8> {
        vec![0; 285]
    }

    /// Connect to a peer on the network.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
        self.node
            .send(
                peer,
                HandshakeRequest {
                    node_type: NodeType::Miner,
                },
            )
            .await?;
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
            let req = deserialize::<MineRequest>(&frame).map_err(|error| {
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
    fn handle_request(&mut self, peer: SocketAddr, req: MineRequest) -> Response {
        use MineRequest::*;

        match req {
            SendBlock { block } => self.receive_pre_block(block),
            SendPartitionList { p_list } => self.receive_partition_list(p_list),
            SendRandomNum { rnum } => self.receive_random_number(rnum),

            // Key agreement stuff
            SendYi { y_i } => self.receive_y_i(peer.to_string(), y_i),
            SendKj { k_j } => self.receive_k_j(peer.to_string(), k_j),
            SendPeerInfo { peer_info } => self.receive_peer_info(peer.to_string(), peer_info),
        }
    }

    /// Handles the receipt of a peer's peer info
    fn receive_peer_info(&mut self, peer: String, peer_info: PeerInfo) -> Response {
        self.key_creator.receive_peer_info(peer, peer_info);

        Response {
            success: true,
            reason: "Received random number successfully",
        }
    }

    /// Handles the receipt of the random number of partitioning
    fn receive_random_number(&mut self, rand_num: Vec<u8>) -> Response {
        self.rand_num = rand_num;

        Response {
            success: true,
            reason: "Received random number successfully",
        }
    }

    /// Handles the receipt of a peer's k_j value for the key creation
    fn receive_k_j(&mut self, peer: String, k_j: Vec<u8>) -> Response {
        self.key_creator.receive_k_j(peer, k_j);

        Response {
            success: true,
            reason: "Received peer's y_i successfully",
        }
    }

    /// Handles the receipt of a peer's y_i value for the miner node
    fn receive_y_i(&mut self, peer: String, y_i: Vec<u8>) -> Response {
        let p_length = self.partition_list.len();
        let own_index = self
            .partition_list
            .iter()
            .position(|&x| x == self.node.address())
            .unwrap();
        let right_index = match own_index + 1 {
            p_length => 0,
            _ => own_index + 1,
        };
        let left_index = match own_index - 1 {
            num if num < 0 => p_length - 1,
            _ => own_index - 1,
        };

        if peer == self.partition_list[left_index].to_string() {
            self.key_creator.left_y_i = y_i;
        } else if peer == self.partition_list[right_index].to_string() {
            self.key_creator.right_y_i = y_i;
        }

        Response {
            success: true,
            reason: "Received peer's y_i successfully",
        }
    }

    /// Handles the receipt of the filled partition list
    fn receive_partition_list(&mut self, p_list: Vec<SocketAddr>) -> Response {
        self.partition_list = p_list.clone();

        if p_list.iter().any(|&x| x == self.address()) {
            // TODO: We won, so it's time to perform PoW on block
        }

        Response {
            success: true,
            reason: "Received partition list successfully",
        }
    }

    /// Sends PoW to a compute node.
    pub async fn send_pow(&mut self, peer: SocketAddr, pow_promise: Vec<u8>) -> Result<()> {
        self.node
            .send(peer, ComputeRequest::SendPoW { pow: pow_promise })
            .await?;
        Ok(())
    }

    /// Sends y_i to a peer miner.
    pub async fn send_y_i(&mut self, peer: SocketAddr, y_i: Vec<u8>) -> Result<()> {
        self.node
            .send(peer, MineRequest::SendYi { y_i: y_i })
            .await?;
        Ok(())
    }

    /// Sends k_j to a peer miner.
    pub async fn send_k_j(&mut self, peer: SocketAddr, k_j: Vec<u8>) -> Result<()> {
        self.node
            .send(peer, MineRequest::SendKj { k_j: k_j })
            .await?;
        Ok(())
    }

    /// Sends own peer info to a peer miner
    pub async fn send_peer_info(&mut self, peer: SocketAddr, peer_info: PeerInfo) -> Result<()> {
        self.node
            .send(
                peer,
                MineRequest::SendPeerInfo {
                    peer_info: peer_info,
                },
            )
            .await?;
        Ok(())
    }

    /// Validates a PoW
    ///
    /// ### Arguments
    ///
    /// * `pow` - PoW to validate
    pub fn validate_pow(pow: &mut ProofOfWork) -> bool {
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

    /// Generates a valid PoW
    ///
    /// ### Arguments
    ///
    /// * `address` - Payment address for a valid PoW
    pub async fn generate_pow(&mut self, address: &'static str) -> Result<ProofOfWork> {
        Ok(task::spawn_blocking(move || {
            let mut nonce = Self::generate_nonce();
            let mut pow = ProofOfWork { address, nonce };

            while !Self::validate_pow(&mut pow) {
                nonce = Self::generate_nonce();
                pow.nonce = nonce;
            }

            pow
        })
        .await?)
    }

    /// Generate a valid PoW and return the hashed value
    ///
    /// ### Arguments
    ///
    /// * `address` - Payment address for a valid PoW
    pub async fn generate_pow_promise(&mut self, address: &'static str) -> Result<Vec<u8>> {
        let pow = self.generate_pow(address).await?;

        *(self.last_pow.write().await) = pow.clone();
        let mut pow_body = pow.address.as_bytes().to_vec();
        pow_body.append(&mut pow.nonce.clone());

        Ok(Sha3_256::digest(&pow_body).to_vec())
    }

    /// Returns the last PoW.
    pub async fn last_pow(&self) -> ProofOfWork {
        self.last_pow.read().await.clone()
    }

    /// Generates a random sequence of values for a nonce
    fn generate_nonce() -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let nonce = (0..10).map(|_| rng.gen_range(1, 200)).collect();

        nonce
    }
}

impl MinerInterface for MinerNode {
    fn new(comms_address: SocketAddr) -> MinerNode {
        MinerNode {
            partition_list: Vec::new(),
            rand_num: Vec::new(),
            current_block: Vec::new(),
            node: Node::new(comms_address, PEER_LIMIT),
            key_creator: KeyAgreement::new(comms_address.to_string(), 12, 10, 5),
            last_pow: Arc::new(RwLock::new(ProofOfWork {
                address: "",
                nonce: Vec::new(),
            })),
        }
    }

    fn receive_pre_block(&mut self, pre_block: Vec<u8>) -> Response {
        self.current_block = pre_block;

        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }
}
