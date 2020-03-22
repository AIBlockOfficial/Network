use crate::comms_handler::CommsError;
use crate::interfaces::{
    Block, ComputeRequest, HandshakeRequest, MinerInterface, NodeType, ProofOfWork, Response,
};
use crate::rand::Rng;
use crate::sha3::Digest;
use crate::Node;
use rand;
use sha3::Sha3_256;
use std::fmt;
use std::net::SocketAddr;

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, MinerError>;

#[derive(Debug)]
pub enum MinerError {
    Network(CommsError),
}

impl fmt::Display for MinerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MinerError::Network(err) => write!(f, "Network error: {}", err),
        }
    }
}

impl From<CommsError> for MinerError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

/// Limit for the number of peers a compute node may have
const PEER_LIMIT: usize = 6;

/// Set the mining difficulty by number of required zeroes
const MINING_DIFFICULTY: usize = 2;

/// An instance of a MinerNode
#[derive(Debug)]
pub struct MinerNode {
    node: Node,
    pub last_pow: ProofOfWork,
}

impl MinerNode {
    /// Start the compute node on the network.
    pub async fn start(&mut self) -> Result<()> {
        Ok(self.node.listen().await?)
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

    /// Sends PoW to a compute node.
    pub async fn send_pow(&mut self, peer: SocketAddr, pow_promise: Vec<u8>) -> Result<()> {
        self.node
            .send(peer, ComputeRequest::SendPoW { pow: pow_promise })
            .await?;
        Ok(())
    }

    /// Validates a PoW
    ///
    /// ### Arguments
    ///
    /// * `pow` - PoW to validate
    pub fn validate_pow(&self, pow: &mut ProofOfWork) -> bool {
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
    pub fn generate_pow(&mut self, address: &'static str) -> ProofOfWork {
        let mut nonce = self.generate_nonce();
        let mut pow = ProofOfWork { address, nonce };

        while !self.validate_pow(&mut pow) {
            nonce = self.generate_nonce();
            pow.nonce = nonce;
        }

        pow
    }

    /// Generate a valid PoW and return the hashed value
    ///
    /// ### Arguments
    ///
    /// * `address` - Payment address for a valid PoW
    pub fn generate_pow_promise(&mut self, address: &'static str) -> Vec<u8> {
        let pow = self.generate_pow(address);

        self.last_pow = pow.clone();
        let mut pow_body = pow.address.as_bytes().to_vec();
        pow_body.append(&mut pow.nonce.clone());

        Sha3_256::digest(&pow_body).to_vec()
    }

    /// Generates a random sequence of values for a nonce
    fn generate_nonce(&self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let nonce = (0..10).map(|_| rng.gen_range(1, 200)).collect();

        nonce
    }
}

impl MinerInterface for MinerNode {
    fn new(comms_address: SocketAddr) -> MinerNode {
        MinerNode {
            node: Node::new(comms_address, PEER_LIMIT),
            last_pow: ProofOfWork {
                address: "",
                nonce: Vec::new(),
            },
        }
    }

    fn receive_pre_block(&self, _pre_block: &Block) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }
}
