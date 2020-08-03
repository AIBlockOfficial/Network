use crate::comms_handler::CommsError;
use crate::interfaces::{
    Asset, ComputeRequest, HandshakeRequest, MinerInterface, NodeType, ProofOfWork, Response,
};
use crate::primitives::block::Block;
use crate::primitives::transaction::{Transaction, TxIn, TxOut};
use crate::rand::Rng;
use crate::script::lang::Script;
use crate::sha3::Digest;
use crate::wallet::{create_address, save_to_wallet, WalletStore};
use crate::Node;

use bincode::{deserialize, serialize};
use bytes::Bytes;
use rand;
use sha3::Sha3_256;
use sodiumoxide::crypto::sign;
use std::io::Error;
use std::{fmt, net::SocketAddr, sync::Arc};
use tokio::{sync::RwLock, task};

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, MinerError>;

#[derive(Debug)]
pub enum MinerError {
    Network(CommsError),
    AsyncTask(task::JoinError),
    Io(Error),
}

impl fmt::Display for MinerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MinerError::Network(err) => write!(f, "Network error: {}", err),
            MinerError::AsyncTask(err) => write!(f, "Async task error: {}", err),
            MinerError::Io(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl From<Error> for MinerError {
    fn from(other: Error) -> Self {
        Self::Io(other)
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

    /// Creates a coinbase tx with a saved pub keypair
    ///
    /// ### Arguments
    ///
    /// * `block_time`      - Current block time
    /// * `block_reward`    - Block reward to be included in coinbase
    pub async fn create_coinbase(
        &mut self,
        block_time: u32,
        block_reward: u64,
    ) -> Result<Transaction> {
        let (pk, sk) = sign::gen_keypair();

        // TxIn
        let mut coinbase_txin = TxIn::new();
        coinbase_txin.previous_out = None;
        coinbase_txin.script_signature = Script::new_for_coinbase(block_time);

        // TxOut
        let mut coinbase_txout = TxOut::new();
        coinbase_txout.script_public_key = Some(pk.0.to_vec());
        coinbase_txout.value = Some(Asset::Token(block_reward));

        // Final coinbase
        let mut coinbase = Transaction::new();
        coinbase.inputs = vec![coinbase_txin];
        coinbase.outputs = vec![coinbase_txout];

        // Create wallet content
        let wallet_content = WalletStore {
            secret_key: sk,
            transactions: vec![coinbase.clone()],
            net: 0,
        };

        // Create address and save to wallet
        let address = create_address(pk, 0);
        let _save_result = save_to_wallet(address, wallet_content).await?;

        Ok(coinbase)
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
    pub async fn generate_pow(&mut self, address: String) -> Result<ProofOfWork> {
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
    pub async fn generate_pow_promise(&mut self, address: String) -> Result<Vec<u8>> {
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

    /// Handles a pre-block for mining and eventual send to compute
    ///
    /// ### Arguments
    ///
    /// * `pre_block`       - Pre-block to be mined and pushed on
    /// * `block_reward`    - Block reward allocated to coinbase
    async fn handle_block_for_mine(
        &mut self,
        pre_block: &Block,
        block_reward: u64,
    ) -> Result<Block> {
        let mut mined_block = pre_block.clone();
        let coinbase = self
            .create_coinbase(pre_block.header.time, block_reward)
            .await?;

        // Handle extra metadata
        mined_block.transactions.push(coinbase);
        mined_block.set_bits();
        let _mhash = mined_block.get_merkle_root();

        // Mine the block hash
        let hash_input = Bytes::from(serialize(&mined_block).unwrap());
        let hash_vec = Sha3_256::digest(&hash_input).to_vec();
        let hash_pow = String::from_utf8_lossy(&hash_vec).to_string();

        let final_pow = self.generate_pow(hash_pow).await?;
        mined_block.header.nonce = final_pow.nonce;

        Ok(mined_block)
    }
}

impl MinerInterface for MinerNode {
    fn new(comms_address: SocketAddr) -> MinerNode {
        MinerNode {
            node: Node::new(comms_address, PEER_LIMIT),
            last_pow: Arc::new(RwLock::new(ProofOfWork {
                address: "".to_string(),
                nonce: Vec::new(),
            })),
        }
    }

    fn receive_pre_block(&mut self, pre_block: &Block, block_reward: u64) -> Response {
        self.handle_block_for_mine(pre_block, block_reward);

        Response {
            success: true,
            reason: "Pre-block received successfully",
        }
    }
}
