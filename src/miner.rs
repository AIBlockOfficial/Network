use crate::comms_handler::{CommsError, Event};
use crate::configurations::MinerNodeConfig;
use crate::constants::{MINING_DIFFICULTY, PEER_LIMIT};
use crate::interfaces::{
    ComputeRequest, MineRequest, MinerInterface, NodeType, ProofOfWork, ProofOfWorkBlock, Response,
};
use crate::utils::get_partition_entry_key;
use crate::wallet::{construct_address, save_transactions_to_wallet, TransactionStore};
use crate::Node;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use rand::{self, Rng};
use sha3::{Digest, Sha3_256};
use std::{
    collections::BTreeMap,
    convert::TryInto,
    error::Error,
    fmt,
    net::SocketAddr,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};
use tokio::{sync::RwLock, task};
use tracing::{debug, info_span, warn};

use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use naom::primitives::transaction_utils::{construct_coinbase_tx, construct_tx_hash};

use sodiumoxide::crypto::secretbox::{gen_key, Key};
use sodiumoxide::crypto::sign;

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, MinerError>;

#[derive(Debug)]
pub enum MinerError {
    ConfigError(&'static str),
    Network(CommsError),
    Serialization(bincode::Error),
    AsyncTask(task::JoinError),
}

impl fmt::Display for MinerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::AsyncTask(err) => write!(f, "Async task error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for MinerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::AsyncTask(e) => Some(e),
            Self::Serialization(ref e) => Some(e),
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

/// An instance of a MinerNode
#[derive(Debug, Clone)]
pub struct MinerNode {
    node: Node,
    pub partition_key: Key,
    pub rand_num: Vec<u8>,
    pub current_block: Block,
    pub current_coinbase: Transaction,
    last_pow: Arc<RwLock<ProofOfWork>>,
    pub partition_list: Vec<ProofOfWork>,
}

impl MinerNode {
    /// Creates a new instance of Mining implementor
    ///
    /// ### Arguments
    ///
    /// * `comms_address`   - endpoint address used for communications
    pub async fn new(config: MinerNodeConfig) -> Result<MinerNode> {
        let addr = config
            .miner_nodes
            .get(config.miner_node_idx)
            .ok_or(MinerError::ConfigError("Invalid miner index"))?
            .address
            .clone();
        Ok(MinerNode {
            node: Node::new(addr, PEER_LIMIT, NodeType::Miner).await?,
            partition_list: Vec::new(),
            rand_num: Vec::new(),
            partition_key: gen_key(),
            current_block: Block::new(),
            current_coinbase: Transaction::new(),
            last_pow: Arc::new(RwLock::new(ProofOfWork {
                address: "".to_string(),
                nonce: Vec::new(),
            })),
        })
    }

    /// Returns the miner node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Generates a garbage coinbase tx for network testing
    fn generate_garbage_coinbase() -> Transaction {
        Transaction::new()
    }

    /// Connect to a peer on the network.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
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
    /// TODO: Find something to do with win_coinbase. Allows to know winner
    fn handle_request(&mut self, _peer: SocketAddr, req: MineRequest) -> Response {
        use MineRequest::*;
        println!("RECEIVED REQUEST: {:?}", req);

        match req {
            NotifyBlockFound { win_coinbase: _ } => Response {
                success: true,
                reason: "Block found",
            },
            SendBlock { block } => self.receive_pre_block(block),
            SendPartitionList { p_list } => self.receive_partition_list(p_list),
            SendRandomNum { rnum } => self.receive_random_number(rnum),
        }
    }

    /// Handles the receipt of the random number of partitioning
    fn receive_random_number(&mut self, rand_num: Vec<u8>) -> Response {
        self.rand_num = rand_num;
        println!("RANDOM NUMBER IN SELF: {:?}", self.rand_num.clone());

        Response {
            success: true,
            reason: "Received random number successfully",
        }
    }

    /// Handles the receipt of the filled partition list
    fn receive_partition_list(&mut self, p_list: Vec<ProofOfWork>) -> Response {
        let key = get_partition_entry_key(p_list.clone());
        let hashed_key = Sha3_256::digest(&key).to_vec();
        let key_slice: [u8; 32] = hashed_key[..].try_into().unwrap();
        self.partition_key = Key(key_slice);

        self.partition_list = p_list.clone();

        Response {
            success: true,
            reason: "Received partition list successfully",
        }
    }

    /// Util function to get a socket address for PID table checks
    fn get_comparison_addr(&self) -> SocketAddr {
        let comparison_port = self.address().port() + 1;
        let mut comparison_addr = self.address().clone();

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Sends PoW to a compute node.
    pub async fn send_pow(
        &mut self,
        peer: SocketAddr,
        pow_promise: ProofOfWorkBlock,
        coinbase: Transaction,
    ) -> Result<()> {
        self.node
            .send(
                peer,
                ComputeRequest::SendPoW {
                    pow: pow_promise,
                    coinbase,
                },
            )
            .await?;
        Ok(())
    }

    /// Sends the light partition PoW to a compute node
    pub async fn send_partition_pow(
        &mut self,
        peer: SocketAddr,
        partition_entry: ProofOfWork,
    ) -> Result<()> {
        self.node
            .send(
                peer,
                ComputeRequest::SendPartitionEntry {
                    partition_entry: partition_entry,
                },
            )
            .await?;
        Ok(())
    }

    /// Sends a request to partition to a Compute node
    pub async fn send_partition_request(&mut self, compute: SocketAddr) -> Result<()> {
        let _peer_span = info_span!("sending partition participation request");

        self.node
            .send(compute, ComputeRequest::SendPartitionRequest {})
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

    /// I'm lazy, so just making another verifier for now
    pub fn validate_pow_block(pow: &mut ProofOfWorkBlock) -> bool {
        let mut pow_body = Bytes::from(serialize(&pow.block).unwrap()).to_vec();
        pow_body.append(&mut pow.nonce.clone());

        let pow_hash = Sha3_256::digest(&pow_body).to_vec();

        for entry in pow_hash[0..MINING_DIFFICULTY].to_vec() {
            if entry != 0 {
                return false;
            }
        }

        true
    }

    /// Generates a valid PoW for a block specifically
    /// TODO: Update the numbers used for reward and block time
    /// TODO: Save pk/sk to temp storage
    ///
    /// ### Arguments
    ///
    /// * `block` - The block to get the PoW for
    pub async fn generate_pow_for_block(
        &mut self,
        block: Block,
    ) -> Result<(ProofOfWorkBlock, Transaction)> {
        Ok(task::spawn_blocking(move || {
            let mut nonce = Self::generate_nonce();
            let (pk, _sk) = sign::gen_keypair();
            let address = construct_address(pk, 0);

            let current_coinbase = construct_coinbase_tx(12, block.header.time, address);
            let coinbase_hash = construct_tx_hash(&current_coinbase);

            let mut block_for_pow = block.clone();
            block_for_pow.transactions.push(coinbase_hash.clone());

            // Create address and save to wallet
            let address = construct_address(pk, 0);

            // Create wallet content
            let transaction_store = TransactionStore {
                address: address,
                net: 0,
            };
            let mut tx_to_save = BTreeMap::new();
            tx_to_save.insert(coinbase_hash, transaction_store);

            let _save_result = save_transactions_to_wallet(tx_to_save);

            // Construct PoW block for mining
            let mut pow = ProofOfWorkBlock {
                nonce,
                block: block_for_pow,
            };

            while !Self::validate_pow_block(&mut pow) {
                nonce = Self::generate_nonce();
                pow.nonce = nonce;
            }

            (pow, current_coinbase)
        })
        .await?)
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
}

impl MinerInterface for MinerNode {
    fn receive_pre_block(&mut self, pre_block: Vec<u8>) -> Response {
        self.current_block = deserialize::<Block>(&pre_block).unwrap();

        Response {
            success: true,
            reason: "Pre-block received successfully",
        }
    }
}
