use crate::comms_handler::{CommsError, Event};
use crate::configurations::MinerNodeConfig;
use crate::constants::PEER_LIMIT;
use crate::hash_block::HashBlock;
use crate::interfaces::{
    ComputeRequest, MineRequest, MinerInterface, NodeType, ProofOfWork, Response,
};
use crate::utils::{
    concat_merkle_coinbase, format_parition_pow_address, get_partition_entry_key,
    validate_pow_block, validate_pow_for_address,
};
use crate::wallet::WalletDb;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::{OutPoint, Transaction};
use naom::primitives::transaction_utils::{construct_coinbase_tx, construct_tx_hash};
use rand::{self, Rng};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::secretbox::Key;
use std::time::SystemTime;
use std::{
    error::Error,
    fmt,
    net::SocketAddr,
    net::{IpAddr, Ipv4Addr},
};
use tokio::task;
use tracing::{debug, error, info, info_span, trace, warn};

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
#[derive(Debug)]
pub struct MinerNode {
    node: Node,
    pub compute_addr: SocketAddr,
    pub extra_connect_addr: Vec<SocketAddr>,
    pub partition_key: Option<Key>,
    pub rand_num: Vec<u8>,
    pub current_block: HashBlock,
    last_pow: Option<ProofOfWork>,
    pub partition_list: Vec<ProofOfWork>,
    wallet_db: WalletDb,
    current_coinbase: Option<(String, Transaction)>,
    current_payment_address: Option<String>,
}

impl MinerNode {
    /// Creates a new instance of Mining implementor
    ///
    /// ### Arguments
    ///
    /// * `config`   - MinerNodeConfig object that hold the miner_nodes and miner_db_mode
    pub async fn new(config: MinerNodeConfig) -> Result<MinerNode> {
        let addr = config
            .miner_nodes
            .get(config.miner_node_idx)
            .ok_or(MinerError::ConfigError("Invalid miner index"))?
            .address;
        let compute_addr = config
            .compute_nodes
            .get(config.miner_compute_node_idx)
            .ok_or(MinerError::ConfigError("Invalid compute index"))?
            .address;
        Ok(MinerNode {
            node: Node::new(addr, PEER_LIMIT, NodeType::Miner).await?,
            compute_addr,
            extra_connect_addr: Vec::new(),
            partition_list: Vec::new(),
            rand_num: Vec::new(),
            partition_key: None,
            current_block: HashBlock::new(),
            last_pow: None,
            wallet_db: WalletDb::new(config.miner_db_mode),
            current_coinbase: None,
            current_payment_address: None,
        })
    }

    /// Returns the node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Returns the node's compute endpoint.
    pub fn compute_address(&self) -> SocketAddr {
        self.compute_addr
    }
    /// Connect to a peer on the network.
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Address of the peer to connect to.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
        Ok(())
    }

    /// Connect info for peers on the network.
    pub fn connect_info_peers(&self) -> (Node, Vec<SocketAddr>, Vec<SocketAddr>) {
        let compute = Some(self.compute_addr);
        let to_connect = compute.iter().chain(&self.extra_connect_addr);
        let expect_connect = compute.iter().chain(&self.extra_connect_addr);
        (
            self.node.clone(),
            to_connect.copied().collect(),
            expect_connect.copied().collect(),
        )
    }

    /// Signal to the node listening loop to complete
    pub async fn stop_listening_loop(&mut self) -> Vec<task::JoinHandle<()>> {
        self.node.stop_listening().await
    }

    /// Listens for new events from peers and handles them, processing any errors.
    /// Return true when a block was mined.
    pub async fn handle_next_event_response(
        &mut self,
        now: SystemTime,
        response: Result<Response>,
    ) -> bool {
        debug!("Response: {:?}", response);

        match response {
            Ok(Response {
                success: true,
                reason: "Received random number successfully",
            }) => {
                info!("RANDOM NUMBER RECEIVED: {:?}", self.rand_num.clone());
                let pow = self.generate_partition_pow().await.unwrap();
                self.send_partition_pow(self.compute_address(), pow)
                    .await
                    .unwrap();
            }
            Ok(Response {
                success: true,
                reason: "Received partition list successfully",
            }) => {
                debug!("RECEIVED PARTITION LIST");
            }
            Ok(Response {
                success: true,
                reason: "Pre-block received successfully",
            }) => {
                info!("PRE-BLOCK RECEIVED");
                let (nonce, current_coinbase) =
                    self.generate_pow_for_current_block().await.unwrap();

                match now.elapsed() {
                    Ok(elapsed) => {
                        debug!("{}", elapsed.as_millis());
                    }
                    Err(e) => {
                        // an error occurred!
                        error!("Error: {:?}", e);
                    }
                }

                self.send_pow(self.compute_address(), nonce, current_coinbase)
                    .await
                    .unwrap();
                return true;
            }
            Ok(Response {
                success: true,
                reason: "Block found",
            }) => {
                info!("Block nonce has been successfully found");
                self.commit_block_found().await;
            }
            Ok(Response {
                success: false,
                reason: "Block not found",
            }) => {}
            Ok(Response {
                success: true,
                reason,
            }) => {
                error!("UNHANDLED RESPONSE TYPE: {:?}", reason);
            }
            Ok(Response {
                success: false,
                reason,
            }) => {
                error!("WARNING: UNHANDLED RESPONSE TYPE FAILURE: {:?}", reason);
            }
            Err(error) => {
                panic!("ERROR HANDLING RESPONSE: {:?}", error);
            }
        };

        false
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event(&mut self) -> Option<Result<Response>> {
        let event = self.node.next_event().await?;
        self.handle_event(event).await.into()
    }

    /// Handles an event using handle_new_frame
    ///
    /// ### Arguments
    ///
    /// * `event`   - Event object to be handled.
    async fn handle_event(&mut self, event: Event) -> Result<Response> {
        match event {
            Event::NewFrame { peer, frame } => Ok(self.handle_new_frame(peer, frame).await?),
        }
    }

    /// Hanldes a new incoming message from a peer.
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Socket address of the peer sending the message.
    /// * `frame`   - Bytes object holding the frame
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
    ///
    /// ### Arguments
    ///
    /// * `req`   - MineRequest object that is the recieved request
    /// TODO: Find something to do with win_coinbase. Allows to know winner
    fn handle_request(&mut self, _peer: SocketAddr, req: MineRequest) -> Response {
        use MineRequest::*;
        trace!("RECEIVED REQUEST: {:?}", req);

        match req {
            NotifyBlockFound { win_coinbase } => self.receive_block_found(win_coinbase),
            SendBlock { block } => self.receive_pre_block(block),
            SendPartitionList { p_list } => self.receive_partition_list(p_list),
            SendRandomNum { rnum } => self.receive_random_number(rnum),
        }
    }

    /// Handles the receipt of a block found
    ///
    /// ### Arguments
    ///
    /// * `win_coinbase`   - String compared to the current block map/hash to check if it matches
    fn receive_block_found(&mut self, win_coinbase: String) -> Response {
        debug!("RECEIVE BLOCK FOUND: {:?}", win_coinbase);
        if Some(&win_coinbase) == self.current_coinbase.as_ref().map(|(hash, _)| hash) {
            Response {
                success: true,
                reason: "Block found",
            }
        } else {
            Response {
                success: false,
                reason: "Block not found",
            }
        }
    }

    /// Handles the receipt of the random number of partitioning
    ///
    /// ### Arguments
    ///
    /// * `rand_num`   - random num to be recieved in Vec<u8>
    fn receive_random_number(&mut self, rand_num: Vec<u8>) -> Response {
        self.rand_num = rand_num;
        debug!("RANDOM NUMBER IN SELF: {:?}", self.rand_num.clone());

        Response {
            success: true,
            reason: "Received random number successfully",
        }
    }

    /// Handles the receipt of the filled partition list
    ///
    /// ### Arguments
    ///
    /// * `p_list`   - Vec<ProofOfWork>. Is the partition list being recieved. It is a Vec containing proof of work objects.
    fn receive_partition_list(&mut self, p_list: Vec<ProofOfWork>) -> Response {
        self.partition_key = Some(get_partition_entry_key(&p_list));
        self.partition_list = p_list;

        Response {
            success: true,
            reason: "Received partition list successfully",
        }
    }

    /// Util function to get a socket address for PID table checks
    fn get_comparison_addr(&self) -> SocketAddr {
        let comparison_port = self.address().port() + 1;
        let mut comparison_addr = self.address();

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Sends PoW to a compute node.
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Socket address of recipient
    /// * `nonce`   - sequence number of a block in Vec<u8>
    /// * `coinbase`   - Transaction object
    pub async fn send_pow(
        &mut self,
        peer: SocketAddr,
        nonce: Vec<u8>,
        coinbase: Transaction,
    ) -> Result<()> {
        self.node
            .send(peer, ComputeRequest::SendPoW { nonce, coinbase })
            .await?;
        Ok(())
    }

    /// Sends the light partition PoW to a compute node
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Socket address of recipient/peer
    /// * `partition_entry`   - partition ProofOfWork being sent
    pub async fn send_partition_pow(
        &mut self,
        peer: SocketAddr,
        partition_entry: ProofOfWork,
    ) -> Result<()> {
        self.node
            .send(peer, ComputeRequest::SendPartitionEntry { partition_entry })
            .await?;
        Ok(())
    }

    /// Sends a request to partition to a Compute node
    ///
    /// ### Arguments
    ///
    /// * `compute`   - Socket address of recipient
    pub async fn send_partition_request(&mut self, compute: SocketAddr) -> Result<()> {
        let _peer_span = info_span!("sending partition participation request");

        self.node
            .send(compute, ComputeRequest::SendPartitionRequest {})
            .await?;

        Ok(())
    }

    /// Handles the receipt of a block found
    pub async fn commit_block_found(&mut self) {
        let address = self.current_payment_address.take().unwrap();
        let (tx_hash, tx) = self.current_coinbase.take().unwrap();

        let tx_out_p = OutPoint::new(tx_hash, 0);
        let tx_amount = tx.outputs.first().unwrap().amount;

        self.wallet_db
            .save_payment_to_wallet(tx_out_p, tx_amount, address)
            .await
            .unwrap();
    }

    /// Generates a valid PoW for a block specifically
    /// TODO: Update the numbers used for reward and block time
    pub async fn generate_pow_for_current_block(&mut self) -> Result<(Vec<u8>, Transaction)> {
        let block = &self.current_block;

        if self.current_payment_address.is_none() {
            let (address, _) = self.wallet_db.generate_payment_address().await;
            self.current_payment_address = Some(address);
        }
        let mining_tx = construct_coinbase_tx(
            block.b_num,
            TokenAmount(12000),
            self.current_payment_address.clone().unwrap(),
        );
        let mining_tx_hash = construct_tx_hash(&mining_tx);
        let pow = self.generate_pow_for_block(mining_tx_hash.clone()).await?;

        self.current_coinbase = Some((mining_tx_hash, mining_tx.clone()));
        Ok((pow, mining_tx))
    }

    /// Generates and returns the nonce of a block.active_raft
    /// Validates the POW using the block, hash and nonce
    ///
    /// ### Arguments
    ///
    /// * `mining_tx_hash`   - block transation hash that is used to check the validity of the ProofOfWork
    async fn generate_pow_for_block(&mut self, mining_tx_hash: String) -> Result<Vec<u8>> {
        let block = self.current_block.clone();
        let hash_to_mine = concat_merkle_coinbase(&block.merkle_hash, &mining_tx_hash);

        Ok(task::spawn_blocking(move || {
            // Mine Block with mining transaction
            let mut nonce = Self::generate_nonce();
            while !validate_pow_block(&block.unicorn, &hash_to_mine, &nonce) {
                nonce = Self::generate_nonce();
            }

            nonce
        })
        .await?)
    }

    /// Generates a valid Partition PoW
    pub async fn generate_partition_pow(&mut self) -> Result<ProofOfWork> {
        let address_proof = format_parition_pow_address(self.address());
        Self::generate_pow_for_address(address_proof, Some(self.rand_num.clone())).await
    }

    /// Generates a ProofOfWork for a given address
    ///
    /// ### Arguments
    ///
    /// * `address`   - Given address to generate the ProofOfWork
    /// * `rand_num`   - A random number used to generate the ProofOfWork in an Option<Vec<u8>>
    async fn generate_pow_for_address(
        address: String,
        rand_num: Option<Vec<u8>>,
    ) -> Result<ProofOfWork> {
        Ok(task::spawn_blocking(move || {
            let mut pow = ProofOfWork {
                address,
                nonce: Self::generate_nonce(),
            };

            while !validate_pow_for_address(&pow, &rand_num.as_ref()) {
                pow.nonce = Self::generate_nonce();
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
        let pow = Self::generate_pow_for_address(address, None).await?;

        self.last_pow = Some(pow.clone());
        let mut pow_body = pow.address.as_bytes().to_vec();
        pow_body.extend(pow.nonce);

        Ok(Sha3_256::digest(&pow_body).to_vec())
    }

    /// Returns the last PoW.
    pub fn last_pow(&self) -> &Option<ProofOfWork> {
        &self.last_pow
    }

    /// Generates a random sequence of values for a nonce
    fn generate_nonce() -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..10).map(|_| rng.gen_range(1, 200)).collect()
    }

    // Get the wallet db
    pub fn get_wallet_db(&self) -> &WalletDb {
        &self.wallet_db
    }
}

impl MinerInterface for MinerNode {
    /// recieves a pre_block and sets it as current block
    ///
    /// ### Arguments
    ///
    /// * `pre_block`   - Vec<u8> representing the pre_block to become the current block
    fn receive_pre_block(&mut self, pre_block: Vec<u8>) -> Response {
        self.current_block = deserialize::<HashBlock>(&pre_block).unwrap();

        Response {
            success: true,
            reason: "Pre-block received successfully",
        }
    }
}
