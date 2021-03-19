use crate::comms_handler::{CommsError, Event};
use crate::configurations::MinerNodeConfig;
use crate::constants::PEER_LIMIT;
use crate::hash_block::HashBlock;
use crate::interfaces::{
    ComputeRequest, MineRequest, MinerInterface, NodeType, ProofOfWork, Response,
};
use crate::utils::{
    concat_merkle_coinbase, format_parition_pow_address, get_paiments_for_wallet,
    get_partition_entry_key, validate_pow_block, validate_pow_for_address, RunningTaskOrResult,
};
use crate::wallet::WalletDb;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::Transaction;
use naom::primitives::transaction_utils::{construct_coinbase_tx, construct_tx_hash};
use rand::{self, Rng};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::secretbox::Key;
use std::{
    error::Error,
    fmt,
    future::Future,
    net::SocketAddr,
    net::{IpAddr, Ipv4Addr},
    time::SystemTime,
};
use tokio::task;
use tracing::{debug, error, error_span, info, trace, warn};
use tracing_futures::Instrument;

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, MinerError>;

/// Block Pow task input/output
#[derive(Debug, Clone)]
pub struct BlockPoWInfo {
    peer: SocketAddr,
    start_time: SystemTime,
    unicorn: String,
    hash_to_mine: String,
    coinbase: (String, Transaction),
    b_num: u64,
    nonce: Vec<u8>,
}

/// Received block
#[derive(Debug)]
pub struct BlockPoWReceived {
    hash_block: HashBlock,
    reward: TokenAmount,
}

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
    pub partition_key: Option<Key>,
    pub rand_num: Vec<u8>,
    current_block: Option<BlockPoWReceived>,
    last_pow: Option<ProofOfWork>,
    pub partition_list: Vec<ProofOfWork>,
    wallet_db: WalletDb,
    current_coinbase: Option<(String, Transaction)>,
    current_payment_address: Option<String>,
    mining_partition_task: RunningTaskOrResult<(ProofOfWork, SocketAddr)>,
    mining_block_task: RunningTaskOrResult<BlockPoWInfo>,
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
            partition_list: Default::default(),
            rand_num: Default::default(),
            partition_key: None,
            current_block: None,
            last_pow: None,
            wallet_db: WalletDb::new(config.miner_db_mode),
            current_coinbase: None,
            current_payment_address: None,
            mining_partition_task: Default::default(),
            mining_block_task: Default::default(),
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
        let to_connect = compute.iter();
        let expect_connect = compute.iter();
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
    pub async fn handle_next_event_response(&mut self, response: Result<Response>) -> bool {
        debug!("Response: {:?}", response);

        match response {
            Ok(Response {
                success: true,
                reason: "Received random number successfully",
            }) => {
                info!("RANDOM NUMBER RECEIVED: {:?}", self.rand_num);
            }
            Ok(Response {
                success: true,
                reason: "Partition PoW complete",
            }) => {
                if self.process_found_partition_pow().await {
                    info!("Partition Pow found and sent");
                }
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
            }
            Ok(Response {
                success: true,
                reason: "Block PoW complete",
            }) => {
                if self.process_found_block_pow().await {
                    info!("Block PoW found and sent");
                    return true;
                }
            }
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
    pub async fn handle_next_event<E: Future<Output = &'static str> + Unpin>(
        &mut self,
        exit: &mut E,
    ) -> Option<Result<Response>> {
        loop {
            // State machines are not keept between iterations or calls.
            // All selection calls (between = and =>), need to be dropable
            // i.e they should only await a channel.
            tokio::select! {
                event = self.node.next_event() => {
                    trace!("handle_next_event evt {:?}", event);
                    if let res @ Some(_) = self.handle_event(event?).await.transpose() {
                        return res;
                    }
                }
                _ = self.mining_partition_task.wait() => {
                    return Some(Ok(Response {
                        success: true,
                        reason: "Partition PoW complete",
                    }));
                }
                _ = self.mining_block_task.wait() => {
                    return Some(Ok(Response {
                        success: true,
                        reason: "Block PoW complete",
                    }));
                }
                reason = &mut *exit => {
                    return Some(Ok(Response {
                        success: true,
                        reason,
                    }));
                }
            }
        }
    }

    /// Handles an event using handle_new_frame
    ///
    /// ### Arguments
    ///
    /// * `event`   - Event object to be handled.
    async fn handle_event(&mut self, event: Event) -> Result<Option<Response>> {
        match event {
            Event::NewFrame { peer, frame } => {
                let peer_span = error_span!("peer", ?peer);
                self.handle_new_frame(peer, frame)
                    .instrument(peer_span)
                    .await
            }
        }
    }

    /// Hanldes a new incoming message from a peer.
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Socket address of the peer sending the message.
    /// * `frame`   - Bytes object holding the frame
    async fn handle_new_frame(
        &mut self,
        peer: SocketAddr,
        frame: Bytes,
    ) -> Result<Option<Response>> {
        let req = deserialize::<MineRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        debug!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a compute request.
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `req`   - MineRequest object that is the recieved request
    /// TODO: Find something to do with win_coinbase. Allows to know winner
    async fn handle_request(&mut self, peer: SocketAddr, req: MineRequest) -> Option<Response> {
        use MineRequest::*;
        trace!("handle_request: {:?}", req);

        match req {
            SendBlock { block, reward } => self.receive_pre_block(peer, block, reward).await,
            SendPartitionList { p_list } => self.receive_partition_list(peer, p_list),
            SendRandomNum {
                rnum,
                win_coinbases,
            } => self.receive_random_number(peer, rnum, win_coinbases).await,
        }
    }

    /// Handles the receipt of the random number of partitioning
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `rand_num` - random num to be recieved in Vec<u8>
    async fn receive_random_number(
        &mut self,
        peer: SocketAddr,
        rand_num: Vec<u8>,
        win_coinbases: Vec<String>,
    ) -> Option<Response> {
        if peer != self.compute_address() {
            return None;
        }

        if self.rand_num == rand_num {
            self.process_found_partition_pow().await;
            return None;
        }

        if self.is_current_coinbase_found(&win_coinbases) {
            self.commit_found_coinbase().await;
        }
        self.start_generate_partition_pow(peer, rand_num).await;
        Some(Response {
            success: true,
            reason: "Received random number successfully",
        })
    }

    /// Handles the receipt of the filled partition list
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `p_list`   - Vec<ProofOfWork>. Is the partition list being recieved. It is a Vec containing proof of work objects.
    fn receive_partition_list(
        &mut self,
        peer: SocketAddr,
        p_list: Vec<ProofOfWork>,
    ) -> Option<Response> {
        if peer != self.compute_address() {
            return None;
        }

        let new_key = Some(get_partition_entry_key(&p_list));
        if self.partition_key == new_key {
            return None;
        }

        self.partition_key = new_key;
        self.partition_list = p_list;
        Some(Response {
            success: true,
            reason: "Received partition list successfully",
        })
    }

    /// Receives a new block to be mined
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `pre_block` - New block to be mined
    /// * `reward`    - The block reward to be paid on successful PoW
    async fn receive_pre_block(
        &mut self,
        peer: SocketAddr,
        pre_block: Vec<u8>,
        reward: TokenAmount,
    ) -> Option<Response> {
        if peer != self.compute_address() {
            return None;
        }

        let new_block = BlockPoWReceived {
            hash_block: deserialize::<HashBlock>(&pre_block).unwrap(),
            reward,
        };

        let new_b_num = Some(new_block.hash_block.b_num);
        let current_b_num = self.current_block.as_ref().map(|c| c.hash_block.b_num);
        if new_b_num <= current_b_num {
            if new_b_num == current_b_num {
                self.process_found_block_pow().await;
            }
            return None;
        }

        self.start_generate_pow_for_current_block(peer, new_block)
            .await;
        Some(Response {
            success: true,
            reason: "Pre-block received successfully",
        })
    }

    /// Util function to get a socket address for PID table checks
    fn get_comparison_addr(&self) -> SocketAddr {
        let comparison_port = self.address().port() + 1;
        let mut comparison_addr = self.address();

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Process the found PoW sending it to the related peer and logging errors
    pub async fn process_found_block_pow(&mut self) -> bool {
        let BlockPoWInfo {
            peer,
            start_time,
            b_num,
            nonce,
            coinbase,
            ..
        } = match self.mining_block_task.completed_result() {
            Some(Ok(v)) => v.clone(),
            Some(Err(e)) => {
                error!("process_found_block_pow PoW {:?}", e);
                return false;
            }
            None => {
                trace!("process_found_block_pow PoW Not ready yet");
                return false;
            }
        };

        if let Ok(elapsed) = start_time.elapsed() {
            debug!("Found block in {}ms", elapsed.as_millis());
        }

        self.current_coinbase = Some(coinbase.clone());
        let (_, coinbase) = coinbase;

        if let Err(e) = self.send_pow(peer, b_num, nonce, coinbase).await {
            error!("process_found_block_pow PoW {:?}", e);
            return false;
        }

        true
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
        block_num: u64,
        nonce: Vec<u8>,
        coinbase: Transaction,
    ) -> Result<()> {
        self.node
            .send(
                peer,
                ComputeRequest::SendPoW {
                    block_num,
                    nonce,
                    coinbase,
                },
            )
            .await?;
        Ok(())
    }

    /// Process the found Pow sending it to the related peer and logging errors
    pub async fn process_found_partition_pow(&mut self) -> bool {
        let (partition_entry, peer) = match self.mining_partition_task.completed_result() {
            Some(Ok((e, p))) => (e.clone(), *p),
            Some(Err(e)) => {
                error!("process_found_partition_pow PoW {:?}", e);
                return false;
            }
            None => {
                trace!("process_found_partition_pow PoW Not ready yet");
                return false;
            }
        };

        if let Err(e) = self.send_partition_pow(peer, partition_entry).await {
            error!("process_found_partition_pow PoW {:?}", e);
            return false;
        }

        true
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
        let peer_span = error_span!("sending partition participation request");
        self.node
            .send(compute, ComputeRequest::SendPartitionRequest {})
            .instrument(peer_span)
            .await?;

        Ok(())
    }

    /// Handles the receipt of a block found
    fn is_current_coinbase_found(&self, win_coinbases: &[String]) -> bool {
        if let Some((tx_hash, _)) = &self.current_coinbase {
            win_coinbases.contains(tx_hash)
        } else {
            false
        }
    }

    /// Handles the receipt of a block found
    async fn commit_found_coinbase(&mut self) {
        self.current_payment_address = None;
        let (hash, transaction) = self.current_coinbase.take().unwrap();

        let payments = get_paiments_for_wallet(Some((&hash, &transaction)).into_iter());

        self.wallet_db
            .save_usable_payments_to_wallet(payments)
            .await
            .unwrap();
    }

    /// Generates a valid PoW for a block specifically
    /// TODO: Update the numbers used for reward and block time
    /// * `peer`      - Peer to send PoW to
    /// * `new_block` - Block for PoW
    pub async fn start_generate_pow_for_current_block(
        &mut self,
        peer: SocketAddr,
        new_block: BlockPoWReceived,
    ) {
        let b_num = new_block.hash_block.b_num;

        if self.current_payment_address.is_none() {
            let (address, _) = self.wallet_db.generate_payment_address().await;
            self.current_payment_address = Some(address);
        }
        let mining_tx = construct_coinbase_tx(
            b_num,
            new_block.reward,
            self.current_payment_address.clone().unwrap(),
        );
        let mining_tx_hash = construct_tx_hash(&mining_tx);

        self.mining_block_task = {
            let merkle_hash = &new_block.hash_block.merkle_hash;
            let hash_to_mine = concat_merkle_coinbase(merkle_hash, &mining_tx_hash).await;
            let unicorn = new_block.hash_block.unicorn.clone();
            let coinbase = (mining_tx_hash, mining_tx);
            let nonce = Vec::new();
            let start_time = SystemTime::now();
            RunningTaskOrResult::Running(Self::generate_pow_for_block(BlockPoWInfo {
                peer,
                start_time,
                unicorn,
                hash_to_mine,
                coinbase,
                b_num,
                nonce,
            }))
        };
        self.current_block = Some(new_block);
    }

    /// Generates and returns the nonce of a block.active_raft
    /// Validates the POW using the block, hash and nonce
    ///
    /// ### Arguments
    ///
    /// * `info`      - Block Proof of work info
    fn generate_pow_for_block(mut info: BlockPoWInfo) -> task::JoinHandle<BlockPoWInfo> {
        task::spawn_blocking(move || {
            // Mine Block with mining transaction
            info.nonce = Self::generate_nonce();
            while !validate_pow_block(&info.unicorn, &info.hash_to_mine, &info.nonce) {
                info.nonce = Self::generate_nonce();
            }

            info
        })
    }

    /// Generates a valid Partition PoW
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Peer to send PoW to
    /// * `rand_num` - random num for PoW
    pub async fn start_generate_partition_pow(&mut self, peer: SocketAddr, rand_num: Vec<u8>) {
        let address_proof = format_parition_pow_address(self.address());

        self.mining_partition_task = RunningTaskOrResult::Running(Self::generate_pow_for_address(
            peer,
            address_proof,
            Some(rand_num.clone()),
        ));
        self.rand_num = rand_num;
    }

    /// Generates a ProofOfWork for a given address
    ///
    /// ### Arguments
    ///
    /// * `peer`      - Peer to send PoW to
    /// * `address`   - Given address to generate the ProofOfWork
    /// * `rand_num`  - A random number used to generate the ProofOfWork in an Option<Vec<u8>>
    fn generate_pow_for_address(
        peer: SocketAddr,
        address: String,
        rand_num: Option<Vec<u8>>,
    ) -> task::JoinHandle<(ProofOfWork, SocketAddr)> {
        task::spawn_blocking(move || {
            let mut pow = ProofOfWork {
                address,
                nonce: Self::generate_nonce(),
            };

            while !validate_pow_for_address(&pow, &rand_num.as_ref()) {
                pow.nonce = Self::generate_nonce();
            }

            (pow, peer)
        })
    }

    /// Generate a valid PoW and return the hashed value
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer to send PoW to
    /// * `address` - Payment address for a valid PoW
    pub async fn generate_pow_promise(
        &mut self,
        peer: SocketAddr,
        address: String,
    ) -> Result<Vec<u8>> {
        let (pow, _) = Self::generate_pow_for_address(peer, address, None).await?;

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
        (0..16).map(|_| rng.gen_range(0, 200)).collect()
    }

    // Get the wallet db
    pub fn get_wallet_db(&self) -> &WalletDb {
        &self.wallet_db
    }
}

impl MinerInterface for MinerNode {}
