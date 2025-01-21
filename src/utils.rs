use crate::asert::{CompactTarget, CompactTargetError, HeaderHash};
use crate::comms_handler::Node;
use crate::configurations::{UnicornFixedInfo, UtxoSetSpec, WalletTxSpec};
use crate::constants::{
    ADDRESS_POW_NONCE_LEN, BLOCK_PREPEND, COINBASE_MATURITY, D_DISPLAY_PLACES_U64,
    MINING_DIFFICULTY, NETWORK_VERSION, POW_RNUM_SELECT, REWARD_ISSUANCE_VAL, REWARD_SMOOTHING_VAL,
};
use crate::interfaces::{
    BlockchainItem, BlockchainItemMeta, DruidDroplet, PowInfo, ProofOfWork, StoredSerializingBlock,
};
use crate::miner_pow::{MineError, PoWError, PoWObject};
use crate::wallet::WalletDb;
use crate::Rs2JsMsg;
use bincode::serialize;
use bincode::{self, Error as BincodeError};
use chrono::Utc;
use futures::future::join_all;
use rand::{self, Rng};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use std::{fmt, vec};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tokio::time::Instant;
use tracing::{info, trace, warn};
use tracing_subscriber::field::debug;
use trust_dns_resolver::TokioAsyncResolver;
use tw_chain::constants::TOTAL_TOKENS;
use tw_chain::crypto::sha3_256;
use tw_chain::crypto::sign_ed25519::{self as sign, PublicKey, SecretKey, Signature};
use tw_chain::primitives::transaction::GenesisTxHashSpec;
use tw_chain::primitives::{
    asset::{Asset, TokenAmount},
    block::{build_hex_txs_hash, Block, BlockHeader},
    transaction::{OutPoint, Transaction, TxConstructor, TxIn, TxOut},
};
use tw_chain::script::{lang::Script, StackEntry};
use tw_chain::utils::transaction_utils::{
    construct_address, construct_payment_tx_ins, construct_tx_core, construct_tx_hash,
    construct_tx_in_signable_asset_hash, construct_tx_in_signable_hash, get_fees_with_out_point,
    get_tx_out_with_out_point, get_tx_out_with_out_point_cloned,
};
use url::Url;

pub type RoutesPoWInfo = Arc<Mutex<BTreeMap<String, usize>>>;
pub type ApiKeys = Arc<Mutex<BTreeMap<String, Vec<String>>>>;
pub type LocalEventSender = MpscTracingSender<LocalEvent>;
pub type LocalEventReceiver = mpsc::Receiver<LocalEvent>;

/// Local command event to nodes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalEvent {
    CoordinatedShutdown(u64),
    ReconnectionComplete,
    Exit(&'static str),
    Ignore,
}

/// Event response processing
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseResult {
    Exit,
    Continue,
}

pub struct MpscTracingSender<T> {
    sender: mpsc::Sender<T>,
}

impl<T> MpscTracingSender<T> {
    pub fn new(sender: mpsc::Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T> Clone for MpscTracingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> From<mpsc::Sender<T>> for MpscTracingSender<T> {
    fn from(sender: mpsc::Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T> MpscTracingSender<T> {
    pub async fn send(&mut self, value: T, tag: &str) -> Result<(), mpsc::error::SendError<T>> {
        use mpsc::error::SendError;
        use mpsc::error::TrySendError;

        match self.sender.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(value)) => {
                trace!("send_tracing({}) full: waiting", tag);
                let start = Instant::now();
                let result = self.sender.send(value).await;
                let elapsed = Instant::now() - start;
                if elapsed < Duration::from_millis(2) {
                    trace!("send_tracing({}) done: waited({:?})", tag, elapsed);
                } else {
                    warn!("send_tracing({}) done: waited({:?})", tag, elapsed);
                }
                result
            }
            Err(TrySendError::Closed(value)) => Err(SendError(value)),
        }
    }
}

/// A running tasks or end result
#[derive(Debug)]
pub enum RunningTaskOrResult<T> {
    None,
    Running(task::JoinHandle<T>),
    Completed(Result<T, task::JoinError>),
}

impl<T> Default for RunningTaskOrResult<T> {
    fn default() -> Self {
        Self::None
    }
}

impl<T> RunningTaskOrResult<T> {
    /// Wait for the handle to complete or wait forever
    pub async fn wait(&mut self) {
        *self = if let RunningTaskOrResult::Running(task) = self {
            Self::Completed(task.await)
        } else {
            std::future::pending().await
        }
    }

    /// Return completed result or None if no completed task
    pub fn completed_result(&self) -> Option<&Result<T, task::JoinError>> {
        if let Self::Completed(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Return true if running task
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Running(_))
    }
}

/// Channel for low volume local events
pub struct LocalEventChannel {
    pub tx: LocalEventSender,
    pub rx: LocalEventReceiver,
}

impl Default for LocalEventChannel {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(10);
        Self { tx: tx.into(), rx }
    }
}

impl fmt::Debug for LocalEventChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

/// A trivial error to output
#[derive(PartialEq, Eq)]
pub struct StringError(pub String);

impl StringError {
    pub fn from<T: Error>(value: T) -> Self {
        Self(value.to_string())
    }
}

impl Error for StringError {}

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for StringError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StringError {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Deserialization of a BlockchainItem
pub enum DeserializedBlockchainItem {
    // Data, block_num, tx_len
    CurrentBlock(StoredSerializingBlock, u64, u32),
    // Data, block_num, tx_num
    CurrentTx(Transaction, u64, u32),
    // Data
    VersionErr(u32),
    // Data
    SerializationErr(bincode::Error),
}

impl DeserializedBlockchainItem {
    pub fn from_item(item: &BlockchainItem) -> Self {
        if item.version != NETWORK_VERSION {
            return Self::VersionErr(item.version);
        }
        match item.item_meta {
            BlockchainItemMeta::Block { block_num, tx_len } => {
                match bincode::deserialize::<StoredSerializingBlock>(&item.data) {
                    Ok(b) => Self::CurrentBlock(b, block_num, tx_len),
                    Err(e) => Self::SerializationErr(e),
                }
            }
            BlockchainItemMeta::Tx { block_num, tx_num } => {
                match bincode::deserialize::<Transaction>(&item.data) {
                    Ok(b) => Self::CurrentTx(b, block_num, tx_num),
                    Err(e) => Self::SerializationErr(e),
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct BackupCheck {
    modulo_block_num: Option<u64>,
}

impl BackupCheck {
    pub fn new(modulo_block_num: Option<u64>) -> Self {
        Self { modulo_block_num }
    }

    pub fn need_backup(&self, current_block: u64) -> bool {
        if let Some(modulo) = self.modulo_block_num {
            current_block != 0 && current_block % modulo == 0
        } else {
            false
        }
    }
}

#[derive(Debug, Default)]
pub struct UtxoReAlignCheck {
    modulo_block_num: Option<u64>,
}

impl UtxoReAlignCheck {
    pub fn new(modulo_block_num: Option<u64>) -> Self {
        Self { modulo_block_num }
    }

    pub fn need_check(&self, current_block: u64) -> bool {
        if let Some(modulo) = self.modulo_block_num {
            current_block != 0 && current_block % modulo == 0
        } else {
            false
        }
    }
}

/// Install a global tracing subscriber that listens for events and
/// filters based on the value of the [`RUST_LOG` environment variable],
/// if one is not already set.
///
/// Default to OFF so if not environment varialbe is provided no log is emitted.
pub fn tracing_log_try_init() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let dirs = std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "off".to_owned());

    let builder = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::new(dirs));

    builder.try_init()
}

/// Attempts to connect to all peers
///
/// ### Arguments
///
/// * `node`     - Node attempting to connect to peers.
/// * `peers`    - Vec of socket addresses of peers
/// * `close_rx` - Receiver for close event or None to finish when all connected
pub async fn loop_connnect_to_peers_async(
    mut node: Node,
    peers: Vec<SocketAddr>,
    mut close_rx: Option<oneshot::Receiver<()>>,
    mut local_events_tx: LocalEventSender,
) {
    let mut is_initial_conn = true;

    loop {
        for peer in node.unconnected_peers(&peers).await {
            trace!(?peer, "Try to connect to");
            if let Err(e) = node.connect_to(peer).await {
                trace!(?peer, ?e, "Try to connect to failed");
            } else {
                trace!(?peer, "Try to connect to succeeded");
                if !is_initial_conn {
                    trace!("Sending PartitionRequest to Mempool node: {peer:?} after reconnection");
                    local_events_tx
                        .send(LocalEvent::ReconnectionComplete, "Reconnect Complete")
                        .await
                        .unwrap();
                }
            }
        }

        if node.unconnected_peers(&peers).await.is_empty() {
            // We finished our initial connections, now set the flag to false
            // to indicate that connections after this are actual reconnections
            // and therefore requires Miners to send PartitionRequest
            is_initial_conn = false;
        }

        let delay_retry = tokio::time::sleep(Duration::from_millis(500));
        if let Some(close_rx) = &mut close_rx {
            tokio::select! {
                _ = delay_retry => (),
                _ = close_rx => return,
            };
        } else {
            if node.unconnected_peers(&peers).await.is_empty() {
                return;
            }
            delay_retry.await;
        }
    }
}

/// check connected to all peers
///
/// ### Arguments
///
/// * `node`     - Node attempting to connect to peers.
/// * `peers`    - Vec of socket addresses of peers
pub async fn loop_wait_connnect_to_peers_async(node: Node, peers: Vec<SocketAddr>) {
    while !node.unconnected_peers(&peers).await.is_empty() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Gets the locally set list of sanctioned addresses
///
/// ### Arguments
///
/// * `path`         - Path to the sanction list
/// * `jurisdiction` - Jurisdiction to fetch sanctioned addresses for
pub fn get_sanction_addresses(path: String, jurisdiction: &str) -> Vec<String> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    let mut buff = String::new();
    file.read_to_string(&mut buff).unwrap();

    let sancs: serde_json::value::Value = serde_json::from_str(&buff).unwrap();

    match sancs[jurisdiction].as_array() {
        Some(v) => (*v
            .iter()
            .map(|i| i.as_str().unwrap().to_string())
            .collect::<Vec<String>>())
        .to_vec(),
        None => Vec::new(),
    }
}

/// Creates a "fake" transaction to save to the local wallet
/// for testing. The transaction will contain 4 tokens
///
/// NOTE: This is a test util function
/// ### Arguments
///
/// * `wallet_db`    - &WalletDb object. Reference to a wallet database
pub async fn create_and_save_fake_to_wallet(
    wallet_db: &mut WalletDb,
) -> Result<(), Box<dyn std::error::Error>> {
    let (final_address, address_keys) = wallet_db.generate_payment_address();
    let (receiver_addr, _) = wallet_db.generate_payment_address();

    let (t_hash, _payment_tx) = create_valid_transaction(
        "00000",
        0,
        &receiver_addr,
        &address_keys.public_key,
        &address_keys.secret_key,
    );
    let tx_out_p = OutPoint::new(t_hash, 0);
    let payment_to_save = Asset::token_u64(4000);
    let payments = vec![(tx_out_p.clone(), payment_to_save, final_address, 0)];
    wallet_db
        .save_usable_payments_to_wallet(payments, 0, false)
        .await
        .unwrap();

    Ok(())
}

/// Address to be used in Proof of Work
///
/// ### Arguments
///
/// * `addr`    - Socket address of used in the proof of work
pub fn format_parition_pow_address(addr: SocketAddr) -> String {
    format!("{addr}")
}

/// Calculates the reward for the next block, to be placed within the coinbase tx
///
/// ### Arguments
///
/// * `current_issuance` - Current issuance of all tokens
pub fn calculate_reward(current_issuance: TokenAmount) -> TokenAmount {
    let smoothing_value_as_token_amount = D_DISPLAY_PLACES_U64 * REWARD_SMOOTHING_VAL as u64;

    if current_issuance.0 >= TOTAL_TOKENS {
        return TokenAmount(smoothing_value_as_token_amount);
    }

    TokenAmount(
        ((TOTAL_TOKENS - current_issuance.0) >> REWARD_ISSUANCE_VAL)
            + smoothing_value_as_token_amount,
    )
}

/// Gets the total amount of tokens for all present coinbase transactions,
/// assuming that they have all received the same amount of reward
///
/// ### Arguments
///
/// * `coinbase_tx` - Coinbase transactions
pub fn get_total_coinbase_tokens(coinbase_tx: &BTreeMap<String, Transaction>) -> TokenAmount {
    let mut total = TokenAmount(0);
    for tx_ind in coinbase_tx.values() {
        total += tx_ind.outputs.iter().map(|x| x.value.token_amount()).sum();
    }
    total
}

/// Concatenates a merkle hash and a coinbase hash to produce a single hash output
///
/// ### Arguments
///
/// * `header     ` - Header to update
/// * `merkle_hash` - Nonce to use
/// * `cb_tx_hash`  - Mining transaction hash
pub fn apply_mining_tx(mut header: BlockHeader, nonce: Vec<u8>, tx_hash: String) -> BlockHeader {
    header.nonce_and_mining_tx_hash = (nonce, tx_hash);
    header
}

/// Generates a random num for use for proof of work
pub fn generate_pow_random_num() -> Vec<u8> {
    generate_random_num(POW_RNUM_SELECT)
}

/// Increments the provided nonce
///
/// ### Arguments
///
/// * `nonce` - Nonce to increment
fn get_nonce_increment(nonce: &[u8]) -> Vec<u8> {
    // Ensure the input vector has exactly 4 bytes
    if nonce.len() != 4 {
        return vec![];
    }

    // Convert the u8 slice to a fixed-size array
    let mut array: [u8; 4] = [0; 4];
    array.copy_from_slice(&nonce);

    // Interpret the fixed-size array as i32
    let current_value = i32::from_le_bytes(array); // Assuming little-endian byte order

    // Increment the value
    let incremented_value = current_value + 1;

    // Convert incremented value back to u8 vector
    let incremented_bytes = incremented_value.to_le_bytes().to_vec(); // Convert i32 to little-endian u8 vector

    incremented_bytes
}

/// Generates a garbage random num for use in network testing
pub fn generate_random_num(len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen_range(1, 200)).collect()
}

/// Parses a URL string and performs DNS resolution for cases where the passed URL is a domain name
///
/// ### Arguments
///
/// * `url_str`    - URL string to parse
pub async fn create_socket_addr(url_str: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let thread_url = url_str.to_owned();
    let handle = tokio::task::spawn_blocking(move || {
        if let Ok(url) = Url::parse(&thread_url.clone()) {
            let host_str = match url.host_str() {
                Some(v) => v,
                None => return None,
            };
            let port = url.port().unwrap_or(80);

            // Check if the host is an IP address
            if let Ok(ip) = host_str.parse::<IpAddr>() {
                // Handle as direct IP address
                Some(SocketAddr::new(ip, port))
            } else {
                let io_loop = Runtime::new().unwrap();

                // Handle as domain name
                let resolver = TokioAsyncResolver::tokio_from_system_conf().unwrap();

                let lookup_future = resolver.lookup_ip(host_str);
                let response = io_loop.block_on(lookup_future).unwrap();
                let ip = match response.iter().next() {
                    Some(ip) => ip,
                    None => return None,
                };
                Some(SocketAddr::new(ip, port))
            }
        } else {
            // Handle as direct IP address with optional port
            let parts: Vec<&str> = thread_url.split(':').collect();
            let ip = match parts[0].parse::<IpAddr>() {
                Ok(ip) => ip,
                Err(_e) => return None,
            };
            let port = if parts.len() > 1 {
                match parts[1].parse::<u16>() {
                    Ok(port) => port,
                    Err(_e) => return None,
                }
            } else {
                80
            };
            Some(SocketAddr::new(ip, port))
        }
    });

    match handle.await {
        Ok(v) => match v {
            Some(v) => Ok(v),
            None => Err("Failed to parse URL".into()),
        },
        Err(_e) => Err("Failed to parse URL".into()),
    }
}

pub async fn create_socket_addr_for_list(
    urls: &[String],
) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
    let mut result = Vec::new();
    for url in urls {
        let socket_addr = create_socket_addr(url).await?;
        result.push(socket_addr);
    }
    Ok(result)
}

/// Generates a ProofOfWork for a given address
///
/// ### Arguments
///
/// * `peer`      - Peer to send PoW to
/// * `address`   - Given address to generate the ProofOfWork
/// * `rand_num`  - A random number used to generate the ProofOfWork in an Option<Vec<u8>>
pub fn generate_pow_for_address(
    peer: SocketAddr,
    pow_info: PowInfo,
    address: String,
    rand_num: Option<Vec<u8>>,
) -> task::JoinHandle<(ProofOfWork, PowInfo, SocketAddr)> {
    task::spawn_blocking(move || {
        let mut pow = ProofOfWork {
            address,
            // TODO: instead of hard-coding a separate constant for this, rewrite this to use
            //       the existing miner infrastructure
            nonce: vec![0; ADDRESS_POW_NONCE_LEN],
        };

        while !validate_pow_for_address(&pow, &rand_num.as_ref()) {
            pow.nonce = get_nonce_increment(&pow.nonce);
        }

        (pow, pow_info, peer)
    })
}

/// Validate Proof of Work an address with a random number
pub fn validate_pow_for_address(pow: &ProofOfWork, rand_num: &Option<&Vec<u8>>) -> bool {
    let mut pow_body = pow.address.as_bytes().to_vec();
    pow_body.extend(rand_num.iter().flat_map(|r| r.iter()).copied());
    pow_body.extend(&pow.nonce);

    validate_pow_leading_zeroes(&pow_body).is_ok()
}

/// Will attempt deserialization of a given byte array using bincode
///
/// ### Arguments
///
/// * `data`    - Byte array to attempt deserialization on
pub fn try_deserialize<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, BincodeError> {
    bincode::deserialize(data)
}

/// Generate Proof of Work for a block with a mining transaction
///
/// ### Arguments
///
/// * `header`   - The header for PoW
pub fn generate_pow_for_block(header: &BlockHeader) -> Result<Option<Vec<u8>>, MineError> {
    use crate::miner_pow::*;

    let mut statistics = Default::default();
    let terminate_flag = None;
    let timeout_duration = None;

    let miner = create_any_miner(Some(
        &header.pow_difficulty().map_err(MineError::GetDifficulty)?,
    ));
    let result = generate_pow(
        &mut *miner.lock().unwrap(),
        header,
        &mut statistics,
        terminate_flag,
        timeout_duration,
    )?;

    match result {
        MineResult::FoundNonce { nonce } => {
            // Verify that the found nonce is actually valid
            let mut header_copy = header.clone();
            header_copy.nonce_and_mining_tx_hash.0 = nonce.clone();
            assert_eq!(
                validate_pow_block(&header_copy),
                Ok(()),
                "generated PoW nonce {} isn't actually valid! block header: {:#?}",
                hex::encode(&nonce),
                header
            );
            Ok(Some(header_copy.nonce_and_mining_tx_hash.0))
        }
        MineResult::Exhausted => Ok(None),
        MineResult::TerminateRequested => Ok(None),
        MineResult::TimeoutReached => Ok(None),
    }
}

/// Verify block is valid & consistent: Can be fully verified from PoW hash.
/// Verify that PoW hash is valid: sufficient leading 0.
/// Return the hex encoded hash with prefix
///
/// ### Arguments
///
/// * `block`   - The block to extract hash from
pub fn construct_valid_block_pow_hash(block: &Block) -> Result<String, StringError> {
    if build_hex_txs_hash(&block.transactions) != block.header.txs_merkle_root_and_hash.1 {
        trace!(
            "Transactions inconsistent with header: {:?}",
            &block.transactions
        );
        return Err(StringError(
            "Transactions inconsistent with header".to_owned(),
        ));
    }

    let hash_digest = validate_pow_block_hash(&block.header).map_err(StringError::from)?;

    let mut hash_digest = hex::encode(hash_digest);
    hash_digest.insert(0, BLOCK_PREPEND as char);
    Ok(hash_digest)
}

/// An error which can occur while validating a block header's proof-of-work.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ValidatePoWBlockError {
    /// Indicates that the block header's nonce is invalid
    InvalidNonce { cause: PoWError },
    /// Indicates that the block header's difficulty is invalid
    InvalidDifficulty { cause: CompactTargetError },
    /// Indicates that the block header has no difficulty threshold, and its hash has an
    /// insufficient number of leading zero bytes.
    InvalidLeadingBytes {
        cause: ValidatePoWLeadingZeroesError,
    },
    /// Indicates that the block header has a threshold, and its hash has an
    /// insufficient number of leading zero bytes.
    DoesntMeetThreshold {
        header_hash: HeaderHash,
        target: CompactTarget,
    },
}

impl std::error::Error for ValidatePoWBlockError {}

impl fmt::Display for ValidatePoWBlockError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidNonce { cause } => write!(f, "Block header nonce is invalid: {}", cause),
            Self::InvalidDifficulty { cause } => {
                write!(f, "Block header difficulty is invalid: {}", cause)
            }
            Self::InvalidLeadingBytes { cause } => write!(
                f,
                "Block header does not meet the difficulty requirements: {}",
                cause
            ),
            Self::DoesntMeetThreshold {
                header_hash: hash,
                target,
            } => write!(
                f,
                "Block header does not meet the difficulty requirements: target={}, hash={:?}",
                target,
                hash.as_bytes()
            ),
        }
    }
}

/// Validate Proof of Work for a block with a mining transaction
///
/// ### Arguments
///
/// * `header`   - The header for PoW
pub fn validate_pow_block(header: &BlockHeader) -> Result<(), ValidatePoWBlockError> {
    validate_pow_block_hash(header).map(|_| ())
}

/// Validate Proof of Work for a block with a mining transaction returning the PoW hash
///
/// ### Arguments
///
/// * `header`   - The header for PoW
fn validate_pow_block_hash(header: &BlockHeader) -> Result<HeaderHash, ValidatePoWBlockError> {
    // [AM] even though we've got explicit activation height in configuration
    // and a hard-coded fallback elsewhere in the code, here
    // we're basically sniffing at the difficulty field in the
    // block header to figure out what the target actually is.
    // this is fine when the choices are:
    // (1) zero-length difficulty vec, or
    // (2) a vec that can be converted to a CompactTarget
    // however, if there's another change we will need to update
    // this switching logic to be a bit smarter. context-free
    // pure functions like this one either need to take additional
    // arguments or be moved to something more stateful that can
    // access configuration.

    // Ensure the nonce length is valid
    BlockHeader::check_nonce_length(header.get_nonce().len())
        .map_err(|cause| ValidatePoWBlockError::InvalidNonce { cause })?;

    if header.difficulty.is_empty() {
        let pow = serialize(header).unwrap();
        validate_pow_leading_zeroes(&pow)
            .map(HeaderHash::from)
            .map_err(|cause| ValidatePoWBlockError::InvalidLeadingBytes { cause })
    } else {
        info!("We have difficulty");

        let target = CompactTarget::try_from_slice(&header.difficulty)
            .map_err(|cause| ValidatePoWBlockError::InvalidDifficulty { cause })?;
        let header_hash = HeaderHash::calculate(header);

        if header_hash.is_below_compact_target(&target) {
            Ok(header_hash)
        } else {
            Err(ValidatePoWBlockError::DoesntMeetThreshold {
                header_hash,
                target,
            })
        }
    }
}

/// An error which indicates that a Proof-of-Work action contained an invalid number of leading
/// zero bytes.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct ValidatePoWLeadingZeroesError {
    /// The computed hash
    pub hash: sha3::digest::Output<sha3::Sha3_256>,
    /// The mining difficulty
    pub mining_difficulty: usize,
}

impl Error for ValidatePoWLeadingZeroesError {}

impl fmt::Display for ValidatePoWLeadingZeroesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Hash {:?} has fewer than {} leading zero bytes",
            self.hash.as_slice(),
            self.mining_difficulty
        )
    }
}

/// Check the hash of given data reach MINING_DIFFICULTY
///
/// ### Arguments
///
/// * `mining_difficulty`    - usize mining difficulty
/// * `pow`                  - &u8 proof of work
fn validate_pow_leading_zeroes_for_diff(
    mining_difficulty: usize,
    pow: &[u8],
) -> Result<sha3::digest::Output<sha3::Sha3_256>, ValidatePoWLeadingZeroesError> {
    let hash = sha3_256::digest(pow);
    if hash.as_slice()[0..mining_difficulty]
        .iter()
        .all(|v| *v == 0)
    {
        Ok(hash)
    } else {
        Err(ValidatePoWLeadingZeroesError {
            hash,
            mining_difficulty,
        })
    }
}

/// Check the hash of given data reach MINING_DIFFICULTY
///
/// ### Arguments
///
/// * `pow`    - &u8 proof of work
fn validate_pow_leading_zeroes(
    pow: &[u8],
) -> Result<sha3::digest::Output<sha3::Sha3_256>, ValidatePoWLeadingZeroesError> {
    validate_pow_leading_zeroes_for_diff(MINING_DIFFICULTY, pow)
}

/// Get the payment info from the given transactions
///
/// ### Arguments
///
/// * `txs`   - The transactions
pub fn get_payments_for_wallet<'a>(
    txs: impl Iterator<Item = (&'a String, &'a Transaction)> + 'a,
) -> Vec<(OutPoint, Asset, String, u64)> {
    let utxo_iterator = get_tx_out_with_out_point_cloned(txs);
    get_payments_for_wallet_from_utxo(utxo_iterator)
}

/// Get the payment info from the given UTXO set/subset
///
/// ### Arguments
///
/// * `utxo_set`   - The UTXO set/subset
pub fn get_payments_for_wallet_from_utxo(
    utxos: impl Iterator<Item = (OutPoint, TxOut)>,
) -> Vec<(OutPoint, Asset, String, u64)> {
    utxos
        .map(|(out_p, tx_out)| {
            (
                out_p,
                tx_out.value,
                tx_out.script_public_key.unwrap(),
                tx_out.locktime,
            )
        })
        .collect()
}

/// Create a valid transaction from given info
pub fn create_valid_transaction(
    t_hash_hex: &str,
    prev_n: i32,
    receiver_addr_hex: &str,
    pub_key: &PublicKey,
    secret_key: &SecretKey,
) -> (String, Transaction) {
    create_valid_transaction_with_ins_outs(
        &[(prev_n, t_hash_hex)],
        &[receiver_addr_hex],
        pub_key,
        secret_key,
        TokenAmount(1),
        None,
    )
}

/// Create a valid transaction from given info
pub fn create_valid_transaction_with_ins_outs(
    tx_in: &[(i32, &str)],
    receiver_addr_hexs: &[&str],
    pub_key: &PublicKey,
    secret_key: &SecretKey,
    amount: TokenAmount,
    address_version: Option<u64>,
) -> (String, Transaction) {
    let tx_ins = {
        let mut tx_in_cons = Vec::new();
        for (prev_n, t_hash_hex) in tx_in {
            let signable = OutPoint::new(t_hash_hex.to_string(), *prev_n);
            let signable_h = construct_tx_in_signable_hash(&signable);

            let signature = sign::sign_detached(signable_h.as_bytes(), secret_key);
            tx_in_cons.push(TxConstructor {
                previous_out: signable,
                signatures: vec![signature],
                pub_keys: vec![*pub_key],
                address_version,
            });
        }

        construct_payment_tx_ins(tx_in_cons)
    };

    let tx_outs = {
        let mut tx_outs = Vec::new();

        for addr in receiver_addr_hexs {
            tx_outs.push(TxOut {
                value: Asset::Token(amount),
                locktime: 0,
                script_public_key: Some(addr.to_string()),
            });
        }
        tx_outs
    };

    let payment_tx = construct_tx_core(tx_ins, tx_outs, None);
    let t_hash = construct_tx_hash(&payment_tx);

    (t_hash, payment_tx)
}

/// Get the string to display for genesis TxIn
///
/// ### Arguments
///
/// * `tx`    - The transaction
pub fn get_genesis_tx_in_display(tx: &Transaction) -> &str {
    if let Some(tx_in) = tx.inputs.first() {
        if let Some(StackEntry::Bytes(v)) = tx_in.script_signature.stack.first() {
            return v;
        }
    }

    ""
}

/// Generate a half_druid value
pub fn generate_half_druid() -> String {
    let (pk, _) = sign::gen_keypair();
    construct_address(&pk)
}

/// Generate utxo_set transactions from seed info
///
/// ### Arguments
///
/// * `seed`      - Set iterated through to generate the transaction set utxo
/// * `tx_in_str` - String to use as genesis transactions TxIn bytes.
pub fn make_utxo_set_from_seed(
    seed: &UtxoSetSpec,
    tx_in_str: &Option<String>,
) -> BTreeMap<String, Transaction> {
    let mut pk_to_address: BTreeMap<String, String> = BTreeMap::new();
    let genesis_tx_in = tx_in_str.clone().map(|tx_in| {
        let mut script_signature = Script::new();
        script_signature.stack.push(StackEntry::Bytes(tx_in));
        TxIn {
            previous_out: None,
            script_signature,
        }
    });
    seed.iter()
        .map(|(tx_hash, tx_out)| {
            let tx = Transaction {
                outputs: tx_out
                    .iter()
                    .map(|out| {
                        let script_public_key =
                            if let Some(addr) = pk_to_address.get(&out.public_key) {
                                addr.clone()
                            } else {
                                let addr = decode_pub_key_as_address(&out.public_key);
                                pk_to_address.insert(out.public_key.clone(), addr.clone());
                                addr
                            };

                        TxOut::new_token_amount(script_public_key, out.amount, Some(out.locktime))
                    })
                    .collect(),
                inputs: genesis_tx_in.clone().into_iter().collect(),
                ..Transaction::default()
            };
            (tx_hash.clone(), tx)
        })
        .collect()
}

/// Generate wallet transactions from seed info
///
/// ### Arguments
///
/// * `seed`    - &WalletTxSpec object containing parameters to generate wallet transactions
pub fn make_wallet_tx_info(
    seed: &WalletTxSpec,
) -> (OutPoint, PublicKey, SecretKey, TokenAmount, Option<u64>) {
    let tx_out_p = decode_wallet_out_point(&seed.out_point);
    let amount = TokenAmount(seed.amount);
    let sk = decode_secret_key(&seed.secret_key).unwrap();
    let pk = decode_pub_key(&seed.public_key).unwrap();
    let version = seed.address_version;

    (tx_out_p, pk, sk, amount, version)
}

/// Decodes a wallet's OutPoint
///
/// ### Arguments
///
/// * `out_point`    - String to be split and decode the wallet OutPoint
pub fn decode_wallet_out_point(out_point: &str) -> OutPoint {
    let mut it = out_point.split('-');
    let n = it.next().unwrap().parse().unwrap();
    let tx_hash = it.next().unwrap().parse().unwrap();
    OutPoint::new(tx_hash, n)
}

/// Decodes the public key as address
///
/// ### Arguments
///
/// * `key`    - key to be decoded to give the public key
pub fn decode_pub_key_as_address(key: &str) -> String {
    construct_address(&decode_pub_key(key).unwrap())
}

/// Decodes the public key
///
/// ### Arguments
///
/// * `key`    - key to be decode
pub fn decode_pub_key(key: &str) -> Result<PublicKey, StringError> {
    if let Ok(key_slice) = hex::decode(key) {
        if let Some(key) = PublicKey::from_slice(&key_slice) {
            return Ok(key);
        }
    }
    Err(StringError(format!("Public key decoding error: {key}")))
}

/// Decodes a secret key
///
/// ### Arguments
///
/// * `key`    - key to decode
pub fn decode_secret_key(key: &str) -> Result<SecretKey, StringError> {
    if let Ok(key_slice) = hex::decode(key) {
        if let Some(key) = SecretKey::from_slice(&key_slice) {
            return Ok(key);
        }
    }
    Err(StringError(format!("Secret key decoding error: {key}")))
}

/// Decodes a signature
///
/// ### Arguments
///
/// * `sig`    - Signature to decode
pub fn decode_signature(sig: &str) -> Result<Signature, StringError> {
    if let Ok(sig_slice) = hex::decode(sig) {
        if let Some(sig) = Signature::from_slice(&sig_slice) {
            return Ok(sig);
        }
    }
    Err(StringError(format!("Signature decoding error: {sig}")))
}

/// Stop listening for connection and disconnect existing ones
/// This will complete sent message in the queues.
///
/// ### Arguments
///
/// * `node_conn` - Node to use for connections
pub async fn shutdown_connections(node_conn: &mut Node) {
    node_conn.abort_heartbeat_handle();
    join_all(node_conn.stop_listening().await).await;
    join_all(node_conn.disconnect_all(None).await).await;
}

/// Loop reconnect and test disconnect
///
/// ### Arguments
///
/// * `node_conn`         - Node to use for connections
/// * `addrs_to_connect`  - Addresses to establish connections to
/// * `local_events_tx`   - Channel for local events
pub fn loops_re_connect_disconnect(
    node_conn: Node,
    addrs_to_connect: Vec<SocketAddr>,
    local_events_tx: LocalEventSender,
) -> (
    (impl Future<Output = ()>, oneshot::Sender<()>),
    (impl Future<Output = ()>, oneshot::Sender<()>),
) {
    let mut local_events_tx_for_disconnect = local_events_tx.clone();
    // PERMANENT CONNEXION HANDLING
    let re_connect = {
        let (stop_re_connect_tx, stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let node_conn = node_conn.clone();
        (
            async move {
                info!("Start connect to requested peers");
                loop_connnect_to_peers_async(
                    node_conn,
                    addrs_to_connect,
                    Some(stop_re_connect_rx),
                    local_events_tx,
                )
                .await;
                info!("Reconnect complete");
            },
            stop_re_connect_tx,
        )
    };

    // TEST DIS-CONNECTION HANDLING
    let disconnect_test = {
        let (stop_re_connect_tx, mut stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let mut node_conn = node_conn;
        let mut paused = true;
        let mut shutdown_num = None;
        (
            async move {
                info!("Start mode input check");
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                        _ = &mut stop_re_connect_rx => break,
                    };

                    paused = pause_and_disconnect_on_path(&mut node_conn, paused).await;
                    shutdown_num = shutdown_on_path(
                        &mut node_conn,
                        &mut local_events_tx_for_disconnect,
                        shutdown_num,
                    )
                    .await;
                }
                info!("Complete mode input check");
            },
            stop_re_connect_tx,
        )
    };
    (re_connect, disconnect_test)
}

/// Check disconnect path and pause/disconnect requested connections.
/// Return the new paused state.
///
/// ### Arguments
///
/// * `node_conn`   - Node to use for connections
/// * `paused`      - Current paused state
async fn pause_and_disconnect_on_path(node_conn: &mut Node, paused: bool) -> bool {
    let disconnect = format!("disconnect_{}", node_conn.local_address().port());
    let path = std::path::Path::new(&disconnect);
    match (paused, path.exists()) {
        (false, true) => {
            let content = std::fs::read_to_string(path).unwrap_or_default();
            let diconnect_addrs: Vec<_> = content
                .split(&[',', '\n'][..])
                .filter_map(|v| v.parse::<SocketAddr>().ok())
                .collect();

            warn!(
                "disconnect from {:?} all {:?}",
                node_conn.local_address(),
                diconnect_addrs
            );
            node_conn.set_pause_listening(true).await;
            if diconnect_addrs.is_empty() {
                join_all(node_conn.disconnect_all(None).await).await;
            } else {
                join_all(node_conn.disconnect_all(Some(&diconnect_addrs)).await).await;
            }
            true
        }
        (true, false) => {
            node_conn.set_pause_listening(false).await;
            false
        }
        _ => paused,
    }
}

/// Check given shutdown path and trigger coordinated or immediate shutdown.
/// Return the new shutdown block num.
///
/// ### Arguments
///
/// * `node_conn`       - Node to use for connections
/// * `local_events_tx` - Channel for local events
/// * `shutdown_num`    - Current shutdown state
async fn shutdown_on_path(
    node_conn: &mut Node,
    local_events_tx: &mut LocalEventSender,
    shutdown_num: Option<(u64, bool)>,
) -> Option<(u64, bool)> {
    let shutdown_now_one = format!("shutdown_now_{}", node_conn.local_address().port());
    let shutdown_now_all = "shutdown_now".to_owned();
    let shutdown_coord_one = format!("shutdown_coordinated_{}", node_conn.local_address().port());
    let shutdown_coord_all = "shutdown_coordinated".to_owned();

    let now = true;
    let path = Some((std::path::Path::new(&shutdown_now_all), now))
        .filter(|(p, _)| p.exists())
        .or_else(|| Some((std::path::Path::new(&shutdown_coord_all), !now)))
        .filter(|(p, _)| p.exists())
        .or_else(|| Some((std::path::Path::new(&shutdown_now_one), now)))
        .filter(|(p, _)| p.exists())
        .or_else(|| Some((std::path::Path::new(&shutdown_coord_one), !now)))
        .filter(|(p, _)| p.exists());

    if let Some((path, is_now)) = path {
        let content = std::fs::read_to_string(path).unwrap_or_default();
        let block_num = content.trim().parse::<u64>().ok().unwrap_or(0);
        let result = Some((block_num, is_now));

        if shutdown_num != result {
            warn!(
                "shutdown from {:?} at block {:?} now={}",
                node_conn.local_address(),
                block_num,
                is_now
            );

            let event = if is_now {
                LocalEvent::Exit("Shutdown")
            } else {
                LocalEvent::CoordinatedShutdown(block_num)
            };
            if let Err(e) = local_events_tx.send(event, "file_shutdown").await {
                warn!("Cound not send {:?} ({:?})", event, e);
            }
        }

        result
    } else {
        shutdown_num
    }
}

/// Get all the script_public_key and OutPoint from the (hash,transactions)
///
/// ### Arguments
///
/// * `txs` - The entries to to provide an update for.
pub fn get_pk_with_out_point<'a>(
    txs: impl Iterator<Item = (&'a String, &'a Transaction)>,
) -> impl Iterator<Item = (&'a String, OutPoint)> {
    get_tx_out_with_out_point(txs)
        .filter_map(|(op, txout)| txout.script_public_key.as_ref().map(|spk| (spk, op)))
}

pub fn get_pk_with_fees<'a>(
    txs: impl Iterator<Item = (&'a String, &'a Transaction)>,
) -> impl Iterator<Item = (&'a String, OutPoint)> {
    get_fees_with_out_point(txs)
        .filter_map(|(op, fees)| fees.script_public_key.as_ref().map(|spk| (spk, op)))
}

pub fn get_pk_with_fees_cloned<'a>(
    txs: impl Iterator<Item = (&'a String, &'a Transaction)> + 'a,
) -> impl Iterator<Item = (String, OutPoint)> + 'a {
    get_fees_with_out_point(txs)
        .filter_map(|(op, fees)| fees.script_public_key.as_ref().map(|spk| (spk.clone(), op)))
}

/// Get all the script_public_key and OutPoint from the (hash,transactions)
///
/// ### Arguments
///
/// * `txs` - The entries to to provide an update for.
pub fn get_pk_with_out_point_cloned<'a>(
    txs: impl Iterator<Item = (&'a String, &'a Transaction)> + 'a,
) -> impl Iterator<Item = (String, OutPoint)> + 'a {
    get_pk_with_out_point(txs).map(|(spk, op)| (spk.clone(), op))
}

/// Get all the script_public_key and OutPoint from the UTXO set
///
/// ### Arguments
///
/// * `utxo_set` - The UTXO set.
pub fn get_pk_with_out_point_from_utxo_set<'a>(
    utxo_set: impl Iterator<Item = (&'a OutPoint, &'a TxOut)>,
) -> impl Iterator<Item = (&'a String, &'a OutPoint)> {
    utxo_set.filter_map(|(op, txout)| txout.script_public_key.as_ref().map(|spk| (spk, op)))
}

/// Get all the script_public_key and OutPoint from the UTXO set
///
/// ### Arguments
///
/// * `utxo_set` - The UTXO set.
pub fn get_pk_with_out_point_from_utxo_set_cloned<'a>(
    utxo_set: impl Iterator<Item = (&'a OutPoint, &'a TxOut)> + 'a,
) -> impl Iterator<Item = (String, OutPoint)> + 'a {
    get_pk_with_out_point_from_utxo_set(utxo_set).map(|(spk, op)| (spk.clone(), op.clone()))
}

/// Concatenate 2 maps of K V.
///
/// ### Arguments
///
/// * `m1` - First map
/// * `m2` - Second map
pub fn concat_maps<K: Clone + Ord, V: Clone>(
    m1: &BTreeMap<K, V>,
    m2: &BTreeMap<K, V>,
) -> BTreeMap<K, V> {
    m1.iter()
        .chain(m2.iter())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Create a new item asset transaction (only used on Mempool node)
///
/// ### Arguments
///
/// * `item_amount`      - Item amount
/// * `script_public_key`   - Public address key
/// * `public key`          - Public key
/// * `signature`           - Signature
pub fn create_item_asset_tx_from_sig(
    b_num: u64,
    item_amount: u64,
    script_public_key: String,
    public_key: String,
    signature: String,
    genesis_hash_spec: GenesisTxHashSpec,
    metadata: Option<String>,
) -> Result<(Transaction, String), StringError> {
    let genesis_hash_create = genesis_hash_spec.get_genesis_hash();
    let item = Asset::item(item_amount, genesis_hash_create.clone(), metadata);
    let asset_hash = construct_tx_in_signable_asset_hash(&item);
    let tx_out = TxOut::new_asset(script_public_key, item, None);
    let public_key = decode_pub_key(&public_key)?;
    let signature = decode_signature(&signature)?;

    let tx_in = TxIn {
        previous_out: None,
        script_signature: Script::new_create_asset(b_num, asset_hash, signature, public_key),
    };

    let tx = construct_tx_core(vec![tx_in], vec![tx_out], None);
    let tx_hash = genesis_hash_create.unwrap_or_else(|| construct_tx_hash(&tx));

    Ok((tx, tx_hash))
}

/// Constructs a coinbase transaction
/// TODO: Adding block number to coinbase construction non-ideal. Consider moving to Mempool
/// construction or mining later
///
/// ### Arguments
///
/// * `b_num`       - Block number for the current coinbase block
/// * `amount`      - Amount of tokens allowed in coinbase
/// * `address`     - Address to send the coinbase amount to
pub fn construct_coinbase_tx(b_num: u64, amount: TokenAmount, address: String) -> Transaction {
    let tx_in = TxIn::new_from_script(Script::new_for_coinbase(b_num));
    let tx_out = TxOut {
        value: Asset::Token(amount),
        script_public_key: Some(address),
        locktime: b_num + COINBASE_MATURITY,
        ..Default::default()
    };

    construct_tx_core(vec![tx_in], vec![tx_out], None)
}

/// Confert to ApiKeys data structure
pub fn to_api_keys(api_keys: BTreeMap<String, Vec<String>>) -> ApiKeys {
    Arc::new(Mutex::new(api_keys.into_iter().collect()))
}

/// Confert to ApiKeys data structure
pub fn to_route_pow_infos(route_pow_infos: BTreeMap<String, usize>) -> RoutesPoWInfo {
    Arc::new(Mutex::new(route_pow_infos.into_iter().collect()))
}

/// Check to see if DDE transaction participants match
pub fn check_druid_participants(droplet: &DruidDroplet) -> bool {
    droplet
        .txs
        .iter()
        .all(|(_, tx)| tx.druid_info.as_ref().map(|i| i.participants) == Some(droplet.participants))
}

/// Test UnicornFixedInfo with fast compuation
pub fn get_test_common_unicorn() -> UnicornFixedInfo {
    UnicornFixedInfo{
        modulus: "6864797660130609714981900799081393217269435300143305409394463459185543183397656052122559640661454554977296311391480858037121987999716643812574028291115057151".to_owned(),
        iterations: 2,
        security: 1
    }
}

/// Get the current timestamp as a string
pub fn get_timestamp_now() -> i64 {
    let now = Utc::now();
    now.timestamp()
}

/// Check if the difference between two timestamps is greater than a given difference in milliseconds
///
/// ### Arguments
///
/// * `timestamp1` - First timestamp
/// * `timestamp2` - Second timestamp
/// * `difference_in_millis` - Difference in milliseconds
pub fn is_timestamp_difference_greater(
    timestamp1: u64,
    timestamp2: u64,
    difference_in_millis: u64,
) -> bool {
    let time1 = UNIX_EPOCH + Duration::from_secs(timestamp1);
    let time2 = UNIX_EPOCH + Duration::from_secs(timestamp2);

    // Calculate the absolute difference in durations
    let duration_difference = if time1 > time2 {
        time1.duration_since(time2).unwrap()
    } else {
        time2.duration_since(time1).unwrap()
    };

    duration_difference > Duration::from_millis(difference_in_millis)
}

/// Attempt to send a message to the UI
///
/// NOTE: This channel is not guaranteed to be open, so we ignore any errors
pub async fn try_send_to_ui(ui_tx: Option<&mpsc::Sender<Rs2JsMsg>>, msg: Rs2JsMsg) {
    if let Some(ui_tx) = ui_tx {
        let _res = ui_tx.send(msg).await;
    }
}

pub mod rug_integer {
    use rug::Integer;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serialisation function for big ints
    pub fn serialize<S>(x: &Integer, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value: String = x.to_string_radix(16);
        value.serialize(s)
    }

    /// Deserialisation function for big ints
    pub fn deserialize<'de, D>(d: D) -> Result<Integer, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(d)?;
        Integer::from_str_radix(&value, 16).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
pub struct UnitsPrefixed {
    pub value: f64,
    pub unit_name: &'static str,
    pub duration: Option<Duration>,
}

impl fmt::Display for UnitsPrefixed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let scales = &[
            (1000_000_000_000f64, "T"),
            (1000_000_000f64, "G"),
            (1000_000f64, "M"),
            (1000f64, "k"),
        ];

        let value = match &self.duration {
            None => self.value,
            Some(duration) => self.value / duration.as_secs_f64(),
        };

        let (divisor, prefix) = scales
            .iter()
            .find(|(threshold, _)| value > *threshold)
            .unwrap_or(&(1f64, ""));

        match &self.duration {
            None => write!(f, "{} {}{}", value / divisor, prefix, self.unit_name),
            Some(_) => write!(f, "{} {}{}/s", value / divisor, prefix, self.unit_name),
        }
    }
}

/// Splits the given integer range into smaller segments with the given maximum size.
///
/// ### Arguments
///
/// * `start`           - the first value
/// * `len`             - the total length of the range
/// * `max_block_size`  - the maximum size of each block
pub fn split_range_into_blocks(
    start: u32,
    len: u32,
    max_block_size: u32,
) -> impl Iterator<Item = (u32, u32)> {
    assert_ne!(max_block_size, 0);

    struct Itr {
        pos: u32,
        remaining: u32,
        max_block_size: u32,
    }

    impl Iterator for Itr {
        type Item = (u32, u32);

        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }

            let block_size = u32::min(self.remaining, self.max_block_size);
            let result = (self.pos, block_size);
            self.pos += block_size;
            self.remaining -= block_size;
            Some(result)
        }
    }

    Itr {
        pos: start,
        remaining: len,
        max_block_size,
    }
}

/// Returns all possible byte strings with the given length.
///
/// Note that for a length of `0` this will return a single 0-length byte string.
///
/// ### Arguments
///
/// * `len`  - the length of the byte strings to generate
pub fn all_byte_strings(len: usize) -> impl Iterator<Item = Box<[u8]>> {
    struct Itr {
        buf: Box<[u8]>,
        done: bool,
    }

    impl Iterator for Itr {
        type Item = Box<[u8]>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.done {
                // Iterator has already reached the end
                return None;
            }

            // Save the current value to be returned later
            let result = self.buf.clone();

            // Try to increment the value
            for byte in self.buf.as_mut() {
                if let Some(next_value) = (*byte).checked_add(1) {
                    // The byte was incremented successfully
                    *byte = next_value;
                    return Some(result);
                } else {
                    // The byte overflowed, set it to 0 and proceed to increment the next one
                    *byte = 0;
                }
            }

            // If we got this far, all the bytes were 0xFF, meaning that we've successfully
            // iterated through the entire sequence.
            self.done = true;

            Some(result)
        }
    }

    Itr {
        buf: vec![0u8; len].into_boxed_slice(),
        done: false,
    }
}

/*---- TESTS ----*/

#[cfg(test)]
mod util_tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    /// Tests whether URL strings can be parsed successfully
    async fn test_create_socket_addr() {
        let ip_raw = "0.0.0.0".to_string();
        let ip_with_port = "0.0.0.0:12300".to_string();
        let domain = "http://localhost".to_string();
        let domain_with_port = "http://localhost:12300".to_string();

        let ip_addr = create_socket_addr(&ip_raw).await.unwrap();
        let ip_with_port_addr = create_socket_addr(&ip_with_port).await.unwrap();
        let domain_addr = create_socket_addr(&domain).await.unwrap();
        let domain_with_port_addr = create_socket_addr(&domain_with_port).await.unwrap();

        assert_eq!(
            ip_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 80)
        );
        assert_eq!(
            ip_with_port_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 12300)
        );

        assert_eq!(
            domain_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 80)
        );

        assert_eq!(
            domain_with_port_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12300)
        );
    }

    #[test]
    fn test_split_range_into_blocks() {
        assert_eq!(
            split_range_into_blocks(0, 16, 4).collect::<Vec<_>>(),
            vec![(0, 4), (4, 4), (8, 4), (12, 4)]
        );

        assert_eq!(
            split_range_into_blocks(0, 15, 4).collect::<Vec<_>>(),
            vec![(0, 4), (4, 4), (8, 4), (12, 3)]
        );

        assert_eq!(
            split_range_into_blocks(0, u32::MAX, 1 << 30).collect::<Vec<_>>(),
            vec![
                (0 << 30, 1 << 30),
                (1 << 30, 1 << 30),
                (2 << 30, 1 << 30),
                (3 << 30, (1 << 30) - 1),
            ]
        );
    }

    #[test]
    fn test_all_byte_strings() {
        assert_eq!(all_byte_strings(0).collect::<Vec<_>>(), vec![Box::from([])]);

        assert_eq!(
            all_byte_strings(1).collect::<Vec<_>>(),
            (u8::MIN..=u8::MAX)
                .map(|b| b.to_le_bytes().into())
                .collect::<Vec<_>>()
        );

        assert_eq!(
            all_byte_strings(2).collect::<Vec<_>>(),
            (u16::MIN..=u16::MAX)
                .map(|b| b.to_le_bytes().into())
                .collect::<Vec<_>>()
        );
    }
}
