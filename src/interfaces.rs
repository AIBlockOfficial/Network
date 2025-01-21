use crate::configurations::MempoolNodeSharedConfig;
use crate::mempool::MempoolError;
use crate::mempool_raft::MempoolConsensusedRuntimeData;
use crate::raft::{CommittedIndex, RaftMessageWrapper};
use crate::tracked_utxo::TrackedUtxoSet;
use crate::unicorn::Unicorn;
use crate::utils::rug_integer;
use bytes::Bytes;
use rug::Integer;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use tw_chain::primitives::asset::Asset;
use tw_chain::primitives::asset::TokenAmount;
use tw_chain::primitives::block::{Block, BlockHeader};
use tw_chain::primitives::druid::DruidExpectation;
use tw_chain::primitives::transaction::{GenesisTxHashSpec, TxIn};
use tw_chain::primitives::transaction::{OutPoint, Transaction, TxOut};

//*======== INITIAL ISSUANCES =========*//

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialIssuance {
    pub amount: TokenAmount,
    pub address: String,
    pub block_height: u64,
}

impl InitialIssuance {
    pub fn new(amount: TokenAmount, address: String, block_height: u64) -> Self {
        InitialIssuance {
            amount,
            address,
            block_height,
        }
    }
}

//*======== BLOCKCHAIN ITEM =========*//

/// Struct used for simplifying JSON deserialization on the client
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutPointData {
    out_point: OutPoint,
    value: Asset,
}

impl OutPointData {
    pub fn new(out_point: OutPoint, value: Asset) -> Self {
        OutPointData { out_point, value }
    }
}

/// Simple BTreeMap structure used to hold `(OutPoint, Asset, String/Data to Sign)` pairs
/// with respect to a public key address
pub type AddressesWithOutPoints = BTreeMap<String, Vec<OutPointData>>;

/// UTXO set type
pub type UtxoSet = BTreeMap<OutPoint, TxOut>;
pub type UtxoSetRef<'a> = BTreeMap<&'a OutPoint, &'a TxOut>;

/// DRUID Pool
pub type DruidPool = BTreeMap<String, DruidDroplet>;

/// Token to uniquely identify messages.
pub type Token = u64;

/// Enum that determines full UTXO retrieval or subset UTXO retrieval
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UtxoFetchType {
    All,
    AnyOf(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionResponse {
    pub tx: Transaction,
    pub meta: TransactionResponseMeta,
}

impl TransactionResponse {
    pub fn new(tx: Transaction, meta: TransactionResponseMeta) -> Self {
        TransactionResponse { tx, meta }
    }
}

/// The status of a transaction as per the mempool
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxStatus {
    pub status: TxStatusType,
    pub timestamp: i64,
    pub additional_info: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxStatusType {
    Pending,
    Confirmed,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionResponseMeta {
    pub block_num: u64,
    pub block_hash: String,
}

impl TransactionResponseMeta {
    pub fn new(block_num: u64, block_hash: String) -> Self {
        TransactionResponseMeta {
            block_num,
            block_hash,
        }
    }
}

/// Struct used to keep-track of the next item-based payment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RbPaymentData {
    pub sender_asset: Asset,
    pub sender_half_druid: String,
    pub tx_ins: Vec<TxIn>,
    pub tx_outs: Vec<TxOut>,
}

/// Struct used to make a request for a new item-based payment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RbPaymentRequestData {
    pub sender_address: String,
    pub sender_half_druid: String,
    pub sender_from_addr: String,
    pub sender_asset: Asset,
    pub sender_drs_tx_expectation: Option<String>,
}

/// Struct used to make a response to a new item-based payment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RbPaymentResponseData {
    pub receiver_address: String,
    pub receiver_half_druid: String,
    pub sender_druid_expectation: DruidExpectation,
}

/// A placeholder struct for sensible feedback
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub success: bool,
    pub reason: String,
}

/// A response struct for payment requests specifically
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaymentResponse {
    pub success: bool,
    pub reason: String,
    pub tx_hash: String,
    pub tx: Option<Transaction>,
}

/// Mined block as stored in DB.
/// TODO: Are we not storing the other info from CommonBlockInfo? (Unicorn etc..._
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct StoredSerializingBlock {
    pub block: Block,
}

/// Common info in all mined block that form a complete block.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonBlockInfo {
    pub block: Block,
    pub block_txs: BTreeMap<String, Transaction>,
    pub pow_p_value: u8,
    pub pow_d_value: u8,
    pub unicorn: Unicorn,
    #[serde(with = "rug_integer")]
    pub unicorn_witness: Integer,
}

/// Mined block structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinedBlock {
    pub common: CommonBlockInfo,
    pub extra_info: MinedBlockExtraInfo,
}

/// Additional info specific to one of the mined block that form a complete block.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct MinedBlockExtraInfo {
    pub shutdown: bool,
}

/// Stored block info needed to generate next block
#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BlockStoredInfo {
    pub block_hash: String,
    pub block_num: u64,
    pub nonce: Vec<u8>,
    pub mining_transactions: BTreeMap<String, Transaction>,
    pub shutdown: bool,
}

/// PoW additional info
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PowInfo {
    pub participant_only: bool,
    pub b_num: u64,
}

/// Transaction hashes that have been mined with DRUID info
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct DruidTxInfo {
    pub tx_hashes: Vec<String>,
}

/// PoW structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofOfWork {
    pub address: String,
    pub nonce: Vec<u8>,
}

/// Winning PoW structure
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WinningPoWInfo {
    pub nonce: Vec<u8>,
    pub mining_tx: (String, Transaction),
    pub p_value: u8,
    pub d_value: u8,
}

/// Druid pool structure for checking and holding participants
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DruidDroplet {
    pub participants: usize,
    pub txs: BTreeMap<String, Transaction>,
}

impl DruidDroplet {
    pub fn new(participants: usize) -> Self {
        let txs = Default::default();
        DruidDroplet { participants, txs }
    }
}

/// A placeholder Contract struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract;

/// A placeholder Heat struct
pub struct Heat;

/// Denotes existing node types
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Miner,
    Storage,
    Mempool,
    User,
    PreLaunch,
}

/// Returns a string to represent the specified node type
pub fn node_type_as_str(node_type: NodeType) -> &'static str {
    match node_type {
        NodeType::Miner => "Miner",
        NodeType::Mempool => "Mempool",
        NodeType::Storage => "Storage",
        NodeType::User => "User",
        NodeType::PreLaunch => "Prelaunch",
    }
}

/// Mined block or transaction as stored in DB.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockchainItem {
    pub version: u32,
    pub item_meta: BlockchainItemMeta,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
    pub data_json: Vec<u8>,
}

impl BlockchainItem {
    pub fn is_empty(&self) -> bool {
        self.key.is_empty()
    }
}

/// Denotes blockchain item metadata
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum BlockchainItemMeta {
    Block { block_num: u64, tx_len: u32 },
    Tx { block_num: u64, tx_num: u32 },
}

impl Default for BlockchainItemMeta {
    fn default() -> Self {
        Self::Block {
            block_num: 0,
            tx_len: 0,
        }
    }
}

impl BlockchainItemMeta {
    pub fn as_type(&self) -> BlockchainItemType {
        match self {
            Self::Block { .. } => BlockchainItemType::Block,
            Self::Tx { .. } => BlockchainItemType::Tx,
        }
    }

    pub fn block_num(&self) -> u64 {
        match self {
            Self::Block { block_num, .. } => *block_num,
            Self::Tx { block_num, .. } => *block_num,
        }
    }
}

/// Denotes blockchain item types
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockchainItemType {
    Block,
    Tx,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbItem {
    pub column: String,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}

/// Internal protocol messages exchanged between nodes.
/// Handle nodes membership & bootstrapping. Wrap higher-level protocols.
///
/// ### Message IDs
/// The requests & response pairs must have corresponding message IDs to make sure
/// that an application is able to match them. However, not all requests require responses,
/// and in this case the application will not be registering its interest in a response with
/// a certain message ID.
///
/// Message IDs are also used for _gossip_: a node will remember message IDs it has seen and will
/// retransmit all messages it has not seen yet.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum CommMessage {
    /// Handshake request that peers send when they connect to someone.
    HandshakeRequest {
        /// Node network version.
        network_version: u32,
        /// Type of node sending the request.
        node_type: NodeType,
        /// Publicly available socket address of the node that can be used for inbound connections.
        public_address: SocketAddr,
    },
    /// Handshake response.
    HandshakeResponse {
        /// Node network version.
        network_version: u32,
        /// Type of node sending the response.
        node_type: NodeType,
        /// contacts of ring members.
        contacts: Vec<SocketAddr>,
        /// Publicly resolved IP address of the node who made the handshake request
        public_address: SocketAddr,
    },
    /// Gossip message, multicast to all peers within the same ring.
    Gossip {
        /// Contents of the gossip message. Can encapsulate other message types.
        payload: Bytes,
        /// Number of hops this message made.
        ttl: u8,
        /// Unique message identifier.
        id: Token,
    },
    /// Wraps a direct message from peer to peer. Can encapsulate other message types.
    Direct {
        /// Contents of a message. Can be serialized message of another type.
        payload: Bytes,
        /// Unique message ID. Can be used for correspondence between requests and responses.
        id: Token,
    },
    HeartBeatProbe(Token),
}

///============ STORAGE NODE ============///

/// Encapsulates storage requests
#[allow(clippy::large_enum_variant)]
#[derive(Deserialize, Serialize, Clone)]
pub enum StorageRequest {
    GetBlockchainItem { key: String },
    SendBlockchainItem { key: String, item: BlockchainItem },
    GetHistory { start_time: u64, end_time: u64 },
    GetUnicornTable { n_last_items: Option<u64> },
    SendPow { pow: ProofOfWork },
    SendBlock { mined_block: Option<MinedBlock> },
    Store { incoming_contract: Contract },
    Closing,
    SendRaftCmd(RaftMessageWrapper),
}

impl fmt::Debug for StorageRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StorageRequest::*;

        match *self {
            GetBlockchainItem { .. } => write!(f, "GetBlockchainItem"),
            SendBlockchainItem { .. } => write!(f, "SendBlockchainItem"),
            GetHistory { .. } => write!(f, "GetHistory"),
            GetUnicornTable { .. } => write!(f, "GetUnicornTable"),
            SendPow { .. } => write!(f, "SendPoW"),
            SendBlock { .. } => write!(f, "SendBlock"),
            Store { .. } => write!(f, "Store"),
            Closing => write!(f, "Closing"),
            SendRaftCmd(_) => write!(f, "SendRaftCmd"),
        }
    }
}

pub trait StorageInterface {
    /// Get a blockchain item from stored history.
    ///
    /// ### Arguments
    ///
    /// * `peer` - The requestor address.
    /// * `key`  - The blockchain item key.
    fn get_blockchain_item(&mut self, peer: SocketAddr, key: String) -> Response;

    /// Receive a blockchain item from storage node.
    ///
    /// ### Arguments
    ///
    /// * `peer` - The requestor address.
    /// * `key`  - The blockchain item key.
    /// * `item` - The data associated with key.
    fn receive_blockchain_item(
        &mut self,
        peer: SocketAddr,
        key: String,
        item: BlockchainItem,
    ) -> Response;

    /// Returns a read only section of a stored history.
    /// Time slices are considered to be block IDs (u64).
    fn get_history(&self, start_time: &u64, end_time: &u64) -> Response;

    /// Whitelists the supplied address for edit permissions.
    ///
    /// ### Arguments
    ///
    /// * `address` - Address to whitelist
    fn whitelist(&mut self, address: SocketAddr) -> Response;

    /// Provides the UnicornShard table, with an optional section slice.
    /// Not providing a section slice will return the entire table.
    ///
    /// ### Arguments
    ///
    /// * `n_last_items`    - Number of last items to fetch from unicorn list
    fn get_unicorn_table(&self, n_last_items: Option<u64>) -> Response;

    /// Receives a PoW to match against the current pre-block.
    ///
    /// ### Arguments
    ///
    /// * `pow` - Proof of Work to match
    fn receive_pow(&self, pow: ProofOfWork) -> Response;

    /// Receives agreed contracts for storage
    ///
    /// ### Arguments
    ///
    /// * `contract`    - Contract to store
    fn receive_contracts(&self, contract: Contract) -> Response;
}

///============ MINER NODE ============///

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize, Clone)]
pub enum MineApiRequest {
    /// Get the connection status of this node
    GetConnectionStatus,
    /// Get mining status
    GetMiningStatus,
    /// Initiate pause mining
    InitiatePauseMining,
    /// Initiate resume mining
    InitiateResumeMining,
    /// Connect to to mempool Node
    ConnectToMempool,
    // Disconnect from mempool Node
    DisconnectFromMempool,
    // Request UTXO set for wallet update
    RequestUTXOSet(UtxoFetchType),
    // Set static miner address
    SetStaticMinerAddress {
        address: Option<String>,
    },
    // Get static miner address
    GetStaticMinerAddress,
}

/// Encapsulates miner requests
#[derive(Serialize, Deserialize, Clone)]
pub enum MineRequest {
    SendBlock {
        pow_info: PowInfo,
        rnum: Vec<u8>,
        win_coinbases: Vec<String>,
        reward: TokenAmount,
        block: Option<BlockHeader>,
        b_num: u64,
    },
    SendBlockchainItem {
        key: String,
        item: BlockchainItem,
    },
    SendTransactions {
        tx_merkle_verification: Vec<String>,
    },
    /// Process received utxo set
    SendUtxoSet {
        utxo_set: UtxoSet,
    },
    MinerRemovedAck,
    MinerNotAuthorized,
    MinerApi(MineApiRequest),
    Closing,
}

impl fmt::Debug for MineRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MineRequest::*;

        match *self {
            SendBlockchainItem { .. } => write!(f, "SendBlockchainItem"),
            SendBlock { .. } => write!(f, "SendBlock"),
            SendTransactions { .. } => write!(f, "SendTransactions"),
            SendUtxoSet { .. } => write!(f, "SendUtxoSet"),
            Closing => write!(f, "Closing"),
            MinerRemovedAck => write!(f, "MinerRemovedAck"),
            MinerNotAuthorized => write!(f, "MinerNotAuthorized"),
            MinerApi(MineApiRequest::GetConnectionStatus) => write!(f, "GetConnectionStatus"),
            MinerApi(MineApiRequest::GetMiningStatus) => write!(f, "GetMiningStatus"),
            MinerApi(MineApiRequest::InitiatePauseMining) => write!(f, "InitiatePauseMining"),
            MinerApi(MineApiRequest::InitiateResumeMining) => write!(f, "InitiateResumeMining"),
            MinerApi(MineApiRequest::ConnectToMempool) => write!(f, "ConnectToMempool"),
            MinerApi(MineApiRequest::DisconnectFromMempool) => write!(f, "DisconnectFromMempool"),
            MinerApi(MineApiRequest::RequestUTXOSet(_)) => write!(f, "RequestUTXOSet"),
            MinerApi(MineApiRequest::SetStaticMinerAddress { .. }) => {
                write!(f, "SetStaticMinerAddress")
            }
            MinerApi(MineApiRequest::GetStaticMinerAddress) => write!(f, "GetStaticMinerAddress"),
        }
    }
}

pub trait MinerInterface {
    /// Receive a blockchain item from storage node.
    ///
    /// ### Arguments
    ///
    /// * `peer` - The requestor address.
    /// * `key`  - The blockchain item key.
    /// * `item` - The data associated with key.
    fn receive_blockchain_item(
        &mut self,
        peer: SocketAddr,
        key: String,
        item: BlockchainItem,
    ) -> Response;
}

/* ---------------- STRUCT TO ENCAPSULATE GENERAL UI FEEDBACK --------------- */
// Note that this struct is primarly used with the desktop UI as well as
// server-side events, and should not form part of the public API.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Rs2JsMsg {
    // Success message
    Success { success: String },
    // Error message
    Error { error: String },
    // Info message
    Info { info: String },
    // Warning Message
    Warning { warning: String },
    // A JSON-serialized value
    Value(serde_json::Value),
    // Close the feedback loop
    Exit,
}

///============ MEMPOOL NODE ============///

// Encapsulates mempool requests injected by API
#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize, Clone)]
pub enum MempoolApiRequest {
    SendCreateItemRequest {
        item_amount: u64,
        script_public_key: String,
        public_key: String,
        signature: String,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    },
    SendTransactions {
        transactions: Vec<Transaction>,
    },
    PauseNodes {
        b_num: u64,
    },
    ResumeNodes,
    SendSharedConfig {
        shared_config: MempoolNodeSharedConfig,
    },
}

/// Encapsulates mempool requests & responses.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Clone)]
pub enum MempoolRequest {
    /// Process an API internal request
    MempoolApi(MempoolApiRequest),

    SendSharedConfig {
        shared_config: MempoolNodeSharedConfig,
    },
    SendUtxoRequest {
        address_list: UtxoFetchType,
        requester_node_type: NodeType,
    },
    SendBlockStored(BlockStoredInfo),
    SendPoW {
        block_num: u64,
        nonce: Vec<u8>,
        coinbase: Transaction,
    },
    SendPartitionEntry {
        pow_info: PowInfo,
        partition_entry: ProofOfWork,
    },
    SendTransactions {
        transactions: Vec<Transaction>,
    },
    SendPartitionRequest {
        mining_api_key: Option<String>,
    },
    SendUserBlockNotificationRequest,
    CoordinatedPause {
        b_num: u64, // Pause the nodes on current b_num + b_num
    },
    CoordinatedResume,
    Closing,
    RequestRemoveMiner,
    RequestRuntimeData,
    SendRuntimeData {
        runtime_data: MempoolConsensusedRuntimeData,
    },
    SendRaftCmd(RaftMessageWrapper),
}

impl fmt::Debug for MempoolRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MempoolRequest::*;

        match *self {
            MempoolApi(MempoolApiRequest::SendCreateItemRequest { .. }) => {
                write!(f, "Api::SendCreateItemRequest")
            }
            MempoolApi(MempoolApiRequest::SendTransactions { .. }) => {
                write!(f, "Api::SendTransactions")
            }
            MempoolApi(MempoolApiRequest::PauseNodes { .. }) => write!(f, "Api::PauseNodes"),
            MempoolApi(MempoolApiRequest::ResumeNodes) => write!(f, "Api::ResumeNodes"),
            MempoolApi(MempoolApiRequest::SendSharedConfig { .. }) => {
                write!(f, "Api::SendSharedConfig")
            }
            SendUtxoRequest { .. } => write!(f, "SendUtxoRequest"),
            SendBlockStored(_) => write!(f, "SendBlockStored"),
            SendPoW { ref block_num, .. } => write!(f, "SendPoW({block_num})"),
            SendPartitionEntry { .. } => write!(f, "SendPartitionEntry"),
            SendTransactions { .. } => write!(f, "SendTransactions"),
            SendUserBlockNotificationRequest => write!(f, "SendUserBlockNotificationRequest"),
            SendPartitionRequest { .. } => write!(f, "SendPartitionRequest"),
            SendSharedConfig { .. } => write!(f, "SendSharedConfig"),
            Closing => write!(f, "Closing"),
            CoordinatedPause { .. } => write!(f, "CoordinatedPause"),
            CoordinatedResume => write!(f, "CoordinatedResume"),
            RequestRemoveMiner => write!(f, "RequestRemoveMiner"),
            RequestRuntimeData => write!(f, "RequestRuntimeData"),
            SendRuntimeData { .. } => write!(f, "SendRuntimeData"),
            SendRaftCmd(_) => write!(f, "SendRaftCmd"),
        }
    }
}

pub trait MempoolInterface {
    /// Fetch UTXO set for given addresses
    fn fetch_utxo_set(
        &mut self,
        peer: SocketAddr,
        address_list: UtxoFetchType,
        node_type: NodeType,
    ) -> Response;

    /// Partitions a set of provided UUIDs for key creation/agreement
    /// TODO: Figure out the correct return type
    fn partition(&self, uuids: Vec<&'static str>) -> Response;

    /// Returns the internal service level data
    fn get_service_levels(&self) -> Response;

    /// Executes a received and approved contract
    ///
    /// ### Arguments
    ///
    /// * `contract` - Contract struct containing the contract to be executed.
    fn execute_contract(&self, contract: Contract) -> Response;

    /// Returns the next block reward value
    fn get_next_block_reward(&self) -> f64;
}

/// Mempool node API
pub trait MempoolApi {
    /// Get mempool node configuration that is shareable between peers
    fn get_shared_config(&self) -> MempoolNodeSharedConfig;

    /// Pause all mempool nodes
    fn pause_nodes(&mut self, b_num: u64) -> Response;

    /// Resume all mempool nodes
    fn resume_nodes(&mut self) -> Response;

    /// Share mempool node config with other mempool nodes
    fn send_shared_config(&mut self, shared_config: MempoolNodeSharedConfig) -> Response;

    /// Get the UTXO tracked set
    fn get_committed_utxo_tracked_set(&self) -> &TrackedUtxoSet;

    /// Get the current issued supply
    fn get_issued_supply(&self) -> TokenAmount;

    /// Get pending DRUID pool
    fn get_pending_druid_pool(&self) -> &DruidPool;

    /// Get the status of transaction/s
    fn get_transaction_status(&self, tx_hashes: Vec<String>) -> BTreeMap<String, TxStatus>;

    /// Receives transactions to be bundled into blocks
    ///
    /// ### Arguments
    ///
    /// * `transactions` - Transactions to be added into blocks.
    fn receive_transactions(&mut self, transactions: Vec<Transaction>) -> Response;

    /// Creates a new set of item assets
    fn create_item_asset_tx(
        &mut self,
        item_amount: u64,
        script_public_key: String,
        public_key: String,
        signature: String,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    ) -> Result<(Transaction, String), MempoolError>;
}

///============ USER NODE ============///

pub trait UserApi {
    fn make_payment(
        &mut self,
        address: String,
        amount: TokenAmount,
        locktime: Option<u64>,
    ) -> PaymentResponse;
}

/// Encapsulates user requests injected by API
#[derive(Deserialize, Serialize, Clone)]
pub enum UserApiRequest {
    /// Request to generate item-based asset
    SendCreateItemRequest {
        item_amount: u64,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    },

    /// Request to fetch UTXO set and update running total for specified addresses
    UpdateWalletFromUtxoSet {
        // TODO: Might need to change this request to a generic type for multiple use cases
        address_list: UtxoFetchType,
    },

    /// Request donation
    RequestDonation { paying_peer: SocketAddr },

    /// Request to make a payment to an IP address
    MakeIpPayment {
        payment_peer: SocketAddr,
        amount: TokenAmount,
        locktime: Option<u64>,
    },

    /// Request to make a payment to a public key address
    MakePayment {
        address: String,
        amount: TokenAmount,
        locktime: Option<u64>,
    },

    /// Request to make a payment to a public key address with a given excess address
    MakePaymentWithExcessAddress {
        address: String,
        amount: TokenAmount,
        excess_address: String,
        locktime: Option<u64>,
    },

    /// Send next payment constructed
    SendNextPayment,

    /// Request to generate a new address
    GenerateNewAddress,

    /// Get the connection status of this node
    GetConnectionStatus,

    /// Connect to mempool node
    ConnectToMempool,

    /// Disconnect from mempool
    DisconnectFromMempool,

    /// Delete addresses
    DeleteAddresses { addresses: BTreeSet<String> },

    /// Merge addresses to an excess address (if provided)
    MergeAddresses {
        addresses: BTreeSet<String>,
        excess_address: Option<String>,
    },
}

/// Encapsulates user requests
#[derive(Deserialize, Serialize, Clone)]
pub enum UserRequest {
    /// Process an API internal request
    UserApi(UserApiRequest),

    /// Request to make a item-based payment
    SendRbPaymentRequest {
        rb_payment_request_data: RbPaymentRequestData,
    },
    /// Provide response for item-based payment request
    SendRbPaymentResponse {
        rb_payment_response: Option<RbPaymentResponseData>,
    },
    /// Request payment address with optional proof of work
    SendAddressRequest,
    /// Provide payment address with optional proof of work
    SendPaymentAddress {
        address: String,
    },
    /// Complete payment
    SendPaymentTransaction {
        transaction: Transaction,
    },

    /// Process received utxo set
    SendUtxoSet {
        utxo_set: UtxoSet,
        b_num: u64,
    },
    /// Process received block being mined
    BlockMining {
        block: Block,
    },
    Closing,
}

impl fmt::Debug for UserRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use UserApiRequest::*;
        use UserRequest::*;

        match *self {
            UserApi(UpdateWalletFromUtxoSet { .. }) => write!(f, "UpdateWalletFromUtxoSet"),
            UserApi(RequestDonation { .. }) => write!(f, "RequestDonation"),
            UserApi(MakeIpPayment { .. }) => write!(f, "MakeIpPayment"),
            UserApi(MakePayment { .. }) => write!(f, "MakePayment"),
            UserApi(SendCreateItemRequest { .. }) => write!(f, "SendCreateItemRequest"),
            UserApi(MakePaymentWithExcessAddress { .. }) => {
                write!(f, "MakePaymentWithExcessAddress")
            }
            UserApi(GenerateNewAddress) => write!(f, "GenerateNewAddress"),
            UserApi(GetConnectionStatus) => write!(f, "GetConnectionStatus"),
            UserApi(ConnectToMempool) => write!(f, "ConnectToMempool"),
            UserApi(DisconnectFromMempool) => write!(f, "DisconnectFromMempool"),
            UserApi(DeleteAddresses { .. }) => write!(f, "DeleteAddresses"),
            UserApi(MergeAddresses { .. }) => write!(f, "MergeAddresses"),
            UserApi(SendNextPayment) => write!(f, "SendNextPayment"),

            SendAddressRequest { .. } => write!(f, "SendAddressRequest"),
            SendPaymentAddress { .. } => write!(f, "SendPaymentAddress"),
            SendPaymentTransaction { .. } => write!(f, "SendPaymentTransaction"),

            SendRbPaymentRequest { .. } => write!(f, "SendRbPaymentRequest"),
            SendRbPaymentResponse { .. } => write!(f, "SendRbPaymentResponse"),

            SendUtxoSet { .. } => write!(f, "SendUtxoSet"),
            BlockMining { .. } => write!(f, "BlockMining"),
            Closing => write!(f, "Closing"),
        }
    }
}
///============ PRE-LAUNCH NODE ============///

/// API Debug Data Struct
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DebugData {
    pub node_type: String,
    #[serde(borrow)]
    pub node_api: Vec<&'static str>,
    pub node_peers: Vec<(String, SocketAddr, String)>,
    pub routes_pow: BTreeMap<String, usize>,
}

/// Encapsulates storage requests
#[derive(Deserialize, Serialize, Clone)]
pub enum PreLaunchRequest {
    SendDbItems {
        committed: CommittedIndex,
        items: Vec<DbItem>,
    },
    Closing,
}

impl fmt::Debug for PreLaunchRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PreLaunchRequest::*;

        match *self {
            SendDbItems { .. } => write!(f, "SendDbItems"),
            Closing => write!(f, "Closing"),
        }
    }
}
