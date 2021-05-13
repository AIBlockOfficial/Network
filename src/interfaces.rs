#![allow(unused)]
use crate::hash_block;
use crate::raft::RaftMessageWrapper;
use bytes::Bytes;
use naom::primitives::asset::TokenAmount;
use naom::primitives::block::Block;
use naom::primitives::transaction::{OutPoint, Transaction, TxOut};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;

/// UTXO set type
pub type UtxoSet = BTreeMap<OutPoint, TxOut>;
/// Token to uniquely identify messages.
pub type Token = u64;

/// A placeholder struct for sensible feedback
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub success: bool,
    pub reason: &'static str,
}

/// Mined block as stored in DB.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct StoredSerializingBlock {
    pub block: Block,
    pub mining_tx_hash_and_nonces: BTreeMap<u64, (String, Vec<u8>)>,
}

/// Common info in all mined block that form a complete block.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonBlockInfo {
    pub block: Block,
    pub block_txs: BTreeMap<String, Transaction>,
}

/// Additional info specific to one of the mined block that form a complete block.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinedBlockExtraInfo {
    pub nonce: Vec<u8>,
    pub mining_tx: (String, Transaction),
    pub shutdown: bool,
}

/// Stored block info needed to generate next block
#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BlockStoredInfo {
    pub block_hash: String,
    pub block_num: u64,
    pub nonce: Vec<u8>,
    pub merkle_hash: String,
    pub mining_transactions: BTreeMap<String, Transaction>,
    pub shutdown: bool,
}

/// PoW structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofOfWork {
    pub address: String,
    pub nonce: Vec<u8>,
}

/// PoW structure for blocks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofOfWorkBlock {
    pub nonce: Vec<u8>,
    pub block: Block,
}

impl Default for ProofOfWorkBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl ProofOfWorkBlock {
    ///Proof of work block constructor
    pub fn new() -> Self {
        ProofOfWorkBlock {
            nonce: Vec::new(),
            block: Block::new(),
        }
    }
}

/// A placeholder Contract struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract;

/// A placeholder Heat struct
pub struct Heat;

/// A placeholder Asset struct
#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum Asset {
    Token(u64),
    Data(Vec<u8>),
}

/// Denotes existing node types
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Miner,
    Storage,
    Compute,
    User,
}

/// Mined block as stored in DB.
#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockchainItem {
    pub version: u32,
    pub item_type: BlockchainItemType,
    pub data: Vec<u8>,
}

/// Denotes blockchain item types
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockchainItemType {
    Block = b'b',
    Tx = b't',
}

impl Default for BlockchainItemType {
    fn default() -> Self {
        Self::Block
    }
}

impl BlockchainItemType {
    pub fn from_u8s(v: &[u8]) -> std::result::Result<Self, String> {
        match v {
            b"b" => Ok(BlockchainItemType::Block),
            b"t" => Ok(BlockchainItemType::Tx),
            v => Err(format!("Unkown BlockchainItemType: {:?}", v)),
        }
    }
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
}

///============ STORAGE NODE ============///

/// Encapsulates storage requests
#[allow(clippy::large_enum_variant)]
#[derive(Deserialize, Serialize, Clone)]
pub enum StorageRequest {
    GetBlockchainItem {
        key: String,
    },
    GetHistory {
        start_time: u64,
        end_time: u64,
    },
    GetUnicornTable {
        n_last_items: Option<u64>,
    },
    SendPow {
        pow: ProofOfWork,
    },
    SendBlock {
        common: CommonBlockInfo,
        mined_info: MinedBlockExtraInfo,
    },
    Store {
        incoming_contract: Contract,
    },
    Closing,
    SendRaftCmd(RaftMessageWrapper),
}

impl fmt::Debug for StorageRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StorageRequest::*;

        match *self {
            GetBlockchainItem { ref key } => write!(f, "GetBlockchainItem"),
            GetHistory {
                ref start_time,
                ref end_time,
            } => write!(f, "GetHistory"),
            GetUnicornTable { ref n_last_items } => write!(f, "GetUnicornTable"),
            SendPow { ref pow } => write!(f, "SendPoW"),
            SendBlock {
                ref common,
                ref mined_info,
            } => write!(f, "SendBlock"),
            Store {
                ref incoming_contract,
            } => write!(f, "Store"),
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

/// Encapsulates miner requests
#[derive(Serialize, Deserialize, Clone)]
pub enum MineRequest {
    SendBlock {
        block: Vec<u8>,
        reward: TokenAmount,
    },
    SendRandomNum {
        rnum: Vec<u8>,
        win_coinbases: Vec<String>,
    },
    SendPartitionList {
        p_list: Vec<ProofOfWork>,
    },
    SendBlockchainItem {
        key: String,
        item: BlockchainItem,
    },
    SendTransactions {
        tx_merkle_verification: Vec<String>,
    },
    Closing,
}

impl fmt::Debug for MineRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MineRequest::*;

        match *self {
            SendBlockchainItem { ref key, ref item } => write!(f, "SendBlockchainItem"),
            SendBlock {
                ref block,
                ref reward,
            } => write!(f, "SendBlock"),
            SendRandomNum {
                ref rnum,
                ref win_coinbases,
            } => write!(f, "SendRandomNum"),
            SendPartitionList { ref p_list } => write!(f, "SendPartitionList"),
            SendTransactions {
                ref tx_merkle_verification,
            } => write!(f, "SendTransactions"),
            Closing => write!(f, "Closing"),
        }
    }
}

pub trait MinerInterface {
    /// Receive a specific block as requested from storage node.
    ///
    /// ### Arguments
    ///
    /// * `block` - The received block.
    fn receive_blockchain_item(
        &mut self,
        peer: SocketAddr,
        key: String,
        item: BlockchainItem,
    ) -> Response;
}

///============ COMPUTE NODE ============///

/// Encapsulates compute requests & responses.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Clone)]
pub enum ComputeRequest {
    SendUtxoRequest {
        address: Option<String>,
    },
    SendBlockStored(BlockStoredInfo),
    SendPoW {
        block_num: u64,
        nonce: Vec<u8>,
        coinbase: Transaction,
    },
    SendPartitionEntry {
        partition_entry: ProofOfWork,
    },
    SendTransactions {
        transactions: Vec<Transaction>,
    },
    SendPartitionRequest,
    SendUserBlockNotificationRequest,
    Closing,
    SendRaftCmd(RaftMessageWrapper),
}

impl fmt::Debug for ComputeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ComputeRequest::*;

        match *self {
            SendUtxoRequest { ref address } => write!(f, "SendUtxoRequest"),
            SendBlockStored(ref _info) => write!(f, "SendBlockStored"),
            SendPoW {
                ref block_num,
                ref nonce,
                ref coinbase,
            } => write!(f, "SendPoW({})", block_num),
            SendPartitionEntry {
                ref partition_entry,
            } => write!(f, "SendPartitionEntry"),
            SendTransactions { ref transactions } => write!(f, "SendTransactions"),
            SendUserBlockNotificationRequest => write!(f, "SendUserBlockNotificationRequest"),
            SendPartitionRequest => write!(f, "SendPartitionRequest"),
            Closing => write!(f, "Closing"),
            SendRaftCmd(_) => write!(f, "SendRaftCmd"),
        }
    }
}

pub trait ComputeInterface {
    ///Fetch UTXO set for given addresses
    fn fetch_utxo_set(&mut self, peer: SocketAddr, address: Option<String>) -> Response;

    /// Partitions a set of provided UUIDs for key creation/agreement
    /// TODO: Figure out the correct return type
    fn partition(&self, uuids: Vec<&'static str>) -> Response;

    /// Returns the internal service level data
    fn get_service_levels(&self) -> Response;

    /// Receives transactions to be bundled into blocks
    ///
    /// ### Arguments
    ///
    /// * `transactions` - Transactions to be added into blocks.
    fn receive_transactions(&mut self, transactions: Vec<Transaction>) -> Response;

    /// Executes a received and approved contract
    ///
    /// ### Arguments
    ///
    /// * `contract` - Contract struct containing the contract to be executed.
    fn execute_contract(&self, contract: Contract) -> Response;

    /// Returns the next block reward value
    fn get_next_block_reward(&self) -> f64;
}

///============ USER NODE ============///

/// Encapsulates user requests
#[derive(Deserialize, Serialize, Clone)]
pub enum UserRequest {
    SendUtxoSet {
        utxo_set: Vec<u8>,
    },
    SendAddressRequest {
        amount: TokenAmount,
    },
    SendPaymentAddress {
        address: String,
        amount: TokenAmount,
    },
    SendPaymentTransaction {
        transaction: Transaction,
    },
    BlockMining {
        block: Block,
    },
    Closing,
}

impl fmt::Debug for UserRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use UserRequest::*;

        match *self {
            SendUtxoSet { .. } => write!(f, "SendUtxoSet"),
            SendAddressRequest { .. } => write!(f, "SendAddressRequest"),
            SendPaymentAddress { .. } => write!(f, "SendPaymentAddress"),
            SendPaymentTransaction { .. } => write!(f, "SendPaymentTransaction"),
            BlockMining { .. } => write!(f, "BlockMining"),
            Closing => write!(f, "Closing"),
        }
    }
}

pub trait UseInterface {
    /// Receives a request for a new payment address to be produced
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer who made the request
    /// * `amount`  - The amount the payment will be
    fn receive_payment_address_request(
        &mut self,
        peer: SocketAddr,
        amount: TokenAmount,
    ) -> Response;

    /// Receive the requested UTXO set from Compute
    ///
    /// ### Arguments
    ///
    /// * `utxo_set` - The requested UTXO set
    fn receive_utxo_set(&mut self, utxo_set: Vec<u8>) -> Response;
}
