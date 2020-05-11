#![allow(unused)]
use crate::key_creation::PeerInfo;
use crate::unicorn::UnicornShard;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;

/// A placeholder struct for sensible feedback
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub success: bool,
    pub reason: &'static str,
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
    pub address: String,
    pub nonce: Vec<u8>,
    pub block: Vec<u8>,
    pub coinbase: Vec<u8>,
}

impl ProofOfWorkBlock {
    pub fn new() -> Self {
        ProofOfWorkBlock {
            address: "".to_string(),
            nonce: Vec::new(),
            block: Vec::new(),
            coinbase: Vec::new(),
        }
    }
}

/// A placeholder tx struct
#[derive(Debug, Serialize, Deserialize)]
pub struct Tx;

/// A placeholder Block struct
#[derive(Debug, Serialize, Deserialize)]
pub struct Block;

/// A placeholder Contract struct
#[derive(Debug, Serialize, Deserialize)]
pub struct Contract;

/// A placeholder Heat struct
pub struct Heat;

/// A placeholder Asset struct
pub struct Asset;

/// Denotes existing node types
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Miner,
    Storage,
    Compute,
}

/// Token to uniquely identify messages.
pub type Token = u64;

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
        /// Type of node that's trying to establish a connection.
        node_type: NodeType,
        /// Publicly available socket address of the node that can be used for inbound connections.
        public_address: SocketAddr,
    },
    /// Handshake response containing contacts of ring members.
    HandshakeResponse { contacts: Vec<SocketAddr> },
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

/// Encapsulates storage requests
#[derive(Deserialize, Serialize, Debug)]
pub enum StorageRequest {
    GetHistory { start_time: u64, end_time: u64 },
    GetUnicornTable { n_last_items: Option<u64> },
    Pow { hash: f64 },
    PreBlock { pre_block: Block },
    Store { contract: Contract },
}

pub trait StorageInterface {
    /// Creates a new instance of a Store implementor
    fn new() -> Self;

    /// Returns a read only section of a stored history.
    /// Time slices are considered to be block IDs (u64).
    fn get_history(&self, start_time: &u64, end_time: &u64) -> Option<Vec<Tx>>;

    /// Whitelists the supplied UUID for edit permissions.
    fn whitelist(&self, uuid: &'static str) -> Response;

    /// Provides the UnicornShard table, with an optional section slice.
    /// Not providing a section slice will return the entire table.
    fn get_unicorn_table(&self, n_last_items: Option<u64>) -> Option<Vec<f64>>;

    /// Receives a PoW to match against the current pre-block.
    /// TODO: Hash needs to be a BigInt
    fn receive_pow(&self, hash: &f64) -> Response;

    /// Receives the new pre-block from the ComputeInterface Ring
    fn receive_pre_block(&self, pre_block: Block) -> Response;

    /// Receives agreed contracts for storage
    fn receive_contracts(&self, contract: Contract) -> Response;
}

/// Encapsulates miner requests
#[derive(Serialize, Deserialize, Clone)]
pub enum MineRequest {
    SendBlock { block: Vec<u8> },
    SendRandomNum { rnum: Vec<u8> },
    SendPartitionList { p_list: Vec<ProofOfWork> },
}

impl fmt::Debug for MineRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MineRequest::*;

        match *self {
            SendBlock { ref block } => write!(f, "SendBlock"),
            SendRandomNum { ref rnum } => write!(f, "SendRandomNum"),
            SendPartitionList { ref p_list } => write!(f, "SendPartitionList"),
        }
    }
}

/// Encapsulates compute requests
#[derive(Serialize, Deserialize, Clone)]
pub enum ComputeRequest {
    SendPoW { pow: ProofOfWorkBlock },
    SendPartitionEntry { partition_entry: ProofOfWork },
    SendPartitionRequest,
}

impl fmt::Debug for ComputeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ComputeRequest::*;

        match *self {
            SendPoW { ref pow } => write!(f, "SendPoW"),
            SendPartitionEntry {
                ref partition_entry,
            } => write!(f, "SendPartitionEntry"),
            SendPartitionRequest => write!(f, "SendPartitionRequest"),
        }
    }
}

pub trait ComputeInterface {
    /// Receives a PoW for inclusion in the UnicornShard build
    ///
    /// ### Arguments
    ///
    /// * `address` - address for the peer providing the PoW
    /// * `pow`     - PoW for potential inclusion
    fn receive_pow(&mut self, peer: SocketAddr, pow: ProofOfWorkBlock) -> Response;

    /// Receives a PoW commit for UnicornShard creation
    ///
    /// ### Arguments
    ///
    /// * `address` - address for the peer providing the PoW
    /// * `commit`  - PoW commit for potential inclusion
    fn receive_commit(&mut self, peer: SocketAddr, commit: ProofOfWork) -> Response;

    /// Returns the internal unicorn table
    fn get_unicorn_table(&self) -> Vec<UnicornShard>;

    /// Partitions a set of provided UUIDs for key creation/agreement
    /// TODO: Figure out the correct return type
    fn partition(&self, uuids: Vec<&'static str>) -> Response;

    /// Returns the internal service level data
    fn get_service_levels(&self) -> Response;

    /// Receives transactions to be bundled into blocks
    fn receive_transactions(&self, transactions: Vec<Tx>) -> Response;

    /// Executes a received and approved contract
    fn execute_contract(&self, contract: Contract) -> Response;

    /// Returns the next block reward value
    fn get_next_block_reward(&self) -> f64;

    /// Validates a PoW
    ///
    /// ### Arguments
    ///
    /// * `pow` - PoW to validate
    fn validate_pow(pow: &mut ProofOfWork) -> bool;
}

pub trait MinerInterface {
    /// Receives a new block to be mined
    ///
    /// ### Arguments
    ///
    /// * `pre_block` - New block to be mined
    fn receive_pre_block(&mut self, pre_block: Vec<u8>) -> Response;
}

pub trait UseInterface {
    /// Creates a new instance of Mining implementor
    fn new() -> Self;

    /// Advertise a contract with a set of peers
    fn advertise_contract<UserNode>(&self, contract: Contract, peers: Vec<UserNode>) -> Response;

    /// Receives an asset/s, which could be tokens or data assets
    fn receive_assets(assets: Vec<Asset>) -> Response;
}
