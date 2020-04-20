#![allow(unused)]
use crate::primitives::block::Block;
use crate::primitives::transaction::Transaction;
use crate::unicorn::UnicornShard;
use serde::{Deserialize, Serialize};
use std::fmt;
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
#[derive(Deserialize, Serialize, Debug)]
pub enum NodeType {
    Miner,
    Storage,
    Compute,
}

/// Handshake request that peers send when they connect to someone
#[derive(Deserialize, Serialize, Debug)]
pub struct HandshakeRequest {
    pub node_type: NodeType,
}

/// Encapsulates storage requests
#[derive(Deserialize, Serialize, Clone)]
pub enum StorageRequest {
    GetHistory { start_time: u64, end_time: u64 },
    GetUnicornTable { n_last_items: Option<u64> },
    SendPow { pow: ProofOfWork },
    SendPreBlock { pre_block: Block },
    Store { incoming_contract: Contract },
}

impl fmt::Debug for StorageRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StorageRequest::*;

        match *self {
            GetHistory {
                ref start_time,
                ref end_time,
            } => write!(f, "GetHistory"),
            GetUnicornTable { ref n_last_items } => write!(f, "GetUnicornTable"),
            SendPow { ref pow } => write!(f, "SendPoW"),
            SendPreBlock { ref pre_block } => write!(f, "SendPreBlock"),
            Store {
                ref incoming_contract,
            } => write!(f, "Store"),
        }
    }
}

pub trait StorageInterface {
    /// Creates a new instance of a Store implementor
    fn new(address: SocketAddr) -> Self;

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

    /// Receives the new pre-block from the ComputeInterface Ring
    ///
    /// ### Arguments
    ///
    /// * `pre_block`   - The pre-block to be stored and checked
    fn receive_pre_block(&mut self, pre_block: Block) -> Response;

    /// Receives agreed contracts for storage
    ///
    /// ### Arguments
    ///
    /// * `contract`    - Contract to store
    fn receive_contracts(&self, contract: Contract) -> Response;
}

/// Encapsulates compute requests
#[derive(Serialize, Deserialize, Clone)]
pub enum ComputeRequest {
    SendPoW { pow: Vec<u8> },
    SendTx { tx: Vec<Transaction> },
}

impl fmt::Debug for ComputeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ComputeRequest::*;

        match *self {
            SendPoW { ref pow } => write!(f, "SendPoW"),
            SendTx { ref tx } => write!(f, "SendTx"),
        }
    }
}

pub trait ComputeInterface {
    /// Generates a new compute node instance
    ///
    /// ### Arguments
    ///
    /// * `address` - Address for the current compute node
    fn new(address: SocketAddr) -> Self;

    /// Receives a PoW for inclusion in the UnicornShard build
    ///
    /// ### Arguments
    ///
    /// * `address` - address for the peer providing the PoW
    /// * `pow`     - PoW for potential inclusion
    fn receive_pow(&mut self, peer: SocketAddr, pow: Vec<u8>) -> Response;

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
    fn receive_transactions(&self, transactions: Vec<Transaction>) -> Response;

    /// Executes a received and approved contract
    fn execute_contract(&self, contract: Contract) -> Response;

    /// Returns the next block reward value
    fn get_next_block_reward(&self) -> f64;
}

pub trait MinerInterface {
    /// Creates a new instance of Mining implementor
    ///
    /// ### Arguments
    ///
    /// * `comms_address`   - endpoint address used for communications
    fn new(comms_address: SocketAddr) -> Self;

    /// Receives a new block to be mined
    ///
    /// ### Arguments
    ///
    /// * `pre_block` - New block to be mined
    fn receive_pre_block(&self, pre_block: &Block) -> Response;
}

/// Encapsulates storage requests
#[derive(Deserialize, Serialize, Clone)]
pub enum UserRequest {
    AdvertiseContract {
        contract: Contract,
        peers: Vec<SocketAddr>,
    },
    SendAssets {
        assets: Vec<Asset>,
    },
}

impl fmt::Debug for UserRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use UserRequest::*;

        match *self {
            AdvertiseContract {
                ref contract,
                ref peers,
            } => write!(f, "AdvertiseContract"),
            SendAssets { ref assets } => write!(f, "SendAsset"),
        }
    }
}

pub trait UseInterface {
    /// Creates a new instance of Mining implementor
    ///
    /// ### Arguments
    ///
    /// * `address` - Socket address of self
    /// * `network` - Network to join
    fn new(address: SocketAddr, network: usize) -> Self;

    /// Checks an advertised contract with a set of peers
    ///
    /// ### Arguments
    ///
    /// * `contract`    - Contract to check
    /// * `peers`       - Peers with whom the contract is arranged
    fn check_contract<UserNode>(&self, contract: Contract, peers: Vec<UserNode>) -> Response;

    /// Receives an asset/s, which could be tokens or data assets
    ///
    /// ### Arguments
    ///
    /// * `assets`  - Assets to receive
    fn receive_assets(&mut self, assets: Vec<Asset>) -> Response;
}
