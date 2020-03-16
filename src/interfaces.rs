use crate::unicorn::UnicornShard;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// A placeholder struct for sensible feedback
#[derive(Debug, Clone)]
pub struct Response {
    pub success: bool,
    pub reason: &'static str,
}

/// PoW structure
#[derive(Debug, Clone)]
pub struct ProofOfWork {
    pub address: &'static str,
    pub nonce: Vec<u8>,
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

/// Encapsulates compute requests
#[derive(Serialize, Deserialize, Clone)]
pub enum ComputeRequest {
    SendPoW { peer: SocketAddr, pow: Vec<u8> },
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
    fn receive_transactions(&self, transactions: Vec<Tx>) -> Response;

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

pub trait UseInterface {
    /// Creates a new instance of Mining implementor
    fn new() -> Self;

    /// Advertise a contract with a set of peers
    fn advertise_contract<UserNode>(&self, contract: Contract, peers: Vec<UserNode>) -> Response;

    /// Receives an asset/s, which could be tokens or data assets
    fn receive_assets(assets: Vec<Asset>) -> Response;
}
