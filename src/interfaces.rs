#![allow(unused)]
use crate::unicorn::UnicornShard;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;

use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;

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
    pub nonce: Vec<u8>,
    pub block: Block,
}

impl ProofOfWorkBlock {
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

///============ STORAGE NODE ============///

/// Encapsulates storage requests
#[derive(Deserialize, Serialize, Clone)]
pub enum StorageRequest {
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
        block: Block,
        tx: BTreeMap<String, Transaction>,
    },
    Store {
        incoming_contract: Contract,
    },
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
            SendBlock { ref block, ref tx } => write!(f, "SendBlock"),
            Store {
                ref incoming_contract,
            } => write!(f, "Store"),
        }
    }
}

pub trait StorageInterface {
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

    /// Receives the new block from the miner with permissions to write
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer that the block is received from
    /// * `block`   - The block to be stored and checked
    /// * `tx`      - The transactions in the block
    fn receive_block(
        &mut self,
        peer: SocketAddr,
        block: Block,
        tx: BTreeMap<String, Transaction>,
    ) -> Response;

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
    SendBlock { block: Vec<u8> },
    SendRandomNum { rnum: Vec<u8> },
    SendPartitionList { p_list: Vec<ProofOfWork> },
    NotifyBlockFound { win_coinbase: Option<String> },
}

impl fmt::Debug for MineRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MineRequest::*;

        match *self {
            SendBlock { ref block } => write!(f, "SendBlock"),
            SendRandomNum { ref rnum } => write!(f, "SendRandomNum"),
            SendPartitionList { ref p_list } => write!(f, "SendPartitionList"),
            NotifyBlockFound { ref win_coinbase } => write!(f, "NotifyBlockFound"),
        }
    }
}

pub trait MinerInterface {
    /// Receives a new block to be mined
    ///
    /// ### Arguments
    ///
    /// * `pre_block` - New block to be mined
    fn receive_pre_block(&mut self, pre_block: Vec<u8>) -> Response;
}

///============ COMPUTE NODE ============///

/// Encapsulates compute requests & responses.
#[derive(Serialize, Deserialize, Clone)]
pub enum ComputeRequest {
    SendPoW {
        pow: ProofOfWorkBlock,
        coinbase: Transaction,
    },
    SendPartitionEntry {
        partition_entry: ProofOfWork,
    },
    SendTransactions {
        transactions: BTreeMap<String, Transaction>,
    },
    SendPartitionRequest,
}

impl fmt::Debug for ComputeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ComputeRequest::*;

        match *self {
            SendPoW {
                ref pow,
                ref coinbase,
            } => write!(f, "SendPoW"),
            SendPartitionEntry {
                ref partition_entry,
            } => write!(f, "SendPartitionEntry"),
            SendTransactions { ref transactions } => write!(f, "SendTransactions"),
            SendPartitionRequest => write!(f, "SendPartitionRequest"),
        }
    }
}

pub trait ComputeInterface {
    /// Receives a PoW for block inclusion
    /// TODO: Coinbase amount currently hardcoded to 12. Make dynamic
    ///
    /// ### Arguments
    ///
    /// * `address`         - address for the peer providing the PoW
    /// * `pow`             - PoW for potential inclusion
    /// * `coinbase`        - Coinbase tx to validate
    fn receive_pow(
        &mut self,
        peer: SocketAddr,
        pow: ProofOfWorkBlock,
        coinbase: Transaction,
    ) -> Response;

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
    fn receive_transactions(&mut self, transactions: BTreeMap<String, Transaction>) -> Response;

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

///============ USER NODE ============///

/// Encapsulates user requests
#[derive(Deserialize, Serialize, Clone)]
pub enum UserRequest {
    AdvertiseContract {
        contract: Contract,
        peers: Vec<SocketAddr>,
    },
    SendPaymentAddress {
        address: String,
    },
    SendPaymentTransaction {
        transaction: Transaction,
    },
    SendAddressRequest,
}

impl fmt::Debug for UserRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use UserRequest::*;

        match *self {
            AdvertiseContract {
                ref contract,
                ref peers,
            } => write!(f, "AdvertiseContract"),
            SendPaymentAddress { ref address } => write!(f, "SendPaymentAddress"),
            SendPaymentTransaction { ref transaction } => write!(f, "SendPaymentTransaction"),
            SendAddressRequest => write!(f, "SendAddressRequest"),
        }
    }
}

pub trait UseInterface {
    /// Checks an advertised contract with a set of peers
    ///
    /// ### Arguments
    ///
    /// * `contract`    - Contract to check
    /// * `peers`       - Peers with whom the contract is arranged
    fn check_contract<UserNode>(&self, contract: Contract, peers: Vec<UserNode>) -> Response;

    /// Receives a request for a new payment address to be produced
    fn receive_payment_address_request(&self) -> Response;
}
