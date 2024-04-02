// use crate::comms_handler::Node;
use crate::mempool_raft::MinerWhitelist;
use crate::db_utils::{CustomDbSpec, SimpleDb};
use crate::interfaces::InitialIssuance;
use crate::wallet::WalletDb;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use tw_chain::primitives::asset::TokenAmount;

pub type UtxoSetSpec = BTreeMap<String, Vec<TxOutSpec>>;

/// Configuration info for TLS
#[derive(Default, Clone, Deserialize)]
pub struct TlsSpec {
    /// Trusted names for address (trusted or not)
    pub socket_name_mapping: BTreeMap<SocketAddr, String>,
    /// Trusted certificates available
    pub pem_certificates: BTreeMap<String, String>,
    /// Private keys available to authenticate with
    pub pem_pkcs8_private_keys: BTreeMap<String, String>,
    /// Untrusted names for which to add certificate to root store (If None trust all)
    pub untrusted_names: Option<BTreeSet<String>>,
    /// Node certificate override to use for this node ignoring pem_certificates
    pub pem_certificate_override: Option<String>,
    /// Private key override to use for this node ignoring pem_pkcs8_private_keys
    pub pem_pkcs8_private_key_override: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TlsPrivateInfo {
    pub pem_certs: String,
    pub pem_pkcs8_private_keys: String,
}

impl fmt::Debug for TlsSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TlsSpec({:?}, key({:?}), cert({:?}))",
            &self.socket_name_mapping,
            self.pem_pkcs8_private_key_override,
            self.pem_certificate_override
        )
    }
}

/// Configuration info for unicorn
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnicornFixedInfo {
    /// UNICORN modulus number
    pub modulus: String,
    /// UNICORN iterations
    pub iterations: u64,
    /// UNICORN security level
    pub security: u32,
}

/// Configuration info for a TxOut
#[derive(Debug, Clone, Deserialize)]
pub struct TxOutSpec {
    /// Hex encoded public key to seed script_public_key
    pub public_key: String,
    /// Amount that this TxOut can spend
    #[serde(deserialize_with = "deserialize_token_amount")]
    pub amount: TokenAmount,
    /// Locktime for the TxOut
    pub locktime: u64,
}

/// Configuration info for a TxOut
#[derive(Debug, Clone, Deserialize)]
pub struct WalletTxSpec {
    /// OutPoint specification: "n-tx_hash"
    pub out_point: String,
    /// Hex encoded secret key
    pub secret_key: String,
    /// Hex encoded public key
    pub public_key: String,
    /// Amount for the transaction
    pub amount: u64,
    /// Version of the address for the public key to use
    pub address_version: Option<u64>,
}

/// Configuration info for a database
#[derive(Default, Debug, Copy, Clone, Deserialize, PartialEq, Eq)]
pub enum DbMode {
    #[default]
    Live,
    Test(usize),
    InMemory,
}

/// Configuration info for a node
#[derive(Debug, Clone, Deserialize)]
pub struct NodeSpec {
    pub address: String,
}

/// Configuration option for a mempool node
#[derive(Debug, Clone, Deserialize)]
pub struct MempoolNodeConfig {
    /// Index of the current node in mempool_nodes
    pub mempool_node_idx: usize,
    /// Use specific database
    pub mempool_db_mode: DbMode,
    /// Configuration for handling TLS
    pub tls_config: TlsSpec,
    /// Initial API keys
    pub api_keys: BTreeMap<String, Vec<String>>,
    /// Configuation for unicorn
    pub mempool_unicorn_fixed_param: UnicornFixedInfo,
    /// All mempool nodes addresses
    pub mempool_nodes: Vec<NodeSpec>,
    /// All storage nodes addresses: only use first
    pub storage_nodes: Vec<NodeSpec>,
    /// All user nodes addresses
    pub user_nodes: Vec<NodeSpec>,
    /// Whether mempool node will use raft or act independently (0)
    pub mempool_raft: usize,
    /// API port
    pub mempool_api_port: u16,
    /// API use TLS
    pub mempool_api_use_tls: bool,
    /// Timeout for ticking raft
    pub mempool_raft_tick_timeout: usize,
    /// Timeout duration between mining event pipelines
    pub mempool_mining_event_timeout: usize,
    /// Timeout duration between committing transactions
    pub mempool_transaction_timeout: usize,
    /// Transaction hash and TxOut info to use to seed utxo
    pub mempool_seed_utxo: UtxoSetSpec,
    /// String to use for genesis block TxIn
    pub mempool_genesis_tx_in: Option<String>,
    /// Partition full size
    pub mempool_partition_full_size: usize,
    /// Minimum miner pool size
    pub mempool_minimum_miner_pool_len: usize,
    /// Node's legal jurisdiction
    pub jurisdiction: String,
    /// Node's address sanction list
    pub sanction_list: Vec<String>,
    // Routes that require PoW validation and their corresponding difficulties
    pub routes_pow: BTreeMap<String, usize>,
    /// Backup block that given modulo result in 0
    pub backup_block_modulo: Option<u64>,
    /// Check UTXO set block modulo
    pub utxo_re_align_block_modulo: Option<u64>,
    /// Restore backup if true
    pub backup_restore: Option<bool>,
    /// Enable trigger messages to reset the pipeline when it gets stuck
    pub enable_trigger_messages_pipeline_reset: Option<bool>,
    /// Enable API-key based whitelisting for miners
    pub mempool_miner_whitelist: MinerWhitelist,
    /// Limit for the number of peers this node can have
    pub peer_limit: usize,
    /// Initial issuances
    pub initial_issuances: Vec<InitialIssuance>,
}

/// Configuration option for a mempool node that can be shared across peers
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct MempoolNodeSharedConfig {
    /// Timeout duration between mining event pipelines
    pub mempool_mining_event_timeout: usize,
    /// Partition full size
    pub mempool_partition_full_size: usize,
    /// Miner whitelisting
    pub mempool_miner_whitelist: MinerWhitelist,
}

/// Configuration option for a storage node
#[derive(Debug, Clone, Deserialize)]
pub struct StorageNodeConfig {
    /// Index of the current node in mempool_nodes
    pub storage_node_idx: usize,
    /// Use specific database
    pub storage_db_mode: DbMode,
    /// Configuration for handling TLS
    pub tls_config: TlsSpec,
    /// Initial API keys
    pub api_keys: BTreeMap<String, Vec<String>>,
    /// All mempool nodes addresses
    pub mempool_nodes: Vec<NodeSpec>,
    /// All storage nodes addresses: only use first
    pub storage_nodes: Vec<NodeSpec>,
    /// Whether storage node will use raft or act independently (0)
    pub storage_raft: usize,
    /// API port
    pub storage_api_port: u16,
    /// API use TLS
    pub storage_api_use_tls: bool,
    /// Timeout for ticking raft
    pub storage_raft_tick_timeout: usize,
    /// Timeout for fetch catchup
    pub storage_catchup_duration: usize,
    // Routes that require PoW validation and their corresponding difficulties
    pub routes_pow: BTreeMap<String, usize>,
    /// Backup block that given modulo result in 0
    pub backup_block_modulo: Option<u64>,
    /// Restore backup if true
    pub backup_restore: Option<bool>,
    /// Limit for the number of peers this node can have
    pub peer_limit: usize,
}

/// Configuration option for a storage node
#[derive(Debug, Clone, Deserialize)]
pub struct MinerNodeConfig {
    /// Socket Address of this miner node
    pub miner_address: String,
    /// Use specific database
    pub miner_db_mode: DbMode,
    /// Configuration for handling TLS
    pub tls_config: TlsSpec,
    /// Initial API keys
    pub api_keys: BTreeMap<String, Vec<String>>,
    /// Index of the mempool node to use in mempool_nodes
    pub miner_mempool_node_idx: usize,
    /// All mempool nodes addresses
    pub mempool_nodes: Vec<NodeSpec>,
    /// API port
    pub miner_api_port: u16,
    /// API use TLS
    pub miner_api_use_tls: bool,
    /// Option of the passphrase used for encryption
    pub passphrase: Option<String>,
    // Routes that require PoW validation and their corresponding difficulties
    pub routes_pow: BTreeMap<String, usize>,
    /// Backup block that given modulo result in 0
    pub backup_block_modulo: Option<u64>,
    /// Restore backup if true
    pub backup_restore: Option<bool>,
    /// When provided, all new coinbase transactions will be assigned to this address
    pub static_miner_address: Option<String>,
    /// When provided, the miner will use this API key to participate in mining
    pub mining_api_key: Option<String>,
    /// Limit for the number of peers this node can have
    pub peer_limit: usize,
    /// Aggregation limit
    pub address_aggregation_limit: Option<usize>,
}

/// Configuration option for a user node
#[derive(Debug, Clone, Deserialize)]
pub struct UserNodeConfig {
    /// Socket Address of this User node
    pub user_address: String,
    /// Use specific database
    pub user_db_mode: DbMode,
    /// Configuration for handling TLS
    pub tls_config: TlsSpec,
    /// Initial API keys
    pub api_keys: BTreeMap<String, Vec<String>>,
    /// Index of the mempool node to use in mempool_nodes
    pub user_mempool_node_idx: usize,
    /// All mempool nodes addresses
    pub mempool_nodes: Vec<NodeSpec>,
    /// API port
    pub user_api_port: u16,
    /// API use TLS
    pub user_api_use_tls: bool,
    /// Wallet seeds
    pub user_wallet_seeds: Vec<WalletTxSpec>,
    /// Option of the passphrase used for encryption
    pub passphrase: Option<String>,
    /// Will donate amount to all unkown incomming payment request.
    /// Only enable in test net for the distribution users.
    pub user_auto_donate: u64,
    /// Configuration options for auto generating transactions for test
    pub user_test_auto_gen_setup: UserAutoGenTxSetup,
    // Routes that require PoW validation and their corresponding difficulties
    pub routes_pow: BTreeMap<String, usize>,
    /// Backup block that given modulo result in 0
    pub backup_block_modulo: Option<u64>,
    /// Limit for the number of peers this node can have
    pub peer_limit: usize,
}

/// Configuration option for a pre-launch node
#[derive(Debug, Clone, Deserialize)]
pub struct PreLaunchNodeConfig {
    /// Type of node to launch
    pub node_type: PreLaunchNodeType,
    /// Index of the current node in mempool_nodes
    pub mempool_node_idx: usize,
    /// Use specific database
    pub mempool_db_mode: DbMode,
    /// Configuration for handling TLS
    pub tls_config: TlsSpec,
    /// Index of the current node in mempool_nodes
    pub storage_node_idx: usize,
    /// Use specific database
    pub storage_db_mode: DbMode,
    /// All mempool nodes addresses
    pub mempool_nodes: Vec<String>,
    /// All storage nodes addresses: only use first
    pub storage_nodes: Vec<String>,
    /// Limit for the number of peers this node can have
    pub peer_limit: usize,
}

/// Type of node in pre-launch mode
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub enum PreLaunchNodeType {
    Mempool,
    Storage,
}

/// Configuration option for setup of user node
#[derive(Default, Debug, Clone, Deserialize)]
pub struct UserAutoGenTxSetup {
    /// Transaction seeds, for each users
    pub user_initial_transactions: Vec<WalletTxSpec>,
    /// How many transaction to group in each requests
    pub user_setup_tx_chunk_size: Option<usize>,
    /// How many TxIn to have for each transactions
    pub user_setup_tx_in_per_tx: Option<usize>,
    /// How many Txin to have for that user at each round
    pub user_setup_tx_max_count: usize,
}

/// Extra params for Node construction
#[derive(Default)]
pub struct ExtraNodeParams {
    pub db: Option<SimpleDb>,
    pub raft_db: Option<SimpleDb>,
    pub wallet_db: Option<SimpleDb>,
    pub shared_wallet_db: Option<WalletDb>,
    pub custom_wallet_spec: Option<CustomDbSpec>,
    pub disable_tcp_listener: bool,
}

///Hacky deserializer to work around deserializatio error with u128
fn deserialize_token_amount<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<TokenAmount, D::Error> {
    let value: u64 = serde::Deserialize::deserialize(deserializer)?;
    Ok(TokenAmount(value))
}
