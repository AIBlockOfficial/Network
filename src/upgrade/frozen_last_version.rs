use crate::db_utils::SimpleDbSpec;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};

pub mod constants {
    // This points to the previous DB version
    pub const NETWORK_VERSION: u32 = 4;
    pub const NETWORK_VERSION_SERIALIZED: Option<&[u8]> = Some(b"4");
    pub const DB_PATH: &str = "src/db/db";
    pub const WALLET_PATH: &str = "src/wallet/wallet";
}

pub mod tw_chain {
    use super::*;

    //
    // Block
    //
    pub type UtxoSet = BTreeMap<OutPoint, TxOut>;
    pub type PublicKey = Vec<u8>;
    pub type SecretKey = Vec<u8>;
    pub type Signature = Vec<u8>;
    pub type Nonce = Vec<u8>;
    pub type Salt = Vec<u8>;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Block {
        pub header: BlockHeader,
        pub transactions: Vec<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct BlockHeader {
        pub version: u32,
        pub bits: usize,
        pub nonce_and_mining_tx_hash: (Vec<u8>, String),
        pub b_num: u64,
        pub seed_value: Vec<u8>, // for commercial
        pub previous_hash: Option<String>,
        pub txs_merkle_root_and_hash: (String, String),
    }

    //
    // Transaction
    //

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Transaction {
        pub inputs: Vec<TxIn>,
        pub outputs: Vec<TxOut>,
        pub version: usize,
        pub druid_info: Option<DdeValues>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DdeValues {
        pub druid: String,
        pub participants: usize,
        pub expectations: Vec<DruidExpectation>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DruidExpectation {
        pub from: String,
        pub to: String,
        pub asset: Asset,
    }

    //
    // TxIn
    //

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct TxIn {
        pub previous_out: Option<OutPoint>,
        pub script_signature: Script,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct OutPoint {
        pub t_hash: String,
        pub n: i32,
    }

    #[derive(Clone, Debug, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Script {
        pub stack: Vec<StackEntry>,
    }

    #[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub enum StackEntry {
        Op(OpCodes),
        Signature(Signature),
        PubKey(PublicKey),
        PubKeyHash(String),
        Num(usize),
        Bytes(String),
    }

    /// Ops code for stack scripts
    #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
    pub enum OpCodes {
        OP_DUP = 0x2b,
        OP_HASH256 = 0x5d,
        OP_EQUALVERIFY = 0x3d,
        OP_CHECKSIG = 0x5f,
        OP_CREATE = 0x6d,
        OP_HASH256_V0 = 0x6e,
        OP_HASH256_TEMP = 0x6f,
    }

    impl Serialize for OpCodes {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            (*self as u32).serialize(s)
        }
    }

    impl<'a> Deserialize<'a> for OpCodes {
        fn deserialize<D: Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
            let value: u32 = Deserialize::deserialize(deserializer)?;
            match value {
                0x2b => Ok(Self::OP_DUP),
                0x5d => Ok(Self::OP_HASH256),
                0x3d => Ok(Self::OP_EQUALVERIFY),
                0x5f => Ok(Self::OP_CHECKSIG),
                0x6d => Ok(Self::OP_CREATE),
                0x6e => Ok(Self::OP_HASH256_V0),
                0x6f => Ok(Self::OP_HASH256_TEMP),
                v => Err(serde::de::Error::custom(format!("Unkown OpCodes x{v:02X}"))),
            }
        }
    }

    //
    // TxOut
    //

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct TxOut {
        pub value: Asset,
        pub locktime: u64,
        pub drs_block_hash: Option<String>,
        pub script_public_key: Option<String>,
    }

    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    pub enum Asset {
        Token(TokenAmount),
        Item(ItemAsset),
    }

    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    pub struct ItemAsset {
        pub amount: u64,
        pub genesis_hash: Option<String>,
        pub metadata: Option<String>,
    }

    #[derive(
        Deserialize, Serialize, Default, Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord,
    )]
    pub struct TokenAmount(pub u64);
}

pub mod interfaces {
    use super::tw_chain::{Block, Transaction};
    use super::*;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct StoredSerializingBlock {
        pub block: Block,
    }

    #[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    pub struct BlockStoredInfo {
        pub block_hash: String,
        pub block_num: u64,
        pub nonce: Vec<u8>,
        pub mining_transactions: BTreeMap<String, Transaction>,
        pub shutdown: bool,
    }
}

pub mod raft_store {
    pub const HARDSTATE_KEY: &str = "HardStateKey";
    pub const SNAPSHOT_DATA_KEY: &str = "SnaphotDataKey";
    pub const SNAPSHOT_META_KEY: &str = "SnaphotMetaKey";
    pub const ENTRY_KEY: &str = "EntryKey";
    pub const LAST_ENTRY_KEY: &str = "LastEntryKey";
}

pub mod mempool {
    use super::*;

    /// Key for local miner list
    pub const REQUEST_LIST_KEY: &str = "RequestListKey";
    pub const USER_NOTIFY_LIST_KEY: &str = "UserNotifyListKey";
    pub const RAFT_KEY_RUN: &str = "RaftKeyRun";

    /// Database columns
    pub const DB_COL_INTERNAL: &str = "internal";
    pub const DB_COL_LOCAL_TXS: &str = "local_transactions";

    // New but compatible with 1.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".mempool",
        columns: &[DB_COL_INTERNAL, DB_COL_LOCAL_TXS],
    };
}

pub mod mempool_raft {
    use super::block_pipeline::*;
    use super::tw_chain::*;
    use super::*;

    // Only serialize the UtxoSet
    pub type TrackedUtxoSet = UtxoSet;

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".mempool_raft",
        columns: &[],
    };

    /// Stub AccumulatingBlockStoredInfo that should not be present in upgrade
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AccumulatingBlockStoredInfo {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum SpecialHandling {
        Shutdown,
        FirstUpgradeBlock,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, PartialEq, Eq)]
    pub enum CoordinatedCommand {
        PauseNodes { b_num: u64 },
        ResumeNodes,
        ApplySharedConfig,
    }

    /// All fields that are consensused between the RAFT group.
    /// These fields need to be written and read from a committed log event.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct MempoolConsensused {
        pub unanimous_majority: usize,
        pub sufficient_majority: usize,
        pub partition_full_size: usize,
        pub tx_pool: BTreeMap<String, Transaction>,
        pub tx_druid_pool: Vec<BTreeMap<String, Transaction>>,
        pub tx_current_block_previous_hash: Option<String>,
        pub initial_utxo_txs: Option<BTreeMap<String, Transaction>>,
        pub utxo_set: TrackedUtxoSet,
        pub current_block_stored_info:
            BTreeMap<Vec<u8>, (AccumulatingBlockStoredInfo, BTreeSet<u64>)>,
        pub current_raft_coordinated_cmd_stored_info: BTreeMap<CoordinatedCommand, BTreeSet<u64>>,
        pub last_committed_raft_idx_and_term: (u64, u64),
        pub current_issuance: TokenAmount,
        pub block_pipeline: MiningPipelineInfo,
        pub last_mining_transaction_hashes: Vec<String>,
        pub special_handling: Option<SpecialHandling>,
    }
}

pub mod unicorn {
    use super::*;
    use crate::utils::rug_integer;
    use rug::Integer;

    #[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
    pub struct Unicorn {
        pub iterations: u64,
        pub security_level: u32,
        #[serde(with = "rug_integer")]
        pub seed: Integer,
        #[serde(with = "rug_integer")]
        pub modulus: Integer,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
    pub struct UnicornFixedParam {
        pub modulus: String,
        pub iterations: u64,
        pub security: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
    pub struct UnicornInfo {
        pub unicorn: Unicorn,
        pub g_value: String,
        #[serde(with = "rug_integer")]
        pub witness: Integer,
    }
}

pub mod raft {
    use super::*;
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct RaftContextKey {
        pub proposer_id: u64,
        pub proposer_run: u64,
        pub proposal_id: u64,
    }
}

pub mod block_pipeline {
    use std::net::SocketAddr;

    use super::tw_chain::*;
    use super::unicorn::UnicornInfo;
    use super::*;

    /// Different states of the mining pipeline
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    pub enum MiningPipelineStatus {
        Halted,
        ParticipantOnlyIntake,
        AllItemsIntake,
    }

    /// Participants collection (unsorted: given order, and lookup collection)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Participants {
        unsorted: Vec<SocketAddr>,
        lookup: BTreeSet<SocketAddr>,
    }

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct WinningPoWInfo {
        pub nonce: Vec<u8>,
        pub mining_tx: (String, Transaction),
        pub p_value: u8,
        pub d_value: u8,
    }

    /// Rolling info particular to a specific mining pipeline
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MiningPipelineInfo {
        pub participants_intake: BTreeMap<u64, Participants>,
        pub participants_mining: BTreeMap<u64, Participants>,
        pub empty_participants: Participants,
        pub last_winning_hashes: BTreeSet<String>,
        pub all_winning_pow: Vec<(SocketAddr, WinningPoWInfo)>,
        pub unicorn_info: UnicornInfo,
        pub winning_pow: Option<(SocketAddr, WinningPoWInfo)>,
        pub mining_pipeline_status: MiningPipelineStatus,
        pub current_phase_timeout_peer_ids: BTreeSet<u64>,
        pub current_phase_reset_pipeline_peer_ids: BTreeSet<u64>,
        pub unicorn_fixed_param: unicorn::UnicornFixedParam,
        pub current_block_num: Option<u64>,
        pub current_block: Option<Block>,
        pub current_block_tx: BTreeMap<String, Transaction>,
        pub current_reward: TokenAmount,
        pub proposed_keys: BTreeSet<raft::RaftContextKey>,
    }
}

pub mod storage {
    use super::*;

    /// Key storing current proposer run
    pub const RAFT_KEY_RUN: &str = "RaftKeyRun";
    pub const LAST_CONTIGUOUS_BLOCK_KEY: &str = "LastContiguousBlockKey";

    /// Database columns
    pub const DB_COL_INTERNAL: &str = "internal";
    pub const DB_COL_BC_ALL: &str = "block_chain_all";
    pub const DB_COL_BC_NAMED: &str = "block_chain_named";
    pub const DB_COL_BC_META: &str = "block_chain_meta";
    pub const DB_COL_BC_JSON: &str = "block_chain_json";
    pub const DB_COL_BC_V0_6_0: &str = "block_chain_v0.6.0";
    pub const DB_COL_BC_V0_5_0: &str = "block_chain_v0.5.0";
    pub const DB_COL_BC_V0_4_0: &str = "block_chain_v0.4.0";
    pub const DB_COL_BC_V0_3_0: &str = "block_chain_v0.3.0";
    pub const DB_COL_BC_V0_2_0: &str = "block_chain_v0.2.0";

    /// Version columns
    pub const DB_COLS_BC: &[(&str, u32)] = &[
        (DB_COL_BC_V0_6_0, 4),
        (DB_COL_BC_V0_5_0, 3),
        (DB_COL_BC_V0_4_0, 2),
        (DB_COL_BC_V0_3_0, 1),
        (DB_COL_BC_V0_2_0, 0),
    ];
    pub const DB_POINTER_SEPARATOR: u8 = b':';

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".storage",
        columns: &[
            DB_COL_INTERNAL,
            DB_COL_BC_ALL,
            DB_COL_BC_NAMED,
            DB_COL_BC_META,
            DB_COL_BC_JSON,
            DB_COL_BC_V0_6_0,
            DB_COL_BC_V0_5_0,
            DB_COL_BC_V0_4_0,
            DB_COL_BC_V0_3_0,
            DB_COL_BC_V0_2_0,
        ],
    };
}

pub mod storage_raft {
    use super::interfaces::BlockStoredInfo;
    use super::*;

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".storage_raft",
        columns: &[],
    };

    /// Stub CompleteBlockBuilder that should not be present in upgrade
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct CompleteBlockBuilder {}

    /// All fields that are consensused between the RAFT group.
    /// These fields need to be written and read from a committed log event.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct StorageConsensused {
        pub sufficient_majority: usize,
        pub current_block_num: u64,
        pub current_block_completed_parts: BTreeMap<Vec<u8>, CompleteBlockBuilder>,
        pub last_committed_raft_idx_and_term: (u64, u64),
        pub last_block_stored: Option<BlockStoredInfo>,
    }
}

pub mod wallet {
    use super::tw_chain::{
        Asset, Nonce, OutPoint, PublicKey, Salt, SecretKey, TokenAmount, Transaction,
    };
    use super::*;

    pub const KNOWN_ADDRESS_KEY: &str = "a";
    pub const FUND_KEY: &str = "f";
    pub const MASTER_KEY_STORE_KEY: &str = "MasterKeyStore";
    pub const TX_GENERATOR_KEY: &str = "TxGeneratorKey";
    pub const LAST_COINBASE_KEY: &str = "LastCoinbaseKey";
    pub const MINING_ADDRESS_KEY: &str = "MiningAddressKey";

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::WALLET_PATH,
        suffix: "",
        columns: &[],
    };

    pub type WalletSavedTransactions = BTreeMap<OutPoint, Asset>;

    pub type Pending