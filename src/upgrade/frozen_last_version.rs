use crate::db_utils::SimpleDbSpec;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey, Signature};
use std::collections::{BTreeMap, BTreeSet};

pub mod constants {
    pub const NETWORK_VERSION_SERIALIZED: Option<&[u8]> = None;
    pub const DB_PATH: &str = "src/db/db";
    pub const WALLET_PATH: &str = "src/wallet/wallet";
}

pub mod naom {
    use super::*;

    //
    // Block
    //

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct StoredSerializingBlock {
        pub block: Block,
        pub mining_tx_hash_and_nonces: BTreeMap<u64, (String, Vec<u8>)>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Block {
        pub header: BlockHeader,
        pub transactions: Vec<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct BlockHeader {
        pub version: u32,
        pub bits: usize,
        pub nonce: Vec<u8>,
        pub b_num: u64,
        pub seed_value: Vec<u8>, // for commercial
        pub previous_hash: Option<String>,
        pub merkle_root_hash: String,
    }

    //
    // Transaction
    //

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct Transaction {
        pub inputs: Vec<TxIn>,
        pub outputs: Vec<TxOut>,
        pub version: usize,
        pub druid: Option<String>,
        pub druid_participants: Option<usize>,
        pub expect_value: Option<Asset>,
        pub expect_value_amount: Option<TokenAmount>,
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
                v => Err(serde::de::Error::custom(format!(
                    "Unkown OpCodes x{:02X}",
                    v
                ))),
            }
        }
    }

    //
    // TxOut
    //

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct TxOut {
        pub value: Option<Asset>,
        pub amount: TokenAmount,
        pub locktime: u64,
        pub drs_block_hash: Option<String>,
        pub drs_tx_hash: Option<String>,
        pub script_public_key: Option<String>,
    }

    #[derive(
        Deserialize, Serialize, Default, Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord,
    )]
    pub struct TokenAmount(pub u64);

    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    pub enum Asset {
        Token(TokenAmount),
        Data(Vec<u8>),
    }
}

pub mod compute {
    use super::*;

    pub const REQUEST_LIST_KEY: &str = "RequestListKey";

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".compute",
        columns: &[],
    };
}

pub mod storage {
    use super::*;

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::DB_PATH,
        suffix: ".storage",
        columns: &[],
    };
}

pub mod wallet {
    use super::naom::{OutPoint, TokenAmount};
    use super::*;

    pub const KNOWN_ADDRESS_KEY: &str = "a";
    pub const FUND_KEY: &str = "f";

    // New but compatible with 0.2.0
    pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
        db_path: constants::WALLET_PATH,
        suffix: "",
        columns: &[],
    };

    #[derive(Default, Debug, Clone, Serialize, Deserialize)]
    pub struct FundStore {
        running_total: TokenAmount,
        transactions: BTreeMap<OutPoint, TokenAmount>,
        spent_transactions: BTreeMap<OutPoint, TokenAmount>,
    }

    pub type KnownAddresses = BTreeSet<String>;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct AddressStore {
        pub public_key: PublicKey,
        pub secret_key: SecretKey,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TransactionStore {
        pub key_address: String,
    }
}

pub mod convert {
    mod old {
        pub use super::super::*;
    }
    use crate::wallet;
    use naom::primitives::{
        asset::{Asset, DataAsset, TokenAmount},
        transaction::{OutPoint, Transaction, TxIn, TxOut},
    };
    use naom::script::{lang::Script, OpCodes, StackEntry};

    pub fn convert_transaction(old: old::naom::Transaction) -> Transaction {
        Transaction {
            inputs: old.inputs.into_iter().map(convert_txin).collect(),
            outputs: old.outputs.into_iter().map(convert_txout).collect(),
            version: old.version,
            druid_info: None,
        }
    }

    pub fn convert_txin(old: old::naom::TxIn) -> TxIn {
        TxIn {
            previous_out: old.previous_out.map(convert_outpoint),
            script_signature: convert_script(old.script_signature),
        }
    }

    pub fn convert_outpoint(old: old::naom::OutPoint) -> OutPoint {
        OutPoint {
            t_hash: old.t_hash,
            n: old.n,
        }
    }

    pub fn convert_script(old: old::naom::Script) -> Script {
        Script {
            stack: old.stack.into_iter().map(convert_stack_entry).collect(),
        }
    }

    pub fn convert_stack_entry(old: old::naom::StackEntry) -> StackEntry {
        match old {
            old::naom::StackEntry::Op(v) => StackEntry::Op(convert_op_code(v)),
            old::naom::StackEntry::Signature(v) => StackEntry::Signature(v),
            old::naom::StackEntry::PubKey(v) => StackEntry::PubKey(v),
            old::naom::StackEntry::PubKeyHash(v) => StackEntry::PubKeyHash(v),
            old::naom::StackEntry::Num(v) => StackEntry::Num(v),
            old::naom::StackEntry::Bytes(v) => StackEntry::Bytes(v),
        }
    }

    pub fn convert_op_code(old: old::naom::OpCodes) -> OpCodes {
        match old {
            old::naom::OpCodes::OP_DUP => OpCodes::OP_DUP,
            old::naom::OpCodes::OP_HASH256 => OpCodes::OP_HASH256,
            old::naom::OpCodes::OP_EQUALVERIFY => OpCodes::OP_EQUALVERIFY,
            old::naom::OpCodes::OP_CHECKSIG => OpCodes::OP_CHECKSIG,
        }
    }

    pub fn convert_txout(old: old::naom::TxOut) -> TxOut {
        TxOut {
            value: convert_asset(old.value, old.amount),
            locktime: old.locktime,
            drs_block_hash: None,
            drs_tx_hash: None,
            script_public_key: old.script_public_key,
        }
    }

    pub fn convert_asset(
        old_val: Option<old::naom::Asset>,
        old_amount: old::naom::TokenAmount,
    ) -> Asset {
        match old_val {
            Some(old::naom::Asset::Token(v)) => Asset::Token(convert_token_amount(v)),
            Some(old::naom::Asset::Data(v)) => Asset::Data(DataAsset {
                data: v,
                amount: old_amount.0,
            }),
            None => Asset::Token(convert_token_amount(old_amount)),
        }
    }

    pub fn convert_token_amount(old: old::naom::TokenAmount) -> TokenAmount {
        TokenAmount(old.0)
    }

    pub fn convert_address_store(old: old::wallet::AddressStore) -> wallet::AddressStore {
        wallet::AddressStore {
            public_key: old.public_key,
            secret_key: old.secret_key,
        }
    }
}
pub use convert::*;
