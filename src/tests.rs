//! Test suite for the network functions.

use crate::configurations::{
    MempoolNodeSharedConfig, TxOutSpec, UserAutoGenTxSetup, UtxoSetSpec, WalletTxSpec,
};
use crate::constants::{NETWORK_VERSION, SANC_LIST_TEST};
use crate::interfaces::{
    BlockStoredInfo, BlockchainItem, BlockchainItemMeta, BlockchainItemType, CommonBlockInfo,
    DruidPool, MempoolApi, MempoolRequest, MinedBlock, MinedBlockExtraInfo, Response,
    StorageRequest, StoredSerializingBlock, UserApiRequest, UserRequest, UtxoFetchType, UtxoSet,
    WinningPoWInfo,
};
use crate::mempool::MempoolNode;
use crate::mempool_raft::MinerWhitelist;
use crate::miner::MinerNode;
use crate::storage::{all_ordered_stored_block_tx_hashes, StorageNode};
use crate::storage_raft::CompleteBlock;
use crate::test_utils::{
    generate_rb_transactions, get_test_tls_spec, map_items, node_join_all_checked,
    remove_all_node_dbs, Network, NetworkConfig, NodeType, RbReceiverData, RbSenderData,
};
use crate::tracked_utxo::TrackedUtxoBalance;
use crate::transactor::Transactor;
use crate::user::UserNode;
use crate::utils::{
    apply_mining_tx, calculate_reward, construct_coinbase_tx, construct_valid_block_pow_hash,
    create_valid_transaction_with_ins_outs, decode_pub_key, decode_secret_key,
    generate_pow_for_block, get_sanction_addresses, tracing_log_try_init, LocalEvent, StringError,
};
use bincode::{deserialize, deserialize_from};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error, error_span, info};
use tracing_futures::Instrument;
use tw_chain::crypto::sha3_256;
use tw_chain::crypto::sign_ed25519 as sign;
use tw_chain::crypto::sign_ed25519::{PublicKey, SecretKey};
use tw_chain::primitives::asset::{Asset, AssetValues, TokenAmount};
use tw_chain::primitives::block::{Block, BlockHeader};
use tw_chain::primitives::druid::DruidExpectation;
use tw_chain::primitives::transaction::{GenesisTxHashSpec, OutPoint, Transaction, TxOut};
use tw_chain::script::StackEntry;
use tw_chain::utils::transaction_utils::{
    construct_address, construct_item_create_tx, construct_tx_hash,
    construct_tx_in_signable_asset_hash, get_tx_out_with_out_point_cloned,
};
use mio::Events;
use linked_hash_map::LinkedHashMap;

const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);

#[cfg(not(debug_assertions))] // Release
const TEST_DURATION_DIVIDER: usize = 10;
#[cfg(debug_assertions)] // Debug
const TEST_DURATION_DIVIDER: usize = 1;

const SEED_UTXO: &[(i32, &str)] = &[
    (1, "00000000000000000000000000000000"),
    (3, "00000000000000000000000000000001"),
    (1, "00000000000000000000000000000002"),
];
const VALID_TXS_IN: &[(i32, &str)] = &[
    (0, "00000000000000000000000000000000"),
    (0, "00000000000000000000000000000001"),
    (1, "00000000000000000000000000000001"),
];
const VALID_TXS_OUT: &[&str] = &[
    "00000000000000000000000000000101",
    "00000000000000000000000000000102",
    "00000000000000000000000000000103",
];
const DEFAULT_SEED_AMOUNT: TokenAmount = TokenAmount(3);

const BLOCK_RECEIVED: &str = "Block received to be added";
const BLOCK_STORED: &str = "Block complete stored";
const BLOCK_RECEIVED_AND_STORED: [&str; 2] = [BLOCK_RECEIVED, BLOCK_STORED];

const SOME_PUB_KEYS: [&str; 3] = [
    COMMON_PUB_KEY,
    "6e86cc1fc5efbe64c2690efbb966b9fe1957facc497dce311981c68dac88e08c",
    "8b835e00c57ebff6637ec32276f2c6c0df71129c8f0860131a78a4692a0b59dc",
];

const SOME_SEC_KEYS: [&str; 3] = [
    COMMON_SEC_KEY,
    "3053020101300506032b65700422042070391d510eb988291d2dca15e5b8a54c552b4f2361bf29b1b945300a3e7cc9b4a1230321006e86cc1fc5efbe64c2690efbb966b9fe1957facc497dce311981c68dac88e08c",
    "3053020101300506032b6570042204201842082f6d8b0ecf75a309544980027e15ed1c00e95a14063705b2a4fc358670a1230321008b835e00c57ebff6637ec32276f2c6c0df71129c8f0860131a78a4692a0b59dc",
];

const SOME_PUB_KEY_ADDRS: [&str; 3] = [
    COMMON_PUB_ADDR,
    "77516e2d91606250e625546f86702510d2e893e4a27edfc932fdba03c955cc1b",
    "4cfd64a6692021fc417368a866d33d94e1c806747f61ac85e0b3935e7d5ed925",