use crate::api::errors::ApiErrorType;
use crate::api::responses::{json_embed, json_embed_block, json_embed_transaction, json_serialize_embed, APIAsset, APICreateResponseContent, CallResponse, JsonReply};
use crate::api::utils::{map_string_err, map_to_string_err};
use crate::comms_handler::Node;
use crate::configurations::MempoolNodeSharedConfig;
use crate::constants::LAST_BLOCK_HASH_KEY;
use crate::db_utils::SimpleDb;
use crate::interfaces::{node_type_as_str, AddressesWithOutPoints, BlockchainItem, BlockchainItemMeta, BlockchainItemType, DebugData, DruidPool, MempoolApi, MineApiRequest, MineRequest, NodeType, OutPointData, StoredSerializingBlock, UserApi, UserApiRequest, UserRequest, UtxoFetchType};
use crate::mempool::MempoolError;
use crate::miner::{BlockPoWReceived, CurrentBlockWithMutex};
use crate::storage::{get_stored_value_from_db, indexed_block_hash_key};
use crate::threaded_call::{self, ThreadedCallSender};
use crate::utils::{decode_pub_key, decode_signature, StringError};
use crate::wallet::{AddressStore, AddressStoreHex, WalletDb, WalletDbError};
use crate::Response;
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{fmt, str};
use tracing::{debug, error};
use tw_chain::constants::{D_DISPLAY_PLACES, TOTAL_TOKENS};
use tw_chain::crypto::sign_ed25519::{PublicKey, Signature};
use tw_chain::primitives::asset::{Asset, ItemAsset, TokenAmount};
use tw_chain::primitives::druid::DdeValues;
use tw_chain::primitives::transaction::{GenesisTxHashSpec, OutPoint, Transaction, TxIn, TxOut};
use tw_chain::script::lang::Script;
use tw_chain::script::{OpCodes, StackEntry};
use tw_chain::utils::transaction_utils::{construct_address_for, construct_tx_hash};
use warp::hyper::StatusCode;
use warp::Reply;

/// Health check handler
pub async fn health_check() -> Result<impl Reply, warp::Rejection> {
    Ok(warp::reply::json(&json!({ "status": "ok" })))
}

pub type DbgPaths = Vec<&'static str>;

/// Data entry from the blockchain
#[derive(Debug, Serialize, Deserialize)]
enum BlockchainData {
    Block(StoredSerializingBlock),
    Transaction(Transaction),
}

/// Private/public keypairs, stored with payment address as key.
/// Values are encrypted
#[derive(Debug, Serialize, Deserialize)]
pub struct Addresses {
    pub addresses: BTreeMap<String, AddressStoreHex>,
}

/// Information about a wallet to be returned to requester
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalletInfo {
    running_total: f64,
    running_total_tokens: u64,
    locked_total: f64,
    locked_total_tokens: u64,
    available_total: f64,
    available_total_tokens: u64,
    item_total: BTreeMap<String, u64>, /* DRS tx hash - amount */
    addresses: AddressesWithOutPoints,
}

/// Encapsulated payment received from client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncapsulatedPayment {
    pub address: String,
    pub amount: TokenAmount,
    pub passphrase: String,
    pub locktime: Option<u64>,
}

/// Item asset creation structure received from client
///
/// This structure is used to create a item asset on EITHER
/// the mempool or user node.
#[derive(Debug, Clone