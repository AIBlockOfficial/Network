use crate::api::handlers;
use crate::comms_handler::Node;
use crate::db_utils::SimpleDb;
use crate::miner::{BlockPoWReceived, CurrentBlockWithMutex};
use crate::tracked_utxo::TrackedUtxoSet;
use crate::wallet::WalletDb;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::{self, Filter, Rejection, Reply};

fn with_node_component<T: Clone + Send>(
    comp: T,
) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || comp.clone())
}

//======= GET ROUTES =======//

// GET CORS
fn get_cors() -> warp::cors::Builder {
    warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["*"])
        .allow_methods(vec!["GET"])
}

// GET wallet info
pub fn wallet_info(db: WalletDb) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("wallet_info")
        .and(warp::get())
        .and(with_node_component(db))
        .and_then(handlers::get_wallet_info)
        .with(get_cors())
}

// GET all keypairs
// TODO: Requires password (will move to POST)
pub fn export_keypairs(
    db: WalletDb,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("export_keypairs")
        .and(warp::get())
        .and(with_node_component(db))
        .and_then(handlers::get_export_keypairs)
        .with(get_cors())
}

// GET new payment address
pub fn payment_address(
    db: WalletDb,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("new_payment_address")
        .and(warp::get())
        .and(with_node_component(db))
        .and_then(handlers::get_new_payment_address)
        .with(get_cors())
}

// GET latest block
pub fn latest_block(
    db: Arc<Mutex<SimpleDb>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("latest_block")
        .and(warp::get())
        .and(with_node_component(db))
        .and_then(handlers::get_latest_block)
        .with(get_cors())
}

// GET debug data
pub fn debug_data(
    node: Node,
    aux_node: Option<Node>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("debug_data")
        .and(warp::get())
        .and(with_node_component(node))
        .and(with_node_component(aux_node))
        .and_then(handlers::get_debug_data)
        .with(get_cors())
}

// GET current block being mined
pub fn current_mining_block(
    current_block: Arc<Mutex<Option<BlockPoWReceived>>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("current_mining_block")
        .and(warp::get())
        .and(with_node_component(current_block))
        .and_then(handlers::get_current_mining_block)
        .with(get_cors())
}

//======= POST ROUTES =======//

// POST CORS
fn post_cors() -> warp::cors::Builder {
    warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "User-Agent",
            "Sec-Fetch-Mode",
            "Referer",
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
            "Access-Control-Allow-Origin",
            "Content-Type",
        ])
        .allow_methods(vec!["POST"])
}

// POST get db item by key
pub fn blockchain_entry_by_key(
    db: Arc<Mutex<SimpleDb>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("blockchain_entry_by_key")
        .and(warp::post())
        .and(with_node_component(db))
        .and(warp::body::json())
        .and_then(handlers::post_blockchain_entry_by_key)
        .with(post_cors())
}

// POST get block information by number
pub fn block_info_by_nums(
    db: Arc<Mutex<SimpleDb>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("block_by_num")
        .and(warp::post())
        .and(with_node_component(db))
        .and(warp::body::json())
        .and_then(handlers::post_block_by_num)
        .with(post_cors())
}

// POST save keypair
// TODO: Requires password
pub fn import_keypairs(
    db: WalletDb,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("import_keypairs")
        .and(warp::post())
        .and(with_node_component(db))
        .and(warp::body::json())
        .and_then(handlers::post_import_keypairs)
        .with(post_cors())
}

// POST make payment
pub fn make_payment(
    db: WalletDb,
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("make_payment")
        .and(warp::post())
        .and(with_node_component(db))
        .and(with_node_component(node))
        .and(warp::body::json())
        .and_then(handlers::post_make_payment)
        .with(post_cors())
}

// POST make payment
pub fn make_ip_payment(
    db: WalletDb,
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("make_ip_payment")
        .and(warp::post())
        .and(with_node_component(db))
        .and(with_node_component(node))
        .and(warp::body::json())
        .and_then(handlers::post_make_ip_payment)
        .with(post_cors())
}

// POST request donation payment
pub fn request_donation(
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("request_donation")
        .and(warp::post())
        .and(with_node_component(node))
        .and(warp::body::json())
        .and_then(handlers::post_request_donation)
        .with(post_cors())
}

// POST update running total
pub fn update_running_total(
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("update_running_total")
        .and(warp::post())
        .and(with_node_component(node))
        .and(warp::body::json())
        .and_then(handlers::post_update_running_total)
        .with(post_cors())
}

// POST fetch balance for addresses
pub fn fetch_utxo_balance(
    tracked_utxo: Arc<Mutex<TrackedUtxoSet>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("fetch_balance")
        .and(warp::post())
        .and(with_node_component(tracked_utxo))
        .and(warp::body::json())
        .and_then(handlers::post_fetch_utxo_balance)
        .with(post_cors())
}

// POST create receipt-based asset transaction
pub fn create_receipt_asset(
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("create_receipt_asset")
        .and(warp::post())
        .and(with_node_component(node))
        .and(warp::body::json())
        .and_then(handlers::post_create_receipt_asset)
        .with(post_cors())
}

// POST change passphrase
pub fn change_passphrase(
    db: WalletDb,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path("change_passphrase")
        .and(warp::post())
        .and(with_node_component(db))
        .and(warp::body::json())
        .and_then(handlers::post_change_wallet_passphrase)
        .with(post_cors())
}

// POST signable transaction
pub fn signable_transactions(
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("signable_transactions")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(handlers::post_signable_transactions)
        .with(post_cors())
}

// POST create transactions
pub fn create_transactions(
    peer: Node,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("create_transactions")
        .and(warp::post())
        .and(with_node_component(peer))
        .and(warp::body::json())
        .and_then(handlers::post_create_transactions)
        .with(post_cors())
}

// POST check for address presence
pub fn blocks_by_tx_hashes(
    db: Arc<Mutex<SimpleDb>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("block_by_tx_hashes")
        .and(warp::post())
        .and(with_node_component(db))
        .and(warp::body::json())
        .and_then(handlers::post_blocks_by_tx_hashes)
        .with(post_cors())
}

// POST construct payment address
pub fn payment_address_construction() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone
{
    warp::path("address_construction")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(handlers::post_payment_address_construction)
        .with(post_cors())
}

//======= NODE ROUTES =======//
//TODO: Nodes share similar routes; We need to find a way to reduce ambiguity

// API routes for User nodes
pub fn user_node_routes(
    db: WalletDb,
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    wallet_info(db.clone())
        .or(make_payment(db.clone(), node.clone()))
        .or(make_ip_payment(db.clone(), node.clone()))
        .or(request_donation(node.clone()))
        .or(export_keypairs(db.clone()))
        .or(import_keypairs(db.clone()))
        .or(update_running_total(node.clone()))
        .or(create_receipt_asset(node.clone()))
        .or(payment_address(db.clone()))
        .or(change_passphrase(db))
        .or(debug_data(node, None))
        .or(payment_address_construction())
}

// API routes for Storage nodes
pub fn storage_node_routes(
    db: Arc<Mutex<SimpleDb>>,
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    block_info_by_nums(db.clone())
        .or(latest_block(db.clone()))
        .or(blockchain_entry_by_key(db.clone()))
        .or(blocks_by_tx_hashes(db))
        .or(debug_data(node, None))
        .or(payment_address_construction())
}

// API routes for Compute nodes
pub fn compute_node_routes(
    tracked_utxo: Arc<Mutex<TrackedUtxoSet>>,
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    fetch_utxo_balance(tracked_utxo)
        .or(create_receipt_asset(node.clone()))
        .or(create_transactions(node.clone()))
        .or(debug_data(node, None))
        .or(payment_address_construction())
}

// API routes for Miner nodes
pub fn miner_node_routes(
    current_block: CurrentBlockWithMutex,
    db: WalletDb,
    node: Node,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    current_mining_block(current_block)
        .or(debug_data(node, None))
        .or(change_passphrase(db.clone()))
        .or(export_keypairs(db.clone()))
        .or(import_keypairs(db.clone()))
        .or(wallet_info(db.clone()))
        .or(payment_address(db))
        .or(payment_address_construction())
}

// API routes for Miner nodes with User node capabilities
pub fn miner_node_with_user_routes(
    db: WalletDb, /* Shared WalletDb */
    current_block: CurrentBlockWithMutex,
    miner_node: Node,
    user_node: Node, /* Additional User `Node` */
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    current_mining_block(current_block)
        .or(debug_data(miner_node, Some(user_node.clone())))
        .or(change_passphrase(db.clone()))
        .or(export_keypairs(db.clone()))
        .or(import_keypairs(db.clone()))
        .or(wallet_info(db.clone()))
        .or(payment_address(db.clone()))
        .or(make_payment(db.clone(), user_node.clone()))
        .or(make_ip_payment(db, user_node.clone()))
        .or(request_donation(user_node.clone()))
        .or(update_running_total(user_node.clone()))
        .or(create_receipt_asset(user_node))
}
