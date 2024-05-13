use crate::api::handlers::{self, DbgPaths};
use crate::api::utils::{
    auth_request, create_new_cache, handle_rejection, map_api_res_and_cache, warp_path,
    with_node_component, ReplyCache, CACHE_LIVE_TIME,
};
use crate::comms_handler::Node;
use crate::db_utils::SimpleDb;
use crate::interfaces::{MempoolApi, UserApi};
use crate::miner::CurrentBlockWithMutex;
use crate::threaded_call::ThreadedCallSender;
use crate::utils::{ApiKeys, RoutesPoWInfo};
use crate::wallet::WalletDb;
use std::sync::{Arc, Mutex};

use warp::{Filter, Rejection, Reply};

//======= GET ROUTES =======//

// GET CORS
pub fn get_cors() -> warp::cors::Builder {
    warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "Accept",
            "User-Agent",
            "Sec-Fetch-Mode",
            "Referer",
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Headers",
            "Content-Type",
            "x-cache-id",
            "x-request-id",
            "x-nonce",
            "x-api-key",
        ])
        .allow_methods(vec!["GET", "OPTIONS"])
}

// GET wallet info
pub fn wallet_info(
    dp: &mut DbgPaths,
    db: WalletDb,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "wallet_info";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(
            warp::path::param::<String>()
                .map(Some)
                .or_else(|_| async { Ok::<(Option<String>,), std::convert::Infallible>((None,)) }),
        )
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, ei, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_wallet_info(db, ei, route, call_id),
            )
        })
        .with(get_cors())
}

// GET all keypairs
// TODO: Requires password (will move to POST)
pub fn export_keypairs(
    dp: &mut DbgPaths,
    db: WalletDb,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "export_keypairs";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_export_keypairs(db, route, call_id),
            )
        })
        .with(get_cors())
}

// GET new payment address
pub fn payment_address(
    dp: &mut DbgPaths,
    db: WalletDb,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "payment_address";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_payment_address(db, route, call_id),
            )
        })
        .with(get_cors())
}

// GET latest block
pub fn latest_block(
    dp: &mut DbgPaths,
    db: Arc<Mutex<SimpleDb>>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "latest_block";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_latest_block(db, route, call_id),
            )
        })
        .with(get_cors())
}

// GET debug data
pub fn debug_data(
    mut dp: DbgPaths,
    node: Node,
    aux_node: Option<Node>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "debug_data";
    warp_path(&mut dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow.clone(), api_keys))
        .and(with_node_component(dp))
        .and(with_node_component(node))
        .and(with_node_component(aux_node))
        .and(with_node_component(routes_pow))
        .and(with_node_component(cache))
        .and_then(
            move |call_id: String, dp, node, aux, routes_pow: RoutesPoWInfo, cache| {
                let routes = routes_pow.lock().unwrap().clone();
                map_api_res_and_cache(
                    call_id.clone(),
                    cache,
                    handlers::get_debug_data(dp, node, aux, route, call_id, routes),
                )
            },
        )
        .with(get_cors())
}

// GET current block being mined
pub fn current_mining_block(
    dp: &mut DbgPaths,
    current_block: CurrentBlockWithMutex,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "current_mining_block";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(current_block))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, cb, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_current_mining_block(cb, route, call_id),
            )
        })
        .with(get_cors())
}

// GET total supply in the system. Can be pulled directly from the blockchain
pub fn total_supply(
    dp: &mut DbgPaths,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "total_supply";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_total_supply(route, call_id),
            )
        })
        .with(get_cors())
}

// GET issued supply of tokens
pub fn issued_supply(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "issued_supply";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, tc, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_issued_supply(tc, route, call_id),
            )
        })
        .with(get_cors())
}

// GET UTXO set addresses
pub fn utxo_addresses(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "utxo_addresses";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, a, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_utxo_addresses(a, route, call_id),
            )
        })
        .with(get_cors())
}

// GET current config for node
pub fn get_shared_config(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "get_shared_config";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(cache))
        .and(with_node_component(threaded_calls))
        .and_then(move |call_id: String, cache, tc| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_shared_config_mempool(tc, route, call_id),
            )
        })
        .with(get_cors())
}

/// GET last constructed transaction
pub fn get_outgoing_txs(
    dp: &mut DbgPaths,
    db: WalletDb,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "outgoing_transactions";
    warp_path(dp, route)
        .and(warp::get())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::get_outgoing_txs(route, db, call_id),
            )
        })
        .with(get_cors())
}

//======= POST ROUTES =======//

// POST CORS
pub fn post_cors() -> warp::cors::Builder {
    warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "Accept",
            "User-Agent",
            "Sec-Fetch-Mode",
            "Referer",
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Headers",
            "Content-Type",
            "x-cache-id",
            "x-request-id",
            "x-nonce",
            "x-api-key",
        ])
        .allow_methods(vec!["POST", "OPTIONS"])
}

// POST get db item by key
pub fn blockchain_entry_by_key(
    dp: &mut DbgPaths,
    db: Arc<Mutex<SimpleDb>>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "blockchain_entry";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_blockchain_entry_by_key(db, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST get block information by number
pub fn block_by_num(
    dp: &mut DbgPaths,
    db: Arc<Mutex<SimpleDb>>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "block_by_num";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_block_by_num(db, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST get block information by number
pub fn transactions_by_key(
    dp: &mut DbgPaths,
    db: Arc<Mutex<SimpleDb>>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "transactions_by_key";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_transactions_by_key(db, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST save keypair
// TODO: Requires password
pub fn import_keypairs(
    dp: &mut DbgPaths,
    db: WalletDb,
    node: Node,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "import_keypairs";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(node))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, node, kp, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_import_keypairs(node, db, kp, route, call_id),
            )
        })
        .with(post_cors())
}

// POST make payment
pub fn make_payment(
    dp: &mut DbgPaths,
    db: WalletDb,
    node: Node,
    threaded_calls: ThreadedCallSender<dyn UserApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "make_payment";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(node))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, node, tc, pi, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_make_payment(db, node, tc, pi, route, call_id),
            )
        })
        .with(post_cors())
}

// POST make payment
pub fn make_ip_payment(
    dp: &mut DbgPaths,
    db: WalletDb,
    node: Node,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "make_ip_payment";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(with_node_component(node))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, node, pi, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_make_ip_payment(db, node, pi, route, call_id),
            )
        })
        .with(post_cors())
}

// POST request donation payment
pub fn request_donation(
    dp: &mut DbgPaths,
    node: Node,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "request_donation";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(node))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, node, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_request_donation(node, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST transaction status
pub fn transaction_status(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "transaction_status";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, tc, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_transaction_status(tc, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST update running total
pub fn update_running_total(
    dp: &mut DbgPaths,
    node: Node,
    db: WalletDb,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "update_running_total";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(node))
        .and(with_node_component(db))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, node, db: WalletDb, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_update_running_total(node, db, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST fetch balance for addresses
pub fn fetch_balance(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "fetch_balance";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, tc, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_fetch_utxo_balance(tc, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST fetch balance for addresses
pub fn fetch_pending(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "fetch_pending";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, tc, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_fetch_druid_pending(tc, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST create item-based asset transaction
pub fn create_item_asset(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "create_item_asset";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, tc, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_create_item_asset(tc, info, route, call_id),
            )
        })
        .with(post_cors())
}

/// POST create a item-based asset transaction on user
pub fn create_item_asset_user(
    dp: &mut DbgPaths,
    node: Node,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "create_item_asset";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(node))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, node, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_create_item_asset_user(node, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST change passphrase
pub fn change_passphrase(
    dp: &mut DbgPaths,
    db: WalletDb,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "change_passphrase";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_change_wallet_passphrase(db, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST create transactions
pub fn create_transactions(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone {
    let route = "create_transactions";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, tc, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_create_transactions(tc, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST serialize transactions
pub fn serialize_transactions(
    dp: &mut DbgPaths,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone {
    let route = "serialize_transactions";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_serialize_transactions(info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST deserialize transactions
pub fn deserialize_transactions(
    dp: &mut DbgPaths,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone {
    let route = "deserialize_transactions";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_deserialize_transactions(info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST check for address presence
pub fn blocks_by_tx_hashes(
    dp: &mut DbgPaths,
    db: Arc<Mutex<SimpleDb>>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone {
    let route = "check_transaction_presence";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(db))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, db, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_blocks_by_tx_hashes(db, info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST construct payment address
pub fn address_construction(
    dp: &mut DbgPaths,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "address_construction";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and_then(move |call_id: String, info, cache| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::post_payment_address_construction(info, route, call_id),
            )
        })
        .with(post_cors())
}

// POST pause nodes
pub fn pause_nodes(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "pause_nodes";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(cache))
        .and(with_node_component(threaded_calls))
        .and(warp::body::json())
        .and_then(move |call_id: String, cache, tc, b_num| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::pause_nodes(tc, route, call_id, b_num),
            )
        })
        .with(post_cors())
}

// POST resume nodes
pub fn resume_nodes(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "resume_nodes";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(with_node_component(cache))
        .and(with_node_component(threaded_calls))
        .and_then(move |call_id: String, cache, tc| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::resume_nodes(tc, route, call_id),
            )
        })
        .with(post_cors())
}

// POST update config in a coordinated manner, sharing it to peers
pub fn update_shared_config(
    dp: &mut DbgPaths,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    routes_pow: RoutesPoWInfo,
    api_keys: ApiKeys,
    cache: ReplyCache,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let route = "update_shared_config";
    warp_path(dp, route)
        .and(warp::post())
        .and(auth_request(routes_pow, api_keys))
        .and(warp::body::json())
        .and(with_node_component(cache))
        .and(with_node_component(threaded_calls))
        .and_then(move |call_id: String, shared_config, cache, tc| {
            map_api_res_and_cache(
                call_id.clone(),
                cache,
                handlers::update_shared_config(tc, shared_config, route, call_id),
            )
        })
        .with(post_cors())
}

//======= NODE ROUTES =======//
//TODO: Nodes share similar routes; We need to find a way to reduce ambiguity

// API routes for User nodes
pub fn user_node_routes(
    api_keys: ApiKeys,
    routes_pow_info: RoutesPoWInfo,
    db: WalletDb,
    node: Node,
    threaded_calls: ThreadedCallSender<dyn UserApi>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let mut dp_vec = DbgPaths::new();
    let dp = &mut dp_vec;
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let routes = wallet_info(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    )
    .or(get_outgoing_txs(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(make_payment(
        dp,
        db.clone(),
        node.clone(),
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(make_ip_payment(
        dp,
        db.clone(),
        node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(request_donation(
        dp,
        node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(export_keypairs(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(import_keypairs(
        dp,
        db.clone(),
        node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(update_running_total(
        dp,
        node.clone(),
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(create_item_asset_user(
        dp,
        node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(payment_address(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(change_passphrase(
        dp,
        db,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    // .or(address_construction(
    //     dp,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(serialize_transactions(
        dp,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(deserialize_transactions(
        dp,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(debug_data(
        dp_vec,
        node,
        None,
        routes_pow_info,
        api_keys,
        cache,
    ));

    routes.recover(handle_rejection)
}

// API routes for Storage nodes
pub fn storage_node_routes(
    api_keys: ApiKeys,
    routes_pow_info: RoutesPoWInfo,
    db: Arc<Mutex<SimpleDb>>,
    node: Node,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let mut dp_vec = DbgPaths::new();
    let dp = &mut dp_vec;
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let routes = block_by_num(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    )
    // .or(transactions_by_key(
    //     dp,
    //     db.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(latest_block(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(blockchain_entry_by_key(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    // .or(blocks_by_tx_hashes(
    //     dp,
    //     db,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(address_construction(
    //     dp,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(debug_data(
        dp_vec,
        node,
        None,
        routes_pow_info,
        api_keys,
        cache,
    ));

    routes.recover(handle_rejection)
}

// API routes for Mempool nodes
// TODO: 1. `fetch_pending` should not return `Transaction` as it contains sensitive information (`Script`)
// TODO: 2. `fetch_pending` should return sensible data once a proper use-case has been found
pub fn mempool_node_routes(
    api_keys: ApiKeys,
    routes_pow_info: RoutesPoWInfo,
    threaded_calls: ThreadedCallSender<dyn MempoolApi>,
    node: Node,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let mut dp_vec = DbgPaths::new();
    let dp = &mut dp_vec;
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let routes = fetch_balance(
        dp,
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    )
    .or(create_item_asset(
        dp,
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(create_transactions(
        dp,
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(total_supply(
        dp,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(issued_supply(
        dp,
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(transaction_status(
        dp,
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    // .or(utxo_addresses(
    //     dp,
    //     threaded_calls.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(address_construction(
    //     dp,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(pause_nodes(
    //     dp,
    //     threaded_calls.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(resume_nodes(
    //     dp,
    //     threaded_calls.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(update_shared_config(
    //     dp,
    //     threaded_calls.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(get_shared_config(
    //     dp,
    //     threaded_calls,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(debug_data(
        dp_vec,
        node,
        None,
        routes_pow_info,
        api_keys,
        cache,
    ));

    routes.recover(handle_rejection)
}

// API routes for Miner nodes
pub fn miner_node_routes(
    api_keys: ApiKeys,
    routes_pow_info: RoutesPoWInfo,
    current_block: CurrentBlockWithMutex,
    db: WalletDb,
    node: Node,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let mut dp_vec = DbgPaths::new();
    let dp = &mut dp_vec;
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let routes = wallet_info(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    )
    .or(export_keypairs(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(import_keypairs(
        dp,
        db.clone(),
        node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(payment_address(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(change_passphrase(
        dp,
        db,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(current_mining_block(
        dp,
        current_block,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    // .or(address_construction(
    //     dp,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(debug_data(
        dp_vec,
        node,
        None,
        routes_pow_info,
        api_keys,
        cache,
    ));

    routes.recover(handle_rejection)
}

// API routes for Miner nodes with User node capabilities
pub fn miner_node_with_user_routes(
    api_keys: ApiKeys,
    routes_pow_info: RoutesPoWInfo,
    current_block: CurrentBlockWithMutex,
    db: WalletDb, /* Shared WalletDb */
    miner_node: Node,
    threaded_calls: ThreadedCallSender<dyn UserApi>,
    user_node: Node, /* Additional User `Node` */
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let mut dp_vec = DbgPaths::new();
    let dp = &mut dp_vec;
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let routes = wallet_info(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    )
    .or(make_payment(
        dp,
        db.clone(),
        user_node.clone(),
        threaded_calls.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(get_outgoing_txs(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    // .or(make_ip_payment(
    //     dp,
    //     db.clone(),
    //     user_node.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    // .or(request_donation(
    //     dp,
    //     user_node.clone(),
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(export_keypairs(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(import_keypairs(
        dp,
        db.clone(),
        user_node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(update_running_total(
        dp,
        user_node.clone(),
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(create_item_asset_user(
        dp,
        user_node.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(payment_address(
        dp,
        db.clone(),
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(change_passphrase(
        dp,
        db,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    .or(current_mining_block(
        dp,
        current_block,
        routes_pow_info.clone(),
        api_keys.clone(),
        cache.clone(),
    ))
    // .or(address_construction(
    //     dp,
    //     routes_pow_info.clone(),
    //     api_keys.clone(),
    //     cache.clone(),
    // ))
    .or(debug_data(
        dp_vec,
        miner_node,
        Some(user_node),
        routes_pow_info,
        api_keys,
        cache,
    ));

    routes.recover(handle_rejection)
}
