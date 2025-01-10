use crate::api::handlers::{self, DbgPaths, health_check};
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
        .or(health_check(dp, routes_pow, api_keys, cache))
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
        .or(health_check(dp, routes_pow, api_keys, cache))
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
        .or(health_check(dp, routes_pow, api_keys, cache))
}