use crate::api::handlers;
use crate::comms_handler::Node;
use crate::wallet::WalletDb;
use std::convert::Infallible;
use warp::{self, Filter};

fn with_peer(peer: Node) -> impl Filter<Extract = (Node,), Error = Infallible> + Clone {
    warp::any().map(move || peer.clone())
}

fn with_db(db: WalletDb) -> impl Filter<Extract = (WalletDb,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}

// GET wallet info
pub fn wallet_info(
    db: WalletDb,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["*"])
        .allow_methods(vec!["GET"]);

    warp::path("wallet_info")
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_wallet_info)
        .with(cors)
}

// POST make payment
pub fn make_payment(
    peer: Node,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let cors = warp::cors()
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
        .allow_methods(vec!["POST"]);

    warp::path("make_payment")
        .and(warp::post())
        .and(with_peer(peer))
        .and(warp::body::json())
        .and_then(handlers::make_payment)
        .with(cors)
}
