use crate::api::handlers;
use crate::comms_handler::Node;
use crate::wallet::WalletDb;
use std::convert::Infallible;
use warp::{self, Filter};

fn with_peer(peer: &Node) -> impl Filter<Extract = (&Node,), Error = Infallible> {
    warp::any().map(move || peer)
}

fn with_db(db: WalletDb) -> impl Filter<Extract = (WalletDb,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}

// GET wallet info
pub fn wallet_info(
    db: WalletDb,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("wallet_info")
        .and(warp::get())
        .and(with_db(db))
        .and_then(handlers::get_wallet_info)
}
