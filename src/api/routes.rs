use crate::api::handlers;
use crate::comms_handler::Node;
use std::convert::Infallible;
use warp::{self, Filter};

fn with_peer(peer: &Node) -> impl Filter<Extract = (&Node,), Error = Infallible> {
    warp::any().map(move || peer)
}

// GET wallet info
fn wallet_info() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("wallet_info")
        .and(warp::get())
        .and_then(handlers::get_wallet_info)
}
