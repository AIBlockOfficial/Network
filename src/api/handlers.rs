use bincode::deserialize;
use naom::constants::D_DISPLAY_PLACES;
use naom::primitives::asset::TokenAmount;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use warp;

use crate::constants::{FUND_KEY, WALLET_PATH};
use crate::interfaces::UserRequest;
use crate::user::UserNode;
use crate::utils::get_db_options;
use crate::wallet::FundStore;

/// Information about a wallet to be returned to requester
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalletInfo {
    running_total: f64,
}

/// Information about a payee to be returned to requester
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PayeeInfo {
    address: String,
}

/// Gets the state of the connected wallet and returns it.
/// Returns a `WalletInfo` struct
pub async fn get_wallet_info() -> Result<impl warp::Reply, &'static str> {
    let opts = get_db_options();
    let db = DB::open(&opts, WALLET_PATH).unwrap();
    let fund_store_state = match db.get(FUND_KEY) {
        Ok(Some(list)) => Some(deserialize(&list).unwrap()),
        Ok(None) => return Err("No funds available for payment!"),
        Err(_) => return Err("Failed to access the wallet database"),
    };

    // At this point a valid fund store must exist
    let fund_store: FundStore = fund_store_state.unwrap();
    let amount_to_send = fund_store.running_total.0 as f64 / D_DISPLAY_PLACES;

    // Final fund info to send
    let send_val = WalletInfo {
        running_total: amount_to_send,
    };

    Ok(warp::reply::json(&send_val))
}

/// Makes a payment to a specific address
pub async fn make_payment(
    address: String,
    amount: f64,
    node: &mut UserNode,
) -> Result<impl warp::Reply, &'static str> {
    let amount_to_set = amount * D_DISPLAY_PLACES;
    node.amount = TokenAmount(amount_to_set as u64);
    let dummy_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    let _ = node.handle_request(
        dummy_addr,
        UserRequest::SendPaymentAddress { address: address },
    );

    get_wallet_info().await
}
