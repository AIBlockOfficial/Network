use bincode::deserialize;
use naom::constants::D_DISPLAY_PLACES;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use warp;

use crate::api::errors;
use crate::constants::{FUND_KEY, WALLET_PATH};
use crate::db_utils::get_db_options;
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
pub async fn get_wallet_info() -> Result<impl warp::Reply, warp::Rejection> {
    let opts = get_db_options();
    let db = DB::open(&opts, WALLET_PATH).unwrap();
    let fund_store_state = match db.get(FUND_KEY) {
        Ok(Some(list)) => Some(deserialize(&list).unwrap()),
        Ok(None) => return Err(warp::reject::custom(errors::ErrorLackOfFunds)),
        Err(_) => return Err(warp::reject::custom(errors::ErrorCannotAccessWallet)),
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
