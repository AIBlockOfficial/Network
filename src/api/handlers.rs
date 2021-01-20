use crate::api::errors;
use crate::comms_handler::Node;
use crate::constants::FUND_KEY;
use crate::interfaces::UserRequest;
use crate::wallet::{FundStore, WalletDb};
use bincode::deserialize;
use naom::constants::D_DISPLAY_PLACES;
use naom::primitives::asset::TokenAmount;
use serde::{Deserialize, Serialize};
use tracing::error;

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
pub async fn get_wallet_info(wallet_db: WalletDb) -> Result<impl warp::Reply, warp::Rejection> {
    let db = wallet_db.db.lock().unwrap();

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

/// Post a new payment from the connected wallet.
pub async fn make_payment(
    peer: Node,
    address: String,
    amount: TokenAmount,
) -> Result<impl warp::Reply, warp::Rejection> {
    let request = UserRequest::SendPaymentAddress { address, amount };
    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:make_payment error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotUserNode));
    }

    Ok(warp::reply::json(&"Payment processing".to_owned()))
}
