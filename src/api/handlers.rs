use crate::api::errors;
use crate::comms_handler::Node;
use crate::interfaces::UserRequest;
use crate::wallet::{WalletDb, AddressStore};
use naom::constants::D_DISPLAY_PLACES;
use naom::primitives::asset::TokenAmount;
use serde::{Deserialize, Serialize};
use tracing::error;

/// All private/public keypairs to send to requester
#[derive(Debug, Serialize, Deserialize)]
struct Addresses {
    addresses: Vec<AddressStore>
}

/// Information about a wallet to be returned to requester
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalletInfo {
    running_total: f64,
}

/// Information about a payee to pay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayeeInfo {
    pub address: String,
    pub amount: TokenAmount,
}


//======= GET HANDLERS =======//

/// Gets the state of the connected wallet and returns it.
/// Returns a `WalletInfo` struct
pub async fn get_wallet_info(wallet_db: WalletDb) -> Result<impl warp::Reply, warp::Rejection> {
    let fund_store = match wallet_db.get_fund_store_err() {
        Ok(fund) => fund,
        Err(_) => return Err(warp::reject::custom(errors::ErrorCannotAccessWallet)),
    };

    let send_val = WalletInfo {
        running_total: fund_store.running_total().0 as f64 / D_DISPLAY_PLACES,
    };

    Ok(warp::reply::json(&send_val))
}

/// Gets all present keys and sends them out for export
/// TODO: This will need to be secured with encryption and a required password
pub async fn get_wallet_keypairs(wallet_db: WalletDb) -> Result<impl warp::Reply, warp::Rejection> {
    let known_addr = wallet_db.get_known_addresses();
    let addresses: Vec<AddressStore> = known_addr.iter().map(|k| wallet_db.get_address_store(k)).collect();

    Ok(warp::reply::json(&Addresses { addresses }))
}


//======= POST HANDLERS =======//

/// Post a new payment from the connected wallet.
pub async fn post_make_payment(
    peer: Node,
    payee_info: PayeeInfo,
) -> Result<impl warp::Reply, warp::Rejection> {
    let request = UserRequest::SendPaymentAddress {
        address: payee_info.address,
        amount: payee_info.amount,
    };

    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:make_payment error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotAccessUserNode));
    }

    Ok(warp::reply::json(&"Payment processing".to_owned()))
}