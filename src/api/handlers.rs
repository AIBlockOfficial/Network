use crate::api::errors;
use crate::comms_handler::Node;
use crate::interfaces::UserRequest;
use crate::wallet::WalletDb;
use naom::constants::D_DISPLAY_PLACES;
use naom::primitives::asset::TokenAmount;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::error;

/// Private/public keypairs, stored with payment address as key.
/// Values are encrypted
#[derive(Debug, Serialize, Deserialize)]
pub struct Addresses {
    addresses: BTreeMap<String, Vec<u8>>,
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
pub async fn get_wallet_keypairs(wallet_db: WalletDb) -> Result<impl warp::Reply, warp::Rejection> {
    let known_addr = wallet_db.get_known_addresses();
    let mut addresses = BTreeMap::new();

    for addr in known_addr {
        addresses.insert(addr.clone(), wallet_db.get_address_store_encrypted(&addr));
    }

    Ok(warp::reply::json(&Addresses { addresses }))
}

//======= POST HANDLERS =======//

/// Post to import new keypairs to the connected wallet
pub async fn post_import_keypairs(
    db: WalletDb,
    keypairs: Addresses,
) -> Result<impl warp::Reply, warp::Rejection> {
    for (addr, address_set) in keypairs.addresses.iter() {
        match db
            .save_encrypted_address_to_wallet(addr.clone(), address_set.clone())
            .await
        {
            Ok(_) => {}
            Err(_e) => {
                return Err(warp::reject::custom(
                    errors::ErrorCannotSaveAddressesToWallet,
                ))
            }
        }
    }

    Ok(warp::reply::json(&"Key/s saved successfully".to_owned()))
}

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
