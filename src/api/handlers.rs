use crate::api::errors;
use crate::comms_handler::Node;
use crate::db_utils::SimpleDb;
use crate::interfaces::{
    node_type_as_str, ComputeApiRequest, DebugData, NodeType, StoredSerializingBlock,
    UserApiRequest, UserRequest, UtxoFetchType,
};
use crate::storage::{get_blocks_by_num, get_last_block_stored, get_stored_value_from_db};
use crate::tracked_utxo::TrackedUtxoSet;
use crate::utils::{decode_pub_key, decode_signature, DeserializedBlockchainItem};
use crate::wallet::WalletDb;
use crate::ComputeRequest;
use naom::constants::D_DISPLAY_PLACES;
use naom::primitives::asset::TokenAmount;
use naom::primitives::druid::DdeValues;
use naom::primitives::transaction::{OutPoint, Transaction, TxIn, TxOut};
use naom::script::lang::Script;
use naom::utils::transaction_utils::construct_tx_in_signable_hash;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str;
use std::sync::{Arc, Mutex};
use tracing::{error, trace};

/// Data entry from the blockchain
#[derive(Debug, Serialize, Deserialize)]
enum BlockchainData {
    Block(StoredSerializingBlock),
    Transaction(Transaction),
}

/// Private/public keypairs, stored with payment address as key.
/// Values are encrypted
#[derive(Debug, Serialize, Deserialize)]
pub struct Addresses {
    pub addresses: BTreeMap<String, Vec<u8>>,
}

/// Information about a wallet to be returned to requester
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalletInfo {
    running_total: f64,
}

/// Public key addresses received from client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicKeyAddresses {
    pub address_list: Vec<String>,
}

/// Encapsulated payment received from client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncapsulatedPayment {
    pub address: String,
    pub amount: TokenAmount,
    pub passphrase: String,
}

/// Receipt asset creation structure received from client
///
/// This structure is used to create a receipt asset on EITHER
/// the compute or user node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReceiptAssetData {
    pub receipt_amount: u64,
    pub script_public_key: Option<String>, /* Not used by user Node */
    pub public_key: Option<String>,        /* Not used by user Node */
    pub signature: Option<String>,         /* Not used by user Node */
}

/// Information needed for the creaion of TxIn script.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CreateTxInScript {
    Pay2PkH {
        /// Signed data to sign
        signed_data: String,
        /// Hex encoded signature
        signature: String,
        /// Hex encoded complete public key
        public_key: String,
    },
}

/// Information needed for the creaion of TxIn.
/// This API would change if types are modified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTxIn {
    /// The previous_out to use
    pub previous_out: Option<OutPoint>,
    /// script info
    pub script_signature: Option<CreateTxInScript>,
}

/// Information necessary for the creation of a Transaction
/// This API would change if types are modified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTransaction {
    /// String to sign in each inputs
    pub inputs: Vec<CreateTxIn>,
    pub outputs: Vec<TxOut>,
    pub version: usize,
    pub druid_info: Option<DdeValues>,
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
        running_total: fund_store.running_total().tokens.0 as f64 / D_DISPLAY_PLACES,
    };

    Ok(warp::reply::json(&send_val))
}

/// Gets all present keys and sends them out for export
pub async fn get_export_keypairs(wallet_db: WalletDb) -> Result<impl warp::Reply, warp::Rejection> {
    let known_addr = wallet_db.get_known_addresses();
    let mut addresses = BTreeMap::new();

    for addr in known_addr {
        addresses.insert(addr.clone(), wallet_db.get_address_store_encrypted(&addr));
    }

    Ok(warp::reply::json(&Addresses { addresses }))
}

/// Gets a newly generated payment address
pub async fn get_new_payment_address(
    wallet_db: WalletDb,
) -> Result<impl warp::Reply, warp::Rejection> {
    let (address, _) = wallet_db.generate_payment_address().await;

    Ok(warp::reply::json(&address))
}

/// Gets the latest block information
pub async fn get_latest_block(
    db: Arc<Mutex<SimpleDb>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let last_block = get_last_block_stored(db);
    Ok(warp::reply::json(&last_block))
}

/// Gets to update running total of connected wallet
pub async fn get_debug_data(node: Node) -> Result<impl warp::Reply, warp::Rejection> {
    let data = node.get_debug_data().await;

    Ok(warp::reply::json(&data))
}

/// Gets transactions info to sign to create a Transaction
pub async fn get_signable_transactions(
    mut data: Vec<CreateTransaction>,
) -> Result<impl warp::Reply, warp::Rejection> {
    for tx in &mut data {
        for input in &mut tx.inputs {
            if let Some(previous_out) = &input.previous_out {
                input.script_signature = Some(CreateTxInScript::Pay2PkH {
                    signed_data: construct_tx_in_signable_hash(previous_out),
                    signature: Default::default(),
                    public_key: Default::default(),
                })
            }
        }
    }

    Ok(warp::reply::json(&data))
}

//======= POST HANDLERS =======//

/// Post to retrieve an item from the blockchain db by hash key
pub async fn post_blockchain_entry_by_key(
    db: Arc<Mutex<SimpleDb>>,
    key: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    let item = match get_stored_value_from_db(db, key.as_bytes()) {
        Some(i) => i,
        None => return Err(warp::reject::custom(errors::ErrorNoDataFoundForKey)),
    };

    let des = match DeserializedBlockchainItem::from_item(&item) {
        DeserializedBlockchainItem::CurrentBlock(b, ..) => BlockchainData::Block(b),
        DeserializedBlockchainItem::CurrentTx(t, ..) => BlockchainData::Transaction(t),
        DeserializedBlockchainItem::VersionErr(err) => {
            error!("Version error: {:?}", err);
            return Err(warp::reject::custom(
                errors::ErrorDataNetworkVersionMismatch,
            ));
        }
        DeserializedBlockchainItem::SerializationErr(err) => {
            error!("Deserializing error: {:?}", err);
            return Err(warp::reject::custom(errors::ErrorCouldNotDeserializeData));
        }
    };

    Ok(warp::reply::json(&des))
}

/// Post to retrieve block information by number
pub async fn post_block_by_num(
    db: Arc<Mutex<SimpleDb>>,
    block_nums: Vec<u64>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let blocks = get_blocks_by_num(db, block_nums);

    Ok(warp::reply::json(&blocks))
}

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

///Post make a new payment from the connected wallet
pub async fn post_make_payment(
    db: WalletDb,
    peer: Node,
    encapsulated_data: EncapsulatedPayment,
) -> Result<impl warp::Reply, warp::Rejection> {
    let EncapsulatedPayment {
        address,
        amount,
        passphrase,
    } = encapsulated_data;

    let request = match db.test_passphrase(passphrase).await {
        Ok(()) => UserRequest::UserApi(UserApiRequest::MakePayment { address, amount }),
        Err(()) => return Err(warp::reject::custom(errors::ErrorInvalidPassphrase)),
    };

    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:make_payment error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotAccessUserNode));
    }

    Ok(warp::reply::json(&"Payment processing".to_owned()))
}

///Post make a new payment from the connected wallet using an ip address
pub async fn post_make_ip_payment(
    db: WalletDb,
    peer: Node,
    encapsulated_data: EncapsulatedPayment,
) -> Result<impl warp::Reply, warp::Rejection> {
    trace!("in the payment");

    let EncapsulatedPayment {
        address,
        amount,
        passphrase,
    } = encapsulated_data;

    let payment_peer: SocketAddr = address
        .parse::<SocketAddr>()
        .map_err(|_| warp::reject::custom(errors::ErrorCannotParseAddress))?;

    let request = match db.test_passphrase(passphrase).await {
        Ok(()) => UserRequest::UserApi(UserApiRequest::MakeIpPayment {
            payment_peer,
            amount,
        }),
        Err(()) => return Err(warp::reject::custom(errors::ErrorInvalidPassphrase)),
    };

    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:make_payment error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotAccessUserNode));
    }

    Ok(warp::reply::json(&"Payment processing".to_owned()))
}

///Post make a donation request from the user node at specified ip address
pub async fn post_request_donation(
    peer: Node,
    address: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    trace!("in request donation");

    let paying_peer: SocketAddr = address
        .parse::<SocketAddr>()
        .map_err(|_| warp::reject::custom(errors::ErrorCannotParseAddress))?;

    let request = UserRequest::UserApi(UserApiRequest::RequestDonation { paying_peer });

    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:equest_donation error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotAccessUserNode));
    }

    Ok(warp::reply::json(&"Donation processing".to_owned()))
}

/// Post to update running total of connected wallet
pub async fn post_update_running_total(
    peer: Node,
    addresses: PublicKeyAddresses,
) -> Result<impl warp::Reply, warp::Rejection> {
    let request = UserRequest::UserApi(UserApiRequest::UpdateWalletFromUtxoSet {
        address_list: UtxoFetchType::AnyOf(addresses.address_list),
    });

    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:update_running_total error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotAccessUserNode));
    }

    Ok(warp::reply::json(&"Running total updated".to_owned()))
}

/// Post to fetch the balance for given addresses in UTXO
pub async fn post_fetch_utxo_balance(
    tracked_utxo: Arc<Mutex<TrackedUtxoSet>>,
    addresses: PublicKeyAddresses,
) -> Result<impl warp::Reply, warp::Rejection> {
    let tutxo = tracked_utxo.lock().unwrap();
    let balances = tutxo.get_balance_for_addresses(&addresses.address_list);

    Ok(warp::reply::json(&json!(balances)))
}

/// Post to create a receipt asset transaction on EITHER Compute or User node type
pub async fn post_create_receipt_asset(
    peer: Node,
    create_receipt_asset_data: CreateReceiptAssetData,
) -> Result<impl warp::Reply, warp::Rejection> {
    let CreateReceiptAssetData {
        receipt_amount,
        script_public_key,
        public_key,
        signature,
    } = create_receipt_asset_data;

    let DebugData {
        node_type,
        node_api: _,
        node_peers: _,
    } = peer.get_debug_data().await;

    let all_some = script_public_key.is_some() && public_key.is_some() && signature.is_some();
    let all_none = script_public_key.is_none() && public_key.is_none() && signature.is_none();

    if node_type == node_type_as_str(NodeType::User) && all_none {
        // Create receipt tx on the user node
        let request =
            UserRequest::UserApi(UserApiRequest::SendCreateReceiptRequest { receipt_amount });
        if let Err(e) = peer.inject_next_event(peer.address(), request) {
            error!("route:post_create_receipt_asset error: {:?}", e);
            return Err(warp::reject::custom(errors::ErrorCannotAccessUserNode));
        }
    } else if node_type == node_type_as_str(NodeType::Compute) && all_some {
        // Create receipt tx on the compute node
        let (script_public_key, public_key, signature) = (
            script_public_key.unwrap_or_default(),
            public_key.unwrap_or_default(),
            signature.unwrap_or_default(),
        );

        let request = ComputeRequest::ComputeApi(ComputeApiRequest::SendCreateReceiptRequest {
            receipt_amount,
            script_public_key,
            public_key,
            signature,
        });

        if let Err(e) = peer.inject_next_event(peer.address(), request) {
            error!("route:post_create_receipt_asset error: {:?}", e);
            return Err(warp::reject::custom(errors::ErrorCannotAccessComputeNode));
        }
    } else {
        return Err(warp::reject::custom(errors::ErrorInvalidJSONStructure));
    }
    Ok(warp::reply::json(&"Creating receipt asset".to_owned()))
}

/// Post transactions to compute node
pub async fn post_create_transactions(
    peer: Node,
    data: Vec<CreateTransaction>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let transactions = {
        let mut transactions = Vec::new();
        for tx in data {
            let tx = to_transaction(tx)?;
            transactions.push(tx);
        }
        transactions
    };

    let request = ComputeRequest::ComputeApi(ComputeApiRequest::SendTransactions { transactions });

    if let Err(e) = peer.inject_next_event(peer.address(), request) {
        error!("route:post_create_transactions error: {:?}", e);
        return Err(warp::reject::custom(errors::ErrorCannotAccessComputeNode));
    }

    Ok(warp::reply::json(&"Creating Transactions".to_owned()))
}

//======= Helpers =======//

/// Generic static string warp error
pub fn generic_error(name: &'static str) -> warp::Rejection {
    warp::reject::custom(errors::ErrorGeneric::new(name))
}

/// Expect optional field
pub fn with_opt_field<T>(field: Option<T>, err: &'static str) -> Result<T, warp::Rejection> {
    field.ok_or_else(|| generic_error(err))
}

pub fn to_transaction(data: CreateTransaction) -> Result<Transaction, warp::Rejection> {
    let CreateTransaction {
        inputs,
        outputs,
        version,
        druid_info,
    } = data;

    let inputs = {
        let mut tx_ins = Vec::new();
        for i in inputs {
            let previous_out = with_opt_field(i.previous_out, "Invalid previous_out")?;
            let script_signature = with_opt_field(i.script_signature, "Invalid script_signature")?;
            let tx_in = {
                let CreateTxInScript::Pay2PkH {
                    signed_data,
                    signature,
                    public_key,
                } = script_signature;

                let signature =
                    with_opt_field(decode_signature(&signature).ok(), "Invalid signature")?;
                let public_key =
                    with_opt_field(decode_pub_key(&public_key).ok(), "Invalid public_key")?;

                TxIn {
                    previous_out: Some(previous_out),
                    script_signature: Script::pay2pkh(signed_data, signature, public_key),
                }
            };

            tx_ins.push(tx_in);
        }
        tx_ins
    };

    Ok(Transaction {
        inputs,
        outputs,
        version,
        druid_info,
    })
}
