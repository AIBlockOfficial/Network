use crate::api::handlers::{
    AddressConstructData, Addresses, ChangePassphraseData, CreateReceiptAssetData,
    CreateTransaction, CreateTxIn, CreateTxInScript, EncapsulatedPayment, PublicKeyAddresses,
};
use crate::api::routes;
use crate::comms_handler::{Event, Node, TcpTlsConfig};
use crate::configurations::DbMode;
use crate::constants::{BLOCK_PREPEND, FUND_KEY};
use crate::db_utils::{new_db, SimpleDb};
use crate::interfaces::{
    BlockchainItemMeta, ComputeApiRequest, NodeType, StoredSerializingBlock, UserApiRequest,
    UserRequest, UtxoFetchType,
};
use crate::storage::{put_named_last_block_to_block_chain, put_to_block_chain, DB_SPEC};
use crate::tracked_utxo::TrackedUtxoSet;
use crate::utils::{decode_pub_key, decode_secret_key};
use crate::wallet::{WalletDb, WalletDbError};
use crate::ComputeRequest;
use bincode::serialize;
use bytes::Bytes;
use naom::crypto::sign_ed25519::{self as sign};
use naom::primitives::asset::{Asset, TokenAmount};
use naom::primitives::block::Block;
use naom::primitives::transaction::{OutPoint, Transaction, TxConstructor, TxIn, TxOut};
use naom::script::lang::Script;
use naom::utils::transaction_utils::{
    construct_payment_tx_ins, construct_tx_hash, construct_tx_in_signable_asset_hash,
    construct_tx_in_signable_hash,
};
use serde_json::json;
use sha3::{Digest, Sha3_256};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use warp::http::{HeaderMap, HeaderValue, StatusCode};

const COMMON_PUB_KEY: &str = "5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_SEC_KEY: &str = "3053020101300506032b6570042204200186bc08f16428d2059227082b93e439ff50f8c162f24b9594b132f2cc15fca4a1230321005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_PUB_ADDR: &str = "13bd3351b78beb2d0dadf2058dcc926c";

const COMMON_ADDR_STORE: (&str, [u8; 152]) = (
    "4348536e3d5a13e347262b5023963edf",
    [
        195, 253, 191, 157, 40, 253, 233, 186, 96, 35, 27, 83, 83, 224, 191, 126, 133, 101, 235,
        168, 233, 122, 174, 109, 18, 247, 175, 139, 253, 55, 164, 187, 238, 175, 251, 110, 53, 47,
        158, 241, 103, 144, 49, 65, 247, 147, 145, 140, 12, 129, 123, 19, 187, 121, 31, 163, 16,
        231, 248, 38, 243, 200, 34, 91, 4, 241, 40, 42, 97, 236, 37, 180, 26, 16, 34, 171, 12, 92,
        4, 8, 53, 193, 181, 209, 97, 76, 164, 76, 0, 122, 44, 120, 212, 27, 145, 224, 20, 207, 215,
        134, 23, 178, 170, 157, 218, 55, 14, 64, 185, 128, 63, 131, 194, 24, 6, 228, 34, 50, 252,
        118, 94, 153, 105, 236, 92, 122, 169, 219, 119, 9, 250, 255, 20, 40, 148, 74, 182, 73, 180,
        83, 10, 240, 193, 201, 45, 5, 205, 34, 188, 174, 229, 96,
    ],
);

/*------- UTILS--------*/

fn from_utf8(data: &[u8]) -> &str {
    std::str::from_utf8(data).unwrap()
}

/// Util function to create a stub DB containing a single block
fn get_db_with_block() -> Arc<Mutex<SimpleDb>> {
    let db = get_db_with_block_no_mutex();
    Arc::new(Mutex::new(db))
}

fn get_db_with_block_no_mutex() -> SimpleDb {
    let block = Block::new();

    let tx = Transaction::new();
    let tx_value = serialize(&tx).unwrap();
    let tx_json = serde_json::to_vec(&tx).unwrap();
    let tx_hash = hex::encode(Sha3_256::digest(&serialize(&tx_value).unwrap()));

    let mut mining_tx_hash_and_nonces = BTreeMap::new();
    mining_tx_hash_and_nonces.insert(0, ("test".to_string(), vec![0, 1, 23]));

    let block_to_input = StoredSerializingBlock {
        block,
        mining_tx_hash_and_nonces,
    };

    let mut db = new_db(DbMode::InMemory, &DB_SPEC, None);
    let mut batch = db.batch_writer();

    // Handle block insert
    let block_input = serialize(&block_to_input).unwrap();
    let block_json = serde_json::to_vec(&block_to_input).unwrap();
    let block_hash = {
        let hash_digest = Sha3_256::digest(&block_input);
        let mut hash_digest = hex::encode(hash_digest);
        hash_digest.insert(0, BLOCK_PREPEND as char);
        hash_digest
    };

    {
        let block_num = 0;
        let tx_len = 0;
        let t = BlockchainItemMeta::Block { block_num, tx_len };
        let pointer = put_to_block_chain(&mut batch, &t, &block_hash, &block_input, &block_json);
        put_named_last_block_to_block_chain(&mut batch, &pointer);
    }
    // Handle tx insert
    {
        let t = BlockchainItemMeta::Tx {
            block_num: 0,
            tx_num: 1,
        };
        put_to_block_chain(&mut batch, &t, &tx_hash, &tx_value, &tx_json);
    }

    let batch = batch.done();
    db.write(batch).unwrap();
    db
}

// Util function to create a transaction.
// Returns the hash of the tx and the tx itself
fn get_transaction() -> (String, Transaction) {
    generate_transaction("tx_hash", COMMON_ADDR_STORE.0)
}

// Generates a transaction using the given `tx_hash` and `script_public_key`
fn generate_transaction(tx_hash: &str, script_public_key: &str) -> (String, Transaction) {
    let asset = TokenAmount(25_200);
    let mut tx = Transaction::new();

    let tx_in = TxIn::new_from_input(OutPoint::new(tx_hash.to_string(), 0), Script::new());
    let tx_out = TxOut::new_token_amount(script_public_key.to_string(), asset);
    tx.inputs = vec![tx_in];
    tx.outputs = vec![tx_out];

    let t_hash = construct_tx_hash(&tx);

    (t_hash, tx)
}

fn success_json() -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    (StatusCode::from_u16(200).unwrap(), headers)
}

fn user_api_request_as_frame(request: UserApiRequest) -> Option<Vec<u8>> {
    let sent_request = UserRequest::UserApi(request);
    Some(serialize(&sent_request).unwrap())
}

fn compute_api_request_as_frame(request: ComputeApiRequest) -> Option<Vec<u8>> {
    let sent_request = ComputeRequest::ComputeApi(request);
    Some(serialize(&sent_request).unwrap())
}

async fn next_event_frame(node: &mut Node) -> Option<Vec<u8>> {
    let evt = node.next_event().await;
    evt.map(|Event::NewFrame { peer: _, frame }| frame.to_vec())
}

async fn new_self_node(node_type: NodeType) -> (Node, SocketAddr) {
    let bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
    let tcp_tls_config = TcpTlsConfig::new_no_tls(bind_address);
    let self_node = Node::new(&tcp_tls_config, 20, node_type).await.unwrap();
    let self_socket = self_node.address();
    (self_node, self_socket)
}

/*------- GET TESTS--------*/

/// Test GET latest block info
#[tokio::test(flavor = "current_thread")]
async fn test_get_latest_block() {
    let db = get_db_with_block();
    let filter = routes::latest_block(db);

    let res = warp::test::request()
        .method("GET")
        .path("/latest_block")
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "{\"Block\":{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{\"0\":[\"test\",[0,1,23]]}}}");
}

/// Test GET wallet keypairs
#[tokio::test(flavor = "current_thread")]
async fn test_get_export_keypairs() {
    //
    // Arrange
    //
    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(String::new());
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let (address, keys) = (
        COMMON_ADDR_STORE.0.to_string(),
        COMMON_ADDR_STORE.1.to_vec(),
    );

    db.save_encrypted_address_to_wallet(address.clone(), keys.clone())
        .await
        .unwrap();

    let request = warp::test::request().method("GET").path("/export_keypairs");

    //
    // Act
    //
    let filter = routes::export_keypairs(db);
    let res = request.reply(&filter).await;
    let expected_addresses = serde_json::to_string(&json!({
        "addresses":{
            address: keys
        }
    }))
    .unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_addresses.as_bytes());
}

/// Test get user debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_user_debug_data() {
    //
    // Arrange
    //
    let (self_node, _self_socket) = new_self_node(NodeType::User).await;
    let request = warp::test::request().method("GET").path("/debug_data");
    //
    // Act
    //
    let filter = routes::debug_data(self_node.clone(), None);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"node_type\":\"User\",\"node_api\":[\"wallet_info\",\"make_payment\",\"make_ip_payment\",\"request_donation\",\"export_keypairs\",\"import_keypairs\",\"update_running_total\",\"new_payment_address\",\"create_receipt_asset\",\"change_passphrase\",\"debug_data\"],\"node_peers\":[]}";
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_string);
}

/// Test get storage debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_storage_debug_data() {
    //
    // Arrange
    //
    let (self_node, _self_socket) = new_self_node(NodeType::Storage).await;
    let request = warp::test::request().method("GET").path("/debug_data");
    //
    // Act
    //
    let filter = routes::debug_data(self_node.clone(), None);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"node_type\":\"Storage\",\"node_api\":[\"latest_block\",\"blockchain_entry_by_key\",\"block_by_num\",\"block_by_tx_hashes\",\"debug_data\"],\"node_peers\":[]}";
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_string);
}

/// Test get compute debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_compute_debug_data() {
    //
    // Arrange
    //
    let (self_node, _self_socket) = new_self_node(NodeType::Compute).await;
    let request = warp::test::request().method("GET").path("/debug_data");
    //
    // Act
    //
    let filter = routes::debug_data(self_node.clone(), None);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"node_type\":\"Compute\",\"node_api\":[\"fetch_balance\",\"create_transactions\",\"create_receipt_asset\",\"debug_data\",\"utxo_addresses\"],\"node_peers\":[]}";
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_string);
}

/// Test get miner debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_miner_debug_data() {
    //
    // Arrange
    //
    let (self_node, _self_socket) = new_self_node(NodeType::Miner).await;
    let request = warp::test::request().method("GET").path("/debug_data");
    //
    // Act
    //
    let filter = routes::debug_data(self_node.clone(), None);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"node_type\":\"Miner\",\"node_api\":[\"current_mining_block\",\"change_passphrase\",\"export_keypairs\",\"wallet_info\",\"import_keypairs\",\"new_payment_address\",\"debug_data\"],\"node_peers\":[]}";
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_string);
}

/// Test GET wallet info
#[tokio::test(flavor = "current_thread")]
async fn test_get_wallet_info() {
    //
    // Arrange
    //
    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(String::new());
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let mut fund_store = db.get_fund_store();
    let out_point = OutPoint::new("tx_hash".to_string(), 0);
    let asset = Asset::token_u64(11);
    fund_store.store_tx(out_point.clone(), asset.clone());

    db.set_db_value(FUND_KEY, serialize(&fund_store).unwrap())
        .await;

    db.save_transaction_to_wallet(out_point, "public_address".to_string())
        .await
        .unwrap();

    let request = warp::test::request().method("GET").path("/wallet_info");

    //
    // Act
    //
    let filter = routes::wallet_info(db);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"running_total\":0.0004365079365079365,\"receipt_total\":0,\"addresses\":{\"public_address\":[[{\"t_hash\":\"tx_hash\",\"n\":0},{\"Token\":11}]]}}");
}

/// Test GET new payment address
#[tokio::test(flavor = "current_thread")]
async fn test_get_payment_address() {
    //
    // Arrange
    //
    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(String::new());
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let request = warp::test::request()
        .method("GET")
        .path("/new_payment_address");

    //
    // Act
    //
    let filter = routes::payment_address(db.clone());
    let res = request.reply(&filter).await;
    let store_address = db.get_known_addresses().pop().unwrap();
    let expected_store_address = serde_json::to_string(&json!(store_address)).unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_store_address.as_bytes());
}

/// Test GET all addresses on the UTXO set
#[tokio::test(flavor = "current_thread")]
async fn test_get_utxo_set_addresses() {
    //
    // Arrange
    //
    let mut bmap = BTreeMap::new();

    let tx_vals = vec![
        generate_transaction("tx_hash_1", "public_address_1"),
        generate_transaction("tx_hash_2", "public_address_2"),
        generate_transaction("tx_hash_3", "public_address_3"),
    ];

    for (_, tx) in tx_vals {
        bmap.insert(
            tx.inputs[0].clone().previous_out.unwrap(),
            tx.outputs[0].clone(),
        );
    }

    let tracked_utxo = Arc::new(Mutex::new(TrackedUtxoSet::new(bmap)));

    let request = warp::test::request().method("GET").path("/utxo_addresses");

    //
    // Act
    //
    let filter = routes::utxo_addresses(tracked_utxo);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(
        res.body(),
        "[\"public_address_1\",\"public_address_2\",\"public_address_3\"]"
    );
}

/*------- POST TESTS--------*/

/// Test POST for get blockchain block by key
#[tokio::test(flavor = "current_thread")]
async fn test_post_blockchain_entry_by_key_block() {
    let db = get_db_with_block();
    let filter = routes::blockchain_entry_by_key(db);

    let res = warp::test::request()
        .method("POST")
        .path("/blockchain_entry_by_key")
        .header("Content-Type", "application/json")
        .json(&"b6d369ad3595c1348772ad89e7ce314032687579f1bbe288b1a4d065a005a9997")
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "{\"Block\":{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{\"0\":[\"test\",[0,1,23]]}}}");
}

/// Test POST for get blockchain tx by key
#[tokio::test(flavor = "current_thread")]
async fn test_post_blockchain_entry_by_key_tx() {
    let db = get_db_with_block();
    let filter = routes::blockchain_entry_by_key(db);

    let res = warp::test::request()
        .method("POST")
        .path("/blockchain_entry_by_key")
        .header("Content-Type", "application/json")
        .json(&"1842d4e51e99e14671077e4cac648339c3ca57e7219257fed707afd0f4d96232")
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(
        res.body(),
        "{\"Transaction\":{\"inputs\":[],\"outputs\":[],\"version\":1,\"druid_info\":null}}"
    );
}

/// Test POST for get blockchain with wrong key
#[tokio::test(flavor = "current_thread")]
async fn test_post_blockchain_entry_by_key_failure() {
    let db = get_db_with_block();
    let filter = routes::blockchain_entry_by_key(db);

    let res = warp::test::request()
        .method("POST")
        .path("/blockchain_entry_by_key")
        .header("Content-Type", "application/json")
        .json(&"test")
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    assert_eq!(res.status(), 500);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "Unhandled rejection: ErrorNoDataFoundForKey");
}

/// Test POST for get block info by nums
#[tokio::test(flavor = "current_thread")]
async fn test_post_block_info_by_nums() {
    let db = get_db_with_block();
    let filter = routes::block_info_by_nums(db);

    let res = warp::test::request()
        .method("POST")
        .path("/block_by_num")
        .header("Content-Type", "application/json")
        .json(&vec![0_u64, 10, 0])
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "[[\"b6d369ad3595c1348772ad89e7ce314032687579f1bbe288b1a4d065a005a9997\",{\"Block\":{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{\"0\":[\"test\",[0,1,23]]}}}],[\"\",{\"Block\":\"\"}],[\"b6d369ad3595c1348772ad89e7ce314032687579f1bbe288b1a4d065a005a9997\",{\"Block\":{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{\"0\":[\"test\",[0,1,23]]}}}]]");
}

/// Test POST make payment
#[tokio::test(flavor = "current_thread")]
async fn test_post_make_payment() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

    let encapsulated_data = EncapsulatedPayment {
        address: COMMON_ADDR_STORE.0.to_string(),
        amount: TokenAmount(25),
        passphrase: String::new(),
    };

    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(encapsulated_data.passphrase.clone());
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let request = warp::test::request()
        .method("POST")
        .path("/make_payment")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&encapsulated_data);

    //
    // Act
    //
    let filter = routes::make_payment(db, self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Payment processing\"");

    // Frame expected
    let (address, amount) = (encapsulated_data.address, encapsulated_data.amount);
    let expected_frame = user_api_request_as_frame(UserApiRequest::MakePayment { address, amount });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST make ip payment with correct address
#[tokio::test(flavor = "current_thread")]
async fn test_post_make_ip_payment() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

    let encapsulated_data = EncapsulatedPayment {
        address: "127.0.0.1:12345".to_owned(),
        amount: TokenAmount(25),
        passphrase: String::new(),
    };

    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(encapsulated_data.passphrase.clone());
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let request = warp::test::request()
        .method("POST")
        .path("/make_ip_payment")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&encapsulated_data);

    //
    // Act
    //
    let filter = routes::make_ip_payment(db, self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Payment processing\"");

    // Frame expected
    let (payment_peer, amount) = (
        encapsulated_data.address.parse::<SocketAddr>().unwrap(),
        encapsulated_data.amount,
    );
    let expected_frame = user_api_request_as_frame(UserApiRequest::MakeIpPayment {
        payment_peer,
        amount,
    });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST construct address from public key
#[tokio::test(flavor = "current_thread")]
async fn test_address_construction() {
    //
    // Arrange
    //

    let address_construct_data = AddressConstructData {
        pub_key: vec![
            109, 133, 37, 100, 46, 243, 13, 156, 189, 123, 142, 12, 24, 169, 49, 186, 187, 0, 63,
            27, 129, 207, 183, 13, 156, 208, 171, 164, 179, 118, 131, 183,
        ],
    };

    let request = warp::test::request()
        .method("POST")
        .path("/address_construction")
        .header("Content-Type", "application/json")
        .json(&address_construct_data);

    //
    // Act
    //
    let filter = routes::payment_address_construction();
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected = Bytes::from_static(b"\"56d5b6da467e6c588966967ef5405dd2\"");
    assert_eq!(res.body(), &expected);
}

/// Test POST make ip payment with correct address
#[tokio::test(flavor = "current_thread")]
async fn test_post_request_donation() {
    //
    // Arange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

    let address = "127.0.0.1:12345".to_owned();
    let paying_peer = address.parse::<SocketAddr>().unwrap();

    let request = warp::test::request()
        .method("POST")
        .path("/request_donation")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&address);

    //
    // Act
    //
    let filter = routes::request_donation(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Donation processing\"");

    // Frame expected
    let expected_frame = user_api_request_as_frame(UserApiRequest::RequestDonation { paying_peer });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST import key-pairs
#[tokio::test(flavor = "current_thread")]
async fn test_post_import_keypairs_success() {
    let passphrase: Option<String> = Some(String::from(""));
    let simple_db: Option<SimpleDb> = Some(get_db_with_block_no_mutex());
    let db = WalletDb::new(DbMode::InMemory, simple_db, passphrase);

    let mut addresses: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    addresses.insert(
        COMMON_ADDR_STORE.0.to_string(),
        COMMON_ADDR_STORE.1.to_vec(),
    );
    let imported_addresses = Addresses { addresses };
    let filter = routes::import_keypairs(db.clone());
    let wallet_addresses_before = db.get_known_addresses();

    let res = warp::test::request()
        .method("POST")
        .path("/import_keypairs")
        .header("Content-Type", "application/json")
        .json(&imported_addresses)
        .reply(&filter)
        .await;

    let wallet_addresses_after = db.get_known_addresses();

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    assert_eq!(wallet_addresses_before, Vec::<String>::new());
    assert_eq!(wallet_addresses_after, vec![COMMON_ADDR_STORE.0]);
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Key/s saved successfully\"");
}

#[tokio::test(flavor = "current_thread")]
async fn test_post_fetch_balance() {
    //
    // Arrange
    //
    let (_, tx) = get_transaction();
    let mut bmap = BTreeMap::new();

    bmap.insert(
        tx.inputs[0].clone().previous_out.unwrap(),
        tx.outputs[0].clone(),
    );

    let tracked_utxo = Arc::new(Mutex::new(TrackedUtxoSet::new(bmap)));
    let addresses = PublicKeyAddresses {
        address_list: vec![COMMON_ADDR_STORE.0.to_string()],
    };

    let request = warp::test::request()
        .method("POST")
        .path("/fetch_balance")
        .header("Content-Type", "application/json")
        .json(&addresses);

    //
    // Act
    //
    let filter = routes::fetch_utxo_balance(tracked_utxo);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(
        res.body(),
        "{\"address_list\":{\"4348536e3d5a13e347262b5023963edf\":[[{\"n\":0,\"t_hash\":\"tx_hash\"},{\"Token\":25200}]]},\"total\":{\"receipts\":0,\"tokens\":25200}}"
    );
}

/// Test POST update running total successful
#[tokio::test(flavor = "current_thread")]
async fn test_post_update_running_total() {
    //
    // Arrange
    //
    let (mut self_node, _self_socket) = new_self_node(NodeType::User).await;

    let addresses = PublicKeyAddresses {
        address_list: vec![COMMON_ADDR_STORE.0.to_string()],
    };
    let address_list = UtxoFetchType::AnyOf(addresses.address_list.clone());

    let request = warp::test::request()
        .method("POST")
        .path("/update_running_total")
        .header("Content-Type", "application/json")
        .json(&addresses);

    //
    // Act
    //
    let filter = routes::update_running_total(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Running total updated\"");

    // Expected Frame
    let expected_frame =
        user_api_request_as_frame(UserApiRequest::UpdateWalletFromUtxoSet { address_list });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST create receipt asset on compute node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_signable_transactions() {
    //
    // Arrange
    //
    let json_body = vec![CreateTransaction {
        inputs: vec![CreateTxIn {
            previous_out: Some(OutPoint::new(COMMON_PUB_ADDR.to_owned(), 0)),
            script_signature: None,
        }],
        outputs: vec![],
        version: 1,
        druid_info: None,
    }];

    let request = warp::test::request()
        .method("POST")
        .path("/signable_transactions")
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::signable_transactions();
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!(
        ((res.status(), res.headers().clone()), from_utf8(res.body())),
        (success_json(), "[{\"inputs\":[{\"previous_out\":{\"t_hash\":\"13bd3351b78beb2d0dadf2058dcc926c\",\"n\":0},\"script_signature\":{\"Pay2PkH\":{\"signed_data\":\"2000000000000000313362643333353162373862656232643064616466323035386463633932366300000000\",\"signature\":\"\",\"public_key\":\"\"}}}],\"outputs\":[],\"version\":1,\"druid_info\":null}]")
    );
}

/// Test POST create receipt asset on compute node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_transactions() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::Compute).await;

    let previous_out = OutPoint::new(COMMON_PUB_ADDR.to_owned(), 0);
    let signed_data = construct_tx_in_signable_hash(&previous_out);
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let raw_signature = sign::sign_detached(signed_data.as_bytes(), &secret_key);
    let signature = hex::encode(raw_signature.as_ref());
    let public_key = COMMON_PUB_KEY.to_owned();

    let json_body = vec![CreateTransaction {
        inputs: vec![CreateTxIn {
            previous_out: Some(previous_out.clone()),
            script_signature: Some(CreateTxInScript::Pay2PkH {
                signed_data,
                signature,
                public_key,
            }),
        }],
        outputs: vec![],
        version: 1,
        druid_info: None,
    }];

    let request = warp::test::request()
        .method("POST")
        .path("/create_transactions")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::create_transactions(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!(
        ((res.status(), res.headers().clone()), from_utf8(res.body())),
        (success_json(), "\"Creating Transactions\"")
    );

    // Expected Frame
    let expected_frame = compute_api_request_as_frame(ComputeApiRequest::SendTransactions {
        transactions: vec![Transaction {
            inputs: construct_payment_tx_ins(vec![TxConstructor {
                previous_out,
                signatures: vec![raw_signature],
                pub_keys: vec![decode_pub_key(COMMON_PUB_KEY).unwrap()],
            }]),
            outputs: vec![],
            version: 1,
            druid_info: None,
        }],
    });

    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST create receipt asset on compute node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_receipt_asset_tx_compute() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::Compute).await;

    let asset_hash = construct_tx_in_signable_asset_hash(&Asset::Receipt(1));
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let signature = hex::encode(sign::sign_detached(asset_hash.as_bytes(), &secret_key).as_ref());

    let json_body = CreateReceiptAssetData {
        receipt_amount: 1,
        script_public_key: Some(COMMON_PUB_ADDR.to_owned()),
        public_key: Some(COMMON_PUB_KEY.to_owned()),
        signature: Some(signature),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_receipt_asset")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::create_receipt_asset(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Creating receipt asset\"");

    // Expected Frame
    let expected_frame =
        compute_api_request_as_frame(ComputeApiRequest::SendCreateReceiptRequest {
            receipt_amount: json_body.receipt_amount,
            script_public_key: json_body.script_public_key.unwrap(),
            public_key: json_body.public_key.unwrap(),
            signature: json_body.signature.unwrap(),
        });

    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST create receipt asset on user node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_receipt_asset_tx_user() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

    let json_body = CreateReceiptAssetData {
        receipt_amount: 1,
        script_public_key: None,
        public_key: None,
        signature: None,
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_receipt_asset")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::create_receipt_asset(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Creating receipt asset\"");

    // Expected Frame
    let expected_frame = user_api_request_as_frame(UserApiRequest::SendCreateReceiptRequest {
        receipt_amount: json_body.receipt_amount,
    });

    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST create receipt asset on compute node failure
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_receipt_asset_tx_compute_failure() {
    //
    // Arrange
    //
    let (self_node, self_socket) = new_self_node(NodeType::Compute).await;

    let json_body = CreateReceiptAssetData {
        receipt_amount: 1,
        // These fields should be occupied for compute node
        script_public_key: None,
        public_key: None,
        signature: None,
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_receipt_asset")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::create_receipt_asset(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    assert_eq!(res.status(), 500);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "Unhandled rejection: ErrorInvalidJSONStructure");
}

/// Test POST create receipt asset on user node failure
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_receipt_asset_tx_user_failure() {
    //
    // Arrange
    //
    let (self_node, self_socket) = new_self_node(NodeType::User).await;

    let json_body = CreateReceiptAssetData {
        receipt_amount: 1,
        // These fields should be empty for user node
        script_public_key: Some(String::new()),
        public_key: Some(String::new()),
        signature: Some(String::new()),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_receipt_asset")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::create_receipt_asset(self_node.clone());
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    assert_eq!(res.status(), 500);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "Unhandled rejection: ErrorInvalidJSONStructure");
}

/// Test POST change passphrase successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_change_passphrase() {
    //
    // Arrange
    //
    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(String::from("old_passphrase"));
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let (payment_address, expected_address_store) = db.generate_payment_address().await;

    let json_body = ChangePassphraseData {
        old_passphrase: String::from("old_passphrase"),
        new_passphrase: String::from("new_passphrase"),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/change_passphrase")
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::change_passphrase(db.clone());
    let res = request.reply(&filter).await;
    let actual = db.test_passphrase(String::from("new_passphrase")).await;
    let actual_address_store = db.get_address_store(&payment_address);

    //
    // Assert
    //
    assert_eq!(
        expected_address_store.secret_key, actual_address_store.secret_key,
        "Not able to decrypt addresses stored in WalletDb"
    );

    assert!(matches!(actual, Ok(())), "{:?}", actual);
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "\"Passphrase changed successfully\"");
}

/// Test POST change passphrase failure
#[tokio::test(flavor = "current_thread")]
async fn test_post_change_passphrase_failure() {
    //
    // Arrange
    //
    let db = {
        let simple_db = Some(get_db_with_block_no_mutex());
        let passphrase = Some(String::from("old"));
        WalletDb::new(DbMode::InMemory, simple_db, passphrase)
    };

    let json_body = ChangePassphraseData {
        old_passphrase: String::from("invalid_passphrase"),
        new_passphrase: String::from("new_passphrase"),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/change_passphrase")
        .header("Content-Type", "application/json")
        .json(&json_body.clone());

    //
    // Act
    //
    let filter = routes::change_passphrase(db.clone());
    let actual = db.test_passphrase(String::from("new_passphrase")).await;
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );

    assert!(
        matches!(actual, Err(WalletDbError::PassphraseError)),
        "{:?}",
        actual
    );
    assert_eq!(res.status(), 500);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "Unhandled rejection: ErrorInvalidPassphrase");
}

/// Test POST fetch block hashes for blocks that contain given `tx_hashes`
#[tokio::test(flavor = "current_thread")]
async fn test_post_block_nums_by_tx_hashes() {
    //
    // Arrange
    //
    let db = get_db_with_block();

    let request = warp::test::request()
        .method("POST")
        .path("/block_by_tx_hashes")
        .header("Content-Type", "application/json")
        .json(&vec![
            "1842d4e51e99e14671077e4cac648339c3ca57e7219257fed707afd0f4d96232",
        ]);

    //
    // Act
    //
    let filter = routes::blocks_by_tx_hashes(db);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "[0]");
}
