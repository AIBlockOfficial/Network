use crate::api::handlers::{Addresses, EncapsulatedData, EncapsulatedPayment, PublicKeyAddresses};
use crate::api::routes;
use crate::comms_handler::{Event, Node, TcpTlsConfig};
use crate::configurations::DbMode;
use crate::constants::{BLOCK_PREPEND, DATA_ENCAPSULATION_KEY, FUND_KEY};
use crate::db_utils::{new_db, SimpleDb};
use crate::interfaces::{
    BlockchainItemMeta, NodeType, StoredSerializingBlock, UserApiRequest, UserRequest,
    UtxoFetchType,
};
use crate::storage::{put_named_last_block_to_block_chain, put_to_block_chain, DB_SPEC};
use crate::wallet::{EncapsulationData, WalletDb};
use bincode::{deserialize, serialize};
use naom::constants::D_DISPLAY_PLACES;
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::OutPoint;
use naom::primitives::{block::Block, transaction::Transaction};
use serde_json::json;
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::sealedbox;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use warp::http::{HeaderMap, HeaderValue, StatusCode};

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

/// Util function to create a stub DB containing a single block
fn get_db_with_block() -> Arc<Mutex<SimpleDb>> {
    let db = get_db_with_block_no_mutex();
    Arc::new(Mutex::new(db))
}

fn get_db_with_block_no_mutex() -> SimpleDb {
    let block = Block::new();

    let tx = Transaction::new();
    let tx_value = serialize(&tx).unwrap();
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
        let pointer = put_to_block_chain(&mut batch, &t, &block_hash, &block_input);
        put_named_last_block_to_block_chain(&mut batch, &pointer);
    }
    // Handle tx insert
    {
        let t = BlockchainItemMeta::Tx {
            block_num: 0,
            tx_num: 1,
        };
        put_to_block_chain(&mut batch, &t, &tx_hash, &tx_value);
    }

    let batch = batch.done();
    db.write(batch).unwrap();
    db
}

async fn create_encapsulation_data(wallet_db: &WalletDb) -> EncapsulationData {
    let _ = wallet_db.generate_encapsulation_data().await;
    wallet_db.get_encapsulation_data().await.unwrap()
}

fn success_json() -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    (StatusCode::from_u16(200).unwrap(), headers)
}

fn api_request_as_frame(request: UserApiRequest) -> Option<Vec<u8>> {
    let sent_request = UserRequest::UserApi(request);
    Some(serialize(&sent_request).unwrap())
}

async fn next_event_frame(node: &mut Node) -> Option<Vec<u8>> {
    let evt = node.next_event().await;
    evt.map(|Event::NewFrame { peer: _, frame }| frame.to_vec())
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
    assert_eq!(res.body(), "{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{\"0\":[\"test\",[0,1,23]]}}");
}

/// Test GET wallet keypairs
#[tokio::test(flavor = "current_thread")]
async fn test_get_wallet_keypairs() {
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

    let request = warp::test::request().method("GET").path("/wallet_keypairs");

    //
    // Act
    //
    let filter = routes::wallet_keypairs(db);
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
    fund_store.store_tx(OutPoint::new("000000".to_string(), 0), TokenAmount(11));
    db.set_db_value(FUND_KEY, serialize(&fund_store).unwrap())
        .await;

    let request = warp::test::request().method("GET").path("/wallet_info");

    //
    // Act
    //
    let filter = routes::wallet_info(db);
    let res = request.reply(&filter).await;
    let expected_running_total = serde_json::to_string(&json!({
        "running_total":fund_store.running_total().0 as f64 / D_DISPLAY_PLACES
    }))
    .unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_running_total.as_bytes());
}

/// Test GET encapsulation data
#[tokio::test(flavor = "current_thread")]
async fn test_get_encapsulation_data() {
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
        .path("/wallet_encapsulation_data");

    //
    // Act
    //
    let filter = routes::wallet_encapsulation_data(db.clone());
    let res = request.reply(&filter).await;
    let expected_data = db.get_db_value(DATA_ENCAPSULATION_KEY).await.unwrap();
    let generated_encapsulation_data: EncapsulationData = deserialize(&expected_data).unwrap();
    let expected_encapsulation_data =
        serde_json::to_string(&json!({ "public_key": generated_encapsulation_data.public_key }))
            .unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_encapsulation_data.as_bytes());
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
        .json(&vec![0_u64])
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "[[\"b6d369ad3595c1348772ad89e7ce314032687579f1bbe288b1a4d065a005a9997\",{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{\"0\":[\"test\",[0,1,23]]}}]]");
}

/// Test POST make payment
#[tokio::test(flavor = "current_thread")]
async fn test_post_make_payment() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_user_node().await;

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

    let encapsulated_message = {
        let keys = create_encapsulation_data(&db).await;
        let message = serde_json::to_vec(&encapsulated_data).unwrap();
        let ciphered_message = sealedbox::seal(&message, &keys.public_key);
        EncapsulatedData { ciphered_message }
    };

    let request = warp::test::request()
        .method("POST")
        .path("/make_payment")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&encapsulated_message);

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
    let expected_frame = api_request_as_frame(UserApiRequest::MakePayment { address, amount });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST make ip payment with correct address
#[tokio::test(flavor = "current_thread")]
async fn test_post_make_ip_payment() {
    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_user_node().await;

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

    let encapsulated_message = {
        let keys = create_encapsulation_data(&db).await;
        let message = serde_json::to_vec(&encapsulated_data).unwrap();
        let ciphered_message = sealedbox::seal(&message, &keys.public_key);
        EncapsulatedData { ciphered_message }
    };

    let request = warp::test::request()
        .method("POST")
        .path("/make_ip_payment")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&encapsulated_message);

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
    let expected_frame = api_request_as_frame(UserApiRequest::MakeIpPayment {
        payment_peer,
        amount,
    });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST make ip payment with correct address
#[tokio::test(flavor = "current_thread")]
async fn test_post_request_donation() {
    //
    // Arange
    //
    let (mut self_node, self_socket) = new_self_user_node().await;

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
    assert_eq!(res.body(), "\"Donnation processing\"");

    // Frame expected
    let expected_frame = api_request_as_frame(UserApiRequest::RequestDonation { paying_peer });
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

/// Test POST update running total successful
#[tokio::test(flavor = "current_thread")]
async fn test_post_update_running_total() {
    //
    // Arrange
    //
    let (mut self_node, _self_socket) = new_self_user_node().await;

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
        api_request_as_frame(UserApiRequest::UpdateWalletFromUtxoSet { address_list });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

async fn new_self_user_node() -> (Node, SocketAddr) {
    let bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
    let tcp_tls_config = TcpTlsConfig::new_no_tls(bind_address);
    let self_node = Node::new(&tcp_tls_config, 20, NodeType::User)
        .await
        .unwrap();
    let self_socket = self_node.address();
    (self_node, self_socket)
}
