use crate::api::handlers::{Addresses, EncapsulatedData, EncapsulatedPayment, PublicKeyAddresses};
use crate::api::routes;
use crate::comms_handler::{Event, Node};
use crate::configurations::DbMode;
use crate::constants::BLOCK_PREPEND;
use crate::db_utils::{new_db, SimpleDb};
use crate::interfaces::{
    BlockchainItemMeta, NodeType, StoredSerializingBlock, UserApiRequest, UserRequest,
    UtxoFetchType,
};
use crate::storage::{put_named_last_block_to_block_chain, put_to_block_chain, DB_SPEC};
use crate::wallet::{EncapsulationData, WalletDb};
use bincode::serialize;
use naom::primitives::asset::TokenAmount;
use naom::primitives::{block::Block, transaction::Transaction};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::sealedbox;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use warp::http::{HeaderMap, HeaderValue};

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
/*------- TESTS --------*/

/// Test POST for get blockchain block by key
#[tokio::test(basic_scheduler)]
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
#[tokio::test(basic_scheduler)]
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
#[tokio::test(basic_scheduler)]
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
#[tokio::test(basic_scheduler)]
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

/// Test GET latest block info
#[tokio::test(basic_scheduler)]
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

/// Test POST make ip payment with correct address
#[tokio::test(basic_scheduler)]
async fn test_post_make_ip_payment() {
    let self_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12360);
    let peer_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12361);

    let amount: u64 = 25;
    let address = String::from("127.0.0.1:12361");
    let amount = TokenAmount(amount);
    let passphrase = String::from("");
    let encapdata = EncapsulatedPayment {
        address,
        amount,
        passphrase,
    };

    let self_node = Node::new(self_socket, 20, NodeType::User).await.unwrap();
    let _peer_node = Node::new(peer_socket, 20, NodeType::User).await.unwrap();

    let passphrase: Option<String> = Some(String::from(""));
    let simple_db: Option<SimpleDb> = Some(get_db_with_block_no_mutex());
    let db = WalletDb::new(DbMode::InMemory, simple_db, passphrase);

    let EncapsulationData {
        public_key,
        secret_key,
    } = create_encapsulation_data(&db).await;

    let _unsused_secret_key_to_avoid_warning = secret_key;
    let message: Vec<u8> = serde_json::to_vec(&encapdata).unwrap();
    let ciphered_message = sealedbox::seal(&message, &public_key);
    let encaped_message = EncapsulatedData { ciphered_message };
    let filter = routes::make_ip_payment(db, self_node);

    let res = warp::test::request()
        .method("POST")
        .path("/make_ip_payment")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .json(&encaped_message)
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "\"Payment processing\"");
}

/// Test POST import key-pairs
#[tokio::test(basic_scheduler)]
async fn test_import_keypairs_success() {
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
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "\"Key/s saved successfully\"");
}

/// Test POST update running total successful
#[tokio::test(basic_scheduler)]
async fn test_update_running_total() {
    let self_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12370);
    let mut self_node = Node::new(self_socket, 20, NodeType::User).await.unwrap();
    let addresses = PublicKeyAddresses {
        address_list: vec![COMMON_ADDR_STORE.0.to_string()],
    };

    let filter = routes::update_running_total(self_node.clone());

    let res = warp::test::request()
        .method("POST")
        .path("/update_running_total")
        .header("Content-Type", "application/json")
        .json(&addresses)
        .reply(&filter)
        .await;

    // Assert
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "\"Running total updated\"");

    let expected_frame = {
        let address_list = UtxoFetchType::AnyOf(addresses.address_list);
        let sent_request =
            UserRequest::UserApi(UserApiRequest::UpdateWalletFromUtxoSet { address_list });
        Some(serialize(&sent_request).unwrap())
    };

    let actual_frame = self_node
        .next_event()
        .await
        .map(|Event::NewFrame { peer: _, frame }| frame.to_vec());

    assert_eq!(expected_frame, actual_frame);
}
