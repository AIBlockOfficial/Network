use crate::api::handlers::{
    AddressConstructData, Addresses, ChangePassphraseData, CreateItemAssetDataMempool,
    CreateItemAssetDataUser, CreateTransaction, CreateTxIn, CreateTxInScript, DbgPaths,
    EncapsulatedPayment, FetchPendingData,
};
use crate::api::routes;
use crate::api::utils::{auth_request, create_new_cache, handle_rejection, CACHE_LIVE_TIME};
use crate::comms_handler::{Event, Node, TcpTlsConfig};
use crate::configurations::{DbMode, MempoolNodeSharedConfig};
use crate::constants::FUND_KEY;
use crate::db_utils::{new_db, SimpleDb};
use crate::interfaces::{
    BlockchainItemMeta, DruidDroplet, DruidPool, MempoolApi, MempoolApiRequest, NodeType, Response,
    StoredSerializingBlock, TxStatus, UserApiRequest, UserRequest, UtxoFetchType,
};
use crate::mempool::MempoolError;
use crate::storage::{put_named_last_block_to_block_chain, put_to_block_chain, DB_SPEC};
use crate::test_utils::{generate_rb_transactions, RbReceiverData, RbSenderData};
use crate::threaded_call::ThreadedCallChannel;
use crate::tracked_utxo::TrackedUtxoSet;
use crate::utils::{
    apply_mining_tx, construct_valid_block_pow_hash, create_item_asset_tx_from_sig,
    decode_secret_key, generate_pow_for_block, to_api_keys, to_route_pow_infos,
    tracing_log_try_init, validate_pow_block, ApiKeys,
};
use crate::wallet::{AddressStore, AddressStoreHex, WalletDb, WalletDbError};
use crate::MempoolRequest;
use bincode::serialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing::error;
use tw_chain::constants::{NETWORK_VERSION_TEMP, NETWORK_VERSION_V0};
use tw_chain::crypto::sign_ed25519::{self as sign, PublicKey, SecretKey};
use tw_chain::primitives::asset::{Asset, TokenAmount};
use tw_chain::primitives::block::{Block, BlockHeader};
use tw_chain::primitives::transaction::{GenesisTxHashSpec, OutPoint, Transaction, TxIn, TxOut};
use tw_chain::script::lang::Script;
use tw_chain::utils::transaction_utils::{
    construct_tx_hash, construct_tx_in_signable_asset_hash, construct_tx_in_signable_hash,
};
use warp::http::{HeaderMap, HeaderValue, StatusCode};
use warp::Filter;

const COMMON_ROUTE_POW_DIFFICULTY: usize = 2;
const COMMON_REQ_ID: &str = "2ae7bc9cba924e3cb73c0249893078d7";
const COMMON_VALID_API_KEY: &str = "some_key";
const COMMON_VALID_API_KEYS: [&str; 2] = ["debug_data", COMMON_VALID_API_KEY];

const COMMON_VALID_POW_NONCE: &str = "81234";
const COMMON_PUB_KEY: &str = "5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_SEC_KEY: &str = "3053020101300506032b6570042204200186bc08f16428d2059227082b93e439ff50f8c162f24b9594b132f2cc15fca4a1230321005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_PUB_ADDR: &str = "13bd3351b78beb2d0dadf2058dcc926c";

const COMMON_ADDRS: &[&str] = &[
    "0008536e3d5a13e347262b5023963000",
    "1118536e3d5a13e347262b5023963111",
    "2228536e3d5a13e347262b5023963222",
];

const BLOCK_NONCE: &str = "780c05806a3b70b15c9673396171674f";

/*------- UTILS--------*/

#[derive(Default)]
struct MempoolTest {
    pub utxo_set: TrackedUtxoSet,
    pub druid_pool: DruidPool,
    pub threaded_calls: ThreadedCallChannel<dyn MempoolApi>,
}

impl MempoolTest {
    fn new(tx_vals: Vec<(String, Transaction)>) -> Self {
        let druid =
            if let Some(druid_info) = tx_vals.first().and_then(|(_, tx)| tx.druid_info.as_ref()) {
                druid_info.druid.clone()
            } else {
                "Druid1".to_owned()
            };
        let droplets = vec![(
            druid,
            DruidDroplet {
                participants: 2,
                txs: tx_vals.clone().into_iter().collect(),
            },
        )];

        Self {
            utxo_set: TrackedUtxoSet::new(
                tx_vals
                    .iter()
                    .map(|(_, tx)| {
                        (
                            tx.inputs[0].clone().previous_out.unwrap(),
                            tx.outputs[0].clone(),
                        )
                    })
                    .collect(),
            ),
            druid_pool: droplets.into_iter().collect(),
            ..Default::default()
        }
    }

    fn spawn(self) -> tokio::task::JoinHandle<Self> {
        tokio::spawn({
            let mut c = self;
            async move {
                let f = c.threaded_calls.rx.recv().await.unwrap();
                f(&mut c);
                c
            }
        })
    }
}

impl MempoolApi for MempoolTest {
    fn get_shared_config(&self) -> MempoolNodeSharedConfig {
        Default::default()
    }

    fn pause_nodes(&mut self, _b_num: u64) -> Response {
        let reason: String = "".to_string();

        Response {
            success: true,
            reason,
        }
    }

    // TODO: Implement over this placeholder
    fn get_transaction_status(
        &self,
        tx_hashes: Vec<String>,
    ) -> BTreeMap<String, crate::interfaces::TxStatus> {
        let mut tx_status = BTreeMap::new();
        for tx_hash in tx_hashes {
            let tx_status_type = TxStatus {
                status: crate::interfaces::TxStatusType::Confirmed,
                additional_info: "".to_string(),
                timestamp: 0,
            };
            tx_status.insert(tx_hash, tx_status_type);
        }
        tx_status
    }

    fn resume_nodes(&mut self) -> Response {
        let reason: String = "".to_string();

        Response {
            success: true,
            reason,
        }
    }

    fn send_shared_config(&mut self, _shared_config: MempoolNodeSharedConfig) -> Response {
        let reason: String = "".to_string();

        Response {
            success: true,
            reason,
        }
    }

    fn get_issued_supply(&self) -> TokenAmount {
        TokenAmount(100)
    }

    fn get_committed_utxo_tracked_set(&self) -> &TrackedUtxoSet {
        &self.utxo_set
    }

    fn get_pending_druid_pool(&self) -> &DruidPool {
        &self.druid_pool
    }

    fn receive_transactions(&mut self, _transactions: Vec<Transaction>) -> Response {
        let reason: String = "".to_string();

        Response {
            success: true,
            reason,
        }
    }

    fn create_item_asset_tx(
        &mut self,
        item_amount: u64,
        script_public_key: String,
        public_key: String,
        signature: String,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    ) -> Result<(Transaction, String), MempoolError> {
        let b_num = 0;
        Ok(create_item_asset_tx_from_sig(
            b_num,
            item_amount,
            script_public_key,
            public_key,
            signature,
            genesis_hash_spec,
            metadata,
        )?)
    }
}

fn from_utf8(data: &[u8]) -> &str {
    std::str::from_utf8(data).unwrap()
}

/// Util function to create a stub DB containing a single block
async fn get_db_with_block() -> Arc<Mutex<SimpleDb>> {
    let db = get_db_with_block_no_mutex().await;
    Arc::new(Mutex::new(db))
}

async fn get_wallet_db(passphrase: &str) -> WalletDb {
    let simple_db = Some(get_db_with_block_no_mutex().await);
    let passphrase = Some(passphrase.to_owned());
    WalletDb::new(DbMode::InMemory, simple_db, passphrase, None).unwrap()
}

async fn get_db_with_block_no_mutex() -> SimpleDb {
    let tx = Transaction {
        // We keep the network version here at 2 to avoid
        // redundant changes to tests at each upgrade when
        // rest of `Transaction` structure does not change
        version: 2,
        ..Default::default()
    };
    let tx_value = serialize(&tx).unwrap();
    let tx_json = serde_json::to_vec(&tx).unwrap();
    let tx_hash = construct_tx_hash(&tx);
    let nonce = hex::decode(BLOCK_NONCE).unwrap();

    let mut block = Block {
        header: BlockHeader {
            // We keep the network version here at 2 to avoid
            // redundant changes to tests at each upgrade when
            // rest of `BlockHeader` structure does not change
            version: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    block.transactions.push(tx_hash.clone());
    block.set_txs_merkle_root_and_hash().await;
    block.header = apply_mining_tx(block.header, nonce, "test".to_string());

    if validate_pow_block(&block.header).is_err() {
        block.header.nonce_and_mining_tx_hash.0 = generate_pow_for_block(&block.header)
            .expect("error occurred while mining block")
            .expect("couldn't find a valid nonce");
        let new_nonce = hex::encode(&block.header.nonce_and_mining_tx_hash.0);
        panic!(
            "get_db_with_block_no_mutex: Out of date nonce: {} -> new({})",
            BLOCK_NONCE, new_nonce
        );
    }

    let block_to_input = StoredSerializingBlock { block };

    let mut db = new_db(DbMode::InMemory, &DB_SPEC, None, None);
    let mut batch = db.batch_writer();

    // Handle block insert
    let block_input = serialize(&block_to_input).unwrap();
    let block_json = serde_json::to_vec(&block_to_input).unwrap();
    let block_hash = construct_valid_block_pow_hash(&block_to_input.block).unwrap();

    {
        let block_num = 0;
        let tx_len = 1;
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
    generate_transaction("tx_hash", COMMON_PUB_ADDR)
}

// Generates a transaction using the given `tx_hash` and `script_public_key`
fn generate_transaction(tx_hash: &str, script_public_key: &str) -> (String, Transaction) {
    let asset = TokenAmount(25_200);
    let mut tx = Transaction::new();

    let tx_in = TxIn::new_from_input(OutPoint::new(tx_hash.to_string(), 0), Script::new());
    let tx_out = TxOut::new_token_amount(script_public_key.to_string(), asset, None);
    tx.inputs = vec![tx_in];
    tx.outputs = vec![tx_out];

    let t_hash = construct_tx_hash(&tx);

    (t_hash, tx)
}

// Util function to create item base transactions.
// Returns the hash of the tx and the tx itself
fn get_rb_transactions() -> Vec<(String, Transaction)> {
    let rb_sender_data = RbSenderData {
        sender_pub_addr: "sender_address".to_owned(),
        sender_pub_key: COMMON_PUB_KEY.to_owned(),
        sender_sec_key: COMMON_SEC_KEY.to_owned(),
        sender_prev_out: OutPoint::new("000000".to_owned(), 0),
        sender_amount: TokenAmount(25_200),
        sender_half_druid: "full_".to_owned(),
        sender_expected_drs: Some("genesis_hash".to_owned()),
    };

    let rb_receiver_data = RbReceiverData {
        receiver_pub_addr: "receiver_address".to_owned(),
        receiver_pub_key: COMMON_PUB_KEY.to_owned(),
        receiver_sec_key: COMMON_SEC_KEY.to_owned(),
        receiver_prev_out: OutPoint::new("000001".to_owned(), 0),
        receiver_half_druid: "druid".to_owned(),
    };

    generate_rb_transactions(rb_sender_data, rb_receiver_data)
}

fn success_json() -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    (StatusCode::from_u16(200).unwrap(), headers)
}

fn fail_json(code: StatusCode) -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    (code, headers)
}

pub async fn ok_reply() -> Result<impl warp::Reply, warp::Rejection> {
    Ok(warp::reply::json(&0))
}

fn user_api_request_as_frame(request: UserApiRequest) -> Option<Vec<u8>> {
    let sent_request = UserRequest::UserApi(request);
    Some(serialize(&sent_request).unwrap())
}

fn mempool_api_request_as_frame(request: MempoolApiRequest) -> Option<Vec<u8>> {
    let sent_request = MempoolRequest::MempoolApi(request);
    Some(serialize(&sent_request).unwrap())
}

async fn next_event_frame(node: &mut Node) -> Option<Vec<u8>> {
    let evt = node.next_event().await;
    evt.map(|Event::NewFrame { peer: _, frame }| frame.to_vec())
}

async fn new_self_node(node_type: NodeType) -> (Node, SocketAddr) {
    new_self_node_with_port(node_type, 0).await
}

async fn new_self_node_with_port(node_type: NodeType, port: u16) -> (Node, SocketAddr) {
    let mut bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
    let mut socket_address = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    bind_address.set_port(port);

    let tcp_tls_config = TcpTlsConfig::new_no_tls(bind_address);
    let self_node = Node::new(&tcp_tls_config, 20, 20, node_type, false, false)
        .await
        .unwrap();
    socket_address.set_port(self_node.local_address().port());
    (self_node, socket_address)
}

fn dp() -> DbgPaths {
    Default::default()
}

/*------- GET TESTS--------*/

/// Test GET latest block info
#[tokio::test(flavor = "current_thread")]
async fn test_get_latest_block() {
    let _ = tracing_log_try_init();

    let db = get_db_with_block().await;
    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/latest_block");
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::latest_block(&mut dp(), db, Default::default(), ks, cache)
        .recover(handle_rejection);
    let res = request.reply(&filter).await;

    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Database item(s) successfully retrieved\",\"route\":\"latest_block\",\"content\":{\"block\":{\"header\":{\"version\":2,\"bits\":0,\"nonce_and_mining_tx_hash\":[[120,12,5,128,106,59,112,177,92,150,115,57,97,113,103,79],\"test\"],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"txs_merkle_root_and_hash\":[\"42fbcc73bc0eeb41a991a32a6f6e145d1d45b2738657db5b4781d1fa707693cf\",\"35260a02627ae9d586dbb9f11de79afd46d1096f41ffb6b9ee88cca6b78bf374\"]},\"transactions\":[\"g98d0ab9304ca82f098a86ad6251803b\"]}}}");
}

/// Test GET wallet keypairs
#[tokio::test(flavor = "current_thread")]
async fn test_get_export_keypairs() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let (address, keys) = (
        COMMON_PUB_ADDR.to_string(),
        AddressStore {
            public_key: PublicKey::from_slice(&hex::decode(COMMON_PUB_KEY).unwrap()).unwrap(),
            secret_key: SecretKey::from_slice(&hex::decode(COMMON_SEC_KEY).unwrap()).unwrap(),
            address_version: None,
        },
    );

    db.save_address_to_wallet(address.clone(), keys.clone())
        .unwrap();

    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/export_keypairs");
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::export_keypairs(&mut dp(), db, Default::default(), ks, cache)
        .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Key-pairs successfully exported\",\"route\":\"export_keypairs\",\"content\":{\"addresses\":{\"13bd3351b78beb2d0dadf2058dcc926c\":{\"public_key\":\"5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c\",\"secret_key\":\"3053020101300506032b6570042204200186bc08f16428d2059227082b93e439ff50f8c162f24b9594b132f2cc15fca4a1230321005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c\",\"address_version\":null}}}}");
}

/// Test get user debug data
// #[tokio::test(flavor = "current_thread")]
// async fn test_get_user_debug_data() {
//     let _ = tracing_log_try_init();

//     //
//     // Arrange
//     //
//     let db = get_wallet_db("").await;
//     let ks: ApiKeys = Arc::new(Mutex::new(BTreeMap::new()));
//     ks.lock().unwrap().insert(
//         COMMON_VALID_API_KEYS[0].to_string(),
//         vec![COMMON_VALID_API_KEYS[1].to_string()],
//     );
//     let (mut self_node, _self_socket) = new_self_node(NodeType::User).await;
//     let (_c_node, c_socket) = new_self_node_with_port(NodeType::Mempool, 13000).await;
//     self_node.connect_to(c_socket).await.unwrap();

//     let request = || {
//         warp::test::request()
//             .method("GET")
//             .header("x-cache-id", COMMON_REQ_ID)
//             .path("/debug_data")
//     };
//     let request_x_api = || request().header("x-api-key", COMMON_VALID_API_KEY);

//     //
//     // Act
//     //
//     let tx = self_node.threaded_calls.tx.clone();
//     let filter = routes::user_node_routes(ks, Default::default(), db, self_node.clone())
//         .recover(handle_rejection);
//     let res_a = request_x_api().reply(&filter).await;
//     let res_m = request().reply(&filter).await;

//     //
//     // Assert
//     //
//     let expected_string = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Debug data successfully retrieved\",\"route\":\"debug_data\",\"content\":{\"node_type\":\"User\",\"node_api\":[\"wallet_info\",\"make_payment\",\"make_ip_payment\",\"request_donation\",\"export_keypairs\",\"import_keypairs\",\"update_running_total\",\"create_item_asset\",\"payment_address\",\"change_passphrase\",\"address_construction\",\"debug_data\"],\"node_peers\":[[\"127.0.0.1:13000\",\"127.0.0.1:13000\",\"Mempool\"]],\"routes_pow\":{}}}";
//     assert_eq!((res_a.status(), res_a.headers().clone()), success_json());
//     assert_eq!(res_a.body(), expected_string);

//     assert_eq!(
//         (res_m.status(), res_m.headers().clone()),
//         fail_json(StatusCode::UNAUTHORIZED)
//     );
//     assert_eq!(res_m.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Unauthorized\",\"route\":\"debug_data\",\"content\":\"null\"}");
// }

/// Test get storage debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_storage_debug_data() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_db_with_block().await;
    let ks: ApiKeys = Arc::new(Mutex::new(BTreeMap::new()));
    ks.lock().unwrap().insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    let (mut self_node, _self_socket) = new_self_node(NodeType::Storage).await;
    let (_c_node, c_socket) = new_self_node_with_port(NodeType::Mempool, 13010).await;
    self_node.connect_to(c_socket).await.unwrap();

    let request = || {
        warp::test::request()
            .method("GET")
            .header("x-cache-id", COMMON_REQ_ID)
            .path("/debug_data")
    };
    let request_x_api = || request().header("x-api-key", COMMON_VALID_API_KEY);

    //
    // Act
    //
    let filter = routes::storage_node_routes(ks, Default::default(), db, self_node.clone())
        .recover(handle_rejection);
    let res_a = request_x_api().reply(&filter).await;
    let res_m = request().reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Debug data successfully retrieved\",\"route\":\"debug_data\",\"content\":{\"node_type\":\"Storage\",\"node_api\":[\"block_by_num\",\"transactions_by_key\",\"latest_block\",\"blockchain_entry\",\"check_transaction_presence\",\"address_construction\",\"debug_data\"],\"node_peers\":[[\"127.0.0.1:13010\",\"127.0.0.1:13010\",\"Mempool\"]],\"routes_pow\":{}}}";
    assert_eq!((res_a.status(), res_a.headers().clone()), success_json());
    assert_eq!(res_a.body(), expected_string);

    assert_eq!(
        (res_m.status(), res_m.headers().clone()),
        fail_json(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(res_m.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Unauthorized\",\"route\":\"debug_data\",\"content\":\"null\"}");
}

/// Test get mempool debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_mempool_debug_data() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(vec![]);
    let ks: ApiKeys = Arc::new(Mutex::new(BTreeMap::new()));
    ks.lock().unwrap().insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    let (mut self_node, _self_socket) = new_self_node(NodeType::Mempool).await;
    let (_c_node, c_socket) = new_self_node_with_port(NodeType::Mempool, 13020).await;
    self_node.connect_to(c_socket).await.unwrap();

    let request = || {
        warp::test::request()
            .method("GET")
            .header("x-cache-id", COMMON_REQ_ID)
            .path("/debug_data")
    };
    let request_x_api = || request().header("x-api-key", COMMON_VALID_API_KEY);

    //
    // Act
    //
    let tx = mempool.threaded_calls.tx.clone();
    let routes_pow = to_route_pow_infos(
        vec![(
            "create_transactions".to_owned(),
            COMMON_ROUTE_POW_DIFFICULTY,
        )]
        .into_iter()
        .collect(),
    );
    let filter = routes::mempool_node_routes(ks, routes_pow, tx, self_node.clone())
        .recover(handle_rejection);
    let res_a = request_x_api().reply(&filter).await;
    let res_m = request().reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Debug data successfully retrieved\",\"route\":\"debug_data\",\"content\":{\"node_type\":\"Mempool\",\"node_api\":[\"fetch_balance\",\"create_item_asset\",\"create_transactions\",\"utxo_addresses\",\"address_construction\",\"pause_nodes\",\"resume_nodes\",\"update_shared_config\",\"get_shared_config\",\"debug_data\"],\"node_peers\":[[\"127.0.0.1:13020\",\"127.0.0.1:13020\",\"Mempool\"]],\"routes_pow\":{\"create_transactions\":2}}}";
    assert_eq!((res_a.status(), res_a.headers().clone()), success_json());
    assert_eq!(res_a.body(), expected_string);

    assert_eq!(
        (res_m.status(), res_m.headers().clone()),
        fail_json(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(res_m.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Unauthorized\",\"route\":\"debug_data\",\"content\":\"null\"}");
}

/// Test get miner debug data
#[tokio::test(flavor = "current_thread")]
async fn test_get_miner_debug_data() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let current_block = Default::default();
    let ks: ApiKeys = Arc::new(Mutex::new(BTreeMap::new()));
    ks.lock().unwrap().insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    let (mut self_node, _self_socket) = new_self_node(NodeType::Miner).await;
    let (_c_node, c_socket) = new_self_node_with_port(NodeType::Mempool, 13030).await;
    self_node.connect_to(c_socket).await.unwrap();

    let request = || {
        warp::test::request()
            .method("GET")
            .header("x-cache-id", COMMON_REQ_ID)
            .path("/debug_data")
    };
    let request_x_api = || request().header("x-api-key", COMMON_VALID_API_KEY);

    //
    // Act
    //
    let filter =
        routes::miner_node_routes(ks, Default::default(), current_block, db, self_node.clone())
            .recover(handle_rejection);
    let res_a = request_x_api().reply(&filter).await;
    let res_m = request().reply(&filter).await;

    //
    // Assert
    //
    let expected_string = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Debug data successfully retrieved\",\"route\":\"debug_data\",\"content\":{\"node_type\":\"Miner\",\"node_api\":[\"wallet_info\",\"export_keypairs\",\"import_keypairs\",\"payment_address\",\"change_passphrase\",\"current_mining_block\",\"address_construction\",\"debug_data\"],\"node_peers\":[[\"127.0.0.1:13030\",\"127.0.0.1:13030\",\"Mempool\"]],\"routes_pow\":{}}}";
    assert_eq!((res_a.status(), res_a.headers().clone()), success_json());
    assert_eq!(res_a.body(), expected_string);

    assert_eq!(
        (res_m.status(), res_m.headers().clone()),
        fail_json(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(res_m.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Unauthorized\",\"route\":\"debug_data\",\"content\":\"null\"}");
}

/// Test get miner with user debug data
// #[tokio::test(flavor = "current_thread")]
// async fn test_get_miner_with_user_debug_data() {
//     let _ = tracing_log_try_init();

//     //
//     // Arrange
//     //
//     let db = get_wallet_db("").await;
//     let current_block = Default::default();
//     let ks: ApiKeys = Arc::new(Mutex::new(BTreeMap::new()));
//     ks.lock().unwrap().insert(
//         COMMON_VALID_API_KEYS[0].to_string(),
//         vec![COMMON_VALID_API_KEYS[1].to_string()],
//     );
//     let (mut self_node, _self_socket) = new_self_node(NodeType::Miner).await;
//     let (mut self_node_u, _self_socket_u) = new_self_node(NodeType::User).await;
//     let (_c_node, c_socket) = new_self_node_with_port(NodeType::Mempool, 13040).await;
//     let (_s_node, s_socket) = new_self_node_with_port(NodeType::Storage, 13041).await;
//     self_node.connect_to(c_socket).await.unwrap();
//     self_node_u.connect_to(s_socket).await.unwrap();

//     let request = || {
//         warp::test::request()
//             .method("GET")
//             .header("x-cache-id", COMMON_REQ_ID)
//             .path("/debug_data")
//     };
//     let request_x_api = || request().header("x-api-key", COMMON_VALID_API_KEY);

//     //
//     // Act
//     //
//     let filter = routes::miner_node_with_user_routes(
//         ks,
//         Default::default(),
//         current_block,
//         db,
//         self_node,
//         self_node_u,
//     )
//     .recover(handle_rejection);
//     let res_a = request_x_api().reply(&filter).await;
//     let res_m = request().reply(&filter).await;

//     //
//     // Assert
//     //
//     let expected_string = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Debug data successfully retrieved\",\"route\":\"debug_data\",\"content\":{\"node_type\":\"Miner/User\",\"node_api\":[\"wallet_info\",\"make_payment\",\"make_ip_payment\",\"request_donation\",\"export_keypairs\",\"import_keypairs\",\"update_running_total\",\"create_item_asset\",\"payment_address\",\"change_passphrase\",\"current_mining_block\",\"address_construction\",\"debug_data\"],\"node_peers\":[[\"127.0.0.1:13040\",\"127.0.0.1:13040\",\"Mempool\"],[\"127.0.0.1:13041\",\"127.0.0.1:13041\",\"Storage\"]],\"routes_pow\":{}}}";
//     assert_eq!((res_a.status(), res_a.headers().clone()), success_json());
//     assert_eq!(res_a.body(), expected_string);

//     assert_eq!(
//         (res_m.status(), res_m.headers().clone()),
//         fail_json(StatusCode::UNAUTHORIZED)
//     );
//     assert_eq!(res_m.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Unauthorized\",\"route\":\"debug_data\",\"content\":\"null\"}");
// }

// Authorize a request where no proof-of-work or API key is required
#[tokio::test(flavor = "current_thread")]
async fn auth_request_no_pow_with_no_api_key() {
    auth_request_common((Default::default(), COMMON_VALID_API_KEY), None, true).await;
}

// Authorize a request where proof-of-work IS required BUT no API key is required
#[tokio::test(flavor = "current_thread")]
async fn auth_request_pow_with_no_api_key() {
    auth_request_common(
        (Default::default(), COMMON_VALID_API_KEY),
        Some((COMMON_ROUTE_POW_DIFFICULTY, COMMON_VALID_POW_NONCE)),
        true,
    )
    .await;
}

// Authorize a request where NO proof-of-work is required BUT a valid API key is required
#[tokio::test(flavor = "current_thread")]
async fn auth_request_no_pow_with_api_key() {
    let mut api_keys = BTreeMap::new();
    api_keys.insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    auth_request_common((api_keys, COMMON_VALID_API_KEY), None, true).await;
}

// Authorize a request where valid proof of work is required AND a valid API key
#[tokio::test(flavor = "current_thread")]
async fn auth_request_pow_with_api_key() {
    let mut api_keys = BTreeMap::new();
    api_keys.insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    auth_request_common(
        (api_keys, COMMON_VALID_API_KEY),
        Some((COMMON_ROUTE_POW_DIFFICULTY, COMMON_VALID_POW_NONCE)),
        true,
    )
    .await;
}

// Authorize a request where valid proof of work is required AND a valid API key
// , but the provided nonce for PoW is invalid
#[tokio::test(flavor = "current_thread")]
async fn auth_request_invalid_pow_with_api_key_failure() {
    let mut api_keys = BTreeMap::new();
    api_keys.insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    auth_request_common(
        (api_keys, COMMON_VALID_API_KEY),
        /* Invalid nonce value */
        Some((COMMON_ROUTE_POW_DIFFICULTY, "99999")),
        false,
    )
    .await;
}

// Authorize a request where valid proof of work is required AND a valid API key
// , but the provided API key is invalid
#[tokio::test(flavor = "current_thread")]
async fn auth_request_pow_with_invalid_api_key_failure() {
    let mut api_keys = BTreeMap::new();
    api_keys.insert(
        COMMON_VALID_API_KEYS[0].to_string(),
        vec![COMMON_VALID_API_KEYS[1].to_string()],
    );
    auth_request_common(
        /* Invalid API key */
        (api_keys, "invalid_api_key"),
        Some((COMMON_ROUTE_POW_DIFFICULTY, COMMON_VALID_POW_NONCE)),
        false,
    )
    .await;
}

async fn auth_request_common(
    api_key_and_keys: (BTreeMap<String, Vec<String>>, &str),
    difficulty_and_nonce: Option<(usize, &str)>,
    authorization_success: bool,
) {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //

    let routes_pow = to_route_pow_infos(
        difficulty_and_nonce
            .into_iter()
            .map(|(d, _)| ("debug_data".to_owned(), d))
            .collect(),
    );
    let nonce = difficulty_and_nonce.map(|(_, n)| n).unwrap_or_default();
    let (api_keys, api_key) = (to_api_keys(api_key_and_keys.0), api_key_and_keys.1);

    let request = || {
        warp::test::request()
            .method("GET")
            .header("x-cache-id", COMMON_REQ_ID)
            .header("x-api-key", api_key)
            .header("x-nonce", nonce)
            .path("/debug_data")
    };

    //
    // Act
    //
    let filter = auth_request(routes_pow, api_keys)
        .and_then(|_| ok_reply())
        .recover(handle_rejection);
    let actual_response = request().reply(&filter).await;

    let (expected_response, expected_response_body) = if authorization_success {
        (success_json(), "0")
    } else {
        (fail_json(StatusCode::UNAUTHORIZED), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Unauthorized\",\"route\":\"debug_data\",\"content\":\"null\"}")
    };

    //
    // Assert
    //
    assert_eq!(
        (actual_response.status(), actual_response.headers().clone()),
        expected_response
    );
    assert_eq!(actual_response.body(), expected_response_body);
}

/// Test GET wallet info
#[tokio::test(flavor = "current_thread")]
async fn test_get_wallet_info() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let mut fund_store = db.get_fund_store();
    let out_point = OutPoint::new("tx_hash".to_string(), 0);
    let out_point_s = OutPoint::new("tx_hash_spent".to_string(), 0);
    let asset = Asset::token_u64(11);
    fund_store.store_tx(out_point.clone(), asset.clone());
    fund_store.store_tx(out_point_s.clone(), asset.clone());
    fund_store.spend_tx(&out_point_s);

    db.set_db_value(FUND_KEY, serialize(&fund_store).unwrap())
        .await;

    db.save_transaction_to_wallet(out_point, "public_address".to_string())
        .await
        .unwrap();
    db.save_transaction_to_wallet(out_point_s, "public_address_spent".to_string())
        .await
        .unwrap();

    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/wallet_info");

    let com_req_id_plus_1 = "2ae7bc9cba924e3cb73c0249893078d8";
    let request_spent = warp::test::request()
        .method("GET")
        .header("x-cache-id", com_req_id_plus_1)
        .path("/wallet_info/spent");
    let cache = create_new_cache(CACHE_LIVE_TIME);

    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let filter =
        routes::wallet_info(&mut dp(), db, Default::default(), ks, cache).recover(handle_rejection);
    let res = request.reply(&filter).await;
    let r_s = request_spent.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.0004365079365079365,\"running_total_tokens\":11,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.0004365079365079365,\"available_total_tokens\":11,\"item_total\":{},\"addresses\":{\"public_address\":[{\"out_point\":{\"t_hash\":\"tx_hash\",\"n\":0},\"value\":{\"Token\":11}}]}}}");

    assert_eq!((r_s.status(), r_s.headers().clone()), success_json());
    assert_eq!(r_s.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d8\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.0004365079365079365,\"running_total_tokens\":11,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.0004365079365079365,\"available_total_tokens\":11,\"item_total\":{},\"addresses\":{\"public_address_spent\":[{\"out_point\":{\"t_hash\":\"tx_hash_spent\",\"n\":0},\"value\":{\"Token\":11}}]}}}");
}

/// Test GET shared config for mempool node
#[tokio::test(flavor = "current_thread")]
async fn test_get_shared_config() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(Default::default());
    let request = warp::test::request()
        .method("GET")
        .path("/get_shared_config")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID);

    //
    // Act
    //
    let filter = routes::get_shared_config(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        Default::default(),
        create_new_cache(CACHE_LIVE_TIME),
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _ = handle.await;
    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Successfully fetched shared config\",\"route\":\"get_shared_config\",\"content\":{\"mempool_mining_event_timeout\":0,\"mempool_partition_full_size\":0,\"mempool_miner_whitelist\":{\"active\":false,\"miner_api_keys\":null,\"miner_addresses\":null}}}");
}

#[tokio::test(flavor = "current_thread")]
async fn test_pagination() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let mut fund_store = db.get_fund_store();
    fund_store.add_transaction_pages();

    for i in 0..100 {
        let out_point = OutPoint::new("tx_hash".to_string() + &i.to_string(), 0);
        let asset = Asset::token_u64(11);
        fund_store.store_tx(out_point.clone(), asset.clone());

        db.set_db_value(FUND_KEY, serialize(&fund_store).unwrap())
            .await;

        db.save_transaction_to_wallet(out_point, "public_address".to_string())
            .await
            .unwrap();
    }

    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/wallet_info/1");
    let cache = create_new_cache(CACHE_LIVE_TIME);

    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let filter =
        routes::wallet_info(&mut dp(), db, Default::default(), ks, cache).recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.04365079365079365,\"running_total_tokens\":1100,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.04365079365079365,\"available_total_tokens\":1100,\"item_total\":{},\"addresses\":{\"public_address\":[{\"out_point\":{\"t_hash\":\"tx_hash25\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash26\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash27\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash28\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash29\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash30\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash31\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash32\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash33\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash34\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash35\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash36\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash37\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash38\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash39\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash40\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash41\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash42\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash43\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash44\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash45\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash46\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash47\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash48\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash49\",\"n\":0},\"value\":{\"Token\":11}}]}}}");
}

#[tokio::test(flavor = "current_thread")]
async fn test_pagination_zero_or_terminal() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let mut fund_store = db.get_fund_store();

    for i in 0..100 {
        let out_point = OutPoint::new("tx_hash".to_string() + &i.to_string(), 0);
        let asset = Asset::token_u64(11);
        fund_store.store_tx(out_point.clone(), asset.clone());

        db.set_db_value(FUND_KEY, serialize(&fund_store).unwrap())
            .await;

        db.save_transaction_to_wallet(out_point, "public_address".to_string())
            .await
            .unwrap();
    }

    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/wallet_info/-10");

    let com_req_id_plus_1 = "2ae7bc9cba924e3cb73c0249893078d8";
    let request_terminal = warp::test::request()
        .method("GET")
        .header("x-cache-id", com_req_id_plus_1)
        .path("/wallet_info/999");
    let cache = create_new_cache(CACHE_LIVE_TIME);

    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let filter =
        routes::wallet_info(&mut dp(), db, Default::default(), ks, cache).recover(handle_rejection);
    let res = request.reply(&filter).await;
    let r_s = request_terminal.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.04365079365079365,\"running_total_tokens\":1100,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.04365079365079365,\"available_total_tokens\":1100,\"item_total\":{},\"addresses\":{\"public_address\":[{\"out_point\":{\"t_hash\":\"tx_hash0\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash1\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash10\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash11\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash12\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash13\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash14\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash15\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash16\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash17\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash18\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash19\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash2\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash20\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash21\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash22\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash23\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash24\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash25\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash26\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash27\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash28\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash29\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash3\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash30\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash31\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash32\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash33\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash34\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash35\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash36\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash37\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash38\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash39\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash4\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash40\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash41\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash42\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash43\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash44\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash45\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash46\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash47\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash48\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash49\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash5\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash50\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash51\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash52\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash53\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash54\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash55\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash56\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash57\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash58\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash59\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash6\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash60\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash61\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash62\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash63\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash64\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash65\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash66\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash67\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash68\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash69\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash7\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash70\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash71\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash72\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash73\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash74\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash75\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash76\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash77\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash78\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash79\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash8\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash80\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash81\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash82\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash83\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash84\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash85\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash86\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash87\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash88\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash89\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash9\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash90\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash91\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash92\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash93\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash94\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash95\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash96\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash97\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash98\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash99\",\"n\":0},\"value\":{\"Token\":11}}]}}}");

    assert_eq!((r_s.status(), r_s.headers().clone()), success_json());
    assert_eq!(r_s.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d8\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.04365079365079365,\"running_total_tokens\":1100,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.04365079365079365,\"available_total_tokens\":1100,\"item_total\":{},\"addresses\":{\"public_address\":[{\"out_point\":{\"t_hash\":\"tx_hash75\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash76\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash77\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash78\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash79\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash80\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash81\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash82\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash83\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash84\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash85\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash86\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash87\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash88\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash89\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash90\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash91\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash92\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash93\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash94\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash95\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash96\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash97\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash98\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash99\",\"n\":0},\"value\":{\"Token\":11}}]}}}");
}

/// Test cache
#[tokio::test(flavor = "current_thread")]
async fn test_cache() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let mut fund_store = db.get_fund_store();
    let out_point = OutPoint::new("tx_hash".to_string(), 0);
    let out_point_s = OutPoint::new("tx_hash_spent".to_string(), 0);
    let asset = Asset::token_u64(11);
    fund_store.store_tx(out_point.clone(), asset.clone());
    fund_store.store_tx(out_point_s.clone(), asset.clone());
    fund_store.spend_tx(&out_point_s);

    db.set_db_value(FUND_KEY, serialize(&fund_store).unwrap())
        .await;

    db.save_transaction_to_wallet(out_point, "public_address".to_string())
        .await
        .unwrap();
    db.save_transaction_to_wallet(out_point_s, "public_address_spent".to_string())
        .await
        .unwrap();

    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/wallet_info");
    let request_spent = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/wallet_info/spent");
    let com_req_id_plus_1 = "2ae7bc9cba924e3cb73c0249893078d8";
    let request_spent_diff_id = warp::test::request()
        .method("GET")
        .header("x-cache-id", com_req_id_plus_1)
        .path("/wallet_info/spent");

    let cache = create_new_cache(1);
    let two_sec = Duration::from_secs(2);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());

    let filter =
        routes::wallet_info(&mut dp(), db, Default::default(), ks, cache).recover(handle_rejection);
    let res = request.reply(&filter).await;
    let r_s = request_spent.reply(&filter).await;
    let r_s_diff_id = request_spent_diff_id.reply(&filter).await;

    //
    // Assert
    //
    let expected_cached_response = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.0004365079365079365,\"running_total_tokens\":11,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.0004365079365079365,\"available_total_tokens\":11,\"item_total\":{},\"addresses\":{\"public_address\":[{\"out_point\":{\"t_hash\":\"tx_hash\",\"n\":0},\"value\":{\"Token\":11}}]}}}";
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), expected_cached_response);

    //test saved value is returned when the id is the same
    assert_eq!((r_s.status(), r_s.headers().clone()), success_json());
    assert_eq!(r_s.body(), expected_cached_response);

    //differnt id used
    assert_eq!(
        (r_s_diff_id.status(), r_s_diff_id.headers().clone()),
        success_json()
    );
    assert_eq!(r_s_diff_id.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d8\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.0004365079365079365,\"running_total_tokens\":11,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.0004365079365079365,\"available_total_tokens\":11,\"item_total\":{},\"addresses\":{\"public_address_spent\":[{\"out_point\":{\"t_hash\":\"tx_hash_spent\",\"n\":0},\"value\":{\"Token\":11}}]}}}");

    thread::sleep(two_sec);
    //repeat with same id after value expires
    let request_spent = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/wallet_info/spent");
    let r_s = request_spent.reply(&filter).await;
    assert_eq!((r_s.status(), r_s.headers().clone()), success_json());
    assert_eq!(r_s.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.0004365079365079365,\"running_total_tokens\":11,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.0004365079365079365,\"available_total_tokens\":11,\"item_total\":{},\"addresses\":{\"public_address_spent\":[{\"out_point\":{\"t_hash\":\"tx_hash_spent\",\"n\":0},\"value\":{\"Token\":11}}]}}}");
}

/// Test GET new payment address
#[tokio::test(flavor = "current_thread")]
async fn test_get_payment_address() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("").await;
    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/payment_address");
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::payment_address(&mut dp(), db.clone(), Default::default(), ks, cache)
        .recover(handle_rejection);
    let res = request.reply(&filter).await;
    let store_address = db.get_known_addresses().pop().unwrap();
    let expected = format!("{{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"New payment address generated\",\"route\":\"payment_address\",\"content\":\"{store_address}\"}}");

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), &expected);
}

/// Test GET all addresses on the UTXO set
#[tokio::test(flavor = "current_thread")]
async fn test_get_utxo_set_addresses() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //

    let tx_vals = vec![
        generate_transaction("tx_hash_1", "public_address_1"),
        generate_transaction("tx_hash_2", "public_address_2"),
        generate_transaction("tx_hash_3", "public_address_3"),
    ];

    let mempool = MempoolTest::new(tx_vals);
    let request = warp::test::request()
        .method("GET")
        .header("x-cache-id", COMMON_REQ_ID)
        .path("/utxo_addresses");

    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::utxo_addresses(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        ks,
        cache,
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _mempool = handle.await.unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(
        res.body(),
        "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"UTXO addresses successfully retrieved\",\"route\":\"utxo_addresses\",\"content\":[\"public_address_1\",\"public_address_2\",\"public_address_3\"]}"
    );
}

/*------- POST TESTS--------*/

/// Test POST for get blockchain block by key
#[tokio::test(flavor = "current_thread")]
async fn test_post_blockchain_entry_by_key_block() {
    let expected_meta = success_json();
    let expected_body = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Database item(s) successfully retrieved\",\"route\":\"blockchain_entry\",\"content\":{\"Block\":{\"block\":{\"header\":{\"version\":2,\"bits\":0,\"nonce_and_mining_tx_hash\":[[120,12,5,128,106,59,112,177,92,150,115,57,97,113,103,79],\"test\"],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"txs_merkle_root_and_hash\":[\"42fbcc73bc0eeb41a991a32a6f6e145d1d45b2738657db5b4781d1fa707693cf\",\"35260a02627ae9d586dbb9f11de79afd46d1096f41ffb6b9ee88cca6b78bf374\"]},\"transactions\":[\"g98d0ab9304ca82f098a86ad6251803b\"]}}}}";

    test_post_blockchain_entry_by_key(
        "b0004e829238707b7a600a95d3089e320448f706c2c7f6b0427201cc384c7fbfc",
        expected_meta.clone(),
        expected_body,
    )
    .await;
    test_post_blockchain_entry_by_key(
        "nIndexedBlockHashKey_0000000000000000",
        expected_meta,
        expected_body,
    )
    .await;
}

/// Test POST for get blockchain tx by key
#[tokio::test(flavor = "current_thread")]
async fn test_post_blockchain_entry_by_key_tx() {
    let expected_meta = success_json();
    let expected_body =
    "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Database item(s) successfully retrieved\",\"route\":\"blockchain_entry\",\"content\":{\"Transaction\":{\"inputs\":[],\"outputs\":[],\"version\":2,\"druid_info\":null}}}";

    test_post_blockchain_entry_by_key(
        "g98d0ab9304ca82f098a86ad6251803b",
        expected_meta.clone(),
        expected_body,
    )
    .await;
}

/// Test POST for get blockchain with wrong key
#[tokio::test(flavor = "current_thread")]
async fn test_post_blockchain_entry_by_key_failure() {
    let expected_meta = fail_json(StatusCode::NO_CONTENT);
    let expected_body = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"No data found for key\",\"route\":\"blockchain_entry\",\"content\":\"null\"}";

    test_post_blockchain_entry_by_key(
        "b6d369ad3595c1348772ad89e7ce314032687579f1bbe288b1a4d065a00000000",
        expected_meta.clone(),
        expected_body,
    )
    .await;
    test_post_blockchain_entry_by_key(
        "1842d4e51e99e14671077e4cac648339c3ca57e7219257fed707afd0f0000000",
        expected_meta.clone(),
        expected_body,
    )
    .await;
    test_post_blockchain_entry_by_key(
        "nIndexedTxHashKey_0000000000000011",
        expected_meta.clone(),
        expected_body,
    )
    .await;
    test_post_blockchain_entry_by_key(
        "nIndexedTxHashKey_0000000000000000_00000011",
        expected_meta.clone(),
        expected_body,
    )
    .await;
    test_post_blockchain_entry_by_key("Test", expected_meta, expected_body).await;
}

async fn test_post_blockchain_entry_by_key(
    key: &str,
    expected_meta: (StatusCode, HeaderMap),
    expected_body: &str,
) {
    let _ = tracing_log_try_init();

    let db = get_db_with_block().await;
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::blockchain_entry_by_key(&mut dp(), db, Default::default(), ks, cache)
        .recover(handle_rejection);

    let res = warp::test::request()
        .method("POST")
        .path("/blockchain_entry")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&key)
        .reply(&filter)
        .await;
    assert_eq!((res.status(), res.headers().clone()), expected_meta);
    assert_eq!(res.body(), expected_body);
}

/// Test POST for get block info by nums
#[tokio::test(flavor = "current_thread")]
async fn test_post_block_info_by_nums() {
    let _ = tracing_log_try_init();

    let db = get_db_with_block().await;
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter = routes::block_by_num(&mut dp(), db, Default::default(), ks, cache)
        .recover(handle_rejection);

    let res = warp::test::request()
        .method("POST")
        .path("/block_by_num")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&vec![0_u64, 10, 0])
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Database item(s) successfully retrieved\",\"route\":\"block_by_num\",\"content\":[[\"b0004e829238707b7a600a95d3089e320448f706c2c7f6b0427201cc384c7fbfc\",{\"block\":{\"header\":{\"version\":2,\"bits\":0,\"nonce_and_mining_tx_hash\":[[120,12,5,128,106,59,112,177,92,150,115,57,97,113,103,79],\"test\"],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"txs_merkle_root_and_hash\":[\"42fbcc73bc0eeb41a991a32a6f6e145d1d45b2738657db5b4781d1fa707693cf\",\"35260a02627ae9d586dbb9f11de79afd46d1096f41ffb6b9ee88cca6b78bf374\"]},\"transactions\":[\"g98d0ab9304ca82f098a86ad6251803b\"]}}],[\"\",\"\"],[\"b0004e829238707b7a600a95d3089e320448f706c2c7f6b0427201cc384c7fbfc\",{\"block\":{\"header\":{\"version\":2,\"bits\":0,\"nonce_and_mining_tx_hash\":[[120,12,5,128,106,59,112,177,92,150,115,57,97,113,103,79],\"test\"],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"txs_merkle_root_and_hash\":[\"42fbcc73bc0eeb41a991a32a6f6e145d1d45b2738657db5b4781d1fa707693cf\",\"35260a02627ae9d586dbb9f11de79afd46d1096f41ffb6b9ee88cca6b78bf374\"]},\"transactions\":[\"g98d0ab9304ca82f098a86ad6251803b\"]}}]]}");
}

/// Test POST for get transactions info by tx_hash
#[tokio::test(flavor = "current_thread")]
async fn test_post_transactions_by_key() {
    let _ = tracing_log_try_init();

    let db = get_db_with_block().await;
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter = routes::transactions_by_key(&mut dp(), db, Default::default(), ks, cache)
        .recover(handle_rejection);

    let res = warp::test::request()
        .method("POST")
        .path("/transactions_by_key")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&vec!["g98d0ab9304ca82f098a86ad6251803b".to_string()])
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Database item(s) successfully retrieved\",\"route\":\"transactions_by_key\",\"content\":[[\"g98d0ab9304ca82f098a86ad6251803b\",{\"inputs\":[],\"outputs\":[],\"version\":2,\"druid_info\":null}]]}");
}

/// Test POST make payment
// #[tokio::test(flavor = "current_thread")]
// async fn test_post_make_payment() {
//     let _ = tracing_log_try_init();

//     //
//     // Arrange
//     //
//     let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

//     let encapsulated_data = EncapsulatedPayment {
//         address: COMMON_PUB_ADDR.to_string(),
//         amount: TokenAmount(25),
//         passphrase: String::new(),
//         locktime: None,
//     };

//     let db = get_wallet_db(&encapsulated_data.passphrase).await;
//     let request = warp::test::request()
//         .method("POST")
//         .path("/make_payment")
//         .remote_addr(self_socket)
//         .header("Content-Type", "application/json")
//         .header("x-cache-id", COMMON_REQ_ID)
//         .json(&encapsulated_data);

//     //
//     // Act
//     //
//     let ks = to_api_keys(Default::default());
//     let cache = create_new_cache(CACHE_LIVE_TIME);
//     let filter = routes::make_payment(
//         &mut dp(),
//         db,
//         self_node.clone(),
//         Default::default(),
//         ks,
//         cache,
//     )
//     .recover(handle_rejection);
//     let res = request.reply(&filter).await;

//     //
//     // Assert
//     //
//     assert_eq!((res.status(), res.headers().clone()), success_json());
//     assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Payment processing\",\"route\":\"make_payment\",\"content\":{\"13bd3351b78beb2d0dadf2058dcc926c\":{\"asset\":{\"Token\":25},\"extra_info\":null}}}");

//     // Frame expected
//     let (address, amount, locktime) = (
//         encapsulated_data.address,
//         encapsulated_data.amount,
//         encapsulated_data.locktime,
//     );
//     let expected_frame = user_api_request_as_frame(UserApiRequest::MakePayment {
//         address,
//         amount,
//         locktime,
//     });
//     let actual_frame = next_event_frame(&mut self_node).await;
//     assert_eq!(expected_frame, actual_frame);
// }

/// Test POST make ip payment with correct address
#[tokio::test(flavor = "current_thread")]
async fn test_post_make_ip_payment() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

    let encapsulated_data = EncapsulatedPayment {
        address: "127.0.0.1:12345".to_owned(),
        amount: TokenAmount(25),
        passphrase: String::new(),
        locktime: None,
    };
    let db = get_wallet_db(&encapsulated_data.passphrase).await;
    let request = warp::test::request()
        .method("POST")
        .path("/make_ip_payment")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&encapsulated_data);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::make_ip_payment(
        &mut dp(),
        db,
        self_node.clone(),
        Default::default(),
        ks,
        cache,
    )
    .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"IP payment processing\",\"route\":\"make_ip_payment\",\"content\":{\"127.0.0.1:12345\":{\"asset\":{\"Token\":25},\"extra_info\":null}}}");

    // Frame expected
    let (payment_peer, amount, locktime) = (
        encapsulated_data.address.parse::<SocketAddr>().unwrap(),
        encapsulated_data.amount,
        encapsulated_data.locktime,
    );
    let expected_frame = user_api_request_as_frame(UserApiRequest::MakeIpPayment {
        payment_peer,
        amount,
        locktime,
    });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST construct address from public key
#[tokio::test(flavor = "current_thread")]
async fn test_address_construction() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mut address1 = BTreeMap::new();
    address1.insert(
        "pub_key",
        vec![
            109, 133, 37, 100, 46, 243, 13, 156, 189, 123, 142, 12, 24, 169, 49, 186, 187, 0, 63,
            27, 129, 207, 183, 13, 156, 208, 171, 164, 179, 118, 131, 183,
        ],
    );

    let mut address2 = BTreeMap::new();
    address2.insert(
        "pub_key_hex",
        "6d8525642ef30d9cbd7b8e0c18a931babb003f1b81cfb70d9cd0aba4b37683b7",
    );

    let address3 = AddressConstructData {
        pub_key_hex: Some(
            "6d8525642ef30d9cbd7b8e0c18a931babb003f1b81cfb70d9cd0aba4b37683b7".to_owned(),
        ),
        version: Some(0),
        ..Default::default()
    };

    let request1 = || {
        warp::test::request()
            .method("POST")
            .path("/address_construction")
            .header("x-cache-id", COMMON_REQ_ID)
            .header("Content-Type", "application/json")
    };

    let com_req_id_plus_1 = "2ae7bc9cba924e3cb73c0249893078d8";
    let request2 = || {
        warp::test::request()
            .method("POST")
            .path("/address_construction")
            .header("x-cache-id", com_req_id_plus_1)
            .header("Content-Type", "application/json")
    };

    let com_req_id_plus_2 = "2ae7bc9cba924e3cb73c0249893078d9";
    let request3 = || {
        warp::test::request()
            .method("POST")
            .path("/address_construction")
            .header("x-cache-id", com_req_id_plus_2)
            .header("Content-Type", "application/json")
    };
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter = routes::address_construction(&mut dp(), Default::default(), ks, cache)
        .recover(handle_rejection);
    let res1 = request1().json(&address1).reply(&filter).await;
    let res2 = request2().json(&address2).reply(&filter).await;
    let res3 = request3().json(&address3).reply(&filter).await;

    //
    // Assert
    //
    let expected1 = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Address successfully constructed\",\"route\":\"address_construction\",\"content\":\"ca0abdcd2826a77218af0914601ee34c7ff44127aab9d0671267b25a7d36946a\"}";
    let expected2 = "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d8\",\"status\":\"Success\",\"reason\":\"Address successfully constructed\",\"route\":\"address_construction\",\"content\":\"ca0abdcd2826a77218af0914601ee34c7ff44127aab9d0671267b25a7d36946a\"}";

    assert_eq!((res1.status(), res1.headers().clone()), success_json());
    assert_eq!(res1.body(), expected1);

    assert_eq!((res2.status(), res2.headers().clone()), success_json());
    assert_eq!(res2.body(), expected2);

    assert_eq!((res3.status(), res3.headers().clone()), success_json());
    assert_eq!(res3.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d9\",\"status\":\"Success\",\"reason\":\"Address successfully constructed\",\"route\":\"address_construction\",\"content\":\"56d5b6da467e6c588966967ef5405dd2\"}");
}

/// Test POST make ip payment with correct address
#[tokio::test(flavor = "current_thread")]
async fn test_post_request_donation() {
    let _ = tracing_log_try_init();

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
        .header("x-cache-id", COMMON_REQ_ID)
        .header("Content-Type", "application/json")
        .json(&address);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter =
        routes::request_donation(&mut dp(), self_node.clone(), Default::default(), ks, cache)
            .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Donation request sent\",\"route\":\"request_donation\",\"content\":\"null\"}");

    // Frame expected
    let expected_frame = user_api_request_as_frame(UserApiRequest::RequestDonation { paying_peer });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST import key-pairs
#[tokio::test(flavor = "current_thread")]
async fn test_post_import_keypairs_success() {
    let _ = tracing_log_try_init();
    let (self_node, _self_socket) = new_self_node(NodeType::User).await;
    let db = get_wallet_db("").await;
    let mut addresses: BTreeMap<String, AddressStoreHex> = BTreeMap::new();
    addresses.insert(
        COMMON_PUB_ADDR.to_string(),
        AddressStoreHex {
            public_key: COMMON_PUB_KEY.to_string(),
            secret_key: COMMON_SEC_KEY.to_string(),
            address_version: None,
        },
    );
    let imported_addresses = Addresses { addresses };
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::import_keypairs(
        &mut dp(),
        db.clone(),
        self_node,
        Default::default(),
        ks,
        cache,
    )
    .recover(handle_rejection);
    let wallet_addresses_before = db.get_known_addresses();

    let res = warp::test::request()
        .method("POST")
        .path("/import_keypairs")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&imported_addresses)
        .reply(&filter)
        .await;

    error!("res: {:?}", res);

    let wallet_addresses_after = db.get_known_addresses();

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    assert_eq!(wallet_addresses_before, Vec::<String>::new());
    assert_eq!(wallet_addresses_after, vec![COMMON_PUB_ADDR.to_string()]);
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Key-pairs successfully imported\",\"route\":\"import_keypairs\",\"content\":[\"13bd3351b78beb2d0dadf2058dcc926c\"]}");
}

#[tokio::test(flavor = "current_thread")]
async fn test_post_fetch_balance() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let tx_vals = vec![get_transaction()];
    let mempool = MempoolTest::new(tx_vals);
    let addresses = vec![COMMON_PUB_ADDR.to_string()];

    let request = warp::test::request()
        .method("POST")
        .path("/fetch_balance")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&addresses);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));

    let filter = routes::fetch_balance(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        ks,
        semaphore,
        cache,
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _mempool = handle.await.unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(
        res.body(),
        "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Balance successfully fetched\",\"route\":\"fetch_balance\",\"content\":{\"total\":{\"tokens\":25200,\"items\":{}},\"address_list\":{\"13bd3351b78beb2d0dadf2058dcc926c\":[{\"out_point\":{\"t_hash\":\"tx_hash\",\"n\":0},\"value\":{\"Token\":25200}}]}}}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_post_fetch_pending() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let tx_vals = get_rb_transactions();
    let mempool = MempoolTest::new(tx_vals);
    let druids = FetchPendingData {
        druid_list: vec!["full_druid".to_owned(), "non_existing_druid".to_owned()],
    };

    let request = warp::test::request()
        .method("POST")
        .path("/fetch_pending")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&druids);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::fetch_pending(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        ks,
        cache,
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _mempool = handle.await.unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(
        res.body(),
        "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Pending transactions successfully fetched\",\"route\":\"fetch_pending\",\"content\":{\"full_druid\":{\"participants\":2,\"txs\":{\"g490b4fc3b3953a8a006ec17ae4a6055\":{\"inputs\":[{\"previous_out\":{\"t_hash\":\"000001\",\"n\":0},\"script_signature\":{\"stack\":[{\"Bytes\":\"754dc248d1c847e8a10c6f8ded6ccad96381551ebb162583aea2a86b9bb78dfa\"},{\"Signature\":[21,103,185,228,19,36,74,158,249,211,229,41,187,113,248,98,27,55,85,97,36,94,216,242,20,156,39,245,55,212,95,22,52,161,77,8,211,241,24,217,126,208,39,154,87,136,126,31,154,177,219,197,151,174,148,122,67,147,4,59,177,191,172,8]},{\"PubKey\":[83,113,131,33,34,168,232,4,250,53,32,236,104,97,195,250,85,74,127,111,182,23,230,240,118,132,82,9,2,7,224,124]},{\"Op\":\"OP_DUP\"},{\"Op\":\"OP_HASH256\"},{\"PubKeyHash\":\"5423e6bd848e0ce5cd794e55235c23138d8833633cd2d7de7f4a10935178457b\"},{\"Op\":\"OP_EQUALVERIFY\"},{\"Op\":\"OP_CHECKSIG\"}]}}],\"outputs\":[{\"value\":{\"Token\":0},\"locktime\":0,\"drs_block_hash\":null,\"script_public_key\":null},{\"value\":{\"Item\":{\"amount\":1,\"genesis_hash\":\"genesis_hash\",\"metadata\":null}},\"locktime\":0,\"drs_block_hash\":null,\"script_public_key\":\"sender_address\"}],\"version\":5,\"druid_info\":{\"druid\":\"full_druid\",\"participants\":2,\"expectations\":[{\"from\":\"6efcefb27d1e1149b243ce319c5e5352bb100dc328a59f630ee7a9fd5ebe9da9\",\"to\":\"receiver_address\",\"asset\":{\"Token\":25200}}]}},\"g8118c848762693bda7be3f804601ad0\":{\"inputs\":[{\"previous_out\":{\"t_hash\":\"000000\",\"n\":0},\"script_signature\":{\"stack\":[{\"Bytes\":\"927b3411743452e5e0d73e9e40a4fa3c842b3d00dabde7f9af7e44661ce02c88\"},{\"Signature\":[35,226,158,202,184,227,77,178,40,234,140,161,109,206,131,187,171,159,103,146,89,201,220,227,212,184,216,166,69,26,92,67,221,248,253,165,17,176,190,4,48,76,146,12,179,195,90,227,170,17,196,234,76,57,254,242,83,89,237,117,68,193,105,10]},{\"PubKey\":[83,113,131,33,34,168,232,4,250,53,32,236,104,97,195,250,85,74,127,111,182,23,230,240,118,132,82,9,2,7,224,124]},{\"Op\":\"OP_DUP\"},{\"Op\":\"OP_HASH256\"},{\"PubKeyHash\":\"5423e6bd848e0ce5cd794e55235c23138d8833633cd2d7de7f4a10935178457b\"},{\"Op\":\"OP_EQUALVERIFY\"},{\"Op\":\"OP_CHECKSIG\"}]}}],\"outputs\":[{\"value\":{\"Token\":0},\"locktime\":0,\"drs_block_hash\":null,\"script_public_key\":null},{\"value\":{\"Token\":25200},\"locktime\":0,\"drs_block_hash\":null,\"script_public_key\":\"receiver_address\"}],\"version\":5,\"druid_info\":{\"druid\":\"full_druid\",\"participants\":2,\"expectations\":[{\"from\":\"b519b3fd271bb33a7ea949a918cc45b00b32095a04f2a9172797f7441f7298e6\",\"to\":\"sender_address\",\"asset\":{\"Item\":{\"amount\":1,\"genesis_hash\":\"genesis_hash\",\"metadata\":null}}}]}}}}}}");
}

/// Test POST update running total successful
#[tokio::test(flavor = "current_thread")]
async fn test_post_update_running_total() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let (mut self_node, _self_socket) = new_self_node(NodeType::User).await;
    let db = get_wallet_db("old_passphrase").await;

    let addresses = vec![COMMON_PUB_ADDR.to_string()];

    let address_list = UtxoFetchType::AnyOf(addresses.clone());

    let request = warp::test::request()
        .method("POST")
        .path("/update_running_total")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&addresses);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::update_running_total(
        &mut dp(),
        self_node.clone(),
        db,
        Default::default(),
        ks,
        cache,
    )
    .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Running total updated\",\"route\":\"update_running_total\",\"content\":\"null\"}");

    // Expected Frame
    let expected_frame =
        user_api_request_as_frame(UserApiRequest::UpdateWalletFromUtxoSet { address_list });
    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST create item asset on mempool node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_transactions() {
    test_post_create_transactions_common(None).await;
}

/// Test POST create item asset on mempool node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_transactions_v0() {
    test_post_create_transactions_common(Some(NETWORK_VERSION_V0)).await;
}

/// Test POST create item asset on mempool node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_transactions_temp() {
    test_post_create_transactions_common(Some(NETWORK_VERSION_TEMP)).await;
}

async fn test_post_create_transactions_common(address_version: Option<u64>) {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(Vec::new());

    let previous_out = OutPoint::new(COMMON_PUB_ADDR.to_owned(), 0);
    let signable_data = construct_tx_in_signable_hash(&previous_out);
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let raw_signature = sign::sign_detached(signable_data.as_bytes(), &secret_key);
    let signature = hex::encode(raw_signature.as_ref());
    let public_key = COMMON_PUB_KEY.to_owned();

    println!("raw_signature: {}", hex::encode(raw_signature.as_ref()));

    let json_body = vec![CreateTransaction {
        inputs: vec![CreateTxIn {
            previous_out: Some(previous_out.clone()),
            script_signature: Some(CreateTxInScript::Pay2PkH {
                signable_data: Some(signable_data),
                signature,
                public_key,
                address_version,
            }),
        }],
        outputs: vec![TxOut {
            value: Asset::Token(TokenAmount(1)),
            script_public_key: Some(COMMON_ADDRS[0].to_owned()),
            locktime: 0,
        }],
        fees: Some(vec![TxOut {
            value: Asset::Token(TokenAmount(1)),
            script_public_key: Some(COMMON_ADDRS[0].to_owned()),
            locktime: 0,
        }]),
        version: 1,
        druid_info: None,
    }];

    let request = warp::test::request()
        .method("POST")
        .path("/create_transactions")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let filter = routes::create_transactions(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        ks,
        semaphore,
        cache,
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _mempool = handle.await.unwrap();

    //
    // Assert
    //
    let expected_response_body = match address_version {
        Some(NETWORK_VERSION_V0) => "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Transaction(s) processing\",\"route\":\"create_transactions\",\"content\":{\"g13b6549a078915299a6081f54f0baa8\":[\"0008536e3d5a13e347262b5023963000\",{\"asset\":{\"Token\":1},\"extra_info\":null}]}}",
        Some(NETWORK_VERSION_TEMP) => "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Transaction(s) processing\",\"route\":\"create_transactions\",\"content\":{\"gf171a06123a55797394de71faa8bb47\":[\"0008536e3d5a13e347262b5023963000\",{\"asset\":{\"Token\":1},\"extra_info\":null}]}}",
        None => "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Transaction(s) processing\",\"route\":\"create_transactions\",\"content\":{\"g52f00edbab20a75dc235ca1d68cf1bf\":[\"0008536e3d5a13e347262b5023963000\",{\"asset\":{\"Token\":1},\"extra_info\":null}]}}",
        _ => Default::default()
    };
    assert_eq!(
        ((res.status(), res.headers().clone()), from_utf8(res.body())),
        (success_json(), expected_response_body)
    );
}

/// Test POST serialize_transactions successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_serialize_transactions() {
    test_post_serialize_transactions_common(None).await;
}

/// Test POST serialize_transactions successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_serialize_transactions_v0() {
    test_post_serialize_transactions_common(Some(NETWORK_VERSION_V0)).await;
}

/// Test POST serialize_transactions successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_serialize_transactions_temp() {
    test_post_serialize_transactions_common(Some(NETWORK_VERSION_TEMP)).await;
}

async fn test_post_serialize_transactions_common(address_version: Option<u64>) {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let previous_out = OutPoint::new(COMMON_PUB_ADDR.to_owned(), 0);
    let signable_data = construct_tx_in_signable_hash(&previous_out);
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let raw_signature = sign::sign_detached(signable_data.as_bytes(), &secret_key);
    let signature = hex::encode(raw_signature.as_ref());
    let public_key = COMMON_PUB_KEY.to_owned();

    let json_body = vec![CreateTransaction {
        inputs: vec![CreateTxIn {
            previous_out: Some(previous_out.clone()),
            script_signature: Some(CreateTxInScript::Pay2PkH {
                signable_data: Some(signable_data),
                signature,
                public_key,
                address_version,
            }),
        }],
        outputs: vec![TxOut {
            value: Asset::Token(TokenAmount(1)),
            script_public_key: Some(COMMON_ADDRS[0].to_owned()),
            locktime: 0,
        }],
        fees: Some(vec![TxOut {
            value: Asset::Token(TokenAmount(1)),
            script_public_key: Some(COMMON_ADDRS[0].to_owned()),
            locktime: 0,
        }]),
        version: 1,
        druid_info: None,
    }];

    let request = warp::test::request()
        .method("POST")
        .path("/serialize_transactions")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());

    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::serialize_transactions(&mut dp(), Default::default(), ks, cache)
        .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected_response_body = match address_version {
        Some(NETWORK_VERSION_V0) => "0100000000000000012000000000000000313362643333353162373862656232643064616466323035386463633932366300000000080000000000000004000000400000000000000032656335333833323233373964343534326166366137363062306433353233383562396235333238366431343361373630313836356231653731333462636564010000004000000000000000505acb926928d876fd24451812d68d88be0e6f4900d943a081895b7ed34b9f4e96236b30cfec1ccdf9c28420725dbb3df81b3431fec4f2d733432db888e8440f0200000020000000000000005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c00000000230000000000000051000000040000002000000000000000313362643333353162373862656232643064616466323035386463633932366300000000350000000000000053000000010000000000000000000000010000000000000000000000000000000120000000000000003030303835333665336435613133653334373236326235303233393633303030010000000000000001000000000000000000000001000000000000000000000000000000012000000000000000303030383533366533643561313365333437323632623530323339363330303000",
        Some(NETWORK_VERSION_TEMP) => "0100000000000000012000000000000000313362643333353162373862656232643064616466323035386463633932366300000000080000000000000004000000400000000000000032656335333833323233373964343534326166366137363062306433353233383562396235333238366431343361373630313836356231653731333462636564010000004000000000000000505acb926928d876fd24451812d68d88be0e6f4900d943a081895b7ed34b9f4e96236b30cfec1ccdf9c28420725dbb3df81b3431fec4f2d733432db888e8440f0200000020000000000000005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c000000002300000000000000520000000400000040000000000000003663366236653865396466386336336432326439656236383762393637316464316365356438396631393562623233313665316231343434383438636432623300000000350000000000000053000000010000000000000000000000010000000000000000000000000000000120000000000000003030303835333665336435613133653334373236326235303233393633303030010000000000000001000000000000000000000001000000000000000000000000000000012000000000000000303030383533366533643561313365333437323632623530323339363330303000",
        None => "0100000000000000012000000000000000313362643333353162373862656232643064616466323035386463633932366300000000080000000000000004000000400000000000000032656335333833323233373964343534326166366137363062306433353233383562396235333238366431343361373630313836356231653731333462636564010000004000000000000000505acb926928d876fd24451812d68d88be0e6f4900d943a081895b7ed34b9f4e96236b30cfec1ccdf9c28420725dbb3df81b3431fec4f2d733432db888e8440f0200000020000000000000005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c000000002300000000000000500000000400000040000000000000003534323365366264383438653063653563643739346535353233356332333133386438383333363333636432643764653766346131303933353137383435376200000000350000000000000053000000010000000000000000000000010000000000000000000000000000000120000000000000003030303835333665336435613133653334373236326235303233393633303030010000000000000001000000000000000000000001000000000000000000000000000000012000000000000000303030383533366533643561313365333437323632623530323339363330303000",
        _ => Default::default()
    };
    let expected_response_hash = construct_tx_hash(
        &bincode::deserialize::<Transaction>(
            hex::decode(expected_response_body).unwrap().as_slice(),
        )
        .unwrap(),
    );

    let expected_response_body = format!(
        "{{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Transaction(s) serialized\",\"route\":\"serialize_transactions\",\"content\":[{{\"txn_hash_hex\":\"{}\",\"txn_hex\":\"{}\"}}]}}", 
        expected_response_hash, expected_response_body);

    assert_eq!(
        ((res.status(), res.headers().clone()), from_utf8(res.body())),
        (success_json(), expected_response_body.as_str())
    );
}

/// Test POST deserialize_transactions successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_deserialize_transactions() {
    test_post_deserialize_transactions_common(None).await;
}

/// Test POST deserialize_transactions successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_deserialize_transactions_v0() {
    test_post_deserialize_transactions_common(Some(NETWORK_VERSION_V0)).await;
}

/// Test POST deserialize_transactions successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_deserialize_transactions_temp() {
    test_post_deserialize_transactions_common(Some(NETWORK_VERSION_TEMP)).await;
}

async fn test_post_deserialize_transactions_common(address_version: Option<u64>) {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let previous_out = OutPoint::new(COMMON_PUB_ADDR.to_owned(), 0);
    let signable_data = construct_tx_in_signable_hash(&previous_out);
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let raw_signature = sign::sign_detached(signable_data.as_bytes(), &secret_key);
    let signature = raw_signature;
    let public_key = COMMON_PUB_KEY;

    let json_body = vec![hex::encode(
        serialize(&Transaction {
            inputs: vec![TxIn {
                previous_out: Some(previous_out.clone()),
                script_signature: Script::pay2pkh(
                    signable_data,
                    signature.to_owned(),
                    PublicKey::from_slice(&hex::decode(public_key).unwrap()).unwrap(),
                    address_version,
                ),
            }],
            outputs: vec![TxOut {
                value: Asset::Token(TokenAmount(1)),
                script_public_key: Some(COMMON_ADDRS[0].to_owned()),
                locktime: 0,
            }],
            fees: vec![TxOut {
                value: Asset::Token(TokenAmount(1)),
                script_public_key: Some(COMMON_ADDRS[0].to_owned()),
                locktime: 0,
            }],
            version: 1,
            druid_info: None,
        })
        .unwrap(),
    )];

    let request = warp::test::request()
        .method("POST")
        .path("/deserialize_transactions")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());

    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::deserialize_transactions(&mut dp(), Default::default(), ks, cache)
        .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    let expected_response_body = match address_version {
        Some(NETWORK_VERSION_V0) => "{\"inputs\":[{\"previous_out\":{\"t_hash\":\"13bd3351b78beb2d0dadf2058dcc926c\",\"n\":0},\"script_signature\":{\"stack\":[{\"Bytes\":\"2ec538322379d4542af6a760b0d352385b9b53286d143a7601865b1e7134bced\"},{\"Signature\":\"505acb926928d876fd24451812d68d88be0e6f4900d943a081895b7ed34b9f4e96236b30cfec1ccdf9c28420725dbb3df81b3431fec4f2d733432db888e8440f\"},{\"PubKey\":\"5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c\"},{\"Op\":\"OP_DUP\"},{\"Op\":\"OP_HASH256_V0\"},{\"Bytes\":\"13bd3351b78beb2d0dadf2058dcc926c\"},{\"Op\":\"OP_EQUALVERIFY\"},{\"Op\":\"OP_CHECKSIG\"}]}}],\"outputs\":[{\"value\":{\"Token\":1},\"locktime\":0,\"script_public_key\":\"0008536e3d5a13e347262b5023963000\"}],\"version\":1,\"fees\":[{\"value\":{\"Token\":1},\"locktime\":0,\"script_public_key\":\"0008536e3d5a13e347262b5023963000\"}],\"druid_info\":null}",
        Some(NETWORK_VERSION_TEMP) => "{\"inputs\":[{\"previous_out\":{\"t_hash\":\"13bd3351b78beb2d0dadf2058dcc926c\",\"n\":0},\"script_signature\":{\"stack\":[{\"Bytes\":\"2ec538322379d4542af6a760b0d352385b9b53286d143a7601865b1e7134bced\"},{\"Signature\":\"505acb926928d876fd24451812d68d88be0e6f4900d943a081895b7ed34b9f4e96236b30cfec1ccdf9c28420725dbb3df81b3431fec4f2d733432db888e8440f\"},{\"PubKey\":\"5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c\"},{\"Op\":\"OP_DUP\"},{\"Op\":\"OP_HASH256_TEMP\"},{\"Bytes\":\"6c6b6e8e9df8c63d22d9eb687b9671dd1ce5d89f195bb2316e1b1444848cd2b3\"},{\"Op\":\"OP_EQUALVERIFY\"},{\"Op\":\"OP_CHECKSIG\"}]}}],\"outputs\":[{\"value\":{\"Token\":1},\"locktime\":0,\"script_public_key\":\"0008536e3d5a13e347262b5023963000\"}],\"version\":1,\"fees\":[{\"value\":{\"Token\":1},\"locktime\":0,\"script_public_key\":\"0008536e3d5a13e347262b5023963000\"}],\"druid_info\":null}",
        None => "{\"inputs\":[{\"previous_out\":{\"t_hash\":\"13bd3351b78beb2d0dadf2058dcc926c\",\"n\":0},\"script_signature\":{\"stack\":[{\"Bytes\":\"2ec538322379d4542af6a760b0d352385b9b53286d143a7601865b1e7134bced\"},{\"Signature\":\"505acb926928d876fd24451812d68d88be0e6f4900d943a081895b7ed34b9f4e96236b30cfec1ccdf9c28420725dbb3df81b3431fec4f2d733432db888e8440f\"},{\"PubKey\":\"5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c\"},{\"Op\":\"OP_DUP\"},{\"Op\":\"OP_HASH256\"},{\"Bytes\":\"5423e6bd848e0ce5cd794e55235c23138d8833633cd2d7de7f4a10935178457b\"},{\"Op\":\"OP_EQUALVERIFY\"},{\"Op\":\"OP_CHECKSIG\"}]}}],\"outputs\":[{\"value\":{\"Token\":1},\"locktime\":0,\"script_public_key\":\"0008536e3d5a13e347262b5023963000\"}],\"version\":1,\"fees\":[{\"value\":{\"Token\":1},\"locktime\":0,\"script_public_key\":\"0008536e3d5a13e347262b5023963000\"}],\"druid_info\":null}",
        _ => Default::default()
    };

    let expected_response_body = format!(
        "{{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Transaction(s) deserialized\",\"route\":\"deserialize_transactions\",\"content\":[{}]}}",
        expected_response_body);

    assert_eq!(
        ((res.status(), res.headers().clone()), from_utf8(res.body())),
        (success_json(), expected_response_body.as_str())
    );
}

/// Test POST create item asset on mempool node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_item_asset_tx_mempool() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(Vec::new());

    let asset_hash = construct_tx_in_signable_asset_hash(&Asset::item(1, None, None));
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let signature = hex::encode(sign::sign_detached(asset_hash.as_bytes(), &secret_key).as_ref());

    let json_body = CreateItemAssetDataMempool {
        item_amount: 1,
        script_public_key: COMMON_PUB_ADDR.to_owned(),
        public_key: COMMON_PUB_KEY.to_owned(),
        signature,
        genesis_hash_spec: GenesisTxHashSpec::Default,
        metadata: None,
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_item_asset")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));

    let filter = routes::create_item_asset(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        ks,
        semaphore,
        cache,
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _mempool = handle.await.unwrap();

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Item asset(s) created\",\"route\":\"create_item_asset\",\"content\":{\"asset\":{\"asset\":{\"Item\":{\"amount\":1,\"genesis_hash\":\"default_genesis_hash\",\"metadata\":null}},\"extra_info\":null},\"to_address\":\"13bd3351b78beb2d0dadf2058dcc926c\",\"tx_hash\":\"default_genesis_hash\"}}");
}

/// Test POST create item asset on user node successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_item_asset_tx_user() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let (mut self_node, self_socket) = new_self_node(NodeType::User).await;

    let json_body = CreateItemAssetDataUser {
        item_amount: 1,
        genesis_hash_spec: GenesisTxHashSpec::Default,
        metadata: Some("metadata".to_owned()),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_item_asset")
        .remote_addr(self_socket)
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter =
        routes::create_item_asset_user(&mut dp(), self_node.clone(), Default::default(), ks, cache)
            .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Item asset(s) created\",\"route\":\"create_item_asset\",\"content\":1}");

    // Expected Frame
    let expected_frame = user_api_request_as_frame(UserApiRequest::SendCreateItemRequest {
        item_amount: json_body.item_amount,
        genesis_hash_spec: GenesisTxHashSpec::Default,
        metadata: json_body.metadata,
    });

    let actual_frame = next_event_frame(&mut self_node).await;
    assert_eq!(expected_frame, actual_frame);
}

/// Test POST create item asset on mempool node failure
#[tokio::test(flavor = "current_thread")]
async fn test_post_create_item_asset_tx_mempool_failure() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(Vec::new());

    let json_body = CreateItemAssetDataUser {
        item_amount: 1,
        genesis_hash_spec: GenesisTxHashSpec::Default,
        metadata: Some("metadata".to_owned()),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/create_item_asset")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let filter = routes::create_item_asset(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        ks,
        semaphore,
        cache,
    )
    .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!(
        (res.status(), res.headers().clone()),
        fail_json(StatusCode::BAD_REQUEST)
    );
    assert_eq!(res.body(), "{\"id\":\"null\",\"status\":\"Error\",\"reason\":\"Bad request\",\"route\":\"null\",\"content\":\"null\"}");
}

/// Test POST change passphrase successfully
#[tokio::test(flavor = "current_thread")]
async fn test_post_change_passphrase() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mut db = get_wallet_db("old_passphrase").await;
    let (payment_address, expected_address_store) = db.generate_payment_address();

    let json_body = ChangePassphraseData {
        old_passphrase: String::from("old_passphrase"),
        new_passphrase: String::from("new_passphrase"),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/change_passphrase")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);

    let filter = routes::change_passphrase(&mut dp(), db.clone(), Default::default(), ks, cache)
        .recover(handle_rejection);
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

    assert!(matches!(actual, Ok(())), "{}", "{actual:?}");
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Passphrase changed successfully\",\"route\":\"change_passphrase\",\"content\":\"null\"}");
}

/// Test POST change passphrase failure
#[tokio::test(flavor = "current_thread")]
async fn test_post_change_passphrase_failure() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("old").await;
    let json_body = ChangePassphraseData {
        old_passphrase: String::from("invalid_passphrase"),
        new_passphrase: String::from("new_passphrase"),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/change_passphrase")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter = routes::change_passphrase(&mut dp(), db.clone(), Default::default(), ks, cache)
        .recover(handle_rejection);
    let actual = db.test_passphrase(String::from("new_passphrase")).await;
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert!(
        matches!(actual, Err(WalletDbError::PassphraseError)),
        "{}",
        "{actual:?}"
    );
    assert_eq!(
        (res.status(), res.headers().clone()),
        fail_json(StatusCode::UNAUTHORIZED)
    ); // TODO: Convert to fail_json
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"Invalid passphrase\",\"route\":\"change_passphrase\",\"content\":\"null\"}");
}

/// Test POST change blank passphrase failure
#[tokio::test(flavor = "current_thread")]
async fn test_post_change_blank_passphrase_failure() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_wallet_db("old_passphrase").await;
    let json_body = ChangePassphraseData {
        old_passphrase: String::from("old_passphrase"),
        new_passphrase: String::from(""),
    };

    let request = warp::test::request()
        .method("POST")
        .path("/change_passphrase")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&json_body.clone());
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter = routes::change_passphrase(&mut dp(), db.clone(), Default::default(), ks, cache)
        .recover(handle_rejection);
    let actual = db.test_passphrase(String::from("")).await;
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert!(
        matches!(actual, Err(WalletDbError::PassphraseError)),
        "{}",
        "{actual:?}"
    );
    assert_eq!(
        (res.status(), res.headers().clone()),
        fail_json(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Error\",\"reason\":\"New passphrase cannot be blank\",\"route\":\"change_passphrase\",\"content\":\"null\"}");
}

/// Test POST fetch block hashes for blocks that contain given `tx_hashes`
#[tokio::test(flavor = "current_thread")]
async fn test_post_block_nums_by_tx_hashes() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let db = get_db_with_block().await;

    let request = warp::test::request()
        .method("POST")
        .path("/check_transaction_presence")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&vec!["g393e26d47ede87b84808c1a5664ea41"]);
    //
    // Act
    //
    let ks = to_api_keys(Default::default());
    let cache = create_new_cache(CACHE_LIVE_TIME);
    let filter = routes::blocks_by_tx_hashes(&mut dp(), db, Default::default(), ks, cache)
        .recover(handle_rejection);
    let res = request.reply(&filter).await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Database item(s) successfully retrieved\",\"route\":\"check_transaction_presence\",\"content\":[]}");
}

/// Test POST pause nodes
#[tokio::test(flavor = "current_thread")]
async fn test_post_pause_nodes() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(Default::default());
    let request = warp::test::request()
        .method("POST")
        .path("/pause_nodes")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&1_u64);

    //
    // Act
    //
    let filter = routes::pause_nodes(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        Default::default(),
        create_new_cache(CACHE_LIVE_TIME),
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _ = handle.await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"\",\"route\":\"pause_nodes\",\"content\":\"null\"}");
}

/// Test POST resume nodes
#[tokio::test(flavor = "current_thread")]
async fn test_post_resume_nodes() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let mempool = MempoolTest::new(Default::default());
    let request = warp::test::request()
        .method("POST")
        .path("/resume_nodes")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID);

    //
    // Act
    //
    let filter = routes::resume_nodes(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        Default::default(),
        create_new_cache(CACHE_LIVE_TIME),
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _ = handle.await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"\",\"route\":\"resume_nodes\",\"content\":\"null\"}");
}

/// Test POST update shared config
#[tokio::test(flavor = "current_thread")]
async fn test_post_update_shared_config() {
    let _ = tracing_log_try_init();

    //
    // Arrange
    //
    let shared_config_body = MempoolNodeSharedConfig {
        mempool_mining_event_timeout: 10000,
        mempool_partition_full_size: 5,
        mempool_miner_whitelist: Default::default(),
    };
    let mempool = MempoolTest::new(Default::default());
    let request = warp::test::request()
        .method("POST")
        .path("/update_shared_config")
        .header("Content-Type", "application/json")
        .header("x-cache-id", COMMON_REQ_ID)
        .json(&shared_config_body);

    //
    // Act
    //
    let filter = routes::update_shared_config(
        &mut dp(),
        mempool.threaded_calls.tx.clone(),
        Default::default(),
        Default::default(),
        create_new_cache(CACHE_LIVE_TIME),
    )
    .recover(handle_rejection);
    let handle = mempool.spawn();
    let res = request.reply(&filter).await;
    let _ = handle.await;

    //
    // Assert
    //
    assert_eq!((res.status(), res.headers().clone()), success_json());
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"\",\"route\":\"update_shared_config\",\"content\":\"null\"}");
}
