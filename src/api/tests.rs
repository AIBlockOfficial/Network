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

    if !validate_pow_block(&block.header) {
        block.header = generate_pow_for_block(block.header);
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
    assert_eq!(res.body(), "{\"id\":\"2ae7bc9cba924e3cb73c0249893078d7\",\"status\":\"Success\",\"reason\":\"Wallet info successfully fetched\",\"route\":\"wallet_info\",\"content\":{\"running_total\":0.04365079365079365\",\"running_total_tokens\":1100,\"locked_total\":0.0,\"locked_total_tokens\":0,\"available_total\":0.04365079365079365\",\"available_total_tokens\":1100,\"item_total\":{},\"addresses\":{\"public_address\":[{\"out_point\":{\"t_hash\":\"tx_hash0\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash1\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash10\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash11\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash12\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash13\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash14\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash15\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash16\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash17\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash18\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash19\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash2\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash20\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash21\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash22\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash23\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash24\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash25\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash26\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash27\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash28\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash29\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash3\",\"n\":0},\"value\":{\"Token\":11}},{\"out_point\":{\"t_hash\":\"tx_hash30\",\"n\":0},\"value\":{\"Token\":11}}]}}}");
}