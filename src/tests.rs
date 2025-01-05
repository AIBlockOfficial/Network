```rust
//! Test suite for the network functions.

use crate::configurations::{
    MempoolNodeSharedConfig, TxOutSpec, UserAutoGenTxSetup, UtxoSetSpec, WalletTxSpec,
};
use crate::constants::{NETWORK_VERSION, SANC_LIST_TEST};
use crate::interfaces::{
    BlockStoredInfo, BlockchainItem, BlockchainItemMeta, BlockchainItemType, CommonBlockInfo,
    DruidPool, MempoolApi, MempoolRequest, MinedBlock, MinedBlockExtraInfo, Response,
    StorageRequest, StoredSerializingBlock, UserApiRequest, UserRequest, UtxoFetchType, UtxoSet,
    WinningPoWInfo,
};
use crate::mempool::MempoolNode;
use crate::mempool_raft::MinerWhitelist;
use crate::miner::MinerNode;
use crate::storage::{all_ordered_stored_block_tx_hashes, StorageNode};
use crate::storage_raft::CompleteBlock;
use crate::test_utils::{
    generate_rb_transactions, get_test_tls_spec, map_items, node_join_all_checked,
    remove_all_node_dbs, Network, NetworkConfig, NodeType, RbReceiverData, RbSenderData,
};
use crate::tracked_utxo::TrackedUtxoBalance;
use crate::transactor::Transactor;
use crate::user::UserNode;
use crate::utils::{
    apply_mining_tx, calculate_reward, construct_coinbase_tx, construct_valid_block_pow_hash,
    create_valid_transaction_with_ins_outs, decode_pub_key, decode_secret_key,
    generate_pow_for_block, get_sanction_addresses, tracing_log_try_init, LocalEvent, StringError,
};
use bincode::{deserialize, deserialize_from};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error, error_span, info};
use tracing_futures::Instrument;
use tw_chain::crypto::sha3_256;
use tw_chain::crypto::sign_ed25519 as sign;
use tw_chain::crypto::sign_ed25519::{PublicKey, SecretKey};
use tw_chain::primitives::asset::{Asset, AssetValues, TokenAmount};
use tw_chain::primitives::block::{Block, BlockHeader};
use tw_chain::primitives::druid::DruidExpectation;
use tw_chain::primitives::transaction::{GenesisTxHashSpec, OutPoint, Transaction, TxOut};
use tw_chain::script::StackEntry;
use tw_chain::utils::transaction_utils::{
    construct_address, construct_item_create_tx, construct_tx_hash,
    construct_tx_in_signable_asset_hash, get_tx_out_with_out_point_cloned,
};

const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);

#[cfg(not(debug_assertions))] // Release
const TEST_DURATION_DIVIDER: usize = 10;
#[cfg(debug_assertions)] // Debug
const TEST_DURATION_DIVIDER: usize = 1;

const SEED_UTXO: &[(i32, &str)] = &[
    (1, "00000000000000000000000000000000"),
    (3, "00000000000000000000000000000001"),
    (1, "00000000000000000000000000000002"),
];
const VALID_TXS_IN: &[(i32, &str)] = &[
    (0, "00000000000000000000000000000000"),
    (0, "00000000000000000000000000000001"),
    (1, "00000000000000000000000000000001"),
];
const VALID_TXS_OUT: &[&str] = &[
    "00000000000000000000000000000101",
    "00000000000000000000000000000102",
    "00000000000000000000000000000103",
];
const DEFAULT_SEED_AMOUNT: TokenAmount = TokenAmount(3);

const BLOCK_RECEIVED: &str = "Block received to be added";
const BLOCK_STORED: &str = "Block complete stored";
const BLOCK_RECEIVED_AND_STORED: [&str; 2] = [BLOCK_RECEIVED, BLOCK_STORED];

const SOME_PUB_KEYS: [&str; 3] = [
    COMMON_PUB_KEY,
    "6e86cc1fc5efbe64c2690efbb966b9fe1957facc497dce311981c68dac88e08c",
    "8b835e00c57ebff6637ec32276f2c6c0df71129c8f0860131a78a4692a0b59dc",
];

const SOME_SEC_KEYS: [&str; 3] = [
    COMMON_SEC_KEY,
    "3053020101300506032b65700422042070391d510eb988291d2dca15e5b8a54c552b4f2361bf29b1b945300a3e7cc9b4a1230321006e86cc1fc5efbe64c2690efbb966b9fe1957facc497dce311981c68dac88e08c",
    "3053020101300506032b6570042204201842082f6d8b0ecf75a309544980027e15ed1c00e95a14063705b2a4fc358670a1230321008b835e00c57ebff6637ec32276f2c6c0df71129c8f0860131a78a4692a0b59dc",
];

const SOME_PUB_KEY_ADDRS: [&str; 3] = [
    COMMON_PUB_ADDR,
    "77516e2d91606250e625546f86702510d2e893e4a27edfc932fdba03c955cc1b",
    "4cfd64a6692021fc417368a866d33d94e1c806747f61ac85e0b3935e7d5ed925",
];

const COMMON_PUB_KEY: &str = "5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_SEC_KEY: &str = "3053020101300506032b6570042204200186bc08f16428d2059227082b93e439ff50f8c162f24b9594b132f2cc15fca4a1230321005371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_PUB_ADDR: &str = "5423e6bd848e0ce5cd794e55235c23138d8833633cd2d7de7f4a10935178457b";

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Cfg {
    All,
    IgnoreStorage,
    IgnoreMempool,
    IgnoreWaitTxComplete,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CfgNum {
    All,
    Majority,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CfgPow {
    First,
    Parallel,
}

#[derive(Clone, Debug)]
enum CfgModif {
    Drop(&'static str),
    Respawn(&'static str),
    HandleEvents(&'static [(&'static str, &'static str)]),
    RestartEventsAll(&'static [(NodeType, &'static str)]),
    RestartUpgradeEventsAll(&'static [(NodeType, &'static str)]),
    Disconnect(&'static str),
    Reconnect(&'static str),
}

#[test]
fn will_get_sanctioned_addresses() {
    let addresses = get_sanction_addresses(SANC_LIST_TEST.to_string(), "US");
    assert!(addresses.contains(&"gjlkhflgkhdfklg".to_string()));

    let no_ju_addresses = get_sanction_addresses(SANC_LIST_TEST.to_string(), "UK");
    assert_eq!(no_ju_addresses, Vec::<String>::new());

    let no_fs_addresses = get_sanction_addresses("/blah.json".to_string(), "UK");
    assert_eq!(no_fs_addresses, Vec::<String>::new());
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_no_raft() {
    full_flow(complete_network_config(10500)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_real_db_1_node() {
    let mut cfg = complete_network_config_with_n_mempool_raft(10505, 1);
    cfg.in_memory_db = false;

    remove_all_node_dbs(&cfg);
    full_flow(cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_1_no_tls_node() {
    full_flow_no_tls(complete_network_config_with_n_mempool_raft(10510, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_1_node() {
    full_flow(complete_network_config_with_n_mempool_raft(10515, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_2_nodes() {
    full_flow(complete_network_config_with_n_mempool_raft(10520, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_3_nodes() {
    full_flow(complete_network_config_with_n_mempool_raft(10530, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_majority_3_nodes() {
    full_flow_tls(
        complete_network_config_with_n_mempool_raft(10540, 3),
        CfgNum::Majority,
        Vec::new(),
    )
    .await;
}

// Only run locally - unstable on repository pipeline
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn full_flow_raft_majority_15_nodes() {
    full_flow_tls(
        complete_network_config_with_n_mempool_raft(10550, 15),
        CfgNum::Majority,
        Vec::new(),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_multi_miners_no_raft() {
    full_flow_multi_miners(complete_network_config_with_n_mempool_miner(
        11000, false, 1, 3,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_single_miner_single_raft_with_aggregation_tx_check() {
    test_step_start();
    //
    // Arrange
    //
    let network_config = complete_network_config_with_n_mempool_miner(11030, true, 1, 1);
    let mut network = Network::create_from_config(&network_config).await;
    let active_nodes = network.all_active_nodes().clone();
    let miner_addr = &active_nodes[&NodeType::Miner][0];
    let mempool_addr = &active_nodes[&NodeType::Mempool][0];
    let mut prev_mining_reward = TokenAmount(0);
    let address_aggregation_limit = network_config.address_aggregation_limit.unwrap_or_default();

    //
    // Act
    //

    // Genesis block
    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    let mut handle_aggregation_tx: bool;
    // Create more blocks
    for _ in 1..(address_aggregation_limit * 5) + 1 {
        create_block_act(&mut network, Cfg::All, CfgNum::All).await;

        // Check if the miner is _about_ to send aggregation tx
        {
            let miner_node = network.miner(miner_addr).unwrap();

            let addrs = miner_node
                .lock()
                .await
                .get_wallet_db()
                .get_known_addresses()
                .len();
            handle_aggregation_tx = addrs % (address_aggregation_limit - 1) == 0;
        }

        if handle_aggregation_tx {
            prev_mining_reward = mempool_get_prev_mining_reward(&mut network, mempool_addr).await;
        }

        proof_of_work_act(
            &mut network,
            CfgPow::Parallel,
            CfgNum::All,
            handle_aggregation_tx,
            Some(prev_mining_reward),
        )
        .await;

        send_block_to_storage_act(&mut network, CfgNum::All).await;
    }

    //
    // Assert
    //

    // Assert that the miner has aggregated the winnings under a single address
    let miner_node = network.miner(miner_addr).unwrap();
    let txs = miner_node
        .lock()
        .await
        .get_wallet_db()
        .get_fund_store()
        .transactions()
        .len();

    assert_eq!(txs, 1);

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_single_miner_single_raft_with_static_miner_address_check() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config_with_n_mempool_miner(11040, true, 1, 1);
    let mut network = Network::create_from_config(&network_config).await;
    let active_nodes = network.all_active_nodes().clone();
    let miner = &active_nodes[&NodeType::Miner][0];
    let mempool = &active_nodes[&NodeType::Mempool][0];
    let user = &active_nodes[&NodeType::User][0];
    let static_addr = user_generate_static_address_for_miner(&mut network, user).await;

    // Update miner's static winning address
    miner_set_static_miner_address(&mut network, miner, static_addr.clone()).await;

    user_update_running_total(&mut network, user).await;
    let initial_tokens = user_get_tokens_held(&mut network, user).await;

    //
    // Act
    //

    // Genesis block
    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    // Run the network to mine 2 blocks
    for _ in 0..2 {
        create_block_act(&mut network, Cfg::All, CfgNum::All).await;
        proof_of_work_act(&mut network, CfgPow::Parallel, CfgNum::All, false, None).await;
        send_block_to_storage_act(&mut network, CfgNum::All).await;
    }

    // Refresh the User's wallet
    request_utxo_set_act(
        &mut network,
        user,
        mempool,
        UtxoFetchType::AnyOf(vec![static_addr.clone()]),
    )
    .await;

    //
    // Assert
    //
    user_update_running_total(&mut network, user).await;
    let tokens_after_mining = user_get_tokens_held(&mut network, user).await;

    assert_eq!(initial_tokens, TokenAmount(0));
    assert_eq!(tokens_after_mining, TokenAmount(7510185)); // 7510185 is the amount of tokens won after mining 2 blocks
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_multi_miners_raft_1_node() {
    full_flow_multi_miners(complete_network_config_with_n_mempool_miner(
        11010, true, 1, 3,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_multi_miners_raft_2_nodes() {
    full_flow_multi_miners(complete_network_config_with_n_mempool_miner(
        11020, true, 2, 6,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_kill_forever_miner_node_3_nodes() {
    let modify_cfg = vec![("After create block 0", CfgModif::Drop("miner2"))];
    let network_config = complete_network_config_with_n_mempool_raft(11130, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_kill_forever_mempool_node_3_nodes() {
    let modify_cfg = vec![("After create block 0", CfgModif::Drop("mempool2"))];

    let network_config = complete_network_config_with_n_mempool_raft(11140, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_kill_forever_storage_node_3_nodes() {
    let modify_cfg = vec![("After create block 0", CfgModif::Drop("storage2"))];

    let network_config = complete_network_config_with_n_mempool_raft(11150, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_kill_miner_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Drop("miner2")),
        ("After create block 1", CfgModif::Respawn("miner2")),
        (
            "After create block 1",
            CfgModif::HandleEvents(&[("mempool2", "Received partition request successfully")]),
        ),
    ];
    let network_config = complete_network_config_with_n_mempool_raft(11160, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_kill_mempool_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Drop("mempool2")),
        ("After create block 1", CfgModif::Respawn("mempool2")),
        (
            "After create block 1",
            CfgModif::HandleEvents(&[
                ("mempool2", "Snapshot applied"),
                ("mempool2", "Winning PoW intake open"),
                ("mempool2", "Pipeline halted"),
                ("mempool2", "Transactions committed"),
                ("mempool2", "Block committed"),
            ]),
        ),
    ];

    let network_config = complete_network_config_with_n_mempool_raft(11170, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_kill_storage_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Drop("storage2")),
        ("After create block 1", CfgModif::Respawn("storage2")),
        (
            "After create block 1",
            CfgModif::HandleEvents(&[("storage2", BLOCK_STORED)]),
        ),
    ];

    let network_config = complete_network_config_with_n_mempool_raft(11180, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_dis_and_re_connect_miner_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Disconnect("miner2")),
        ("After create block 1", CfgModif::Reconnect("miner2")),
    ];
    let network_config = complete_network_config_with_n_mempool_raft(11190, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_dis_and_re_connect_mempool_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Disconnect("mempool2")),
        ("After create block 1", CfgModif::Reconnect("mempool2")),
        (
            "After create block 1",
            CfgModif::HandleEvents(&[
                ("mempool2", "Winning PoW intake open"),
                ("mempool2", "Pipeline halted"),
                ("mempool2", "Transactions committed"),
                ("mempool2", "Block committed"),
            ]),
        ),
    ];

    let network_config = complete_network_config_with_n_mempool_raft(11200, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_flow_raft_dis_and_re_connect_storage_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Disconnect("storage2")),
        ("After create block 1", CfgModif::Reconnect("storage2")),
        (
            "After create block 1",
            CfgModif::HandleEvents(&[("storage2", BLOCK_STORED)]),
        ),
    ];

    let network_config = complete_network_config_with_n_mempool_raft(11220, 3);
    full_flow_tls(network_config, CfgNum::All, modify_cfg).await;
}

async fn full_flow_multi_miners(mut network_config: NetworkConfig) {
    network_config.mempool_partition_full_size = 2;
    network_config.mempool_minimum_miner_pool_len = 3;
    full_flow(network_config).await;
}

async fn full_flow(network_config: NetworkConfig) {
    full_flow_tls(network_config, CfgNum::All, Vec::new()).await;
}

async fn full_flow_no_tls(network_config: NetworkConfig) {
    full_flow_common(network_config, CfgNum::All, Vec::new()).await;
}

async fn full_flow_tls(
    mut network_config: NetworkConfig,
    cfg_num: CfgNum,
    modify_cfg: Vec<(&str, CfgModif)>,
) {
    network_config.tls_config = get_test_tls_spec();
    full_flow_common(network_config, cfg_num, modify_cfg).await;
}

async fn full_flow_common(
    network_config: NetworkConfig,
    cfg_num: CfgNum,
    modify_cfg: Vec<(&str, CfgModif)>,
) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let initial_utxo_txs = network.collect_initial_uxto_txs();
    let mining_reward = network.mining_reward();
    let transactions = valid_transactions(true);

    //
    // Act
    //
    create_first_block_act(&mut network).await;
    modify_network(&mut network, "After create block 0", &modify_cfg).await;
    proof_of_work_act(&mut network, CfgPow::First, cfg_num, false, None).await;
    send_block_to_storage_act(&mut network, cfg_num).await;
    let stored0 = storage_get_last_block_stored(&mut network, "storage1").await;

    add_transactions_act(&mut network, &transactions).await;
    create_block_act(&mut network, Cfg::All, cfg_num).await;
    modify_network(&mut network, "After create block 1", &modify_cfg).await;

    proof_of_work_act(&mut network, CfgPow::Parallel, cfg_num, false, None).await;
    send_block_to_storage_act(&mut network, cfg_num).await;
    let stored1 = storage_get_last_block_stored(&mut network, "storage1").await;

    create_block_act(&mut network, Cfg::All, cfg_num).await;

    //
    // Assert
    //
    let active_nodes = network.all_active_nodes().clone();
    let storage_nodes = &active_nodes[&NodeType::Storage];
    let miner_nodes = &active_nodes[&NodeType::Miner];

    let actual1 = storage_all_get_last_stored_info(&mut network, storage_nodes).await;
    assert_eq!(equal_first(&actual1), node_all(storage_nodes, true));

    let stored0 = stored0.unwrap();
    let stored1 = stored1.unwrap();
    let stored0_mining_tx_len = stored0.mining_transactions.len();
    let stored1_mining_tx_len = stored1.mining_transactions.len();
    assert_eq!((stored0_mining_tx_len, stored1_mining_tx_len), (1, 1));

    let actual0_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    let expected_block0_db_count = 1 + initial_utxo_txs.len() + stored0_mining_tx_len;
    let expected_block1_db_count = 1 + transactions.len() + stored1_mining_tx_len;
    assert_eq!(
        actual0_db_count,
        node_all(
            storage_nodes,
            expected_block0_db_count + expected_block1_db_count
        )
    );

    let actual_w0 = node_all_combined_get_wallet_info(&mut network, miner_nodes).await;
    let expected_w0 =
        node_all_combined_expected_wallet_info(miner_nodes, mining_reward, &[&stored0]);
    assert_eq!((actual_w0.0, actual_w0.1.len(), actual_w0.2), expected_w0);

    test_step_complete(network).await;
}

async fn modify_network(network: &mut Network, tag: &str, modif_config: &[(&str, CfgModif)]) {
    for (_tag, modif) in modif_config.iter().filter(|(t, _)| tag == *t) {
        match modif {
            CfgModif::Drop(v) => network.close_loops_and_drop_named(&[v.to_string()]).await,
            CfgModif::Respawn(v) => {
                let nodes = vec![v.to_string()];
                network.re_spawn_nodes_named(&nodes).await;
                network.send_startup_requests_named(&nodes).await;
            }
            CfgModif::HandleEvents(es) => {
                let all_nodes = network.all_active_nodes_name_vec();
                let all_raisons = network.all_active_nodes_events(|_, n| {
                    let es_for_t = es.iter().filter(|(ne, _)| *ne == n);
                    es_for_t.map(|(_, e)| e.to_string()).collect()
                });
                node_all_handle_different_event(network, &all_nodes, &all_raisons).await
            }
            CfgModif::RestartEventsAll(es) | CfgModif::RestartUpgradeEventsAll(es) => {
                let all_nodes = network.all_active_nodes_name_vec();
                let all_raisons = network.all_active_nodes_events(|t, _| {
                    let es_for_t = es.iter().filter(|(te, _)| *te == t);
                    es_for_t.map(|(_, e)| e.to_string()).collect()
                });
                network.close_loops_and_drop_named(&all_nodes).await;
                if matches!(modif, CfgModif::RestartUpgradeEventsAll(_)) {
                    network.upgrade_closed_nodes().await;
                }
                network.re_spawn_nodes_named(&all_nodes).await;
                network.send_startup_requests_named(&all_nodes).await;
                if !es.is_empty() {
                    node_all_handle_different_event(network, &all_nodes, &all_raisons).await;
                }
            }
            CfgModif::Disconnect(v) => network.disconnect_nodes_named(&[v.to_string()]).await,
            CfgModif::Reconnect(v) => {
                network.re_connect_nodes_named(&[v.to_string()]).await;
                let mut event_tx = network.get_local_event_tx(v).await.unwrap();
                event_tx
                    .send(
                        LocalEvent::ReconnectionComplete,
                        "reconnection complete test",
                    )
                    .await
                    .unwrap();

                let event = Some((
                    v.to_string(),
                    vec!["Sent startup requests on reconnection".to_string()],
                ))
                .into_iter()
                .collect();
                node_all_handle_different_event(network, &[v.to_string()], &event).await;

                // Process miner's request at mempool node
                if let Some(miner) = network.miner(v) {
                    let mempool_addr = miner.lock().await.mempool_address();

                    let mempool_nodes = network.all_active_nodes()[&NodeType::Mempool].clone();
                    for c in mempool_nodes {
                        if network.mempool(&c).unwrap().lock().await.local_address() == mempool_addr
                        {
                            mempool_handle_event(
                                network,
                                &c,
                                &["Received partition request successfully"],
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn create_first_block_no_raft() {
    create_first_block(complete_network_config(10000)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_first_block_raft_1_node() {
    create_first_block(complete_network_config_with_n_mempool_raft(10010, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_first_block_raft_2_nodes() {
    create_first_block(complete_network_config_with_n_mempool_raft(10020, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_first_block_raft_3_nodes() {
    create_first_block(complete_network_config_with_n_mempool_raft(10030, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_first_block_raft_15_nodes() {
    create_first_block(complete_network_config_with_n_mempool_raft(10040, 15)).await;
}

async fn create_first_block(network_config: NetworkConfig) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let expected_utxo = to_utxo_set(&network.collect_initial_uxto_txs());

    //
    // Act
    //
    create_first_block_act(&mut network).await;

    //
    // Assert
    //
    let utxo_set_after = mempool_all_committed_utxo_set(&mut network, mempool_nodes).await;
    assert_eq!(utxo_set_after, node_all(mempool_nodes, expected_utxo));

    test_step_complete(network).await;
}

async fn create_first_block_act(network: &mut Network) {
    let config = network.config().clone();
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let first_request_size = config.mempool_minimum_miner_pool_len;

    info!("Test Step Connect nodes");
    for mempool in mempool_nodes {
        let miners = &config.mempool_to_miner_mapping[mempool];
        for (idx, miner) in miners.iter().enumerate() {
            node_send_startup_requests(network, miner).await;
            let evt = if idx == first_request_size - 1 {
                "Received first full partition request"
            } else {
                "Received partition request successfully"
            };
            mempool_handle_event(network, mempool, &[evt]).await;
        }
    }

    info!("Test Step Create first Block");
    node_all_handle_event(network, mempool_nodes, &["First Block committed"]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_first_block_to_storage_no_raft() {
    send_first_block_to_storage(complete_network_config(10800)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_first_block_to_storage_raft_1_node() {
    send_first_block_to_storage(complete_network_config_with_n_mempool_raft(10810, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_first_block_to_storage_raft_2_nodes() {
    send_first_block_to_storage(complete_network_config_with_n_mempool_raft(10820, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_first_block_to_storage_raft_3_nodes() {
    send_first_block_to_storage(complete_network_config_with_n_mempool_raft(10830, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_first_block_to_storage_raft_majority_3_nodes() {
    send_first_block_to_storage_common(
        complete_network_config_with_n_mempool_raft(10840, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_first_block_to_storage_raft_15_nodes() {
    send_first_block_to_storage(complete_network_config_with_n_mempool_raft(10850, 15)).await;
}

async fn send_first_block_to_storage(network_config: NetworkConfig) {
    send_first_block_to_storage_common(network_config, CfgNum::All).await;
}

async fn send_first_block_to_storage_common(network_config: NetworkConfig, cfg_num: CfgNum) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let initial_utxo_txs = network.collect_initial_uxto_txs();
    let (expected0, block_info0) = complete_first_block(&initial_utxo_txs).await;

    create_first_block_act(&mut network).await;
    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;

    //
    // Act
    //
    send_block_to_storage_act(&mut network, cfg_num).await;

    //
    // Assert
    //
    let actual0 = storage_all_get_last_stored_info(&mut network, storage_nodes).await;
    assert_eq!(
        actual0[0],
        (
            Some(expected0.1),
            Some((expected0.0, 0 /*b_num*/, 1 /*single mining tx*/,))
        )
    );
    assert_eq!(equal_first(&actual0), node_all(storage_nodes, true));

    let actual0_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    assert_eq!(
        actual0_db_count,
        node_all(storage_nodes, 1 + initial_utxo_txs.len() + 1)
    );

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_no_raft() {
    add_transactions(complete_network_config(10600)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_1_node() {
    add_transactions(complete_network_config_with_n_mempool_raft(10610, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_restart_tx_raft_1_node() {
    let tag = "After add local Transactions";
    let modify_cfg = vec![
        (tag, CfgModif::Drop("mempool1")),
        (tag, CfgModif::Respawn("mempool1")),
        (
            tag,
            CfgModif::HandleEvents(&[("mempool1", "Snapshot applied")]),
        ),
    ];

    let network_config = complete_network_config_with_n_mempool_raft(10615, 1);
    add_transactions_common(network_config, &modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_2_nodes() {
    add_transactions(complete_network_config_with_n_mempool_raft(10620, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_3_nodes() {
    add_transactions(complete_network_config_with_n_mempool_raft(10630, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_15_nodes() {
    add_transactions(complete_network_config_with_n_mempool_raft(10640, 15)).await;
}

async fn add_transactions(network_config: NetworkConfig) {
    add_transactions_common(network_config, &[]).await;
}

async fn add_transactions_common(network_config: NetworkConfig, modify_cfg: &[(&str, CfgModif)]) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let transactions = valid_transactions(true);

    create_first_block_act(&mut network).await;

    //
    // Act
    //
    add_transactions_act_with(&mut network, &transactions, Cfg::IgnoreWaitTxComplete).await;
    modify_network(&mut network, "After add local Transactions", modify_cfg).await;
    add_transactions_act_with(&mut network, &Default::default(), Cfg::All).await;

    //
    // Assert
    //
    let actual = mempool_all_committed_tx_pool(&mut network, mempool_nodes).await;
    assert_eq!(actual[0], transactions);
    assert_eq!(equal_first(&actual), node_all(mempool_nodes, true));

    test_step_complete(network).await;
}

async fn add_transactions_act(network: &mut Network, txs: &BTreeMap<String, Transaction>) {
    add_transactions_act_with(network, txs, Cfg::All).await;
}

async fn add_transactions_act_with(
    network: &mut Network,
    txs: &BTreeMap<String, Transaction>,
    cfg: Cfg,
) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];

    for tx in txs.values() {
        info!("Test Step Add Transactions");
        user_send_transaction_to_mempool(network, "user1", "mempool1", tx).await;
    }
    for _tx in txs.values() {
        mempool_handle_event(network, "mempool1", &["Transactions added to tx pool"]).await;
    }

    if cfg != Cfg::IgnoreWaitTxComplete {
        node_all_handle_event(network, mempool_nodes, &["Transactions committed"]).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_no_raft() {
    create_block(complete_network_config(10100)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_1_node() {
    create_block(complete_network_config_with_n_mempool_raft(10110, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_2_nodes() {
    create_block(complete_network_config_with_n_mempool_raft(10120, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_3_nodes() {
    create_block(complete_network_config_with_n_mempool_raft(10130, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_majority_3_nodes() {
    create_block_common(
        complete_network_config_with_n_mempool_raft(10140, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_15_nodes() {
    create_block(complete_network_config_with_n_mempool_raft(10150, 15)).await;
}

async fn create_block(network_config: NetworkConfig) {
    create_block_common(network_config, CfgNum::All).await;
}

async fn create_block_common(network_config: NetworkConfig, cfg_num: CfgNum) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let transactions = valid_transactions(true);
    let transactions_h = transactions.keys().cloned().collect::<Vec<_>>();
    let transactions_utxo = to_utxo_set(&transactions);
    let (_, block_info0) = complete_first_block(&network.collect_initial_uxto_txs()).await;
    let block0_mining_tx = complete_block_mining_txs(&block_info0);
    let block0_mining_utxo = to_utxo_set(&block0_mining_tx);

    let mut left_init_utxo = to_utxo_set(&network.collect_initial_uxto_txs());
    remove_keys(&mut left_init_utxo, valid_txs_in().keys());

    create_first_block_act(&mut network).await;
    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    add_transactions_act(&mut network, &transactions).await;

    //
    // Act
    //
    let block_transaction_before =
        mempool_all_current_block_transactions(&mut network, mempool_nodes).await;

    create_block_act(&mut network, Cfg::All, cfg_num).await;

    let block_transaction_after =
        mempool_all_current_block_transactions(&mut network, mempool_nodes).await;
    let utxo_set_after = mempool_all_committed_utxo_set(&mut network, mempool_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_transaction_before, node_all(mempool_nodes, None));
    assert_eq!(
        block_transaction_after,
        node_all(mempool_nodes, Some(transactions_h))
    );

    let expected_utxo = merge_txs_3(&left_init_utxo, &transactions_utxo, &block0_mining_utxo);
    assert_eq!(len_and_map(&utxo_set_after[0]), len_and_map(&expected_utxo));
    assert_eq!(equal_first(&utxo_set_after), node_all(mempool_nodes, true));

    test_step_complete(network).await;
}

async fn create_block_act(network: &mut Network, cfg: Cfg, cfg_num: CfgNum) {
    create_block_act_with(network, cfg, cfg_num, 0).await
}

async fn create_block_act_with(network: &mut Network, cfg: Cfg, cfg_num: CfgNum, block_num: u64) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let (msg_c_nodes, msg_s_nodes) = node_combined_select(
        &network.config().nodes[&NodeType::Mempool],
        &network.config().nodes[&NodeType::Storage],
        network.dead_nodes(),
        cfg_num,
    );

    info!("Test Step Storage signal new block");

    if cfg == Cfg::IgnoreStorage {
        let block = BlockStoredInfo {
            block_num,
            ..Default::default()
        };
        let req = MempoolRequest::SendBlockStored(block);
        mempool_all_inject_next_event(network, &msg_s_nodes, &msg_c_nodes, req).await;
    } else {
        storage_all_send_stored_block(network, &msg_s_nodes).await;
    }
    mempool_all_handle_event(network, &msg_c_nodes, "Received block stored").await;

    info!("Test Step Generate Block");
    node_all_handle_event(network, mempool_nodes, &["Block committed"]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_with_seed() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config_with_n_mempool_raft(11600, 1);
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let mempool = &mempool_nodes[0];
    let transactions = valid_transactions(true);
    let (_, block_info0) = complete_first_block(&network.collect_initial_uxto_txs()).await;

    //
    // Act
    //
    create_first_block_act(&mut network).await;
    let block0 = mempool_current_mining_block(&mut network, mempool).await;
    let seed0 = block0.as_ref().map(|b| b.header.seed_value.as_slice());
    let seed0 = seed0.and_then(|s| std::str::from_utf8(s).ok());

    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    add_transactions_act(&mut network, &transactions).await;
    create_block_act(&mut network, Cfg::All, CfgNum::All).await;
    let block1 = mempool_current_mining_block(&mut network, mempool).await;
    let seed1 = block1.as_ref().map(|b| b.header.seed_value.as_slice());
    let seed1 = seed1.and_then(|s| std::str::from_utf8(s).ok());

    //
    // Assert
    //
    let expected_seed0 = Some("102382207707718734792748219972459444372508601011471438062299221139355828742917-4092482189202844858141461446747443254065713079405017303649880917821984131927979764736076459305831834761744847895656303682167530187457916798745160233343351193");
    let expected_seed1 = Some("106228469607590785158439105345663906454407776332951582911215086478921447072494-890801478181097874306473746009596725559978631952297198866826970472802729293224119401323816857729365460297789241934487318750450632705046684127753444374170213");
    assert_eq!((seed0, seed1), (expected_seed0, expected_seed1));

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_no_raft() {
    proof_of_work(complete_network_config(10200)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_raft_1_node() {
    proof_of_work(complete_network_config_with_n_mempool_raft(10210, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_raft_2_nodes() {
    proof_of_work(complete_network_config_with_n_mempool_raft(10220, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_raft_3_nodes() {
    proof_of_work(complete_network_config_with_n_mempool_raft(10230, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_raft_majority_3_nodes() {
    proof_of_work_common(
        complete_network_config_with_n_mempool_raft(10240, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_multi_no_raft() {
    let mut cfg = complete_network_config_with_n_mempool_miner(10250, false, 1, 3);
    cfg.mempool_partition_full_size = 2;
    cfg.mempool_minimum_miner_pool_len = 3;
    proof_of_work(cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_multi_raft_1_node() {
    let mut cfg = complete_network_config_with_n_mempool_miner(10260, true, 1, 3);
    cfg.mempool_partition_full_size = 2;
    cfg.mempool_minimum_miner_pool_len = 3;
    proof_of_work(cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_multi_raft_2_nodes() {
    let mut cfg = complete_network_config_with_n_mempool_miner(10270, true, 2, 6);
    cfg.mempool_partition_full_size = 2;
    cfg.mempool_minimum_miner_pool_len = 3;
    proof_of_work(cfg).await;
}

async fn proof_of_work(network_config: NetworkConfig) {
    proof_of_work_common(network_config, CfgNum::All).await;
}

async fn proof_of_work_common(network_config: NetworkConfig, cfg_num: CfgNum) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];

    create_first_block_act(&mut network).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;

    //
    // Act
    //
    let block_before = mempool_all_mined_block_num(&mut network, mempool_nodes).await;

    proof_of_work_act(&mut network, CfgPow::First, cfg_num, false, None).await;
    proof_of_work_send_more_act(&mut network, cfg_num).await;

    let block_after = mempool_all_mined_block_num(&mut network, mempool_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_before, node_all(mempool_nodes, None));
    assert_eq!(block_after, node_all(mempool_nodes, Some(1)));

    test_step_complete(network).await;
}

async fn proof_of_work_act(
    network: &mut Network,
    cfg_pow: CfgPow,
    cfg_num: CfgNum,
    handle_aggregation_tx: bool,
    prev_mining_reward: Option<TokenAmount>,
) {
    proof_of_work_participation_act(network, cfg_num, cfg_pow).await;
    proof_of_work_block_act(network, cfg_num, handle_aggregation_tx, prev_mining_reward).await;
}

async fn proof_of_work_participation_act(network: &mut Network, cfg_num: CfgNum, cfg_pow: CfgPow) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let c_mined = &node_select(mempool_nodes, cfg_num);
    let active_mempool_to_miner_mapping = network.active_mempool_to_miner_mapping().clone();
    const POWS_COMPLETE: [&str; 2] = ["Partition PoW complete", "Block PoW complete"];

    info!("Test Step Miner block Proof of Work: partition-> rand num -> num pow -> pre-block -> block pow");
    if cfg_pow == CfgPow::First {
        for mempool in c_mined {
            let c_miners = &active_mempool_to_miner_mapping.get(mempool).unwrap();
            mempool_flood_rand_and_block_to_partition(network, mempool).await;
            miner_all_handle_event(network, c_miners, "Received random number successfully").await;
            for miner in c_miners.iter() {
                miner_handle_event(network, miner, "Partition PoW complete").await;
                miner_process_found_partition_pow(network, miner).await;
                mempool_handle_event(network, mempool, &["Partition PoW received successfully"])
                    .await;
            }
        }

        node_all_handle_event(network, mempool_nodes, &["Winning PoW intake open"]).await;
    }
}

async fn proof_of_work_block_act(
    network: &mut Network,
    cfg_num: CfgNum,
    handle_aggregation_tx: bool,
    prev_mining_reward: Option<TokenAmount>,
) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let c_mined = &node_select(mempool_nodes, cfg_num);
    let active_mempool_to_miner_mapping = network.active_mempool_to_miner_mapping().clone();

    for mempool in c_mined {
        let c_miners = &active_mempool_to_miner_mapping.get(mempool).unwrap();
        let in_miners = mempool_get_filtered_participants(network, mempool, c_miners).await;
        let in_miners = &in_miners;
        mempool_flood_rand_and_block_to_partition(network, mempool).await;
        // TODO: Better to validate all miners => needs to have the miner with barrier as well.

        let all_evts = block_and_partition_evt_in_miner_pow(c_miners, in_miners);
        node_all_handle_different_event(network, c_miners, &all_evts).await;
        mempool_flood_transactions_to_partition(network, mempool).await;
        miner_all_handle_event(network, in_miners, "Block is valid").await;

        for miner in c_miners.iter() {
            miner_process_found_partition_pow(network, miner).await;
            // `handle_aggregation_tx` indicates that an aggregation tx has been sent by the miner
            // Mempool node needs to handle this special case
            if handle_aggregation_tx {
                // Supplied as an array because these Tx event and Partition event can happen in any order
                let results = &[
                    "Transactions added to tx pool",
                    "Transactions committed",
                    "Partition PoW received successfully",
                ];
                mempool_handle_event(network, mempool, results).await;
                mempool_handle_event(network, mempool, results).await;
                mempool_handle_event(network, mempool, results).await;
            } else {
                // Mempool node needs to process the UTXO request sent by the miner
                if let Some(addr) = miner_has_aggregation_tx_active(network, miner).await {
                    mempool_handle_event(network, mempool, &["Received UTXO fetch request"]).await;

                    // Send the UTXO set to the miner
                    {
                        let mempool_node = network.mempool(mempool).unwrap();
                        mempool_node
                            .lock()
                            .await
                            .send_fetched_utxo_set()
                            .await
                            .unwrap();
                    }

                    // Miner handles the UTXO set and updates its balance
                    miner_handle_event(network, miner, "Received UTXO set").await;

                    {
                        let miner_node = network.miner(miner).unwrap();
                        miner_node.lock().await.update_running_total().await;

                        let running_total = miner_node
                            .lock()
                            .await
                            .get_wallet_db()
                            .get_fund_store()
                            .running_total()
                            .clone();

                        let utxo_bal = mempool_get_utxo_balance_for_addresses(
                            network,
                            mempool,
                            vec![addr.clone()],
                        )
                        .await
                        .get_asset_values()
                        .tokens;

                        if let Some(amount) = prev_mining_reward {
                            // Negate the mining reward from the running total as this check
                            // happens in the next immediate block after the aggregation tx is processed.
                            let actual_running_total = running_total.tokens - amount;
                            assert_eq!(utxo_bal, actual_running_total);
                        }
                    }
                }
                mempool_handle_event(network, mempool, &["Partition PoW received successfully"])
                    .await;
            }
        }
        for miner in in_miners {
            miner_process_found_block_pow(network, miner).await;
            mempool_handle_event(network, mempool, &["Received PoW successfully"]).await;
        }
    }
    node_all_handle_event(network, mempool_nodes, &["Pipeline halted"]).await;
}

async fn proof_of_work_send_more_act(network: &mut Network, cfg_num: CfgNum) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let c_mined = &node_select(mempool_nodes, cfg_num);
    let active_mempool_to_miner_mapping = network.active_mempool_to_miner_mapping().clone();

    info!("Test Step Miner block Proof of Work to late");
    for mempool in c_mined {
        let c_miners = active_mempool_to_miner_mapping.get(mempool).unwrap();
        let in_miners = mempool_get_filtered_participants(network, mempool, c_miners).await;
        for miner in &in_miners {
            miner_process_found_block_pow(network, miner).await;
            mempool_handle_error(network, mempool, &["Not block currently mined"]).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn proof_winner_no_raft() {
    proof_winner(complete_network_config(10900)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_winner_raft_1_node() {
    proof_winner(complete_network_config_with_n_mempool_raft(10910, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_winner_multi_no_raft() {
    proof_winner_multi(complete_network_config_with_n_mempool_miner(
        10920, false, 1, 3,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_winner_multi_raft_1_node() {
    proof_winner_multi(complete_network_config_with_n_mempool_miner(
        10930, true, 1, 3,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_winner_multi_raft_2_nodes() {
    proof_winner_multi(complete_network_config_with_n_mempool_miner(
        10940, true, 2, 6,
    ))
    .await;
}

async fn proof_winner_multi(mut network_config: NetworkConfig) {
    network_config.mempool_partition_full_size = 2;
    network_config.mempool_minimum_miner_pool_len = 3;
    proof_winner(network_config).await;
}

async fn proof_winner(network_config: NetworkConfig) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let miner_nodes = &network_config.nodes[&NodeType::Miner];
    let mining_reward = network.mining_reward();

    create_first_block_act(&mut network).await;

    //
    // Act
    // Does not allow miner reuse.
    //
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    create_block_act(&mut network, Cfg::All, CfgNum::All).await;

    let info_before = node_all_get_wallet_info(&mut network, miner_nodes).await;
    proof_winner_act(&mut network).await;
    let info_after = node_all_get_wallet_info(&mut network, miner_nodes).await;
    let winner_index = info_after.iter().position(|i| !i.0.is_empty());
    let winner_index = winner_index.unwrap_or_default();

    //
    // Assert
    //
    let expected_before = node_all(miner_nodes, (AssetValues::default(), 1, 0));
    let mut expected_after = expected_before.clone();
    expected_after[winner_index] = (AssetValues::new(mining_reward, Default::default()), 2, 1);
    assert_eq!(
        info_before
            .iter()
            .map(|i| (i.0.clone(), i.1.len(), i.2.len()))
            .collect::<Vec<_>>(),
        expected_before,
        "Info Before: {info_before:?}"
    );
    assert_eq!(
        info_after
            .iter()
            .map(|i| (i.0.clone(), i.1.len(), i.2.len()))
            .collect::<Vec<_>>(),
        expected_after,
        "Info After: {info_after:?}"
    );

    test_step_complete(network).await;
}

async fn proof_winner_act(network: &mut Network) {
    let config = network.config().clone();
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let miner_nodes = &active_nodes[&NodeType::Miner];

    if miner_nodes.len() < mempool_nodes.len() {
        info!("Test Step Miner winner: Ignored/Miner re-use");
        return;
    }

    info!("Test Step Miner winner:");
    for mempool in mempool_nodes {
        let c_miners = &config.mempool_to_miner_mapping.get(mempool).unwrap();
        let in_miners = mempool_get_filtered_participants(network, mempool, c_miners).await;
        let in_miners = &in_miners;

        mempool_flood_rand_and_block_to_partition(network, mempool).await;
        let all_evts = block_and_partition_evt_in_miner_pow(c_miners, in_miners);
        node_all_handle_different_event(network, c_miners, &all_evts).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn send_block_to_storage_no_raft() {
    send_block_to_storage(complete_network_config(10300)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_block_to_storage_raft_1_node() {
    send_block_to_storage(complete_network_config_with_n_mempool_raft(10310, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_block_to_storage_raft_2_nodes() {
    send_block_to_storage(complete_network_config_with_n_mempool_raft(10320, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_block_to_storage_raft_3_nodes() {
    send_block_to_storage(complete_network_config_with_n_mempool_raft(10330, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_block_to_storage_raft_majority_3_nodes() {
    send_block_to_storage_common(
        complete_network_config_with_n_mempool_raft(10340, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn send_block_to_storage_raft_15_nodes() {
    send_block_to_storage(complete_network_config_with_n_mempool_raft(10350, 15)).await;
}

async fn send_block_to_storage(network_config: NetworkConfig) {
    send_block_to_storage_common(network_config, CfgNum::All).await;
}

async fn send_block_to_storage_common(network_config: NetworkConfig, cfg_num: CfgNum) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let initial_utxo_txs = network.collect_initial_uxto_txs();
    let (expected0, block_info0) = complete_first_block(&initial_utxo_txs).await;

    create_first_block_act(&mut network).await;
    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;

    //
    // Act
    //
    send_block_to_storage_act(&mut network, cfg_num).await;

    //
    // Assert
    //
    let actual0 = storage_all_get_last_stored_info(&mut network, storage_nodes).await;
    assert_eq!(
        actual0[0],
        (
            Some(expected0.1),
            Some((expected0.0, 0 /*b_num*/, 1 /*single mining tx*/,))
        )
    );
    assert_eq!(equal_first(&actual0), node_all(storage_nodes, true));

    let actual0_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    assert_eq!(
        actual0_db_count,
        node_all(storage_nodes, 1 + initial_utxo_txs.len() + 1)
    );

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_no_raft() {
    add_transactions(complete_network_config(10600)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_1_node() {
    add_transactions(complete_network_config_with_n_mempool_raft(10610, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_restart_tx_raft_1_node() {
    let tag = "After add local Transactions";
    let modify_cfg = vec![
        (tag, CfgModif::Drop("mempool1")),
        (tag, CfgModif::Respawn("mempool1")),
        (
            tag,
            CfgModif::HandleEvents(&[("mempool1", "Snapshot applied")]),
        ),
    ];

    let network_config = complete_network_config_with_n_mempool_raft(10615, 1);
    add_transactions_common(network_config, &modify_cfg).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_2_nodes() {
    add_transactions(complete_network_config_with_n_mempool_raft(10620, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_3_nodes() {
    add_transactions(complete_network_config_with_n_mempool_raft(10630, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn add_transactions_raft_15_nodes() {
    add_transactions(complete_network_config_with_n_mempool_raft(10640, 15)).await;
}

async fn add_transactions(network_config: NetworkConfig) {
    add_transactions_common(network_config, &[]).await;
}

async fn add_transactions_common(network_config: NetworkConfig, modify_cfg: &[(&str, CfgModif)]) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let transactions = valid_transactions(true);

    create_first_block_act(&mut network).await;

    //
    // Act
    //
    add_transactions_act_with(&mut network, &transactions, Cfg::IgnoreWaitTxComplete).await;
    modify_network(&mut network, "After add local Transactions", modify_cfg).await;
    add_transactions_act_with(&mut network, &Default::default(), Cfg::All).await;

    //
    // Assert
    //
    let actual = mempool_all_committed_tx_pool(&mut network, mempool_nodes).await;
    assert_eq!(actual[0], transactions);
    assert_eq!(equal_first(&actual), node_all(mempool_nodes, true));

    test_step_complete(network).await;
}

async fn add_transactions_act(network: &mut Network, txs: &BTreeMap<String, Transaction>) {
    add_transactions_act_with(network, txs, Cfg::All).await;
}

async fn add_transactions_act_with(
    network: &mut Network,
    txs: &BTreeMap<String, Transaction>,
    cfg: Cfg,
) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];

    for tx in txs.values() {
        info!("Test Step Add Transactions");
        user_send_transaction_to_mempool(network, "user1", "mempool1", tx).await;
    }
    for _tx in txs.values() {
        mempool_handle_event(network, "mempool1", &["Transactions added to tx pool"]).await;
    }

    if cfg != Cfg::IgnoreWaitTxComplete {
        node_all_handle_event(network, mempool_nodes, &["Transactions committed"]).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_no_raft() {
    create_block(complete_network_config(10100)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_1_node() {
    create_block(complete_network_config_with_n_mempool_raft(10110, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_2_nodes() {
    create_block(complete_network_config_with_n_mempool_raft(10120, 2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_3_nodes() {
    create_block(complete_network_config_with_n_mempool_raft(10130, 3)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_majority_3_nodes() {
    create_block_common(
        complete_network_config_with_n_mempool_raft(10140, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_raft_15_nodes() {
    create_block(complete_network_config_with_n_mempool_raft(10150, 15)).await;
}

async fn create_block(network_config: NetworkConfig) {
    create_block_common(network_config, CfgNum::All).await;
}

async fn create_block_common(network_config: NetworkConfig, cfg_num: CfgNum) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let transactions = valid_transactions(true);
    let transactions_h = transactions.keys().cloned().collect::<Vec<_>>();
    let transactions_utxo = to_utxo_set(&transactions);
    let (_, block_info0) = complete_first_block(&network.collect_initial_uxto_txs()).await;
    let block0_mining_tx = complete_block_mining_txs(&block_info0);
    let block0_mining_utxo = to_utxo_set(&block0_mining_tx);

    let mut left_init_utxo = to_utxo_set(&network.collect_initial_uxto_txs());
    remove_keys(&mut left_init_utxo, valid_txs_in().keys());

    create_first_block_act(&mut network).await;
    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    add_transactions_act(&mut network, &transactions).await;

    //
    // Act
    //
    let block_transaction_before =
        mempool_all_current_block_transactions(&mut network, mempool_nodes).await;

    create_block_act(&mut network, Cfg::All, cfg_num).await;

    let block_transaction_after =
        mempool_all_current_block_transactions(&mut network, mempool_nodes).await;
    let utxo_set_after = mempool_all_committed_utxo_set(&mut network, mempool_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_transaction_before, node_all(mempool_nodes, None));
    assert_eq!(
        block_transaction_after,
        node_all(mempool_nodes, Some(transactions_h))
    );

    let expected_utxo = merge_txs_3(&left_init_utxo, &transactions_utxo, &block0_mining_utxo);
    assert_eq!(len_and_map(&utxo_set_after[0]), len_and_map(&expected_utxo));
    assert_eq!(equal_first(&utxo_set_after), node_all(mempool_nodes, true));

    test_step_complete(network).await;
}

async fn create_block_act(network: &mut Network, cfg: Cfg, cfg_num: CfgNum) {
    create_block_act_with(network, cfg, cfg_num, 0).await
}

async fn create_block_act_with(network: &mut Network, cfg: Cfg, cfg_num: CfgNum, block_num: u64) {
    let active_nodes = network.all_active_nodes().clone();
    let mempool_nodes = &active_nodes[&NodeType::Mempool];
    let (msg_c_nodes, msg_s_nodes) = node_combined_select(
        &network.config().nodes[&NodeType::Mempool],
        &network.config().nodes[&NodeType::Storage],
        network.dead_nodes(),
        cfg_num,
    );

    info!("Test Step Storage signal new block");

    if cfg == Cfg::IgnoreStorage {
        let block = BlockStoredInfo {
            block_num,
            ..Default::default()
        };
        let req = MempoolRequest::SendBlockStored(block);
        mempool_all_inject_next_event(network, &msg_s_nodes, &msg_c_nodes, req).await;
    } else {
        storage_all_send_stored_block(network, &msg_s_nodes).await;
    }
    mempool_all_handle_event(network, &msg_c_nodes, "Received block stored").await;

    info!("Test Step Generate Block");
    node_all_handle_event(network, mempool_nodes, &["Block committed"]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_block_with_seed() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config_with_n_mempool_raft(11600, 1);
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let mempool = &mempool_nodes[0];
    let transactions = valid_transactions(true);
    let (_, block_info0) = complete_first_block(&network.collect_initial_uxto_txs()).await;

    //
    // Act
    //
    create_first_block_act(&mut network).await;
    let block0 = mempool_current_mining_block(&mut network, mempool).await;
    let seed0 = block0.as_ref().map(|b| b.header.seed_value.as_slice());
    let seed0 = seed0.and_then(|s| std::str::from_utf8(s).ok());

    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    add_transactions_act(&mut network, &transactions).await;
    create_block_act(&mut network, Cfg::All