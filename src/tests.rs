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

    let transactions = valid_transactions(true);
    let (_, block_info0) = complete_first_block(&network.collect_initial_uxto_txs()).await;
    let (expected1, block_info1) = complete_block(1, Some("0"), &transactions).await;
    let (_expected3, wrong_block3) = complete_block(3, Some("0"), &BTreeMap::new()).await;
    let block1_mining_tx = complete_block_mining_txs(&block_info1);

    create_first_block_act(&mut network).await;
    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    mempool_all_skip_block_gen(&mut network, mempool_nodes, &block_info1).await;
    mempool_all_skip_mining(&mut network, mempool_nodes, &block_info1).await;

    let initial_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;

    //
    // Act
    //
    storage_inject_send_block_to_storage(&mut network, "mempool1", "storage1", &wrong_block3).await;
    storage_handle_event(&mut network, "storage1", BLOCK_RECEIVED).await;

    send_block_to_storage_act(&mut network, cfg_num).await;

    //
    // Assert
    //
    let actual1 = storage_all_get_last_stored_info(&mut network, storage_nodes).await;
    assert_eq!(
        actual1[0],
        (
            Some(expected1.1),
            Some((expected1.0, 1 /*b_num*/, 1 /*mining txs*/,))
        )
    );
    assert_eq!(equal_first(&actual1), node_all(storage_nodes, true));

    let actual0_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    assert_eq!(
        substract_vec(&actual0_db_count, &initial_db_count),
        node_all(
            storage_nodes,
            1 + transactions.len() + block1_mining_tx.len()
        )
    );

    test_step_complete(network).await;
}

async fn send_block_to_storage_act(network: &mut Network, cfg_num: CfgNum) {
    let active_nodes = network.all_active_nodes().clone();
    let storage_nodes = &active_nodes[&NodeType::Storage];

    let mempool_no_mining = network
        .active_mempool_to_miner_mapping()
        .iter()
        .filter(|(_, miners)| miners.is_empty())
        .map(|(c, _)| c.clone());
    let mut dead_nodes = network.dead_nodes().clone();
    dead_nodes.extend(mempool_no_mining);

    let (msg_c_nodes, msg_s_nodes) = node_combined_select(
        &network.config().nodes[&NodeType::Mempool],
        &network.config().nodes[&NodeType::Storage],
        &dead_nodes,
        cfg_num,
    );

    info!("Test Step Mempool Send block to Storage");
    mempool_all_send_block_to_storage(network, &msg_c_nodes).await;
    storage_all_handle_event(network, &msg_s_nodes, BLOCK_RECEIVED).await;
    node_all_handle_event(network, storage_nodes, &[BLOCK_STORED]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_raft_1_node_with_file_backup() {
    let mut network_config = complete_network_config_with_n_mempool_raft(11300, 1);
    network_config.in_memory_db = false;
    network_config.backup_block_modulo = Some(2);
    remove_all_node_dbs(&network_config);
    main_loops_raft_1_node_common(network_config, vec![3], TokenAmount(17), 20, 1, 1_000, &[]).await
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_raft_2_node() {
    let network_config = complete_network_config_with_n_mempool_raft(11310, 2);
    main_loops_raft_1_node_common(network_config, vec![2], TokenAmount(17), 20, 1, 1_000, &[]).await
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_raft_3_node() {
    let network_config = complete_network_config_with_n_mempool_raft(11320, 3);
    main_loops_raft_1_node_common(network_config, vec![2], TokenAmount(17), 20, 1, 1_000, &[]).await
}

// Slow: Only run when explicitely specified for performance and large tests
// `RUST_LOG="info,raft=warn" cargo test --lib --release -- --ignored --nocapture main_loops_many_txs_threaded_raft_1_node`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn main_loops_many_txs_threaded_raft_1_node() {
    let mut network_config = complete_network_config_with_n_mempool_raft(11330, 1);
    network_config.test_duration_divider = 1;

    // 5_000 TxIn in transactions of 3 TxIn.
    // 10_000 TxIn available so we can create full transaction batch even if previous block did
    // not contain all transactions already created.
    main_loops_raft_1_node_common(
        network_config,
        vec![5],
        TokenAmount(1),
        1_000_000,
        10_000,
        5_000 / 3,
        &[],
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_restart_raft_1_node() {
    let network_config = complete_network_config_with_n_mempool_raft(11340, 1);
    let stop_nums = vec![1, 2];
    let amount = TokenAmount(17);
    let modify = vec![("Before start 1", CfgModif::RestartEventsAll(&[]))];
    main_loops_raft_1_node_common(network_config, stop_nums, amount, 20, 1, 1_000, &modify).await
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_restart_raft_3_node() {
    let network_config = complete_network_config_with_n_mempool_raft(11350, 3);
    let stop_nums = vec![1, 2];
    let amount = TokenAmount(17);
    let modify = vec![("Before start 1", CfgModif::RestartEventsAll(&[]))];
    main_loops_raft_1_node_common(network_config, stop_nums, amount, 20, 1, 1_000, &modify).await
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_restart_upgrade_raft_1_node() {
    let network_config = complete_network_config_with_n_mempool_raft(11360, 1);
    let stop_nums = vec![1, 2];
    let amount = TokenAmount(17);
    let modify = vec![("Before start 1", CfgModif::RestartUpgradeEventsAll(&[]))];
    main_loops_raft_1_node_common(network_config, stop_nums, amount, 20, 1, 1_000, &modify).await
}

#[tokio::test(flavor = "current_thread")]
async fn main_loops_few_txs_restart_upgrade_raft_3_node() {
    let network_config = complete_network_config_with_n_mempool_raft(11370, 3);
    let stop_nums = vec![1, 2];
    let amount = TokenAmount(17);
    let modify = vec![("Before start 1", CfgModif::RestartUpgradeEventsAll(&[]))];
    main_loops_raft_1_node_common(network_config, stop_nums, amount, 20, 1, 1_000, &modify).await
}

async fn main_loops_raft_1_node_common(
    mut network_config: NetworkConfig,
    expected_block_nums: Vec<u64>,
    initial_amount: TokenAmount,
    seed_count: i32,
    seed_wallet_count: i32,
    tx_max_count: usize,
    modify_cfg: &[(&str, CfgModif)],
) {
    test_step_start();

    //
    // Arrange
    // initial_amount is split into TokenAmount(1) TxOuts for the next round.
    //
    network_config.mempool_seed_utxo =
        make_mempool_seed_utxo(&[(seed_count, "000000")], initial_amount);
    network_config.user_test_auto_gen_setup = UserAutoGenTxSetup {
        user_initial_transactions: (0..seed_wallet_count)
            .map(|i| wallet_seed((i, "000000"), &initial_amount))
            .collect::<Vec<WalletTxSpec>>(),
        user_setup_tx_chunk_size: Some(5),
        user_setup_tx_in_per_tx: Some(3),
        user_setup_tx_max_count: tx_max_count,
    };

    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];

    //
    // Act
    //
    let (mut actual_stored, mut expected_stored) = (Vec::new(), Vec::new());
    let (mut actual_mempool, mut expected_mempool) = (Vec::new(), Vec::new());
    for (idx, expected_block_num) in expected_block_nums.iter().copied().enumerate() {
        let tag = format!("Before start {idx}");
        modify_network(&mut network, &tag, modify_cfg).await;

        for node_name in mempool_nodes {
            node_send_coordinated_shutdown(&mut network, node_name, expected_block_num).await;
        }

        let handles = network
            .spawn_main_node_loops(TIMEOUT_TEST_WAIT_DURATION)
            .await;
        node_join_all_checked(handles, &"").await.unwrap();

        actual_stored
            .push(storage_all_get_last_block_stored_num(&mut network, storage_nodes).await);
        actual_mempool
            .push(mempool_all_committed_current_block_num(&mut network, mempool_nodes).await);
        expected_stored.push(node_all(storage_nodes, Some(expected_block_num)));
        expected_mempool.push(node_all(mempool_nodes, Some(expected_block_num)));
    }

    //
    // Assert
    //
    assert_eq!(actual_stored, expected_stored);
    assert_eq!(actual_mempool, expected_mempool);

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn receive_payment_tx_user() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(10400);
    network_config
        .nodes_mut(NodeType::User)
        .push("user2".to_string());
    network_config.mempool_seed_utxo = make_mempool_seed_utxo(SEED_UTXO, TokenAmount(11));
    network_config.user_wallet_seeds = vec![vec![wallet_seed(VALID_TXS_IN[0], &TokenAmount(11))]];
    let mut network = Network::create_from_config(&network_config).await;
    let user_nodes = &network_config.nodes[&NodeType::User];
    let amount = TokenAmount(5);
    let locktime = Some(5);

    create_first_block_act(&mut network).await;

    //
    // Act/Assert
    //
    let before = node_all_get_wallet_info(&mut network, user_nodes).await;

    node_connect_to(&mut network, "user1", "user2").await;

    // Process requested transactions:
    user_send_address_request(&mut network, "user1", "user2", amount, locktime).await;
    user_handle_event(&mut network, "user2", "New address ready to be sent").await;

    user_send_address_to_trading_peer(&mut network, "user2").await;
    user_handle_event(&mut network, "user1", "Next payment transaction ready").await;

    user_send_next_payment_to_destinations(&mut network, "user1", "mempool1").await;
    mempool_handle_event(&mut network, "mempool1", &["Transactions added to tx pool"]).await;
    mempool_handle_event(&mut network, "mempool1", &["Transactions committed"]).await;
    user_handle_event(&mut network, "user2", "Payment transaction received").await;

    // Ignore donations:
    user_send_donation_address_to_peer(&mut network, "user2", "user1").await;
    user_handle_error(&mut network, "user1", "Ignore unexpected transaction").await;

    let after_payment_user1_user2 = node_all_get_wallet_info(&mut network, user_nodes).await;

    // Handle trying to spend locked funds:
    create_block_act_with(&mut network, Cfg::IgnoreStorage, CfgNum::All, 0).await; // Create next block
    user_send_address_request(&mut network, "user2", "user1", amount, None).await;
    user_handle_event(&mut network, "user1", "New address ready to be sent").await;

    user_send_address_to_trading_peer(&mut network, "user1").await;
    // Insufficient funds due to locked funds:
    user_handle_event_failure(&mut network, "user2", "Insufficient funds for payment").await;

    // Handle trying to spend unlocked funds:
    for b_num in 1..10 {
        // Create next block
        create_block_act_with(&mut network, Cfg::IgnoreStorage, CfgNum::All, b_num).await;
        // Update locked coinbase
        users_filter_locked_coinbase(&mut network, &["user1", "user2"], b_num).await;
    }

    user_send_address_request(&mut network, "user2", "user1", amount, None).await;
    user_handle_event(&mut network, "user1", "New address ready to be sent").await;

    user_send_address_to_trading_peer(&mut network, "user1").await;
    user_handle_event(&mut network, "user2", "Next payment transaction ready").await;

    user_send_next_payment_to_destinations(&mut network, "user2", "mempool1").await;
    mempool_handle_event(&mut network, "mempool1", &["Transactions added to tx pool"]).await;
    mempool_handle_event(&mut network, "mempool1", &["Transactions committed"]).await;
    user_handle_event(&mut network, "user1", "Payment transaction received").await;

    let after_payment_user2_user1 = node_all_get_wallet_info(&mut network, user_nodes).await;

    //
    // Assert
    //
    assert_eq!(
        before
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(11), AssetValues::token_u64(0),]
    );
    assert_eq!(
        after_payment_user1_user2
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(6), AssetValues::token_u64(5)]
    );
    assert_eq!(
        after_payment_user2_user1
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(11), AssetValues::token_u64(0),]
    );

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn payment_address_from_public_key() {
    let (public_key, _) = sign::gen_keypair();
    let public_key_vec: Vec<u8> = public_key.as_ref().to_vec();
    let pub_key = PublicKey::from_slice(&public_key_vec).unwrap();
    assert_eq!(pub_key, public_key);

    construct_address(&pub_key);
}

#[tokio::test(flavor = "current_thread")]
async fn receive_testnet_donation_payment_tx_user() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(10405);
    network_config
        .nodes_mut(NodeType::User)
        .push("user2".to_string());
    network_config.user_auto_donate = 5;
    network_config.mempool_seed_utxo = make_mempool_seed_utxo(SEED_UTXO, TokenAmount(11));
    network_config.user_wallet_seeds = vec![vec![wallet_seed(VALID_TXS_IN[0], &TokenAmount(11))]];
    let mut network = Network::create_from_config(&network_config).await;
    let user_nodes = &network_config.nodes[&NodeType::User];

    create_first_block_act(&mut network).await;

    //
    // Act/Assert
    //
    let before = node_all_get_wallet_info(&mut network, user_nodes).await;

    node_connect_to(&mut network, "user1", "user2").await;

    user_send_donation_address_to_peer(&mut network, "user2", "user1").await;
    user_handle_event(&mut network, "user1", "Next payment transaction ready").await;

    user_send_next_payment_to_destinations(&mut network, "user1", "mempool1").await;
    mempool_handle_event(&mut network, "mempool1", &["Transactions added to tx pool"]).await;
    user_handle_event(&mut network, "user2", "Payment transaction received").await;

    let after = node_all_get_wallet_info(&mut network, user_nodes).await;

    //
    // Assert
    //
    assert_eq!(
        before
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(11), AssetValues::token_u64(0)]
    );
    assert_eq!(
        after
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(6), AssetValues::token_u64(5)]
    );

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn reject_payment_txs() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(10410);
    network_config
        .nodes_mut(NodeType::User)
        .push("user2".to_string());
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];

    let valid_txs = valid_transactions(true);
    let invalid_txs = vec![
        // New keys not matching utxo_set
        valid_transactions_with(false, DEFAULT_SEED_AMOUNT, false),
        // Too much output amount for given inputs
        valid_transactions_with(true, DEFAULT_SEED_AMOUNT + TokenAmount(1), false),
        // Too little output amount for given inputs
        valid_transactions_with(true, DEFAULT_SEED_AMOUNT - TokenAmount(1), false),
        // Invalid script
        {
            let (k, v) = valid_txs.iter().next().unwrap();
            let (k, mut v) = (k.clone(), v.clone());
            v.inputs[0].script_signature.stack.push(StackEntry::Num(0));
            Some((k, v)).into_iter().collect()
        },
    ];

    create_first_block_act(&mut network).await;

    //
    // Act/Assert
    //
    for tx in invalid_txs.iter().flat_map(|txs| txs.values()) {
        user_send_transaction_to_mempool(&mut network, "user2", "mempool1", tx).await;
    }
    for _tx in invalid_txs.iter().flat_map(|txs| txs.values()) {
        mempool_handle_error(
            &mut network,
            "mempool1",
            &["No valid transactions provided"],
        )
        .await;
    }
    add_transactions_act(&mut network, &valid_txs).await;

    //
    // Assert
    //
    let actual = mempool_all_committed_tx_pool(&mut network, mempool_nodes).await;
    assert_eq!(actual[0], valid_txs);
    assert_eq!(equal_first(&actual), node_all(mempool_nodes, true));

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn gen_transactions_no_restart() {
    let network_config = complete_network_config(10420);
    gen_transactions_common(network_config, &[]).await
}

#[tokio::test(flavor = "current_thread")]
async fn gen_transactions_restart() {
    let tag = "After block notification request";
    let name = "mempool1";
    let modify_cfg = vec![(tag, CfgModif::Drop(name)), (tag, CfgModif::Respawn(name))];

    let network_config = complete_network_config(10425);
    gen_transactions_common(network_config, &modify_cfg).await
}

async fn gen_transactions_common(
    mut network_config: NetworkConfig,
    modify_cfg: &[(&str, CfgModif)],
) {
    test_step_start();

    //
    // Arrange
    //
    network_config.user_test_auto_gen_setup = UserAutoGenTxSetup {
        user_initial_transactions: vec![wallet_seed(VALID_TXS_IN[0], &DEFAULT_SEED_AMOUNT)],
        user_setup_tx_chunk_size: None,
        user_setup_tx_in_per_tx: Some(2),
        user_setup_tx_max_count: 4,
    };
    let mut network = Network::create_from_config(&network_config).await;

    //
    // Act
    //
    node_send_startup_requests(&mut network, "user1").await;
    mempool_handle_event(&mut network, "mempool1", &["Received block notification"]).await;
    modify_network(&mut network, "After block notification request", modify_cfg).await;

    let mut tx_expected = Vec::new();
    let mut tx_committed = Vec::new();
    for b_num in 0..3 {
        if b_num == 0 {
            create_first_block_act(&mut network).await;
        } else {
            create_block_act_with(&mut network, Cfg::IgnoreStorage, CfgNum::All, b_num - 1).await;
        }

        mempool_flood_block_to_users(&mut network, "mempool1").await;
        user_handle_event(&mut network, "user1", "Block mining notified").await;
        let transactions = user_process_mining_notified(&mut network, "user1").await;
        mempool_handle_event(&mut network, "mempool1", &["Transactions added to tx pool"]).await;
        mempool_handle_event(&mut network, "mempool1", &["Transactions committed"]).await;
        let committed = mempool_committed_tx_pool(&mut network, "mempool1").await;

        tx_expected.push(transactions.unwrap());
        tx_committed.push(committed);
    }

    //
    // Assert
    //
    assert_eq!(tx_committed, tx_expected);

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn proof_of_work_reject() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config(10430);
    let mut network = Network::create_from_config(&network_config).await;

    let mempool = "mempool1";
    let miner = "miner1";
    let block_num = 1;
    create_first_block_act(&mut network).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;
    mempool_flood_rand_and_block_to_partition(&mut network, mempool).await;
    miner_handle_event(&mut network, miner, "Received random number successfully").await;
    miner_handle_event(&mut network, miner, "Partition PoW complete").await;
    miner_process_found_partition_pow(&mut network, miner).await;
    mempool_handle_event(
        &mut network,
        mempool,
        &["Partition PoW received successfully"],
    )
    .await;
    mempool_handle_event(&mut network, mempool, &["Winning PoW intake open"]).await;

    //
    // Act
    //
    {
        // From node not in partition (user1 is not a miner).
        let request = MempoolRequest::SendPoW {
            block_num,
            nonce: Default::default(),
            coinbase: Default::default(),
        };
        mempool_inject_next_event(&mut network, "user1", mempool, request).await;
        mempool_handle_error(&mut network, mempool, &["Not block currently mined"]).await;
    }
    {
        // For not currently mined block number.
        let request = MempoolRequest::SendPoW {
            block_num: block_num + 1,
            nonce: Default::default(),
            coinbase: Default::default(),
        };
        mempool_inject_next_event(&mut network, miner, mempool, request).await;
        mempool_handle_error(&mut network, mempool, &["Not block currently mined"]).await;
    }
    {
        // For miner in partition and correct block, but wrong nonce/coinbase
        let request = MempoolRequest::SendPoW {
            block_num,
            nonce: Default::default(),
            coinbase: Default::default(),
        };
        mempool_inject_next_event(&mut network, miner, mempool, request).await;
        mempool_handle_error(&mut network, mempool, &["Coinbase transaction invalid"]).await;
    }

    //
    // Assert
    //
    let block = mempool_current_mining_block(&mut network, mempool).await;
    assert!(block.is_some(), "Expect still mining");

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_lost_no_restart_no_raft() {
    handle_message_lost_common(complete_network_config(10440), &[]).await
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_lost_no_restart_raft_1_node() {
    handle_message_lost_common(complete_network_config_with_n_mempool_raft(10450, 1), &[]).await
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_lost_restart_block_stored_raft_1_node() {
    let network_config = complete_network_config_with_n_mempool_raft(10460, 1);
    handle_message_lost_restart_block_stored_raft_1_node_common(network_config).await
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_lost_restart_block_stored_raft_real_db_1_node() {
    let mut network_config = complete_network_config_with_n_mempool_raft(10465, 1);
    network_config.in_memory_db = false;

    remove_all_node_dbs(&network_config);
    handle_message_lost_restart_block_stored_raft_1_node_common(network_config).await
}

async fn handle_message_lost_restart_block_stored_raft_1_node_common(
    network_config: NetworkConfig,
) {
    let modify_cfg = vec![(
        "After store block 0",
        CfgModif::RestartEventsAll(&[
            (NodeType::Mempool, "Snapshot applied"),
            (NodeType::Mempool, "Winning PoW intake open"),
            (NodeType::Mempool, "Pipeline halted"),
            (NodeType::Mempool, "Received partition request successfully"),
            (NodeType::Storage, "Snapshot applied"),
        ]),
    )];
    handle_message_lost_common(network_config, &modify_cfg).await
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_lost_restart_block_complete_raft_1_node() {
    let modify_cfg = vec![(
        "After create block 1",
        CfgModif::RestartEventsAll(&[
            (NodeType::Mempool, "Snapshot applied"),
            (NodeType::Mempool, "Received partition request successfully"),
            (NodeType::Storage, "Snapshot applied"),
        ]),
    )];

    let network_config = complete_network_config_with_n_mempool_raft(10470, 1);
    handle_message_lost_common(network_config, &modify_cfg).await
}

#[tokio::test(flavor = "current_thread")]
async fn handle_message_lost_restart_upgrade_block_complete_raft_1_node() {
    let modify_cfg = vec![(
        "After store block 0",
        CfgModif::RestartUpgradeEventsAll(&[
            (NodeType::Mempool, "Snapshot applied"),
            (NodeType::Mempool, "Received first full partition request"),
            (NodeType::Storage, "Snapshot applied"),
        ]),
    )];

    let network_config = complete_network_config_with_n_mempool_raft(10480, 1);
    handle_message_lost_common_with_pow(network_config, CfgPow::First, &modify_cfg).await
}

/// In this test we simulate a network partition where the miners chosen to mine the block
/// suddenly disconnect, leaving the pipeline in a stuck state.
///
/// To address this issue the pipeline needs to reset to participant intake
#[tokio::test(flavor = "current_thread")]
async fn handle_messages_lost_reset_pipeline_stage() {
    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11520, 1);
    // Enable pipeline resets when message retrigger threshold has been reached
    network_config.enable_pipeline_reset = Some(true);
    network_config.test_duration_divider = 10;
    let mut network = Network::create_from_config(&network_config).await;

    let modify_cfg = vec![
        // Miner 1 get's dropped as soon as it's supposed to mine the block
        ("After Winning PoW intake open", CfgModif::Drop("miner1")),
        // The pipeline status changes back to participant intake after a the threshold
        // for retrigger messages has been reached
        (
            "After Winning PoW intake open",
            CfgModif::HandleEvents(&[("mempool1", "Pipeline reset")]),
        ),
        // After the pipeline status has been reset, the pipeline should be able to
        // accept participants again, so we respawn Miner 1
        ("After Winning PoW intake open", CfgModif::Respawn("miner1")),
        (
            "After Winning PoW intake open",
            CfgModif::Reconnect("miner1"),
        ),
        (
            "After Winning PoW intake open",
            CfgModif::HandleEvents(&[("mempool1", "Received partition request successfully")]),
        ),
    ];

    //
    // Act
    //
    create_first_block_act(&mut network).await;

    // Normal PoW procedure
    proof_of_work_participation_act(&mut network, CfgNum::All, CfgPow::First).await;

    // Disconnect Miner 1 and wait for pipeline reset
    // Reconnect Miner 1 and continue with pipeline from participant intake
    modify_network(&mut network, "After Winning PoW intake open", &modify_cfg).await;

    // Normal PoW procedure
    proof_of_work_participation_act(&mut network, CfgNum::All, CfgPow::First).await;
    proof_of_work_block_act(&mut network, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    //
    // Assert
    //
    let actual0 = storage_get_last_stored_info(&mut network, "storage1").await;
    let actual0_values = actual0.1.as_ref();
    let actual0_values = actual0_values.map(|(_, b_num, min_tx)| (*b_num, *min_tx));
    assert_eq!(actual0_values, Some((0, 1)), "Actual: {actual0:?}");
}

async fn handle_message_lost_common(
    network_config: NetworkConfig,
    modify_cfg: &[(&str, CfgModif)],
) {
    handle_message_lost_common_with_pow(network_config, CfgPow::Parallel, modify_cfg).await
}

async fn handle_message_lost_common_with_pow(
    mut network_config: NetworkConfig,
    cfg_pow: CfgPow,
    modify_cfg: &[(&str, CfgModif)],
) {
    test_step_start();

    //
    // Arrange
    //
    network_config.test_duration_divider = 10;
    let mut network = Network::create_from_config(&network_config).await;
    let miner_nodes = &network_config.nodes[&NodeType::Miner];
    let all_nodes = network.all_active_nodes_name_vec();
    let mining_reward = network.mining_reward();

    let expected_events_b1_create = network.all_active_nodes_events(|t, _| match t {
        NodeType::Mempool => vec![
            "Received block stored".to_owned(),
            "Block committed".to_owned(),
        ],
        NodeType::Storage | NodeType::Miner | NodeType::User => vec![],
    });
    let expected_events_b1_stored = network.all_active_nodes_events(|t, _| match t {
        NodeType::Storage => vec![BLOCK_RECEIVED.to_owned(), BLOCK_STORED.to_owned()],
        NodeType::Mempool if cfg_pow == CfgPow::Parallel => vec![
            "Partition PoW received successfully".to_owned(),
            "Received PoW successfully".to_owned(),
            "Pipeline halted".to_owned(),
        ],
        NodeType::Mempool => vec![
            "Partition PoW received successfully".to_owned(),
            "Winning PoW intake open".to_owned(),
            "Partition PoW received successfully".to_owned(),
            "Received PoW successfully".to_owned(),
            "Pipeline halted".to_owned(),
        ],
        NodeType::Miner if cfg_pow == CfgPow::Parallel => vec![
            "Pre-block received successfully".to_owned(),
            "Partition PoW complete".to_owned(),
            "Block PoW complete".to_owned(),
        ],
        NodeType::Miner => vec![
            "Received random number successfully".to_owned(),
            "Partition PoW complete".to_owned(),
            "Pre-block received successfully".to_owned(),
            "Partition PoW complete".to_owned(),
            "Block PoW complete".to_owned(),
        ],
        NodeType::User => vec![],
    });

    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    let stored0 = storage_get_last_block_stored(&mut network, "storage1").await;

    //
    // Act
    //
    modify_network(&mut network, "After store block 0", modify_cfg).await;
    node_all_handle_different_event(&mut network, &all_nodes, &expected_events_b1_create).await;
    modify_network(&mut network, "After create block 1", modify_cfg).await;
    node_all_handle_different_event(&mut network, &all_nodes, &expected_events_b1_stored).await;

    //
    // Assert
    //
    let actual1 = storage_get_last_stored_info(&mut network, "storage1").await;
    let actual1_values = actual1.1.as_ref();
    let actual1_values = actual1_values.map(|(_, b_num, min_tx)| (*b_num, *min_tx));
    assert_eq!(actual1_values, Some((1, 1)), "Actual: {actual1:?}");

    let stored0 = stored0.unwrap();
    let actual_w0 = node_all_combined_get_wallet_info(&mut network, miner_nodes).await;
    let expected_w0 =
        node_all_combined_expected_wallet_info(miner_nodes, mining_reward, &[&stored0]);
    assert_eq!((actual_w0.0, actual_w0.1.len(), actual_w0.2), expected_w0);

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn request_blockchain_item_no_raft() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(11400);
    network_config.test_duration_divider = 10;

    let mut network = Network::create_from_config(&network_config).await;
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let transactions = vec![network.collect_initial_uxto_txs(), valid_transactions(true)];
    let all_txs = transactions.iter().cloned();
    let all_txs: Vec<Vec<_>> = all_txs.map(|v| v.into_iter().collect()).collect();
    let ((block_keys, _), blocks) = complete_blocks(11, &transactions).await;

    for block in &blocks {
        storage_inject_send_block_to_storage(&mut network, "mempool1", "storage1", block).await;
        node_all_handle_event(&mut network, storage_nodes, &BLOCK_RECEIVED_AND_STORED).await;
    }

    let inputs_block: Vec<&str> = vec![
        block_keys[0].as_str(),
        "nLastBlockHashKey",
        "nIndexedBlockHashKey_0000000000000000",
        "nIndexedBlockHashKey_000000000000000a",
        "0000_non_existent",
        "nIndexedBlockHashKey_000000000000000b",
    ];
    let expected_block = vec![
        Some((0, ('b', 0, 4))),
        Some((10, ('b', 10, 1))),
        Some((0, ('b', 0, 4))),
        Some((10, ('b', 10, 1))),
        None,
        None,
    ];

    let inputs_tx: Vec<&str> = vec![
        all_txs[0][0].0.as_str(),
        "nIndexedTxHashKey_0000000000000000_00000000",
        "nIndexedTxHashKey_0000000000000000_00000001",
        "nIndexedTxHashKey_0000000000000001_00000000",
        "nIndexedTxHashKey_0000000000000000_00000100",
    ];
    let expected_tx = {
        let t00 = &all_txs[0][0].1;
        let t01 = &all_txs[0][1].1;
        let t10 = &all_txs[1][0].1;
        vec![
            Some((t00.inputs.len(), t00.outputs.len(), ('t', 0, 0))),
            Some((t00.inputs.len(), t00.outputs.len(), ('t', 0, 0))),
            Some((t01.inputs.len(), t01.outputs.len(), ('t', 0, 1))),
            Some((t10.inputs.len(), t10.outputs.len(), ('t', 1, 0))),
            None,
        ]
    };

    //
    // Act
    //
    let mut actual_block = Vec::new();
    let mut actual_tx = Vec::new();

    node_connect_to(&mut network, "miner1", "storage1").await;
    for input in inputs_block {
        request_blockchain_item_act(&mut network, "miner1", "storage1", input).await;
        actual_block.push(miner_get_blockchain_item_received_b_num(&mut network, "miner1").await);
    }
    for input in inputs_tx {
        request_blockchain_item_act(&mut network, "miner1", "storage1", input).await;
        actual_tx.push(miner_get_blockchain_item_received_tx_lens(&mut network, "miner1").await);
    }

    //
    // Assert
    //
    assert_eq!(actual_block, expected_block);
    assert_eq!(actual_tx, expected_tx);

    test_step_complete(network).await;
}

async fn request_blockchain_item_act(
    network: &mut Network,
    miner_from: &str,
    storage_to: &str,
    block_key: &str,
) {
    let storage_node_addr = network
        .storage(storage_to)
        .unwrap()
        .clone()
        .lock()
        .await
        .local_address();
    miner_request_blockchain_item(network, miner_from, block_key, storage_node_addr).await;
    storage_handle_event(network, storage_to, "Blockchain item fetched from storage").await;
    storage_send_blockchain_item(network, storage_to).await;
    miner_handle_event(network, miner_from, "Blockchain item received").await;
}

async fn miner_request_blockchain_item(
    network: &mut Network,
    miner_from: &str,
    block_key: &str,
    storage_node_addr: SocketAddr,
) {
    let mut m = network.miner(miner_from).unwrap().lock().await;
    m.request_blockchain_item(block_key.to_owned(), storage_node_addr)
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn catchup_fetch_blockchain_item_raft() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11420, 3);
    network_config.test_duration_divider = 10;

    let mut network = Network::create_from_config(&network_config).await;
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let transactions = vec![network.collect_initial_uxto_txs(), valid_transactions(true)];
    let all_txs = transactions.iter().cloned();
    let _all_txs: Vec<Vec<_>> = all_txs.map(|v| v.into_iter().collect()).collect();
    let ((_block_keys, _), blocks) = complete_blocks(3, &transactions).await;

    let tx_lens = transactions.iter().map(|v| v.len());
    let items_counts = tx_lens.chain(std::iter::repeat(0)).map(|tx| 2 + tx);
    let items_counts: Vec<usize> = items_counts.take(blocks.len()).collect();

    let modify_cfg = vec![
        ("Before store block 1", CfgModif::Drop("storage3")),
        ("Before store block 3", CfgModif::Respawn("storage3")),
        (
            "Before store block 3",
            CfgModif::HandleEvents(&[
                ("storage3", "Snapshot applied"),
                ("storage3", "Snapshot applied: Fetch missing blocks"),
            ]),
        ),
    ];

    //
    // Act
    //
    for (i, block) in blocks.iter().enumerate() {
        info!("Test Step Storage add block {}", i);

        let tag = format!("Before store block {i}");
        modify_network(&mut network, &tag, &modify_cfg).await;
        let storage_nodes = network.active_nodes(NodeType::Storage).to_owned();

        storage_inject_send_block_to_storage(&mut network, "mempool1", "storage1", block).await;
        storage_inject_send_block_to_storage(&mut network, "mempool2", "storage2", block).await;

        storage_all_handle_event(&mut network, &storage_nodes[0..2], BLOCK_RECEIVED).await;
        node_all_handle_event(&mut network, &storage_nodes, &[BLOCK_STORED]).await;
    }

    let tag = "Before store block 3";
    modify_network(&mut network, tag, &modify_cfg).await;

    // Process event and treat it as messaged lost => switch to storage2
    storage_handle_event(&mut network, "storage3", "Catch up stored blocks").await;

    for (b_num, items_count) in items_counts.iter().copied().enumerate().skip(1) {
        for i in 0..items_count {
            info!(
                "Test Step Storage catchup b_num={} (count={}) i={}",
                b_num, items_count, i,
            );

            let network = &mut network;
            let receive_evt = if i < items_count - 1 {
                "Blockchain item received"
            } else if b_num < items_counts.len() - 1 {
                "Blockchain item received: Block stored"
            } else {
                "Blockchain item received: Block stored(Done)"
            };

            storage_handle_event(network, "storage3", "Catch up stored blocks").await;
            storage_catchup_fetch_blockchain_item(network, "storage3").await;
            storage_handle_event(network, "storage2", "Blockchain item fetched from storage").await;
            storage_send_blockchain_item(network, "storage2").await;
            storage_handle_event(network, "storage3", receive_evt).await;
        }
    }

    //
    // Assert
    //
    let all_items_count = items_counts[..].iter().sum::<usize>();
    let actual_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    assert_eq!(actual_db_count, node_all(storage_nodes, all_items_count));

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn relaunch_with_new_raft_nodes() {
    test_step_start();

    //
    // Arrange
    //
    let extra_params = {
        let mut network_config = complete_network_config_with_n_mempool_raft(11430, 1);
        network_config.nodes.insert(NodeType::User, vec![]);
        let mut network = Network::create_from_config(&network_config).await;

        // Create intiial snapshoot in mempool and storage
        create_first_block_act(&mut network).await;
        proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
        send_block_to_storage_act(&mut network, CfgNum::All).await;
        network.close_raft_loops_and_drop().await
    };

    let mut network_config = complete_network_config_with_n_mempool_raft(11430, 3);
    network_config.mempool_seed_utxo = Default::default();
    network_config.nodes.insert(NodeType::User, vec![]);

    let mut network = Network::create_stopped_from_config(&network_config);
    network.set_all_extra_params(extra_params);
    network.upgrade_closed_nodes().await;

    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let raft_nodes: Vec<String> = mempool_nodes.iter().chain(storage_nodes).cloned().collect();

    let restart_raft_reasons: BTreeMap<String, Vec<String>> = {
        let snap_applied = vec!["Snapshot applied".to_owned()];
        let snap_applied_fetch = vec!["Snapshot applied: Fetch missing blocks".to_owned()];
        vec![
            ("storage1".to_owned(), snap_applied.clone()),
            ("storage2".to_owned(), snap_applied_fetch.clone()),
            ("storage3".to_owned(), snap_applied_fetch),
            ("mempool1".to_owned(), snap_applied.clone()),
            ("mempool2".to_owned(), snap_applied.clone()),
            ("mempool3".to_owned(), snap_applied),
        ]
        .into_iter()
        .collect()
    };

    //
    // Act
    //
    let (mut actual_stored, mut expected_stored) = (Vec::new(), Vec::new());
    let (mut actual_mempool, mut expected_mempool) = (Vec::new(), Vec::new());

    network.pre_launch_nodes_named(&raft_nodes).await;
    for (pre_launch, expected_block_num) in vec![(true, 0u64), (false, 1u64)].into_iter() {
        if !pre_launch {
            for node_name in mempool_nodes {
                node_send_coordinated_shutdown(&mut network, node_name, expected_block_num).await;
            }
        }

        let handles = network
            .spawn_main_node_loops(TIMEOUT_TEST_WAIT_DURATION)
            .await;
        node_join_all_checked(handles, &"").await.unwrap();

        if pre_launch {
            network.close_loops_and_drop_named(&raft_nodes).await;
            network.re_spawn_dead_nodes().await;
            node_all_handle_different_event(&mut network, &raft_nodes, &restart_raft_reasons).await;
        }

        actual_stored
            .push(storage_all_get_last_block_stored_num(&mut network, storage_nodes).await);
        actual_mempool
            .push(mempool_all_committed_current_block_num(&mut network, mempool_nodes).await);
        expected_stored.push(node_all(storage_nodes, Some(expected_block_num)));
        expected_mempool.push(node_all(mempool_nodes, Some(expected_block_num)));
    }

    //
    // Assert
    //
    assert_eq!(actual_stored, expected_stored);
    assert_eq!(actual_mempool, expected_mempool);

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
async fn request_utxo_set_raft_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11440, 1);
    network_config.mempool_seed_utxo = make_mempool_seed_utxo_with_info({
        let a = DEFAULT_SEED_AMOUNT;
        let pk = SOME_PUB_KEYS;
        &[
            ("000000", vec![(pk[0], a)]),
            ("000001", vec![(pk[1], a), (pk[0], a)]),
            ("000002", vec![(pk[2], a)]),
        ]
    });
    let mut network = Network::create_from_config(&network_config).await;
    create_first_block_act(&mut network).await;

    let committed_utxo_set = mempool_committed_utxo_set(&mut network, "mempool1").await;

    let addresses = {
        let a = SOME_PUB_KEY_ADDRS;
        let ne = vec!["DoesNotExist".to_string()];
        vec![
            //Request full UTXO set
            UtxoFetchType::All,
            //Request single address with multiple OutPoints
            UtxoFetchType::AnyOf(vec![a[0].to_string()]),
            //Request single address with one OutPoint
            UtxoFetchType::AnyOf(vec![a[1].to_string()]),
            //Request single address with one OutPoint
            UtxoFetchType::AnyOf(vec![a[2].to_string()]),
            //Request combination of different addresses with different OutPoints
            UtxoFetchType::AnyOf(vec![a[1].to_string(), a[2].to_string()]),
            //Request combination of different addresses containing invalid address
            UtxoFetchType::AnyOf(vec![
                "DoesNotExist".to_string(),
                a[1].to_string(),
                a[2].to_string(),
            ]),
            //Single invalid address
            UtxoFetchType::AnyOf(ne),
        ]
    };
    let expected = vec![
        //No address - Should return full UTXO set
        committed_utxo_set.keys().cloned().collect(),
        //Single address with multiple OutPoints
        vec![
            OutPoint::new("000000".to_string(), 0),
            OutPoint::new("000001".to_string(), 1),
        ],
        //Single address with one OutPoint
        vec![OutPoint::new("000001".to_string(), 0)],
        //Single address with one OutPoint
        vec![OutPoint::new("000002".to_string(), 0)],
        //Combination of different addresses with different OutPoints
        vec![
            OutPoint::new("000001".to_string(), 0),
            OutPoint::new("000002".to_string(), 0),
        ],
        //Combination of different addresses containing an invalid address - Should filter out invalid address
        vec![
            OutPoint::new("000001".to_string(), 0),
            OutPoint::new("000002".to_string(), 0),
        ],
        //Single invalid address - Should return an empty set
        Vec::new(),
    ];

    //
    // Act
    //
    let mut actual = Vec::new();
    for address_list in addresses {
        request_utxo_set_act(&mut network, "user1", "mempool1", address_list).await;
        actual.push(user_get_received_utxo_set_keys(&mut network, "user1").await);
    }

    //
    // Assert
    //
    assert_eq!(actual, expected);
    test_step_complete(network).await;
}

async fn request_utxo_set_act(
    network: &mut Network,
    user: &str,
    mempool: &str,
    address_list: UtxoFetchType,
) {
    user_send_request_utxo_set(network, user, address_list).await;
    mempool_handle_event(network, mempool, &["Received UTXO fetch request"]).await;
    mempool_send_utxo_set(network, mempool).await;
    user_handle_event(network, user, "Received UTXO set").await;
}

#[tokio::test(flavor = "current_thread")]
async fn request_utxo_set_and_update_running_total_raft_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11450, 1);
    network_config.mempool_seed_utxo = make_mempool_seed_utxo_with_info({
        let a = DEFAULT_SEED_AMOUNT;
        let pk = SOME_PUB_KEYS;
        &[
            ("000000", vec![(pk[0], a)]),
            ("000001", vec![(pk[1], a), (pk[0], a)]),
            ("000002", vec![(pk[2], a), (pk[1], a)]),
        ]
    });
    network_config.user_wallet_seeds = {
        let pk = SOME_PUB_KEYS;
        let sk = SOME_SEC_KEYS;
        vec![vec![
            WalletTxSpec {
                out_point: "0-000000".to_string(),
                secret_key: sk[0].to_string(),
                public_key: pk[0].to_string(),
                amount: 3,
                address_version: None,
            },
            WalletTxSpec {
                out_point: "0-000001".to_string(),
                secret_key: sk[1].to_string(),
                public_key: pk[1].to_string(),
                amount: 3,
                address_version: None,
            },
            WalletTxSpec {
                out_point: "0-000002".to_string(),
                secret_key: sk[2].to_string(),
                public_key: pk[2].to_string(),
                amount: 3,
                address_version: None,
            },
        ]]
    };

    let user_nodes = &network_config.nodes[&NodeType::User];
    let mut network = Network::create_from_config(&network_config).await;
    let address_list: UtxoFetchType =
        UtxoFetchType::AnyOf(SOME_PUB_KEY_ADDRS.iter().map(|v| v.to_string()).collect());

    //
    // Act
    //
    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    let before = node_all_get_wallet_info(&mut network, user_nodes).await;
    request_utxo_set_and_update_running_total_act(&mut network, "user1", "mempool1", address_list)
        .await;
    let after = node_all_get_wallet_info(&mut network, user_nodes).await;

    //
    // Assert
    //
    assert_eq!(
        before
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(9)]
    );

    assert_eq!(
        after
            .iter()
            .map(|(total, _, _)| total.clone())
            .collect::<Vec<_>>(),
        vec![AssetValues::token_u64(15)]
    );

    test_step_complete(network).await;
}

async fn request_utxo_set_and_update_running_total_act(
    network: &mut Network,
    user: &str,
    mempool: &str,
    address_list: UtxoFetchType,
) {
    user_trigger_update_wallet_from_utxo_set(network, user, address_list).await;
    user_handle_event(network, user, "Request UTXO set").await;
    mempool_handle_event(network, mempool, &["Received UTXO fetch request"]).await;
    mempool_send_utxo_set(network, mempool).await;
    user_handle_event(network, user, "Received UTXO set").await;
    user_update_running_total(network, user).await;
}

#[tokio::test(flavor = "current_thread")]
pub async fn create_item_asset_raft_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11460, 1);
    network_config.mempool_seed_utxo = BTreeMap::new();
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    create_first_block_act(&mut network).await;
    let item_metadata = Some("item_metadata".to_string());

    //
    // Act
    //
    let actual_running_total_before = node_get_wallet_info(&mut network, "user1").await.0;
    let tx_hash =
        create_item_asset_act(&mut network, "user1", "mempool1", 10, item_metadata.clone()).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;

    let committed_utxo_set = mempool_all_committed_utxo_set(&mut network, mempool_nodes).await;
    let actual_utxo_item: Vec<Vec<_>> = committed_utxo_set
        .into_iter()
        .map(|v| v.into_values().map(|v| v.value).collect())
        .collect();

    let actual_running_total_after = node_get_wallet_info(&mut network, "user1").await.0;

    //
    // Assert
    //
    assert_eq!(
        actual_utxo_item,
        node_all(mempool_nodes, vec![Asset::item(10, None, item_metadata)])
    );
    assert_eq!(
        actual_running_total_before,
        AssetValues::item(Default::default())
    );
    assert_eq!(
        actual_running_total_after,
        AssetValues::item(map_items(vec![(tx_hash, 10)]))
    );

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
pub async fn create_item_asset_on_mempool_raft_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11465, 1);
    network_config.mempool_seed_utxo = BTreeMap::new();
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    network_config.mempool_seed_utxo =
        make_mempool_seed_utxo_with_info(&[("000000", vec![(COMMON_PUB_KEY, TokenAmount(0))])]);
    create_first_block_act(&mut network).await;
    let item_metadata = Some("item_metadata".to_string());

    //
    // Act
    //
    let asset_hash =
        construct_tx_in_signable_asset_hash(&Asset::item(1, None, item_metadata.clone()));
    let secret_key = decode_secret_key(COMMON_SEC_KEY).unwrap();
    let signature = hex::encode(sign::sign_detached(asset_hash.as_bytes(), &secret_key).as_ref());

    mempool_create_item_asset_tx(
        &mut network,
        "mempool1",
        1,
        COMMON_PUB_ADDR.to_string(),
        COMMON_PUB_KEY.to_string(),
        signature,
        item_metadata.clone(),
    )
    .await;

    mempool_handle_event(&mut network, "mempool1", &["Transactions committed"]).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;

    let committed_utxo_set = mempool_all_committed_utxo_set(&mut network, mempool_nodes).await;
    let actual_utxo_item: Vec<Vec<_>> = committed_utxo_set
        .into_iter()
        .map(|v| v.into_values().map(|v| v.value).collect())
        .collect();

    //
    // Assert
    //
    assert_eq!(
        actual_utxo_item,
        node_all(mempool_nodes, vec![Asset::item(1, None, item_metadata)]) /* DRS tx hash will reflect as `None` on UTXO set for newly created `Item`s */
    );

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
pub async fn make_item_based_payment_raft_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11470, 1);
    network_config
        .nodes_mut(NodeType::User)
        .push("user2".to_string());
    network_config.mempool_seed_utxo = make_mempool_seed_utxo(SEED_UTXO, TokenAmount(11));
    network_config.user_wallet_seeds = vec![vec![wallet_seed(VALID_TXS_IN[0], &TokenAmount(11))]];
    let mut network = Network::create_from_config(&network_config).await;
    let mempool_nodes = &network_config.nodes[&NodeType::Mempool];
    // This metadata ONLY forms part of the create transaction
    // , and is not present in any on-spending
    let item_metadata = Some("item metadata".to_string());

    create_first_block_act(&mut network).await;
    node_connect_to(&mut network, "user1", "user2").await;
    let tx_hash =
        create_item_asset_act(&mut network, "user2", "mempool1", 5, item_metadata.clone()).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;

    //
    // Act
    //
    let wallet_assets_before_actual =
        user_get_wallet_asset_totals_for_tx(&mut network, "user1", "user2").await;

    make_item_based_payment_act(
        &mut network,
        "user1",
        "user2",
        "mempool1",
        Asset::Token(DEFAULT_SEED_AMOUNT),
        Some(tx_hash.clone()),
    )
    .await;

    let wallet_assets_after_actual =
        user_get_wallet_asset_totals_for_tx(&mut network, "user1", "user2").await;

    let committed_tx_druid_pool: Vec<Transaction> =
        mempool_all_committed_tx_druid_pool(&mut network, mempool_nodes)
            .await
            .into_iter()
            .flatten()
            .flatten()
            .map(|(_, tx)| tx)
            .collect();

    let actual_druid_string: Vec<String> = committed_tx_druid_pool
        .iter()
        .map(|v| v.druid_info.clone().unwrap().druid)
        .collect();

    let actual_participants: Vec<usize> = committed_tx_druid_pool
        .iter()
        .map(|v| v.druid_info.clone().unwrap().participants)
        .collect();

    let actual_assets: Vec<Vec<Asset>> = committed_tx_druid_pool
        .into_iter()
        .map(|v| {
            v.druid_info
                .unwrap()
                .expectations
                .iter()
                .map(|ex| ex.asset.clone())
                .collect()
        })
        .collect();

    //
    // Assert
    //
    //Assert wallet asset running total before and after
    assert_eq!(
        wallet_assets_before_actual,
        (
            AssetValues::token_u64(11),
            AssetValues::item(map_items(vec![(tx_hash.clone(), 5)]))
        )
    );
    assert_eq!(
        wallet_assets_after_actual,
        (
            AssetValues::new(TokenAmount(8), map_items(vec![(tx_hash.clone(), 1)])),
            AssetValues::new(TokenAmount(3), map_items(vec![(tx_hash.clone(), 4)]))
        )
    );

    //Assert committed DDE transactions have identical DRUID value
    assert_eq!(actual_druid_string[0], actual_druid_string[1]);

    //Assert committed DDE transactions contain valid DDE participation count
    assert!(actual_participants.iter().all(|v| *v == 2));

    //Assert committed DDE transactions contain valid DDE asset values
    let expected_assets = vec![
        vec![Asset::token_u64(3)],
        vec![Asset::item(
            1,
            Some(tx_hash.clone()), /* tx_hash of create transaction */
            None,                  /* metadata of create transaction */
        )],
    ];
    assert!(actual_assets.iter().all(|v| expected_assets.contains(v)));

    test_step_complete(network).await;
}

#[tokio::test(flavor = "current_thread")]
pub async fn make_multiple_item_based_payments_raft_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11480, 1);
    network_config
        .nodes_mut(NodeType::User)
        .push("user2".to_string());
    network_config.mempool_seed_utxo = make_mempool_seed_utxo_with_info({
        let a = TokenAmount(11);
        let pk = SOME_PUB_KEYS;
        &[("000000", vec![(pk[0], a)]), ("000001", vec![(pk[1], a)])]
    });
    network_config.user_wallet_seeds = {
        let pk = SOME_PUB_KEYS;
        let sk = SOME_SEC_KEYS;
        vec![
            vec![WalletTxSpec {
                out_point: "0-000000".to_string(),
                secret_key: sk[0].to_string(),
                public_key: pk[0].to_string(),
                amount: 11,
                address_version: None,
            }],
            vec![WalletTxSpec {
                out_point: "0-000001".to_string(),
                secret_key: sk[1].to_string(),
                public_key: pk[1].to_string(),
                amount: 11,
                address_version: None,
            }],
        ]
    };

    let item_metadata = Some("item metadata".to_string());
    let mut network = Network::create_from_config(&network_config).await;
    create_first_block_act(&mut network).await;
    // Item type "one" belongs to "user1"
    let tx_hash_1 =
        create_item_asset_act(&mut network, "user1", "mempool1", 5, item_metadata.clone()).await;
    // Item type "two" belongs to "user2"
    let tx_hash_2 =
        create_item_asset_act(&mut network, "user2", "mempool1", 10, item_metadata.clone()).await;
    node_connect_to(&mut network, "user1", "user2").await;

    //
    // Act
    //
    let amounts_to_pay = vec![
        // Pay 3 `Token` assets from "user1" to "user2" in exchange for a type "two" `Item` asset
        (TokenAmount(3), "user1", "user2", tx_hash_2.clone(), 0),
        // Pay 4 `Token` assets from "user1" to "user2" in exchange for a type "two" `Item` asset
        (TokenAmount(4), "user1", "user2", tx_hash_2.clone(), 1),
        // Pay 2 `Token` assets from "user2" to "user1" in exchange for a type "one" `Item` asset
        (TokenAmount(2), "user2", "user1", tx_hash_1.clone(), 2),
        // Pay 6 `Token` assets from "user2" to "user1" in exchange for a type "one" `Item` asset
        (TokenAmount(6), "user2", "user1", tx_hash_1.clone(), 3),
    ];

    let mut all_gathered_wallet_asset_info: BTreeMap<
        (&'static str, &'static str),
        Vec<(AssetValues, AssetValues)>,
    > = BTreeMap::new();

    for (payment_amount, from, to, genesis_hash, b_num) in amounts_to_pay {
        create_block_act_with(&mut network, Cfg::IgnoreStorage, CfgNum::All, b_num).await;
        let initial_info = vec![user_get_wallet_asset_totals_for_tx(&mut network, from, to).await];
        let infos = all_gathered_wallet_asset_info
            .entry((from, to))
            .or_insert(initial_info);

        make_item_based_payment_act(
            &mut network,
            from,
            to,
            "mempool1",
            Asset::Token(payment_amount),
            Some(genesis_hash.clone()),
        )
        .await;

        infos.push(user_get_wallet_asset_totals_for_tx(&mut network, from, to).await);
    }
    create_block_act_with(&mut network, Cfg::IgnoreStorage, CfgNum::All, 4).await;

    let all_wallet_assets_actual: Vec<_> = all_gathered_wallet_asset_info.into_iter().collect();

    let users_utxo_balance_actual = (
        mempool_get_utxo_balance_for_user(&mut network, "mempool1", "user1").await,
        mempool_get_utxo_balance_for_user(&mut network, "mempool1", "user2").await,
    );

    let all_assets_expected = vec![
        (
            ("user1", "user2"),
            vec![
                (
                    AssetValues::new(TokenAmount(11), map_items(vec![(tx_hash_1.clone(), 5)])),
                    AssetValues::new(TokenAmount(11), map_items(vec![(tx_hash_2.clone(), 10)])),
                ),
                (
                    AssetValues::new(
                        TokenAmount(8),
                        map_items(vec![(tx_hash_1.clone(), 5), (tx_hash_2.clone(), 1)]),
                    ),
                    AssetValues::new(TokenAmount(14), map_items(vec![(tx_hash_2.clone(), 9)])),
                ),
                (
                    AssetValues::new(
                        TokenAmount(4),
                        map_items(vec![(tx_hash_1.clone(), 5), (tx_hash_2.clone(), 2)]),
                    ),
                    AssetValues::new(TokenAmount(18), map_items(vec![(tx_hash_2.clone(), 8)])),
                ),
            ],
        ),
        (
            ("user2", "user1"),
            vec![
                (
                    AssetValues::new(TokenAmount(18), map_items(vec![(tx_hash_2.clone(), 8)])),
                    AssetValues::new(
                        TokenAmount(4),
                        map_items(vec![(tx_hash_1.clone(), 5), (tx_hash_2.clone(), 2)]),
                    ),
                ),
                (
                    AssetValues::new(
                        TokenAmount(16),
                        map_items(vec![(tx_hash_2.clone(), 8), (tx_hash_1.clone(), 1)]),
                    ),
                    AssetValues::new(
                        TokenAmount(6),
                        map_items(vec![(tx_hash_1.clone(), 4), (tx_hash_2.clone(), 2)]),
                    ),
                ),
                (
                    AssetValues::new(
                        TokenAmount(10),
                        map_items(vec![(tx_hash_2.clone(), 8), (tx_hash_1.clone(), 2)]),
                    ),
                    AssetValues::new(
                        TokenAmount(12),
                        map_items(vec![(tx_hash_1.clone(), 3), (tx_hash_2.clone(), 2)]),
                    ),
                ),
            ],
        ),
    ];

    let users_utxo_balance_expected = (
        AssetValues::new(
            TokenAmount(12),
            map_items(vec![(tx_hash_1.clone(), 3), (tx_hash_2.clone(), 2)]),
        ),
        AssetValues::new(
            TokenAmount(10),
            map_items(vec![(tx_hash_2.clone(), 8), (tx_hash_1.clone(), 2)]),
        ),
    );

    //
    // Assert
    //
    assert_eq!(all_wallet_assets_actual, all_assets_expected);
    assert_eq!(users_utxo_balance_actual, users_utxo_balance_expected);

    test_step_complete(network).await;
}

async fn create_item_asset_act(
    network: &mut Network,
    user: &str,
    mempool: &str,
    item_amount: u64,
    item_metadata: Option<String>,
) -> String {
    let tx_hash = user_send_item_asset(network, user, mempool, item_amount, item_metadata).await;
    mempool_handle_event(network, mempool, &["Transactions added to tx pool"]).await;
    mempool_handle_event(network, mempool, &["Transactions committed"]).await;
    tx_hash
}

async fn mempool_create_item_asset_tx(
    network: &mut Network,
    mempool: &str,
    item_amount: u64,
    script_public_key: String,
    public_key: String,
    signature: String,
    item_metadata: Option<String>,
) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    let genesis_hash_spec = GenesisTxHashSpec::Create; /* Generate unique DRS tx hash */
    let (tx, _) = c
        .create_item_asset_tx(
            item_amount,
            script_public_key,
            public_key,
            signature,
            genesis_hash_spec,
            item_metadata,
        )
        .unwrap();
    c.receive_transactions(vec![tx]);
}

async fn make_item_based_payment_act(
    network: &mut Network,
    from: &str,
    to: &str,
    mempool: &str,
    send_asset: Asset,
    genesis_hash: Option<String>, /* Expected `genesis_hash` of `Item` asset to receive */
) {
    user_send_item_based_payment_request(network, from, to, send_asset, genesis_hash).await;
    user_handle_event(network, to, "Received item-based payment request").await;
    user_send_item_based_payment_response(network, to).await;
    user_handle_event(network, from, "Received item-based payment response").await;
    user_send_item_based_payment_to_desinations(network, from, mempool).await;
    mempool_handle_event(network, mempool, &["Transactions added to tx pool"]).await;
    user_handle_event(network, to, "Payment transaction received").await;
    user_send_item_based_payment_to_desinations(network, to, mempool).await;
    mempool_handle_event(network, mempool, &["Transactions added to tx pool"]).await;
    user_handle_event(network, from, "Payment transaction received").await;
    mempool_handle_event(network, mempool, &["Transactions committed"]).await;
}

#[tokio::test(flavor = "current_thread")]
async fn restart_user_with_seed() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(11490);
    let modify_cfg = vec![
        ("stop", CfgModif::Drop("user1")),
        ("start", CfgModif::Respawn("user1")),
    ];
    let seed_0 = vec![vec![wallet_seed(VALID_TXS_IN[0], &TokenAmount(11))]];
    let seed_1 = vec![vec![wallet_seed(VALID_TXS_IN[1], &TokenAmount(12))]];
    network_config.user_wallet_seeds = seed_0;
    let mut network = Network::create_from_config(&network_config).await;

    //
    // Act
    //
    let db_0 = node_get_wallet_info(&mut network, "user1").await;
    modify_network(&mut network, "stop", &modify_cfg).await;
    network.mut_config().user_wallet_seeds = seed_1;
    modify_network(&mut network, "start", &modify_cfg).await;
    let db_1 = node_get_wallet_info(&mut network, "user1").await;

    //
    // Assert
    //
    assert_eq!(db_0, db_1);
}

#[tokio::test(flavor = "current_thread")]
async fn reject_item_based_payment() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11500, 1);
    network_config.mempool_seed_utxo = make_mempool_seed_utxo_with_info({
        &[("000000", vec![(SOME_PUB_KEYS[0], DEFAULT_SEED_AMOUNT)])]
    });
    let mut network = Network::create_from_config(&network_config).await;
    create_first_block_act(&mut network).await;
    let mut create_item_asset_txs = BTreeMap::default();
    let tx = construct_item_create_tx(
        0,
        decode_pub_key(SOME_PUB_KEYS[1]).unwrap(),
        &decode_secret_key(SOME_SEC_KEYS[1]).unwrap(),
        1,
        GenesisTxHashSpec::Create,
        None,
        None,
    );
    let tx_hash = construct_tx_hash(&tx);
    create_item_asset_txs.insert(tx_hash.to_owned(), tx);
    add_transactions_act(&mut network, &create_item_asset_txs).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    create_block_act(&mut network, Cfg::All, CfgNum::All).await;

    /* User1 -> User2 */
    let rb_sender_data = RbSenderData {
        sender_pub_addr: SOME_PUB_KEY_ADDRS[0].to_owned(),
        sender_pub_key: SOME_PUB_KEYS[0].to_owned(),
        sender_sec_key: SOME_SEC_KEYS[0].to_owned(),
        sender_prev_out: OutPoint::new("000000".to_owned(), 0),
        sender_amount: DEFAULT_SEED_AMOUNT,
        sender_half_druid: "sender_druid".to_owned(),
        sender_expected_drs: Some(tx_hash.to_owned()),
    };

    let rb_receiver_data = RbReceiverData {
        receiver_pub_addr: SOME_PUB_KEY_ADDRS[1].to_owned(),
        receiver_pub_key: SOME_PUB_KEYS[1].to_owned(),
        receiver_sec_key: SOME_SEC_KEYS[1].to_owned(),
        receiver_prev_out: OutPoint::new(tx_hash.to_owned(), 0),
        receiver_half_druid: "receiver_druid".to_owned(),
    };

    let rb_txs = generate_rb_transactions(rb_sender_data, rb_receiver_data);
    let rb_send_txs = vec![rb_txs[0].1.clone(), rb_txs[0].1.clone()];
    let mut rb_recv_txs = vec![rb_txs[1].1.clone(), rb_txs[1].1.clone()];

    // Invalid participant count
    rb_recv_txs[0].druid_info.as_mut().unwrap().participants = 3;

    // Invalid DRUID expectations
    rb_recv_txs[1].druid_info.as_mut().unwrap().expectations = vec![DruidExpectation {
        from: "wrong_from_addr".to_owned(), /* Invalid From Addr */
        to: SOME_PUB_KEY_ADDRS[1].to_owned(),
        asset: Asset::token_u64(6), /* Invalid TokenAmount */
    }];

    //
    // Act
    //
    for i in 0..2 {
        user_send_transaction_to_mempool(&mut network, "user1", "mempool1", &rb_send_txs[i]).await;
        mempool_handle_event(&mut network, "mempool1", &["Transactions added to tx pool"]).await;
        user_send_transaction_to_mempool(&mut network, "user1", "mempool1", &rb_recv_txs[i]).await;
        mempool_handle_event(
            &mut network,
            "mempool1",
            &["Some transactions invalid. Adding valid transactions only"],
        )
        .await;
    }

    //
    // Assert
    //
    let actual_pending_druid_pool = mempool_pending_druid_pool(&mut network, "mempool1").await;
    let actual_local_druid_pool = mempool_local_druid_pool(&mut network, "mempool1").await;

    assert_eq!(
        (
            actual_pending_druid_pool.len(),
            actual_local_druid_pool.len()
        ),
        (0, 0)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mempool_pause_update_and_resume_raft_3_nodes() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config_with_n_mempool_raft(11550, 3);

    // Initial network config as loaded
    let initial_network_config = MempoolNodeSharedConfig {
        mempool_partition_full_size: network_config.mempool_partition_full_size,
        mempool_mining_event_timeout: 500,
        mempool_miner_whitelist: Default::default(), // No whitelisting
    };

    // This is the configuration we want applied to all mempool nodes during runtime
    let shared_config_to_send = MempoolNodeSharedConfig {
        mempool_partition_full_size: 5,
        mempool_mining_event_timeout: 10000,
        mempool_miner_whitelist: MinerWhitelist {
            active: true, // This will enable whitelisting
            miner_api_keys: Some(
                // Filter based on API keys
                vec!["key1".to_string(), "key2".to_string()]
                    .into_iter()
                    .collect(),
            ),
            miner_addresses: None,
        },
    };

    let mempool_ring = &[
        "mempool1".to_string(),
        "mempool2".to_string(),
        "mempool3".to_string(),
    ];

    let mut network = Network::create_from_config(&network_config).await;
    // Complete very first mining round
    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    //
    // Act
    //

    // Create pause all nodes at block 0 + 1 = 1
    mempool_initiate_coordinated_pause_act("mempool1", mempool_ring, &mut network, 1).await;
    create_block_act(&mut network, Cfg::All, CfgNum::All).await;

    // Confirm that the nodes are actually all paused.
    let all_nodes_paused_after_block_create =
        mempool_all_nodes_paused(&mut network, mempool_ring).await;

    // All nodes should be paused now
    // ,and ready to update their configs
    // with the one received from "mempool1"
    mempool_initiate_send_shared_config_act(
        shared_config_to_send.clone(),
        "mempool1",
        mempool_ring,
        &mut network,
    )
    .await;

    // The miner should now get disconnected, since whitelisting has been applied
    miner_handle_event_failure(&mut network, "miner1", "Miner not authorized").await;

    // All shared config values should now be the same
    let all_nodes_same_config = all_nodes_same_config(&mut network, mempool_ring).await;

    // Resume the nodes again
    mempool_initiate_coordinated_resume_act("mempool1", mempool_ring, &mut network).await;

    // Confirm all nodes are now resumed
    let all_nodes_resumed_after_coordinated_resume =
        mempool_all_nodes_resumed(&mut network, mempool_ring).await;

    //
    // Assert
    //
    assert!(all_nodes_paused_after_block_create);
    assert!(all_nodes_same_config);
    assert!(all_nodes_resumed_after_coordinated_resume);
    assert!(initial_network_config != shared_config_to_send);
}

#[tokio::test(flavor = "current_thread")]
async fn mempool_re_align_utxo_set_1_node() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config_with_n_mempool_raft(11650, 1);
    let utxo_re_align_block_count = 2;
    network_config.utxo_re_align_block_modulo = Some(utxo_re_align_block_count);
    let mut network = Network::create_from_config(&network_config).await;

    // Complete very first mining round
    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, CfgPow::First, CfgNum::All, false, None).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    //
    // Act
    //

    // Create blocks 1 to 4
    //
    // At block 3, re-alignment will take place
    for i in 1..utxo_re_align_block_count + 2 {
        create_block_act(&mut network, Cfg::All, CfgNum::All).await;
        proof_of_work_act(
            &mut network,
            CfgPow::Parallel,
            CfgNum::Majority,
            false,
            None,
        )
        .await;
        send_block_to_storage_act(&mut network, CfgNum::Majority).await;

        // After the first block, misalign `pk_cache` with `base` of `TrackedUtxoSet`
        if i == 1 {
            let mut committed_utxo_set = mempool_committed_utxo_set(&mut network, "mempool1").await;
            if let Some(entry) = committed_utxo_set.first_entry() {
                let addr_to_remove = entry.get().script_public_key.clone().unwrap_or_default();
                // Remove entry for mis-alignment
                mempool_remove_entry_from_pk_cache(&mut network, "mempool1", &addr_to_remove).await;
            }
        }
    }

    let pk_cache_after_re_alignment = mempool_get_pk_cache(&mut network, "mempool1").await;
    let committed_utxo_set = mempool_committed_utxo_set(&mut network, "mempool1").await;

    // Ensure that `pk_cache` is contains all `OutPoint` values from `committed_utxo_set`
    let all_outpoints_present = move || {
        for (out_point, tx_out) in committed_utxo_set.into_iter() {
            let addr = tx_out.script_public_key.clone().unwrap_or_default();
            let pk_cache_entry = pk_cache_after_re_alignment.get(&addr).unwrap();
            if !pk_cache_entry.contains(&out_point) {
                return false;
            }
        }
        true
    };

    //
    // Assert
    //
    assert!(all_outpoints_present());
}

async fn mempool_remove_entry_from_pk_cache<'a>(network: &mut Network, mempool: &str, entry: &str) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    c.remove_pk_cache_entry(entry);
}

async fn mempool_get_pk_cache(
    network: &mut Network,
    mempool: &str,
) -> HashMap<String, BTreeSet<OutPoint>> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_pk_cache()
}

async fn mempool_get_shared_config(
    network: &mut Network,
    mempool: &str,
) -> MempoolNodeSharedConfig {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_shared_config()
}

async fn all_nodes_same_config(network: &mut Network, mempool_ring: &[String]) -> bool {
    let mut all_shared_config_same = true;
    let first_shared_config = mempool_get_shared_config(network, &mempool_ring[0]).await;
    for mempool in mempool_ring {
        let shared_config = mempool_get_shared_config(network, mempool).await;
        if shared_config != first_shared_config {
            all_shared_config_same = false;
            break;
        }
    }
    all_shared_config_same
}

async fn mempool_all_nodes_paused(network: &mut Network, mempool_ring: &[String]) -> bool {
    let mut all_nodes_paused = true;
    for mempool in mempool_ring {
        let c = network.mempool(mempool).unwrap().lock().await;
        if !c.is_paused().await {
            all_nodes_paused = false;
            break;
        }
    }
    all_nodes_paused
}

async fn mempool_all_nodes_resumed(network: &mut Network, mempool_ring: &[String]) -> bool {
    let mut all_nodes_resumed = true;
    for mempool in mempool_ring {
        let c = network.mempool(mempool).unwrap().lock().await;
        if c.is_paused().await {
            all_nodes_resumed = false;
            break;
        }
    }
    all_nodes_resumed
}

async fn mempool_initiate_coordinated_pause_act(
    mempool: &str,
    mempool_ring: &[String],
    network: &mut Network,
    b_num_from_current: u64,
) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    let _ = c.pause_nodes(b_num_from_current);
    drop(c); // Drop mempool node to avoid borrow checker violation
    node_all_handle_event(
        network,
        mempool_ring,
        &["Received coordinated pause request"],
    )
    .await;
    node_all_handle_event(network, mempool_ring, &["Node pause configuration set"]).await;
}

async fn mempool_initiate_send_shared_config_act(
    shared_config: MempoolNodeSharedConfig,
    mempool: &str,
    mempool_peers: &[String],
    network: &mut Network,
) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    let _ = c.send_shared_config(shared_config);
    drop(c); // Drop mempool node to avoid borrow checker violation
    node_all_handle_event(network, mempool_peers, &["Received shared config"]).await;
    node_all_handle_event(network, mempool_peers, &["Shared config applied"]).await;
}

async fn mempool_initiate_coordinated_resume_act(
    mempool: &str,
    mempool_ring: &[String],
    network: &mut Network,
) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    let _ = c.resume_nodes();
    drop(c); // Drop mempool node to avoid borrow checker violation
    node_all_handle_event(
        network,
        mempool_ring,
        &["Received coordinated resume request"],
    )
    .await;
    node_all_handle_event(network, mempool_ring, &["Node resumed"]).await;
}

//
// Node helpers
//

fn node_all<T: Clone>(nodes: &[String], value: T) -> Vec<T> {
    let len = nodes.len();
    (0..len).map(|_| value.clone()).collect()
}

fn node_select(nodes: &[String], cfg_num: CfgNum) -> Vec<String> {
    let len = node_select_len(nodes, cfg_num);
    nodes.iter().take(len).cloned().collect()
}

fn node_combined_select(
    nodes1: &[String],
    nodes2: &[String],
    ignore: &BTreeSet<String>,
    cfg_num: CfgNum,
) -> (Vec<String>, Vec<String>) {
    let len = node_select_len(nodes1, cfg_num);
    nodes1
        .iter()
        .cloned()
        .zip(nodes2.iter().cloned())
        .filter(|(n1, _)| !ignore.contains(n1))
        .filter(|(_, n2)| !ignore.contains(n2))
        .take(len)
        .unzip()
}

fn node_select_len(nodes: &[String], cfg_num: CfgNum) -> usize {
    let len = nodes.len();
    if cfg_num == CfgNum::Majority {
        len / 2 + 1
    } else {
        len
    }
}

async fn node_connect_to(network: &mut Network, from: &str, to: &str) {
    let to_addr = network.get_address(to).await.unwrap();
    if let Some(u) = network.user(from) {
        u.lock().await.connect_to(to_addr).await.unwrap();
    } else if let Some(m) = network.miner(from) {
        m.lock().await.connect_to(to_addr).await.unwrap();
    } else {
        panic!("node not found");
    }
}

async fn node_all_handle_event(network: &mut Network, node_group: &[String], reason_str: &[&str]) {
    let reason_str: Vec<_> = reason_str.iter().map(|s| s.to_string()).collect();
    let all_raisons = node_group
        .iter()
        .map(|n| (n.clone(), reason_str.clone()))
        .collect();
    node_all_handle_different_event(network, node_group, &all_raisons).await
}

async fn node_all_handle_different_event<'a>(
    network: &mut Network,
    node_group: &[String],
    all_raisons: &BTreeMap<String, Vec<String>>,
) {
    let mut join_handles = BTreeMap::new();
    let barrier = Arc::new(Barrier::new(node_group.len()));
    for node_name in node_group {
        let barrier = barrier.clone();
        let reason_str = all_raisons.get(node_name).cloned().unwrap_or_default();
        let node_name = node_name.clone();
        let mempool = network.mempool(&node_name).cloned();
        let storage = network.storage(&node_name).cloned();
        let miner = network.miner(&node_name).cloned();
        let user = network.user(&node_name).cloned();

        let peer_span = error_span!("peer", ?node_name);
        join_handles.insert(
            node_name.clone(),
            tokio::spawn(
                async move {
                    if let Some(mempool) = mempool {
                        mempool_one_handle_event(&mempool, &barrier, &reason_str).await;
                    } else if let Some(storage) = storage {
                        storage_one_handle_event(&storage, &barrier, &reason_str).await;
                    } else if let Some(miner) = miner {
                        miner_one_handle_event(&miner, &barrier, &reason_str).await;
                    } else if let Some(user) = user {
                        user_one_handle_event(&user, &barrier, &reason_str).await;
                    } else {
                        panic!("Node not found");
                    }
                }
                .instrument(peer_span),
            ),
        );
    }

    node_join_all_checked(join_handles, all_raisons)
        .await
        .unwrap();
}

async fn node_get_wallet_info(
    network: &mut Network,
    node: &str,
) -> (
    AssetValues,
    Vec<String>,
    BTreeMap<OutPoint, (String, Asset)>,
) {
    let (miner, user) = if let Some(miner) = network.miner(node) {
        (Some(miner.lock().await), None)
    } else if let Some(user) = network.user(node) {
        (None, Some(user.lock().await))
    } else {
        (None, None)
    };

    let wallet = match (&miner, &user) {
        (Some(m), _) => m.get_wallet_db(),
        (_, Some(u)) => u.get_wallet_db(),
        _ => panic!("node not found"),
    };

    let addresses = wallet.get_known_addresses();

    let fund = wallet.get_fund_store();
    let total = fund.running_total().clone();

    let mut txs_to_address_and_ammount = BTreeMap::new();
    for (out_p, asset) in fund.into_transactions().into_iter() {
        let addr = wallet.get_transaction_address(&out_p);
        txs_to_address_and_ammount.insert(out_p, (addr, asset));
    }
    (total, addresses, txs_to_address_and_ammount)
}

async fn node_all_get_wallet_info(
    network: &mut Network,
    miner_group: &[String],
) -> Vec<(
    AssetValues,
    Vec<String>,
    BTreeMap<OutPoint, (String, Asset)>,
)> {
    let mut result = Vec::new();
    for name in miner_group {
        let r = node_get_wallet_info(network, name).await;
        result.push(r);
    }
    result
}

async fn user_get_wallet_asset_totals_for_tx(
    network: &mut Network,
    from: &str,
    to: &str,
) -> (AssetValues, AssetValues) {
    let from_info = node_get_wallet_info(network, from).await.0;
    let to_info = node_get_wallet_info(network, to).await.0;
    (from_info, to_info)
}

async fn node_all_combined_get_wallet_info(
    network: &mut Network,
    miner_group: &[String],
) -> (
    AssetValues,
    Vec<String>,
    BTreeMap<OutPoint, (String, Asset)>,
) {
    let mut total = AssetValues::default();
    let mut addresses = Vec::new();
    let mut txs_to_address_and_ammount = BTreeMap::new();
    for name in miner_group {
        let (t, mut a, mut txs) = node_get_wallet_info(network, name).await;
        total += t;
        addresses.append(&mut a);
        txs_to_address_and_ammount.append(&mut txs);
    }
    (total, addresses, txs_to_address_and_ammount)
}

#[allow(clippy::type_complexity)]
fn node_all_combined_expected_wallet_info(
    miner_group: &[String],
    mining_reward: TokenAmount,
    stored: &[&BlockStoredInfo],
) -> (AssetValues, usize, BTreeMap<OutPoint, (String, Asset)>) {
    let mining_txs: Vec<_> = {
        let txs = stored.iter().flat_map(|b| b.mining_transactions.iter());
        txs.collect()
    };

    let total = mining_reward * mining_txs.len() as u64;
    let addresses = miner_group.len() + mining_txs.len();
    let txs_to_address_and_ammount = {
        let mining_tx_out = get_tx_out_with_out_point_cloned(mining_txs.iter().copied());
        mining_tx_out
            .map(|(k, tx_out)| (k, (tx_out.script_public_key, tx_out.value)))
            .map(|(k, (addr, amount))| (k, (addr.unwrap(), amount)))
            .collect()
    };

    (
        AssetValues::new(total, Default::default()),
        addresses,
        txs_to_address_and_ammount,
    )
}

async fn node_send_coordinated_shutdown(network: &mut Network, node: &str, at_block: u64) {
    let mut event_tx = network.get_local_event_tx(node).await.unwrap();
    let event = LocalEvent::CoordinatedShutdown(at_block);
    event_tx.send(event, "test shutdown").await.unwrap();
}

async fn node_send_startup_requests(network: &mut Network, node: &str) {
    network
        .send_startup_requests_named(&[node.to_string()])
        .await;
}

//
// MempoolNode helpers
//

async fn mempool_handle_event(network: &mut Network, mempool: &str, reason_str: &[&str]) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    mempool_handle_event_for_node(&mut c, true, reason_str, &mut test_timeout()).await;
}

async fn mempool_all_handle_event(
    network: &mut Network,
    mempool_group: &[String],
    reason_str: &str,
) {
    for mempool in mempool_group {
        mempool_handle_event(network, mempool, &[reason_str]).await;
    }
}

async fn mempool_handle_error(network: &mut Network, mempool: &str, reason_str: &[&str]) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    mempool_handle_event_for_node(&mut c, false, reason_str, &mut test_timeout()).await;
}

async fn mempool_all_handle_error(
    network: &mut Network,
    mempool_group: &[String],
    reason_str: &str,
) {
    for mempool in mempool_group {
        mempool_handle_error(network, mempool, &[reason_str]).await;
    }
}

async fn mempool_handle_event_for_node<E: Future<Output = &'static str> + Unpin>(
    c: &mut MempoolNode,
    success_val: bool,
    reason_val: &[&str],
    exit: &mut E,
) {
    let addr = c.local_address();
    match c.handle_next_event(exit).await {
        Some(Ok(Response { success, reason }))
            if success == success_val && reason_val.contains(&reason.as_str()) =>
        {
            info!("Mempool handle_next_event {} success ({})", reason, addr);
        }
        other => {
            error!(
                "Unexpected Mempool result: {:?} (expected:{:?})({})",
                other, reason_val, addr
            );
            panic!(
                "Unexpected Mempool result: {:?} (expected:{:?})({})",
                other, reason_val, addr
            );
        }
    }
}

async fn mempool_one_handle_event(
    mempool: &Arc<Mutex<MempoolNode>>,
    barrier: &Barrier,
    reason_str: &[String],
) {
    debug!("Start wait for event");

    let mut mempool = mempool.lock().await;
    for reason in reason_str {
        mempool_handle_event_for_node(&mut mempool, true, &[reason.as_str()], &mut test_timeout())
            .await;
    }

    debug!("Start wait for completion of other in raft group");

    let mut exit = test_timeout_barrier(barrier);
    mempool_handle_event_for_node(&mut mempool, true, &["Barrier complete"], &mut exit).await;

    debug!("Stop wait for event");
}

async fn mempool_skip_block_gen(network: &mut Network, mempool: &str, block_info: &CompleteBlock) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    let common = block_info.common.clone();
    c.test_skip_block_gen(common.block, common.block_txs);
}

async fn mempool_all_skip_block_gen(
    network: &mut Network,
    mempool_group: &[String],
    block_info: &CompleteBlock,
) {
    for mempool in mempool_group {
        mempool_skip_block_gen(network, mempool, block_info).await;
    }
}

async fn mempool_mined_block_num(network: &mut Network, mempool: &str) -> Option<u64> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_current_mined_block()
        .as_ref()
        .map(|b| b.common.block.header.b_num)
}

async fn mempool_all_mined_block_num(
    network: &mut Network,
    mempool_group: &[String],
) -> Vec<Option<u64>> {
    let mut result = Vec::new();
    for name in mempool_group {
        let r = mempool_mined_block_num(network, name).await;
        result.push(r);
    }
    result
}

async fn mempool_all_current_block_transactions(
    network: &mut Network,
    mempool_group: &[String],
) -> Vec<Option<Vec<String>>> {
    let mut result = Vec::new();
    for name in mempool_group {
        let r = mempool_current_block_transactions(network, name).await;
        result.push(r);
    }
    result
}

async fn mempool_current_block_transactions(
    network: &mut Network,
    mempool: &str,
) -> Option<Vec<String>> {
    mempool_current_mining_block(network, mempool)
        .await
        .map(|b| b.transactions)
}

async fn mempool_current_mining_block(network: &mut Network, mempool: &str) -> Option<Block> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_mining_block().clone()
}

async fn mempool_committed_current_block_num(network: &mut Network, mempool: &str) -> Option<u64> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_committed_current_block_num()
}

async fn mempool_all_committed_current_block_num(
    network: &mut Network,
    mempool_group: &[String],
) -> Vec<Option<u64>> {
    let mut result = Vec::new();
    for name in mempool_group {
        let r = mempool_committed_current_block_num(network, name).await;
        result.push(r);
    }
    result
}

async fn mempool_all_committed_utxo_set(
    network: &mut Network,
    mempool_group: &[String],
) -> Vec<UtxoSet> {
    let mut result = Vec::new();
    for name in mempool_group {
        let r = mempool_committed_utxo_set(network, name).await;
        result.push(r);
    }
    result
}

async fn mempool_committed_utxo_set(network: &mut Network, mempool: &str) -> UtxoSet {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_committed_utxo_set().clone()
}

async fn mempool_all_committed_tx_pool(
    network: &mut Network,
    mempool_group: &[String],
) -> Vec<BTreeMap<String, Transaction>> {
    let mut result = Vec::new();
    for name in mempool_group {
        let r = mempool_committed_tx_pool(network, name).await;
        result.push(r);
    }
    result
}

pub async fn mempool_committed_tx_pool(
    network: &mut Network,
    mempool: &str,
) -> BTreeMap<String, Transaction> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_committed_tx_pool().clone()
}

async fn mempool_all_committed_tx_druid_pool(
    network: &mut Network,
    mempool_group: &[String],
) -> Vec<Vec<BTreeMap<String, Transaction>>> {
    let mut result = Vec::new();
    for name in mempool_group {
        let r = mempool_committed_tx_druid_pool(network, name).await;
        result.push(r);
    }
    result
}

async fn mempool_committed_tx_druid_pool(
    network: &mut Network,
    mempool: &str,
) -> Vec<BTreeMap<String, Transaction>> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_committed_tx_druid_pool().clone()
}

async fn mempool_pending_druid_pool(network: &mut Network, mempool: &str) -> DruidPool {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_pending_druid_pool().clone()
}

async fn mempool_local_druid_pool(
    network: &mut Network,
    mempool: &str,
) -> Vec<BTreeMap<String, Transaction>> {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_local_druid_pool().clone()
}

async fn mempool_get_utxo_balance_for_user(
    network: &mut Network,
    mempool: &str,
    user: &str,
) -> AssetValues {
    let addresses = user_get_all_known_addresses(network, user).await;
    mempool_get_utxo_balance_for_addresses(network, mempool, addresses)
        .await
        .get_asset_values()
        .clone()
}

async fn miner_get_last_aggregation_address(network: &mut Network, miner: &str) -> Option<String> {
    let miner = network.miner(miner).unwrap().lock().await;
    miner.has_aggregation_tx_active()
}

async fn mempool_get_utxo_balance_for_addresses(
    network: &mut Network,
    mempool: &str,
    addresses: Vec<String>,
) -> TrackedUtxoBalance {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_committed_utxo_tracked_set()
        .get_balance_for_addresses(&addresses)
}

async fn mempool_get_prev_mining_reward(network: &mut Network, mempool: &str) -> TokenAmount {
    let c = network.mempool(mempool).unwrap().lock().await;
    c.get_current_mining_reward()
}

async fn mempool_all_inject_next_event(
    network: &mut Network,
    from_group: &[String],
    to_mempool_group: &[String],
    request: MempoolRequest,
) {
    for (from, to) in from_group.iter().zip(to_mempool_group.iter()) {
        mempool_inject_next_event(network, from, to, request.clone()).await;
    }
}

async fn mempool_inject_next_event(
    network: &mut Network,
    from: &str,
    to_mempool: &str,
    request: MempoolRequest,
) {
    let from_addr = network.get_address(from).await.unwrap();
    let c = network.mempool(to_mempool).unwrap().lock().await;

    c.inject_next_event(from_addr, request).unwrap();
}

async fn mempool_flood_rand_and_block_to_partition(network: &mut Network, mempool: &str) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    c.flood_rand_and_block_to_partition().await.unwrap();
}

async fn mempool_flood_transactions_to_partition(network: &mut Network, mempool: &str) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    c.flood_transactions_to_partition().await.unwrap();
}

async fn mempool_flood_block_to_users(network: &mut Network, mempool: &str) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    c.flood_block_to_users().await.unwrap();
}

async fn mempool_send_block_to_storage(network: &mut Network, mempool: &str) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    c.send_block_to_storage().await.unwrap();
}

async fn mempool_all_send_block_to_storage(network: &mut Network, mempool_group: &[String]) {
    for mempool in mempool_group {
        mempool_send_block_to_storage(network, mempool).await;
    }
}

async fn mempool_skip_mining(network: &mut Network, mempool: &str, block_info: &CompleteBlock) {
    let mut c = network.mempool(mempool).unwrap().lock().await;

    let seed = block_info.common.block.header.seed_value.clone();
    let winning_pow_info = complete_block_winning_pow(block_info);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

    c.test_skip_mining((addr, winning_pow_info), seed)
}

async fn mempool_all_skip_mining(
    network: &mut Network,
    mempool_group: &[String],
    block_info: &CompleteBlock,
) {
    for mempool in mempool_group {
        mempool_skip_mining(network, mempool, block_info).await;
    }
}

async fn mempool_get_filtered_participants(
    network: &mut Network,
    mempool: &str,
    mining_group: &[String],
) -> Vec<String> {
    let c = network.mempool(mempool).unwrap().lock().await;
    let sockets = c.get_mining_participants();

    let mut participants = Vec::new();
    for miner in mining_group {
        let socket = network.get_address(miner).await.unwrap();
        if sockets.contains(&socket) {
            participants.push(miner.clone());
        }
    }
    participants
}

async fn mempool_send_utxo_set(network: &mut Network, mempool: &str) {
    let mut c = network.mempool(mempool).unwrap().lock().await;
    c.send_fetched_utxo_set().await.unwrap();
}

//
// StorageNode helpers
//

async fn storage_inject_next_event(
    network: &mut Network,
    from: &str,
    to_storage: &str,
    request: StorageRequest,
) {
    let from_addr = network.get_address(from).await.unwrap();
    let s = network.storage(to_storage).unwrap().lock().await;

    s.inject_next_event(from_addr, request).unwrap();
}

async fn storage_send_blockchain_item(network: &mut Network, from_storage: &str) {
    let mut s = network.storage(from_storage).unwrap().lock().await;
    s.send_blockchain_item().await.unwrap();
}

async fn storage_catchup_fetch_blockchain_item(network: &mut Network, from_storage: &str) {
    let mut s = network.storage(from_storage).unwrap().lock().await;
    s.catchup_fetch_blockchain_item().await.unwrap();
}

async fn storage_inject_send_block_to_storage(
    network: &mut Network,
    mempool: &str,
    storage: &str,
    block_info: &CompleteBlock,
) {
    let mined_block = Some(MinedBlock {
        common: block_info.common.clone(),
        extra_info: block_info.extra_info.clone(),
    });
    let request = StorageRequest::SendBlock { mined_block };
    storage_inject_next_event(network, mempool, storage, request).await;
}

async fn storage_get_stored_key_values_count(network: &mut Network, storage: &str) -> usize {
    let s = network.storage(storage).unwrap().lock().await;
    s.get_stored_values_count()
}

async fn storage_all_get_stored_key_values_count(
    network: &mut Network,
    storage_group: &[String],
) -> Vec<usize> {
    let mut result = Vec::new();
    for name in storage_group {
        let r = storage_get_stored_key_values_count(network, name).await;
        result.push(r);
    }
    result
}

async fn storage_get_last_block_stored(
    network: &mut Network,
    storage: &str,
) -> Option<BlockStoredInfo> {
    let s = network.storage(storage).unwrap().lock().await;
    s.get_last_block_stored().clone()
}

async fn storage_all_get_last_block_stored_num(
    network: &mut Network,
    storage_group: &[String],
) -> Vec<Option<u64>> {
    let mut result = Vec::new();
    for name in storage_group {
        let r = storage_get_last_block_stored(network, name).await;
        result.push(r.map(|bs| bs.block_num));
    }
    result
}

async fn storage_get_last_stored_info(
    network: &mut Network,
    storage: &str,
) -> (Option<String>, Option<(String, u64, usize)>) {
    let s = network.storage(storage).unwrap().lock().await;
    if let Some(info) = s.get_last_block_stored() {
        let complete = storage_get_stored_complete_block_for_node(&s, &info.block_hash);

        (
            complete,
            Some((
                info.block_hash.clone(),
                info.block_num,
                info.mining_transactions.len(),
            )),
        )
    } else {
        (None, None)
    }
}

fn storage_get_stored_complete_block_for_node(s: &StorageNode, block_hash: &str) -> Option<String> {
    let stored_block = {
        let stored_value = s.get_stored_value(block_hash)?;
        let stored_value =
            checked_blockchain_item_data(stored_value, BlockchainItemType::Block).unwrap();
        {
            // Check match expected block_hash:
            let mut reader = Cursor::new(stored_value.as_slice());
            let header = match deserialize_from::<_, BlockHeader>(&mut reader) {
                Err(e) => return Some(format!("error: {e:?}")),
                Ok(v) => v,
            };

            let (header_ser, txs_ser) = stored_value.split_at(reader.position() as usize);
            let header_hash = "b".to_owned() + &hex::encode(sha3_256::digest(header_ser));
            let txs_hash = hex::encode(sha3_256::digest(txs_ser));

            if block_hash != header_hash || txs_hash != header.txs_merkle_root_and_hash.1 {
                return Some(format!(
                    "error: {}!={} or {}!={}",
                    block_hash, header_hash, txs_hash, header.txs_merkle_root_and_hash.1
                ));
            }
        }

        match deserialize::<StoredSerializingBlock>(&stored_value) {
            Err(e) => return Some(format!("error: {e:?}")),
            Ok(v) => v,
        }
    };

    let mut block_txs = BTreeMap::new();
    let all_txs = all_ordered_stored_block_tx_hashes(
        &stored_block.block.transactions,
        std::iter::once(&stored_block.block.header.nonce_and_mining_tx_hash),
    );

    for (_, tx_hash) in all_txs {
        let stored_value = s.get_stored_value(tx_hash)?;
        let stored_value =
            checked_blockchain_item_data(stored_value, BlockchainItemType::Tx).unwrap();
        let stored_tx = match deserialize::<Transaction>(&stored_value) {
            Err(e) => return Some(format!("error tx hash: {e:?} : {tx_hash:?}")),
            Ok(v) => v,
        };
        block_txs.insert(tx_hash.clone(), stored_tx);
    }

    let extra_info = MinedBlockExtraInfo { shutdown: false };

    let common = CommonBlockInfo {
        block: stored_block.block,
        block_txs,
        pow_d_value: Default::default(),
        pow_p_value: Default::default(),
        unicorn: Default::default(),
        unicorn_witness: Default::default(),
    };
    let complete = CompleteBlock { common, extra_info };
    Some(format!("{complete:?}"))
}

async fn storage_all_get_last_stored_info(
    network: &mut Network,
    storage_group: &[String],
) -> Vec<(Option<String>, Option<(String, u64, usize)>)> {
    let mut result = Vec::new();
    for name in storage_group {
        let r = storage_get_last_stored_info(network, name).await;
        result.push(r);
    }
    result
}

async fn storage_send_stored_block(network: &mut Network, storage: &str) {
    let mut s = network.storage(storage).unwrap().lock().await;
    s.send_stored_block().await.unwrap();
}

async fn storage_all_send_stored_block(network: &mut Network, storage_group: &[String]) {
    for storage in storage_group {
        storage_send_stored_block(network, storage).await;
    }
}

async fn storage_handle_event(network: &mut Network, storage: &str, reason_str: &str) {
    let mut s = network.storage(storage).unwrap().lock().await;
    storage_handle_event_for_node(&mut s, true, reason_str, &mut test_timeout()).await;
}

async fn storage_all_handle_event(
    network: &mut Network,
    storage_group: &[String],
    reason_str: &str,
) {
    for storage in storage_group {
        storage_handle_event(network, storage, reason_str).await;
    }
}

async fn storage_handle_event_for_node<E: Future<Output = &'static str> + Unpin>(
    s: &mut StorageNode,
    success_val: bool,
    reason_val: &str,
    exit: &mut E,
) {
    let addr = s.local_address();
    match s.handle_next_event(exit).await {
        Some(Ok(Response { success, reason }))
            if success == success_val && reason == reason_val =>
        {
            info!(
                "Storage handle_next_event {} success ({})",
                reason_val, addr
            );
        }
        other => {
            error!(
                "Unexpected Storage result: {:?} (expected:{})({})",
                other, reason_val, addr
            );
            panic!(
                "Unexpected Storage result: {:?} (expected:{})({})",
                other, reason_val, addr
            );
        }
    }
}

async fn storage_one_handle_event(
    storage: &Arc<Mutex<StorageNode>>,
    barrier: &Barrier,
    reason_str: &[String],
) {
    debug!("Start wait for event");

    let mut storage = storage.lock().await;
    for reason in reason_str {
        storage_handle_event_for_node(&mut storage, true, reason, &mut test_timeout()).await;
    }

    debug!("Start wait for completion of other in raft group");

    let mut exit = test_timeout_barrier(barrier);
    storage_handle_event_for_node(&mut storage, true, "Barrier complete", &mut exit).await;

    debug!("Stop wait for event");
}

//
// UserNode helpers
//

async fn user_handle_event_failure(network: &mut Network, user: &str, reason_val: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    user_handle_event_for_node(&mut u, false, reason_val, &mut test_timeout()).await;
}

async fn user_handle_event(network: &mut Network, user: &str, reason_val: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    user_handle_event_for_node(&mut u, true, reason_val, &mut test_timeout()).await;
}

async fn user_handle_error(network: &mut Network, user: &str, reason_val: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    user_handle_event_for_node(&mut u, false, reason_val, &mut test_timeout()).await;
}

async fn user_handle_event_for_node<E: Future<Output = &'static str> + Unpin>(
    u: &mut UserNode,
    success_val: bool,
    reason_val: &str,
    exit: &mut E,
) {
    let addr = u.local_address();
    match u.handle_next_event(exit).await {
        Some(Ok(Response { success, reason }))
            if success == success_val && reason == reason_val =>
        {
            info!("User handle_next_event {} success ({})", reason_val, addr);
        }
        other => {
            error!(
                "Unexpected User result: {:?} (expected:{})({})",
                other, reason_val, addr
            );
            panic!(
                "Unexpected User result: {:?} (expected:{})({})",
                other, reason_val, addr
            );
        }
    }
}

async fn user_one_handle_event(
    user: &Arc<Mutex<UserNode>>,
    barrier: &Barrier,
    reason_str: &[String],
) {
    debug!("Start wait for event");

    let mut user = user.lock().await;
    for reason in reason_str {
        user_handle_event_for_node(&mut user, true, reason, &mut test_timeout()).await;
    }

    debug!("Start wait for completion of other in raft group");

    let mut exit = test_timeout_barrier(barrier);
    user_handle_event_for_node(&mut user, true, "Barrier complete", &mut exit).await;

    debug!("Stop wait for event");
}

async fn user_send_transaction_to_mempool(
    network: &mut Network,
    from_user: &str,
    to_mempool: &str,
    tx: &Transaction,
) {
    let mempool_node_addr = network.get_address(to_mempool).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_transactions_to_mempool(mempool_node_addr, vec![tx.clone()])
        .await
        .unwrap();
}

async fn user_send_next_payment_to_destinations(
    network: &mut Network,
    from_user: &str,
    to_mempool: &str,
) {
    let mempool_node_addr = network.get_address(to_mempool).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_next_payment_to_destinations(mempool_node_addr)
        .await
        .unwrap();
}

async fn users_filter_locked_coinbase(network: &mut Network, users: &[&str], b_num: u64) {
    for user in users {
        user_filter_locked_coinbase(network, user, b_num).await;
    }
}

async fn user_filter_locked_coinbase(network: &mut Network, user: &str, b_num: u64) {
    let mut u = network.user(user).unwrap().lock().await;
    u.filter_locked_coinbase(b_num).await;
}

async fn user_send_address_request(
    network: &mut Network,
    from_user: &str,
    to_user: &str,
    amount: TokenAmount,
    locktime: Option<u64>,
) {
    let user_node_addr = network.get_address(to_user).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_address_request(user_node_addr, amount, locktime)
        .await
        .unwrap();
}

async fn user_generate_static_address_for_miner(network: &mut Network, user: &str) -> String {
    let mut u = network.user(user).unwrap().lock().await;
    u.generate_static_address_for_miner().await
}

async fn user_send_donation_address_to_peer(network: &mut Network, from_user: &str, to_user: &str) {
    let user_node_addr = network.get_address(to_user).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_donation_address_to_peer(user_node_addr)
        .await
        .unwrap();
}

async fn user_send_address_to_trading_peer(network: &mut Network, user: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    u.send_address_to_trading_peer().await.unwrap();
}

async fn user_process_mining_notified(
    network: &mut Network,
    user: &str,
) -> Option<BTreeMap<String, Transaction>> {
    let mut u = network.user(user).unwrap().lock().await;
    u.process_mining_notified().await;
    u.pending_test_auto_gen_txs()
        .cloned()
        .map(|m| m.into_iter().map(|(k, (tx, _))| (k, tx)).collect())
}

async fn user_get_received_utxo_set_keys(network: &mut Network, user: &str) -> Vec<OutPoint> {
    let u = network.user(user).unwrap().lock().await;
    let received_utxo_set = u.get_received_utxo().unwrap();
    received_utxo_set.keys().cloned().collect()
}

async fn user_update_running_total(network: &mut Network, user: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    u.update_running_total().await;
}

async fn user_get_tokens_held(network: &mut Network, user: &str) -> TokenAmount {
    let u = network.user(user).unwrap().lock().await;
    u.get_wallet_db().get_fund_store().running_total().tokens
}

async fn user_get_all_known_addresses(network: &mut Network, user: &str) -> Vec<String> {
    let u = network.user(user).unwrap().lock().await;
    u.get_wallet_db().get_known_addresses()
}

async fn user_trigger_update_wallet_from_utxo_set(
    network: &mut Network,
    user: &str,
    address_list: UtxoFetchType,
) {
    let u = network.user(user).unwrap().lock().await;
    let request = UserRequest::UserApi(UserApiRequest::UpdateWalletFromUtxoSet { address_list });
    u.api_inputs()
        .1
        .inject_next_event(u.local_address(), request)
        .unwrap();
}

async fn user_send_request_utxo_set(
    network: &mut Network,
    user: &str,
    address_list: UtxoFetchType,
) {
    let mut u = network.user(user).unwrap().lock().await;
    let mempool_addr = u.mempool_address();
    u.send_request_utxo_set(
        address_list,
        mempool_addr,
        crate::interfaces::NodeType::User,
    )
    .await
    .unwrap();
}

async fn user_send_item_based_payment_request(
    network: &mut Network,
    from: &str,
    to: &str,
    send_asset: Asset,
    genesis_hash: Option<String>, /* Expected DRS tx hash from recipient */
) {
    let mut u = network.user(from).unwrap().lock().await;
    let to_addr = network.get_address(to).await.unwrap();
    u.send_rb_payment_request(to_addr, send_asset, genesis_hash)
        .await
        .unwrap();
}

async fn user_send_item_based_payment_response(network: &mut Network, user: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    u.send_rb_payment_response().await.unwrap();
}

async fn user_send_item_based_payment_to_desinations(
    network: &mut Network,
    user: &str,
    mempool: &str,
) {
    let mut u = network.user(user).unwrap().lock().await;
    let mempool_addr = network.get_address(mempool).await.unwrap();
    u.send_next_rb_transaction_to_destinations(mempool_addr)
        .await
        .unwrap();
}

async fn user_send_item_asset(
    network: &mut Network,
    user: &str,
    mempool: &str,
    item_amount: u64,
    item_metadata: Option<String>,
) -> String {
    // Returns transactio hash
    let mut u = network.user(user).unwrap().lock().await;
    let mempool_addr = network.get_address(mempool).await.unwrap();
    u.generate_item_asset_tx(item_amount, GenesisTxHashSpec::Create, item_metadata)
        .await;
    let tx_hash = construct_tx_hash(&u.get_next_payment_transaction().unwrap().1);
    u.send_next_payment_to_destinations(mempool_addr)
        .await
        .unwrap();
    tx_hash
}

//
// MinerNode helpers
//

async fn miner_get_blockchain_item_received_b_num(
    network: &mut Network,
    miner: &str,
) -> Option<(u64, (char, u64, u32))> {
    let mut m = network.miner(miner).unwrap().lock().await;
    let (_, item, _) = m.get_blockchain_item_received().await.as_ref()?;
    let block: StoredSerializingBlock = deserialize(&item.data).ok()?;
    Some((block.block.header.b_num, miner_blockchain_item_meta(item)))
}

async fn miner_get_blockchain_item_received_tx_lens(
    network: &mut Network,
    miner: &str,
) -> Option<(usize, usize, (char, u64, u32))> {
    let mut m = network.miner(miner).unwrap().lock().await;
    let (_, item, _) = m.get_blockchain_item_received().await.as_ref()?;
    let tx: Transaction = deserialize(&item.data).ok()?;
    Some((
        tx.inputs.len(),
        tx.outputs.len(),
        miner_blockchain_item_meta(item),
    ))
}

fn miner_blockchain_item_meta(item: &BlockchainItem) -> (char, u64, u32) {
    match item.item_meta {
        BlockchainItemMeta::Block { block_num, tx_len } => ('b', block_num, tx_len),
        BlockchainItemMeta::Tx { block_num, tx_num } => ('t', block_num, tx_num),
    }
}

async fn miner_handle_event_failure(network: &mut Network, miner: &str, reason_val: &str) {
    let mut m = network.miner(miner).unwrap().lock().await;
    miner_handle_event_for_node(&mut m, false, reason_val, &mut test_timeout()).await;
}

async fn miner_handle_event(network: &mut Network, miner: &str, reason_val: &str) {
    let mut m = network.miner(miner).unwrap().lock().await;
    miner_handle_event_for_node(&mut m, true, reason_val, &mut test_timeout()).await;
}

async fn miner_all_handle_event(network: &mut Network, miner_group: &[String], reason_str: &str) {
    for miner in miner_group {
        miner_handle_event(network, miner, reason_str).await;
    }
}

async fn miner_handle_event_for_node<E: Future<Output = &'static str> + Unpin>(
    m: &mut MinerNode,
    success_val: bool,
    reason_val: &str,
    exit: &mut E,
) {
    let addr = m.local_address();
    match m.handle_next_event(exit).await {
        Some(Ok(Response { success, reason }))
            if success == success_val && reason == reason_val =>
        {
            info!("Miner handle_next_event {} success ({})", reason_val, addr);
        }
        other => {
            error!(
                "Unexpected Miner result: {:?} (expected:{})({})",
                other, reason_val, addr
            );
            panic!(
                "Unexpected Miner result: {:?} (expected:{})({})",
                other, reason_val, addr
            );
        }
    }
}

async fn miner_one_handle_event(
    miner: &Arc<Mutex<MinerNode>>,
    barrier: &Barrier,
    reason_str: &[String],
) {
    debug!("Start wait for event");

    let mut miner = miner.lock().await;
    for reason in reason_str {
        miner_handle_event_for_node(&mut miner, true, reason, &mut test_timeout()).await;
    }

    debug!("Start wait for completion of other in raft group");

    let mut exit = test_timeout_barrier(barrier);
    miner_handle_event_for_node(&mut miner, true, "Barrier complete", &mut exit).await;

    debug!("Stop wait for event");
}

async fn miner_process_found_partition_pow(network: &mut Network, from_miner: &str) {
    let mut m = network.miner(from_miner).unwrap().lock().await;
    m.process_found_partition_pow().await;
}

async fn miner_has_aggregation_tx_active(
    network: &mut Network,
    from_miner: &str,
) -> Option<String> {
    let m = network.miner(from_miner).unwrap().lock().await;
    m.has_aggregation_tx_active()
}

async fn miner_process_found_block_pow(network: &mut Network, from_miner: &str) {
    let mut m = network.miner(from_miner).unwrap().lock().await;
    m.process_found_block_pow().await;
}

async fn miner_set_static_miner_address(network: &mut Network, miner: &str, static_addr: String) {
    let mut m = network.miner(miner).unwrap().lock().await;
    m.set_static_miner_address(Some(static_addr)).await;
}

//
// Test helpers
//

fn test_step_start() {
    let _ = tracing_log_try_init();
    info!("Test Step start");
}

async fn test_step_complete(network: Network) {
    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
}

fn valid_transactions(fixed: bool) -> BTreeMap<String, Transaction> {
    valid_transactions_with(fixed, DEFAULT_SEED_AMOUNT, true)
}

fn valid_transactions_with(
    fixed: bool,
    amount: TokenAmount,
    _with_create: bool,
) -> BTreeMap<String, Transaction> {
    let (pk, sk) = if !fixed {
        let (pk, sk) = sign::gen_keypair();
        (pk, sk)
    } else {
        let sk_slice = hex::decode(COMMON_SEC_KEY).unwrap();
        let pk_slice = hex::decode(COMMON_PUB_KEY).unwrap();
        let sk = SecretKey::from_slice(&sk_slice).unwrap();
        let pk = PublicKey::from_slice(&pk_slice).unwrap();
        (pk, sk)
    };

    let txs = vec![
        (&VALID_TXS_IN[0..1], &VALID_TXS_OUT[0..1]),
        (&VALID_TXS_IN[1..3], &VALID_TXS_OUT[1..3]),
    ];

    let mut transactions = BTreeMap::new();
    for (ins, outs) in &txs {
        let (t_hash, payment_tx) =
            create_valid_transaction_with_ins_outs(ins, outs, &pk, &sk, amount, None);
        transactions.insert(t_hash, payment_tx);
    }

    transactions
}

fn make_mempool_seed_utxo(seed: &[(i32, &str)], amount: TokenAmount) -> UtxoSetSpec {
    let seed: Vec<_> = seed
        .iter()
        .copied()
        .map(|(n, v)| (v, (0..n).map(|_| (COMMON_PUB_KEY, amount)).collect()))
        .collect();
    make_mempool_seed_utxo_with_info(&seed)
}

fn make_mempool_seed_utxo_with_info(seed: &[(&str, Vec<(&str, TokenAmount)>)]) -> UtxoSetSpec {
    seed.iter()
        .map(|(v, txo)| {
            (
                v.to_string(),
                txo.iter()
                    .map(|(pk, amount)| TxOutSpec {
                        public_key: pk.to_string(),
                        amount: *amount,
                        locktime: 0,
                    })
                    .collect(),
            )
        })
        .collect()
}

fn block_and_partition_evt_in_miner_pow(
    c_miners: &[String],
    in_miners: &[String],
) -> BTreeMap<String, Vec<String>> {
    let init_evt = |m: &String| (m.clone(), vec![]);
    let mut evts = c_miners.iter().map(init_evt).collect::<BTreeMap<_, _>>();
    for (key, evt) in evts.iter_mut() {
        if in_miners.contains(key) {
            evt.push("Pre-block received successfully".to_owned());
            evt.push("Partition PoW complete".to_owned());
            evt.push("Block PoW complete".to_owned());
        } else {
            evt.push("Received random number successfully".to_owned());
            evt.push("Partition PoW complete".to_owned());
        }
    }
    evts
}

fn panic_on_timeout<E>(response: &Result<Response, E>, tag: &str) {
    if let Ok(Response {
        success: true,
        reason: _,
    }) = response
    {
        panic!("Test timeout elapsed - {}", tag);
    }
}

fn test_timeout() -> impl Future<Output = &'static str> + Unpin {
    Box::pin(async move {
        time::sleep(TIMEOUT_TEST_WAIT_DURATION).await;
        "Test timeout elapsed"
    })
}

fn test_timeout_barrier(barrier: &'_ Barrier) -> impl Future<Output = &'static str> + Unpin + '_ {
    Box::pin(async move {
        tokio::select! {
            r = test_timeout() => r,
            _ = barrier.wait() => "Barrier complete",
        }
    })
}

fn equal_first<T: Eq>(values: &[T]) -> Vec<bool> {
    values.iter().map(|v| *v == values[0]).collect()
}

fn len_and_map<K, V>(values: &BTreeMap<K, V>) -> (usize, &BTreeMap<K, V>) {
    (values.len(), values)
}

fn remove_keys<'a, Q: 'a + ?Sized + Ord, K: std::borrow::Borrow<Q> + Ord, V>(
    value: &mut BTreeMap<K, V>,
    keys: impl Iterator<Item = &'a Q>,
) {
    for key in keys {
        value.remove(key).unwrap();
    }
}

fn substract_vec(value1: &[usize], value2: &[usize]) -> Vec<usize> {
    value1
        .iter()
        .zip(value2.iter())
        .map(|(v1, v2)| v1 - v2)
        .collect()
}

fn merge_txs_3(v1: &UtxoSet, v2: &UtxoSet, v3: &UtxoSet) -> UtxoSet {
    v1.clone()
        .into_iter()
        .chain(v2.clone())
        .chain(v3.clone())
        .collect()
}

fn wallet_seed(out_p: (i32, &str), amount: &TokenAmount) -> WalletTxSpec {
    WalletTxSpec {
        out_point: format!("{}-{}", out_p.0, out_p.1),
        secret_key: COMMON_SEC_KEY.to_owned(),
        public_key: COMMON_PUB_KEY.to_owned(),
        amount: amount.0,
        address_version: None,
    }
}

fn valid_txs_in() -> UtxoSet {
    VALID_TXS_IN
        .iter()
        .map(|(n, h)| (OutPoint::new(h.to_string(), *n), TxOut::new()))
        .collect()
}

fn to_utxo_set(txs: &BTreeMap<String, Transaction>) -> UtxoSet {
    get_tx_out_with_out_point_cloned(txs.iter()).collect()
}

fn complete_block_mining_txs(block_info: &CompleteBlock) -> BTreeMap<String, Transaction> {
    let winning_pow_info = complete_block_winning_pow(block_info);
    std::iter::once(winning_pow_info.mining_tx).collect()
}

fn complete_block_winning_pow(block_info: &CompleteBlock) -> WinningPoWInfo {
    let header = &block_info.common.block.header;
    let (nonce, tx_hash) = header.nonce_and_mining_tx_hash.clone();
    let tx = block_info.common.block_txs.get(&tx_hash).unwrap().clone();
    WinningPoWInfo {
        nonce,
        mining_tx: (tx_hash, tx),
        p_value: block_info.common.pow_p_value,
        d_value: block_info.common.pow_d_value,
    }
}

async fn complete_first_block(
    next_block_tx: &BTreeMap<String, Transaction>,
) -> ((String, String), CompleteBlock) {
    complete_block(0, None, next_block_tx).await
}

async fn construct_mining_common_info(
    mut block: Block,
    mut block_txs: BTreeMap<String, Transaction>,
    addr: String,
) -> CommonBlockInfo {
    let block_num = block.header.b_num;
    let amount = calculate_reward(TokenAmount(0));
    let tx = construct_coinbase_tx(block_num, amount, addr);
    let hash = construct_tx_hash(&tx);
    block.header = apply_mining_tx(block.header, Vec::new(), hash.clone());
    block.header.nonce_and_mining_tx_hash.0 = generate_pow_for_block(&block.header)
        .expect("error occurred while mining block")
        .expect("couldn't find a valid nonce");
    block_txs.insert(hash, tx);

    CommonBlockInfo {
        block,
        block_txs,
        pow_d_value: Default::default(),
        pow_p_value: Default::default(),
        unicorn: Default::default(),
        unicorn_witness: Default::default(),
    }
}

fn checked_blockchain_item_data(
    v: BlockchainItem,
    t: BlockchainItemType,
) -> Result<Vec<u8>, StringError> {
    if v.version != NETWORK_VERSION || v.item_meta.as_type() != t {
        Err(StringError(format!(
            "blockchain_item check v:{}={}, t:{:?}=={:?}",
            v.version,
            NETWORK_VERSION,
            v.item_meta.as_type(),
            t
        )))
    } else {
        Ok(v.data)
    }
}

async fn complete_blocks(
    block_count: usize,
    block_txs: &[BTreeMap<String, Transaction>],
) -> ((Vec<String>, Vec<String>), Vec<CompleteBlock>) {
    let no_transactions = BTreeMap::new();

    let mut block_keys: Vec<String> = Vec::new();
    let mut complete_strs: Vec<String> = Vec::new();
    let mut blocks: Vec<CompleteBlock> = Vec::new();

    for i in 0..block_count {
        let previous = block_keys.last().map(|v| v.as_str());
        let transactions = block_txs.get(i).unwrap_or(&no_transactions);
        let ((block_key, complete_str), block) =
            complete_block(i as u64, previous, transactions).await;

        block_keys.push(block_key);
        complete_strs.push(complete_str);
        blocks.push(block);
    }
    ((block_keys, complete_strs), blocks)
}

async fn complete_block(
    block_num: u64,
    previous_hash: Option<&str>,
    block_txs: &BTreeMap<String, Transaction>,
) -> ((String, String), CompleteBlock) {
    complete_block_with_seed(
        block_num,
        previous_hash,
        block_txs,
        "fixed_test_seed".to_owned(),
    )
    .await
}

async fn complete_block_with_seed(
    block_num: u64,
    previous_hash: Option<&str>,
    block_txs: &BTreeMap<String, Transaction>,
    seed: String,
) -> ((String, String), CompleteBlock) {
    let mut block = Block::new();
    block.header.seed_value = seed.into_bytes();
    block.header.b_num = block_num;
    block.header.previous_hash = previous_hash.map(|v| v.to_string());
    block.transactions = block_txs.keys().cloned().collect();
    block.set_txs_merkle_root_and_hash().await;

    let addr = hex::encode(vec![block_num as u8, 1_u8]);
    let common = construct_mining_common_info(block.clone(), block_txs.clone(), addr).await;

    let complete = CompleteBlock {
        common,
        extra_info: MinedBlockExtraInfo { shutdown: false },
    };

    let stored = StoredSerializingBlock {
        block: complete.common.block.clone(),
    };

    let hash_key = construct_valid_block_pow_hash(&stored.block).unwrap();
    let complete_str = format!("{complete:?}");

    ((hash_key, complete_str), complete)
}

fn basic_network_config(initial_port: u16) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        mempool_raft: false,
        storage_raft: false,
        in_memory_db: true,
        mempool_partition_full_size: 1,
        mempool_minimum_miner_pool_len: 1,
        nodes: vec![(NodeType::User, vec!["user1".to_string()])]
            .into_iter()
            .collect(),
        mempool_seed_utxo: make_mempool_seed_utxo(SEED_UTXO, DEFAULT_SEED_AMOUNT),
        mempool_genesis_tx_in: None,
        user_wallet_seeds: Vec::new(),
        mempool_to_miner_mapping: Default::default(),
        test_duration_divider: TEST_DURATION_DIVIDER,
        passphrase: Some("Test Passphrase".to_owned()),
        user_auto_donate: 0,
        user_test_auto_gen_setup: Default::default(),
        tls_config: Default::default(),
        routes_pow: Default::default(),
        backup_block_modulo: Default::default(),
        utxo_re_align_block_modulo: Default::default(),
        backup_restore: Default::default(),
        enable_pipeline_reset: Default::default(),
        static_miner_address: Default::default(),
        mempool_miner_whitelist: Default::default(),
        mining_api_key: Default::default(),
        peer_limit: 1000,
        address_aggregation_limit: Some(5),
        initial_issuances: Default::default(),
    }
}

fn complete_network_config(initial_port: u16) -> NetworkConfig {
    complete_network_config_with_n_mempool_miner(initial_port, false, 1, 1)
}

fn complete_network_config_with_n_mempool_raft(
    initial_port: u16,
    mempool_count: usize,
) -> NetworkConfig {
    complete_network_config_with_n_mempool_miner(initial_port, true, mempool_count, mempool_count)
}

fn complete_network_config_with_n_mempool_miner(
    initial_port: u16,
    use_raft: bool,
    raft_count: usize,
    miner_count: usize,
) -> NetworkConfig {
    basic_network_config(initial_port)
        .with_raft(use_raft)
        .with_groups(raft_count, miner_count)
}
