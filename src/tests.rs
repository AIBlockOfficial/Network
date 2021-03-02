//! Test suite for the network functions.

use crate::compute::ComputeNode;
use crate::configurations::{TxOutSpec, UserNodeSetup, UtxoSetSpec, WalletTxSpec};
use crate::constants::SANC_LIST_TEST;
use crate::interfaces::{
    BlockStoredInfo, CommonBlockInfo, ComputeRequest, MinedBlockExtraInfo, Response,
    StorageRequest, UtxoSet,
};
use crate::storage::{StorageNode, StoredSerializingBlock};
use crate::storage_raft::CompleteBlock;
use crate::test_utils::{remove_all_node_dbs, Network, NetworkConfig, NodeType};
use crate::transaction_gen::TransactionGen;
use crate::utils::{
    calculate_reward, concat_merkle_coinbase, create_valid_transaction_with_ins_outs,
    get_sanction_addresses, validate_pow_block,
};
use bincode::serialize;
use futures::future::join_all;
use naom::primitives::asset::TokenAmount;
use naom::primitives::block::Block;
use naom::primitives::transaction::{OutPoint, Transaction, TxOut};
use naom::primitives::transaction_utils::{
    construct_coinbase_tx, construct_tx_hash, get_tx_out_with_out_point_cloned,
    get_tx_with_out_point,
};
use naom::script::StackEntry;
use rand::{self, Rng};
use sha3::Digest;
use sha3::Sha3_256;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error_span, info, warn};
use tracing_futures::Instrument;

const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);

#[cfg(not(debug_assertions))] // Release
const TEST_DURATION_DIVIDER: usize = 10;
#[cfg(debug_assertions)] // Debug
const TEST_DURATION_DIVIDER: usize = 1;

const SEED_UTXO: &[(i32, &str)] = &[(1, "000000"), (3, "000001"), (1, "000002")];
const VALID_TXS_IN: &[(i32, &str)] = &[(0, "000000"), (0, "000001"), (1, "000001")];
const VALID_TXS_OUT: &[&str] = &["000101", "000102", "000103"];
const DEFAULT_SEED_AMOUNT: TokenAmount = TokenAmount(3);

const BLOCK_RECEIVED: &str = "Block received to be added";
const BLOCK_STORED: &str = "Block complete stored";
const BLOCK_RECEIVED_AND_STORED: [&str; 2] = [BLOCK_RECEIVED, BLOCK_STORED];

const COMMON_PUB_KEY: &str = "5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";
const COMMON_SEC_KEY: &str = "0186bc08f16428d2059227082b93e439ff50f8c162f24b9594b132f2cc15fca45371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c";

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Cfg {
    All,
    IgnoreStorage,
    IgnoreCompute,
    IgnoreMiner,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CfgNum {
    All,
    Majority,
}

#[derive(Clone, Debug)]
enum CfgModif {
    Drop(&'static str),
    Respawn(&'static str),
    HandleEvents(&'static str, &'static [&'static str]),
    Disconnect(&'static str),
    Reconnect(&'static str),
}

#[test]
fn will_get_sanctioned_addresses() {
    let addresses = get_sanction_addresses(SANC_LIST_TEST.to_string(), &"US".to_string());
    assert!(addresses.contains(&"gjlkhflgkhdfklg".to_string()));

    let no_ju_addresses = get_sanction_addresses(SANC_LIST_TEST.to_string(), &"UK".to_string());
    assert_eq!(no_ju_addresses, Vec::<String>::new());

    let no_fs_addresses = get_sanction_addresses("/blah.json".to_string(), &"UK".to_string());
    assert_eq!(no_fs_addresses, Vec::<String>::new());
}

#[tokio::test(basic_scheduler)]
async fn full_flow_no_raft() {
    full_flow(complete_network_config(10500)).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_real_db_1_node() {
    let mut cfg = complete_network_config_with_n_compute_raft(10505, 1);
    cfg.in_memory_db = false;

    remove_all_node_dbs(&cfg);
    full_flow(cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_1_node() {
    full_flow(complete_network_config_with_n_compute_raft(10510, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_2_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10520, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_3_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10530, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_majority_3_nodes() {
    full_flow_common(
        complete_network_config_with_n_compute_raft(10540, 3),
        CfgNum::Majority,
        Vec::new(),
    )
    .await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_20_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10550, 20)).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_multi_miners_no_raft() {
    full_flow_multi_miners(complete_network_config_with_n_compute_miner(
        11000, false, 1, 3,
    ))
    .await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_multi_miners_raft_1_node() {
    full_flow_multi_miners(complete_network_config_with_n_compute_miner(
        11010, true, 1, 3,
    ))
    .await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_multi_miners_raft_2_nodes() {
    full_flow_multi_miners(complete_network_config_with_n_compute_miner(
        11020, true, 2, 6,
    ))
    .await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_kill_miner_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Drop("miner2")),
        ("After create block 1", CfgModif::Respawn("miner2")),
    ];
    let network_config = complete_network_config_with_n_compute_miner(11120, true, 3, 3);
    full_flow_common(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_kill_compute_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Drop("compute2")),
        ("After create block 1", CfgModif::Respawn("compute2")),
        (
            "After create block 1",
            CfgModif::HandleEvents(
                "compute2",
                &[
                    "Snapshot applied",
                    "Transactions committed",
                    "Block committed",
                ],
            ),
        ),
    ];

    let network_config = complete_network_config_with_n_compute_miner(11140, true, 3, 3);
    full_flow_common(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_kill_storage_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Drop("storage2")),
        ("After create block 1", CfgModif::Respawn("storage2")),
        (
            "After create block 1",
            CfgModif::HandleEvents("storage2", &[BLOCK_STORED]),
        ),
    ];

    let network_config = complete_network_config_with_n_compute_miner(11160, true, 3, 3);
    full_flow_common(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_dis_and_re_connect_miner_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Disconnect("miner2")),
        ("After create block 1", CfgModif::Reconnect("miner2")),
    ];
    let network_config = complete_network_config_with_n_compute_miner(11180, true, 3, 3);
    full_flow_common(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_dis_and_re_connect_compute_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Disconnect("compute2")),
        ("After create block 1", CfgModif::Reconnect("compute2")),
        (
            "After create block 1",
            CfgModif::HandleEvents("compute2", &["Transactions committed", "Block committed"]),
        ),
    ];

    let network_config = complete_network_config_with_n_compute_miner(11200, true, 3, 3);
    full_flow_common(network_config, CfgNum::All, modify_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_dis_and_re_connect_storage_node_3_nodes() {
    let modify_cfg = vec![
        ("After create block 0", CfgModif::Disconnect("storage2")),
        ("After create block 1", CfgModif::Reconnect("storage2")),
        (
            "After create block 1",
            CfgModif::HandleEvents("storage2", &[BLOCK_STORED]),
        ),
    ];

    let network_config = complete_network_config_with_n_compute_miner(11220, true, 3, 3);
    full_flow_common(network_config, CfgNum::All, modify_cfg).await;
}

async fn full_flow_multi_miners(mut network_config: NetworkConfig) {
    network_config.compute_partition_full_size = 2;
    network_config.compute_minimum_miner_pool_len = 3;
    full_flow(network_config).await;
}

async fn full_flow(network_config: NetworkConfig) {
    full_flow_common(network_config, CfgNum::All, Vec::new()).await;
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
    let compute_nodes = &network_config.nodes[&NodeType::Compute];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let miner_nodes = &network_config.nodes[&NodeType::Miner];
    let initial_utxo_txs = network.collect_initial_uxto_txs();
    let transactions = valid_transactions(true);

    //
    // Act
    //
    create_first_block_act(&mut network).await;
    modify_network(&mut network, "After create block 0", &modify_cfg).await;
    proof_of_work_act(&mut network, Cfg::All, cfg_num).await;
    send_block_to_storage_act(&mut network, cfg_num).await;
    let stored0 = storage_get_last_block_stored(&mut network, "storage1").await;

    add_transactions_act(&mut network, &transactions).await;
    create_block_act(&mut network, Cfg::All, cfg_num).await;
    modify_network(&mut network, "After create block 1", &modify_cfg).await;
    proof_winner_act(&mut network).await;
    proof_of_work_act(&mut network, Cfg::All, cfg_num).await;
    send_block_to_storage_act(&mut network, cfg_num).await;
    let stored1 = storage_get_last_block_stored(&mut network, "storage1").await;

    //
    // Assert
    //
    let actual1 = storage_all_get_last_stored_info(&mut network, storage_nodes).await;
    assert_eq!(equal_first(&actual1), node_all(storage_nodes, true));

    let actual0_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    let expected_block0_db_count =
        1 + initial_utxo_txs.len() + stored0.as_ref().unwrap().mining_transactions.len();
    let expected_block1_db_count =
        1 + transactions.len() + stored1.as_ref().unwrap().mining_transactions.len();

    assert_eq!(
        actual0_db_count,
        node_all(
            storage_nodes,
            expected_block0_db_count + expected_block1_db_count
        )
    );

    let actual_w0 = node_all_combined_get_wallet_info(&mut network, miner_nodes).await;
    let expected_w0 = if miner_nodes.len() < compute_nodes.len() {
        (TokenAmount(0), vec![])
    } else {
        let mining_txs = &stored0.as_ref().unwrap().mining_transactions;
        let total = TokenAmount(916770 * mining_txs.len() as u64);
        let mining_tx_out = get_tx_with_out_point(mining_txs.iter());

        (total, mining_tx_out.map(|(k, _)| k).collect::<Vec<_>>())
    };
    assert_eq!(
        (actual_w0.0, actual_w0.2.keys().cloned().collect::<Vec<_>>()),
        expected_w0
    );

    test_step_complete(network).await;
}

async fn modify_network(network: &mut Network, tag: &str, modif_config: &[(&str, CfgModif)]) {
    for (_tag, modif) in modif_config.iter().filter(|(t, _)| tag == *t) {
        match modif {
            CfgModif::Drop(v) => network.close_raft_loops_and_drop_named(&[v]).await,
            CfgModif::Respawn(v) => network.re_spawn_nodes_named(&[v]).await,
            CfgModif::HandleEvents(n, es) => {
                let computes = network.active_nodes(NodeType::Compute);
                let storage = network.active_nodes(NodeType::Storage);
                let node_group: Vec<String> = computes.iter().chain(storage).cloned().collect();
                let raisons: Vec<String> = es.iter().map(|e| e.to_string()).collect();
                let all_raisons = Some((n.to_string(), raisons)).into_iter().collect();
                node_all_handle_different_event(network, &node_group, &all_raisons).await
            }
            CfgModif::Disconnect(v) => network.disconnect_nodes_named(&[v]).await,
            CfgModif::Reconnect(v) => network.re_connect_nodes_named(&[v]).await,
        }
    }
}

#[tokio::test(basic_scheduler)]
async fn create_first_block_no_raft() {
    create_first_block(complete_network_config(10000)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_first_block_raft_1_node() {
    create_first_block(complete_network_config_with_n_compute_raft(10010, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_first_block_raft_2_nodes() {
    create_first_block(complete_network_config_with_n_compute_raft(10020, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_first_block_raft_3_nodes() {
    create_first_block(complete_network_config_with_n_compute_raft(10030, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_first_block_raft_20_nodes() {
    create_first_block(complete_network_config_with_n_compute_raft(10040, 20)).await;
}

async fn create_first_block(network_config: NetworkConfig) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let compute_nodes = &network_config.nodes[&NodeType::Compute];
    let expected_utxo = to_utxo_set(&network.collect_initial_uxto_txs());

    //
    // Act
    //
    create_first_block_act(&mut network).await;

    //
    // Assert
    //
    let utxo_set_after = compute_all_committed_utxo_set(&mut network, compute_nodes).await;
    assert_eq!(utxo_set_after, node_all(compute_nodes, expected_utxo));

    test_step_complete(network).await;
}

async fn create_first_block_act(network: &mut Network) {
    let config = network.config().clone();
    let active_nodes = network.all_active_nodes().clone();
    let compute_nodes = &active_nodes[&NodeType::Compute];
    let first_request_size = config.compute_minimum_miner_pool_len;

    info!("Test Step Connect nodes");
    for compute in compute_nodes {
        let miners = &config.compute_to_miner_mapping[compute];
        for (idx, miner) in miners.iter().enumerate() {
            miner_send_partition_request(network, miner, compute).await;
            let evt = if idx == first_request_size - 1 {
                "Received first full partition request"
            } else {
                "Received partition request successfully"
            };
            compute_handle_event(network, compute, evt).await;
        }
    }

    info!("Test Step Create first Block");
    compute_all_propose_initial_uxto_set(network, compute_nodes).await;
    node_all_handle_event(network, compute_nodes, &["First Block committed"]).await;
}

#[tokio::test(basic_scheduler)]
async fn send_first_block_to_storage_no_raft() {
    send_first_block_to_storage(complete_network_config(10800)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_first_block_to_storage_raft_1_node() {
    send_first_block_to_storage(complete_network_config_with_n_compute_raft(10810, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_first_block_to_storage_raft_2_nodes() {
    send_first_block_to_storage(complete_network_config_with_n_compute_raft(10820, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_first_block_to_storage_raft_3_nodes() {
    send_first_block_to_storage(complete_network_config_with_n_compute_raft(10830, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_first_block_to_storage_raft_majority_3_nodes() {
    send_first_block_to_storage_common(
        complete_network_config_with_n_compute_raft(10840, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(basic_scheduler)]
async fn send_first_block_to_storage_raft_20_nodes() {
    send_first_block_to_storage(complete_network_config_with_n_compute_raft(10850, 20)).await;
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
    let compute_nodes = &network_config.nodes[&NodeType::Compute];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let initial_utxo_txs = network.collect_initial_uxto_txs();
    let c_mined = &node_select(compute_nodes, cfg_num);
    let (expected0, block_info0) = complete_first_block(&initial_utxo_txs, c_mined.len()).await;

    create_first_block_act(&mut network).await;
    compute_all_mining_block_mined(&mut network, "miner1", c_mined, &block_info0).await;

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
            Some((
                expected0.0,
                0,             /*b_num*/
                c_mined.len(), /*mining txs*/
            ))
        )
    );
    assert_eq!(equal_first(&actual0), node_all(storage_nodes, true));

    let actual0_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;
    assert_eq!(
        actual0_db_count,
        node_all(storage_nodes, 1 + initial_utxo_txs.len() + c_mined.len())
    );

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
async fn add_transactions_no_raft() {
    add_transactions(complete_network_config(10600)).await;
}

#[tokio::test(basic_scheduler)]
async fn add_transactions_raft_1_node() {
    add_transactions(complete_network_config_with_n_compute_raft(10610, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn add_transactions_raft_2_nodes() {
    add_transactions(complete_network_config_with_n_compute_raft(10620, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn add_transactions_raft_3_nodes() {
    add_transactions(complete_network_config_with_n_compute_raft(10630, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn add_transactions_raft_20_nodes() {
    add_transactions(complete_network_config_with_n_compute_raft(10640, 20)).await;
}

async fn add_transactions(network_config: NetworkConfig) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let compute_nodes = &network_config.nodes[&NodeType::Compute];
    let transactions = valid_transactions(true);

    create_first_block_act(&mut network).await;

    //
    // Act
    //
    add_transactions_act(&mut network, &transactions).await;

    //
    // Assert
    //
    let actual = compute_all_committed_tx_pool(&mut network, compute_nodes).await;
    assert_eq!(actual[0], transactions);
    assert_eq!(equal_first(&actual), node_all(compute_nodes, true));

    test_step_complete(network).await;
}

async fn add_transactions_act(network: &mut Network, txs: &BTreeMap<String, Transaction>) {
    let active_nodes = network.all_active_nodes().clone();
    let compute_nodes = &active_nodes[&NodeType::Compute];

    info!("Test Step Add Transactions");
    for tx in txs.values() {
        user_send_transaction_to_compute(network, "user1", "compute1", tx).await;
    }
    for _tx in txs.values() {
        compute_handle_event(network, "compute1", "Transactions added to tx pool").await;
    }
    node_all_handle_event(network, compute_nodes, &["Transactions committed"]).await;
}

#[tokio::test(basic_scheduler)]
async fn create_block_no_raft() {
    create_block(complete_network_config(10100)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_block_raft_1_node() {
    create_block(complete_network_config_with_n_compute_raft(10110, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_block_raft_2_nodes() {
    create_block(complete_network_config_with_n_compute_raft(10120, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_block_raft_3_nodes() {
    create_block(complete_network_config_with_n_compute_raft(10130, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn create_block_raft_majority_3_nodes() {
    create_block_common(
        complete_network_config_with_n_compute_raft(10140, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(basic_scheduler)]
async fn create_block_raft_20_nodes() {
    create_block(complete_network_config_with_n_compute_raft(10150, 20)).await;
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
    let compute_nodes = &network_config.nodes[&NodeType::Compute];
    let transactions = valid_transactions(true);
    let transactions_h = transactions.keys().cloned().collect::<Vec<_>>();
    let transactions_utxo = to_utxo_set(&transactions);
    let (_, block_info0) = complete_first_block(&BTreeMap::new(), compute_nodes.len()).await;
    let block0_mining_tx = complete_block_mining_txs(&block_info0);
    let block0_mining_utxo = to_utxo_set(&block0_mining_tx);

    let mut left_init_utxo = to_utxo_set(&network.collect_initial_uxto_txs());
    remove_keys(&mut left_init_utxo, valid_txs_in().keys());

    create_first_block_act(&mut network).await;
    compute_all_mining_block_mined(&mut network, "miner1", compute_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    add_transactions_act(&mut network, &transactions).await;

    //
    // Act
    //
    let block_transaction_before =
        compute_all_current_block_transactions(&mut network, compute_nodes).await;

    create_block_act(&mut network, Cfg::All, cfg_num).await;

    let block_transaction_after =
        compute_all_current_block_transactions(&mut network, compute_nodes).await;
    let utxo_set_after = compute_all_committed_utxo_set(&mut network, compute_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_transaction_before, node_all(compute_nodes, None));
    assert_eq!(
        block_transaction_after,
        node_all(compute_nodes, Some(transactions_h))
    );

    let expected_utxo = merge_txs_3(&left_init_utxo, &transactions_utxo, &block0_mining_utxo);
    assert_eq!(len_and_map(&utxo_set_after[0]), len_and_map(&expected_utxo));
    assert_eq!(equal_first(&utxo_set_after), node_all(compute_nodes, true));

    test_step_complete(network).await;
}

async fn create_block_act(network: &mut Network, cfg: Cfg, cfg_num: CfgNum) {
    create_block_act_with(network, cfg, cfg_num, 0).await
}

async fn create_block_act_with(network: &mut Network, cfg: Cfg, cfg_num: CfgNum, block_num: u64) {
    let active_nodes = network.all_active_nodes().clone();
    let compute_nodes = &active_nodes[&NodeType::Compute];
    let (msg_c_nodes, msg_s_nodes) = node_combined_select(
        &network.config().nodes[&NodeType::Compute],
        &network.config().nodes[&NodeType::Storage],
        &network.dead_nodes(),
        cfg_num,
    );

    info!("Test Step Storage signal new block");

    if cfg == Cfg::IgnoreStorage {
        let block = BlockStoredInfo {
            block_num,
            ..Default::default()
        };
        let req = ComputeRequest::SendBlockStored(block);
        compute_all_inject_next_event(network, &msg_s_nodes, &msg_c_nodes, req).await;
    } else {
        storage_all_send_stored_block(network, &msg_s_nodes).await;
    }
    compute_all_handle_event(network, &msg_c_nodes, "Received block stored").await;

    info!("Test Step Generate Block");
    node_all_handle_event(network, compute_nodes, &["Block committed"]).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_no_raft() {
    proof_of_work(complete_network_config(10200)).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_raft_1_node() {
    proof_of_work(complete_network_config_with_n_compute_raft(10210, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_raft_2_nodes() {
    proof_of_work(complete_network_config_with_n_compute_raft(10220, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_raft_3_nodes() {
    proof_of_work(complete_network_config_with_n_compute_raft(10230, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_raft_majority_3_nodes() {
    proof_of_work_common(
        complete_network_config_with_n_compute_raft(10240, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_multi_no_raft() {
    let mut cfg = complete_network_config_with_n_compute_miner(10250, false, 1, 3);
    cfg.compute_partition_full_size = 2;
    cfg.compute_minimum_miner_pool_len = 3;
    proof_of_work(cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_multi_raft_1_node() {
    let mut cfg = complete_network_config_with_n_compute_miner(10260, true, 1, 3);
    cfg.compute_partition_full_size = 2;
    cfg.compute_minimum_miner_pool_len = 3;
    proof_of_work(cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_multi_raft_2_nodes() {
    let mut cfg = complete_network_config_with_n_compute_miner(10270, true, 2, 6);
    cfg.compute_partition_full_size = 2;
    cfg.compute_minimum_miner_pool_len = 3;
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
    let compute_nodes = &network_config.nodes[&NodeType::Compute];

    create_first_block_act(&mut network).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;

    //
    // Act
    //
    let block_before = compute_all_mined_block_num(&mut network, compute_nodes).await;

    proof_of_work_act(&mut network, Cfg::All, cfg_num).await;
    proof_of_work_send_more_act(&mut network, cfg_num).await;

    let block_after = compute_all_mined_block_num(&mut network, compute_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_before, node_all(compute_nodes, None));
    assert_eq!(
        block_after,
        node_all_or(compute_nodes, cfg_num, Some(1), None)
    );

    test_step_complete(network).await;
}

async fn proof_of_work_act(network: &mut Network, cfg: Cfg, cfg_num: CfgNum) {
    let config = network.config().clone();
    let active_nodes = network.all_active_nodes().clone();
    let compute_nodes = &active_nodes[&NodeType::Compute];
    let partition_size = config.compute_partition_full_size;
    let c_mined = &node_select(compute_nodes, cfg_num);
    let active_compute_to_miner_mapping = network.active_compute_to_miner_mapping().clone();

    info!("Test Step Miner block Proof of Work: partition-> rand num -> num pow -> pre-block -> block pow");
    if cfg == Cfg::IgnoreMiner {
        let (_, block_info) = complete_first_block(&BTreeMap::new(), c_mined.len()).await;
        compute_all_mining_block_mined(network, "miner1", c_mined, &block_info).await;
        return;
    }

    for compute in c_mined {
        let partition_last_idx = partition_size - 1;
        let c_miners = &active_compute_to_miner_mapping.get(compute).unwrap();
        let in_miners: &[String] = &c_miners[0..std::cmp::min(partition_size, c_miners.len())];

        compute_flood_rand_num_to_requesters(network, compute).await;
        miner_all_handle_event(network, c_miners, "Received random number successfully").await;

        for (idx, miner) in c_miners.iter().enumerate() {
            miner_send_partition_pow(network, miner, compute).await;

            use std::cmp::Ordering::*;
            let (success, evt) = match idx.cmp(&partition_last_idx) {
                Less => (true, "Partition PoW received successfully"),
                Equal => (true, "Partition list is full"),
                Greater => (false, "Partition list is already full"),
            };

            if success {
                compute_handle_event(network, compute, evt).await;
            } else {
                compute_handle_error(network, compute, evt).await;
            }
        }

        compute_flood_block_to_partition(network, compute).await;
        miner_all_handle_event(network, in_miners, "Pre-block received successfully").await;

        if !c_miners.is_empty() {
            let win_miner: &String = &in_miners[0];
            miner_send_pow_for_current(network, win_miner, compute).await;
            compute_handle_event(network, compute, "Received PoW successfully").await;
        }
    }
}

async fn proof_of_work_send_more_act(network: &mut Network, cfg_num: CfgNum) {
    let config = network.config().clone();
    let active_nodes = network.all_active_nodes().clone();
    let compute_nodes = &active_nodes[&NodeType::Compute];
    let c_mined = &node_select(compute_nodes, cfg_num);

    info!("Test Step Miner block Proof of Work to late");
    for compute in c_mined {
        let c_miners = config.compute_to_miner_mapping.get(compute).unwrap();
        for miner in c_miners {
            miner_send_pow_for_current(network, miner, compute).await;
            compute_handle_error(network, compute, "Not block currently mined").await;
        }
    }
}

#[tokio::test(basic_scheduler)]
async fn proof_winner_no_raft() {
    proof_winner(complete_network_config(10900)).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_winner_raft_1_node() {
    proof_winner(complete_network_config_with_n_compute_raft(10910, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_winner_multi_no_raft() {
    proof_winner_multi(complete_network_config_with_n_compute_miner(
        10920, false, 1, 3,
    ))
    .await;
}

#[tokio::test(basic_scheduler)]
async fn proof_winner_multi_raft_1_node() {
    proof_winner_multi(complete_network_config_with_n_compute_miner(
        10930, true, 1, 3,
    ))
    .await;
}

#[tokio::test(basic_scheduler)]
async fn proof_winner_multi_raft_2_nodes() {
    proof_winner_multi(complete_network_config_with_n_compute_miner(
        10940, true, 2, 6,
    ))
    .await;
}

async fn proof_winner_multi(mut network_config: NetworkConfig) {
    network_config.compute_partition_full_size = 2;
    network_config.compute_minimum_miner_pool_len = 3;
    proof_winner(network_config).await;
}

async fn proof_winner(network_config: NetworkConfig) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let winning_miners: Vec<String> = network_config
        .compute_to_miner_mapping
        .values()
        .map(|ms| ms.first().unwrap().clone())
        .collect();

    create_first_block_act(&mut network).await;

    //
    // Act
    // Does not allow miner reuse.
    //
    proof_of_work_act(&mut network, Cfg::All, CfgNum::All).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    create_block_act(&mut network, Cfg::All, CfgNum::All).await;

    let info_before = node_all_get_wallet_info(&mut network, &winning_miners).await;
    proof_winner_act(&mut network).await;
    let info_after = node_all_get_wallet_info(&mut network, &winning_miners).await;

    //
    // Assert
    //

    assert_eq!(
        info_before
            .iter()
            .map(|i| (&i.0, i.1.len(), i.2.len()))
            .collect::<Vec<_>>(),
        node_all(&winning_miners, (&TokenAmount(0), 1, 0)),
        "Info Before: {:?}",
        info_before
    );
    assert_eq!(
        info_after
            .iter()
            .map(|i| (&i.0, i.1.len(), i.2.len()))
            .collect::<Vec<_>>(),
        node_all(&winning_miners, (&TokenAmount(916770), 1, 1)),
        "Info After: {:?}",
        info_after
    );

    test_step_complete(network).await;
}

async fn proof_winner_act(network: &mut Network) {
    let config = network.config().clone();
    let active_nodes = network.all_active_nodes().clone();
    let compute_nodes = &active_nodes[&NodeType::Compute];
    let miner_nodes = &active_nodes[&NodeType::Miner];

    if miner_nodes.len() < compute_nodes.len() {
        info!("Test Step Miner winner: Ignored/Miner re-use");
        return;
    }

    info!("Test Step Miner winner:");
    for compute in compute_nodes {
        let c_miners = &config.compute_to_miner_mapping.get(compute).unwrap();
        let win_miner: &String = &c_miners[0];

        if compute_send_bf_found(network, compute).await {
            miner_handle_event(network, win_miner, "Block found").await;
            miner_commit_block_found(network, win_miner).await;
        }
    }
}

#[tokio::test(basic_scheduler)]
async fn send_block_to_storage_no_raft() {
    send_block_to_storage(complete_network_config(10300)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_block_to_storage_raft_1_node() {
    send_block_to_storage(complete_network_config_with_n_compute_raft(10310, 1)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_block_to_storage_raft_2_nodes() {
    send_block_to_storage(complete_network_config_with_n_compute_raft(10320, 2)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_block_to_storage_raft_3_nodes() {
    send_block_to_storage(complete_network_config_with_n_compute_raft(10330, 3)).await;
}

#[tokio::test(basic_scheduler)]
async fn send_block_to_storage_raft_majority_3_nodes() {
    send_block_to_storage_common(
        complete_network_config_with_n_compute_raft(10340, 3),
        CfgNum::Majority,
    )
    .await;
}

#[tokio::test(basic_scheduler)]
async fn send_block_to_storage_raft_20_nodes() {
    send_block_to_storage(complete_network_config_with_n_compute_raft(10350, 20)).await;
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
    let compute_nodes = &network_config.nodes[&NodeType::Compute];
    let storage_nodes = &network_config.nodes[&NodeType::Storage];
    let c_mined = &node_select(compute_nodes, cfg_num);

    let transactions = valid_transactions(true);
    let (expected1, block_info1) = complete_block(1, Some("0"), &transactions, c_mined.len()).await;
    let (_expected3, wrong_block3) = complete_block(3, Some("0"), &BTreeMap::new(), 1).await;
    let block1_mining_tx = complete_block_mining_txs(&block_info1);

    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, Cfg::IgnoreMiner, CfgNum::All).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    compute_all_set_mining_block(&mut network, c_mined, &block_info1).await;
    compute_all_mining_block_mined(&mut network, "miner1", c_mined, &block_info1).await;

    let initial_db_count =
        storage_all_get_stored_key_values_count(&mut network, storage_nodes).await;

    //
    // Act
    //
    storage_inject_send_block_to_storage(&mut network, "compute1", "storage1", &wrong_block3).await;
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
            Some((
                expected1.0,
                1,             /*b_num*/
                c_mined.len(), /*mining txs*/
            ))
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

    let compute_no_mining = network
        .active_compute_to_miner_mapping()
        .iter()
        .filter(|(_, miners)| miners.is_empty())
        .map(|(c, _)| c.clone());
    let mut dead_nodes = network.dead_nodes().clone();
    dead_nodes.extend(compute_no_mining);

    let (msg_c_nodes, msg_s_nodes) = node_combined_select(
        &network.config().nodes[&NodeType::Compute],
        &network.config().nodes[&NodeType::Storage],
        &dead_nodes,
        cfg_num,
    );

    info!("Test Step Compute Send block to Storage");
    compute_all_send_block_to_storage(network, &msg_c_nodes).await;
    storage_all_handle_event(network, &msg_s_nodes, BLOCK_RECEIVED).await;
    node_all_handle_event(network, storage_nodes, &[BLOCK_STORED]).await;
}

#[tokio::test(basic_scheduler)]
async fn receive_payment_tx_user() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(10400);
    network_config
        .nodes_mut(NodeType::User)
        .push("user2".to_string());
    network_config.compute_seed_utxo = make_compute_seed_utxo(SEED_UTXO, TokenAmount(11));
    network_config.user_wallet_seeds = vec![vec![wallet_seed(VALID_TXS_IN[0], &TokenAmount(11))]];
    let mut network = Network::create_from_config(&network_config).await;
    let user_nodes = &network_config.nodes[&NodeType::User];
    let amount = TokenAmount(5);

    create_first_block_act(&mut network).await;

    //
    // Act/Assert
    //
    let before = node_all_get_wallet_info(&mut network, user_nodes).await;

    node_connect_to(&mut network, "user1", "user2").await;

    user_send_address_request(&mut network, "user1", "user2", amount).await;
    user_handle_event(&mut network, "user2", "New address ready to be sent").await;

    user_send_address_to_trading_peer(&mut network, "user2").await;
    user_handle_event(&mut network, "user1", "Next payment transaction ready").await;

    user_send_next_payment_to_destinations(&mut network, "user1", "compute1").await;
    compute_handle_event(&mut network, "compute1", "Transactions added to tx pool").await;
    user_handle_event(&mut network, "user2", "Payment transaction received").await;

    let after = node_all_get_wallet_info(&mut network, user_nodes).await;

    //
    // Assert
    //
    assert_eq!(
        before
            .iter()
            .map(|(total, _, _)| *total)
            .collect::<Vec<_>>(),
        vec![TokenAmount(11), TokenAmount(0)]
    );
    assert_eq!(
        after.iter().map(|(total, _, _)| *total).collect::<Vec<_>>(),
        vec![TokenAmount(6), TokenAmount(5)]
    );

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
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
    let compute_nodes = &network_config.nodes[&NodeType::Compute];

    let valid_txs = valid_transactions(true);
    let invalid_txs = vec![
        // New keys not matching utxo_set
        valid_transactions(false),
        // Too much output amount for given inputs
        valid_transactions_with(true, DEFAULT_SEED_AMOUNT + TokenAmount(1)),
        // Too little output amount for given inputs
        valid_transactions_with(true, DEFAULT_SEED_AMOUNT - TokenAmount(1)),
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
        user_send_transaction_to_compute(&mut network, "user2", "compute1", tx).await;
    }
    for _tx in invalid_txs.iter().flat_map(|txs| txs.values()) {
        compute_handle_error(&mut network, "compute1", "No valid transactions provided").await;
    }
    add_transactions_act(&mut network, &valid_txs).await;

    //
    // Assert
    //
    let actual = compute_all_committed_tx_pool(&mut network, compute_nodes).await;
    assert_eq!(actual[0], valid_txs);
    assert_eq!(equal_first(&actual), node_all(compute_nodes, true));

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
async fn gen_transactions() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config(10420);
    let mut network = Network::create_from_config(&network_config).await;
    let mut transaction_gen =
        TransactionGen::new(vec![wallet_seed(VALID_TXS_IN[0], &DEFAULT_SEED_AMOUNT)]);

    //
    // Act
    //
    user_send_block_notification_request(&mut network, "user1").await;
    compute_handle_event(&mut network, "compute1", "Received block notification").await;

    let mut tx_expected = Vec::new();
    let mut tx_committed = Vec::new();
    for b_num in 0..3 {
        if b_num == 0 {
            create_first_block_act(&mut network).await;
        } else {
            create_block_act_with(&mut network, Cfg::IgnoreStorage, CfgNum::All, b_num - 1).await;
        }

        let transactions = {
            compute_flood_block_to_users(&mut network, "compute1").await;
            user_handle_event(&mut network, "user1", "Block mining notified").await;
            let committed = user_last_block_notified_txs(&mut network, "user1").await;

            transaction_gen.commit_transactions(&committed);
            let transactions = transaction_gen.make_all_transactions(Some(2), 4);
            transactions.into_iter().collect()
        };

        add_transactions_act(&mut network, &transactions).await;
        let committed = compute_committed_tx_pool(&mut network, "compute1").await;

        tx_expected.push(transactions);
        tx_committed.push(committed);
    }

    //
    // Assert
    //
    assert_eq!(tx_committed, tx_expected);

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
async fn proof_of_work_reject() {
    test_step_start();

    //
    // Arrange
    //
    let network_config = complete_network_config(10430);
    let mut network = Network::create_from_config(&network_config).await;

    let compute = "compute1";
    let miner = "miner1";
    let block_num = 1;
    create_first_block_act(&mut network).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;
    compute_flood_rand_num_to_requesters(&mut network, compute).await;
    miner_handle_event(&mut network, miner, "Received random number successfully").await;
    miner_send_partition_pow(&mut network, miner, compute).await;
    compute_handle_event(&mut network, compute, "Partition list is full").await;

    //
    // Act
    //
    {
        // From node not in partition (user1 is not a miner).
        let request = ComputeRequest::SendPoW {
            block_num,
            nonce: Default::default(),
            coinbase: Default::default(),
        };
        compute_inject_next_event(&mut network, "user1", compute, request).await;
        compute_handle_error(&mut network, compute, "Not block currently mined").await;
    }
    {
        // For not currently mined block number.
        let request = ComputeRequest::SendPoW {
            block_num: block_num + 1,
            nonce: Default::default(),
            coinbase: Default::default(),
        };
        compute_inject_next_event(&mut network, miner, compute, request).await;
        compute_handle_error(&mut network, compute, "Not block currently mined").await;
    }
    {
        // For miner in partition and correct block, but wrong nonce/coinbase
        let request = ComputeRequest::SendPoW {
            block_num,
            nonce: Default::default(),
            coinbase: Default::default(),
        };
        compute_inject_next_event(&mut network, miner, compute, request).await;
        compute_handle_error(&mut network, compute, "Coinbase transaction invalid").await;
    }

    //
    // Assert
    //
    let block = compute_current_mining_block(&mut network, compute).await;
    assert!(block.is_some(), "Expect still mining");

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
async fn main_loops_few_txs_raft_1_node() {
    let network_config = complete_network_config_with_n_compute_raft(10440, 1);
    main_loops_raft_1_node_common(network_config, TokenAmount(17), 20, 1, 1_000).await
}

// Slow: Only run when explicitely specified for performance and large tests
// `RUST_LOG="info,raft=warn" cargo test --lib --release -- --ignored --nocapture main_loops_many_txs_threaded_raft_1_node`
#[tokio::test(threaded_scheduler)]
#[ignore]
async fn main_loops_many_txs_threaded_raft_1_node() {
    let mut network_config = complete_network_config_with_n_compute_raft(10450, 1);
    network_config.test_duration_divider = 1;

    // 5_000 TxIn in transactions of 3 TxIn.
    // 10_000 TxIn available so we can create full transaction batch even if previous block did
    // not contain all transactions already created.
    main_loops_raft_1_node_common(network_config, TokenAmount(1), 1_000_000, 10_000, 5_000 / 3)
        .await
}

async fn main_loops_raft_1_node_common(
    mut network_config: NetworkConfig,
    initial_amount: TokenAmount,
    seed_count: i32,
    seed_wallet_count: i32,
    tx_max_count: usize,
) {
    test_step_start();

    //
    // Arrange
    // initial_amount is split into TokenAmount(1) TxOuts for the next round.
    //
    network_config.compute_seed_utxo =
        make_compute_seed_utxo(&[(seed_count, "000000")], initial_amount);
    let mut network = Network::create_from_config(&network_config).await;

    let mut tx_generator = TransactionGen::new(
        (0..seed_wallet_count)
            .map(|i| wallet_seed((i, "000000"), &initial_amount))
            .collect(),
    );
    let setup = UserNodeSetup {
        user_setup_tx_chunk_size: Some(5),
        user_setup_tx_in_per_tx: Some(3),
        user_setup_tx_max_count: tx_max_count,
        ..Default::default()
    };
    let expected_blocks = 6;
    let expected_block_num = expected_blocks - 1;

    //
    // Act
    //
    user_send_block_notification_request(&mut network, "user1").await;

    let mut handles = Vec::new();
    handles.push(tokio::spawn({
        let node = network.compute("compute1").unwrap().clone();
        async move {
            let mut node = node.lock().await;
            let mut expected_blocks = expected_blocks;
            while let Some(response) = node.handle_next_event().await {
                if node.handle_next_event_response(response).await {
                    expected_blocks -= 1;
                    if expected_blocks == 0 {
                        break;
                    }
                }
            }
        }
    }));

    handles.push(tokio::spawn({
        let node = network.storage("storage1").unwrap().clone();
        async move {
            let mut node = node.lock().await;
            let mut expected_blocks = expected_blocks;
            while let Some(response) = node.handle_next_event().await {
                if node.handle_next_event_response(response).await {
                    expected_blocks -= 1;
                    if expected_blocks == 0 {
                        break;
                    }
                }
            }
        }
    }));

    handles.push(tokio::spawn({
        let now = SystemTime::now();
        let node = network.miner("miner1").unwrap().clone();
        async move {
            let mut node = node.lock().await;
            let mut expected_blocks = expected_blocks;

            let address = node.compute_address();
            node.send_partition_request(address).await.unwrap();

            while let Some(response) = node.handle_next_event().await {
                if node.handle_next_event_response(now, response).await {
                    expected_blocks -= 1;
                    if expected_blocks == 0 {
                        break;
                    }
                }
            }
        }
    }));

    handles.push(tokio::spawn({
        let node = network.user("user1").unwrap().clone();
        async move {
            let mut node = node.lock().await;
            let mut expected_blocks = expected_blocks;

            let address = node.compute_address();
            node.send_block_notification_request(address).await.unwrap();

            while let Some(response) = node.handle_next_event().await {
                if node
                    .handle_next_event_response(&setup, &mut tx_generator, response)
                    .await
                {
                    expected_blocks -= 1;
                    if expected_blocks == 0 {
                        break;
                    }
                }
            }
        }
    }));

    join_all(handles).await;

    //
    // Assert
    //
    let last_stored_b_num = {
        let block_stored = storage_get_last_block_stored(&mut network, "storage1").await;
        block_stored.map(|bs| bs.block_num)
    };
    assert_eq!(last_stored_b_num, Some(expected_block_num));

    test_step_complete(network).await;
}

//
// Node helpers
//

fn node_all<T: Clone>(nodes: &[String], value: T) -> Vec<T> {
    let len = nodes.len();
    (0..len).map(|_| value.clone()).collect()
}

fn node_all_or<T: Clone>(
    nodes: &[String],
    cfg_num: CfgNum,
    value: T,
    unselected_value: T,
) -> Vec<T> {
    let select_len = node_select_len(nodes, cfg_num);
    let len = nodes.len();

    let selected = (0..select_len).map(|_| value.clone());
    let unselected = (select_len..len).map(|_| unselected_value.clone());
    selected.chain(unselected).collect()
}

fn node_select(nodes: &[String], cfg_num: CfgNum) -> Vec<String> {
    let len = node_select_len(nodes, cfg_num);
    nodes.iter().cloned().take(len).collect()
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
    let mut join_handles = Vec::new();
    let barrier = Arc::new(Barrier::new(node_group.len()));
    for node_name in node_group {
        let barrier = barrier.clone();
        let reason_str = all_raisons.get(node_name).cloned().unwrap_or_default();
        let node_name = node_name.clone();
        let compute = network.compute(&node_name).cloned();
        let storage = network.storage(&node_name).cloned();

        let peer_span = error_span!("peer", ?node_name);
        join_handles.push(tokio::spawn(
            async move {
                if let Some(compute) = compute {
                    compute_one_handle_event(&compute, &barrier, &reason_str).await;
                } else if let Some(storage) = storage {
                    storage_one_handle_event(&storage, &barrier, &reason_str).await;
                } else {
                    panic!("Node not found");
                }
            }
            .instrument(peer_span),
        ));
    }

    let failed_join: Vec<_> = join_all(join_handles)
        .await
        .into_iter()
        .zip(node_group)
        .filter(|(r, _)| r.is_err())
        .map(|(_, name)| name)
        .collect();
    if !failed_join.is_empty() {
        panic!("Failed joined {:?}", failed_join);
    }
}

async fn node_get_wallet_info(
    network: &mut Network,
    node: &str,
) -> (
    TokenAmount,
    Vec<String>,
    BTreeMap<OutPoint, (String, TokenAmount)>,
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

    let addresses = wallet.get_known_address();

    let fund = wallet.get_fund_store();
    let total = fund.running_total();

    let mut txs_to_address_and_ammount = BTreeMap::new();
    for (tx, amount) in fund.into_transactions().into_iter() {
        let addr = wallet.get_transaction_address(&tx);
        txs_to_address_and_ammount.insert(tx, (addr, amount));
    }
    (total, addresses, txs_to_address_and_ammount)
}

async fn node_all_get_wallet_info(
    network: &mut Network,
    miner_group: &[String],
) -> Vec<(
    TokenAmount,
    Vec<String>,
    BTreeMap<OutPoint, (String, TokenAmount)>,
)> {
    let mut result = Vec::new();
    for name in miner_group {
        let r = node_get_wallet_info(network, name).await;
        result.push(r);
    }
    result
}

async fn node_all_combined_get_wallet_info(
    network: &mut Network,
    miner_group: &[String],
) -> (
    TokenAmount,
    Vec<String>,
    BTreeMap<OutPoint, (String, TokenAmount)>,
) {
    let mut total = TokenAmount(0);
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

//
// ComputeNode helpers
//

async fn compute_handle_event(network: &mut Network, compute: &str, reason_str: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    compute_handle_event_for_node(&mut c, true, reason_str).await;
}

async fn compute_all_handle_event(
    network: &mut Network,
    compute_group: &[String],
    reason_str: &str,
) {
    for compute in compute_group {
        compute_handle_event(network, compute, reason_str).await;
    }
}

async fn compute_handle_error(network: &mut Network, compute: &str, reason_str: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    compute_handle_event_for_node(&mut c, false, reason_str).await;
}

async fn compute_all_handle_error(
    network: &mut Network,
    compute_group: &[String],
    reason_str: &str,
) {
    for compute in compute_group {
        compute_handle_error(network, compute, reason_str).await;
    }
}

async fn compute_handle_event_for_node(c: &mut ComputeNode, success_val: bool, reason_val: &str) {
    match time::timeout(TIMEOUT_TEST_WAIT_DURATION, c.handle_next_event()).await {
        Ok(Some(Ok(Response { success, reason })))
            if success == success_val && reason == reason_val => {}
        other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
    }
}

async fn compute_one_handle_event(
    compute: &Arc<Mutex<ComputeNode>>,
    barrier: &Barrier,
    reason_str: &[String],
) {
    debug!("Start wait for event");

    let mut compute = compute.lock().await;
    for reason in reason_str {
        compute_handle_event_for_node(&mut compute, true, &reason).await;
    }

    debug!("Start wait for completion of other in raft group");
    let result = tokio::select!(
       _ = barrier.wait() => (),
       _ = compute_handle_event_for_node(&mut compute, true, "Not an event") => (),
    );

    debug!("Stop wait for event: {:?}", result);
}

async fn compute_set_mining_block(
    network: &mut Network,
    compute: &str,
    block_info: &CompleteBlock,
) {
    let mut c = network.compute(compute).unwrap().lock().await;
    let common = block_info.common.clone();
    c.set_committed_mining_block(common.block, common.block_txs);
}

async fn compute_all_set_mining_block(
    network: &mut Network,
    compute_group: &[String],
    block_info: &CompleteBlock,
) {
    for compute in compute_group {
        compute_set_mining_block(network, compute, block_info).await;
    }
}

async fn compute_mined_block_num(network: &mut Network, compute: &str) -> Option<u64> {
    let c = network.compute(compute).unwrap().lock().await;
    c.current_mined_block.as_ref().map(|b| b.block.header.b_num)
}

async fn compute_all_mined_block_num(
    network: &mut Network,
    compute_group: &[String],
) -> Vec<Option<u64>> {
    let mut result = Vec::new();
    for name in compute_group {
        let r = compute_mined_block_num(network, name).await;
        result.push(r);
    }
    result
}

async fn compute_all_current_block_transactions(
    network: &mut Network,
    compute_group: &[String],
) -> Vec<Option<Vec<String>>> {
    let mut result = Vec::new();
    for name in compute_group {
        let r = compute_current_block_transactions(network, name).await;
        result.push(r);
    }
    result
}

async fn compute_current_block_transactions(
    network: &mut Network,
    compute: &str,
) -> Option<Vec<String>> {
    compute_current_mining_block(network, compute)
        .await
        .map(|b| b.transactions)
}

async fn compute_current_mining_block(network: &mut Network, compute: &str) -> Option<Block> {
    let c = network.compute(compute).unwrap().lock().await;
    c.get_mining_block().clone()
}

async fn compute_all_committed_utxo_set(
    network: &mut Network,
    compute_group: &[String],
) -> Vec<UtxoSet> {
    let mut result = Vec::new();
    for name in compute_group {
        let r = compute_committed_utxo_set(network, name).await;
        result.push(r);
    }
    result
}

async fn compute_committed_utxo_set(network: &mut Network, compute: &str) -> UtxoSet {
    let c = network.compute(compute).unwrap().lock().await;
    c.get_committed_utxo_set().clone()
}

async fn compute_all_committed_tx_pool(
    network: &mut Network,
    compute_group: &[String],
) -> Vec<BTreeMap<String, Transaction>> {
    let mut result = Vec::new();
    for name in compute_group {
        let r = compute_committed_tx_pool(network, name).await;
        result.push(r);
    }
    result
}

async fn compute_committed_tx_pool(
    network: &mut Network,
    compute: &str,
) -> BTreeMap<String, Transaction> {
    let c = network.compute(compute).unwrap().lock().await;
    c.get_committed_tx_pool().clone()
}

async fn compute_all_inject_next_event(
    network: &mut Network,
    from_group: &[String],
    to_compute_group: &[String],
    request: ComputeRequest,
) {
    for (from, to) in from_group.iter().zip(to_compute_group.iter()) {
        compute_inject_next_event(network, from, to, request.clone()).await;
    }
}

async fn compute_inject_next_event(
    network: &mut Network,
    from: &str,
    to_compute: &str,
    request: ComputeRequest,
) {
    let from_addr = network.get_address(from).await.unwrap();
    let c = network.compute(to_compute).unwrap().lock().await;

    c.inject_next_event(from_addr, request).unwrap();
}

async fn compute_propose_initial_uxto_set(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.propose_initial_uxto_set().await;
}

async fn compute_all_propose_initial_uxto_set(network: &mut Network, compute_group: &[String]) {
    for compute in compute_group {
        compute_propose_initial_uxto_set(network, compute).await;
    }
}

async fn compute_flood_rand_num_to_requesters(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.flood_rand_num_to_requesters().await.unwrap();
}

async fn compute_flood_block_to_partition(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.flood_block_to_partition().await.unwrap();
}

async fn compute_flood_block_to_users(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.flood_block_to_users().await.unwrap();
}

async fn compute_send_block_to_storage(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    if let Err(e) = c.send_block_to_storage().await {
        warn!("Block not sent to storage {:?}", e);
    }
}

async fn compute_all_send_block_to_storage(network: &mut Network, compute_group: &[String]) {
    for compute in compute_group {
        compute_send_block_to_storage(network, compute).await;
    }
}

async fn compute_send_bf_found(network: &mut Network, compute: &str) -> bool {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.send_bf_notification().await.unwrap()
}

async fn compute_mining_block_mined(
    network: &mut Network,
    miner: &str,
    compute: &str,
    block_info: &CompleteBlock,
) {
    let miner_addr = network.get_address(miner).await.unwrap();
    let id = network.get_position(compute).unwrap() as u64 + 1;
    let mut c = network.compute(compute).unwrap().lock().await;
    let mined = block_info.per_node.get(&id).unwrap();

    c.mining_block_mined(mined.nonce.clone(), miner_addr, mined.mining_tx.clone());
}

async fn compute_all_mining_block_mined(
    network: &mut Network,
    miner: &str,
    compute_group: &[String],
    block_info: &CompleteBlock,
) {
    for compute in compute_group {
        compute_mining_block_mined(network, miner, compute, block_info).await;
    }
}

async fn compute_miner_request_list_is_empty(network: &mut Network, compute: &str) -> bool {
    let c = network.compute(compute).unwrap().lock().await;
    c.request_list.is_empty()
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

async fn storage_inject_send_block_to_storage(
    network: &mut Network,
    compute: &str,
    storage: &str,
    block_info: &CompleteBlock,
) {
    let id: u64;
    if network.get_position(compute) == None {
        id = 1;
    } else {
        id = network.get_position(compute).unwrap() as u64 + 1;
    }

    let mined = block_info.per_node.get(&id).unwrap();
    let block = block_info.common.block.clone();
    let block_txs = block_info.common.block_txs.clone();
    let nonce = mined.nonce.clone();
    let mining_tx = mined.mining_tx.clone();

    let request = StorageRequest::SendBlock {
        common: CommonBlockInfo { block, block_txs },
        mined_info: MinedBlockExtraInfo { nonce, mining_tx },
    };

    storage_inject_next_event(network, compute, storage, request).await;
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
    let stored_block = match s.get_stored_block(block_hash) {
        Err(e) => return Some(format!("error: {:?}", e)),
        Ok(None) => return None,
        Ok(Some(v)) => v,
    };

    let mut block_txs = BTreeMap::new();
    for tx_hash in &stored_block.block.transactions {
        let stored_tx = match s.get_stored_tx(tx_hash) {
            Err(e) => return Some(format!("error tx hash: {:?} : {:?}", e, tx_hash)),
            Ok(None) => return Some(format!("error tx not found: {:?}", tx_hash)),
            Ok(Some(v)) => v,
        };
        block_txs.insert(tx_hash.clone(), stored_tx);
    }

    let mut per_node = BTreeMap::new();
    for (idx, (tx_hash, nonce)) in &stored_block.mining_tx_hash_and_nonces {
        let stored_tx = match s.get_stored_tx(tx_hash) {
            Err(e) => return Some(format!("error mining tx hash: {:?} : {:?}", e, tx_hash)),
            Ok(None) => return Some(format!("error mining tx not found: {:?}", tx_hash)),
            Ok(Some(v)) => v,
        };
        per_node.insert(
            *idx,
            MinedBlockExtraInfo {
                nonce: nonce.clone(),
                mining_tx: (tx_hash.clone(), stored_tx),
            },
        );
    }

    let block = stored_block.block;
    let common = CommonBlockInfo { block, block_txs };
    let complete = CompleteBlock { common, per_node };
    Some(format!("{:?}", complete))
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
    storage_handle_event_for_node(&mut s, true, reason_str).await;
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

async fn storage_handle_event_for_node(s: &mut StorageNode, success_val: bool, reason_val: &str) {
    match time::timeout(TIMEOUT_TEST_WAIT_DURATION, s.handle_next_event()).await {
        Ok(Some(Ok(Response { success, reason })))
            if success == success_val && reason == reason_val => {}
        other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
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
        storage_handle_event_for_node(&mut storage, true, &reason).await;
    }

    debug!("Start wait for completion of other in raft group");
    let result = tokio::select!(
       _ = barrier.wait() => (),
       _ = storage_handle_event_for_node(&mut storage, true, "Not an event") => (),
    );

    debug!("Stop wait for event: {:?}", result);
}

//
// UserNode helpers
//

async fn user_handle_event(network: &mut Network, user: &str, reason_val: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    let success_val = true;

    match time::timeout(TIMEOUT_TEST_WAIT_DURATION, u.handle_next_event()).await {
        Ok(Some(Ok(Response { success, reason })))
            if success == success_val && reason == reason_val => {}
        other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
    }
}

async fn user_send_transaction_to_compute(
    network: &mut Network,
    from_user: &str,
    to_compute: &str,
    tx: &Transaction,
) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_transactions_to_compute(compute_node_addr, vec![tx.clone()])
        .await
        .unwrap();
}

async fn user_send_next_payment_to_destinations(
    network: &mut Network,
    from_user: &str,
    to_compute: &str,
) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_next_payment_to_destinations(compute_node_addr)
        .await
        .unwrap();
}

async fn user_send_address_request(
    network: &mut Network,
    from_user: &str,
    to_user: &str,
    amount: TokenAmount,
) {
    let user_node_addr = network.get_address(to_user).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_address_request(user_node_addr, amount)
        .await
        .unwrap();
}

async fn user_send_address_to_trading_peer(network: &mut Network, user: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    u.send_address_to_trading_peer().await.unwrap();
}

async fn user_send_block_notification_request(network: &mut Network, user: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    let address = u.compute_address();
    u.send_block_notification_request(address).await.unwrap();
}

async fn user_last_block_notified_txs(network: &mut Network, user: &str) -> Vec<String> {
    let u = network.user(user).unwrap().lock().await;
    u.last_block_notified.transactions.clone()
}

//
// MinerNode helpers
//
async fn miner_handle_event(network: &mut Network, miner: &str, reason_val: &str) {
    let mut m = network.miner(miner).unwrap().lock().await;
    let success_val = true;

    match time::timeout(TIMEOUT_TEST_WAIT_DURATION, m.handle_next_event()).await {
        Ok(Some(Ok(Response { success, reason })))
            if success == success_val && reason == reason_val => {}
        other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
    }
}

async fn miner_all_handle_event(network: &mut Network, miner_group: &[String], reason_str: &str) {
    for miner in miner_group {
        miner_handle_event(network, miner, reason_str).await;
    }
}

async fn miner_send_partition_request(network: &mut Network, from_miner: &str, to_compute: &str) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut m = network.miner(from_miner).unwrap().lock().await;
    m.send_partition_request(compute_node_addr).await.unwrap();
}

async fn miner_send_partition_pow(network: &mut Network, from_miner: &str, to_compute: &str) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut m = network.miner(from_miner).unwrap().lock().await;

    let pow = m.generate_partition_pow().await.unwrap();
    m.send_partition_pow(compute_node_addr, pow).await.unwrap();
}

async fn miner_send_pow_for_current(network: &mut Network, from_miner: &str, to_compute: &str) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut m = network.miner(from_miner).unwrap().lock().await;

    let (b_num, nonce, transaction) = m.generate_pow_for_current_block().await.unwrap();
    m.send_pow(compute_node_addr, b_num, nonce, transaction)
        .await
        .unwrap();
}

async fn miner_commit_block_found(network: &mut Network, miner: &str) {
    let mut m = network.miner(miner).unwrap().lock().await;
    m.commit_block_found().await;
}

async fn miner_all_send_pow_for_current(
    network: &mut Network,
    from_miner: &str,
    to_compute_group: &[String],
) {
    for to_compute in to_compute_group {
        miner_send_pow_for_current(network, from_miner, to_compute).await;
    }
}

//
// Test helpers
//

fn test_step_start() {
    let _ = tracing_subscriber::fmt::try_init();
    info!("Test Step start");
}

async fn test_step_complete(network: Network) {
    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
}

fn valid_transactions(fixed: bool) -> BTreeMap<String, Transaction> {
    valid_transactions_with(fixed, DEFAULT_SEED_AMOUNT)
}

fn valid_transactions_with(fixed: bool, amount: TokenAmount) -> BTreeMap<String, Transaction> {
    let (pk, sk) = if !fixed {
        let (pk, sk) = sign::gen_keypair();
        println!("sk: {}, pk: {}", hex::encode(&sk), hex::encode(&pk));
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
            create_valid_transaction_with_ins_outs(ins, outs, &pk, &sk, amount);
        transactions.insert(t_hash, payment_tx);
    }

    transactions
}

fn make_compute_seed_utxo(seed: &[(i32, &str)], amount: TokenAmount) -> UtxoSetSpec {
    seed.iter()
        .map(|(n, v)| {
            (
                v.to_string(),
                (0..*n)
                    .map(|_| TxOutSpec {
                        public_key: COMMON_PUB_KEY.to_owned(),
                        amount,
                    })
                    .collect(),
            )
        })
        .collect()
}

fn equal_first<T: Eq>(values: &[T]) -> Vec<bool> {
    values.iter().map(|v| *v == values[0]).collect()
}

fn len_and_map<K, V>(values: &BTreeMap<K, V>) -> (usize, &BTreeMap<K, V>) {
    (values.len(), &values)
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
        .chain(v2.clone().into_iter())
        .chain(v3.clone().into_iter())
        .collect()
}

fn wallet_seed(out_p: (i32, &str), amount: &TokenAmount) -> WalletTxSpec {
    WalletTxSpec {
        out_point: format!("{}-{}", out_p.0, out_p.1),
        secret_key: COMMON_SEC_KEY.to_owned(),
        public_key: COMMON_PUB_KEY.to_owned(),
        amount: amount.0,
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

fn complete_block_mining_txs(block: &CompleteBlock) -> BTreeMap<String, Transaction> {
    block
        .per_node
        .values()
        .map(|m_info| m_info.mining_tx.clone())
        .collect()
}

async fn complete_first_block(
    next_block_tx: &BTreeMap<String, Transaction>,
    mining_txs: usize,
) -> ((String, String), CompleteBlock) {
    complete_block(0, None, &next_block_tx, mining_txs).await
}

async fn construct_mining_extra_info(
    block: Block,
    block_num: u64,
    addr: String,
) -> MinedBlockExtraInfo {
    let amount = calculate_reward(TokenAmount(0));
    let tx = construct_coinbase_tx(block_num, amount, addr);
    let hash = construct_tx_hash(&tx);

    MinedBlockExtraInfo {
        nonce: generate_pow_for_block(&block.clone(), hash.clone()).await,
        mining_tx: (hash, tx),
    }
}

async fn complete_block(
    block_num: u64,
    previous_hash: Option<&str>,
    block_txs: &BTreeMap<String, Transaction>,
    mining_txs: usize,
) -> ((String, String), CompleteBlock) {
    let mut block = Block::new();
    block.header.b_num = block_num;
    block.header.previous_hash = previous_hash.map(|v| v.to_string());
    block.transactions = block_txs.keys().cloned().collect();

    let per_node_init: BTreeMap<u64, String> = (0..mining_txs)
        .map(|i| i as u64 + 1)
        .map(|idx| (idx, hex::encode(vec![block_num as u8, idx as u8])))
        .collect();

    let mut per_node: BTreeMap<u64, MinedBlockExtraInfo> = BTreeMap::new();
    let init_keys: Vec<_> = per_node_init.keys().cloned().collect();
    for idx in init_keys {
        let addr = per_node_init.get(&idx).unwrap().clone();
        let mined_block_info = construct_mining_extra_info(block.clone(), block_num, addr).await;
        per_node.insert(idx, mined_block_info);
    }

    let complete = CompleteBlock {
        common: CommonBlockInfo {
            block,
            block_txs: block_txs.clone(),
        },
        per_node,
    };
    let stored = StoredSerializingBlock {
        block: complete.common.block.clone(),
        mining_tx_hash_and_nonces: complete
            .per_node
            .iter()
            .map(|(idx, v)| (*idx, (v.mining_tx.0.clone(), v.nonce.clone())))
            .collect(),
    };

    let hash_key = {
        let hash_input = serialize(&stored).unwrap();
        let hash_digest = Sha3_256::digest(&hash_input);
        hex::encode(hash_digest)
    };
    let complete_str = format!("{:?}", complete);

    ((hash_key, complete_str), complete)
}

async fn generate_pow_for_block(block: &Block, mining_tx_hash: String) -> Vec<u8> {
    let hash_to_mine =
        concat_merkle_coinbase(&block.header.merkle_root_hash, &mining_tx_hash).await;
    let mut nonce: Vec<u8> = generate_nonce();
    let prev_hash: String;
    let temp_option = block.header.previous_hash.clone();
    match temp_option {
        None => prev_hash = String::from(""),
        _ => prev_hash = temp_option.unwrap(),
    }

    while !validate_pow_block(&prev_hash, &hash_to_mine, &nonce) {
        nonce = generate_nonce();
    }
    nonce
}
/// Generates a random sequence of values for a nonce
fn generate_nonce() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..16).map(|_| rng.gen_range(0, 200)).collect()
}

fn complete_network_config(initial_port: u16) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        compute_raft: false,
        storage_raft: false,
        in_memory_db: true,
        compute_partition_full_size: 1,
        compute_minimum_miner_pool_len: 1,
        nodes: vec![
            (NodeType::Miner, vec!["miner1".to_string()]),
            (NodeType::Compute, vec!["compute1".to_string()]),
            (NodeType::Storage, vec!["storage1".to_string()]),
            (NodeType::User, vec!["user1".to_string()]),
        ]
        .into_iter()
        .collect(),
        compute_seed_utxo: make_compute_seed_utxo(SEED_UTXO, DEFAULT_SEED_AMOUNT),
        user_wallet_seeds: Vec::new(),
        compute_to_miner_mapping: Some(("compute1".to_string(), vec!["miner1".to_string()]))
            .into_iter()
            .collect(),
        test_duration_divider: TEST_DURATION_DIVIDER,
    }
}

fn complete_network_config_with_n_compute_raft(
    initial_port: u16,
    compute_count: usize,
) -> NetworkConfig {
    complete_network_config_with_n_compute_miner(initial_port, true, compute_count, 1)
}

fn complete_network_config_with_n_compute_miner(
    initial_port: u16,
    use_raft: bool,
    compute_count: usize,
    miner_count: usize,
) -> NetworkConfig {
    let mut cfg = complete_network_config(initial_port);
    cfg.compute_raft = use_raft;
    cfg.storage_raft = use_raft;
    *cfg.nodes_mut(NodeType::Compute) = (0..compute_count)
        .map(|idx| format!("compute{}", idx + 1))
        .collect();
    *cfg.nodes_mut(NodeType::Storage) = (0..compute_count)
        .map(|idx| format!("storage{}", idx + 1))
        .collect();
    *cfg.nodes_mut(NodeType::Miner) = (0..miner_count)
        .map(|idx| format!("miner{}", idx + 1))
        .collect();
    cfg.compute_to_miner_mapping = {
        let miner_nodes = &cfg.nodes[&NodeType::Miner];
        let compute_nodes = &cfg.nodes[&NodeType::Compute];
        let miners = miner_nodes.iter().cloned().cycle();
        let computes = compute_nodes.iter().cloned().cycle();
        let connections = std::cmp::max(miner_nodes.len(), compute_nodes.len());
        let mut mapping = BTreeMap::new();
        for (miner, compute) in miners.zip(computes).take(connections) {
            mapping.entry(compute).or_insert_with(Vec::new).push(miner);
        }
        mapping
    };

    cfg
}
