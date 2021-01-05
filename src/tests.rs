//! Test suite for the network functions.

use crate::compute::ComputeNode;
use crate::constants::{DB_PATH, DB_PATH_TEST};
use crate::interfaces::{
    BlockStoredInfo, CommonBlockInfo, ComputeRequest, MinedBlockExtraInfo, Response, StorageRequest,
};
use crate::storage::{StorageNode, StoredSerializingBlock};
use crate::storage_raft::CompleteBlock;
use crate::test_utils::{Network, NetworkConfig};
use crate::utils::create_valid_transaction;
use bincode::serialize;
use futures::future::join_all;
use naom::primitives::asset::TokenAmount;
use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use naom::primitives::transaction_utils::{construct_coinbase_tx, construct_tx_hash};
use sha3::Digest;
use sha3::Sha3_256;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error_span, info};
use tracing_futures::Instrument;

const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);

const SEED_UTXO: [&str; 1] = ["000000"];
const SEED_UTXO_BLOCK_HASH: &str =
    "e18f57f62c7bb00811c032b56c8113c83520c1bf9b8428cc96e4c8d5b704d11b";
const HASH_LEN: usize = 64;

const BLOCK_RECEIVED: &str = "Block received to be added";
const BLOCK_STORED: &str = "Block complete stored";
const BLOCK_RECEIVED_AND_STORED: [&str; 2] = [BLOCK_RECEIVED, BLOCK_STORED];

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

#[tokio::test(basic_scheduler)]
async fn full_flow_no_raft() {
    full_flow(complete_network_config(10500)).await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_no_raft_real_db() {
    let mut cfg = complete_network_config(10505);
    let storage_node_db_path = format!("{}/{}.{}", DB_PATH, DB_PATH_TEST, 10507);
    if let Err(e) = std::fs::remove_dir_all(storage_node_db_path) {
        info!("Not removed storage db: {:?}", e);
    }
    cfg.in_memory_db = false;

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
    )
    .await;
}

#[tokio::test(basic_scheduler)]
async fn full_flow_raft_20_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10550, 20)).await;
}

async fn full_flow(network_config: NetworkConfig) {
    full_flow_common(network_config, CfgNum::All).await;
}

async fn full_flow_common(network_config: NetworkConfig, cfg_num: CfgNum) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let storage_nodes = &network_config.storage_nodes;
    let initial_utxo = network.collect_initial_uxto_set();
    let (transactions, _t_hash, tx) = valid_transactions(true);

    //
    // Act
    //
    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, Cfg::All, cfg_num).await;
    send_block_to_storage_act(&mut network, cfg_num).await;
    let stored0 = storage_get_last_block_stored(&mut network, "storage1").await;

    add_transactions_act(&mut network, &tx).await;
    create_block_act(&mut network, Cfg::All, cfg_num).await;
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
        1 + initial_utxo.len() + stored0.unwrap().mining_transactions.len();
    let expected_block1_db_count =
        1 + transactions.len() + stored1.unwrap().mining_transactions.len();
    assert_eq!(
        actual0_db_count,
        node_all(
            storage_nodes,
            expected_block0_db_count + expected_block1_db_count
        )
    );

    test_step_complete(network).await;
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
    let compute_nodes = &network_config.compute_nodes;
    let expected_utxo = network.collect_initial_uxto_set();

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
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;

    info!("Test Step Create first Block");
    node_all_handle_event(network, compute_nodes, &["First Block committed"]).await;

    info!("Test Step Connect nodes");
    compute_all_connect_to_storage(network, compute_nodes).await;
    node_connect_to_all(network, "miner1", compute_nodes).await;
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
    let compute_nodes = &network_config.compute_nodes;
    let storage_nodes = &network_config.storage_nodes;
    let initial_utxo = network.collect_initial_uxto_set();
    let c_mined = &node_select(compute_nodes, cfg_num);
    let (expected0, block_info0) = complete_block(0, None, &initial_utxo, c_mined.len());

    create_first_block_act(&mut network).await;
    compute_all_mining_block_mined(&mut network, c_mined, &block_info0).await;

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
        node_all(storage_nodes, 1 + initial_utxo.len() + c_mined.len())
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
    let compute_nodes = &network_config.compute_nodes;
    let (transactions, _t_hash, tx) = valid_transactions(true);
    create_first_block_act(&mut network).await;

    //
    // Act
    //
    add_transactions_act(&mut network, &tx).await;

    //
    // Assert
    //
    let actual = compute_all_committed_tx_pool(&mut network, compute_nodes).await;
    assert_eq!(actual[0], transactions);
    assert_eq!(equal_first(&actual), node_all(compute_nodes, true));

    test_step_complete(network).await;
}

async fn add_transactions_act(network: &mut Network, tx: &Transaction) {
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;

    info!("Test Step Add Transactions");
    node_connect_to(network, "user1", "compute1").await;
    user_send_payment_to_compute(network, "user1", "compute1", tx).await;
    compute_handle_event(network, "compute1", "Transactions added to tx pool").await;
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
    let compute_nodes = &network_config.compute_nodes;
    let (transactions, t_hash, tx) = valid_transactions(true);
    let (_, block_info0) = complete_block(0, None, &BTreeMap::new(), compute_nodes.len());
    let block0_mining_tx = complete_block_mining_txs(&block_info0);

    create_first_block_act(&mut network).await;
    compute_all_mining_block_mined(&mut network, compute_nodes, &block_info0).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;
    add_transactions_act(&mut network, &tx).await;

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
        node_all(compute_nodes, Some(vec![t_hash]))
    );

    let expected_utxo = merge_txs(&transactions, &block0_mining_tx);
    assert_eq!(len_and_map(&utxo_set_after[0]), len_and_map(&expected_utxo));
    assert_eq!(equal_first(&utxo_set_after), node_all(compute_nodes, true));

    test_step_complete(network).await;
}

async fn create_block_act(network: &mut Network, cfg: Cfg, cfg_num: CfgNum) {
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;
    let storage_nodes = &config.storage_nodes;
    let msg_c_nodes = &node_select(compute_nodes, cfg_num);
    let msg_s_nodes = &node_select(storage_nodes, cfg_num);

    info!("Test Step Storage signal new block");
    if cfg == Cfg::IgnoreStorage {
        let req = ComputeRequest::SendBlockStored(Default::default());
        compute_all_inject_next_event(network, msg_s_nodes, msg_c_nodes, req).await;
    } else {
        storage_all_send_stored_block(network, msg_s_nodes).await;
    }
    compute_all_handle_event(network, msg_c_nodes, "Received block stored").await;

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
        complete_network_config_with_n_compute_raft(10140, 3),
        CfgNum::Majority,
    )
    .await;
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
    let compute_nodes = &network_config.compute_nodes;
    let c_mined = &node_select(compute_nodes, cfg_num);

    create_first_block_act(&mut network).await;
    create_block_act(&mut network, Cfg::IgnoreStorage, CfgNum::All).await;

    //
    // Act
    //
    let block_before = compute_all_mined_block_num(&mut network, compute_nodes).await;

    proof_of_work_act(&mut network, Cfg::All, cfg_num).await;

    miner_all_send_pow_for_current(&mut network, "miner1", c_mined).await;
    compute_all_handle_error(&mut network, c_mined, "Not mining given block").await;

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
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;
    let c_mined = &node_select(compute_nodes, cfg_num);

    info!("Test Step Miner block Proof of Work: partition-> rand num -> num pow -> pre-block -> block pow");
    if cfg == Cfg::IgnoreMiner {
        let (_, block_info) = complete_block(0, None, &BTreeMap::new(), c_mined.len());
        compute_all_mining_block_mined(network, c_mined, &block_info).await;
        return;
    }

    for compute in c_mined {
        if compute_miner_request_list_is_empty(network, compute).await {
            miner_send_partition_request(network, "miner1", compute).await;
            compute_handle_event(network, compute, "Received first full partition request").await;
        }

        compute_flood_rand_num_to_requesters(network, compute).await;
        miner_handle_event(network, "miner1", "Received random number successfully").await;

        miner_send_partition_pow(network, "miner1", compute).await;
        compute_handle_event(network, compute, "Partition list is full").await;

        compute_flood_block_to_partition(network, compute).await;
        miner_handle_event(network, "miner1", "Pre-block received successfully").await;

        miner_send_pow_for_current(network, "miner1", compute).await;
        compute_handle_event(network, compute, "Received PoW successfully").await;
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
    let compute_nodes = &network_config.compute_nodes;
    let storage_nodes = &network_config.storage_nodes;
    let c_mined = &node_select(compute_nodes, cfg_num);

    let (transactions, _t_hash, _tx) = valid_transactions(true);
    let (expected1, block_info1) = complete_block(1, Some("0"), &transactions, c_mined.len());
    let (_expected3, wrong_block3) = complete_block(3, Some("0"), &BTreeMap::new(), 1);
    let block1_mining_tx = complete_block_mining_txs(&block_info1);

    create_first_block_act(&mut network).await;
    proof_of_work_act(&mut network, Cfg::IgnoreMiner, CfgNum::All).await;
    send_block_to_storage_act(&mut network, CfgNum::All).await;

    compute_all_set_mining_block(&mut network, c_mined, &block_info1).await;
    compute_all_mining_block_mined(&mut network, c_mined, &block_info1).await;

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
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;
    let storage_nodes = &config.storage_nodes;
    let msg_c_nodes = &node_select(compute_nodes, cfg_num);
    let msg_s_nodes = &node_select(storage_nodes, cfg_num);

    info!("Test Step Compute Send block to Storage");
    compute_all_send_block_to_storage(network, msg_c_nodes).await;
    storage_all_handle_event(network, msg_s_nodes, BLOCK_RECEIVED).await;
    node_all_handle_event(network, storage_nodes, &[BLOCK_STORED]).await;
}

#[tokio::test(basic_scheduler)]
async fn receive_payment_tx_user() {
    test_step_start();

    //
    // Arrange
    //
    let mut network_config = complete_network_config(10400);
    network_config.user_nodes.push("user2".to_string());
    let mut network = Network::create_from_config(&network_config).await;

    //
    // Act/Assert
    //
    node_connect_to(&mut network, "user1", "user2").await;
    node_connect_to(&mut network, "user1", "compute1").await;
    user_send_address_request(&mut network, "user1", "user2").await;
    user_handle_event(&mut network, "user2", "New address ready to be sent").await;

    //
    // Assert
    //
    let actual = user_trading_peer(&mut network, "user2").await;
    let expected = network.get_address("user1").await;
    assert_eq!(actual, expected);

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
    }
}

async fn node_connect_to_all(network: &mut Network, from: &str, tos: &[String]) {
    for to in tos {
        node_connect_to(network, from, to).await;
    }
}

async fn node_all_handle_event(network: &mut Network, node_group: &[String], reason_str: &[&str]) {
    let mut join_handles = Vec::new();
    let barrier = Arc::new(Barrier::new(node_group.len()));
    for node_name in node_group {
        let barrier = barrier.clone();
        let reason_str: Vec<_> = reason_str.iter().map(|s| s.to_string()).collect();
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
    let _ = join_all(join_handles).await;
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
       _ = time::delay_for(TIMEOUT_TEST_WAIT_DURATION) => panic!("Timeout {:?}", reason_str),
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
    let c = network.compute(compute).unwrap().lock().await;
    c.get_mining_block()
        .as_ref()
        .map(|b| b.transactions.clone())
}

async fn compute_all_committed_utxo_set(
    network: &mut Network,
    compute_group: &[String],
) -> Vec<BTreeMap<String, Transaction>> {
    let mut result = Vec::new();
    for name in compute_group {
        let r = compute_committed_utxo_set(network, name).await;
        result.push(r);
    }
    result
}

async fn compute_committed_utxo_set(
    network: &mut Network,
    compute: &str,
) -> BTreeMap<String, Transaction> {
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

async fn compute_connect_to_storage(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.connect_to_storage().await.unwrap();
}

async fn compute_all_connect_to_storage(network: &mut Network, compute_group: &[String]) {
    for compute in compute_group {
        compute_connect_to_storage(network, compute).await;
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

async fn compute_send_block_to_storage(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.send_block_to_storage().await.unwrap();
}

async fn compute_all_send_block_to_storage(network: &mut Network, compute_group: &[String]) {
    for compute in compute_group {
        compute_send_block_to_storage(network, compute).await;
    }
}

async fn compute_mining_block_mined(
    network: &mut Network,
    compute: &str,
    block_info: &CompleteBlock,
) {
    let id = network.get_position(compute).unwrap() as u64 + 1;
    let mut c = network.compute(compute).unwrap().lock().await;
    let mined = block_info.per_node.get(&id).unwrap();

    c.mining_block_mined(mined.nonce.clone(), mined.mining_tx.clone());
}

async fn compute_all_mining_block_mined(
    network: &mut Network,
    compute_group: &[String],
    block_info: &CompleteBlock,
) {
    for compute in compute_group {
        compute_mining_block_mined(network, compute, block_info).await;
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
    let id = network.get_position(compute).unwrap() as u64 + 1;

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
       _ = time::delay_for(TIMEOUT_TEST_WAIT_DURATION) => panic!("Timeout {:?}", reason_str),
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
        other => panic!("Unexpected result: {:?}", other),
    }
}

async fn user_send_payment_to_compute(
    network: &mut Network,
    from_user: &str,
    to_compute: &str,
    tx: &Transaction,
) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_payment_to_compute(compute_node_addr, tx.clone())
        .await
        .unwrap();
}

async fn user_send_address_request(network: &mut Network, from_user: &str, to_user: &str) {
    let user_node_addr = network.get_address(to_user).await.unwrap();
    let mut u = network.user(from_user).unwrap().lock().await;
    u.send_address_request(user_node_addr).await.unwrap();
}

async fn user_trading_peer(network: &mut Network, user: &str) -> Option<SocketAddr> {
    let u = network.user(user).unwrap().lock().await;
    u.trading_peer
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
        other => panic!("Unexpected result: {:?}", other),
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

    let (nonce, transaction) = m.generate_pow_for_current_block().await.unwrap();
    m.send_pow(compute_node_addr, nonce, transaction)
        .await
        .unwrap();
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

fn valid_transactions(fixed: bool) -> (BTreeMap<String, Transaction>, String, Transaction) {
    let intial_t_hash = SEED_UTXO[0];
    let receiver_addr = "000001";

    let (pk, sk) = if !fixed {
        let (pk, sk) = sign::gen_keypair();
        println!("sk: {}, pk: {}", hex::encode(&sk), hex::encode(&pk));
        (pk, sk)
    } else {
        let sk_slice = hex::decode("0186bc08f16428d2059227082b93e439ff50f8c162f24b9594b132f2cc15fca45371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c").unwrap();
        let pk_slice =
            hex::decode("5371832122a8e804fa3520ec6861c3fa554a7f6fb617e6f0768452090207e07c")
                .unwrap();
        let sk = SecretKey::from_slice(&sk_slice).unwrap();
        let pk = PublicKey::from_slice(&pk_slice).unwrap();
        (pk, sk)
    };

    let (t_hash, payment_tx) = create_valid_transaction(intial_t_hash, receiver_addr, &pk, &sk);

    let transactions = {
        let mut m = BTreeMap::new();
        m.insert(t_hash.clone(), payment_tx.clone());
        m
    };

    (transactions, t_hash, payment_tx)
}

fn equal_first<T: Eq>(values: &[T]) -> Vec<bool> {
    values.iter().map(|v| *v == values[0]).collect()
}

fn len_and_map<K, V>(values: &BTreeMap<K, V>) -> (usize, &BTreeMap<K, V>) {
    (values.len(), &values)
}

fn substract_vec(value1: &[usize], value2: &[usize]) -> Vec<usize> {
    value1
        .iter()
        .zip(value2.iter())
        .map(|(v1, v2)| v1 - v2)
        .collect()
}

fn merge_txs(
    v1: &BTreeMap<String, Transaction>,
    v2: &BTreeMap<String, Transaction>,
) -> BTreeMap<String, Transaction> {
    merge_txs_3(v1, v2, &BTreeMap::new())
}

fn merge_txs_3(
    v1: &BTreeMap<String, Transaction>,
    v2: &BTreeMap<String, Transaction>,
    v3: &BTreeMap<String, Transaction>,
) -> BTreeMap<String, Transaction> {
    v1.clone()
        .into_iter()
        .chain(v2.clone().into_iter())
        .chain(v3.clone().into_iter())
        .collect()
}

fn complete_block_mining_txs(block: &CompleteBlock) -> BTreeMap<String, Transaction> {
    block
        .per_node
        .values()
        .map(|m_info| m_info.mining_tx.clone())
        .collect()
}

fn complete_block(
    block_num: u64,
    previous_hash: Option<&str>,
    block_txs: &BTreeMap<String, Transaction>,
    mining_txs: usize,
) -> ((String, String), CompleteBlock) {
    let mut block = Block::new();
    block.header.b_num = block_num;
    block.header.time = block_num as u32;
    block.header.previous_hash = previous_hash.map(|v| v.to_string());
    block.transactions = block_txs.keys().cloned().collect();

    let construct_mining_extra_info = |addr: String| -> MinedBlockExtraInfo {
        let amount = TokenAmount(12000);
        let tx = construct_coinbase_tx(amount, block.header.time, addr.clone());
        let hash = construct_tx_hash(&tx);
        MinedBlockExtraInfo {
            nonce: addr.as_bytes().to_vec(),
            mining_tx: (hash, tx),
        }
    };

    let per_node = (0..mining_txs)
        .map(|i| i as u64 + 1)
        .map(|idx| (idx, hex::encode(vec![block_num as u8, idx as u8])))
        .map(|(idx, addr)| (idx, construct_mining_extra_info(addr)))
        .collect();

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

fn complete_network_config(initial_port: u16) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        compute_raft: false,
        storage_raft: false,
        in_memory_db: true,
        miner_nodes: vec!["miner1".to_string()],
        compute_nodes: vec!["compute1".to_string()],
        storage_nodes: vec!["storage1".to_string()],
        user_nodes: vec!["user1".to_string()],
        compute_seed_utxo: SEED_UTXO.iter().map(|v| v.to_string()).collect(),
    }
}

fn complete_network_config_with_n_miners(initial_port: u16, miner_count: usize) -> NetworkConfig {
    let mut cfg = complete_network_config(initial_port);
    cfg.miner_nodes = (0..miner_count)
        .map(|idx| format!("miner{}", idx + 1))
        .collect();
    cfg
}

fn complete_network_config_with_n_compute_raft(
    initial_port: u16,
    compute_count: usize,
) -> NetworkConfig {
    let mut cfg = complete_network_config(initial_port);
    cfg.compute_raft = true;
    cfg.storage_raft = true;
    cfg.compute_nodes = (0..compute_count)
        .map(|idx| format!("compute{}", idx + 1))
        .collect();
    cfg.storage_nodes = (0..compute_count)
        .map(|idx| format!("storage{}", idx + 1))
        .collect();
    cfg
}
