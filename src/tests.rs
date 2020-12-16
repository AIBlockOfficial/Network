//! Test suite for the network functions.

use crate::compute::{ComputeNode, MinedBlock};
use crate::interfaces::{CommonBlockInfo, ComputeRequest, MinedBlockExtraInfo, Response};
use crate::storage::StorageNode;
use crate::storage_raft::CompleteBlock;
use crate::test_utils::{Network, NetworkConfig};
use crate::utils::create_valid_transaction;
use bincode::serialize;
use futures::future::join_all;
use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use naom::primitives::transaction_utils::{construct_coinbase_tx, construct_tx_hash};
use sha3::Digest;
use sha3::Sha3_256;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::{debug, error_span, info};
use tracing_futures::Instrument;

const SEED_UTXO: [&str; 1] = ["000000"];
const SEED_UTXO_BLOCK_HASH: &str =
    "e18f57f62c7bb00811c032b56c8113c83520c1bf9b8428cc96e4c8d5b704d11b";
const HASH_LEN: usize = 64;

const BLOCK_RECEIVED_AND_STORED: [&str; 2] =
    ["Block received to be added", "Block complete stored"];

#[derive(Clone, Debug, PartialEq, Eq)]
enum Cfg {
    All,
    IgnoreStorage,
    IgnoreCompute,
}

#[tokio::test(threaded_scheduler)]
async fn full_flow_no_raft() {
    full_flow(complete_network_config(10500)).await;
}

#[tokio::test(threaded_scheduler)]
async fn full_flow_raft_1_node() {
    full_flow(complete_network_config_with_n_compute_raft(10510, 1)).await;
}

#[tokio::test(threaded_scheduler)]
async fn full_flow_raft_2_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10520, 2)).await;
}

#[tokio::test(threaded_scheduler)]
async fn full_flow_raft_3_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10530, 3)).await;
}

#[tokio::test(threaded_scheduler)]
async fn full_flow_raft_20_nodes() {
    full_flow(complete_network_config_with_n_compute_raft(10540, 20)).await;
}

async fn full_flow(network_config: NetworkConfig) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let (_transactions, _t_hash, tx) = valid_transactions(true);

    //
    // Act
    //
    first_block_act(&mut network, Cfg::All).await;
    add_transactions_act(&mut network, &tx).await;
    create_block_act(&mut network, Cfg::All).await;
    proof_of_work_act(&mut network).await;
}

#[tokio::test(threaded_scheduler)]
async fn first_block_no_raft() {
    first_block(complete_network_config(10000)).await;
}

#[tokio::test(threaded_scheduler)]
async fn first_block_raft_1_node() {
    first_block(complete_network_config_with_n_compute_raft(10010, 1)).await;
}

#[tokio::test(threaded_scheduler)]
async fn first_block_raft_2_nodes() {
    first_block(complete_network_config_with_n_compute_raft(10020, 2)).await;
}

#[tokio::test(threaded_scheduler)]
async fn first_block_raft_3_nodes() {
    first_block(complete_network_config_with_n_compute_raft(10030, 3)).await;
}

#[tokio::test(threaded_scheduler)]
async fn first_block_raft_20_nodes() {
    first_block(complete_network_config_with_n_compute_raft(10040, 20)).await;
}

async fn first_block(network_config: NetworkConfig) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let storage_nodes = &network_config.storage_nodes;
    let expected_utxo = network.collect_initial_uxto_set();
    let (expected0, _block_info) = complete_block(0, None, expected_utxo, 0);

    //
    // Act
    //
    first_block_act(&mut network, Cfg::All).await;

    //
    // Assert
    //
    let actual0 = storage_all_get_last_block_stored(&mut network, storage_nodes).await;
    assert_eq!(
        actual0[0],
        Some((
            expected0.1,
            expected0.0,
            0, /*b_num*/
            0, /*mining txs*/
        ))
    );
    assert_eq!(
        actual0.iter().map(|v| *v == actual0[0]).collect::<Vec<_>>(),
        node_all(storage_nodes, true)
    );

    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
}

async fn first_block_act(network: &mut Network, cfg: Cfg) {
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;
    let storage_nodes = &config.storage_nodes;

    info!("Test Step First Block");
    node_all_handle_event(network, compute_nodes, &["First Block committed"]).await;
    if cfg != Cfg::IgnoreStorage {
        compute_all_connect_to_storage(network, compute_nodes).await;
        compute_all_send_first_block_to_storage(network, compute_nodes).await;
        node_all_handle_event(network, storage_nodes, &BLOCK_RECEIVED_AND_STORED).await;
    }
}

#[tokio::test(threaded_scheduler)]
async fn add_transactions_no_raft() {
    add_transactions(complete_network_config(10600)).await;
}

#[tokio::test(threaded_scheduler)]
async fn add_transactions_raft_1_node() {
    add_transactions(complete_network_config_with_n_compute_raft(10610, 1)).await;
}

#[tokio::test(threaded_scheduler)]
async fn add_transactions_raft_2_nodes() {
    add_transactions(complete_network_config_with_n_compute_raft(10620, 2)).await;
}

#[tokio::test(threaded_scheduler)]
async fn add_transactions_raft_3_nodes() {
    add_transactions(complete_network_config_with_n_compute_raft(10630, 3)).await;
}

#[tokio::test(threaded_scheduler)]
async fn add_transactions_raft_20_nodes() {
    add_transactions(complete_network_config_with_n_compute_raft(10640, 20)).await;
}

async fn add_transactions(network_config: NetworkConfig) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let compute_nodes = &network_config.compute_nodes;
    let (transactions, _t_hash, tx) = valid_transactions(true);
    first_block_act(&mut network, Cfg::IgnoreStorage).await;

    //
    // Act
    //
    add_transactions_act(&mut network, &tx).await;

    //
    // Assert
    //
    assert_eq!(
        compute_all_committed_tx_pool(&mut network, compute_nodes).await,
        node_all(compute_nodes, transactions)
    );

    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
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

#[tokio::test(threaded_scheduler)]
async fn create_block_no_raft() {
    create_block(complete_network_config(10100)).await;
}

#[tokio::test(threaded_scheduler)]
async fn create_block_raft_1_node() {
    create_block(complete_network_config_with_n_compute_raft(10110, 1)).await;
}

#[tokio::test(threaded_scheduler)]
async fn create_block_raft_2_nodes() {
    create_block(complete_network_config_with_n_compute_raft(10120, 2)).await;
}

#[tokio::test(threaded_scheduler)]
async fn create_block_raft_3_nodes() {
    create_block(complete_network_config_with_n_compute_raft(10130, 3)).await;
}

#[tokio::test(threaded_scheduler)]
async fn create_block_raft_20_nodes() {
    create_block(complete_network_config_with_n_compute_raft(10140, 20)).await;
}

async fn create_block(network_config: NetworkConfig) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let compute_nodes = &network_config.compute_nodes;
    let (_transactions, t_hash, tx) = valid_transactions(true);

    first_block_act(&mut network, Cfg::All).await;
    add_transactions_act(&mut network, &tx).await;

    //
    // Act
    //
    let block_transaction_before =
        compute_all_current_block_transactions(&mut network, compute_nodes).await;

    create_block_act(&mut network, Cfg::All).await;

    let block_transaction_after =
        compute_all_current_block_transactions(&mut network, compute_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_transaction_before, node_all(compute_nodes, None));
    assert_eq!(
        block_transaction_after,
        node_all(compute_nodes, Some(vec![t_hash]))
    );

    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
}

async fn create_block_act(network: &mut Network, cfg: Cfg) {
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;

    info!("Test Step Storage signal new block");
    if cfg != Cfg::IgnoreStorage {
        storage_send_stored_block(network, "storage1").await;
    } else {
        let req = ComputeRequest::SendBlockStored(Default::default());
        compute_inject_next_event(network, "storage1", "compute1", req).await;
    }
    compute_handle_event(network, "compute1", "Received block stored").await;

    info!("Test Step Generate Block");
    node_all_handle_event(network, compute_nodes, &["Block committed"]).await;
}

#[tokio::test(threaded_scheduler)]
async fn proof_of_work_no_raft() {
    proof_of_work(complete_network_config(10200)).await;
}

#[tokio::test(threaded_scheduler)]
async fn proof_of_work_raft_1_node() {
    proof_of_work(complete_network_config_with_n_compute_raft(10210, 1)).await;
}

#[tokio::test(threaded_scheduler)]
async fn proof_of_work_raft_2_nodes() {
    proof_of_work(complete_network_config_with_n_compute_raft(10220, 2)).await;
}

#[tokio::test(threaded_scheduler)]
async fn proof_of_work_raft_3_nodes() {
    proof_of_work(complete_network_config_with_n_compute_raft(10230, 3)).await;
}

async fn proof_of_work(network_config: NetworkConfig) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut network = Network::create_from_config(&network_config).await;
    let compute_nodes = &network_config.compute_nodes;
    first_block_act(&mut network, Cfg::IgnoreStorage).await;
    create_block_act(&mut network, Cfg::IgnoreStorage).await;

    //
    // Act
    //
    let block_before = compute_all_mined_block_num(&mut network, compute_nodes).await;

    proof_of_work_act(&mut network).await;

    let block_after = compute_all_mined_block_num(&mut network, compute_nodes).await;

    //
    // Assert
    //
    assert_eq!(block_before, node_all(compute_nodes, None));
    assert_eq!(block_after, node_all(compute_nodes, Some(1)));
}

async fn proof_of_work_act(network: &mut Network) {
    let config = network.config.clone();
    let compute_nodes = &config.compute_nodes;

    info!("Test Step Miner Compute and Send Proof of work");
    let block = Block::new();
    node_connect_to_all(network, "miner1", compute_nodes).await;
    miner_all_send_pow(network, "miner1", compute_nodes, &block).await;
    miner_all_send_pow(network, "miner1", compute_nodes, &block).await;
    compute_all_handle_event(network, compute_nodes, "Received PoW successfully").await;
    compute_all_handle_error(network, compute_nodes, "Not mining given block").await;
}

#[tokio::test(threaded_scheduler)]
async fn send_block_to_storage_no_raft() {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let network_config = complete_network_config(10300);
    let mut network = Network::create_from_config(&network_config).await;
    compute_connect_to_storage(&mut network, "compute1").await;
    compute_handle_event(&mut network, "compute1", "First Block committed").await;
    compute_send_first_block_to_storage(&mut network, "compute1").await;
    storage_receive_and_store_block(&mut network, "storage1").await;

    let (transactions, _t_hash, _tx) = valid_transactions(true);
    let (_expected3, block_info3) = complete_block(3, Some("0"), BTreeMap::new(), 1);
    let (expected1, block_info1) = complete_block(1, Some("0"), transactions, 1);

    //
    // Act
    //
    compute_send_block_to_storage(&mut network, "compute1", &block_info3).await;
    storage_receive_block(&mut network, "storage1").await;

    compute_send_block_to_storage(&mut network, "compute1", &block_info1).await;
    storage_receive_and_store_block(&mut network, "storage1").await;
    let actual1 = storage_get_last_block_stored(&mut network, "storage1").await;

    //
    // Assert
    //
    assert_eq!(
        actual1,
        Some((
            expected1.1,
            expected1.0,
            1, /*b_num*/
            1  /*mining txs*/
        ))
    );
}

#[tokio::test(threaded_scheduler)]
async fn receive_payment_tx_user() {
    let _ = tracing_subscriber::fmt::try_init();

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
}

//
// Node helpers
//

fn node_all<T: Clone>(group: &[String], value: T) -> Vec<T> {
    group.iter().map(|_| value.clone()).collect()
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
    match c.handle_next_event().await {
        Some(Ok(Response { success, reason }))
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

async fn compute_set_current_block(network: &mut Network, compute: &str, block: Block) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.set_committed_mining_block(block, BTreeMap::new());
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

async fn compute_send_first_block_to_storage(network: &mut Network, compute: &str) {
    let mut c = network.compute(compute).unwrap().lock().await;
    c.send_first_block_to_storage().await.unwrap();
}

async fn compute_all_send_first_block_to_storage(network: &mut Network, compute_group: &[String]) {
    for compute in compute_group {
        compute_send_first_block_to_storage(network, compute).await;
    }
}

async fn compute_send_block_to_storage(
    network: &mut Network,
    compute: &str,
    block_info: &CompleteBlock,
) {
    let id = network.get_position(compute).unwrap() as u64 + 1;
    let mut c = network.compute(compute).unwrap().lock().await;
    let mined = block_info.per_node.get(&id).unwrap();

    let mined_block = MinedBlock {
        nonce: mined.nonce.clone(),
        block: block_info.common.block.clone(),
        block_tx: block_info.common.block_txs.clone(),
        mining_transaction: mined.mining_tx.clone(),
    };
    c.current_mined_block = Some(mined_block);

    c.send_block_to_storage().await.unwrap();
}

//
// StorageNode helpers
//

async fn storage_get_last_block_stored(
    network: &mut Network,
    storage: &str,
) -> Option<(String, String, u64, usize)> {
    let s = network.storage(storage).unwrap().lock().await;
    s.get_last_block_stored().clone().map(|(complete, info)| {
        (
            format!("{:?}", complete),
            info.block_hash.clone(),
            info.block_num,
            info.mining_transactions.len(),
        )
    })
}

async fn storage_all_get_last_block_stored(
    network: &mut Network,
    storage_group: &[String],
) -> Vec<Option<(String, String, u64, usize)>> {
    let mut result = Vec::new();
    for name in storage_group {
        let r = storage_get_last_block_stored(network, name).await;
        result.push(r);
    }
    result
}

async fn storage_send_stored_block(network: &mut Network, storage: &str) {
    let mut s = network.storage(storage).unwrap().lock().await;
    s.send_stored_block().await.unwrap();
}

async fn storage_receive_and_store_block(network: &mut Network, storage_str: &str) {
    storage_receive_block(network, storage_str).await;
    storage_store_block(network, storage_str).await;
}

async fn storage_handle_event_for_node(s: &mut StorageNode, success_val: bool, reason_val: &str) {
    match s.handle_next_event().await {
        Some(Ok(Response { success, reason }))
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

async fn storage_receive_block(network: &mut Network, storage_str: &str) {
    let mut storage = network.storage(storage_str).unwrap().lock().await;
    storage_handle_event_for_node(&mut storage, true, "Block received to be added").await;
}

async fn storage_store_block(network: &mut Network, storage_str: &str) {
    let mut storage = network.storage(storage_str).unwrap().lock().await;
    storage_handle_event_for_node(&mut storage, true, "Block complete stored").await;
}

//
// UserNode helpers
//

async fn user_handle_event(network: &mut Network, user: &str, reason_val: &str) {
    let mut u = network.user(user).unwrap().lock().await;
    let success_val = true;

    match u.handle_next_event().await {
        Some(Ok(Response { success, reason }))
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

//
// MinerNode helpers
//

async fn miner_send_pow(network: &mut Network, from_miner: &str, to_compute: &str, block: &Block) {
    let compute_node_addr = network.get_address(to_compute).await.unwrap();
    let mut m = network.miner(from_miner).unwrap().lock().await;

    let (pow, transaction) = m.generate_pow_for_block(block.clone()).await.unwrap();
    m.send_pow(compute_node_addr, pow, transaction)
        .await
        .unwrap();
}

async fn miner_all_send_pow(
    network: &mut Network,
    from_miner: &str,
    to_compute_group: &[String],
    block: &Block,
) {
    for to_compute in to_compute_group {
        miner_send_pow(network, from_miner, to_compute, block).await;
    }
}

//
// Test helpers
//

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

fn complete_block(
    block_num: u64,
    previous_hash: Option<&str>,
    block_txs: BTreeMap<String, Transaction>,
    mining_txs: usize,
) -> ((String, String), CompleteBlock) {
    let mut block = Block::new();
    block.header.b_num = block_num;
    block.header.time = block_num as u32;
    block.header.previous_hash = previous_hash.map(|v| v.to_string());
    block.transactions = block_txs.keys().cloned().collect();

    let construct_mining_extra_info = |addr: String| -> MinedBlockExtraInfo {
        let tx = construct_coinbase_tx(12, block.header.time, addr.clone());
        let hash = construct_tx_hash(&tx);
        MinedBlockExtraInfo {
            nonce: addr.as_bytes().to_vec(),
            mining_tx: (hash, tx),
        }
    };

    let per_node = (0..mining_txs)
        .map(|i| i as u64 + 1)
        .map(|idx| (idx, hex::encode(vec![idx as u8])))
        .map(|(idx, addr)| (idx, construct_mining_extra_info(addr)))
        .collect();

    let complete = CompleteBlock {
        common: CommonBlockInfo { block, block_txs },
        per_node,
    };

    let hash_key = {
        let hash_input = serialize(&complete).unwrap();
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
