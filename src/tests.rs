//! Test suite for the network functions.

use crate::compute::ComputeNode;
use crate::interfaces::Response;
use crate::test_utils::{Network, NetworkConfig};
use crate::utils::create_valid_transaction;
use futures::future::join_all;
use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use sodiumoxide::crypto::sign;
use std::collections::BTreeMap;

#[tokio::test(threaded_scheduler)]
async fn create_block() {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let network_config = complete_network_config(10000);
    let mut network = Network::create_from_config(&network_config)
        .await
        .spawn_raft_loops()
        .await;

    let (seed_utxo, _transactions, t_hash, tx) = valid_transactions();
    compute_seed_utxo(&mut network, "compute1", &seed_utxo);

    //
    // Act
    //
    spawn_connect_and_send_payment_to_compute(&mut network, "user1", "compute1", &tx);
    compute_handle_event(
        &mut network,
        "compute1",
        "All transactions successfully added to tx pool",
    )
    .await;
    let block_transaction_before = compute_current_block_transactions(&mut network, "compute1");
    compute_generate_block(&mut network, "compute1");
    let block_transaction_after = compute_current_block_transactions(&mut network, "compute1");

    //
    // Assert
    //
    assert_eq!(block_transaction_before, None);
    assert_eq!(block_transaction_after, Some(vec![t_hash]));
}

#[tokio::test(threaded_scheduler)]
async fn create_block_raft() {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let num_compute = 1;
    let network_config = complete_network_config_with_n_compute(10200, num_compute);
    let mut network = Network::create_from_config(&network_config)
        .await
        .spawn_raft_loops()
        .await;

    let (seed_utxo, _transactions, t_hash, tx) = valid_transactions();
    compute_seed_utxo(&mut network, "compute1", &seed_utxo);
    spawn_connect_and_send_payment_to_compute(&mut network, "user1", "compute1", &tx);
    compute_handle_event(
        &mut network,
        "compute1",
        "All transactions successfully added to tx pool",
    )
    .await;

    //
    // Act
    //
    compute_vote_generate_block(&mut network, "compute1").await;
    let block_transaction_before = compute_current_block_transactions(&mut network, "compute1");

    {
        let mut join_handles = Vec::new();
        for compute_name in &network_config.compute_nodes {
            let compute_name = compute_name.clone();
            let mut compute = network.take_compute(&compute_name).unwrap();

            join_handles.push(async move {
                compute_handle_event_for_node(&mut compute, "Block committed").await;
                (compute_name, compute)
            });
        }
        for (name, c) in join_all(join_handles).await {
            network.add_back_compute(name, c);
        }
    }

    let block_transaction_after = compute_current_block_transactions(&mut network, "compute1");

    //
    // Assert
    //
    assert_eq!(block_transaction_before, None);
    assert_eq!(block_transaction_after, Some(vec![t_hash]));
}

#[tokio::test(threaded_scheduler)]
async fn proof_of_work() {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let network_config = complete_network_config_with_n_miners(10010, 3);
    let mut network = Network::create_from_config(&network_config)
        .await
        .spawn_raft_loops()
        .await;

    let block = Block::new();

    //
    // Act
    //
    spawn_connect_and_send_pow(&mut network, "miner1", "compute1", &block);
    spawn_connect_and_send_pow(&mut network, "miner2", "compute1", &block);
    spawn_connect_and_send_pow(&mut network, "miner3", "compute1", &block);

    let block_hash_before = compute_block_hash(&mut network, "compute1");
    compute_handle_event(&mut network, "compute1", "Received PoW successfully").await;
    compute_handle_event(&mut network, "compute1", "Received PoW successfully").await;
    compute_handle_event(&mut network, "compute1", "Received PoW successfully").await;
    let block_hash_after = compute_block_hash(&mut network, "compute1");

    //
    // Assert
    //
    assert_eq!(block_hash_before.len(), 0);
    assert_eq!(block_hash_after.len(), 64);
}

#[tokio::test(threaded_scheduler)]
async fn send_block_to_storage() {
    let _ = tracing_subscriber::fmt::try_init();

    let network_config = complete_network_config(10020);
    let mut network = Network::create_from_config(&network_config)
        .await
        .spawn_raft_loops()
        .await;

    {
        let comp = network.compute("compute1").unwrap();
        comp.current_block = Some(Block::new());

        let mut c = comp.clone();
        tokio::spawn(async move {
            c.connect_to_storage().await.unwrap();
            let _write_to_store = c.send_block_to_storage().await.unwrap();
        });
    }

    {
        let storage = network.storage("storage1").unwrap();
        match storage.handle_next_event().await {
            Some(Ok(Response {
                success: true,
                reason: "Block received and added",
            })) => (),
            other => panic!("Unexpected result: {:?}", other),
        }
    }
}

#[tokio::test(threaded_scheduler)]
async fn receive_payment_tx_user() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut network_config = complete_network_config(10030);
    network_config.user_nodes.push("user2".to_string());
    let mut network = Network::create_from_config(&network_config)
        .await
        .spawn_raft_loops()
        .await;

    let compute_node_addr = network.get_address("compute1").unwrap();
    let user2_addr = network.get_address("user2").unwrap();
    let user = network.user("user1").unwrap();

    {
        let mut u = user.clone();
        tokio::spawn(async move {
            u.connect_to(user2_addr).await.unwrap();
            u.connect_to(compute_node_addr).await.unwrap();
            u.amount = 10;

            u.send_address_request(user2_addr).await.unwrap();
        });
    }

    {
        let u2 = network.user("user2").unwrap();
        match u2.handle_next_event().await {
            Some(Ok(Response {
                success: true,
                reason: "New address ready to be sent",
            })) => return (),
            other => panic!("Unexpected result: {:?}", other),
        }
    }
}

async fn compute_handle_event(network: &mut Network, compute: &str, reason_str: &str) {
    let c = network.compute(compute).unwrap();
    compute_handle_event_for_node(c, reason_str).await;
}

async fn compute_handle_event_for_node(c: &mut ComputeNode, reason_str: &str) {
    match c.handle_next_event().await {
        Some(Ok(Response {
            success: true,
            reason,
        })) if reason == reason_str => (),
        other => panic!("Unexpected result: {:?}", other),
    }
}

fn compute_seed_utxo(
    network: &mut Network,
    compute: &str,
    seed_utxo: &BTreeMap<String, Transaction>,
) {
    let c = network.compute(compute).unwrap();
    c.seed_utxo_set(seed_utxo.clone());
}

fn compute_generate_block(network: &mut Network, compute: &str) {
    let c = network.compute(compute).unwrap();
    c.generate_block();
}

async fn compute_vote_generate_block(network: &mut Network, compute: &str) {
    let c = network.compute(compute).unwrap();
    c.vote_generate_block().await;
}

fn compute_block_hash(network: &mut Network, compute: &str) -> String {
    let c = network.compute(compute).unwrap();
    c.last_block_hash.clone()
}

fn compute_current_block_transactions(network: &mut Network, compute: &str) -> Option<Vec<String>> {
    let c = network.compute(compute).unwrap();
    c.current_block.as_ref().map(|b| b.transactions.clone())
}

fn spawn_connect_and_send_payment_to_compute(
    network: &mut Network,
    from_user: &str,
    to_compute: &str,
    tx: &Transaction,
) {
    let compute_node_addr = network.get_address(to_compute).unwrap().clone();
    let user = network.user(from_user).unwrap();
    let tx = tx.clone();
    let mut u = user.clone();

    tokio::spawn(async move {
        u.connect_to(compute_node_addr).await.unwrap();
        u.send_payment_to_compute(compute_node_addr, tx)
            .await
            .unwrap();
    });
}

fn spawn_connect_and_send_pow(
    network: &mut Network,
    from_miner: &str,
    to_compute: &str,
    block: &Block,
) {
    let compute_node_addr = network.get_address(to_compute).unwrap();
    let miner = network.miner(from_miner).unwrap();
    let mut m = miner.clone();
    let mut m2 = miner.clone();
    let miner_block = block.clone();

    tokio::spawn(async move {
        let (pow, _conn) = tokio::join!(
            m2.generate_pow_for_block(miner_block),
            m.connect_to(compute_node_addr)
        );
        let (pow, transaction) = pow.unwrap();
        m.send_pow(compute_node_addr, pow, transaction)
            .await
            .unwrap();
    });
}

fn valid_transactions() -> (
    BTreeMap<String, Transaction>,
    BTreeMap<String, Transaction>,
    String,
    Transaction,
) {
    let intial_t_hash = "000000".to_owned();
    let receiver_addr = "000000".to_owned();

    let (pk, sk) = sign::gen_keypair();
    let (t_hash, payment_tx) = create_valid_transaction(&intial_t_hash, &receiver_addr, &pk, &sk);

    let transactions = {
        let mut m = BTreeMap::new();
        m.insert(t_hash.clone(), payment_tx.clone());
        m
    };
    let seed_utxo = {
        let mut m = BTreeMap::new();
        m.insert(intial_t_hash, Transaction::new());
        m
    };
    (seed_utxo, transactions, t_hash, payment_tx)
}

fn complete_network_config(initial_port: u16) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        miner_nodes: vec!["miner1".to_string()],
        compute_nodes: vec!["compute1".to_string()],
        storage_nodes: vec!["storage1".to_string()],
        user_nodes: vec!["user1".to_string()],
    }
}

fn complete_network_config_with_n_miners(initial_port: u16, miner_count: usize) -> NetworkConfig {
    let mut cfg = complete_network_config(initial_port);
    cfg.miner_nodes = (0..miner_count)
        .map(|idx| format!("miner{}", idx + 1))
        .collect();
    cfg
}

fn complete_network_config_with_n_compute(
    initial_port: u16,
    compute_count: usize,
) -> NetworkConfig {
    let mut cfg = complete_network_config(initial_port);
    cfg.compute_nodes = (0..compute_count)
        .map(|idx| format!("compute{}", idx + 1))
        .collect();
    cfg
}
