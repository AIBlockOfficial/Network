//! Test suite for the network functions.

use crate::interfaces::{ComputeRequest, Response};
use crate::test_utils::{Network, NetworkConfig};
use crate::utils::create_valid_transaction;
use naom::primitives::block::Block;
use naom::primitives::transaction::Transaction;
use sodiumoxide::crypto::sign;
use std::collections::BTreeMap;

#[tokio::test(threaded_scheduler)]
async fn create_block() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut network = Network::create_from_config(&NetworkConfig {
        initial_port: 10000,
        miner_nodes: Vec::new(),
        compute_nodes: vec!["compute".to_string()],
        storage_nodes: vec!["storage".to_string()],
        user_nodes: vec!["user".to_string()],
    })
    .await;

    let (seed_utxo, transactions, t_hash, tx) = {
        let intial_t_hash = "000000".to_owned();
        let receiver_addr = "000000".to_owned();

        let (pk, sk) = sign::gen_keypair();
        let (t_hash, payment_tx) =
            create_valid_transaction(&intial_t_hash, &receiver_addr, &pk, &sk);

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
    };

    // {
    //     let compute_node_addr = network.get_address("compute").unwrap().clone();
    //     let user = network.user("user").unwrap();

    //     let mut u = user.clone();
    //     tokio::spawn(async move {
    //         u.connect_to(compute_node_addr).await.unwrap();
    //         u.send_payment_to_compute(compute_node_addr, tx.clone())
    //             .await
    //             .unwrap();
    //     });
    // }

    // {
    //     let compute = network.compute("compute").unwrap();
    //     compute.seed_utxo_set(seed_utxo);
    //     match compute.handle_next_event().await {
    //         Some(Ok(Response {
    //             success: true,
    //             reason: "All transactions successfully added to tx pool",
    //         })) => (),
    //         other => panic!("Unexpected result: {:?}", other),
    //     }

    //     assert!(compute.current_block.is_none());
    //     compute.generate_block();

    //     let block_transactions = compute
    //         .current_block
    //         .as_ref()
    //         .map(|b| b.transactions.clone());
    //     assert_eq!(block_transactions, Some(vec![t_hash]));
    // }
}

#[tokio::test(threaded_scheduler)]
async fn proof_of_work() {
    let _ = tracing_subscriber::fmt::try_init();

    let miner_nodes = vec![
        "miner1".to_string(),
        "miner2".to_string(),
        "miner3".to_string(),
    ];
    let miners_count = miner_nodes.len();

    let mut network = Network::create_from_config(&NetworkConfig {
        initial_port: 10010,
        miner_nodes,
        compute_nodes: vec!["compute".to_string()],
        storage_nodes: vec!["storage".to_string()],
        user_nodes: Vec::new(),
    })
    .await;

    let compute_node_addr = network.get_address("compute").unwrap();
    let block = Block::new();

    for miner in network.miners_iter_mut() {
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

    {
        let comp = network.compute("compute").unwrap();

        for _i in 0..miners_count {
            match comp.handle_next_event().await {
                Some(Ok(Response {
                    success: true,
                    reason: "Received PoW successfully",
                })) => (),
                other => panic!("Unexpected result: {:?}", other),
            }
        }
    }

    // let _resp1 = compute_node.receive_pow(m1_address, pow1);
    // let _resp2 = compute_node.receive_commit(m1_address, miner1.last_pow);

    // let _resp3 = compute_node.receive_pow(m2_address, pow2);
    // let _resp4 = compute_node.receive_commit(m2_address, miner2.last_pow);

    // let _resp5 = compute_node.receive_pow(m3_address, pow3);
    // let _resp6 = compute_node.receive_commit(m3_address, miner3.last_pow);
}

#[tokio::test(threaded_scheduler)]
async fn send_block_to_storage() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut network = Network::create_from_config(&NetworkConfig {
        initial_port: 10020,
        miner_nodes: Vec::new(),
        compute_nodes: vec!["compute".to_string()],
        storage_nodes: vec!["storage".to_string()],
        user_nodes: Vec::new(),
    })
    .await;

    {
        let comp = network.compute("compute").unwrap();
        comp.current_block = Some(Block::new());

        let mut c = comp.clone();
        tokio::spawn(async move {
            c.connect_to_storage().await.unwrap();
            let _write_to_store = c.send_block_to_storage().await.unwrap();
        });
    }

    {
        let storage = network.storage("storage").unwrap();
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

    let user_nodes = vec!["user1".to_string(), "user2".to_string()];

    let mut network = Network::create_from_config(&NetworkConfig {
        initial_port: 10040,
        miner_nodes: Vec::new(),
        compute_nodes: vec!["compute".to_string()],
        storage_nodes: vec!["storage".to_string()],
        user_nodes,
    })
    .await;

    let compute_node_addr = network.get_address("compute").unwrap();
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
