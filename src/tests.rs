//! Test suite for the network functions.

use crate::interfaces::Response;
use crate::test_utils::{Network, NetworkConfig};
use naom::primitives::block::Block;

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
        initial_port: 10000,
        miner_nodes,
        compute_nodes: vec!["compute".to_string()],
        storage_nodes: vec!["storage".to_string()],
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
        initial_port: 10010,
        miner_nodes: Vec::new(),
        compute_nodes: vec!["compute".to_string()],
        storage_nodes: vec!["storage".to_string()],
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
