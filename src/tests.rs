//! Test suite for the network functions.

use crate::interfaces::Response;
use crate::test_utils::{Network, NetworkConfig};

#[tokio::test(threaded_scheduler)]
async fn proof_of_work() {
    tracing_subscriber::fmt::init();

    let miner_nodes = vec![
        "miner1".to_string(),
        "miner2".to_string(),
        "miner3".to_string(),
    ];
    let miners_count = miner_nodes.len();

    let mut network = Network::create_from_config(NetworkConfig {
        miner_nodes,
        compute_nodes: vec!["compute".to_string()],
    });
    network.start();

    let compute_node_addr = network.get_address("compute").unwrap();

    for miner in network.miners_iter_mut() {
        let mut m = miner.clone();
        let mut m2 = miner.clone();

        tokio::spawn(async move {
            let (pow, _conn) = tokio::join!(
                m2.generate_pow_promise("123123".to_string()),
                m.connect_to(compute_node_addr)
            );
            m.send_pow(compute_node_addr, pow.unwrap()).await.unwrap();
        });
    }

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

    // let _resp1 = compute_node.receive_pow(m1_address, pow1);
    // let _resp2 = compute_node.receive_commit(m1_address, miner1.last_pow);

    // let _resp3 = compute_node.receive_pow(m2_address, pow2);
    // let _resp4 = compute_node.receive_commit(m2_address, miner2.last_pow);

    // let _resp5 = compute_node.receive_pow(m3_address, pow3);
    // let _resp6 = compute_node.receive_commit(m3_address, miner3.last_pow);
}
