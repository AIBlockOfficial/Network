//! App to run a compute node.

use clap::{App, Arg};
use futures::future::join_all;
use std::time::Duration;
use system::configurations::{ComputeNodeConfig, ComputeNodeSetup};
use system::{
    create_valid_transaction_with_info, get_sanction_addresses, loop_connnect_to_peers_async,
    loop_wait_connnect_to_peers_async, SANC_LIST_PROD,
};
use system::{ComputeNode, ComputeRequest, Response};
use tracing::error;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Compute Node")
        .about("Runs a basic compute node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the compute node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("initial_block_config")
                .long("initial_block_config")
                .help("Run the compute node using the given initial block config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified compute node index from config file")
                .takes_value(true),
        )
        .get_matches();

    let (setup, mut config) = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");
        let intial_block_setting_file = matches
            .value_of("initial_block_config")
            .unwrap_or("src/bin/initial_block.json");

        settings
            .set_default("sanction_list", Vec::<String>::new())
            .unwrap();
        settings.set_default("jurisdiction", "US").unwrap();
        settings.set_default("compute_node_idx", 0).unwrap();
        settings.set_default("compute_raft", 0).unwrap();
        settings
            .set_default("compute_raft_tick_timeout", 10)
            .unwrap();
        settings
            .set_default("compute_transaction_timeout", 100)
            .unwrap();

        settings
            .merge(config::File::with_name(setting_file))
            .unwrap();
        settings
            .merge(config::File::with_name(intial_block_setting_file))
            .unwrap();
        if let Some(index) = matches.value_of("index") {
            settings.set("compute_node_idx", index).unwrap();
        }

        let setup: ComputeNodeSetup = settings.clone().try_into().unwrap();
        let config: ComputeNodeConfig = settings.try_into().unwrap();
        (setup, config)
    };
    println!("Start node with config {:?}", config);
    println!("Start node with setup {:?}", setup);

    config.sanction_list = get_sanction_addresses(SANC_LIST_PROD.to_string(), &config.jurisdiction);
    let compute_node_idx = config.compute_node_idx;
    let node = ComputeNode::new(config).await.unwrap();

    println!("Started node at {}", node.address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = {
        let (node_conn, mut addrs_to_connect, all_addr_raft) = node.connect_to_raft_peers();
        let mut expected_connected_addrs = all_addr_raft;
        addrs_to_connect.push(node.storage_addr);
        expected_connected_addrs.push(node.storage_addr);
        (node_conn, addrs_to_connect, expected_connected_addrs)
    };

    // PERMANENT CONNEXION HANDLING
    let (conn_loop_handle, stop_re_connect_tx) = {
        let (stop_re_connect_tx, stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let node_conn = node_conn.clone();
        (
            tokio::spawn(async move {
                println!("Start connect to compute peers");
                loop_connnect_to_peers_async(node_conn, addrs_to_connect, Some(stop_re_connect_rx))
                    .await;
                println!("Reconnect complete");
            }),
            stop_re_connect_tx,
        )
    };

    // TEST DIS-CONNECTION HANDLING
    let (disconn_loop_handle, stop_disconnect_tx) = {
        let (stop_re_connect_tx, mut stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let disconnect = format!("disconnect_{}", node.address().port());
        let mut node_conn = node_conn.clone();
        (
            tokio::spawn(async move {
                println!("Start mode input check");
                loop {
                    tokio::select! {
                        _ = tokio::time::delay_for(Duration::from_millis(500)) => {
                            if std::path::Path::new(&disconnect).exists() {
                                join_all(node_conn.disconnect_all().await).await;
                            }
                        },
                        _ = &mut stop_re_connect_rx => break,
                    };
                }
                println!("Complete mode input check");
            }),
            stop_re_connect_tx,
        )
    };

    // Need to connect first so Raft messages can be sent.
    loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;
    println!("Raft and Storage connection complete");

    // RAFT HANDLING
    let raft_loop_handle = {
        let raft_loop = node.raft_loop();
        tokio::spawn(async move {
            println!("Peer connect complete, start Raft");
            raft_loop.await;
            println!("Raft complete");
        })
    };

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;

        // Kick off with some transactions
        let initial_send_transactions = setup
            .compute_initial_transactions
            .get(compute_node_idx)
            .map(|tx_seeds| {
                let transactions = tx_seeds
                    .iter()
                    .map(create_valid_transaction_with_info)
                    .collect();

                ComputeRequest::SendTransactions { transactions }
            });

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "Received partition request successfully",
                    }) => {}
                    Ok(Response {
                        success: true,
                        reason: "Received first full partition request",
                    }) => {
                        node.propose_initial_uxto_set().await;
                    }
                    Ok(Response {
                        success: true,
                        reason: "Partition list is full",
                    }) => {
                        node.flood_list_to_partition().await.unwrap();
                        node.flood_block_to_partition().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received PoW successfully",
                    }) => {
                        println!("Send Block to storage");
                        println!("CURRENT MINED BLOCK: {:?}", node.current_mined_block);
                        if let Err(e) = node.send_block_to_storage().await {
                            error!("Block not sent to storage {:?}", e);
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: "Transactions added to tx pool",
                    }) => {
                        println!("Transactions received and processed successfully");
                    }
                    Ok(Response {
                        success: true,
                        reason: "First Block committed",
                    }) => {
                        println!("First Block ready to mine: {:?}", node.get_mining_block());
                        node.flood_rand_num_to_requesters().await.unwrap();

                        if let Some(txs) = &initial_send_transactions {
                            // Only add transactions when they can be accepted
                            let resp = node
                                .inject_next_event("0.0.0.0:6666".parse().unwrap(), txs.clone());
                            println!("initial transactions inject Response: {:?}", resp);
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: "Block committed",
                    }) => {
                        println!("Block ready to mine: {:?}", node.get_mining_block());
                        node.send_bf_notification().await.unwrap();
                        node.flood_rand_num_to_requesters().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Transactions committed",
                    }) => {
                        println!("Transactions ready to be used in next block");
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received block stored",
                    }) => {
                        println!("Block info received from storage: ready to generate block");
                    }
                    Ok(Response {
                        success: true,
                        reason: &_,
                    }) => {
                        println!("UNHANDLED RESPONSE TYPE: {:?}", response.unwrap().reason);
                    }
                    Ok(Response {
                        success: false,
                        reason: &_,
                    }) => {
                        println!("WARNING: UNHANDLED RESPONSE TYPE FAILURE");
                    }
                    Err(error) => {
                        panic!("ERROR HANDLING RESPONSE: {:?}", error);
                    }
                }
            }
            node.close_raft_loop().await;
            stop_re_connect_tx.send(()).unwrap();
            stop_disconnect_tx.send(()).unwrap();
        }
    });

    let (main, raft, conn, disconn) = tokio::join!(
        main_loop_handle,
        raft_loop_handle,
        conn_loop_handle,
        disconn_loop_handle
    );
    main.unwrap();
    raft.unwrap();
    conn.unwrap();
    disconn.unwrap();
}
