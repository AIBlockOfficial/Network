//! App to run a storage node.

use clap::{App, Arg};
use naom::primitives::asset::TokenAmount;
use system::configurations::{UserNodeConfig, UserNodeSetup};
use system::{loop_wait_connnect_to_peers_async, loops_re_connect_disconnect};
use system::{routes, Response, TransactionGen, UserNode};
use tracing::{debug, error, info};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Mining Node")
        .about("Runs a basic miner node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the user node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("initial_block_config")
                .long("initial_block_config")
                .help("Run the compute node using the given initial block config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("api_port")
                .long("api_port")
                .help("The port to run the http API from")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("amount")
                .short("a")
                .long("amount")
                .help("The amount of tokens to send to a recipient address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified user node index from config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compute_index")
                .long("compute_index")
                .help("Endpoint index of a compute node that the user should connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("peer_user_index")
                .long("peer_user_index")
                .help("Endpoint index of a peer user node that the user should connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("peer_user_connect")
                .long("peer_user_connect")
                .help("connect to a peer user node"),
        )
        .get_matches();

    let (setup, config) = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");
        let intial_block_setting_file = matches
            .value_of("initial_block_config")
            .unwrap_or("src/bin/initial_block.json");

        settings.set_default("api_port", 3000).unwrap();
        settings.set_default("user_node_idx", 0).unwrap();
        settings.set_default("user_compute_node_idx", 0).unwrap();
        settings.set_default("peer_user_node_idx", 0).unwrap();
        settings
            .set_default("user_setup_tx_in_max_count", 0)
            .unwrap();
        settings
            .merge(config::File::with_name(setting_file))
            .unwrap();
        settings
            .merge(config::File::with_name(intial_block_setting_file))
            .unwrap();

        if let Some(index) = matches.value_of("index") {
            settings.set("user_node_idx", index).unwrap();
            let mut db_mode = settings.get_table("user_db_mode").unwrap();
            if let Some(test_idx) = db_mode.get_mut("Test") {
                let index = {
                    let user_index_offset = 1000;
                    let index = index.parse::<usize>().unwrap() + user_index_offset;
                    index.to_string()
                };
                *test_idx = config::Value::new(None, index);
                settings.set("user_db_mode", db_mode).unwrap();
            }
        }

        if let Some(api_port) = matches.value_of("api_port") {
            settings.set("api_port", api_port).unwrap();
        }

        if let Some(index) = matches.value_of("compute_index") {
            settings.set("user_compute_node_idx", index).unwrap();
        }

        if let Some(index) = matches.value_of("peer_user_index") {
            settings.set("peer_user_node_idx", index).unwrap();
        }

        let setup: UserNodeSetup = settings.clone().try_into().unwrap();
        let config: UserNodeConfig = settings.try_into().unwrap();
        (setup, config)
    };
    println!("Starting node with config: {:?}", config);
    println!("Start node with setup {:?}", setup);
    println!();

    let peer_user_node_connected = if matches.is_present("peer_user_connect") {
        Some(
            config
                .user_nodes
                .get(config.peer_user_node_idx)
                .unwrap()
                .address,
        )
    } else {
        None
    };

    // Handle a payment amount
    let amount_to_send = match matches.value_of("amount").map(|a| a.parse::<u64>()) {
        None => TokenAmount(0),
        Some(Ok(v)) => TokenAmount(v as u64),
        Some(Err(e)) => panic!("Unable to pay with amount specified due to error: {:?}", e),
    };

    let user_node_idx = config.user_node_idx;
    let mut node = UserNode::new(config).await.unwrap();
    println!("Started node at {}", node.address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();
    let api_inputs = node.api_inputs();

    // PERMANENT CONNEXION/DISCONNECTION HANDLING
    let ((conn_loop_handle, stop_re_connect_tx), (disconn_loop_handle, stop_disconnect_tx)) = {
        let (re_connect, disconnect_test) =
            loops_re_connect_disconnect(node_conn.clone(), addrs_to_connect);

        (
            (tokio::spawn(re_connect.0), re_connect.1),
            (tokio::spawn(disconnect_test.0), disconnect_test.1),
        )
    };

    // Need to connect first so Raft messages can be sent.
    loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;

    // Send any requests here

    if let Some(peer_user_node) = peer_user_node_connected {
        println!("Connect to user address: {:?}", peer_user_node);
        // Connect to a peer user node for payment.
        node.connect_to(peer_user_node).await.unwrap();

        // Request a new payment address from peer user
        node.send_address_request(peer_user_node, amount_to_send)
            .await
            .unwrap();
    }

    // Generate automatic transactions
    let (tx_generator_active, tx_generator) = {
        let initial_transactions = setup
            .user_initial_transactions
            .get(user_node_idx)
            .cloned()
            .unwrap_or_default();
        (
            !initial_transactions.is_empty() && setup.user_setup_tx_in_max_count != 0,
            TransactionGen::new(initial_transactions),
        )
    };

    // send notification request
    if tx_generator_active {
        node.send_block_notification_request(node.compute_address())
            .await
            .unwrap();
    }

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;
        let mut tx_generator = tx_generator;

        async move {
            while let Some(response) = node.handle_next_event().await {
                debug!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "New address ready to be sent",
                    }) => {
                        debug!("Sending new payment address");
                        node.send_address_to_trading_peer().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Next payment transaction ready",
                    }) => {
                        node.send_next_payment_to_destinations(node.compute_address())
                            .await
                            .unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Block mining notified",
                    }) => {
                        // Process next transactions
                        tx_generator.commit_transactions(&node.last_block_notified.transactions);
                        let txs = tx_generator.make_all_transactions(
                            setup.user_setup_tx_in_per_tx,
                            setup.user_setup_tx_in_max_count,
                        );
                        if !txs.is_empty() {
                            info!("New Generated txs:{}", txs.len());
                        }
                        let chunk_size = setup.user_setup_tx_chunk_size.unwrap_or(usize::MAX);
                        for txs_chunk in txs.chunks(chunk_size) {
                            let txs_chunk: Vec<_> =
                                txs_chunk.iter().map(|(_, tx)| tx).cloned().collect();

                            debug!("New Generated txs:{:?}", txs_chunk);
                            if let Err(e) = node
                                .send_transactions_to_compute(node.compute_address(), txs_chunk)
                                .await
                            {
                                error!("Autogenerated tx not sent to {:?}", e);
                            }
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason,
                    }) => {
                        error!("UNHANDLED RESPONSE TYPE: {:?}", reason);
                    }
                    Ok(Response {
                        success: false,
                        reason,
                    }) => {
                        error!("WARNING: UNHANDLED RESPONSE TYPE FAILURE: {:?}", reason);
                    }
                    Err(error) => {
                        panic!("ERROR HANDLING RESPONSE: {:?}", error);
                    }
                }
            }
            stop_re_connect_tx.send(()).unwrap();
            stop_disconnect_tx.send(()).unwrap();
        }
    });

    // Warp API
    let warp_handle = tokio::spawn({
        println!("Warp API starting at port 3000");
        println!();

        let (db, node, api_addr) = api_inputs;

        async move {
            use warp::Filter;
            warp::serve(routes::wallet_info(db).or(routes::make_payment(node)))
                .run(api_addr)
                .await;
        }
    });

    let (main_result, warp_result, conn, disconn) = tokio::join!(
        main_loop_handle,
        warp_handle,
        conn_loop_handle,
        disconn_loop_handle
    );
    main_result.unwrap();
    warp_result.unwrap();
    conn.unwrap();
    disconn.unwrap();
}
