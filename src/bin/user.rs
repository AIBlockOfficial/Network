//! App to run a storage node.

use clap::{App, Arg};
use naom::primitives::asset::TokenAmount;
use std::time::Duration;
use system::configurations::UserNodeConfig;
use system::{routes, Response, UserNode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            Arg::with_name("compute_connect")
                .long("compute_connect")
                .help("connect to the compute node"),
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

    let config = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");

        settings.set_default("api_port", 3000).unwrap();
        settings.set_default("user_node_idx", 0).unwrap();
        settings.set_default("user_compute_node_idx", 0).unwrap();
        settings.set_default("peer_user_node_idx", 0).unwrap();
        settings
            .merge(config::File::with_name(setting_file))
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

        if let Some(index) = matches.value_of("compute_index") {
            settings.set("user_compute_node_idx", index).unwrap();
        }

        if let Some(index) = matches.value_of("peer_user_index") {
            settings.set("peer_user_node_idx", index).unwrap();
        }

        let config: UserNodeConfig = settings.try_into().unwrap();
        config
    };
    println!("Starting node with config: {:?}", config);
    println!();

    let compute_node_connected = if matches.is_present("compute_connect") {
        Some(
            config
                .compute_nodes
                .get(config.user_compute_node_idx)
                .unwrap()
                .address,
        )
    } else {
        None
    };

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
        Some(Ok(v)) => TokenAmount(v),
        Some(Err(e)) => panic!("Unable to pay with amount specified due to error: {:?}", e),
    };

    let mut node = UserNode::new(config).await?;
    println!("Started node at {}", node.address());

    if let Some(compute_node) = compute_node_connected {
        // Connect to a compute node.
        while let Err(e) = node.connect_to(compute_node).await {
            println!("Compute connection error: {:?}", e);
            tokio::time::delay_for(Duration::from_millis(500)).await;
        }
        println!("Compute connection complete");
    }

    if let Some(peer_user_node) = peer_user_node_connected {
        println!("ADDRESS: {:?}", peer_user_node);
        // Connect to a peer user node for payment.
        node.connect_to(peer_user_node).await?;

        // Request a new payment address from peer user
        node.send_address_request(peer_user_node, amount_to_send)
            .await
            .unwrap();
    }

    let api_inputs = (node.wallet_db.clone(), node.node.clone());

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "New address ready to be sent",
                    }) => {
                        println!("Sending new payment address");
                        println!();

                        node.send_address_to_trading_peer().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Next payment transaction ready",
                    }) => {
                        let next_payment = node.next_payment.take().unwrap();

                        // Send the payment to compute node
                        node.send_payment_to_compute(
                            compute_node_connected.unwrap(),
                            next_payment.clone(),
                        )
                        .await
                        .unwrap();

                        // Send the payment to the receiving user
                        if let Some(peer_user) = peer_user_node_connected {
                            node.send_payment_to_receiver(peer_user, next_payment)
                                .await
                                .unwrap();
                        }

                        if let Some(r_payment) = node.return_payment.clone() {
                            // Handle return payment construction
                            node.construct_return_payment_tx(r_payment.tx_in, r_payment.amount)
                                .await
                                .unwrap();
                            let return_payment = node.return_payment.clone();

                            node.send_payment_to_compute(
                                compute_node_connected.unwrap(),
                                return_payment.unwrap().transaction,
                            )
                            .await
                            .unwrap();
                            node.return_payment = None;
                        }
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
        }
    });

    // Warp API
    let warp_handle = tokio::spawn({
        println!("Warp API starting at port 3000");
        println!();

        let (db, node) = api_inputs;

        async {
            use warp::Filter;
            warp::serve(routes::wallet_info(db).or(routes::make_payment(node)))
                .run(([127, 0, 0, 1], 3000))
                .await;
        }
    });

    let (main_result, warp_result) = tokio::join!(main_loop_handle, warp_handle);
    main_result.unwrap();
    warp_result.unwrap();

    Ok(())
}
