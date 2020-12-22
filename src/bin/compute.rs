//! App to run a compute node.

use clap::{App, Arg};
use sodiumoxide::crypto::sign;
use system::configurations::{ComputeNodeConfig, ComputeNodeSetup};
use system::create_valid_transaction;
use system::{ComputeNode, ComputeRequest, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified compute node index from config file")
                .takes_value(true),
        )
        .get_matches();

    let (setup, config) = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");

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
        if let Some(index) = matches.value_of("index") {
            settings.set("compute_node_idx", index).unwrap();
        }

        let setup: ComputeNodeSetup = settings.clone().try_into().unwrap();
        let config: ComputeNodeConfig = settings.try_into().unwrap();
        (setup, config)
    };
    println!("Start node with config {:?}", config);
    let node = ComputeNode::new(config).await?;

    println!("Started node at {}", node.address());

    // RAFT HANDLING
    let raft_loop_handle = {
        let connect_all = node.connect_to_raft_peers();
        let raft_loop = node.raft_loop();
        tokio::spawn(async move {
            // Need to connect first so Raft messages can be sent.
            println!("Start connect to compute peers");
            let result = connect_all.await;
            println!("Peer connect complete, start Raft: {:?}", result);
            raft_loop.await;
            println!("Raft complete");
        })
    };

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;

        // Kick off with some transactions
        let initial_send_transactions = {
            let (pk, sk) = sign::gen_keypair();

            let transactions = setup
                .compute_initial_transactions
                .iter()
                .map(|transaction| {
                    create_valid_transaction(
                        &transaction.t_hash,
                        &transaction.receiver_address,
                        &pk,
                        &sk,
                    )
                })
                .collect();

            ComputeRequest::SendTransactions { transactions }
        };

        let storage_connected = {
            let result = node.connect_to_storage().await;
            println!("Storage connection: {:?}", result);
            result.is_ok()
        };

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "Received partition request successfully",
                    }) => {
                        let _flood = node.flood_rand_num_to_requesters().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Partition list is full",
                    }) => {
                        let _list_flood = node.flood_list_to_partition().await.unwrap();
                        node.partition_list = Vec::new();

                        let _block_flood = node.flood_block_to_partition().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received PoW successfully",
                    }) => {
                        if storage_connected && node.has_current_mined_block() {
                            println!("Send Block to storage");
                            println!("CURRENT MINED BLOCK: {:?}", node.current_mined_block);
                            let _write_to_store = node.send_block_to_storage().await.unwrap();
                        }
                        let _flood = node.flood_block_found_notification().await.unwrap();
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
                        println!("Block ready to be mined: {:?}", node.get_mining_block());
                        let _write_to_store = node.send_first_block_to_storage().await.unwrap();

                        // Only add transactions when they can be accepted
                        let resp = node.inject_next_event(
                            "0.0.0.0:6666".parse().unwrap(),
                            initial_send_transactions.clone(),
                        );
                        println!("initial transactions inject Response: {:?}", resp);
                    }
                    Ok(Response {
                        success: true,
                        reason: "Block committed",
                    }) => {
                        println!("Block ready to be mined: {:?}", node.get_mining_block());
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
        }
    });

    let (main, raft) = tokio::join!(main_loop_handle, raft_loop_handle);
    main.unwrap();
    raft.unwrap();
    Ok(())
}
