//! App to run a compute node.

use clap::{App, Arg};
use config;
use naom::primitives::transaction::Transaction;
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

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;

        // Add initial utxo

        {
            let seed_utxo = setup
                .compute_seed_utxo
                .iter()
                .map(|hash| (hash.clone(), Transaction::new()))
                .collect();
            node.seed_utxo_set(seed_utxo);
        }

        // Kick off with some transactions
        {
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

            let resp = node.inject_next_event(
                "0.0.0.0:6666".parse().unwrap(),
                ComputeRequest::SendTransactions { transactions },
            );
            println!("initial transactions inject Response: {:?}", resp);
        }

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
                        reason: "Partition request received successfully",
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
                        reason: "All transactions successfully added to tx pool",
                    }) => {
                        println!("Transactions received and processed successfully");
                    }
                    Ok(Response {
                        success: true,
                        reason: "Block committed",
                    }) => {
                        println!("Block ready to be mined: {:?}", node.get_mining_block());
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

    let (result,) = tokio::join!(main_loop_handle);
    result.unwrap();
    Ok(())
}
