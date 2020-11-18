//! App to run a mining node.

use clap::{App, Arg};
use std::time::SystemTime;
use system::configurations::MinerNodeConfig;
use system::{MinerNode, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Mining Node")
        .about("Runs a basic miner node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the miner node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified miner node index from config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compute_index")
                .long("compute_index")
                .help("Endpoint index of a compute node that the miner should connect to")
                .takes_value(true),
        )
        .get_matches();

    let config = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");

        settings.set_default("miner_node_idx", 0).unwrap();
        settings
            .merge(config::File::with_name(setting_file))
            .unwrap();
        if let Some(index) = matches.value_of("index") {
            settings.set("miner_node_idx", index).unwrap();
        }
        if let Some(index) = matches.value_of("compute_index") {
            settings.set("miner_compute_node_idx", index).unwrap();
        }

        let config: MinerNodeConfig = settings.try_into().unwrap();
        config
    };
    println!("Start node with config {:?}", config);

    let endpoint = format!(
        "{}:{}",
        matches.value_of("ip").unwrap_or("0.0.0.0"),
        matches.value_of("port").unwrap_or("0")
    );
    let compute_node_connected = config
        .miner_compute_node_idx
        .and_then(|idx| config.compute_nodes.get(idx))
        .map(|spec| spec.address.clone());

    let mut node = MinerNode::new(config).await?;
    println!("Started node at {}", node.address());

    if let Some(compute_node) = compute_node_connected {
        // Connect to a compute node.
        node.connect_to(compute_node).await?;
    }

    let now = SystemTime::now();

    tokio::spawn({
        let mut node = node.clone();

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "Received random number successfully",
                    }) => {
                        println!("RANDOM NUMBER RECEIVED: {:?}", node.rand_num.clone());
                        let participation_pow = node.generate_pow(endpoint.clone()).await.unwrap();

                        let _send_pow = node
                            .send_partition_pow(compute_node_connected.unwrap(), participation_pow)
                            .await
                            .unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received partition list successfully",
                    }) => {
                        println!("RECEIVED PARTITION LIST");
                    }
                    Ok(Response {
                        success: true,
                        reason: "Pre-block received successfully",
                    }) => {
                        println!("PRE-BLOCK RECEIVED");
                        let (block_pow, current_coinbase) = node
                            .generate_pow_for_block(node.current_block.clone())
                            .await
                            .unwrap();

                        node.current_coinbase = current_coinbase.clone();
                        match now.elapsed() {
                            Ok(elapsed) => {
                                println!("{}", elapsed.as_millis());
                            }
                            Err(e) => {
                                // an error occurred!
                                println!("Error: {:?}", e);
                            }
                        }

                        let _send_pow = node
                            .send_pow(compute_node_connected.unwrap(), block_pow, current_coinbase)
                            .await
                            .unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Block found",
                    }) => {
                        println!("Block nonce has been successfully found");
                    }
                    Ok(Response {
                        success: true,
                        reason: &_,
                    }) => {
                        println!("UNHANDLED RESPONSE TYPE");
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

    // Send any requests to the compute node here

    // Send partition request
    println!("MINER ADDRESS: {:?}", node.address());

    let _result = node
        .send_partition_request(compute_node_connected.unwrap())
        .await
        .unwrap();

    loop {}
}
