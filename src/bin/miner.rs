//! App to run a mining node.

use clap::{App, Arg};
use system::{MinerInterface, MinerNode, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Mining Node")
        .about("Runs a basic miner node.")
        .arg(
            Arg::with_name("ip")
                .long("ip")
                .value_name("ADDRESS")
                .help("Run the miner node at the given IP address (defaults to 0.0.0.0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Run the miner node at the given port number (defaults to 0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("connect")
                .short("c")
                .long("connect")
                .help("(Optional) Endpoint address of a compute node that the miner should connect to")
                .takes_value(true),
        )
        .get_matches();

    let endpoint = format!(
        "{}:{}",
        matches.value_of("ip").unwrap_or("0.0.0.0"),
        matches.value_of("port").unwrap_or("0")
    );

    let mut node = MinerNode::new(endpoint.parse().unwrap());
    let mut compute_node_connected = None;

    node.start().await?;

    println!("Started node at {}", node.address());

    if let Some(compute_node) = matches.value_of("connect") {
        compute_node_connected = Some(compute_node.parse().unwrap());

        // Connect to a compute node.
        node.connect_to(compute_node_connected.unwrap()).await?;
    }

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
                        reason: "Pre-block received successfully",
                    }) => {
                        println!("PRE-BLOCK RECEIVED");
                        let block = node.current_block.clone();
                        let block_pow = node
                            .generate_pow_for_block(endpoint.clone(), block)
                            .await
                            .unwrap();

                        let _send_pow = node
                            .send_pow(compute_node_connected.unwrap(), block_pow)
                            .await
                            .unwrap();
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
