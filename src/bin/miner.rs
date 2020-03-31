//! App to run a mining node.

use clap::{App, Arg};
use system::{command_input_to_socket, MinerInterface, MinerNode};

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
        compute_node_connected = Some(compute_node.to_string());

        // Connect to a compute node.
        node.connect_to(compute_node.parse().unwrap()).await?;
    }

    tokio::spawn({
        let mut node = node.clone();

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);
            }
        }
    });

    // Send any requests to the compute node here

    // Send partition request
    let compute_socket_addr = command_input_to_socket(compute_node_connected.unwrap());
    println!("MINER ADDRESS: {:?}", node.address());
    let _result = node
        .send_partition_request(compute_socket_addr)
        .await
        .unwrap();

    loop {}
}
