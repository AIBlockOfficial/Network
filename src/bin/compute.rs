//! App to run a compute node.

use clap::{App, Arg};
use system::{ComputeInterface, ComputeNode, Response, PARTITION_LIMIT};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Compute Node")
        .about("Runs a basic compute node.")
        .arg(
            Arg::with_name("ip")
                .long("ip")
                .value_name("ADDRESS")
                .help("Run the compute node at the given IP address (defaults to 0.0.0.0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Run the compute node at the given port number (defaults to 0)")
                .takes_value(true),
        )
        .get_matches();

    let endpoint = format!(
        "{}:{}",
        matches.value_of("ip").unwrap_or("0.0.0.0"),
        matches.value_of("port").unwrap_or("0")
    );

    let mut node = ComputeNode::new(endpoint.parse().unwrap());

    node.start().await?;

    println!("Started node at {}", node.address());

    let partition_response = Response {
        success: true,
        reason: "Partition request received successfully",
    };

    tokio::spawn({
        let mut node = node.clone();

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);

                if response.unwrap() == partition_response
                    && node.partition_list.len() == PARTITION_LIMIT
                {
                    let _flood = node.flood_partition_list().await.unwrap();
                }
            }
        }
    });

    loop {}
}
