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
                        reason: "Received partition list successfully",
                    }) => {
                        println!("RECEIVED PARTITION LIST");
                        println!("RIGHT: {:?}", node.right_index);
                        println!("LEFT: {:?}", node.left_index);

                        let _init_connect_right =
                            node.connect_to(node.right_index.unwrap()).await.unwrap();

                        println!("CONNECTED TO RIGHT");

                        if node.left_index != node.right_index {
                            let _init_connect_left =
                                node.connect_to(node.left_index.unwrap()).await.unwrap();
                        }

                        let _right_reply = node
                            .send_y_i_request(node.right_index.unwrap())
                            .await
                            .unwrap();
                        let _left_reply = node
                            .send_y_i_request(node.left_index.unwrap())
                            .await
                            .unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Pre-block received successfully",
                    }) => {
                        println!("PRE-BLOCK RECEIVED");
                        let block = node.current_block.clone();

                        // let block_pow = node
                        //     .generate_pow_for_block(endpoint.clone(), block)
                        //     .await
                        //     .unwrap();

                        // println!("BLOCK PoW: {:?}", block_pow);

                        // let _send_pow = node
                        //     .send_pow(compute_node_connected.unwrap(), block_pow)
                        //     .await
                        //     .unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received y_i request successfully",
                    }) => {
                        for entry in node.y_i_requests.clone() {
                            let y_i = node.key_creator.y_i.clone();
                            let _sent_y_i = node.send_y_i(entry, y_i).await.unwrap();
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received peer's y_i successfully",
                    }) => {
                        if !node.key_creator.left_y_i.is_empty()
                            && !node.key_creator.right_y_i.is_empty()
                        {
                            let left_y_i = node.key_creator.left_y_i.clone();
                            let right_y_i = node.key_creator.right_y_i.clone();

                            // Perform second round
                            node.key_creator.second_round(left_y_i, right_y_i);

                            // Flood peer info
                            let peer_info = node.key_creator.get_peer_info();
                            for entry in node.partition_list.clone() {
                                if entry != node.address() {
                                    let _send_peer_info = node
                                        .send_peer_info(entry, peer_info.clone())
                                        .await
                                        .unwrap();
                                }
                            }
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received peer info. Third round complete",
                    }) => {
                        // Perform third round
                        node.key_creator.third_round();

                        let k_j = node.key_creator.k_j.clone();

                        for entry in node.partition_list.clone() {
                            if entry != node.address() {
                                let _send_k_j = node.send_k_j(entry, k_j.clone()).await.unwrap();
                            }
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received peer k_j. All k_j values received",
                    }) => {
                        // Compute shared key
                        node.key_creator.compute_key();

                        println!("SHARED KEY: {:?}", node.key_creator.shared_key);

                        // TODO: Decrypt the pre-block, attach coinbase, mine, send block to compute
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
    node.generate_key_agreement();

    let _result = node
        .send_partition_request(compute_node_connected.unwrap())
        .await
        .unwrap();

    loop {}
}
