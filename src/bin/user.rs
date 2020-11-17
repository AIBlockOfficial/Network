//! App to run a storage node.

use clap::{App, Arg};
use system::{Response, UserNode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta User Node")
        .about("Runs a basic user node.")
        .arg(
            Arg::with_name("ip")
                .long("ip")
                .value_name("ADDRESS")
                .help("Run the user node at the given IP address (defaults to 0.0.0.0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .help("Run the user node at the given port number (defaults to 0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("connect-user")
                .short("cu")
                .long("connect-user")
                .help("(Optional) Endpoint address of another user node that the user node should connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("connect-compute")
                .short("cc")
                .long("connect-compute")
                .help("Endpoint address of a compute node that the user node should connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("amount")
                .short("a")
                .long("amount")
                .help("(Optional) Amount of Zeno token to send to pair user")
                .takes_value(true),
        )
        .get_matches();

    let endpoint = format!(
        "{}:{}",
        matches.value_of("ip").unwrap_or("0.0.0.0"),
        matches.value_of("port").unwrap_or("0")
    )
    .parse()
    .unwrap();
    let mut node = UserNode::new(endpoint).await?;

    let mut compute_node_connected = None;
    let mut pair_user_node_connected = None;
    let mut amount_to_send: u64 = 0;

    // Handle a payment amount
    if let Some(amount) = matches.value_of("amount") {
        amount_to_send = match amount.parse::<u64>() {
            Ok(v) => v,
            Err(e) => panic!(
                "Unable to payment with amount specified due to error: {:?}",
                e
            ),
        };
    }

    // Update payment amount as needed
    node.amount = amount_to_send;

    if let Some(compute_node) = matches.value_of("connect-compute") {
        compute_node_connected = Some(compute_node.parse().unwrap());

        // Connect to a compute node.
        node.connect_to(compute_node_connected.unwrap()).await?;
    }

    if let Some(pair_user_node) = matches.value_of("connect-user") {
        pair_user_node_connected = Some(pair_user_node.parse().unwrap());

        // Connect to a pairing user node.
        node.connect_to(pair_user_node_connected.unwrap()).await?;
        node.send_address_request(pair_user_node_connected.unwrap())
            .await
            .unwrap();
    }

    println!("Started node at {}", node.address());

    // REQUEST HANDLING
    tokio::spawn({
        let mut node = node.clone();

        async move {
            while let Some(response) = node.handle_next_event().await {
                println!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "New address ready to be sent",
                    }) => {
                        let _ = node
                            .send_address_to_peer(pair_user_node_connected.unwrap())
                            .await
                            .unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Next payment transaction successfully constructed",
                    }) => {
                        let _ = node
                            .send_payment_to_compute(
                                compute_node_connected.unwrap(),
                                node.next_payment.clone().unwrap(),
                            )
                            .await
                            .unwrap();
                        node.next_payment = None;

                        if node.return_payment.is_some() {
                            let _ = node
                                .send_payment_to_compute(
                                    compute_node_connected.unwrap(),
                                    node.return_payment.clone().unwrap(),
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

    loop {}
}
