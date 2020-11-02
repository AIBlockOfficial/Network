//! App to run a compute node.

use clap::{App, Arg};
use naom::primitives::transaction_utils::{
    construct_payment_tx, construct_payment_tx_ins, construct_tx_hash,
};
use naom::primitives::{asset::Asset, transaction::TxConstructor};
use sodiumoxide::crypto::sign;
use std::collections::BTreeMap;
use system::{ComputeInterface, ComputeNode, Response};

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

    let node = ComputeNode::new(endpoint.parse().unwrap()).await?;

    println!("Started node at {}", node.address());

    // REQUEST HANDLING
    tokio::spawn({
        let mut node = node.clone();

        // Kick off with fake transactions
        let (pk, sk) = sign::gen_keypair();
        let t_hash = vec![0, 0, 0];
        let signature = sign::sign_detached(&hex::encode(t_hash.clone()).as_bytes(), &sk);

        let tx_const = TxConstructor {
            t_hash: hex::encode(t_hash),
            prev_n: 0,
            b_hash: hex::encode(vec![0]),
            signatures: vec![signature],
            pub_keys: vec![pk],
        };

        let tx_ins = construct_payment_tx_ins(vec![tx_const]);
        let payment_tx = construct_payment_tx(
            tx_ins,
            hex::encode(vec![0, 0, 0]),
            None,
            None,
            Asset::Token(4),
            4,
        );

        println!("");
        println!("Getting hash");
        println!("");

        let t_hash = construct_tx_hash(&payment_tx);

        let mut transactions = BTreeMap::new();
        transactions.insert(t_hash, payment_tx);
        let _resp = node.receive_transactions(transactions);
        println!("");
        println!("CURRENT BLOCK IN BIN: {:?}", node.current_block);
        println!("");

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
                        // BIG TODO: Send block to storage
                        //let _write_to_store = node.send_block_to_storage().await.unwrap();
                        let _flood = node.flood_block_found_notification().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "All transactions successfully added to tx pool",
                    }) => {
                        println!("Transactions received and processed successfully");
                        println!("CURRENT BLOCK: {:?}", node.current_block);
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
