extern crate chrono;
extern crate hex;
extern crate rand;
extern crate rug;
extern crate sha3;
extern crate sodiumoxide;

mod comms_handler;
mod compute;
mod interfaces;
mod key_creation;
mod miner;
mod unicorn;

use std::net::SocketAddr;
use tokio::runtime::Runtime;
use tracing::{error, trace_span};
use tracing_futures::Instrument;

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
use compute::ComputeNode;
use interfaces::*;
use key_creation::KeyAgreement;
use miner::MinerNode;
#[cfg(features = "mock")]
pub(crate) use mock::Node;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    key_agreement();

    // Setup logging
    tracing_subscriber::fmt::init();

    // Setup runtime
    let mut runtime = Runtime::new()?;

    let cn_address = "127.0.0.1:8079".parse()?;
    let m1_address = "0.0.0.0:8080".parse()?;
    let m2_address = "0.0.0.0:8081".parse()?;
    let m3_address = "0.0.0.0:8082".parse()?;

    runtime.spawn(
        async move {
            let mut compute_node = ComputeNode::new(cn_address);
            let _ = compute_node.start().await;
        }
        .instrument(trace_span!("compute1")),
    );

    trace_span!("miner1").in_scope(|| {
        run_miner(
            &mut runtime,
            cn_address,
            MinerNode::new(m1_address),
            "A12g2340984jfk09",
        )
    });

    trace_span!("miner2").in_scope(|| {
        run_miner(
            &mut runtime,
            cn_address,
            MinerNode::new(m2_address),
            "B12g2340984jfk09",
        )
    });

    trace_span!("miner3").in_scope(|| {
        run_miner(
            &mut runtime,
            cn_address,
            MinerNode::new(m3_address),
            "C12g2340984jfk09",
        )
    });

    // let _resp1 = compute_node.receive_pow(m1_address, pow1);
    // let _resp2 = compute_node.receive_commit(m1_address, miner1.last_pow);

    // let _resp3 = compute_node.receive_pow(m2_address, pow2);
    // let _resp4 = compute_node.receive_commit(m2_address, miner2.last_pow);

    // let _resp5 = compute_node.receive_pow(m3_address, pow3);
    // let _resp6 = compute_node.receive_commit(m3_address, miner3.last_pow);

    // Run until we receive SIGTERM.
    loop {}
}

fn run_miner(
    runtime: &mut Runtime,
    cn_address: SocketAddr,
    mut miner: MinerNode,
    pow_address: &'static str,
) {
    runtime.spawn(
        async move {
            let pow = miner.generate_pow_promise(pow_address);

            match miner.connect_to(cn_address).await {
                Ok(()) => (),
                Err(error) => {
                    error!(
                        error = tracing::field::display(error),
                        ?cn_address,
                        "connect_to"
                    );
                    return;
                }
            }
            match miner.start().await {
                Ok(()) => (),
                Err(error) => error!(error = tracing::field::display(error), "start"),
            }

            miner.send_pow(cn_address, pow).await.unwrap();
        }
        .in_current_span(),
    );
}

fn key_agreement() {
    // Key agreement input
    let mut first_addr = vec![0, 12, 3, 4, 5];
    let mut first_uni = vec![10, 51, 1, 20, 0];
    let mut first_nonce = vec![0, 0, 0, 0, 0];

    let mut second_addr = vec![9, 9, 9, 9, 9];
    let mut second_uni = vec![1, 1, 1, 1, 1];
    let mut second_nonce = vec![10, 9, 1, 0, 0];

    let mut third_addr = vec![19, 9, 4, 1, 9];
    let mut third_uni = vec![1, 100, 17, 2, 4];
    let mut third_nonce = vec![10, 2, 1, 99, 0];

    let mut fourth_addr = vec![9, 9, 19, 19, 19];
    let mut fourth_uni = vec![1, 17, 91, 1, 21];
    let mut fourth_nonce = vec![10, 90, 15, 50, 20];

    let mut fifth_addr = vec![2, 2, 2, 9, 9];
    let mut fifth_uni = vec![1, 1, 1, 2, 2];
    let mut fifth_nonce = vec![10, 2, 2, 12, 0];

    let mut sixth_addr = vec![9, 6, 9, 6, 9];
    let mut sixth_uni = vec![6, 1, 6, 1, 6];
    let mut sixth_nonce = vec![1, 96, 61, 0, 60];

    let mut seventh_addr = vec![9, 59, 59, 59, 9];
    let mut seventh_uni = vec![1, 15, 51, 1, 11];
    let mut seventh_nonce = vec![109, 79, 61, 50, 50];

    // Actual key agreement process
    let mut first = KeyAgreement::new(1, 12, 10, 5);
    let mut second = KeyAgreement::new(2, 12, 2, 1);
    let mut third = KeyAgreement::new(3, 12, 2, 10);
    let mut fourth = KeyAgreement::new(4, 12, 10, 9);
    let mut fifth = KeyAgreement::new(5, 12, 4, 11);
    let mut sixth = KeyAgreement::new(6, 12, 5, 1);
    let mut seventh = KeyAgreement::new(7, 12, 2, 8);

    // First round
    first.first_round(&mut first_addr, &mut first_uni, &mut first_nonce);
    second.first_round(&mut second_addr, &mut second_uni, &mut second_nonce);
    third.first_round(&mut third_addr, &mut third_uni, &mut third_nonce);
    fourth.first_round(&mut fourth_addr, &mut fourth_uni, &mut fourth_nonce);
    fifth.first_round(&mut fifth_addr, &mut fifth_uni, &mut fifth_nonce);
    sixth.first_round(&mut sixth_addr, &mut sixth_uni, &mut sixth_nonce);
    seventh.first_round(&mut seventh_addr, &mut seventh_uni, &mut seventh_nonce);

    // Second round
    first.second_round(seventh.y_i.clone(), second.y_i.clone());
    second.second_round(first.y_i.clone(), third.y_i.clone());
    third.second_round(second.y_i.clone(), fourth.y_i.clone());
    fourth.second_round(third.y_i.clone(), fifth.y_i.clone());
    fifth.second_round(fourth.y_i.clone(), sixth.y_i.clone());
    sixth.second_round(fifth.y_i.clone(), seventh.y_i.clone());
    seventh.second_round(sixth.y_i.clone(), first.y_i.clone());

    // Get peer infos
    let first_peer = first.get_peer_info();
    let second_peer = second.get_peer_info();
    let third_peer = third.get_peer_info();
    let fourth_peer = fourth.get_peer_info();
    let fifth_peer = fifth.get_peer_info();
    let sixth_peer = sixth.get_peer_info();
    let seventh_peer = seventh.get_peer_info();

    first.receive_peer_info(2, second_peer.clone());
    first.receive_peer_info(3, third_peer.clone());
    first.receive_peer_info(4, fourth_peer.clone());
    first.receive_peer_info(5, fifth_peer.clone());
    first.receive_peer_info(6, sixth_peer.clone());
    first.receive_peer_info(7, seventh_peer.clone());

    second.receive_peer_info(1, first_peer.clone());
    second.receive_peer_info(3, third_peer.clone());
    second.receive_peer_info(4, fourth_peer.clone());
    second.receive_peer_info(5, fifth_peer.clone());
    second.receive_peer_info(6, sixth_peer.clone());
    second.receive_peer_info(7, seventh_peer.clone());

    third.receive_peer_info(1, first_peer.clone());
    third.receive_peer_info(2, second_peer.clone());
    third.receive_peer_info(4, fourth_peer.clone());
    third.receive_peer_info(5, fifth_peer.clone());
    third.receive_peer_info(6, sixth_peer.clone());
    third.receive_peer_info(7, seventh_peer.clone());

    fourth.receive_peer_info(1, first_peer.clone());
    fourth.receive_peer_info(3, third_peer.clone());
    fourth.receive_peer_info(2, second_peer.clone());
    fourth.receive_peer_info(5, fifth_peer.clone());
    fourth.receive_peer_info(6, sixth_peer.clone());
    fourth.receive_peer_info(7, seventh_peer.clone());

    fifth.receive_peer_info(1, first_peer.clone());
    fifth.receive_peer_info(3, third_peer.clone());
    fifth.receive_peer_info(4, fourth_peer.clone());
    fifth.receive_peer_info(2, second_peer.clone());
    fifth.receive_peer_info(6, sixth_peer.clone());
    fifth.receive_peer_info(7, seventh_peer.clone());

    sixth.receive_peer_info(1, first_peer.clone());
    sixth.receive_peer_info(3, third_peer.clone());
    sixth.receive_peer_info(4, fourth_peer.clone());
    sixth.receive_peer_info(2, second_peer.clone());
    sixth.receive_peer_info(5, fifth_peer.clone());
    sixth.receive_peer_info(7, seventh_peer.clone());

    seventh.receive_peer_info(1, first_peer.clone());
    seventh.receive_peer_info(3, third_peer.clone());
    seventh.receive_peer_info(4, fourth_peer.clone());
    seventh.receive_peer_info(2, second_peer.clone());
    seventh.receive_peer_info(6, sixth_peer.clone());
    seventh.receive_peer_info(5, fifth_peer.clone());

    // Third round
    first.third_round();
    second.third_round();
    third.third_round();
    fourth.third_round();
    fifth.third_round();
    sixth.third_round();
    seventh.third_round();

    // Send Kjs
    first.receive_k_j(2, second.k_j.clone());
    first.receive_k_j(3, third.k_j.clone());
    first.receive_k_j(4, fourth.k_j.clone());
    first.receive_k_j(5, fifth.k_j.clone());
    first.receive_k_j(6, sixth.k_j.clone());
    first.receive_k_j(7, seventh.k_j.clone());

    second.receive_k_j(1, first.k_j.clone());
    second.receive_k_j(7, seventh.k_j.clone());
    second.receive_k_j(3, third.k_j.clone());
    second.receive_k_j(4, fourth.k_j.clone());
    second.receive_k_j(5, fifth.k_j.clone());
    second.receive_k_j(6, sixth.k_j.clone());

    third.receive_k_j(1, first.k_j.clone());
    third.receive_k_j(7, seventh.k_j.clone());
    third.receive_k_j(2, second.k_j.clone());
    third.receive_k_j(4, fourth.k_j.clone());
    third.receive_k_j(5, fifth.k_j.clone());
    third.receive_k_j(6, sixth.k_j.clone());

    fourth.receive_k_j(1, first.k_j.clone());
    fourth.receive_k_j(7, seventh.k_j.clone());
    fourth.receive_k_j(3, third.k_j.clone());
    fourth.receive_k_j(2, second.k_j.clone());
    fourth.receive_k_j(5, fifth.k_j.clone());
    fourth.receive_k_j(6, sixth.k_j.clone());

    fifth.receive_k_j(1, first.k_j.clone());
    fifth.receive_k_j(7, seventh.k_j.clone());
    fifth.receive_k_j(3, third.k_j.clone());
    fifth.receive_k_j(2, second.k_j.clone());
    fifth.receive_k_j(4, fourth.k_j.clone());
    fifth.receive_k_j(6, sixth.k_j.clone());

    sixth.receive_k_j(1, first.k_j.clone());
    sixth.receive_k_j(7, seventh.k_j.clone());
    sixth.receive_k_j(3, third.k_j.clone());
    sixth.receive_k_j(2, second.k_j.clone());
    sixth.receive_k_j(5, fifth.k_j.clone());
    sixth.receive_k_j(4, fourth.k_j.clone());

    seventh.receive_k_j(1, first.k_j.clone());
    seventh.receive_k_j(4, fourth.k_j.clone());
    seventh.receive_k_j(3, third.k_j.clone());
    seventh.receive_k_j(2, second.k_j.clone());
    seventh.receive_k_j(5, fifth.k_j.clone());
    seventh.receive_k_j(6, sixth.k_j.clone());

    // Finally compute key
    first.compute_key();
    second.compute_key();
    third.compute_key();
    fourth.compute_key();
    fifth.compute_key();
    sixth.compute_key();
    seventh.compute_key();

    println!("First: {:?}", first.shared_key);
    println!("Second: {:?}", second.shared_key);
    println!("Third: {:?}", third.shared_key);
    println!("fourth: {:?}", fourth.shared_key);
    println!("fifth: {:?}", fifth.shared_key);
    println!("sixth: {:?}", sixth.shared_key);
    println!("seventh: {:?}", seventh.shared_key);
}
