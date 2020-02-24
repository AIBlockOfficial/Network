extern crate chrono;
extern crate sha3;
extern crate rand;
extern crate sodiumoxide;

mod miner;
mod unicorn;
mod compute;
mod interfaces;
mod comms_handler;

use sha3::Sha3_256;
use crate::sha3::Digest;
use interfaces::*;

use compute::ComputeNode;
use miner::MinerNode;


fn main() {
    let mut compute_node = ComputeNode::new("0.0.0.0:8079");

    let mut miner1 = MinerNode::new("0.0.0.0:8080");
    let mut miner2 = MinerNode::new("0.0.0.0:8081");
    let mut miner3 = MinerNode::new("0.0.0.0:8082");

    let pow1 = miner1.generate_pow_promise("A12g2340984jfk09");
    let pow2 = miner2.generate_pow_promise("B12g2340984jfk09");
    let pow3 = miner3.generate_pow_promise("C12g2340984jfk09");

    let _resp1 = compute_node.receive_pow(miner1.comms_address, pow1);
    let _resp2 = compute_node.receive_commit(miner1.comms_address, miner1.last_pow);

    let _resp3 = compute_node.receive_pow(miner2.comms_address, pow2);
    let _resp4 = compute_node.receive_commit(miner2.comms_address, miner2.last_pow);

    let _resp5 = compute_node.receive_pow(miner3.comms_address, pow3);
    let _resp6 = compute_node.receive_commit(miner3.comms_address, miner3.last_pow);

    println!("{:?}", compute_node.unicorn_list);
}
