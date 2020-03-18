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

use key_creation::KeyAgreement;

fn main() {
    // Key agreement input
    let mut first_addr = vec![0, 12, 3, 4, 5];
    let mut first_uni = vec![10, 51, 1, 20, 0];
    let mut first_nonce = vec![0, 0, 0, 0, 0];

    let mut second_addr = vec![9, 9, 9, 9, 9];
    let mut second_uni = vec![1, 1, 1, 1, 1];
    let mut second_nonce = vec![10, 9, 1, 0, 0];

    // Actual key agreement process
    let mut first = KeyAgreement::new(12, 10, 5);
    let mut second = KeyAgreement::new(12, 2, 1);

    // First round
    first.first_round(&mut first_addr, &mut first_uni, &mut first_nonce);
    second.first_round(&mut second_addr, &mut second_uni, &mut second_nonce);

    println!("FIRST: {:?}", first);
}
