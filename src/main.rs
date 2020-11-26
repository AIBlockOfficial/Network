#![allow(dead_code)]

extern crate async_std;
extern crate chrono;
extern crate crypto;
extern crate hex;
extern crate rand;
extern crate rug;
extern crate sha3;
extern crate sodiumoxide;

mod comms_handler;
mod compute;
mod compute_raft;
mod configurations;
mod constants;
mod interfaces;
mod key_creation;
mod miner;
mod raft;
mod storage;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod unicorn;
mod user;
mod utils;
mod wallet;

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
#[cfg(features = "mock")]
pub(crate) use mock::Node;

fn main() {}
