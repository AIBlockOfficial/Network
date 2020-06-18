extern crate bit_vec;
extern crate chrono;
extern crate crypto;
extern crate hex;
extern crate merkletree;
extern crate rand;
extern crate rocksdb;
extern crate rug;
extern crate sha3;
extern crate sodiumoxide;

mod comms_handler;
mod compute;
mod constants;
mod db;
mod interfaces;
mod key_creation;
mod miner;
mod primitives;
mod script;
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

use sodiumoxide::crypto::sign;
use wallet::create_address;

fn main() {}
