//! # Art
//!
//! A library for modeling artistic concepts.
#![allow(dead_code)]

mod comms_handler;
mod compute;
pub mod configurations;
mod constants;
mod interfaces;
pub mod key_creation;
mod miner;
mod storage;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod unicorn;
mod user;
mod utils;
mod wallet;

pub use compute::ComputeNode;
pub use constants::PARTITION_LIMIT;
pub use interfaces::{ComputeInterface, MinerInterface, Response, StorageInterface, UseInterface};
pub use miner::MinerNode;
pub use storage::StorageNode;
pub use user::UserNode;
pub use utils::{command_input_to_socket, create_valid_transaction};

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
#[cfg(features = "mock")]
pub(crate) use mock::{Node, RingNode};
