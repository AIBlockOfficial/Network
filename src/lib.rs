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
mod utils;
mod wallet;

pub use compute::ComputeNode;
pub use constants::PARTITION_LIMIT;
pub use interfaces::{ComputeInterface, MinerInterface, Response, StorageInterface};
pub use miner::MinerNode;
pub use storage::StorageNode;
pub use utils::command_input_to_socket;

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
#[cfg(features = "mock")]
pub(crate) use mock::{Node, RingNode};
