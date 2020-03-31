//! Zenotta Network Protocol library.

mod comms_handler;
mod compute;
mod interfaces;
pub mod key_creation;
mod miner;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod unicorn;

pub use compute::ComputeNode;
pub use interfaces::{ComputeInterface, MinerInterface};
pub use miner::MinerNode;

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
#[cfg(features = "mock")]
pub(crate) use mock::Node;
