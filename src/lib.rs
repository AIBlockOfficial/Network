//! # Art
//!
//! A library for modeling artistic concepts.
#![allow(dead_code)]

mod active_raft;
mod api;
mod comms_handler;
mod compute;
mod compute_raft;
pub mod configurations;
mod constants;
mod db_utils;
mod hash_block;
mod interfaces;
pub mod key_creation;
mod miner;
mod raft;
mod raft_store;
mod raft_util;
mod storage;
mod storage_raft;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod transaction_gen;
mod unicorn;
mod user;
mod utils;
mod wallet;

pub use api::routes;
pub use compute::ComputeNode;
pub use constants::SANC_LIST_PROD;
pub use db_utils::get_db_options;
pub use interfaces::{ComputeRequest, MinerInterface, Response, StorageInterface, UseInterface};
pub use miner::MinerNode;
pub use storage::StorageNode;
pub use transaction_gen::TransactionGen;
pub use user::UserNode;
pub use utils::{
    command_input_to_socket, create_and_save_fake_to_wallet, create_valid_transaction,
    get_sanction_addresses, loop_connnect_to_peers_async, loop_wait_connnect_to_peers_async,
    loops_re_connect_disconnect,
};

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
#[cfg(features = "mock")]
pub(crate) use mock::{Node, RingNode};
