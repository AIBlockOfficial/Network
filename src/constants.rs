#![allow(unused)]

/// Limit for the number of peers a compute node may have
pub const PEER_LIMIT: usize = 6;

/// Limit for the number of PoWs a compute node may have for UnicornShard creation
pub const UNICORN_LIMIT: usize = 5;

/// Limit for the number of miner nodes allowed in a given partition
pub const PARTITION_LIMIT: usize = 2;

/// Set the mining difficulty by number of required zeroes
pub const MINING_DIFFICULTY: usize = 1;

/// The size of a block in bytes
pub const BLOCK_SIZE: usize = 1000000;

/// Key agreement generator value
pub const KA_GENERATOR: u64 = 12;
