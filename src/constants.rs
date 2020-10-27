use naom::script::OpCodes;

/*------- BLOCK CONSTANTS --------*/

// Maximum number of bytes that a block can contain
pub const MAX_BLOCK_SIZE: usize = 1000;

/*------- SCRIPT CONSTANTS -------*/

// Maximum number of bytes pushable to the stack
pub const MAX_SCRIPT_ELEMENT_SIZE: u16 = 520;

// Maximum number of non-push operations per script
pub const MAX_OPS_PER_SCRIPT: u8 = 201;

// Maximum number of public keys per multisig
pub const MAX_PUB_KEYS_PER_MULTISIG: u8 = 20;

// Maximum script length in bytes
pub const MAX_SCRIPT_SIZE: u16 = 10000;

// Maximum number of values on script interpreter stack
pub const MAX_STACK_SIZE: u16 = 1000;

// Threshold for lock_time: below this value it is interpreted as block number,
// otherwise as UNIX timestamp.
pub const LOCKTIME_THRESHOLD: u32 = 500000000; // Tue Nov 5 00:53:20 1985 UTC

// Maximum value that an opcode can be
pub const MAX_OPCODE: u8 = OpCodes::OP_NOP10 as u8;

/*------- STORAGE CONSTANTS -------*/

/// Path to chain DB
pub const DB_PATH: &'static str = "src/db/db";

/// Path to test net DB
pub const DB_PATH_TEST: &'static str = "test";

/// Path to live net DB
pub const DB_PATH_LIVE: &'static str = "live";

/// Path to wallet DB
pub const WALLET_PATH: &'static str = "src/wallet/wallet";

/*------- LIMIT CONSTANTS -------*/

/// Limit for the transaction pool per compute node
pub const TX_POOL_LIMIT: usize = 10000000;

/// Limit for the number of peers a compute node may have
pub const PEER_LIMIT: usize = 6;

/// Limit for the number of PoWs a compute node may have for UnicornShard creation
pub const UNICORN_LIMIT: usize = 5;

/// Limit for the number of miner nodes allowed in a given partition
pub const PARTITION_LIMIT: usize = 1;

/// Set the mining difficulty by number of required zeroes
pub const MINING_DIFFICULTY: usize = 1;

/// The size of a block in bytes
pub const BLOCK_SIZE: usize = 1000000;

/// The size of the block in transactions (approx)
pub const BLOCK_SIZE_IN_TX: usize = BLOCK_SIZE / 500;
