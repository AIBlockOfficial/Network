use crate::script::OpCodes;

/*------- BLOCK CONSTANTS --------*/

// Maximum number of bytes that a block can contain
pub const MAX_BLOCK_SIZE: usize = 1000;

// Maximum number of transactions in a given tx_pool
pub const MAX_TX_POOL_SIZE: usize = 1000000;

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
