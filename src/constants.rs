pub use naom::constants::*;

/*------- NETWORK CONSTANTS --------*/
pub const NETWORK_VERSION: u32 = 1;
pub const NETWORK_VERSION_SERIALIZED: &[u8] = b"1";

/*------- BLOCK CONSTANTS --------*/

// Bit shifting value for reward issuance
pub const REWARD_ISSUANCE_VAL: u8 = 25;

/*------- STORAGE CONSTANTS -------*/

/// Key pointing to the current db version.
pub const DB_VERSION_KEY: &str = "DbVersionKey";

/// The constant prepending character for a block hash
pub const BLOCK_PREPEND: char = 'b';

/// Path to chain DB
pub const DB_PATH: &str = "src/db/db";

/// Path to test net DB
pub const DB_PATH_TEST: &str = "test";

/// Path to live net DB
pub const DB_PATH_LIVE: &str = "live";

/// Path to wallet DB
pub const WALLET_PATH: &str = "src/wallet/wallet";

/// Key for local addresses in wallet
pub const KNOWN_ADDRESS_KEY: &str = "a";

/// Key for a running total of wallet funds
pub const FUND_KEY: &str = "f";

///Key for storing encapsulation details
pub const DATA_ENCAPSULATION_KEY: &str = "e";

/// Path to sanction list
pub const SANC_LIST_PROD: &str = "src/db/sanc_list.json";

/// Path to test sanction list
pub const SANC_LIST_TEST: &str = "src/db/sanc_list_test.json";

/*------- LIMIT CONSTANTS -------*/

/// Limit for the transaction pool per compute node
pub const TX_POOL_LIMIT: usize = 10_000_000;

/// Limit for the number of peers a compute node may have
pub const PEER_LIMIT: usize = 50;

/// Limit for the number of PoWs a compute node may have for UnicornShard creation
pub const UNICORN_LIMIT: usize = 5;

/// Set the mining difficulty by number of required zeroes
pub const MINING_DIFFICULTY: usize = 1;

/// The size of a block in bytes
pub const BLOCK_SIZE: usize = 1_000_000;

/// The size of the block in transactions (approx)
pub const BLOCK_SIZE_IN_TX: usize = BLOCK_SIZE / 500;

/// Number of rounds for Miller Rabin primality testing
pub const MR_PRIME_ITERS: u32 = 15;
