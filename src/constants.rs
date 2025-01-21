pub use tw_chain::constants::*;

/*------- BLOCK CONSTANTS --------*/

/// Bit shifting value for reward issuance
pub const REWARD_ISSUANCE_VAL: u8 = 27;
pub const REWARD_SMOOTHING_VAL: u8 = 8;

/*------- ISSUANCE CONSTANTS --------*/

pub const ISSUANCE_INTERVALS: usize = 48;

/*------- CSPRNG USAGE NUMS -------*/

/// CSPRNG usage number for winning miner selection
pub const WINNING_MINER_UN: u128 = 3;

/// Usage number for participating miner selection
pub const MINER_PARTICIPATION_UN: u128 = 2;

/*------- STORAGE CONSTANTS -------*/

/// Key pointing to the current db version.
pub const DB_VERSION_KEY: &str = "DbVersionKey";

/// The constant prepending character for a block hash
pub const BLOCK_PREPEND: u8 = b'b';

/// The constant prepending character for named value
pub const NAMED_CONSTANT_PREPEND: u8 = b'n';

/// The constant for the named last block hash with NAMED_CONSTANT_PREPEND.
pub const LAST_BLOCK_HASH_KEY: &str = "nLastBlockHashKey";

/// The constant for the named indexed block hash with NAMED_CONSTANT_PREPEND.
/// The index number is 0-indexed hexadecimal value of 16 characters with leading 0.
pub const INDEXED_BLOCK_HASH_PREFIX_KEY: &str = "nIndexedBlockHashKey_";

/// The constant for the named indexed transaction hash with NAMED_CONSTANT_PREPEND.
/// The block index number is 0-indexed hexadecimal value of 16 characters with leading 0.
/// The transaction index number is 0-indexed hexadecimal value of 8 characters with leading 0.
/// The block and transaction number need to be separated by `_`.
pub const INDEXED_TX_HASH_PREFIX_KEY: &str = "nIndexedTxHashKey_";

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

/// Length of address PoW nonce
pub const ADDRESS_POW_NONCE_LEN: usize = 4;

/// Maximum length of PoW nonce
pub const POW_NONCE_MAX_LEN: usize = 32; // The size of a SHA3-256 digest

/// Length of random number generation for miner subselection
pub const POW_RNUM_SELECT: usize = 10;

/// Default limit on number of internal transactions for a miner node
pub const INTERNAL_TX_LIMIT: usize = 999;

/// Default limit on the number of concurrent API connections per node
pub const API_CONCURRENCY_LIMIT: usize = 100;

/// Maximum number of attempts to resend trigger messages before proposing to reset the mining pipeline
pub const RESEND_TRIGGER_MESSAGES_COMPUTE_LIMIT: usize = 5;

/// Limit for the transaction pool per mempool node
pub const TX_POOL_LIMIT: usize = 10_000_000;

/// Limit for the number of PoWs a mempool node may have for UnicornShard creation
pub const UNICORN_LIMIT: usize = 5;

/// Set the mining difficulty by number of required zeroes
pub const MINING_DIFFICULTY: usize = 1;

/// The size of a block in bytes
pub const BLOCK_SIZE: usize = 1_000_000;

/// The size of the block in transactions (approx)
pub const BLOCK_SIZE_IN_TX: usize = BLOCK_SIZE / 500;

/// Number of rounds for Miller Rabin primality testing
pub const MR_PRIME_ITERS: u32 = 15;

/// Number of old backups to keep before purging
pub const OLD_BACKUP_COUNT: usize = 5;

/// Coinbase locktime constant
/// TODO: Update to 5 once locktime tests are introduced
pub const COINBASE_MATURITY: u64 = if cfg!(test) { 0 } else { 100 };

// todo: actually set this!
//       note that this can be overriden through configuration,
//       which is handy for running locally or for low-difficulty test networks.
/// Block height at which ASERT DAA is activated
pub const ACTIVATION_HEIGHT_ASERT: u64 = 3;
/// Number of desired hashes submitted per block interval by miners
pub const ASERT_TARGET_HASHES_PER_BLOCK: u64 = 11;

const BCH_SECONDS_PER_BLOCK: u64 = 10 * 60;
const BCH_HALF_LIFE: u64 = 2 * 24 * 60 * 60;
/// ASERT algorithm constant for smoothing difficulty target adjustments over a number of blocks
///
/// Our mapping function projects number of hashes to seconds, so we feed ASERT_TARGET_HASHES_PER_BLOCK
/// to asert as the target block time. This constant adjusts the half life constant such that ASERT's
/// smoothing/spreading/smearing acts over the same number of blocks as it would in BCH.
pub const ASERT_HALF_LIFE: u64 =
    BCH_HALF_LIFE * ASERT_TARGET_HASHES_PER_BLOCK / BCH_SECONDS_PER_BLOCK;

/*------- TESTS -------*/

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn validate_key_prefixes_ordering() {
        //
        // Arrange
        //
        let first_possible_v2_block =
            "0000000000000000000000000000000000000000000000000000000000000000";
        let last_possible_v2_block =
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let first_possible_v3_block =
            "b0000000000000000000000000000000000000000000000000000000000000000";
        let last_possible_v3_block =
            "bffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

        let first_v2_initial_tx = "000001";
        let last_v2_initial_tx = "000009";
        let first_possible_v2_tx = "g0000000000000000000000000000000";
        let last_possible_v2_tx = "gfffffffffffffffffffffffffffffff";

        let expected_ordered_keys = vec![
            first_possible_v2_block,
            // Careful: Is within block range, outside normal Tx range
            first_v2_initial_tx,
            last_v2_initial_tx,
            // V3 block are within the range of v2 blocks
            first_possible_v3_block,
            last_possible_v3_block,
            last_possible_v2_block,
            // Normal transaction are after last block with different prefix
            first_possible_v2_tx,
            last_possible_v2_tx,
            // Named constants with same prefix
            INDEXED_BLOCK_HASH_PREFIX_KEY,
            LAST_BLOCK_HASH_KEY,
        ];

        //
        // Assert
        // Verify that some condition holds for the constant
        // so we can manipulate sorted collection of keys to iterate
        // over blocks or transactions.
        //
        assert_eq!(first_possible_v3_block.as_bytes()[0], BLOCK_PREPEND);
        assert_eq!(last_possible_v3_block.as_bytes()[0], BLOCK_PREPEND);
        assert_eq!(LAST_BLOCK_HASH_KEY.as_bytes()[0], NAMED_CONSTANT_PREPEND);
        assert_eq!(
            INDEXED_BLOCK_HASH_PREFIX_KEY.as_bytes()[0],
            NAMED_CONSTANT_PREPEND
        );

        assert_eq!(first_possible_v2_tx.as_bytes()[0], TX_PREPEND);
        assert_eq!(last_possible_v2_tx.as_bytes()[0], TX_PREPEND);

        assert_eq!(&expected_ordered_keys, &{
            let mut v = expected_ordered_keys.clone();
            v.sort_unstable();
            v
        });
    }
}
