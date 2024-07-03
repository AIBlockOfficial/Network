pub mod cpu;
pub mod opengl;
pub mod vulkan;

use std::fmt;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use tw_chain::primitives::block::BlockHeader;
use crate::asert::CompactTarget;
use crate::constants::{ADDRESS_POW_NONCE_LEN, MINING_DIFFICULTY, POW_NONCE_MAX_LEN};
use crate::interfaces::ProofOfWork;
use crate::miner_pow::cpu::CpuMiner;
use crate::utils::{split_range_into_blocks, UnitsPrefixed};

pub const SHA3_256_BYTES: usize = 32;
pub const BLOCK_HEADER_MAX_BYTES: usize = 1024;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum PoWDifficulty {
    LeadingZeroBytes {
        leading_zeroes: usize,
    },
    TargetHashAlwaysPass,
    TargetHash {
        target_hash: [u8; SHA3_256_BYTES],
    },
}

fn find_nonce_location<T: Clone + serde::Serialize>(
    object: &T,
    nonce_setter: impl Fn(&mut T, Vec<u8>) -> (),
    nonce_length: usize,
) -> (Box<[u8]>, Box<[u8]>) {
    assert_ne!(nonce_length, 0, "nonce_length may not be 0!");

    let mut object = object.clone();

    let nonce_00 = vec![0x00u8; nonce_length];
    let nonce_ff = vec![0xFFu8; nonce_length];

    nonce_setter(&mut object, nonce_00.clone());
    let serialized_00 = bincode::serialize(&object).unwrap();
    nonce_setter(&mut object, nonce_ff.clone());
    let serialized_ff = bincode::serialize(&object).unwrap();

    assert_ne!(serialized_00, serialized_ff,
               "changing the nonce didn't affect the serialized object?!?");
    assert_eq!(serialized_00.len(), serialized_ff.len(),
               "changing the nonce affected the object's serialized length?!?");

    // find the index at which the two headers differ
    let nonce_offset = (0..serialized_00.len())
        .find(|offset| serialized_00[*offset] != serialized_ff[*offset])
        .expect("the serialized objects are not equal, but are equal at every index?!?");

    assert_eq!(&serialized_00.as_slice()[nonce_offset..nonce_offset + 4], nonce_00.as_slice(),
               "serialized object with nonce 0x00000000 has different bytes at presumed nonce offset!");
    assert_eq!(&serialized_ff.as_slice()[nonce_offset..nonce_offset + 4], nonce_ff.as_slice(),
               "serialized object with nonce 0xFFFFFFFF has different bytes at presumed nonce offset!");

    let leading_bytes = serialized_00[..nonce_offset].into();
    let trailing_bytes = serialized_00[nonce_offset + nonce_length..].into();
    (leading_bytes, trailing_bytes)
}

fn extract_target_difficulty(
    difficulty: &[u8],
) -> Result<PoWDifficulty, &'static str> {
    if difficulty.is_empty() {
        // There is no difficulty function enabled
        return Ok(PoWDifficulty::LeadingZeroBytes {
            leading_zeroes: MINING_DIFFICULTY,
        });
    }

    // Decode the difficulty bytes into a CompactTarget and then expand that into a target
    // hash threshold.
    let compact_target = CompactTarget::try_from_slice(difficulty)
        .ok_or("block header contains invalid difficulty")?;

    match expand_compact_target_difficulty(compact_target) {
        // The target value is higher than the largest possible SHA3-256 hash.
        None => Ok(PoWDifficulty::TargetHashAlwaysPass),
        Some(target_hash) => Ok(PoWDifficulty::TargetHash { target_hash }),
    }
}

fn expand_compact_target_difficulty(compact_target: CompactTarget) -> Option<[u8; SHA3_256_BYTES]> {
    let expanded_target = compact_target.expand_integer();
    let byte_digits: Vec<u8> = expanded_target.to_digits(rug::integer::Order::MsfBe);
    if byte_digits.len() > SHA3_256_BYTES {
        // The target value is higher than the largest possible SHA3-256 hash.
        return None;
    }

    // Pad the target hash with leading zeroes to make it exactly SHA3_256_BYTES bytes long.
    let mut result = [0u8; SHA3_256_BYTES];
    result[SHA3_256_BYTES - byte_digits.len()..].copy_from_slice(&byte_digits);

    assert_eq!(expanded_target, rug::Integer::from_digits(&result, rug::integer::Order::MsfBe));

    Some(result)
}

/// An object which contains a nonce and can therefore be mined.
pub trait PoWObject {
    /// Gets the difficulty requirements for mining this object.
    fn pow_difficulty(&self) -> PoWDifficulty;

    /// Gets a range containing all nonce lengths permitted by this object.
    fn permitted_nonce_lengths(&self) -> Range<usize>;

    /// Gets the leading and trailing bytes for this object.
    ///
    /// These bytes are concatenated with a nonce of the given length in the middle while mining.
    ///
    /// ### Arguments
    ///
    /// * `nonce_length`  - The length (in bytes) of the nonce to be inserted between the leading
    ///                     and trailing bytes
    fn get_leading_and_trailing_bytes_for_mine(
        &self,
        nonce_length: usize,
    ) -> Result<(Box<[u8]>, Box<[u8]>), usize>;
}

impl PoWObject for BlockHeader {
    fn pow_difficulty(&self) -> PoWDifficulty {
        extract_target_difficulty(&self.difficulty)
            .expect("Block header contains invalid difficulty!")
    }

    fn permitted_nonce_lengths(&self) -> Range<usize> {
        1..POW_NONCE_MAX_LEN + 1
    }

    fn get_leading_and_trailing_bytes_for_mine(
        &self,
        nonce_length: usize,
    ) -> Result<(Box<[u8]>, Box<[u8]>), usize> {
        if !self.permitted_nonce_lengths().contains(&nonce_length) {
            return Err(nonce_length);
        }

        Ok(find_nonce_location(
            self,
            |block_header, nonce| block_header.nonce_and_mining_tx_hash.0 = nonce,
            nonce_length,
        ))
    }
}

impl PoWObject for ProofOfWork {
    fn pow_difficulty(&self) -> PoWDifficulty {
        // see utils::validate_pow_for_address()
        PoWDifficulty::LeadingZeroBytes {
            leading_zeroes: MINING_DIFFICULTY,
        }
    }

    fn permitted_nonce_lengths(&self) -> Range<usize> {
        ADDRESS_POW_NONCE_LEN..ADDRESS_POW_NONCE_LEN + 1
    }

    fn get_leading_and_trailing_bytes_for_mine(
        &self,
        nonce_length: usize,
    ) -> Result<(Box<[u8]>, Box<[u8]>), usize> {
        if !self.permitted_nonce_lengths().contains(&nonce_length) {
            return Err(nonce_length);
        }

        Ok(find_nonce_location(
            self,
            |pow, nonce| pow.nonce = nonce,
            nonce_length,
        ))
    }
}

/// A response from a mining operation which didn't encounter any errors.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum MineResult {
    /// Indicates that a valid Proof-of-Work was found for the block.
    FoundNonce {
        /// The found nonce which meets the block's difficulty requirements.
        nonce: Vec<u8>,
    },
    /// Indicates that despite testing all possible nonce values, the miner was unable to find a
    /// nonce which could meet the block's difficulty requirements.
    Exhausted,
    /// Indicates that the miner terminated prematurely because it received an interrupt request.
    TerminateRequested,
    /// Indicates that the miner terminated prematurely because it reached the timeout duration
    /// without finding a valid nonce.
    TimeoutReached,
}

/// Statistics indicating current mining performance.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct MinerStatistics {
    pub total_computed_hashes: u128,
    pub total_mining_duration: Duration,
}

impl MinerStatistics {
    /// Updates these statistics with the given data from a completed mining round.
    ///
    /// ### Arguments
    ///
    /// * `computed_hashes`  - The number of hashes computed in this mining round
    /// * `duration`         - How long it took to compute the indicated number of hashes
    pub fn update_immediate(&mut self, computed_hashes: u128, duration: Duration) {
        (self.total_computed_hashes, self.total_mining_duration) = (
            self.total_computed_hashes.checked_add(computed_hashes).unwrap(),
            self.total_mining_duration.checked_add(duration).unwrap(),
        );
    }

    /// Gets a handle to update these statistics safely, even in the event of an error.
    pub fn update_safe(&mut self) -> MinerStatisticsUpdater {
        MinerStatisticsUpdater {
            statistics: self,
            computed_hashes: 0,
            start_time: Instant::now(),
        }
    }

    /// Gets a human-readable indication of the current mining hash rate.
    pub fn hash_rate_units(&self) -> UnitsPrefixed {
        UnitsPrefixed {
            value: self.total_computed_hashes as f64,
            unit_name: "H",
            duration: Some(self.total_mining_duration),
        }
    }
}

/// A handle for eventually and safely updating a `MinerStatistics`.
pub struct MinerStatisticsUpdater<'a> {
    statistics: &'a mut MinerStatistics,
    computed_hashes: u128,
    start_time: Instant,
}

impl<'a> MinerStatisticsUpdater<'a> {
    pub fn computed_hashes(&mut self, count: u128) {
        self.computed_hashes += count;
    }
}

impl<'a> Drop for MinerStatisticsUpdater<'a> {
    fn drop(&mut self) {
        self.statistics.update_immediate(self.computed_hashes, self.start_time.elapsed())
    }
}

impl fmt::Display for MinerStatistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Computed {} hashes in {:.3}s, speed: {:.3}",
               self.total_computed_hashes,
               self.total_mining_duration.as_secs_f64(),
               self.hash_rate_units())
    }
}

/// An error which is thrown by a miner.
#[derive(Debug)]
pub enum MineError {
    Wrapped(Box<dyn std::error::Error>),
}

impl MineError {
    pub fn wrap<E: std::error::Error + 'static>(value: E) -> Self {
        Self::Wrapped(Box::new(value))
    }
}

impl std::error::Error for MineError {}

impl fmt::Display for MineError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Wrapped(cause) => write!(f, "An error occurred while mining: {cause}"),
        }
    }
}

pub trait SHA3_256PoWMiner {
    /// Returns true if this miner is hardware-accelerated.
    fn is_hw_accelerated(&self) -> bool;

    /// Returns the recommended minimum number of nonces which this implementation should compute
    /// at a time. Calling `generate_pow_block_internal` with a `nonce_count` smaller than this
    /// will likely cause the implementation's overhead to exceed any potential performance
    /// advantages.
    fn min_nonce_count(&self) -> u32;

    /// Returns the recommended number of nonces to test at a time.
    ///
    /// The number is implementation-defined, but should be a value which results in minimal
    /// overhead while also preventing unnecessarily long wait times from a single mining
    /// iteration.
    fn nonce_peel_amount(&self) -> u32;

    /// Tries to generate a Proof-of-Work for the given bytes.
    ///
    /// This will try to generate a Proof-of-Work by inserting 4-byte (32-bit) nonces (in
    /// little-endian encoding) between the given leading and trailing byte sequences and hashing
    /// the result. It will return the first nonce which met the given difficulty requirements, or
    /// `None` if none of the nonces could meet the difficulty requirements.
    ///
    /// ### Arguments
    ///
    /// * `leading_bytes`     - The bytes to hash before the 4-byte nonce
    /// * `trailing_bytes`    - The bytes to after the 4-byte nonce
    /// * `difficulty`        - The difficulty requirements
    /// * `first_nonce`       - The first nonce to test
    /// * `nonce_count`       - The number of nonces to test
    /// * `statistics`        - A `MinerStatistics` instance to be updated with information
    ///                         about the mining progress
    fn generate_pow_internal(
        &mut self,
        leading_bytes: &[u8],
        trailing_bytes: &[u8],
        difficulty: &PoWDifficulty,
        first_nonce: u32,
        nonce_count: u32,
        statistics: &mut MinerStatistics,
    ) -> Result<Option<u32>, MineError>;
}

/// Tries to generate a Proof-of-Work for the given object.
///
/// This will try to generate a Proof-of-Work by inserting nonces between the given leading and
/// trailing byte sequences and hashing the result. It will return the first nonce which met the
/// given difficulty requirements, or `None` if none of the nonces could meet the object's
/// difficulty requirements.
///
/// ### Arguments
///
/// * `object`           - The object to be mined
/// * `statistics`       - A `MinerStatistics` instance to be updated with information
///                        about the mining progress
/// * `terminate_flag`   - If set, this is a reference to an externally mutable boolean flag.
///                        Setting the value to `true` will signal the miner to stop.
/// * `timeout_duration` - If set, this is the maximum duration which the miner will run for.
///                        The miner will stop after approximately this duration, regardless
///                        of whether a result was found.
pub fn generate_pow(
    miner: &mut dyn SHA3_256PoWMiner,
    object: &impl PoWObject,
    statistics: &mut MinerStatistics,
    terminate_flag: Option<Arc<AtomicBool>>,
    timeout_duration: Option<Duration>,
) -> Result<MineResult, MineError> {
    let difficulty = object.pow_difficulty();

    // TODO: Change the nonce prefix if we run out of hashes to try
    let nonce_prefix_length = std::mem::size_of::<u32>();
    let (leading_bytes, trailing_bytes) =
        object.get_leading_and_trailing_bytes_for_mine(nonce_prefix_length)
            .expect("Object doesn't permit a 32-bit nonce!");

    let peel_amount = miner.nonce_peel_amount();

    let total_start_time = Instant::now();

    for (first_nonce, nonce_count) in split_range_into_blocks(0, u32::MAX, peel_amount) {
        let result = miner.generate_pow_internal(
            &leading_bytes,
            &trailing_bytes,
            &difficulty,
            first_nonce,
            nonce_count,
            statistics,
        )?;

        println!("Mining statistics: {}", statistics);

        if let Some(nonce) = result {
            return Ok(MineResult::FoundNonce {
                nonce: nonce.to_le_bytes().to_vec(),
            });
        }

        if let Some(terminate_flag) = &terminate_flag {
            if terminate_flag.load(Ordering::Acquire) {
                debug!("Miner terminating after being requested to terminate");
                return Ok(MineResult::TerminateRequested);
            }
        }

        if let Some(timeout_duration) = &timeout_duration {
            let total_elapsed_time = total_start_time.elapsed();
            if total_elapsed_time >= *timeout_duration {
                debug!(
                    "Miner terminating after reaching timeout (timeout={}s, elapsed time={}s)",
                    timeout_duration.as_secs_f64(),
                    total_elapsed_time.as_secs_f64());
                return Ok(MineResult::TimeoutReached);
            }
        }
    }

    Ok(MineResult::Exhausted)
}

/// Creates a miner.
pub fn create_any_miner(
    difficulty: Option<&PoWDifficulty>,
) -> Box<dyn SHA3_256PoWMiner> {
    if let Some(difficulty) = difficulty {
        match difficulty {
            // If the difficulty is sufficiently low that the overhead of a GPU miner would make
            // things slower, don't bother!
            PoWDifficulty::TargetHashAlwaysPass |
            PoWDifficulty::LeadingZeroBytes { leading_zeroes: ..=1 } =>
                return Box::new(CpuMiner::new()),
            _ => (),
        }
    }

    match opengl::OpenGlMiner::new() {
        Ok(miner) => return Box::new(miner),
        Err(cause) => warn!("Failed to create OpenGL miner: {cause}"),
    };

    Box::new(CpuMiner::new())
}

#[cfg(test)]
pub(super) mod test {
    use crate::miner_pow::cpu::CpuMiner;
    use crate::miner_pow::opengl::OpenGlMiner;
    use crate::miner_pow::vulkan::VulkanMiner;
    use super::*;

    #[derive(Copy, Clone, Debug)]
    pub struct TestBlockMinerInternal {
        pub name: &'static str,
        pub difficulty: &'static [u8],
        pub expected_nonce: u32,
        pub max_nonce_count: u32,
        pub requires_hw_accel: (bool, bool),
    }

    impl TestBlockMinerInternal {
        const NO_DIFFICULTY: Self = Self {
            name: "NO_DIFFICULTY",
            difficulty: &[],
            expected_nonce: 455,
            max_nonce_count: 1024,
            requires_hw_accel: (false, false),
        };
        const THRESHOLD_EASY: Self = Self {
            name: "THRESHOLD_EASY",
            difficulty: b"\x22\x00\x00\x01",
            expected_nonce: 28,
            max_nonce_count: 1024,
            requires_hw_accel: (false, false),
        };
        const THRESHOLD_HARD: Self = Self {
            name: "THRESHOLD_HARD",
            difficulty: b"\x20\x00\x00\x01",
            expected_nonce: 4894069,
            max_nonce_count: 4900000,
            requires_hw_accel: (true, false),
        };
        const THRESHOLD_VERY_HARD: Self = Self {
            name: "THRESHOLD_VERY_HARD",
            difficulty: b"\x1f\x00\x00\xFF",
            expected_nonce: 14801080,
            max_nonce_count: 15000000,
            requires_hw_accel: (true, true),
        };

        pub const ALL_TEST: &'static [TestBlockMinerInternal] = &[
            Self::NO_DIFFICULTY,
            Self::THRESHOLD_EASY,
            Self::THRESHOLD_HARD,
            Self::THRESHOLD_VERY_HARD,
        ];

        pub const ALL_BENCH: &'static [TestBlockMinerInternal] = &[
            Self::NO_DIFFICULTY,
            Self::THRESHOLD_EASY,
            Self::THRESHOLD_HARD,
            Self::THRESHOLD_VERY_HARD,
        ];

        pub fn test_miner(&self, miner: &mut impl SHA3_256PoWMiner, is_bench: bool) {
            if !miner.is_hw_accelerated()
                && if is_bench { self.requires_hw_accel.1 } else { self.requires_hw_accel.0 } {
                println!("Skipping test case {} (too hard)", self.name);
                return;
            }

            let block_header = test_block_header(self.difficulty);

            let difficulty = block_header.pow_difficulty();
            let (leading_bytes, trailing_bytes) =
                block_header.get_leading_and_trailing_bytes_for_mine(4)
                    .unwrap();

            let mut statistics = Default::default();
            assert_eq!(
                miner.generate_pow_internal(
                    &leading_bytes,
                    &trailing_bytes,
                    &difficulty,
                    0, self.max_nonce_count, &mut statistics,
                ).unwrap(),
                Some(self.expected_nonce),
                "Test case {:?}", self);

            println!("Test case {} statistics: {}", self.name, statistics);
        }
    }

    pub const TEST_MINING_DIFFICULTY: &'static [u8] = b"\x22\x00\x00\x01";

    fn test_block_header(difficulty: &[u8]) -> BlockHeader {
        BlockHeader {
            version: 1337,
            bits: 10973,
            nonce_and_mining_tx_hash: (vec![], "abcde".to_string()),
            b_num: 2398927,
            timestamp: 29837637,
            difficulty: difficulty.to_vec(),
            seed_value: b"2983zuifsigezd".to_vec(),
            previous_hash: Some("jeff".to_string()),
            txs_merkle_root_and_hash: ("merkle_root".to_string(), "hash".to_string()),
        }
    }

    #[test]
    fn test_big_integer_behaves_as_expected() {
        use rug::Integer;
        use std::cmp::Ordering::{self, *};
        type DigestArr = [u8; SHA3_256_BYTES];

        fn to_int(digits: &[u8]) -> Integer {
            let res = Integer::from_digits(digits, rug::integer::Order::MsfBe);
            assert_eq!(res, Integer::from_digits(digits, rug::integer::Order::MsfLe));
            res
        }

        fn test(hash: &[u8], target: &[u8], order: Ordering) {
            let (hash, target) = (to_int(hash), to_int(target));
            assert_eq!(PartialOrd::partial_cmp(&hash, &target), Some(order),
                       "hash: {hash}, target: {target}");
        }

        test(b"\x00", b"\x00", Equal);
        test(b"\x02", b"\x00", Greater);
        test(b"\x02", b"\x80", Less);

        test(b"\x0201234567", b"\x8001234567", Less);
        test(b"\x8001234567", b"\x8001234567", Equal);
        test(b"01234567890abcde01234567890abcde", b"01234567890abcde01234567890abcde", Equal);
        test(b"01234567890abcde01234567890abcde", b"91234567890abcde01234567890abcde", Less);
        test(b"01234567890abcde01234567890abcde", b"01234567890abcde91234567890abcde", Less);
    }

    #[test]
    fn test_expand_compact_target_difficulty() {
        fn test_hex(compact_target: u32, target_hash_hex: Option<&str>) {
            let target_hash = expand_compact_target_difficulty(CompactTarget::from_array(compact_target.to_be_bytes()))
                .map(|h| hex::encode_upper(&h));
            assert_eq!(target_hash.as_ref().map(|s| s.as_str()), target_hash_hex,
                       "compact_target=0x{:08x}", compact_target)
        }

        test_hex(0x00000000, Some("0000000000000000000000000000000000000000000000000000000000000000"));
        test_hex(0x03000000, Some("0000000000000000000000000000000000000000000000000000000000000000"));
        test_hex(0xFF000000, Some("0000000000000000000000000000000000000000000000000000000000000000"));

        test_hex(0x03000001, Some("0000000000000000000000000000000000000000000000000000000000000001"));
        test_hex(0x037FFFFF, Some("00000000000000000000000000000000000000000000000000000000007FFFFF"));
        test_hex(0x03FFFFFF, Some("00000000000000000000000000000000000000000000000000000000007FFFFF"));
        test_hex(0x02FFFFFF, Some("0000000000000000000000000000000000000000000000000000000000007FFF"));
        test_hex(0x01FFFFFF, Some("000000000000000000000000000000000000000000000000000000000000007F"));
        test_hex(0x00FFFFFF, Some("0000000000000000000000000000000000000000000000000000000000000000"));

        test_hex(0x22000001, Some("0100000000000000000000000000000000000000000000000000000000000000"));

        test_hex(0xFFFFFFFF, None);
    }

    #[test]
    fn verify_cpu() {
        let mut miner = CpuMiner::new();
        for case in TestBlockMinerInternal::ALL_TEST {
            case.test_miner(&mut miner, false);
        }
    }

    #[test]
    fn verify_opengl() {
        let mut miner = OpenGlMiner::new().unwrap();
        for case in TestBlockMinerInternal::ALL_TEST {
            case.test_miner(&mut miner, false);
        }
    }

    #[test]
    fn verify_vulkan() {
        let mut miner = VulkanMiner::new().unwrap();
        for case in TestBlockMinerInternal::ALL_TEST {
            case.test_miner(&mut miner, false);
        }
    }
}

// cargo bench --package aiblock_network --lib miner_pow::bench --features benchmark_miners -- --show-output --test
#[cfg(test)]
#[cfg(feature = "benchmark_miners")]
mod bench {
    use crate::miner_pow::cpu::CpuMiner;
    use crate::miner_pow::opengl::OpenGlMiner;
    use crate::miner_pow::vulkan::VulkanMiner;
    use super::*;
    use super::test::*;

    #[test]
    fn bench_cpu() {
        let mut miner = CpuMiner::new();
        for case in TestBlockMinerInternal::ALL_BENCH {
            case.test_miner(&mut miner, true);
        }
    }

    #[test]
    fn bench_opengl() {
        let mut miner = OpenGlMiner::new().unwrap();
        for case in TestBlockMinerInternal::ALL_BENCH {
            case.test_miner(&mut miner, true);
        }
    }

    #[test]
    fn bench_vulkan() {
        let mut miner = VulkanMiner::new().unwrap();
        for case in TestBlockMinerInternal::ALL_BENCH {
            case.test_miner(&mut miner, true);
        }
    }
}
