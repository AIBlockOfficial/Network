pub mod cpu;
pub mod opengl;
pub mod vulkan;

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::debug;
use tw_chain::primitives::block::BlockHeader;
use crate::asert::CompactTarget;
use crate::constants::POW_NONCE_MAX_LEN;
use crate::utils::{split_range_into_blocks, UnitsPrefixed};

pub const SHA3_256_BYTES: usize = 32;
pub const BLOCK_HEADER_MAX_BYTES: usize = 1024;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct PreparedBlockHeader {
    pub header_bytes: Box<[u8]>,
    pub header_nonce_offset: usize,
    pub difficulty: PreparedBlockDifficulty,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum PreparedBlockDifficulty {
    LeadingZeroBytes,
    TargetHashAlwaysPass,
    TargetHash {
        target_hash: [u8; SHA3_256_BYTES],
    },
}

fn find_block_header_nonce_location(
    block_header: &BlockHeader,
    nonce_length: usize,
) -> (Box<[u8]>, usize) {
    assert!(nonce_length > 0 && nonce_length <= POW_NONCE_MAX_LEN,
            "invalid nonce_length {}, should be in range [0, {}]",
            nonce_length, POW_NONCE_MAX_LEN);

    let mut block_header = block_header.clone();

    block_header.nonce_and_mining_tx_hash.0 = vec![0x00u8; nonce_length];
    let serialized_00 = bincode::serialize(&block_header).unwrap();
    block_header.nonce_and_mining_tx_hash.0 = vec![0xFFu8; nonce_length];
    let serialized_ff = bincode::serialize(&block_header).unwrap();

    assert_ne!(serialized_00, serialized_ff,
               "changing the nonce didn't affect the serialized block header?!?");
    assert_eq!(serialized_00.len(), serialized_ff.len(),
               "changing the nonce affected the block header's serialized length?!?");

    // find the index at which the two headers differ
    let nonce_offset = (0..serialized_00.len())
        .find(|offset| serialized_00[*offset] != serialized_ff[*offset])
        .expect("the serialized block headers are not equal, but are equal at every index?!?");

    assert_eq!(&serialized_00.as_slice()[nonce_offset..nonce_offset + 4], vec![0x00u8; nonce_length].as_slice(),
               "serialized block header with nonce 0x00000000 has different bytes at presumed nonce offset!");
    assert_eq!(&serialized_ff.as_slice()[nonce_offset..nonce_offset + 4], vec![0xFFu8; nonce_length].as_slice(),
               "serialized block header with nonce 0xFFFFFFFF has different bytes at presumed nonce offset!");

    (serialized_00.into_boxed_slice(), nonce_offset)
}

fn extract_target_difficulty(
    difficulty: &[u8],
) -> Result<PreparedBlockDifficulty, &'static str> {
    if difficulty.is_empty() {
        // There is no difficulty function enabled
        return Ok(PreparedBlockDifficulty::LeadingZeroBytes);
    }

    // Decode the difficulty bytes into a CompactTarget and then expand that into a target
    // hash threshold.
    let compact_target = CompactTarget::try_from_slice(difficulty)
        .ok_or("block header contains invalid difficulty")?;

    match expand_compact_target_difficulty(compact_target) {
        // The target value is higher than the largest possible SHA3-256 hash.
        None => Ok(PreparedBlockDifficulty::TargetHashAlwaysPass),
        Some(target_hash) => Ok(PreparedBlockDifficulty::TargetHash { target_hash }),
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

impl PreparedBlockHeader {
    pub fn prepare(
        block_header: &BlockHeader,
        nonce_length: usize,
    ) -> Result<Self, &'static str> {
        let (header_bytes, header_nonce_offset) =
            find_block_header_nonce_location(block_header, nonce_length);

        let difficulty = extract_target_difficulty(&block_header.difficulty)?;

        Ok(Self {
            header_bytes,
            header_nonce_offset,
            difficulty,
        })
    }
}

/// A response from a mining operation which didn't encounter any errors.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum BlockMineResult {
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

pub trait PoWBlockMiner {
    type Error : Sized + Debug;

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

    /// Tries to generate a Proof-of-Work for a block header.
    ///
    /// This will try to generate a Proof-of-Work using the requested 32-bit nonces and
    /// return the first nonce which met the requirements, or `None` if none of the nonces
    /// could fulfil the difficulty requirements.
    ///
    /// ### Arguments
    ///
    /// * `prepared_block_header`  - A `PreparedBlockHeader` for the block header to be mined
    /// * `first_nonce`            - The first nonce to test
    /// * `nonce_count`            - The number of nonces to test
    /// * `statistics`             - A `MinerStatistics` instance to be updated with information
    ///                              about the mining progress
    fn generate_pow_block_internal(
        &mut self,
        prepared_block_header: &PreparedBlockHeader,
        first_nonce: u32,
        nonce_count: u32,
        statistics: &mut MinerStatistics,
    ) -> Result<Option<u32>, Self::Error>;

    /// Tries to generate a Proof-of-Work for a block header.
    ///
    /// This will try to generate a Proof-of-Work using any valid nonce, and will keep going until
    /// a valid nonce is found, the provided termination flag becomes `true`, or the given timeout
    /// duration is reached.
    ///
    /// ### Arguments
    ///
    /// * `block_header`     - The block header to find a Proof-of-Work for.
    /// * `statistics`       - A `MinerStatistics` instance to be updated with information
    ///                        about the mining progress
    /// * `terminate_flag`   - If set, this is a reference to an externally mutable boolean flag.
    ///                        Setting the value to `true` will signal the miner to stop.
    /// * `timeout_duration` - If set, this is the maximum duration which the miner will run for.
    ///                        The miner will stop after approximately this duration, regardless
    ///                        of whether a result was found.
    fn generate_pow_block(
        &mut self,
        block_header: &BlockHeader,
        statistics: &mut MinerStatistics,
        terminate_flag: Option<Arc<AtomicBool>>,
        timeout_duration: Option<Duration>,
    ) -> Result<BlockMineResult, Self::Error> {
        // TODO: Change the nonce prefix if we run out of hashes to try
        let prepared_header = PreparedBlockHeader::prepare(block_header, std::mem::size_of::<u32>()).unwrap();

        let peel_amount = self.nonce_peel_amount();

        let total_start_time = Instant::now();

        for (first_nonce, nonce_count) in split_range_into_blocks(0, u32::MAX, peel_amount) {
            let result = self.generate_pow_block_internal(
                &prepared_header,
                first_nonce,
                nonce_count,
                statistics,
            )?;

            println!("Mining statistics: {}", statistics);

            if let Some(nonce) = result {
                return Ok(BlockMineResult::FoundNonce {
                    nonce: nonce.to_le_bytes().to_vec(),
                });
            }

            if let Some(terminate_flag) = &terminate_flag {
                if terminate_flag.load(Ordering::Acquire) {
                    debug!("Miner terminating after being requested to terminate");
                    return Ok(BlockMineResult::TerminateRequested);
                }
            }

            if let Some(timeout_duration) = &timeout_duration {
                let total_elapsed_time = total_start_time.elapsed();
                if total_elapsed_time >= *timeout_duration {
                    debug!(
                        "Miner terminating after reaching timeout (timeout={}s, elapsed time={}s)",
                        timeout_duration.as_secs_f64(),
                        total_elapsed_time.as_secs_f64());
                    return Ok(BlockMineResult::TimeoutReached);
                }
            }
        }

        Ok(BlockMineResult::Exhausted)
    }
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

        pub fn test_miner(&self, miner: &mut impl PoWBlockMiner, is_bench: bool) {
            if !miner.is_hw_accelerated()
                && if is_bench { self.requires_hw_accel.1 } else { self.requires_hw_accel.0 } {
                println!("Skipping test case {} (too hard)", self.name);
                return;
            }

            let prepared_block_header = test_prepared_block_header(self.difficulty);

            let mut statistics = Default::default();
            assert_eq!(miner.generate_pow_block_internal(&prepared_block_header, 0, self.max_nonce_count, &mut statistics).unwrap(),
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

    fn test_prepared_block_header(difficulty: &[u8]) -> PreparedBlockHeader {
        PreparedBlockHeader::prepare(&test_block_header(difficulty), 4).unwrap()
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
