use tw_chain::crypto::sha3_256;
use crate::constants::MINING_DIFFICULTY;
use crate::miner_pow::{MinerStatistics, SHA3_256PoWMiner, PoWDifficulty};

/// A miner which runs on the CPU.
#[derive(Copy, Clone, Debug)]
pub struct CpuMiner(); // this is stateless

impl CpuMiner {
    /// Creates a new CPU miner instance.
    pub fn new() -> Self {
        Self()
    }
}

impl SHA3_256PoWMiner for CpuMiner {
    type Error = ();

    fn is_hw_accelerated(&self) -> bool {
        false
    }

    fn min_nonce_count(&self) -> u32 {
        1
    }

    fn nonce_peel_amount(&self) -> u32 {
        // TODO: We'd probably want to choose a better metric for this
        1 << 10
    }

    fn generate_pow_internal(
        &mut self,
        leading_bytes: &[u8],
        trailing_bytes: &[u8],
        difficulty: &PoWDifficulty,
        first_nonce: u32,
        nonce_count: u32,
        statistics: &mut MinerStatistics,
    ) -> Result<Option<u32>, Self::Error> {
        if nonce_count == 0 {
            return Ok(None);
        }

        let mut stats_updater = statistics.update_safe();

        let difficulty_target = match difficulty {
            // The target value is higher than the largest possible SHA3-256 hash. Therefore,
            // every hash will meet the required difficulty threshold, so we can just return
            // an arbitrary nonce.
            PoWDifficulty::TargetHashAlwaysPass => return Ok(Some(first_nonce)),

            PoWDifficulty::LeadingZeroBytes { leading_zeroes } => {
                assert_eq!(*leading_zeroes, MINING_DIFFICULTY);
                None
            },
            PoWDifficulty::TargetHash { target_hash } => Some(target_hash),
        };

        let block_header_nonce_offset = leading_bytes.len();
        let mut block_header_bytes =
            [ leading_bytes, &0u32.to_le_bytes(), trailing_bytes ].concat();

        for i in 0..nonce_count {
            let nonce = first_nonce.wrapping_add(i);

            block_header_bytes
                [block_header_nonce_offset..block_header_nonce_offset + 4]
                .copy_from_slice(&nonce.to_ne_bytes());

            let hash = sha3_256::digest(&block_header_bytes);

            stats_updater.computed_hashes(1);

            match difficulty_target {
                None => {
                    // There isn't a difficulty function, check if the first MINING_DIFFICULTY bytes
                    // of the hash are 0
                    let hash_prefix = hash.first_chunk::<MINING_DIFFICULTY>().unwrap();
                    if hash_prefix == &[0u8; MINING_DIFFICULTY] {
                        return Ok(Some(nonce));
                    }
                }
                Some(target) => {
                    if hash.as_slice() <= target.as_slice() {
                        return Ok(Some(nonce));
                    }
                }
            }
        }

        Ok(None)
    }
}