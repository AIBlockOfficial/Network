use tw_chain::crypto::sha3_256;
use crate::constants::MINING_DIFFICULTY;
use crate::miner_pow::{MinerStatistics, PoWBlockMiner, PreparedBlockDifficulty, PreparedBlockHeader};

/// A miner which runs on the CPU.
#[derive(Copy, Clone, Debug)]
pub struct CpuMiner(); // this is stateless

impl CpuMiner {
    /// Creates a new CPU miner instance.
    pub fn new() -> Self {
        Self()
    }
}

impl PoWBlockMiner for CpuMiner {
    type Error = ();

    fn min_nonce_count(&self) -> u32 {
        1
    }

    fn nonce_peel_amount(&self) -> u32 {
        // TODO: We'd probably want to choose a better metric for this
        1 << 10
    }

    fn generate_pow_block_internal(
        &mut self,
        prepared_block_header: &PreparedBlockHeader,
        first_nonce: u32,
        nonce_count: u32,
        statistics: &mut MinerStatistics,
    ) -> Result<Option<u32>, Self::Error> {
        if nonce_count == 0 {
            return Ok(None);
        }

        let mut stats_updater = statistics.update_safe();

        let difficulty_target = match &prepared_block_header.difficulty {
            // The target value is higher than the largest possible SHA3-256 hash. Therefore,
            // every hash will meet the required difficulty threshold, so we can just return
            // an arbitrary nonce.
            PreparedBlockDifficulty::TargetHashAlwaysPass => return Ok(Some(first_nonce)),

            PreparedBlockDifficulty::LeadingZeroBytes => None,
            PreparedBlockDifficulty::TargetHash { target_hash } => Some(target_hash),
        };

        let block_header_nonce_offset = prepared_block_header.header_nonce_offset;
        let mut block_header_bytes = prepared_block_header.header_bytes.clone();

        for nonce in first_nonce..=first_nonce.saturating_add(nonce_count - 1) {
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

#[cfg(test)]
mod test {
    use crate::miner_pow::test::TestBlockMinerInternal;
    use super::*;

    #[test]
    fn verify_cpu() {
        let mut miner = CpuMiner::new();
        for case in TestBlockMinerInternal::ALL_EASY {
            case.test_miner(&mut miner);
        }
    }
}