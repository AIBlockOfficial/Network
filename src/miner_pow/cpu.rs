use tw_chain::crypto::sha3_256;
use crate::constants::MINING_DIFFICULTY;
use crate::miner_pow::{PreparedBlockDifficulty, PreparedBlockHeader};

/// Tries to generate a Proof-of-Work for a block.
pub fn generate_pow_block_cpu(
    prepared_block_header: &PreparedBlockHeader,
    first_nonce: u32,
    nonce_count: u32,
) -> Result<Option<u32>, &'static str> {
    if nonce_count == 0 {
        return Ok(None);
    }

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

#[cfg(test)]
mod test {
    use crate::miner_pow::test::{test_block_header, TEST_MINING_DIFFICULTY};
    use super::*;

    #[test]
    fn verify_cpu() {
        let header = test_block_header(TEST_MINING_DIFFICULTY);
        assert_eq!(generate_pow_block_cpu(&header, 0, 1024),
                   Ok(Some(28)));

        let header = test_block_header(&[]);
        assert_eq!(generate_pow_block_cpu(&header, 0, 1024),
                   Ok(Some(455)));
    }
}