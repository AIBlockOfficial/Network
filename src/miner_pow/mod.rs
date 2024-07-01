pub mod cpu;

use tw_chain::primitives::block::BlockHeader;
use crate::asert::CompactTarget;
use crate::constants::POW_NONCE_LEN;

pub const SHA3_256_BYTES: usize = 32;

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

fn find_block_header_nonce_location(block_header: &BlockHeader) -> (Box<[u8]>, usize) {
    assert_eq!(POW_NONCE_LEN, 4, "POW_NONCE_LEN has changed?!?");

    let mut block_header = block_header.clone();

    block_header.nonce_and_mining_tx_hash.0 = vec![0x00u8; POW_NONCE_LEN];
    let serialized_00 = bincode::serialize(&block_header).unwrap();
    block_header.nonce_and_mining_tx_hash.0 = vec![0xFFu8; POW_NONCE_LEN];
    let serialized_ff = bincode::serialize(&block_header).unwrap();

    assert_ne!(serialized_00, serialized_ff,
               "changing the nonce didn't affect the serialized block header?!?");
    assert_eq!(serialized_00.len(), serialized_ff.len(),
               "changing the nonce affected the block header's serialized length?!?");

    // find the index at which the two headers differ
    let nonce_offset = (0..serialized_00.len())
        .find(|offset| serialized_00[*offset] != serialized_ff[*offset])
        .expect("the serialized block headers are not equal, but are equal at every index?!?");

    assert_eq!(serialized_00.as_slice()[nonce_offset..nonce_offset + 4], [0x00u8; 4],
               "serialized block header with nonce 0x00000000 has different bytes at presumed nonce offset!");
    assert_eq!(serialized_ff.as_slice()[nonce_offset..nonce_offset + 4], [0xFFu8; 4],
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
    pub fn prepare(block_header: &BlockHeader) -> Result<Self, &'static str> {
        let (header_bytes, header_nonce_offset) = find_block_header_nonce_location(block_header);

        let difficulty = extract_target_difficulty(&block_header.difficulty)?;

        Ok(Self {
            header_bytes,
            header_nonce_offset,
            difficulty,
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    pub const TEST_MINING_DIFFICULTY: &'static [u8] = b"\x22\x00\x00\x01";

    pub fn test_block_header(difficulty: &[u8]) -> PreparedBlockHeader {
        PreparedBlockHeader::prepare(&BlockHeader {
            version: 1337,
            bits: 10973,
            nonce_and_mining_tx_hash: (vec![], "abcde".to_string()),
            b_num: 2398927,
            timestamp: 29837637,
            difficulty: difficulty.to_vec(),
            seed_value: b"2983zuifsigezd".to_vec(),
            previous_hash: Some("jeff".to_string()),
            txs_merkle_root_and_hash: ("merkle_root".to_string(), "hash".to_string()),
        }).unwrap()
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
}
