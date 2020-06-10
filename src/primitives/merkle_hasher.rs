use crypto::digest::Digest;
use crypto::sha3::{Sha3, Sha3Mode};
use merkletree::hash::Algorithm;
use std::hash::Hasher;

pub struct MerkleHasher(Sha3);

impl MerkleHasher {
    pub fn new() -> MerkleHasher {
        MerkleHasher(Sha3::new(Sha3Mode::Sha3_256))
    }
}

impl Default for MerkleHasher {
    fn default() -> MerkleHasher {
        MerkleHasher::new()
    }
}

impl Hasher for MerkleHasher {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.input(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

impl Algorithm<[u8; 32]> for MerkleHasher {
    #[inline]
    fn hash(&mut self) -> [u8; 32] {
        let mut h = [0u8; 32];
        self.0.result(&mut h);
        h
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }
}
