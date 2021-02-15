#![allow(unused)]
use naom::constants::MAX_BLOCK_SIZE;
use naom::primitives::asset::Asset;
use naom::primitives::block::Block;
use naom::primitives::block::BlockHeader;
use naom::primitives::transaction::{Transaction, TxIn, TxOut};
use sha3::Digest;

use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha3::Sha3_256;
use sodiumoxide::crypto::sign::ed25519::PublicKey;
use std::convert::TryInto;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Block header, which contains a smaller footprint view of the block.
/// Hash records are assumed to be 256 bit
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HashBlockHeader {
    pub version: u32,
    pub bits: usize,
    pub nonce: Vec<u8>,
    pub b_num: u64,
    pub seed_value: Vec<u8>, // for commercial
    pub previous_hash: Option<String>,
    pub merkle_root_hash: String,
}

impl Default for HashBlockHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl HashBlockHeader {
    /// Creates a new BlockHeader
    pub fn new() -> HashBlockHeader {
        HashBlockHeader {
            version: 0,
            previous_hash: None,
            merkle_root_hash: String::default(),
            seed_value: Vec::new(),
            bits: 0,
            b_num: 0,
            nonce: Vec::new(),
        }
    }

    /// Checks whether a BlockHeader is empty
    pub fn is_null(&self) -> bool {
        self.bits == 0
    }
}

/// A block, a collection of transactions for processing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HashBlock {
    pub header: HashBlockHeader,
    pub transactions: u64,
}

impl Default for HashBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl HashBlock {
    /// Creates a new instance of a block
    pub fn new() -> HashBlock {
        HashBlock {
            header: HashBlockHeader::new(),
            transactions: u64::MIN,
        }
    }

    pub fn set_header(&mut self, header_block: &BlockHeader) {
        self.header.version = header_block.version;
        self.header.bits = header_block.bits;
        self.header.nonce = header_block.nonce.clone();
        self.header.b_num = header_block.b_num;
        self.header.seed_value = header_block.seed_value.clone();
        self.header.previous_hash = header_block.previous_hash.clone();
        self.header.merkle_root_hash = header_block.merkle_root_hash.clone();
    }

    pub fn hash_blocking(&mut self, block: &Block) {
        self.set_header(&block.header);
        let mut hasher = DefaultHasher::new();
        (block.transactions).hash(&mut hasher);
        self.transactions = hasher.finish();
    }
    /// Sets the internal number of bits based on length
    pub fn set_bits(&mut self) {
        let bytes = Bytes::from(serialize(&self).unwrap());
        self.header.bits = bytes.len();
    }

    /// Checks whether a block has hit its maximum size
    pub fn is_full(&self) -> bool {
        let bytes = Bytes::from(serialize(&self).unwrap());
        bytes.len() >= MAX_BLOCK_SIZE
    }

    /// Get the merkle root for the current set of transactions
    pub fn get_merkle_root(&mut self) -> String {
        //let merkle = self.build_merkle_tree();
        //let root_hash = merkle.root().to_vec();
        let root_hash = String::from(" ");

        self.header.merkle_root_hash = root_hash.clone();
        root_hash
    }

    // /// Builds a merkle tree for secure reference
    // fn build_merkle_tree(&self) -> MerkleTree<[u8; 32], MerkleHasher, VecStore<[u8; 32]>> {
    //     let mut tx_hashes = Vec::new();

    //     for tx in &self.transactions {
    //         let tx_bytes = Bytes::from(serialize(&tx).unwrap());
    //         let tx_hash = Sha3_256::digest(&tx_bytes).to_vec();
    //         let merkle_hash = from_slice(tx_hash.as_slice());

    //         tx_hashes.push(merkle_hash);
    //     }

    //     MerkleTree::try_from_iter(tx_hashes.clone().into_iter().map(Ok)).unwrap()
    // }
}




