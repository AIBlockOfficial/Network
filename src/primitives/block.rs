#![allow(unused)]
use crate::constants::MAX_BLOCK_SIZE;
use crate::interfaces::Asset;
use crate::primitives::merkle_hasher::MerkleHasher;
use crate::primitives::transaction::{Transaction, TxIn, TxOut};
use crate::sha3::Digest;

use bincode::{deserialize, serialize};
use bytes::Bytes;
use merkletree::merkle::MerkleTree;
use merkletree::store::VecStore;
use serde::{Deserialize, Serialize};
use sha3::Sha3_256;
use sodiumoxide::crypto::sign::ed25519::PublicKey;
use std::convert::TryInto;

/// Block header, which contains a smaller footprint view of the block.
/// Hash records are assumed to be 256 bit
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockHeader {
    pub version: u32,
    pub time: u32,
    pub bits: usize,
    pub nonce: Vec<u8>,
    pub seed_value: Vec<u8>, // for commercial
    pub previous_hash: Vec<u8>,
    pub merkle_root_hash: Vec<u8>,
}

impl BlockHeader {
    /// Creates a new BlockHeader
    pub fn new() -> BlockHeader {
        BlockHeader {
            version: 0,
            previous_hash: Vec::with_capacity(32),
            merkle_root_hash: Vec::with_capacity(32),
            seed_value: Vec::new(),
            time: 0,
            bits: 0,
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
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
}

impl Block {
    /// Creates a new instance of a block
    pub fn new() -> Block {
        Block {
            header: BlockHeader::new(),
            transactions: Vec::new(),
        }
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
    pub fn get_merkle_root(&mut self) -> Vec<u8> {
        let merkle = self.build_merkle_tree();
        let root_hash = merkle.root().to_vec();

        self.header.merkle_root_hash = root_hash.clone();
        root_hash
    }

    /// Builds a merkle tree for secure reference
    fn build_merkle_tree(&self) -> MerkleTree<[u8; 32], MerkleHasher, VecStore<[u8; 32]>> {
        let mut tx_hashes = Vec::new();

        for tx in &self.transactions {
            let tx_bytes = Bytes::from(serialize(&tx).unwrap());
            let tx_hash = Sha3_256::digest(&tx_bytes).to_vec();
            let merkle_hash = from_slice(tx_hash.as_slice());

            tx_hashes.push(merkle_hash);
        }

        MerkleTree::try_from_iter(tx_hashes.clone().into_iter().map(Ok)).unwrap()
    }
}

/*---- FUNCTIONS ----*/

/// Converts a dynamic array into a static 32 bit
///
/// ### Arguments
///
/// * `bytes`   - Bytes to cast
fn from_slice(bytes: &[u8]) -> [u8; 32] {
    let mut array = [0; 32];
    let bytes = &bytes[..array.len()]; // panics if not enough data
    array.copy_from_slice(bytes);
    array
}

/// Builds the scaffold of a genesis block
///
/// ### Arguments
///
/// * `time`            - Time value of block
/// * `nonce`           - Block nonce
/// * `bits`            - Bit length of block
/// * `version`         - Network version to keep track of
/// * `genesis_reward`  - Token reward for the block output
/// * `genesis_output`  - Output script for the genesis output (STILL TODO)
pub fn create_raw_genesis_block(
    time: &u32,
    nonce: Vec<u8>,
    bits: &usize,
    version: &u32,
    genesis_reward: &u64,
    genesis_output: String,
) -> Block {
    let unicorn_val = String::from("Belgien hat pro Kopf nun am meisten Todesfälle");
    let mut genesis = Block::new();

    let mut gen_transaction = Transaction::new();
    let mut tx_in = TxIn::new();
    let mut tx_out = TxOut::new();

    // Handle genesis transaction
    let hashed_key = Sha3_256::digest(&unicorn_val.as_bytes()).to_vec();
    let unicorn_key: [u8; 32] = hashed_key[..].try_into().unwrap();

    //tx_in.script_signature = Some(PublicKey(unicorn_key));
    //tx_out.value = Some(Asset::Token(*genesis_reward));

    gen_transaction.inputs.push(tx_in);
    gen_transaction.outputs.push(tx_out);

    // Handle block header
    genesis.header.version = *version;
    genesis.header.bits = *bits;
    genesis.header.nonce = nonce.clone();
    genesis.header.time = *time;

    // Add genesis transaction
    genesis.transactions.push(gen_transaction);

    // Other stuff accepts defaults, so just return the block
    genesis
}

/// Creates a final genesis block for inclusion in the chain
///
/// ### Arguments
///
/// * `time`            - Time of the genesis block
/// * `nonce`           - Nonce of the genesis block
/// * `bits`            - Bit length of the block
/// * `version`         - Version of the block
/// * `genesis_reward`  - Coinbase reward from the initial block
pub fn create_genesis_block(
    time: u32,
    nonce: Vec<u8>,
    bits: usize,
    version: u32,
    genesis_reward: u64,
) -> Block {
    // Using straight constant in this case, but will need to incorporate some kind of scripting situation
    create_raw_genesis_block(
        &time,
        nonce,
        &bits,
        &version,
        &genesis_reward,
        "".to_string(),
    )
}
