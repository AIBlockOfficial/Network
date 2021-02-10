#![allow(unused)]
use naom::constants::MAX_BLOCK_SIZE;
use naom::primitives::asset::Asset;
use naom::primitives::transaction::{Transaction, TxIn, TxOut};
use naom::primitives::block::Block;
use naom::primitives::block::BlockHeader;
use sha3::Digest;
use crypto_hash::{Algorithm, hex_digest};

use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha3::Sha3_256;
use sodiumoxide::crypto::sign::ed25519::PublicKey;
use std::convert::TryInto;

/// Block header, which contains a smaller footprint view of the block.
/// Hash records are assumed to be 256 bit
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HashBlockHeader {
    pub version: u32,
    pub time: u32,
    pub bits: usize,
    pub nonce: Vec<u8>,
    pub b_num: u64,
    pub seed_value: Vec<u8>, // for commercial
    pub previous_hash: Option<String>,
    pub merkle_root_hash: Vec<u8>,
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
            merkle_root_hash: Vec::with_capacity(32),
            seed_value: Vec::new(),
            time: 0,
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
    pub transactions: String,
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
            transactions: String::from(""),
        }
    }

    pub fn setHeader(&mut self, headerBlock: BlockHeader)
    {
        self.header.version = headerBlock.version;
        self.header.time = headerBlock.time;
        self.header.bits = headerBlock.bits;
        self.header.nonce = headerBlock.nonce;
        self.header.b_num = headerBlock.b_num;
        self.header.seed_value = headerBlock.seed_value;
        self.header.previous_hash = headerBlock.previous_hash;
        self.header.merkle_root_hash = headerBlock.merkle_root_hash;

    }

    pub fn hashBlock(&mut self, block:Block)
    {
        self.setHeader(block.header);
        self.transactions = hex_digest(Algorithm::SHA256, block.transactions);
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
        //let merkle = self.build_merkle_tree();
        //let root_hash = merkle.root().to_vec();
        let root_hash = Vec::new();

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
) -> HashBlock {
    let unicorn_val = String::from("Belgien hat pro Kopf nun am meisten Todesfälle");
    let mut genesis = HashBlock::new();

    let mut gen_transaction = Transaction::new();
    let mut tx_in = TxIn::new();
    let mut tx_out = TxOut::new();
    let mut transPreHash = Vec::new();

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
    genesis.header.nonce = nonce;
    genesis.header.time = *time;

    let hash_input = Bytes::from(serialize(&gen_transaction).unwrap());
    let hash_key = hex::encode(Sha3_256::digest(&hash_input));

    // Add genesis transaction
    transPreHash.push(hash_key);
    genesis.transactions = hex_digest(Algorithm::SHA256, transPreHash);
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
) -> HashBlock {
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