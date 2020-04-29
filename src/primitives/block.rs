use crate::interfaces::Asset;
use crate::primitives::transaction::{Transaction, TxIn, TxOut};
use serde::{Deserialize, Serialize};

/// Block header, which contains a smaller footprint view of the block.
/// Hash records are assumed to be 256 bit
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockHeader {
    pub version: u32,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
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
            nonce: 0,
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
}

/*---- FUNCTIONS ----*/

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
    nonce: &u32,
    bits: &u32,
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
    tx_out.value = Some(Asset::Token(*genesis_reward));

    gen_transaction.inputs.push(tx_in);
    gen_transaction.outputs.push(tx_out);

    // Handle block header
    genesis.header.version = *version;
    genesis.header.bits = *bits;
    genesis.header.nonce = *nonce;
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
    nonce: u32,
    bits: u32,
    version: u32,
    genesis_reward: u64,
) -> Block {
    // Using straight constant in this case, but will need to incorporate some kind of scripting situation
    create_raw_genesis_block(
        &time,
        &nonce,
        &bits,
        &version,
        &genesis_reward,
        "".to_string(),
    )
}
