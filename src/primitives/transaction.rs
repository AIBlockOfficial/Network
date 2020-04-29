#![allow(unused)]
use bincode::serialize;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::interfaces::Asset;
use crate::script::lang::Script;
use crate::utils::is_valid_amount;

/// An outpoint - a combination of a transaction hash and an index n into its vout
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutPoint {
    pub hash: Vec<u8>,
    pub n: i32,
}

// TODO: Hashes are currently Vec<u8>, can be stored some other way
impl OutPoint {
    /// Creates a new outpoint instance
    fn new(hash: Vec<u8>, n: i32) -> OutPoint {
        OutPoint { hash: hash, n: n }
    }
}

/// An input of a transaction. It contains the location of the previous
/// transaction's output that it claims and a signature that matches the
/// output's public key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxIn {
    pub previous_out: Option<OutPoint>,
    pub sequence: u32,
    pub script_signature: Option<Script>,
}

impl TxIn {
    /// Creates a new TxIn instance
    pub fn new() -> TxIn {
        TxIn {
            previous_out: None,
            sequence: 0,
            script_signature: None,
        }
    }
}

/// An output of a transaction. It contains the public key that the next input
/// must be able to sign with to claim it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxOut {
    pub value: Option<Asset>,
    pub script_public_key: Option<Script>,
}

impl TxOut {
    /// Creates a new TxOut instance
    pub fn new() -> TxOut {
        TxOut {
            value: None,
            script_public_key: None,
        }
    }
}

/// The basic transaction that is broadcasted on the network and contained in
/// blocks. A transaction can contain multiple inputs and outputs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub inputs: Vec<TxIn>,
    pub outputs: Vec<TxOut>,
    pub version: i32,
    pub lock_time: u32,
}

impl Transaction {
    /// Creates a new Transaction instance
    pub fn new() -> Transaction {
        Transaction {
            inputs: Vec::new(),
            outputs: Vec::new(),
            version: 0,
            lock_time: 0,
        }
    }

    /// Gets the total value of all outputs and checks that it is within the
    /// possible amounts set by chain system
    fn get_output_value(&mut self) -> u64 {
        let mut total_value: u64 = 0;

        for txout in &mut self.outputs {
            if txout.value.is_some() {
                // we're safe to unwrap here
                let this_value = txout.value.clone().unwrap();

                if let Asset::Token(token_val) = this_value {
                    if !is_valid_amount(&token_val) {
                        panic!("TxOut value {value} out of range", value = token_val);
                    }

                    total_value += token_val;

                    if !is_valid_amount(&total_value) {
                        panic!(
                            "Total TxOut value of {value} out of range",
                            value = total_value
                        );
                    }
                }
            }
        }

        total_value
    }

    /// Get the total transaction size in bytes
    fn get_total_size(&self) -> usize {
        let data = Bytes::from(serialize(&self).unwrap());
        data.len()
    }

    /// Returns whether current transaction is a coinbase tx
    fn is_coinbase(&self) -> bool {
        self.inputs.len() == 1 && self.inputs[0].previous_out != None
    }
}
