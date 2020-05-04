#![allow(unused)]
use bincode::serialize;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::sign::ed25519::{PublicKey, Signature};

use crate::interfaces::Asset;
use crate::script::lang::Script;
use crate::script::{OpCodes, StackEntry};
use crate::utils::is_valid_amount;

/// A user-friendly construction struct for a TxIn
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxConstructor {
    pub prev_hash: Vec<u8>,
    pub prev_n: i32,
    pub signature: Signature,
    pub pub_key: PublicKey,
}

/// An outpoint - a combination of a transaction hash and an index n into its vout
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutPoint {
    pub hash: Vec<u8>,
    pub n: i32,
}

// TODO: Hashes are currently Vec<u8>, can be stored some other way
impl OutPoint {
    /// Creates a new outpoint instance
    pub fn new(hash: Vec<u8>, n: i32) -> OutPoint {
        OutPoint { hash: hash, n: n }
    }
}

/// An input of a transaction. It contains the location of the previous
/// transaction's output that it claims and a signature that matches the
/// output's public key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxIn {
    pub previous_out: Option<OutPoint>,
    pub script_signature: Script,
}

impl TxIn {
    /// Creates a new TxIn instance
    pub fn new() -> TxIn {
        let mut script_sig = Script::new();
        script_sig.stack.push(StackEntry::Op(OpCodes::OP_0));

        TxIn {
            previous_out: None,
            script_signature: script_sig,
        }
    }

    /// Creates a new TxIn instance from provided inputs
    ///
    /// ### Arguments
    ///
    /// * `previous_out`    - Outpoint of the previous transaction
    /// * `script_sig`      - Script signature of the previous outpoint
    pub fn new_from_input(previous_out: OutPoint, script_sig: Script) -> TxIn {
        TxIn {
            previous_out: Some(previous_out),
            script_signature: script_sig,
        }
    }
}

/// An output of a transaction. It contains the public key that the next input
/// must be able to sign with to claim it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxOut {
    pub value: Option<Asset>,
    pub script_public_key: Option<Vec<u8>>,
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
    pub version: usize,
}

impl Transaction {
    /// Creates a new Transaction instance
    pub fn new() -> Transaction {
        Transaction {
            inputs: Vec::new(),
            outputs: Vec::new(),
            version: 0,
        }
    }

    /// Creates a new Transaction instance from inputs
    ///
    /// ### Arguments
    ///
    /// * `inputs`  - Transaction inputs
    /// * `outputs` - Transaction outputs
    /// * `version` - Network version
    pub fn new_from_input(inputs: Vec<TxIn>, outputs: Vec<TxOut>, version: usize) -> Transaction {
        Transaction {
            inputs: inputs,
            outputs: outputs,
            version: version,
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
