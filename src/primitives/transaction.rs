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
    pub b_hash: Vec<u8>,
    pub t_hash: Vec<u8>,
    pub prev_n: i32,
    pub signatures: Vec<Signature>,
    pub pub_keys: Vec<PublicKey>,
}

/// An outpoint - a combination of a block hash, transaction hash and an index n into its vout
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutPoint {
    pub b_hash: Vec<u8>,
    pub t_hash: Vec<u8>,
    pub n: i32,
}

// TODO: Hashes are currently Vec<u8>, can be stored some other way
impl OutPoint {
    /// Creates a new outpoint instance
    pub fn new(b_hash: Vec<u8>, t_hash: Vec<u8>, n: i32) -> OutPoint {
        OutPoint {
            b_hash: b_hash,
            t_hash: t_hash,
            n: n,
        }
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
/// must be able to sign with to claim it. It also contains the block hash for the
/// potential DRS if this is a data asset transaction
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxOut {
    pub value: Option<Asset>,
    pub amount: u64,
    pub drs_block_hash: Option<Vec<u8>>,
    pub script_public_key: Option<Vec<u8>>,
}

impl TxOut {
    /// Creates a new TxOut instance
    pub fn new() -> TxOut {
        TxOut {
            value: None,
            amount: 0,
            drs_block_hash: None,
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
    pub druid: Option<Vec<u8>>,
    pub druid_participants: Option<usize>,
    pub expect_value: Option<Asset>,
    pub expect_value_amount: Option<u64>,
}

impl Transaction {
    /// Creates a new Transaction instance
    pub fn new() -> Transaction {
        Transaction {
            inputs: Vec::new(),
            outputs: Vec::new(),
            version: 0,
            druid: None,
            druid_participants: None,
            expect_value: None,
            expect_value_amount: None,
        }
    }

    /// Creates a new Transaction instance from inputs
    ///
    /// ### Arguments
    ///
    /// * `inputs`              - Transaction inputs
    /// * `outputs`             - Transaction outputs
    /// * `version`             - Network version
    /// * `druid`               - DRUID value for a dual double entry
    /// * `expect_value`        - Value expected in return for this payment (only in dual double)
    /// * `expect_value_amount` - Amount of value expected in return for this payment (only in dual double)
    pub fn new_from_input(
        inputs: Vec<TxIn>,
        outputs: Vec<TxOut>,
        version: usize,
        druid: Option<Vec<u8>>,
        druid_participants: Option<usize>,
        expect_value: Option<Asset>,
        expect_value_amount: Option<u64>,
    ) -> Transaction {
        Transaction {
            inputs: inputs,
            outputs: outputs,
            version: version,
            druid: druid,
            druid_participants: druid_participants,
            expect_value: expect_value,
            expect_value_amount: expect_value_amount,
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
