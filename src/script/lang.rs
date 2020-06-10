#![allow(unused)]
use crate::script::{OpCodes, StackEntry};
use crate::sha3::Digest;
use bincode::serialize;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha3::Sha3_256;
use sodiumoxide::crypto::sign::{sign_detached, PublicKey, Signature};
use tracing::{error, warn};

/// Scripts are defined as a sequence of stack entries
/// NOTE: A tuple struct could probably work here as well
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Script {
    pub stack: Vec<StackEntry>,
}

impl Script {
    /// Constructs a new script
    pub fn new() -> Script {
        Script { stack: Vec::new() }
    }

    /// Constructs a new script for coinbase
    ///
    /// ### Arguments
    ///
    /// * `block_time`  - The block time to push
    pub fn new_for_coinbase(block_time: u32) -> Script {
        let mut coinbase_stack = Vec::new();
        coinbase_stack.push(StackEntry::Num(block_time as usize));

        Script {
            stack: coinbase_stack,
        }
    }

    /// Constructs a pay to public key hash script
    ///
    /// ### Arguments
    ///
    /// * `check_data`  - Check data to provide signature
    /// * `signature`   - Signature of check data
    /// * `pub_key`     - Public key of the payer
    pub fn pay2pkh(check_data: Vec<u8>, signature: Signature, pub_key: PublicKey) -> Script {
        let mut new_script = Script::new();
        let pub_key_stack_entry = StackEntry::PubKey(pub_key);

        let pub_key_bytes = Bytes::from(serialize(&pub_key_stack_entry).unwrap());
        let new_key = Sha3_256::digest(&pub_key_bytes).to_vec();

        new_script.stack.push(StackEntry::Bytes(check_data));
        new_script.stack.push(StackEntry::Signature(signature));
        new_script.stack.push(pub_key_stack_entry.clone());
        new_script.stack.push(StackEntry::Op(OpCodes::OP_DUP));
        new_script.stack.push(StackEntry::Op(OpCodes::OP_HASH256));
        new_script.stack.push(StackEntry::PubKeyHash(new_key));
        new_script
            .stack
            .push(StackEntry::Op(OpCodes::OP_EQUALVERIFY));
        new_script.stack.push(StackEntry::Op(OpCodes::OP_CHECKSIG));

        new_script
    }

    /// Constructs one part of a multiparty transaction script
    ///
    /// ### Arguments
    ///
    /// * `check_data`  - Data to be signed for verification
    /// * `pub_key`     - Public key of this party
    /// * `signature`   - Signature of this party
    pub fn member_multisig(
        check_data: Vec<u8>,
        pub_key: PublicKey,
        signature: Signature,
    ) -> Script {
        let mut new_script = Script::new();

        new_script.stack.push(StackEntry::Bytes(check_data));
        new_script.stack.push(StackEntry::Signature(signature));
        new_script.stack.push(StackEntry::PubKey(pub_key));
        new_script.stack.push(StackEntry::Op(OpCodes::OP_CHECKSIG));

        new_script
    }

    /// Constructs a multisig locking script
    ///
    /// ### Arguments
    ///
    /// * `m`           - Number of signatures required to unlock
    /// * `n`           - Number of valid signatures total
    /// * `check_data`  - Data to have checked against signatures
    /// * `pub_keys`    - The constituent public keys
    pub fn multisig_lock(
        m: usize,
        n: usize,
        check_data: Vec<u8>,
        pub_keys: Vec<PublicKey>,
    ) -> Script {
        let mut new_script = Script::new();

        if n > pub_keys.len() || m > pub_keys.len() {
            error!("The number of keys required for multisig is greater than the number of keys provided");
        } else if m > n {
            error!("Multisig requiring more keys to lock than the total number of keys");
        } else {
            let mut new_stack = Vec::with_capacity(3 + pub_keys.len());

            new_stack.push(StackEntry::Bytes(check_data));
            new_stack.push(StackEntry::Num(m));
            new_stack.append(
                &mut pub_keys
                    .clone()
                    .iter()
                    .map(|e| StackEntry::PubKey(*e))
                    .collect(),
            );
            new_stack.push(StackEntry::Num(n));
            new_stack.push(StackEntry::Op(OpCodes::OP_CHECKMULTISIG));

            new_script.stack = new_stack;
        }

        new_script
    }

    /// Constructs a multisig unlocking script
    ///
    /// ### Arguments
    ///
    /// * `check_data`  - Data to have signed
    /// * `signatures`  - Signatures to unlock with
    pub fn multisig_unlock(check_data: Vec<u8>, signatures: Vec<Signature>) -> Script {
        let mut new_script = Script::new();
        new_script.stack = vec![StackEntry::Bytes(check_data)];
        new_script.stack.append(
            &mut signatures
                .iter()
                .map(|e| StackEntry::Signature(*e))
                .collect(),
        );

        new_script
    }

    /// Constructs a multisig validation script
    ///
    /// ### Arguments
    ///
    /// * `m`           - Number of signatures to assure validity
    /// * `n`           - Number of public keys that are valid
    /// * `signatures`  - Signatures to validate
    /// * `pub_keys`    - Public keys to validate
    pub fn multisig_validation(
        m: usize,
        n: usize,
        check_data: Vec<u8>,
        signatures: Vec<Signature>,
        pub_keys: Vec<PublicKey>,
    ) -> Script {
        let mut new_script = Script::new();

        if n > pub_keys.len() || m > pub_keys.len() {
            error!("The number of keys required for multisig is greater than the number of keys provided");
        } else if m > n {
            error!("Multisig requiring more keys to lock than the total number of keys");
        } else {
            new_script.stack = vec![StackEntry::Bytes(check_data)];

            // Handle signatures
            new_script.stack.append(
                &mut signatures
                    .iter()
                    .map(|e| StackEntry::Signature(*e))
                    .collect(),
            );

            new_script.stack.push(StackEntry::Num(m));

            // Handle pub keys
            new_script
                .stack
                .append(&mut pub_keys.iter().map(|e| StackEntry::PubKey(*e)).collect());
            new_script.stack.push(StackEntry::Num(n));
            new_script
                .stack
                .push(StackEntry::Op(OpCodes::OP_CHECKMULTISIG));
        }

        new_script
    }

    /// Checks whether all ops codes within script are valid
    pub fn has_valid_ops(&self) -> bool {
        warn!("has_valid_ops not implemented");
        for entry in &self.stack {
            // TODO
        }

        true
    }

    /// Gets the op_code for a specific data entry, if it exists
    pub fn get_op_code(&self, entry: &u8) -> Option<u8> {
        let mut op_code = OpCodes::OP_INVALIDOPCODE as u8;
        println!("get_op_code not implemented");

        // TODO

        Some(op_code)
    }
}
