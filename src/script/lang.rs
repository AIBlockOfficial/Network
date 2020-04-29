use crate::script::{OpCodes, StackEntry};
use crate::sha3::Digest;
use serde::{Deserialize, Serialize};
use sha3::Sha3_256;
use sodiumoxide::crypto::sign::PublicKey;
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

    /// Constructs a pay to public key hash script
    ///
    /// ### Arguments
    ///
    /// * `signature`   - Signature of the payer
    /// * `pub_key`     - Public key of the payer
    pub fn pay2pkh(signature: Vec<u8>, pub_key: PublicKey) -> Script {
        let mut new_script = Script::new();
        let new_key = Sha3_256::digest(&pub_key.0).to_vec();

        new_script.stack.push(StackEntry::Signature(signature));
        new_script.stack.push(StackEntry::PubKey(new_key));
        new_script.stack.push(StackEntry::Op(OpCodes::OP_DUP));
        new_script.stack.push(StackEntry::Op(OpCodes::OP_HASH256));
        new_script
            .stack
            .push(StackEntry::Op(OpCodes::OP_EQUALVERIFY));
        new_script.stack.push(StackEntry::Op(OpCodes::OP_CHECKSIG));

        new_script
    }

    /// Constructs a multisig locking script
    ///
    /// ### Arguments
    ///
    /// * `m`           - Number of signatures required to unlock
    /// * `n`           - Number of valid signatures total
    /// * `pub_keys`    - The constituent public keys
    pub fn multisig_lock(m: usize, n: usize, pub_keys: Vec<PublicKey>) -> Script {
        let mut new_script = Script::new();

        if n > pub_keys.len() || m > pub_keys.len() {
            error!("The number of keys required for multisig is greater than the number of keys provided");
        } else if m > n {
            error!("Multisig requiring more keys to lock than the total number of keys");
        } else {
            let mut new_stack = Vec::with_capacity(3 + pub_keys.len());
            let mut stack_entry_keys = Vec::new();

            for key in pub_keys {
                let new_key = Sha3_256::digest(&key.0).to_vec();
                stack_entry_keys.push(StackEntry::PubKey(new_key));
            }

            new_stack.push(StackEntry::Num(m));
            new_stack.append(&mut stack_entry_keys);
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
    /// * `signatures`  - Signatures to unlock with
    pub fn multisig_unlock(signatures: Vec<Vec<u8>>) -> Script {
        let mut new_script = Script::new();
        new_script.stack = vec![StackEntry::Op(OpCodes::OP_0)];

        for sig in signatures {
            new_script.stack.push(StackEntry::Signature(sig));
        }

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
        signatures: Vec<Vec<u8>>,
        pub_keys: Vec<PublicKey>,
    ) -> Script {
        let mut new_script = Script::new();

        if n > pub_keys.len() || m > pub_keys.len() {
            error!("The number of keys required for multisig is greater than the number of keys provided");
        } else if m > n {
            error!("Multisig requiring more keys to lock than the total number of keys");
        } else {
            new_script.stack = vec![StackEntry::Op(OpCodes::OP_0)];

            // Handle signatures
            for sig in signatures {
                new_script.stack.push(StackEntry::Signature(sig));
            }

            new_script.stack.push(StackEntry::Num(m));
            for key in pub_keys {
                let new_key = Sha3_256::digest(&key.0).to_vec();
                new_script.stack.push(StackEntry::PubKey(new_key));
            }
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
