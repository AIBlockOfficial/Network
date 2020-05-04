use crate::db::utils::tx_has_spent;
use crate::interfaces::Asset;
use crate::primitives::transaction::*;
use crate::script::lang::Script;
use crate::script::{OpCodes, StackEntry};
use crate::sha3::Digest;
use bincode::serialize;
use bytes::Bytes;
use sha3::Sha3_256;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, Signature};
use tracing::{debug, error, info};

/// Verifies that all incoming tx_ins are allowed to be spent. Returns false if a single
/// transaction doesn't verify
///
/// ### Arguments
///
/// * `tx_ins`  - Tx_ins to verify
pub fn tx_ins_are_valid(tx_ins: Vec<TxIn>) -> bool {
    for tx_in in tx_ins {
        if !tx_has_valid_p2pkh_sig(tx_in.script_signature) || tx_has_spent(tx_in.previous_out) {
            return false;
        }
    }

    true
}

/// Checks whether a transaction to spend tokens in P2PKH has a valid signature
///
/// ### Arguments
///
/// * `script`  - Script to validate
fn tx_has_valid_p2pkh_sig(script: Script) -> bool {
    let mut current_stack: Vec<StackEntry> = Vec::with_capacity(script.stack.len());

    for stack_entry in script.stack {
        match stack_entry {
            StackEntry::Op(OpCodes::OP_DUP) => {
                println!("Duplicating last entry in script stack");
                let dup = current_stack[current_stack.len() - 1].clone();
                current_stack.push(dup);
            }
            StackEntry::Op(OpCodes::OP_HASH256) => {
                println!("256 bit hashing last stack entry");
                let last_entry = current_stack.pop().unwrap();
                let pub_key_bytes = Bytes::from(serialize(&last_entry).unwrap());
                let new_entry = Sha3_256::digest(&pub_key_bytes).to_vec();

                current_stack.push(StackEntry::PubKeyHash(new_entry));
            }
            StackEntry::Op(OpCodes::OP_EQUALVERIFY) => {
                println!("Verifying p2pkh hash");
                let input_hash = current_stack.pop();
                let computed_hash = current_stack.pop();

                if input_hash != computed_hash {
                    error!("Hash not valid. Transaction input invalid");
                    return false;
                }
            }
            StackEntry::Op(OpCodes::OP_CHECKSIG) => {
                println!("Checking p2pkh signature");
                let pub_key: PublicKey = match current_stack.pop().unwrap() {
                    StackEntry::PubKey(pub_key) => pub_key,
                    _ => panic!("Public key not present to verify transaction"),
                };

                let sig: Signature = match current_stack.pop().unwrap() {
                    StackEntry::Signature(sig) => sig,
                    _ => panic!("Signature not present to verify transaction"),
                };

                let check_data = match current_stack.pop().unwrap() {
                    StackEntry::Bytes(check_data) => check_data,
                    _ => panic!("Check data bytes not present to verify transaction"),
                };

                if (!sign::verify_detached(&sig, &check_data, &pub_key)) {
                    error!("Signature not valid. Transaction input invalid");
                    return false;
                }
            }
            _ => {
                println!("Adding constant to stack: {:?}", stack_entry);
                current_stack.push(stack_entry);
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Util function to create TxIns
    fn create_payment_tx_ins(tx_values: Vec<TxConstructor>) -> Vec<TxIn> {
        let mut tx_ins = Vec::new();

        for entry in tx_values {
            let mut new_tx_in = TxIn::new();
            new_tx_in.script_signature =
                Script::pay2pkh(entry.prev_hash.clone(), entry.signature, entry.pub_key);
            new_tx_in.previous_out = Some(OutPoint::new(entry.prev_hash, entry.prev_n));

            tx_ins.push(new_tx_in);
        }

        tx_ins
    }

    #[test]
    /// Checks that correct p2pkh transaction signatures are validated as such
    fn should_pass_p2pkh_sig_valid() {
        let (pk, sk) = sign::gen_keypair();
        let prev_hash = vec![0, 0, 0];
        let signature = sign::sign_detached(&prev_hash.clone(), &sk);

        let tx_const = TxConstructor {
            prev_hash: prev_hash,
            prev_n: 0,
            signature: signature,
            pub_key: pk,
        };

        let tx_ins = create_payment_tx_ins(vec![tx_const]);

        assert!(tx_has_valid_p2pkh_sig(tx_ins[0].clone().script_signature));
    }

    #[test]
    /// Checks that invalid p2pkh transaction signatures are validated as such
    fn should_fail_p2pkh_sig_invalid() {
        let (pk, sk) = sign::gen_keypair();
        let (second_pk, _s) = sign::gen_keypair();
        let prev_hash = vec![0, 0, 0];
        let signature = sign::sign_detached(&prev_hash.clone(), &sk);

        let tx_const = TxConstructor {
            prev_hash: prev_hash,
            prev_n: 0,
            signature: signature,
            pub_key: second_pk,
        };

        let tx_ins = create_payment_tx_ins(vec![tx_const]);

        assert_eq!(
            tx_has_valid_p2pkh_sig(tx_ins[0].clone().script_signature),
            false
        );
    }
}
