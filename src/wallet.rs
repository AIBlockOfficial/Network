use crate::constants::WALLET_PATH;
use crate::primitives::transaction::Transaction;
use crate::sha3::Digest;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use sha3::Sha3_256;
use sodiumoxide::crypto::sign::{PublicKey, SecretKey};
use std::io::Error;
use tokio::task;

/// Data structure for wallet storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletStore {
    pub secret_key: SecretKey,
    pub transactions: Vec<Transaction>,
    pub net: usize,
}

/// Saves an address and the associated data with it to the wallet
///
/// ### Arguments
///
/// * `address`         - Address to save to wallet
/// * `save_content`    - The content to save for the given address
pub async fn save_to_wallet(address: Vec<u8>, save_content: WalletStore) -> Result<(), Error> {
    Ok(task::spawn_blocking(move || {
        let hash_key = Sha3_256::digest(&address);

        let db = DB::open_default(WALLET_PATH).unwrap();
        let mut wallet_content = save_content.clone();

        // Check whether the address was used before
        let existing_address_content: Option<WalletStore> = match db.get(hash_key.clone()) {
            Ok(Some(content)) => Some(deserialize(&content).unwrap()),
            Ok(None) => None,
            Err(e) => panic!("Failed to get address from wallet with error: {:?}", e),
        };

        // Update transactions if pre-existing
        if let Some(content) = existing_address_content {
            wallet_content
                .transactions
                .append(&mut content.transactions.clone());
        }

        let hash_input = Bytes::from(serialize(&wallet_content).unwrap());

        db.put(hash_key, hash_input).unwrap();
        let _ = DB::destroy(&Options::default(), WALLET_PATH);
    })
    .await?)
}

/// Builds an address from a public key
///
/// ### Arguments
///
/// * `pub_key` - A public key to build an address from
/// * `net`     - Network version
pub fn create_address(pub_key: PublicKey, net: usize) -> Vec<u8> {
    let first_pubkey_bytes = Bytes::from(serialize(&pub_key).unwrap());
    let mut first_hash = Sha3_256::digest(&first_pubkey_bytes).to_vec();

    // TODO: Add RIPEMD

    first_hash.insert(0, net as u8);
    let mut second_hash = Sha3_256::digest(&first_hash).to_vec();
    second_hash.truncate(25);

    second_hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Creating a valid payment address
    fn should_create_address_valid() {
        let pk = PublicKey([
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ]);
        let addr = create_address(pk, 0);

        assert_eq!(
            addr,
            [
                253, 134, 242, 35, 15, 79, 213, 191, 217, 205, 136, 39, 50, 121, 34, 121, 166, 73,
                234, 222, 126, 206, 175, 96, 248
            ]
        );
    }

    #[test]
    /// Creating a payment address of 25 bytes
    fn should_create_address_valid_length() {
        let pk = PublicKey([
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ]);
        let addr = create_address(pk, 0);

        assert_eq!(addr.len(), 25);
    }
}
