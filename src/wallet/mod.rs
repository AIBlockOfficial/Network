use crate::constants::{ADDRESS_KEY, WALLET_PATH};
use bincode::{deserialize, serialize};
use bytes::Bytes;
use naom::primitives::transaction::Transaction;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::io::Error;
use tokio::task;

/// Data structure for wallet storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletStore {
    pub secret_key: SecretKey,
    pub transactions: Vec<Transaction>,
    pub net: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressStore {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

/// Generates a new payment address, saving the related keys to the wallet
///
/// ### Arguments
///
/// * `net`     - Network version
pub async fn generate_payment_address(net: u8) -> String {
    let (pk, sk) = sign::gen_keypair();
    let final_address = construct_address(pk, net);
    let address_keys = AddressStore {
        public_key: pk,
        secret_key: sk,
    };

    let save_result = save_address_to_wallet(final_address.clone(), address_keys).await;
    if save_result.is_err() {
        panic!("Error writing address to wallet");
    }

    final_address
}

/// Saves an address and its ancestor keys to the wallet
///
/// ### Arguments
///
/// * `address` - Address to save to wallet
/// * `keys`    - Address-related keys to save
pub async fn save_address_to_wallet(address: String, keys: AddressStore) -> Result<(), Error> {
    Ok(task::spawn_blocking(move || {
        let mut address_list: BTreeMap<String, AddressStore> = BTreeMap::new();

        // Wallet DB handling
        let db = DB::open_default(WALLET_PATH).unwrap();
        let address_list_state = match db.get(ADDRESS_KEY) {
            Ok(Some(list)) => Some(deserialize(&list).unwrap()),
            Ok(None) => None,
            Err(e) => panic!("Failed to access the wallet database with error: {:?}", e),
        };

        if let Some(list) = address_list_state {
            address_list = list;
        }

        // Assign the new address to the store
        address_list.insert(address.clone(), keys);

        // Save to disk
        db.put(ADDRESS_KEY, Bytes::from(serialize(&address_list).unwrap()))
            .unwrap();
        let _ = DB::destroy(&Options::default(), WALLET_PATH);
    })
    .await?)
}

/// Saves an address and the associated transactions with it to the wallet
///
/// ### Arguments
///
/// * `address`         - Address to save to wallet
/// * `save_content`    - The content to save for the given address
pub async fn save_transactions_to_wallet(
    address: String,
    save_content: WalletStore,
) -> Result<(), Error> {
    Ok(task::spawn_blocking(move || {
        let db = DB::open_default(WALLET_PATH).unwrap();
        let mut wallet_content = save_content.clone();

        // Check whether the address was used before
        let existing_address_content: Option<WalletStore> = match db.get(address.clone()) {
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

        db.put(address.clone(), hash_input).unwrap();
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
pub fn construct_address(pub_key: PublicKey, net: u8) -> String {
    let first_pubkey_bytes = Bytes::from(serialize(&pub_key).unwrap());
    let mut first_hash = Sha3_256::digest(&first_pubkey_bytes).to_vec();

    // TODO: Add RIPEMD

    first_hash.insert(0, net as u8);
    let mut second_hash = Sha3_256::digest(&first_hash).to_vec();
    second_hash.truncate(16);

    hex::encode(second_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Creating a valid payment address
    fn should_construct_address_valid() {
        let pk = PublicKey([
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ]);
        let addr = construct_address(pk, 0);

        assert_eq!(addr, "fd86f2230f4fd5bfd9cd882732792279".to_string());
    }

    #[test]
    /// Creating a payment address of 25 bytes
    fn should_construct_address_valid_length() {
        let pk = PublicKey([
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ]);
        let addr = construct_address(pk, 0);

        assert_eq!(addr.len(), 32);
    }
}
