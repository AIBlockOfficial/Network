use crate::configurations::DbMode;
use crate::constants::{ADDRESS_KEY, DB_PATH_LIVE, DB_PATH_TEST, FUND_KEY, WALLET_PATH};
use crate::db_utils::SimpleDb;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use naom::primitives::asset::TokenAmount;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::io::Error;
use std::sync::{Arc, Mutex};
use tokio::task;

/// Data structure for wallet storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStore {
    pub address: String,
    pub net: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressStore {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

/// A reference to fund stores, where `transactions` contains the hash
/// of the transaction and a `u64` of its holding amount
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundStore {
    pub running_total: TokenAmount,
    pub transactions: BTreeMap<String, TokenAmount>,
}

#[derive(Debug, Clone)]
pub struct WalletDb {
    pub db: Arc<Mutex<SimpleDb>>,
}

impl WalletDb {
    pub fn new(db_mode: DbMode) -> Self {
        Self {
            db: Arc::new(Mutex::new(Self::new_db(db_mode))),
        }
    }

    fn new_db(db_mode: DbMode) -> SimpleDb {
        let save_path = match db_mode {
            DbMode::Live => format!("{}/{}", WALLET_PATH, DB_PATH_LIVE),
            DbMode::Test(idx) => format!("{}/{}.{}", WALLET_PATH, DB_PATH_TEST, idx),
            DbMode::InMemory => {
                return SimpleDb::new_in_memory();
            }
        };

        SimpleDb::new_file(save_path).unwrap()
    }

    /// Generates a new payment address, saving the related keys to the wallet
    /// TODO: Add static address capability for frequent payments
    ///
    /// ### Arguments
    ///
    /// * `net`     - Network version
    pub async fn generate_payment_address(&self, net: u8) -> (String, AddressStore) {
        let (pk, sk) = sign::gen_keypair();
        let final_address = construct_address(pk, net);
        let address_keys = AddressStore {
            public_key: pk,
            secret_key: sk,
        };

        let save_result = self
            .save_address_to_wallet(final_address.clone(), address_keys.clone())
            .await;
        if save_result.is_err() {
            panic!("Error writing address to wallet");
        }

        (final_address, address_keys)
    }

    /// Saves an address and its ancestor keys to the wallet
    ///
    /// ### Arguments
    ///
    /// * `address` - Address to save to wallet
    /// * `keys`    - Address-related keys to save
    async fn save_address_to_wallet(
        &self,
        address: String,
        keys: AddressStore,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut address_list: BTreeMap<String, AddressStore> = BTreeMap::new();

            // Wallet DB handling
            let mut db = db.lock().unwrap();
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
        })
        .await?)
    }

    /// Saves an address and the associated transactions with it to the wallet
    ///
    /// ### Arguments
    ///
    /// * `tx_to_save`  - All transactions that are required to be saved to wallet
    pub async fn save_transactions_to_wallet(
        &self,
        tx_to_save: BTreeMap<String, TransactionStore>,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            let keys: Vec<_> = tx_to_save.keys().cloned().collect();

            for key in keys {
                let input = Bytes::from(serialize(&tx_to_save.get(&key).unwrap()).unwrap());
                db.put(key.clone(), input).unwrap();
            }
        })
        .await?)
    }

    /// Saves a received payment to the local wallet
    ///
    /// ### Arguments
    ///
    /// * `hash`    - Hash of the transaction
    /// * `amount`  - Amount of tokens in the payment
    pub async fn save_payment_to_wallet(
        &self,
        hash: String,
        amount: TokenAmount,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut fund_store = FundStore {
                running_total: TokenAmount(0),
                transactions: BTreeMap::new(),
            };

            // Wallet DB handling
            let mut db = db.lock().unwrap();
            let fund_store_state = match db.get(FUND_KEY) {
                Ok(Some(list)) => Some(deserialize(&list).unwrap()),
                Ok(None) => None,
                Err(e) => panic!("Failed to access the wallet database with error: {:?}", e),
            };
            if let Some(store) = fund_store_state {
                fund_store = store;
            }
            // Update the running total and add the transaction to the tab list
            fund_store.running_total.0 += amount.0;
            fund_store.transactions.insert(hash, amount);
            // Save to disk
            db.put(FUND_KEY, Bytes::from(serialize(&fund_store).unwrap()))
                .unwrap();
        })
        .await?)
    }
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
