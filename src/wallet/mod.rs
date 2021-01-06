use crate::configurations::DbMode;
use crate::constants::{
    ADDRESS_KEY, DB_PATH_LIVE, DB_PATH_TEST, FUND_KEY, NETWORK_VERSION, WALLET_PATH,
};
use crate::db_utils::SimpleDb;
use bincode::{deserialize, serialize};
use naom::primitives::asset::TokenAmount;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::io::Error;
use std::sync::{Arc, Mutex};
use tokio::task;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaymentAddress {
    pub address: String,
    pub net: u8,
}

/// Data structure for wallet storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStore {
    pub address: String,
    pub net: u8,
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

    /// Creates a new DB instance for a given environment, including construction and
    /// teardown
    ///
    /// ### Arguments
    ///
    /// * `db_mode` - The environment to set the DB up in
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
    pub async fn generate_payment_address(&self) -> (PaymentAddress, AddressStore) {
        let (public_key, secret_key) = sign::gen_keypair();
        let final_address = construct_address(public_key, NETWORK_VERSION);
        let address_keys = AddressStore {
            public_key,
            secret_key,
        };

        let save_result = self
            .save_address_to_wallet(final_address.address.clone(), address_keys.clone())
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
            db.put(ADDRESS_KEY, &serialize(&address_list).unwrap())
                .unwrap();
        })
        .await?)
    }

    /// Saves an address and the associated transaction with it to the wallet
    ///
    /// ### Arguments
    ///
    /// * `tx_hash`  - Transaction hash
    /// * `address`  - Transaction Address
    pub async fn save_transaction_to_wallet(
        &self,
        tx_hash: String,
        address: PaymentAddress,
    ) -> Result<(), Error> {
        let PaymentAddress { address, net } = address;
        let tx_store = TransactionStore { address, net };
        let tx_to_save = Some((tx_hash, tx_store)).into_iter().collect();

        self.save_transactions_to_wallet(tx_to_save).await
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

            for (key, value) in tx_to_save {
                let input = serialize(&value).unwrap();
                db.put(&key, &input).unwrap();
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

            println!("Testing payment to wallet");
            // Save to disk
            db.put(FUND_KEY, &serialize(&fund_store).unwrap()).unwrap();
        })
        .await?)
    }

    pub fn get_fund_store(&self) -> Option<FundStore> {
        match self.db.lock().unwrap().get(FUND_KEY) {
            Ok(Some(list)) => Some(deserialize(&list).unwrap()),
            Ok(None) => None,
            Err(e) => panic!("Failed to access the wallet database with error: {:?}", e),
        }
    }

    pub fn set_fund_store(&self, fund_store: FundStore) {
        self.db
            .lock()
            .unwrap()
            .put(FUND_KEY, &serialize(&fund_store).unwrap())
            .unwrap();
    }

    pub fn get_address_stores(&self) -> BTreeMap<String, AddressStore> {
        match self.db.lock().unwrap().get(ADDRESS_KEY) {
            Ok(Some(list)) => deserialize(&list).unwrap(),
            Ok(None) => panic!("No address store present in wallet"),
            Err(e) => panic!("Error accessing wallet: {:?}", e),
        }
    }

    pub fn set_address_stores(&self, address_store: BTreeMap<String, AddressStore>) {
        self.db
            .lock()
            .unwrap()
            .put(ADDRESS_KEY, &serialize(&address_store).unwrap())
            .unwrap();
    }

    pub fn get_transaction_store(&self, tx_hash: &str) -> TransactionStore {
        match self.db.lock().unwrap().get(tx_hash) {
            Ok(Some(list)) => deserialize(&list).unwrap(),
            Ok(None) => panic!("No address store present in wallet"),
            Err(e) => panic!("Error accessing wallet: {:?}", e),
        }
    }

    pub fn delete_key(&self, key: &str) {
        self.db.lock().unwrap().delete(key).unwrap();
    }
}

/// Builds an address from a public key
///
/// ### Arguments
///
/// * `pub_key` - A public key to build an address from
/// * `net`     - Network version
pub fn construct_address(pub_key: PublicKey, net: u8) -> PaymentAddress {
    let first_pubkey_bytes = serialize(&pub_key).unwrap();
    let mut first_hash = Sha3_256::digest(&first_pubkey_bytes).to_vec();

    // TODO: Add RIPEMD

    first_hash.insert(0, net);
    let mut second_hash = Sha3_256::digest(&first_hash).to_vec();
    second_hash.truncate(16);

    PaymentAddress {
        address: hex::encode(second_hash),
        net,
    }
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

        assert_eq!(
            addr,
            PaymentAddress {
                address: "fd86f2230f4fd5bfd9cd882732792279".to_string(),
                net: 0
            }
        );
    }

    #[test]
    /// Creating a payment address of 25 bytes
    fn should_construct_address_valid_length() {
        let pk = PublicKey([
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ]);
        let addr = construct_address(pk, 0);

        assert_eq!(addr.address.len(), 32);
    }
}
