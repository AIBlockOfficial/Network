use crate::configurations::{DbMode, WalletTxSpec};
use crate::constants::{FUND_KEY, KNOWN_ADDRESS_KEY, WALLET_PATH};
use crate::db_utils::{self, DBError, SimpleDb};
use crate::utils::make_wallet_tx_info;
use bincode::{deserialize, serialize};
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::{OutPoint, TxConstructor, TxIn};
use naom::primitives::transaction_utils::{construct_address, construct_payment_tx_ins};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::{PublicKey, SecretKey};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Error;
use std::sync::{Arc, Mutex};
use tokio::task;

mod fund_store;
pub use fund_store::FundStore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressStore {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStore {
    pub key_address: String,
}

#[derive(Debug, Clone)]
pub struct WalletDb {
    db: Arc<Mutex<SimpleDb>>,
}

impl WalletDb {
    pub fn new(db_mode: DbMode, db: Option<SimpleDb>) -> Self {
        let db = db.unwrap_or_else(|| db_utils::new_db(db_mode, WALLET_PATH, ""));
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }

    pub async fn with_seed(self, index: usize, seeds: &[Vec<WalletTxSpec>]) -> Self {
        let seeds = if let Some(seeds) = seeds.get(index) {
            seeds
        } else {
            return self;
        };

        for seed in seeds {
            let (tx_out_p, pk, sk, amount) = make_wallet_tx_info(seed);
            let (address, _) = self.store_payment_address(pk, sk).await;
            let payments = vec![(tx_out_p, amount, address)];
            self.save_usable_payments_to_wallet(payments).await.unwrap();
        }
        self
    }

    /// Extract persistent storage of a closed raft
    pub async fn take_closed_persistent_store(&mut self) -> SimpleDb {
        std::mem::replace(&mut self.db.lock().unwrap(), SimpleDb::new_in_memory())
    }

    /// Generates a new payment address, saving the related keys to the wallet
    /// TODO: Add static address capability for frequent payments
    pub async fn generate_payment_address(&self) -> (String, AddressStore) {
        let (public_key, secret_key) = sign::gen_keypair();
        self.store_payment_address(public_key, secret_key).await
    }

    /// Store a new payment address, saving the related keys to the wallet
    pub async fn store_payment_address(
        &self,
        public_key: PublicKey,
        secret_key: SecretKey,
    ) -> (String, AddressStore) {
        let final_address = construct_address(public_key);
        let address_keys = AddressStore {
            public_key,
            secret_key,
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
            // Wallet DB handling
            let mut db = db.lock().unwrap();

            let mut address_list = get_known_key_address(&db);
            address_list.insert(address.clone());

            // Save to disk
            save_address_store_to_wallet(&mut db, &address, &keys);
            set_known_key_address(&mut db, address_list);
        })
        .await?)
    }

    /// Saves an address and the associated transaction with it to the wallet
    ///
    /// ### Arguments
    ///
    /// * `out_p`        - Transaction hash/index
    /// * `key_address`  - Transaction Address
    pub async fn save_transaction_to_wallet(
        &self,
        out_p: OutPoint,
        key_address: String,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            let store = TransactionStore { key_address };
            save_transaction_to_wallet(&mut db, &out_p, &store);
        })
        .await?)
    }

    /// Saves a received payment to the local wallet
    ///
    /// ### Arguments
    ///
    /// * `payments - Payments OutPoint, amount and receiver key address
    pub async fn save_usable_payments_to_wallet(
        &self,
        payments: Vec<(OutPoint, TokenAmount, String)>,
    ) -> Result<Vec<(OutPoint, TokenAmount, String)>, Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();

            let usable_payments: Vec<_> = {
                let addresses = get_known_key_address(&db);
                payments
                    .into_iter()
                    .filter(|(_, _, address)| addresses.contains(address))
                    .collect()
            };

            for (out_p, _, key_address) in &usable_payments {
                let key_address = key_address.clone();
                let store = TransactionStore { key_address };
                save_transaction_to_wallet(&mut db, out_p, &store);
            }
            save_payment_to_fund_store(
                &mut db,
                usable_payments
                    .clone()
                    .into_iter()
                    .map(|(out_p, amount, _)| (out_p, amount)),
            );

            usable_payments
        })
        .await?)
    }

    /// Fetches valid TxIns based on the wallet's running total and available unspent
    /// transactions, and total value
    ///
    /// TODO: Replace errors here with Error enum types that the Result can return
    /// TODO: Possibly sort addresses found ascending, so that smaller amounts are consumed
    ///
    /// ### Arguments
    ///
    /// * `amount_required` - Amount needed
    pub async fn fetch_inputs_for_payment(
        &mut self,
        amount_required: TokenAmount,
    ) -> (Vec<TxConstructor>, TokenAmount, Vec<(OutPoint, String)>) {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let db = db.lock().unwrap();
            fetch_inputs_for_payment_from_db(&db, amount_required)
        })
        .await
        .unwrap()
    }

    /// Consume given used transaction and produce TxIns
    ///
    /// ### Arguments
    ///
    /// * `tx_cons`         - TxIn TxConstructors
    /// * `tx_used`         - TxOut used for TxIns
    pub async fn consume_inputs_for_payment(
        &mut self,
        tx_cons: Vec<TxConstructor>,
        tx_used: Vec<(OutPoint, String)>,
    ) -> Vec<TxIn> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            consume_inputs_for_payment_to_wallet(&mut db, tx_cons, tx_used)
        })
        .await
        .unwrap()
    }

    /// Destroy the used transactions with keys purging them from the wallet
    /// Handle the case where same address is reused for multiple transactions
    pub async fn destroy_spent_transactions_and_keys(
        &mut self,
    ) -> (BTreeSet<String>, BTreeMap<OutPoint, TokenAmount>) {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            destroy_spent_transactions_and_keys(&mut db)
        })
        .await
        .unwrap()
    }

    /// Get a the serialized value stored at given key
    pub async fn get_db_value(&self, key: &'static str) -> Option<Vec<u8>> {
        let db = self.db.clone();
        task::spawn_blocking(move || db.lock().unwrap().get(key).unwrap())
            .await
            .unwrap()
    }

    /// Set a the serialized value stored at given key
    pub async fn set_db_value(&self, key: &'static str, value: Vec<u8>) {
        let db = self.db.clone();
        task::spawn_blocking(move || db.lock().unwrap().put(key, &value).unwrap())
            .await
            .unwrap()
    }

    /// Delete value stored at given key
    pub async fn delete_db_value(&self, key: &'static str) {
        let db = self.db.clone();
        task::spawn_blocking(move || db.lock().unwrap().delete(key).unwrap())
            .await
            .unwrap()
    }

    /// Get the wallet fund store
    pub fn get_fund_store(&self) -> FundStore {
        get_fund_store(&self.db.lock().unwrap())
    }

    /// Get the wallet fund store with errors
    pub fn get_fund_store_err(&self) -> Result<FundStore, DBError> {
        get_fund_store_err(&self.db.lock().unwrap())
    }

    /// Get the wallet address
    pub fn get_transaction_store(&self, out_p: &OutPoint) -> TransactionStore {
        get_transaction_store(&self.db.lock().unwrap(), out_p)
    }

    /// Gets the address store based on a provided key
    ///
    /// ### Arguments
    /// 
    ///  * `key_addr` - Key to get the address store for
    pub fn get_address_store(&self, key_addr: &str) -> AddressStore {
        get_address_store(&self.db.lock().unwrap(), key_addr)
    }

    /// Get the wallet addresses
    pub fn get_known_addresses(&self) -> Vec<String> {
        get_known_key_address(&self.db.lock().unwrap())
            .into_iter()
            .collect()
    }

    /// Get the wallet transaction address
    pub fn get_transaction_address(&self, out_p: &OutPoint) -> String {
        self.get_transaction_store(out_p).key_address
    }
}

/// Get the wallet fund store
pub fn get_fund_store(db: &SimpleDb) -> FundStore {
    match get_fund_store_err(db) {
        Ok(v) => v,
        Err(e) => panic!("Failed to access the wallet database with error: {:?}", e),
    }
}

/// Get the wallet fund store
pub fn get_fund_store_err(db: &SimpleDb) -> Result<FundStore, DBError> {
    match db.get(FUND_KEY) {
        Ok(Some(list)) => Ok(deserialize(&list).unwrap()),
        Ok(None) => Ok(FundStore::default()),
        Err(e) => Err(e),
    }
}

/// Set the wallet fund store
pub fn set_fund_store(db: &mut SimpleDb, fund_store: FundStore) {
    db.put(FUND_KEY, &serialize(&fund_store).unwrap()).unwrap();
}

/// Save a payment to fund store
pub fn save_payment_to_fund_store(
    db: &mut SimpleDb,
    payments: impl Iterator<Item = (OutPoint, TokenAmount)>,
) {
    let mut fund_store = get_fund_store(db);
    for (out_p, amount) in payments {
        fund_store.store_tx(out_p, amount);
    }
    set_fund_store(db, fund_store);
}

/// Get the wallet known address
pub fn get_known_key_address(db: &SimpleDb) -> BTreeSet<String> {
    match db.get(KNOWN_ADDRESS_KEY) {
        Ok(Some(list)) => deserialize(&list).unwrap(),
        Ok(None) => Default::default(),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Set the wallet known address
pub fn set_known_key_address(db: &mut SimpleDb, address_store: BTreeSet<String>) {
    db.put(KNOWN_ADDRESS_KEY, &serialize(&address_store).unwrap())
        .unwrap();
}

/// Get the wallet AddressStore
pub fn get_address_store(db: &SimpleDb, key_addr: &str) -> AddressStore {
    match db.get(key_addr) {
        Ok(Some(store)) => deserialize(&store).unwrap(),
        Ok(None) => panic!("Key address not present in wallet: {}", key_addr),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Delete AddressStore
pub fn delete_address_store(db: &mut SimpleDb, key_addr: &str) {
    db.delete(key_addr).unwrap();
}

/// Save AddressStore
pub fn save_address_store_to_wallet(db: &mut SimpleDb, key_addr: &str, store: &AddressStore) {
    let input = serialize(store).unwrap();
    db.put(key_addr, &input).unwrap();
}

/// Get the wallet transaction store
pub fn get_transaction_store(db: &SimpleDb, out_p: &OutPoint) -> TransactionStore {
    match db.get(&serialize(&out_p).unwrap()) {
        Ok(Some(store)) => deserialize(&store).unwrap(),
        Ok(None) => panic!("Transaction not present in wallet: {:?}", out_p),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Delete transaction store
pub fn delete_transaction_store(db: &mut SimpleDb, out_p: &OutPoint) {
    let key = serialize(&out_p).unwrap();
    db.delete(&key).unwrap();
}

/// Save transaction
pub fn save_transaction_to_wallet(db: &mut SimpleDb, out_p: &OutPoint, store: &TransactionStore) {
    let key = serialize(out_p).unwrap();
    let input = serialize(store).unwrap();
    db.put(&key, &input).unwrap();
}

/// Make TxConstructors from stored TxOut
/// Also return the used info for db cleanup
pub fn fetch_inputs_for_payment_from_db(
    db: &SimpleDb,
    amount_required: TokenAmount,
) -> (Vec<TxConstructor>, TokenAmount, Vec<(OutPoint, String)>) {
    let mut tx_cons = Vec::new();
    let mut tx_used = Vec::new();
    let mut amount_made = TokenAmount(0);

    let fund_store = get_fund_store(db);
    if fund_store.running_total() < amount_required {
        panic!("Not enough funds available for payment!");
    }

    for (out_p, amount) in fund_store.into_transactions() {
        amount_made += amount;

        let (cons, used) = tx_constructor_from_prev_out(db, out_p);
        tx_cons.push(cons);
        tx_used.push(used);

        if amount_made >= amount_required {
            break;
        }
    }

    (tx_cons, amount_made, tx_used)
}

/// Consume the used transactions updating the wallet and produce TxIn
/// Non destructive operation: key material and data will still be present in wallet
pub fn consume_inputs_for_payment_to_wallet(
    db: &mut SimpleDb,
    tx_cons: Vec<TxConstructor>,
    tx_used: Vec<(OutPoint, String)>,
) -> Vec<TxIn> {
    let mut fund_store = get_fund_store(db);
    for (out_p, _) in tx_used {
        fund_store.spend_tx(&out_p);
    }
    set_fund_store(db, fund_store);

    construct_payment_tx_ins(tx_cons)
}

/// Destroy the used transactions with keys purging them from the wallet
/// Handle the case where same address is reused for multiple transactions
pub fn destroy_spent_transactions_and_keys(
    db: &mut SimpleDb,
) -> (BTreeSet<String>, BTreeMap<OutPoint, TokenAmount>) {
    let mut fund_store = get_fund_store(db);
    let mut address_store = get_known_key_address(db);

    //
    // Gather data for update
    //

    let spent_txs = fund_store.remove_spent_transactions();

    let remove_key_addresses: BTreeSet<_> = {
        let unspent_key_addresses: BTreeSet<_> = fund_store
            .transactions()
            .keys()
            .map(|out_p| get_transaction_store(db, out_p).key_address)
            .collect();

        spent_txs
            .keys()
            .map(|out_p| get_transaction_store(db, out_p).key_address)
            .filter(|addr| !unspent_key_addresses.contains(addr))
            .collect()
    };

    for keys_address in &remove_key_addresses {
        address_store.remove(keys_address);
    }

    //
    // Update database
    //
    set_fund_store(db, fund_store);
    set_known_key_address(db, address_store);
    for keys_address in &remove_key_addresses {
        delete_address_store(db, keys_address);
    }
    for out_p in spent_txs.keys() {
        delete_transaction_store(db, out_p);
    }

    (remove_key_addresses, spent_txs)
}

/// Make TxConstructor from stored TxOut
/// Also return the used info for db cleanup
pub fn tx_constructor_from_prev_out(
    db: &SimpleDb,
    out_p: OutPoint,
) -> (TxConstructor, (OutPoint, String)) {
    let key_address = get_transaction_store(db, &out_p).key_address;
    let needed_store = get_address_store(db, &key_address);

    let hash_to_sign = hex::encode(serialize(&out_p).unwrap());
    let signature = sign::sign_detached(&hash_to_sign.as_bytes(), &needed_store.secret_key);

    let tx_const = TxConstructor {
        t_hash: out_p.t_hash.clone(),
        prev_n: out_p.n,
        signatures: vec![signature],
        pub_keys: vec![needed_store.public_key],
    };

    (tx_const, (out_p, key_address))
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
        let addr = construct_address(pk);

        assert_eq!(addr, "197e990a0e00fd6ae13daecc18180df6".to_string(),);
    }

    #[test]
    /// Creating a payment address of 25 bytes
    fn should_construct_address_valid_length() {
        let pk = PublicKey([
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ]);
        let addr = construct_address(pk);

        assert_eq!(addr.len(), 32);
    }

    #[tokio::test(basic_scheduler)]
    async fn wallet_life_cycle() {
        //
        // Arrange
        //
        let out_p1 = OutPoint::new(String::new(), 1);
        let out_p2 = OutPoint::new(String::new(), 2);
        let amount1 = TokenAmount(3);

        let out_p3 = OutPoint::new(String::new(), 3);
        let amount3 = TokenAmount(5);

        let out_p4 = OutPoint::new(String::new(), 3);
        let amount4 = TokenAmount(6);
        let key_addr_non_existent = "000000".to_owned();

        let out_p_non_pay = OutPoint::new(String::new(), 4);
        let key_addr_non_pay = "".to_owned();

        let amount_out = TokenAmount(4);

        //
        // Act
        //
        let mut wallet = WalletDb::new(DbMode::InMemory, None);

        // Unlinked keys and transactions
        let (_key_addr_unused, _) = wallet.generate_payment_address().await;
        wallet
            .save_transaction_to_wallet(out_p_non_pay, key_addr_non_pay)
            .await
            .unwrap();

        // Store paiments
        let (key_addr1, _) = wallet.generate_payment_address().await;
        let (key_addr2, _) = wallet.generate_payment_address().await;
        let stored_usable = wallet
            .save_usable_payments_to_wallet(vec![
                (out_p1.clone(), amount1, key_addr1.clone()),
                (out_p2.clone(), amount1, key_addr2.clone()),
                (out_p3, amount3, key_addr2),
                (out_p4, amount4, key_addr_non_existent),
            ])
            .await
            .unwrap();

        // Pay out
        let (tx_cons, fetched_amount, tx_used) = wallet.fetch_inputs_for_payment(amount_out).await;
        let tx_ins = wallet.consume_inputs_for_payment(tx_cons, tx_used).await;

        // clean up db
        let (destroyed_keys, destroyed_txs) = wallet.destroy_spent_transactions_and_keys().await;

        //
        // Assert
        //
        assert_eq!(stored_usable.len(), 3);
        assert_eq!(fetched_amount, amount1 + amount1);
        assert_eq!(tx_ins.len(), 2);

        let expected_destroyed_keys: BTreeSet<_> = vec![key_addr1].into_iter().collect();
        assert_eq!(destroyed_keys, expected_destroyed_keys);

        let expected_destroyedkeys: BTreeMap<_, _> = vec![(out_p1, amount1), (out_p2, amount1)]
            .into_iter()
            .collect();
        assert_eq!(destroyed_txs, expected_destroyedkeys);
    }
}
