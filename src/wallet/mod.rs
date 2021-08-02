use crate::configurations::{DbMode, WalletTxSpec};
use crate::constants::{FUND_KEY, KNOWN_ADDRESS_KEY, WALLET_PATH};
use crate::db_utils::{
    self, SimpleDb, SimpleDbError, SimpleDbSpec, SimpleDbWriteBatch, DB_COL_DEFAULT,
};
use crate::utils::make_wallet_tx_info;
use bincode::{deserialize, serialize};
use naom::crypto::secretbox_chacha20_poly1305 as secretbox;
use naom::crypto::sign_ed25519 as sign;
use naom::crypto::sign_ed25519::{PublicKey, SecretKey};
use naom::primitives::asset::Asset;
use naom::primitives::transaction::{OutPoint, TxConstructor, TxIn};
use naom::utils::transaction_utils::{construct_address, construct_payment_tx_ins};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::pwhash;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Error;
use std::sync::{Arc, Mutex};
use tokio::task;
mod fund_store;
pub use fund_store::{AssetValues, FundStore};

///Storage key for a &[u8] of the word 'MasterKeyStore'
pub const MASTER_KEY_STORE_KEY: &[u8] = "MasterKeyStore".as_bytes();

pub const DB_SPEC: SimpleDbSpec = SimpleDbSpec {
    db_path: WALLET_PATH,
    suffix: "",
    columns: &[],
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressStore {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStore {
    pub key_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterKeyStore {
    pub salt: pwhash::Salt,
    pub nonce: secretbox::Nonce,
    pub enc_master_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WalletDb {
    db: Arc<Mutex<SimpleDb>>,
    encryption_key: secretbox::Key,
}

impl WalletDb {
    pub fn new(db_mode: DbMode, db: Option<SimpleDb>, passphrase: Option<String>) -> Self {
        let mut db = db_utils::new_db(db_mode, &DB_SPEC, db);
        let mut batch = db.batch_writer();

        let passphrase = passphrase.as_deref().unwrap_or("").as_bytes();
        let masterkey = get_or_save_master_key_store(&db, &mut batch, passphrase);

        let batch = batch.done();
        db.write(batch).unwrap();
        Self {
            db: Arc::new(Mutex::new(db)),
            encryption_key: masterkey,
        }
    }

    /// Test if an entered passphrase is correct
    pub async fn test_passphrase(&self, passphrase: String) -> Result<(), ()> {
        match self
            .db
            .lock()
            .unwrap()
            .get_cf(DB_COL_DEFAULT, MASTER_KEY_STORE_KEY)
        {
            Ok(Some(store)) => {
                let store: MasterKeyStore = deserialize(&store).unwrap();
                let pass_key = make_key(passphrase.as_bytes(), store.salt);
                match secretbox::open(store.enc_master_key, &store.nonce, &pass_key) {
                    Some(_) => Ok(()),
                    None => Err(()),
                }
            }
            Ok(None) => Err(()),
            Err(_) => Err(()),
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
            let payments = vec![(tx_out_p, Asset::Token(amount), address)];
            self.save_usable_payments_to_wallet(payments).await.unwrap();
        }
        self
    }

    /// Extract persistent storage of a closed raft
    pub async fn take_closed_persistent_store(&mut self) -> SimpleDb {
        self.db.lock().unwrap().take()
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
        let final_address = construct_address(&public_key);
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
        let encryption_key = self.encryption_key.clone();
        Ok(task::spawn_blocking(move || {
            // Wallet DB handling
            let mut db = db.lock().unwrap();
            let mut batch = db.batch_writer();

            let mut address_list = get_known_key_address(&db);
            address_list.insert(address.clone());

            // Save to disk
            save_address_store_to_wallet(&mut batch, &address, keys, &encryption_key);
            set_known_key_address(&mut batch, address_list);
            let batch = batch.done();
            db.write(batch).unwrap();
        })
        .await?)
    }

    /// Saves an AddressStore to wallet in a directly encrypted state
    ///
    /// ### Arguments
    ///
    /// * `address` - Address to save to wallet
    /// * `keys`    - Address-related keys in an encrypted state
    pub async fn save_encrypted_address_to_wallet(
        &self,
        address: String,
        keys: Vec<u8>,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            let mut batch = db.batch_writer();

            let mut address_list = get_known_key_address(&db);
            address_list.insert(address.clone());

            batch.put_cf(DB_COL_DEFAULT, address, keys);
            set_known_key_address(&mut batch, address_list);

            let batch = batch.done();
            db.write(batch).unwrap();
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
            let mut batch = db.batch_writer();

            let store = TransactionStore { key_address };
            save_transaction_to_wallet(&mut batch, &out_p, &store);

            let batch = batch.done();
            db.write(batch).unwrap();
        })
        .await?)
    }

    /// Saves a received payment to the local wallet
    ///
    /// ### Arguments
    ///
    /// * `payments` - Payments OutPoint, amount and receiver key address
    pub async fn save_usable_payments_to_wallet(
        &self,
        payments: Vec<(OutPoint, Asset, String)>,
    ) -> Result<Vec<(OutPoint, Asset, String)>, Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            let mut batch = db.batch_writer();
            let mut fund_store = get_fund_store(&db);
            let addresses = get_known_key_address(&db);

            let usable_payments: Vec<_> = payments
                .into_iter()
                .filter(|(_, _, a)| addresses.contains(a))
                .collect();

            for (out_p, asset, key_address) in &usable_payments {
                let key_address = key_address.clone();
                let store = TransactionStore { key_address };
                fund_store.store_tx(out_p.clone(), asset.clone());
                save_transaction_to_wallet(&mut batch, out_p, &store);
            }

            set_fund_store(&mut batch, fund_store);

            let batch = batch.done();
            db.write(batch).unwrap();
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
    /// * `asset_required` - Asset needed
    pub async fn fetch_inputs_for_payment(
        &self,
        asset_required: Asset,
    ) -> (Vec<TxConstructor>, Asset, Vec<(OutPoint, String)>) {
        let db = self.db.clone();
        let encryption_key = self.encryption_key.clone();
        task::spawn_blocking(move || {
            let db = db.lock().unwrap();
            fetch_inputs_for_payment_from_db(&db, asset_required, &encryption_key)
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
            let mut batch = db.batch_writer();
            let mut fund_store = get_fund_store(&db);

            for (out_p, _) in tx_used {
                fund_store.spend_tx(&out_p);
            }
            set_fund_store(&mut batch, fund_store);
            let batch = batch.done();
            db.write(batch).unwrap();

            construct_payment_tx_ins(tx_cons)
        })
        .await
        .unwrap()
    }

    /// Destroy the used transactions with keys purging them from the wallet
    /// Handle the case where same address is reused for multiple transactions
    pub async fn destroy_spent_transactions_and_keys(
        &mut self,
    ) -> (BTreeSet<String>, BTreeMap<OutPoint, Asset>) {
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
        task::spawn_blocking(move || db.lock().unwrap().get_cf(DB_COL_DEFAULT, key).unwrap())
            .await
            .unwrap()
    }

    /// Set a the serialized value stored at given key
    pub async fn set_db_value(&self, key: &'static str, value: Vec<u8>) {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            db.lock()
                .unwrap()
                .put_cf(DB_COL_DEFAULT, key, &value)
                .unwrap()
        })
        .await
        .unwrap()
    }

    /// Delete value stored at given key
    pub async fn delete_db_value(&self, key: &'static str) {
        let db = self.db.clone();
        task::spawn_blocking(move || db.lock().unwrap().delete_cf(DB_COL_DEFAULT, key).unwrap())
            .await
            .unwrap()
    }

    /// Get the wallet fund store
    pub fn get_fund_store(&self) -> FundStore {
        get_fund_store(&self.db.lock().unwrap())
    }

    /// Get the wallet fund store with errors
    pub fn get_fund_store_err(&self) -> Result<FundStore, SimpleDbError> {
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
        get_address_store(&self.db.lock().unwrap(), key_addr, &self.encryption_key)
    }

    /// Gets the address store based on a provided key, but returns
    /// the result in an encrypted state for external storage
    ///
    /// ### Arguments
    ///
    ///  * `key_addr` - Key to get the address store for
    pub fn get_address_store_encrypted(&self, key_addr: &str) -> Vec<u8> {
        get_address_store_encrypted(&self.db.lock().unwrap(), key_addr)
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
pub fn get_fund_store_err(db: &SimpleDb) -> Result<FundStore, SimpleDbError> {
    match db.get_cf(DB_COL_DEFAULT, FUND_KEY) {
        Ok(Some(list)) => Ok(deserialize(&list).unwrap()),
        Ok(None) => Ok(FundStore::default()),
        Err(e) => Err(e),
    }
}

/// Set the wallet fund store
pub fn set_fund_store(db: &mut SimpleDbWriteBatch, fund_store: FundStore) {
    db.put_cf(DB_COL_DEFAULT, FUND_KEY, &serialize(&fund_store).unwrap())
}

/// Get the wallet known address
pub fn get_known_key_address(db: &SimpleDb) -> BTreeSet<String> {
    match db.get_cf(DB_COL_DEFAULT, KNOWN_ADDRESS_KEY) {
        Ok(Some(list)) => deserialize(&list).unwrap(),
        Ok(None) => Default::default(),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Set the wallet known address
pub fn set_known_key_address(db: &mut SimpleDbWriteBatch, address_store: BTreeSet<String>) {
    db.put_cf(
        DB_COL_DEFAULT,
        KNOWN_ADDRESS_KEY,
        &serialize(&address_store).unwrap(),
    );
}

/// Gets the wallet AddressStore in an encrypted state for external storage
pub fn get_address_store_encrypted(db: &SimpleDb, key_addr: &str) -> Vec<u8> {
    match db.get_cf(DB_COL_DEFAULT, key_addr) {
        Ok(Some(store)) => store,
        Ok(None) => panic!("Key address not present in wallet: {}", key_addr),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Get the wallet AddressStore
pub fn get_address_store(
    db: &SimpleDb,
    key_addr: &str,
    encryption_key: &secretbox::Key,
) -> AddressStore {
    match db.get_cf(DB_COL_DEFAULT, key_addr) {
        Ok(Some(store)) => {
            let (nonce, output) = store.split_at(secretbox::NONCE_LEN);
            let nonce = secretbox::Nonce::from_slice(nonce).unwrap();
            match secretbox::open(output.to_vec(), &nonce, encryption_key) {
                Some(decrypted) => deserialize(&decrypted).unwrap(),
                _ => panic!("Error accessing wallet"),
            }
        }
        Ok(None) => panic!("Key address not present in wallet: {}", key_addr),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Delete AddressStore
pub fn delete_address_store(db: &mut SimpleDbWriteBatch, key_addr: &str) {
    db.delete_cf(DB_COL_DEFAULT, key_addr);
}

/// Save AddressStore
pub fn save_address_store_to_wallet(
    db: &mut SimpleDbWriteBatch,
    key_addr: &str,
    store: AddressStore,
    encryption_key: &secretbox::Key,
) {
    let input = {
        let store = serialize(&store).unwrap();
        let nonce = secretbox::gen_nonce();
        let mut input: Vec<u8> = nonce.as_ref().to_vec();
        input.append(&mut secretbox::seal(store, &nonce, encryption_key).unwrap());
        input
    };
    db.put_cf(DB_COL_DEFAULT, key_addr, &input);
}

/// Get the wallet transaction store
pub fn get_transaction_store(db: &SimpleDb, out_p: &OutPoint) -> TransactionStore {
    match db.get_cf(DB_COL_DEFAULT, &serialize(&out_p).unwrap()) {
        Ok(Some(store)) => deserialize(&store).unwrap(),
        Ok(None) => panic!("Transaction not present in wallet: {:?}", out_p),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

/// Delete transaction store
pub fn delete_transaction_store(db: &mut SimpleDbWriteBatch, out_p: &OutPoint) {
    let key = serialize(&out_p).unwrap();
    db.delete_cf(DB_COL_DEFAULT, &key);
}

/// Save transaction
pub fn save_transaction_to_wallet(
    db: &mut SimpleDbWriteBatch,
    out_p: &OutPoint,
    store: &TransactionStore,
) {
    let key = serialize(out_p).unwrap();
    let input = serialize(store).unwrap();
    db.put_cf(DB_COL_DEFAULT, &key, &input);
}

/// Get the wallet salt store
pub fn get_or_save_master_key_store(
    db: &SimpleDb,
    batch: &mut SimpleDbWriteBatch,
    passphrase: &[u8],
) -> secretbox::Key {
    match db.get_cf(DB_COL_DEFAULT, MASTER_KEY_STORE_KEY) {
        Ok(Some(store)) => {
            let store: MasterKeyStore = deserialize(&store).unwrap();
            let pass_key = make_key(passphrase, store.salt);
            match secretbox::open(store.enc_master_key, &store.nonce, &pass_key) {
                Some(master_key) => secretbox::Key::from_slice(&master_key).unwrap(),
                None => panic!("WalletDb: invalid passphrase"),
            }
        }
        Ok(None) => {
            let salt = pwhash::gen_salt();
            let master_key = secretbox::gen_key();
            let nonce = secretbox::gen_nonce();
            let pass_key = make_key(passphrase, salt);
            let enc_master_key =
                secretbox::seal(master_key.as_ref().to_vec(), &nonce, &pass_key).unwrap();
            let store = serialize(&MasterKeyStore {
                salt,
                nonce,
                enc_master_key,
            })
            .unwrap();
            batch.put_cf(DB_COL_DEFAULT, MASTER_KEY_STORE_KEY, &store);
            master_key
        }
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

///Creates a secretbox key to be used for encryption and decryption
///
/// ### Arguments
///
/// * `passphrase` - String used as the password when creating the encryption key
/// * `salt` - Salt value added to the passphrase to ensure safe encryption
pub fn make_key(passphrase: &[u8], salt: pwhash::Salt) -> secretbox::Key {
    let mut kb = [0; secretbox::KEY_LEN];
    pwhash::derive_key(
        &mut kb,
        passphrase,
        &salt,
        pwhash::OPSLIMIT_INTERACTIVE,
        pwhash::MEMLIMIT_INTERACTIVE,
    )
    .unwrap();
    secretbox::Key::from_slice(&kb).unwrap()
}

/// Make TxConstructors from stored TxOut
/// Also return the used info for db cleanup
pub fn fetch_inputs_for_payment_from_db(
    db: &SimpleDb,
    asset_required: Asset,
    encryption_key: &secretbox::Key,
) -> (Vec<TxConstructor>, Asset, Vec<(OutPoint, String)>) {
    let mut tx_cons = Vec::new();
    let mut tx_used = Vec::new();
    let fund_store = get_fund_store(db);
    let mut amount_made = Asset::default_of_type(&asset_required);

    if !fund_store.running_total().has_enough(&asset_required) {
        panic!(
            "Not enough funds available for payment!: {:?}",
            fund_store.running_total()
        );
    }

    for (out_p, amount) in fund_store.into_transactions() {
        if amount_made.add_assign(&amount) {
            let (cons, used) = tx_constructor_from_prev_out(db, out_p, encryption_key);
            tx_cons.push(cons);
            tx_used.push(used);
        }
        if let Some(true) = amount_made.is_greater_or_equal_to(&asset_required) {
            break;
        }
    }

    (tx_cons, amount_made, tx_used)
}

/// Destroy the used transactions with keys purging them from the wallet
/// Handle the case where same address is reused for multiple transactions
pub fn destroy_spent_transactions_and_keys(
    db: &mut SimpleDb,
) -> (BTreeSet<String>, BTreeMap<OutPoint, Asset>) {
    let mut batch = db.batch_writer();
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
    set_fund_store(&mut batch, fund_store);
    set_known_key_address(&mut batch, address_store);
    for keys_address in &remove_key_addresses {
        delete_address_store(&mut batch, keys_address);
    }
    for out_p in spent_txs.keys() {
        delete_transaction_store(&mut batch, out_p);
    }

    let batch = batch.done();
    db.write(batch).unwrap();

    (remove_key_addresses, spent_txs)
}

/// Make TxConstructor from stored TxOut
/// Also return the used info for db cleanup
pub fn tx_constructor_from_prev_out(
    db: &SimpleDb,
    out_p: OutPoint,
    encryption_key: &secretbox::Key,
) -> (TxConstructor, (OutPoint, String)) {
    let key_address = get_transaction_store(db, &out_p).key_address;
    let needed_store = get_address_store(db, &key_address, encryption_key);

    let hash_to_sign = hex::encode(serialize(&out_p).unwrap());
    let signature = sign::sign_detached(hash_to_sign.as_bytes(), &needed_store.secret_key);

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
        let pk = PublicKey::from_slice(&[
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ])
        .unwrap();
        let addr = construct_address(&pk);

        assert_eq!(addr, "197e990a0e00fd6ae13daecc18180df6".to_string(),);
    }

    #[test]
    /// Creating a payment address of 25 bytes
    fn should_construct_address_valid_length() {
        let pk = PublicKey::from_slice(&[
            196, 234, 50, 92, 76, 102, 62, 4, 231, 81, 211, 133, 33, 164, 134, 52, 44, 68, 174, 18,
            14, 59, 108, 187, 150, 190, 169, 229, 215, 130, 78, 78,
        ])
        .unwrap();
        let addr = construct_address(&pk);

        assert_eq!(addr.len(), 32);
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "WalletDb: invalid passphrase")]
    async fn incorrect_password() {
        //Arrange
        let db = WalletDb::new(
            DbMode::InMemory,
            None,
            Some(String::from("Test Passphrase1")),
        )
        .take_closed_persistent_store()
        .await;

        //Act/Panic - Wrong Passphrase
        let _db = WalletDb::new(
            DbMode::InMemory,
            Some(db),
            Some("Test Passphrase 2".to_owned()),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wallet_life_cycle() {
        //
        // Arrange
        //
        let out_p1 = OutPoint::new(String::new(), 1);
        let out_p2 = OutPoint::new(String::new(), 2);
        let amount1 = Asset::token_u64(3);

        let out_p3 = OutPoint::new(String::new(), 3);
        let amount3 = Asset::token_u64(5);

        let out_p4 = OutPoint::new(String::new(), 3);
        let amount4 = Asset::token_u64(6);
        let key_addr_non_existent = "000000".to_owned();

        let out_p_non_pay = OutPoint::new(String::new(), 4);
        let key_addr_non_pay = "".to_owned();

        let amount_out = Asset::token_u64(4);

        //
        // Act
        //
        let mut wallet = WalletDb::new(DbMode::InMemory, None, Some("Test Passphrase".to_owned()));

        // Unlinked keys and transactions
        let (_key_addr_unused, _) = wallet.generate_payment_address().await;
        wallet
            .save_transaction_to_wallet(out_p_non_pay, key_addr_non_pay)
            .await
            .unwrap();

        // Store payments
        let (key_addr1, _) = wallet.generate_payment_address().await;
        let (key_addr2, _) = wallet.generate_payment_address().await;
        let stored_usable = wallet
            .save_usable_payments_to_wallet(vec![
                (out_p1.clone(), amount1.clone(), key_addr1.clone()),
                (out_p2.clone(), amount1.clone(), key_addr2.clone()),
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
        assert_eq!(
            fetched_amount,
            Asset::Token(amount1.token_amount() + amount1.token_amount())
        );
        assert_eq!(tx_ins.len(), 2);

        let expected_destroyed_keys: BTreeSet<_> = vec![key_addr1].into_iter().collect();
        assert_eq!(destroyed_keys, expected_destroyed_keys);

        let expected_destroyedkeys: BTreeMap<_, _> =
            vec![(out_p1, amount1.clone()), (out_p2, amount1)]
                .into_iter()
                .collect();
        assert_eq!(destroyed_txs, expected_destroyedkeys);
    }
}
