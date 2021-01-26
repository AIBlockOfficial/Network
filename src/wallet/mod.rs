use crate::configurations::DbMode;
use crate::constants::{
    ADDRESS_KEY, DB_PATH_LIVE, DB_PATH_TEST, FUND_KEY, NETWORK_VERSION, WALLET_PATH,
};
use crate::db_utils::{DBError, SimpleDb};
use bincode::{deserialize, serialize};
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::{OutPoint, TxConstructor, TxIn};
use naom::primitives::transaction_utils::construct_payment_tx_ins;
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
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct FundStore {
    pub running_total: TokenAmount,
    pub transactions: BTreeMap<OutPoint, TokenAmount>,
}

#[derive(Debug, Clone)]
pub struct WalletDb {
    db: Arc<Mutex<SimpleDb>>,
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

    pub async fn with_seed(self, index: usize, seeds: &[Vec<String>]) -> Self {
        let seeds = if let Some(seeds) = seeds.get(index) {
            seeds
        } else {
            return self;
        };

        for seed in seeds {
            let mut it = seed.split('-');

            let n = it.next().unwrap().parse().unwrap();
            let tx_hash = it.next().unwrap().parse().unwrap();
            let amount = it.next().unwrap().parse().unwrap();

            let amount = TokenAmount(amount);
            let tx_out_p = OutPoint::new(tx_hash, n);

            let (address, _) = self.generate_payment_address().await;
            self.save_payment_to_wallet(tx_out_p, amount, address)
                .await
                .unwrap();
        }
        self
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
            // Wallet DB handling
            let mut db = db.lock().unwrap();
            let mut address_list = get_address_stores(&db);

            // Assign the new address to the store
            address_list.insert(address.clone(), keys);

            // Save to disk
            set_address_stores(&mut db, address_list);
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
        tx_hash: OutPoint,
        address: PaymentAddress,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            save_transaction_to_wallet(&mut db, tx_hash, address);
        })
        .await?)
    }

    /// Saves a received payment to the local wallet
    ///
    /// ### Arguments
    ///
    /// * `tx_hash  - Hash of the transaction
    /// * `amount`  - Amount of tokens in the payment
    /// * `address` - Transaction Address
    pub async fn save_payment_to_wallet(
        &self,
        tx_hash: OutPoint,
        amount: TokenAmount,
        address: PaymentAddress,
    ) -> Result<(), Error> {
        let db = self.db.clone();
        Ok(task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            save_transaction_to_wallet(&mut db, tx_hash.clone(), address);
            save_payment_to_fund_store(&mut db, tx_hash, amount);
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
    ) -> (
        Vec<TxConstructor>,
        TokenAmount,
        Vec<(OutPoint, TransactionStore)>,
    ) {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let db = db.lock().unwrap();
            fetch_inputs_for_payment(&db, amount_required)
        })
        .await
        .unwrap()
    }

    /// Consume given used transaction and produce TxIns
    ///
    ///
    /// ### Arguments
    ///
    /// * `tx_cons`         - TxIn TxConstructors
    /// * `amount_consumed` - Amount consumed by tx_cons & tx_used
    /// * `tx_used`         - TxOut used for TxIns
    pub async fn consume_inputs_for_payment(
        &mut self,
        tx_cons: Vec<TxConstructor>,
        amount_consumed: TokenAmount,
        tx_used: Vec<(OutPoint, TransactionStore)>,
    ) -> Vec<TxIn> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut db = db.lock().unwrap();
            consume_inputs_for_payment(&mut db, tx_cons, amount_consumed, tx_used)
        })
        .await
        .unwrap()
    }

    // Get the wallet fund store
    pub fn get_fund_store(&self) -> FundStore {
        get_fund_store(&self.db.lock().unwrap())
    }

    // Get the wallet fund store with errors
    pub fn get_fund_store_err(&self) -> Result<FundStore, DBError> {
        get_fund_store_err(&self.db.lock().unwrap())
    }

    // Get the wallet address store
    pub fn get_address_stores(&self) -> BTreeMap<String, AddressStore> {
        get_address_stores(&self.db.lock().unwrap())
    }

    // Get the wallet transaction store
    pub fn get_transaction_store(&self, tx_hash: &OutPoint) -> TransactionStore {
        get_transaction_store(&self.db.lock().unwrap(), tx_hash)
    }

    // Get the wallet addresses
    pub fn get_known_address(&self) -> Vec<String> {
        self.get_address_stores()
            .into_iter()
            .map(|(addr, _)| addr)
            .collect()
    }

    // Get the wallet transaction address
    pub fn get_transaction_address(&self, tx_hash: &OutPoint) -> String {
        self.get_transaction_store(tx_hash).address
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

// Get the wallet fund store
pub fn get_fund_store(db: &SimpleDb) -> FundStore {
    match get_fund_store_err(db) {
        Ok(v) => v,
        Err(e) => panic!("Failed to access the wallet database with error: {:?}", e),
    }
}

// Get the wallet fund store
pub fn get_fund_store_err(db: &SimpleDb) -> Result<FundStore, DBError> {
    match db.get(FUND_KEY) {
        Ok(Some(list)) => Ok(deserialize(&list).unwrap()),
        Ok(None) => Ok(FundStore::default()),
        Err(e) => Err(e),
    }
}

// Set the wallet fund store
pub fn set_fund_store(db: &mut SimpleDb, fund_store: FundStore) {
    db.put(FUND_KEY, &serialize(&fund_store).unwrap()).unwrap();
}

// Save a payment to fund store
pub fn save_payment_to_fund_store(db: &mut SimpleDb, hash: OutPoint, amount: TokenAmount) {
    let mut fund_store = get_fund_store(db);

    let old_amount = fund_store.transactions.insert(hash, amount);
    fund_store.running_total -= old_amount.unwrap_or_default();
    fund_store.running_total += amount;

    set_fund_store(db, fund_store);
}

// Get the wallet address store
pub fn get_address_stores(db: &SimpleDb) -> BTreeMap<String, AddressStore> {
    match db.get(ADDRESS_KEY) {
        Ok(Some(list)) => deserialize(&list).unwrap(),
        Ok(None) => BTreeMap::new(),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

// Set the wallet address store
pub fn set_address_stores(db: &mut SimpleDb, address_store: BTreeMap<String, AddressStore>) {
    db.put(ADDRESS_KEY, &serialize(&address_store).unwrap())
        .unwrap();
}

// Get the wallet transaction store
pub fn get_transaction_store(db: &SimpleDb, tx_hash: &OutPoint) -> TransactionStore {
    match db.get(&serialize(&tx_hash).unwrap()) {
        Ok(Some(list)) => deserialize(&list).unwrap(),
        Ok(None) => panic!("Transaction not present in wallet: {:?}", tx_hash),
        Err(e) => panic!("Error accessing wallet: {:?}", e),
    }
}

// Delete transaction store
pub fn delete_transaction_store(db: &mut SimpleDb, tx_hash: &OutPoint) {
    let tx_hash_ser = serialize(&tx_hash).unwrap();
    db.delete(&tx_hash_ser).unwrap();
}

// Save transaction
pub fn save_transaction_to_wallet(db: &mut SimpleDb, tx_hash: OutPoint, address: PaymentAddress) {
    let PaymentAddress { address, net } = address;
    let tx_store = TransactionStore { address, net };

    let key = serialize(&tx_hash).unwrap();
    let input = serialize(&tx_store).unwrap();
    db.put(&key, &input).unwrap();
}

// Make TxConstructors from stored TxOut
// Also return the used info for db cleanup
pub fn fetch_inputs_for_payment(
    db: &SimpleDb,
    amount_required: TokenAmount,
) -> (
    Vec<TxConstructor>,
    TokenAmount,
    Vec<(OutPoint, TransactionStore)>,
) {
    let mut tx_cons = Vec::new();
    let mut tx_used = Vec::new();
    let mut amount_made = TokenAmount(0);

    let fund_store = get_fund_store(db);
    if fund_store.running_total < amount_required {
        panic!("Not enough funds available for payment!");
    }

    for (tx_hash, amount) in fund_store.transactions {
        amount_made += amount;

        let (cons, used) = tx_constructor_from_prev_out(db, tx_hash);
        tx_cons.push(cons);
        tx_used.push(used);

        if amount_made >= amount_required {
            break;
        }
    }

    (tx_cons, amount_made, tx_used)
}

// Consume the used transactions updating the wallet and produce TxIn
pub fn consume_inputs_for_payment(
    db: &mut SimpleDb,
    tx_cons: Vec<TxConstructor>,
    amount_consumed: TokenAmount,
    tx_used: Vec<(OutPoint, TransactionStore)>,
) -> Vec<TxIn> {
    let mut fund_store = get_fund_store(db);
    let mut address_store = get_address_stores(db);

    fund_store.running_total -= amount_consumed;
    for (tx_hash, tx_store) in tx_used {
        fund_store.transactions.remove(&tx_hash).unwrap();
        address_store.remove(&tx_store.address).unwrap();
        delete_transaction_store(db, &tx_hash);
    }

    set_fund_store(db, fund_store);
    set_address_stores(db, address_store);

    construct_payment_tx_ins(tx_cons)
}

// Make TxConstructor from stored TxOut
// Also return the used info for db cleanup
pub fn tx_constructor_from_prev_out(
    db: &SimpleDb,
    tx_hash: OutPoint,
) -> (TxConstructor, (OutPoint, TransactionStore)) {
    let address_store = get_address_stores(db);
    let tx_store = get_transaction_store(db, &tx_hash);

    let needed_store: &AddressStore = address_store.get(&tx_store.address).unwrap();
    let signature = sign::sign_detached(&tx_hash.t_hash.as_bytes(), &needed_store.secret_key);

    let tx_const = TxConstructor {
        t_hash: tx_hash.t_hash.clone(),
        prev_n: tx_hash.n,
        signatures: vec![signature],
        pub_keys: vec![needed_store.public_key],
    };

    (tx_const, (tx_hash, tx_store))
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
