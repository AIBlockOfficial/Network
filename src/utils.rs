use crate::interfaces::ProofOfWork;
use crate::wallet::{
    construct_address, save_address_to_wallet, save_payment_to_wallet, save_transactions_to_wallet,
    AddressStore, TransactionStore,
};
use naom::primitives::transaction_utils::{
    construct_payment_tx, construct_payment_tx_ins, construct_tx_hash,
};
use naom::primitives::{
    asset::Asset,
    transaction::{Transaction, TxConstructor},
};
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use rocksdb::{DBCompressionType, Options};

/// Creates a set of DB opening options for rocksDB instances
pub fn get_db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Snappy);

    opts
}

/// Creates a "fake" transaction to save to the local wallet
/// for testing. The transaction will contain 4 tokens
///
/// NOTE: This is a test util function
pub async fn create_and_save_fake_to_wallet() -> Result<(), Box<dyn std::error::Error>> {
    let (pk, sk) = sign::gen_keypair();
    let final_address = construct_address(pk, 0);
    let address_keys = AddressStore {
        public_key: pk,
        secret_key: sk.clone(),
    };

    let (pkb, _sk) = sign::gen_keypair();
    let receiver_addr = construct_address(pkb, 0);
    let (t_hash, _payment_tx) =
        create_valid_transaction(&"00000".to_owned(), &receiver_addr, &pk, &sk);

    // Save address store
    let _save_a_result = save_address_to_wallet(final_address.clone(), address_keys).await;

    // Save fund store
    let _save_f_result = save_payment_to_wallet(t_hash.clone(), 4).await;

    // Save transaction store
    let mut t_store = BTreeMap::new();
    let t_map = TransactionStore {
        address: final_address,
        net: 0,
    };
    t_store.insert(t_hash, t_map);
    println!("TX STORE: {:?}", t_store);
    let _save_t_result = save_transactions_to_wallet(t_store).await;

    Ok(())
}

/// Determines whether the passed value is within bounds of
/// available tokens in the supply.
///
/// TODO: Currently placeholder, needs to be filled in once requirements known
pub fn is_valid_amount(_value: &u64) -> bool {
    true
}

/// Returns a socket address from command input
pub fn command_input_to_socket(command_input: String) -> SocketAddr {
    let ip_and_port: Vec<&str> = command_input.split(":").collect();
    let port = ip_and_port[1].parse::<u16>().unwrap();
    let ip: Vec<u8> = ip_and_port[0]
        .split(".")
        .map(|x| x.parse::<u8>().unwrap())
        .collect();
    let ip_addr = IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]));

    SocketAddr::new(ip_addr, port)
}

/// Computes a key that will be shared from a vector of PoWs
pub fn get_partition_entry_key(p_list: Vec<ProofOfWork>) -> Vec<u8> {
    let mut key = Vec::new();
    for entry in p_list {
        let mut next_entry = entry.address.as_bytes().to_vec();
        next_entry.append(&mut entry.nonce.clone());
        key.append(&mut next_entry);
    }

    key
}

pub fn create_valid_transaction(
    t_hash_hex: &str,
    receiver_addr_hex: &str,
    pub_key: &PublicKey,
    secret_key: &SecretKey,
) -> (String, Transaction) {
    let signature = sign::sign_detached(&t_hash_hex.as_bytes(), &secret_key);

    let tx_const = TxConstructor {
        t_hash: t_hash_hex.to_string(),
        prev_n: 0,
        signatures: vec![signature],
        pub_keys: vec![pub_key.clone()],
    };

    let tx_ins = construct_payment_tx_ins(vec![tx_const]);
    let payment_tx = construct_payment_tx(
        tx_ins,
        receiver_addr_hex.to_string(),
        None,
        None,
        Asset::Token(4),
        4,
    );
    let t_hash = construct_tx_hash(&payment_tx);
    (t_hash, payment_tx)
}
