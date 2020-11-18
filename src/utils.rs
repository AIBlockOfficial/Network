use crate::interfaces::ProofOfWork;
use naom::primitives::transaction_utils::{
    construct_payment_tx, construct_payment_tx_ins, construct_tx_hash,
};
use naom::primitives::{
    asset::Asset,
    transaction::{Transaction, TxConstructor},
};
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
    t_hash_hex: &String,
    pub_key: &PublicKey,
    secret_key: &SecretKey,
) -> (String, Transaction) {
    let signature = sign::sign_detached(&t_hash_hex.as_bytes(), &secret_key);

    let tx_const = TxConstructor {
        t_hash: t_hash_hex.clone(),
        prev_n: 0,
        signatures: vec![signature],
        pub_keys: vec![pub_key.clone()],
    };

    let tx_ins = construct_payment_tx_ins(vec![tx_const]);
    let payment_tx = construct_payment_tx(
        tx_ins,
        hex::encode(vec![0, 0, 0]),
        None,
        None,
        Asset::Token(4),
        4,
    );
    let t_hash = construct_tx_hash(&payment_tx);
    (t_hash, payment_tx)
}
