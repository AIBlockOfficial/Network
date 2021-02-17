use crate::comms_handler::Node;
use crate::configurations::{InititalTxSpec, UtxoSetSpec, WalletTxSpec};
use crate::constants::MINING_DIFFICULTY;
use crate::interfaces::ProofOfWork;
use crate::wallet::WalletDb;
use bincode::serialize;
use futures::future::join_all;
use naom::primitives::transaction_utils::{
    construct_address, construct_payment_tx_ins, construct_payments_tx, construct_tx_hash,
};
use naom::primitives::{
    asset::{Asset, TokenAmount},
    block::Block,
    transaction::{OutPoint, Transaction, TxConstructor, TxOut},
};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::secretbox::Key;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::collections::BTreeMap;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{trace, warn};

use crate::hash_block::*;

pub struct MpscTracingSender<T> {
    sender: mpsc::Sender<T>,
}

impl<T> Clone for MpscTracingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> From<mpsc::Sender<T>> for MpscTracingSender<T> {
    fn from(sender: mpsc::Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T> MpscTracingSender<T> {
    pub async fn send(&mut self, value: T, tag: &str) -> Result<(), mpsc::error::SendError<T>> {
        use mpsc::error::SendError;
        use mpsc::error::TrySendError;

        match self.sender.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(value)) => {
                trace!("send_tracing({}) full: waiting", tag);
                let start = Instant::now();
                let result = self.sender.send(value).await;
                let elapsed = Instant::now() - start;
                if elapsed < Duration::from_millis(2) {
                    trace!("send_tracing({}) done: waited({:?})", tag, elapsed);
                } else {
                    warn!("send_tracing({}) done: waited({:?})", tag, elapsed);
                }
                result
            }
            Err(TrySendError::Closed(value)) => Err(SendError(value)),
        }
    }
}

/// Attempts to connect to all peers
///
/// ### Arguments
///
/// * `node`     - Node attempting to connect to peers.
/// * `peers`    - Vec of socket addresses of peers
/// * `close_rx` - Receiver for close event or None to finish when all connected
pub async fn loop_connnect_to_peers_async(
    mut node: Node,
    peers: Vec<SocketAddr>,
    mut close_rx: Option<oneshot::Receiver<()>>,
) {
    loop {
        for peer in node.unconnected_peers(&peers).await {
            trace!(?peer, "Try to connect to");
            if let Err(e) = node.connect_to(peer).await {
                trace!(?peer, ?e, "Try to connect to failed");
            } else {
                trace!(?peer, "Try to connect to succeeded");
            }
        }

        let delay_retry = tokio::time::delay_for(Duration::from_millis(500));
        if let Some(close_rx) = &mut close_rx {
            tokio::select! {
                _ = delay_retry => (),
                _ = close_rx => return,
            };
        } else {
            if node.unconnected_peers(&peers).await.is_empty() {
                return;
            }
            delay_retry.await;
        }
    }
}

/// check connected to all peers
///
/// ### Arguments
///
/// * `node`     - Node attempting to connect to peers.
/// * `peers`    - Vec of socket addresses of peers
pub async fn loop_wait_connnect_to_peers_async(node: Node, peers: Vec<SocketAddr>) {
    while !node.unconnected_peers(&peers).await.is_empty() {
        tokio::time::delay_for(Duration::from_millis(10)).await;
    }
}

/// Gets the locally set list of sanctioned addresses
///
/// ### Arguments
///
/// * `path`         - Path to the sanction list
/// * `jurisdiction` - Jurisdiction to fetch sanctioned addresses for
pub fn get_sanction_addresses(path: String, jurisdiction: &str) -> Vec<String> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    let mut buff = String::new();
    file.read_to_string(&mut buff).unwrap();

    let sancs: serde_json::value::Value = serde_json::from_str(&buff).unwrap();

    match sancs[jurisdiction].as_array() {
        Some(v) => (*v
            .iter()
            .map(|i| i.as_str().unwrap().to_string())
            .collect::<Vec<String>>())
        .to_vec(),
        None => Vec::new(),
    }
}

/// Creates a "fake" transaction to save to the local wallet
/// for testing. The transaction will contain 4 tokens
///
/// NOTE: This is a test util function
/// ### Arguments
///
/// * `wallet_db`    - &WalletDb object. Reference to a wallet database
pub async fn create_and_save_fake_to_wallet(
    wallet_db: &WalletDb,
) -> Result<(), Box<dyn std::error::Error>> {
    let (final_address, address_keys) = wallet_db.generate_payment_address().await;
    let (receiver_addr, _) = wallet_db.generate_payment_address().await;

    let (t_hash, _payment_tx) = create_valid_transaction(
        &"00000".to_owned(),
        0,
        &receiver_addr,
        &address_keys.public_key,
        &address_keys.secret_key,
    );
    let tx_out_p = OutPoint::new(t_hash, 0);
    let payment_to_save = TokenAmount(4000);
    wallet_db
        .save_payment_to_wallet(tx_out_p.clone(), payment_to_save, final_address)
        .await
        .unwrap();

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
///
/// ### Arguments
///
/// * `comand_input` - command line input to find the socket address
pub fn command_input_to_socket(command_input: String) -> SocketAddr {
    let ip_and_port: Vec<&str> = command_input.split(':').collect();
    let port = ip_and_port[1].parse::<u16>().unwrap();
    let ip: Vec<u8> = ip_and_port[0]
        .split('.')
        .map(|x| x.parse::<u8>().unwrap())
        .collect();
    let ip_addr = IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]));

    SocketAddr::new(ip_addr, port)
}

/// Computes a key that will be shared from a vector of PoWs
///
/// ### Arguments
///
/// * `p_list` - Vectoor of PoWs
pub fn get_partition_entry_key(p_list: &[ProofOfWork]) -> Key {
    let key_sha_seed: Vec<u8> = p_list
        .iter()
        .flat_map(|e| e.address.as_bytes().iter().chain(&e.nonce))
        .copied()
        .collect();

    use std::convert::TryInto;
    let hashed_key = Sha3_256::digest(&key_sha_seed).to_vec();
    let key_slice: [u8; 32] = hashed_key[..].try_into().unwrap();
    Key(key_slice)
}

/// Address to be used in Proof of Work
///
/// ### Arguments
///
/// * `addr`    - Socket address of used in the proof of work
pub fn format_parition_pow_address(addr: SocketAddr) -> String {
    format!("{}", addr)
}

/// Block to be used in Proof of Work
///
/// ### Arguments
///
/// * `block`    - &Block reference to be used in proof of work
pub fn serialize_block_for_pow(block: &Block) -> Vec<u8> {
    serialize(block).unwrap()
}

/// HashBlock to be used in Proof of Work
///
/// ### Arguments
///
/// * `block`    - &HashBlock reference to be used in proof of work
pub fn serialize_hashblock_for_pow(block: &HashBlock) -> Vec<u8> {
    serialize(block).unwrap()
}

/// Concatenates a merkle hash and a coinbase hash to produce a single hash output
///
/// ### Arguments
///
/// * `merkle_hash` - Merkle hash to concatenate onto
/// * `cb_tx_hash`  - Coinbase transaction hash
pub fn concat_merkle_coinbase(merkle_hash: &str, cb_tx_hash: &str) -> String {
    let mut concat = merkle_hash.to_string();
    concat.push_str(&cb_tx_hash.to_string());

    concat
}

/// Validate Proof of Work an address with a random number
pub fn validate_pow_for_address(pow: &ProofOfWork, rand_num: &Option<&Vec<u8>>) -> bool {
    let mut pow_body = pow.address.as_bytes().to_vec();
    pow_body.extend(rand_num.iter().flat_map(|r| r.iter()).copied());
    pow_body.extend(&pow.nonce);

    validate_pow(&pow_body)
}

/// Validate Proof of Work for a block with a mining transaction
///
/// ### Arguments
///
/// * `prev_hash`   - The hash of the previous block
/// * `merkle_hash` - The merkle hash (+ coinbase)
/// * `nonce`       - Nonce
pub fn validate_pow_block(prev_hash: &str, merkle_hash: &str, nonce: &[u8]) -> bool {
    let mut pow = nonce.to_owned().to_vec();
    pow.extend_from_slice(merkle_hash.as_bytes());
    pow.extend_from_slice(prev_hash.as_bytes());

    validate_pow(&pow)
}
/// Check the hash of given data reach MINING_DIFFICULTY
///
/// ### Arguments
///
/// * `pow`    - &u8 proof of work
fn validate_pow(pow: &[u8]) -> bool {
    let pow_hash = Sha3_256::digest(pow).to_vec();
    pow_hash[0..MINING_DIFFICULTY].iter().all(|v| *v == 0)
}

/// Create a valid transaction from givent info
pub fn create_valid_transaction(
    t_hash_hex: &str,
    prev_n: i32,
    receiver_addr_hex: &str,
    pub_key: &PublicKey,
    secret_key: &SecretKey,
) -> (String, Transaction) {
    create_valid_transaction_with_ins_outs(
        &[(prev_n, t_hash_hex)],
        &[receiver_addr_hex],
        pub_key,
        secret_key,
        TokenAmount(1),
    )
}

/// Create a valid transaction from givent info
///
/// ### Arguments
///
/// * `tx`    - &InititialTxSpecs. An object containing the intialisation values
pub fn create_valid_transaction_with_info(tx: &InititalTxSpec) -> (String, Transaction) {
    let tx_out_p = decode_wallet_out_point(&tx.out_point);
    let sk = decode_secret_key(&tx.secret_key);
    let pk = decode_pub_key(&tx.public_key);
    let receiver_public_key = decode_pub_key(&tx.receiver_public_key);
    let receiver_address = construct_address(receiver_public_key);

    create_valid_transaction_with_ins_outs(
        &[(tx_out_p.n, &tx_out_p.t_hash)],
        &[&receiver_address],
        &pk,
        &sk,
        TokenAmount(1),
    )
}

/// Create a valid transaction from givent info
pub fn create_valid_transaction_with_ins_outs(
    tx_in: &[(i32, &str)],
    receiver_addr_hexs: &[&str],
    pub_key: &PublicKey,
    secret_key: &SecretKey,
    amount: TokenAmount,
) -> (String, Transaction) {
    let tx_ins = {
        let mut tx_in_cons = Vec::new();
        for (prev_n, t_hash_hex) in tx_in {
            let signable = OutPoint::new(t_hash_hex.to_string(), *prev_n);
            let signable_h = hex::encode(serialize(&signable).unwrap());

            let signature = sign::sign_detached(&signable_h.as_bytes(), &secret_key);
            tx_in_cons.push(TxConstructor {
                t_hash: t_hash_hex.to_string(),
                prev_n: *prev_n,
                signatures: vec![signature],
                pub_keys: vec![*pub_key],
            });
        }

        construct_payment_tx_ins(tx_in_cons)
    };

    let tx_outs = {
        let mut tx_outs = Vec::new();

        for addr in receiver_addr_hexs {
            tx_outs.push(TxOut {
                value: Some(Asset::Token(amount)),
                amount,
                locktime: 0,
                script_public_key: Some(addr.to_string()),
                drs_block_hash: None,
                drs_tx_hash: None,
            });
        }
        tx_outs
    };

    let payment_tx = construct_payments_tx(tx_ins, tx_outs);
    let t_hash = construct_tx_hash(&payment_tx);

    (t_hash, payment_tx)
}

/// Generate utxo_set transactions from seed info
///
/// ### Arguments
///
/// * `seed`    - &UtxoSetSpec object iterated through to generate the transaction set utxo
pub fn make_utxo_set_from_seed(seed: &UtxoSetSpec) -> BTreeMap<String, Transaction> {
    seed.iter()
        .map(|(tx_hash, tx_out)| {
            let tx = Transaction {
                outputs: tx_out
                    .iter()
                    .map(|out| {
                        let pk_slice = hex::decode(&out.public_key).unwrap();
                        let pk = PublicKey::from_slice(&pk_slice).unwrap();
                        let script_public_key = construct_address(pk);

                        TxOut::new_amount(script_public_key, out.amount)
                    })
                    .collect(),
                ..Transaction::default()
            };
            (tx_hash.clone(), tx)
        })
        .collect()
}

/// Generate wallet transactions from seed info
///
/// ### Arguments
///
/// * `seed`    - &WalletTxSpec object containing parameters to generate wallet transactions
pub fn make_wallet_tx_info(seed: &WalletTxSpec) -> (OutPoint, PublicKey, SecretKey, TokenAmount) {
    let tx_out_p = decode_wallet_out_point(&seed.out_point);
    let amount = TokenAmount(seed.amount);
    let sk = decode_secret_key(&seed.secret_key);
    let pk = decode_pub_key(&seed.public_key);

    (tx_out_p, pk, sk, amount)
}

/// Decodes a wallet's Outpoint
///
/// ### Arguments
///
/// * `out_point`    - String to be split and decode the wallet OutPoint
pub fn decode_wallet_out_point(out_point: &str) -> OutPoint {
    let mut it = out_point.split('-');
    let n = it.next().unwrap().parse().unwrap();
    let tx_hash = it.next().unwrap().parse().unwrap();
    OutPoint::new(tx_hash, n)
}

/// Decodes the public key
///
/// ### Arguments
///
/// * `key`    - key to be decoded to give the public key
pub fn decode_pub_key(key: &str) -> PublicKey {
    let key_slice = hex::decode(key).unwrap();
    PublicKey::from_slice(&key_slice).unwrap()
}

/// Decodes a secret key from a given key
///
/// ### Arguments
///
/// * `key`    - key to decoded to give the secret key
pub fn decode_secret_key(key: &str) -> SecretKey {
    let key_slice = hex::decode(key).unwrap();
    SecretKey::from_slice(&key_slice).unwrap()
}

/// Loop reconnect and test disconnect
///
/// ### Arguments
///
/// * `node_conn`         - Node to use for connections
/// * `addrs_to_connect`  - Addresses to establish connections to
pub fn loops_re_connect_disconnect(
    node_conn: Node,
    addrs_to_connect: Vec<SocketAddr>,
) -> (
    (impl Future<Output = ()>, oneshot::Sender<()>),
    (impl Future<Output = ()>, oneshot::Sender<()>),
) {
    // PERMANENT CONNEXION HANDLING
    let re_connect = {
        let (stop_re_connect_tx, stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let node_conn = node_conn.clone();
        (
            async move {
                println!("Start connect to requested peers");
                loop_connnect_to_peers_async(node_conn, addrs_to_connect, Some(stop_re_connect_rx))
                    .await;
                println!("Reconnect complete");
            },
            stop_re_connect_tx,
        )
    };

    // TEST DIS-CONNECTION HANDLING
    let disconnect_test = {
        let (stop_re_connect_tx, mut stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let disconnect = format!("disconnect_{}", node_conn.address().port());
        let mut node_conn = node_conn;
        let mut paused = true;
        (
            async move {
                println!("Start mode input check");
                loop {
                    tokio::select! {
                        _ = tokio::time::delay_for(Duration::from_millis(500)) => (),
                        _ = &mut stop_re_connect_rx => break,
                    };

                    match (paused, std::path::Path::new(&disconnect).exists()) {
                        (false, true) => {
                            node_conn.set_pause_listening(true).await;
                            join_all(node_conn.disconnect_all().await).await;
                            paused = true;
                        }
                        (true, false) => {
                            node_conn.set_pause_listening(false).await;
                            paused = false;
                        }
                        _ => (),
                    };
                }
                println!("Complete mode input check");
            },
            stop_re_connect_tx,
        )
    };
    (re_connect, disconnect_test)
}
