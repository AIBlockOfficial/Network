use crate::comms_handler::Node;
use crate::constants::MINING_DIFFICULTY;
use crate::interfaces::ProofOfWork;
use crate::wallet::WalletDb;
use bincode::serialize;
use naom::primitives::transaction_utils::{
    construct_payment_tx, construct_payment_tx_ins, construct_tx_hash,
};
use naom::primitives::{
    asset::{Asset, TokenAmount},
    block::Block,
    transaction::{Transaction, TxConstructor},
};
use sha3::{Digest, Sha3_256};
use sodiumoxide::crypto::secretbox::Key;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::{trace, warn};

/// Blocks & waits for timeout.
pub async fn timeout_at(timeout: Instant) {
    if let Ok(()) = time::timeout_at(timeout, future::pending::<()>()).await {
        panic!("pending completed");
    }
}

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

/// Return future that will connect to given peers on the network.
pub async fn loop_connnect_to_peers_async(mut node: Node, peers: Vec<SocketAddr>) {
    for peer in peers {
        trace!(?peer, "Try to connect to");
        while let Err(e) = node.connect_to(peer).await {
            trace!(?peer, ?e, "Try to connect to failed");
            tokio::time::delay_for(Duration::from_millis(500)).await;
        }
        trace!(?peer, "Try to connect to succeeded");
    }
}

/// Creates a "fake" transaction to save to the local wallet
/// for testing. The transaction will contain 4 tokens
///
/// NOTE: This is a test util function
pub async fn create_and_save_fake_to_wallet(
    wallet_db: &WalletDb,
) -> Result<(), Box<dyn std::error::Error>> {
    let (final_address, address_keys) = wallet_db.generate_payment_address().await;
    let (receiver_addr, _) = wallet_db.generate_payment_address().await;

    let (t_hash, _payment_tx) = create_valid_transaction(
        &"00000".to_owned(),
        &receiver_addr.address,
        &address_keys.public_key,
        &address_keys.secret_key,
    );

    // Save fund store
    let payment_to_save = TokenAmount(4000);
    wallet_db
        .save_payment_to_wallet(t_hash.clone(), payment_to_save)
        .await
        .unwrap();

    // Save transaction store
    println!("TX STORE: {:?}", (&t_hash, &final_address));
    wallet_db
        .save_transaction_to_wallet(t_hash, final_address)
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
pub fn format_parition_pow_address(addr: SocketAddr) -> String {
    format!("{}", addr)
}

/// Block to be used in Proof of Work
pub fn serialize_block_for_pow(block: &Block) -> Vec<u8> {
    serialize(block).unwrap()
}

/// Validate Proof of Work an address with a random number
pub fn validate_pow_for_address(pow: &ProofOfWork, rand_num: &Option<&Vec<u8>>) -> bool {
    let mut pow_body = pow.address.as_bytes().to_vec();
    pow_body.extend(rand_num.iter().flat_map(|r| r.iter()).copied());
    pow_body.extend(&pow.nonce);

    validate_pow(&pow_body)
}

/// Validate Proof of Work for a block with a mining transaction
/// Note: serialized_block is also manipulated as a buffer and restored before return.
pub fn validate_pow_block(serialized_block: &mut Vec<u8>, mining_tx: &str, nonce: &[u8]) -> bool {
    let serialized_block_len = serialized_block.len();
    serialized_block.extend(mining_tx.as_bytes());
    serialized_block.extend(nonce);

    let result = validate_pow(&serialized_block);
    serialized_block.truncate(serialized_block_len);
    result
}

/// Check the hash of given data reach MINING_DIFFICULTY
fn validate_pow(pow: &[u8]) -> bool {
    let pow_hash = Sha3_256::digest(pow).to_vec();
    pow_hash[0..MINING_DIFFICULTY].iter().all(|v| *v == 0)
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
        pub_keys: vec![*pub_key],
    };

    let amount = TokenAmount(4000);
    let tx_ins = construct_payment_tx_ins(vec![tx_const]);
    let payment_tx = construct_payment_tx(
        tx_ins,
        receiver_addr_hex.to_string(),
        None,
        None,
        Asset::Token(amount.clone()),
        amount,
    );
    let t_hash = construct_tx_hash(&payment_tx);
    (t_hash, payment_tx)
}
