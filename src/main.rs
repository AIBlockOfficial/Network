#![allow(unused)]

extern crate async_std;
extern crate chrono;
extern crate crypto;
extern crate hex;
extern crate rand;
extern crate rug;
extern crate sha3;
extern crate sodiumoxide;

mod comms_handler;
mod compute;
mod configurations;
mod constants;
mod interfaces;
mod key_creation;
mod miner;
mod storage;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod unicorn;
mod user;
mod utils;
mod wallet;

#[cfg(not(features = "mock"))]
pub(crate) use comms_handler::Node;
#[cfg(features = "mock")]
pub(crate) use mock::Node;

use sodiumoxide::crypto::sign;

use crate::constants::{DB_PATH, DB_PATH_LIVE, DB_PATH_TEST};
use crate::sha3::Digest;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use sha3::Sha3_256;

fn main() {
    // Payment variables
    // let receiver_address = vec![0, 1, 2, 3, 4, 5, 6];
    // let (_pk, sk) = sign::gen_keypair();
    // let (pk, _sk) = sign::gen_keypair();
    // let t_hash = vec![0, 0, 0];
    // let signature = sign::sign_detached(&t_hash.clone(), &sk);
    // let drs_block_hash = vec![1, 2, 3, 4, 5, 6];

    // let tx_const = TxConstructor {
    //     t_hash: t_hash,
    //     prev_n: 0,
    //     b_hash: vec![0],
    //     signatures: vec![signature],
    //     pub_keys: vec![pk],
    // };

    // let tx_ins = construct_payment_tx_ins(vec![tx_const]);
    // let payment_tx = construct_payment_tx(tx_ins, receiver_address, Some(drs_block_hash), 4);

    // // Block variables
    // let mut block = Block::new();
    // block.header.b_hash = 1;
    // block.transactions.push(payment_tx);

    // // Save the shit
    // let hash_input = Bytes::from(serialize(&block).unwrap());
    // let hash_key = Sha3_256::digest(&hash_input);
    // let save_path = format!("{}/{}", DB_PATH, DB_PATH_TEST);

    // //println!("Hash Key: {:?}", hash_key);

    // let db = DB::open_default(save_path.clone()).unwrap();

    // let new_hash_key = [
    //     11, 176, 211, 40, 76, 147, 38, 195, 112, 150, 107, 40, 216, 226, 134, 169, 126, 185, 48,
    //     35, 194, 23, 124, 251, 183, 150, 11, 50, 57, 8, 39, 160,
    // ];
    // // match db.get(new_hash_key) {
    // //     Ok(Some(value)) => println!(
    // //         "retrieved value {:?}",
    // //         deserialize::<Block>(&value).unwrap()
    // //     ),
    // //     Ok(None) => println!("value not found"),
    // //     Err(e) => println!("operational problem encountered: {}", e),
    // // }
    // //db.put(hash_key, serialize(&block).unwrap()).unwrap();

    // let _ = DB::destroy(&Options::default(), save_path.clone());
}
