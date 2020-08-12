use crate::constants::{DB_PATH, DB_PATH_LIVE, DB_PATH_TEST};
use crate::primitives::block::Block;
use crate::primitives::transaction::{OutPoint, Transaction};
use crate::sha3::Digest;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use rocksdb::{Options, DB};
use sha3::Sha3_256;

/// Determines whether a transaction has been spent before
///
/// ### Arguments
///
/// * `prev_out`    - OutPoint of previous transaction
pub fn tx_has_spent(prev_out: Option<OutPoint>) -> bool {
    if let Some(o) = prev_out {
        // TODO: Allow for net type change
        let _prev_out_tx = match get_transaction(o.b_hash.clone(), o.t_hash.clone(), 0) {
            Some(t) => t,
            None => return true,
        };

        let load_path = format!("{}/{}", DB_PATH, DB_PATH_TEST);
        let db = DB::open_default(load_path.clone()).unwrap();
        let mut iter = db.raw_iterator();

        // Read the last key that starts with 'a'
        iter.seek(o.b_hash);

        while iter.valid() {
            iter.next();
            let next_block = deserialize::<Block>(&iter.value().unwrap()).unwrap();

            for tx in next_block.transactions {
                let hash_input = Bytes::from(serialize(&tx).unwrap());
                let hash_key = Sha3_256::digest(&hash_input);

                if hash_key.to_vec() == o.t_hash {
                    return true;
                }
            }
        }
    }

    false
}

/// Finds the relevant transaction based on a block and tx hash. If the transaction is not found
/// the return value will be None
///
/// ### Arguments
///
/// * `b_hash`  - Block hash
/// * `t_hash`  - Transaction hash
/// * `net`     - Which network blockchain to fetch
pub fn get_transaction(b_hash: Vec<u8>, t_hash: Vec<u8>, net: usize) -> Option<Transaction> {
    let load_path = match net {
        0 => format!("{}/{}", DB_PATH, DB_PATH_TEST),
        _ => format!("{}/{}", DB_PATH, DB_PATH_LIVE),
    };

    let db = DB::open_default(load_path.clone()).unwrap();

    let block = match db.get(b_hash) {
        Ok(Some(value)) => deserialize::<Block>(&value).unwrap(),
        Ok(None) => panic!("Block hash not present in this blockchain"),
        Err(e) => panic!("Error retrieving block: {:?}", e),
    };

    for tx in block.transactions {
        let hash_input = Bytes::from(serialize(&tx).unwrap());
        let hash_key = Sha3_256::digest(&hash_input);

        if hash_key.to_vec() == t_hash {
            return Some(tx);
        }
    }

    None
}
