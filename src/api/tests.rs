use crate::api::routes;
use crate::configurations::DbMode;
use crate::constants::BLOCK_PREPEND;
use crate::db_utils::{new_db, SimpleDb};
use crate::storage::{put_named_block_to_block_chain, put_to_block_chain, DB_SPEC};

use bincode::serialize;
use naom::primitives::block::Block;
use sha3::{Digest, Sha3_256};
use std::sync::{Arc, Mutex};
use warp::http::{HeaderMap, HeaderValue};

/// Util function to create a stub DB containing a single block
fn get_db_with_block() -> Arc<Mutex<SimpleDb>> {
    let block = Block::new();

    let mut db = new_db(DbMode::InMemory, &DB_SPEC, None);
    let mut batch = db.batch_writer();

    let block_input = serialize(&block).unwrap();
    let block_hash = {
        let hash_digest = Sha3_256::digest(&block_input);
        let mut hash_digest = hex::encode(hash_digest);
        hash_digest.insert(0, BLOCK_PREPEND as char);
        hash_digest
    };

    let pointer = put_to_block_chain(&mut batch, &block_hash, &block_input);
    put_named_block_to_block_chain(&mut batch, &pointer, 0);
    let batch = batch.done();
    db.write(batch).unwrap();

    Arc::new(Mutex::new(db))
}

/*------- TESTS --------*/

#[tokio::test(basic_scheduler)]
async fn should_fetch_block_info_by_nums() {
    let db = get_db_with_block();

    let filter = routes::block_info_by_nums(db);

    let res = warp::test::request()
        .method("POST")
        .path("/block_by_num")
        .header("Content-Type", "application/json")
        .json(&vec![0_u64])
        .reply(&filter)
        .await;

    // Header to match
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers(), &headers);
    assert_eq!(res.body(), "[{\"block\":{\"header\":{\"version\":1,\"bits\":0,\"nonce\":[],\"b_num\":0,\"seed_value\":[],\"previous_hash\":null,\"merkle_root_hash\":\"\"},\"transactions\":[]},\"mining_tx_hash_and_nonces\":{}}]");
}
