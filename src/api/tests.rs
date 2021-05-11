use crate::api::routes;
use crate::configurations::DbMode;
use crate::db_utils::new_db;
use crate::storage_raft::DB_SPEC;
use naom::primitives::block::Block;
use std::sync::{Arc, Mutex};

#[tokio::test(basic_scheduler)]
async fn test_block_info_by_nums() {
    let mut block = Block::new();
    let db = new_db(DbMode::InMemory, &DB_SPEC, None);

    let filter = routes::block_info_by_nums(Arc::new(Mutex::new(db)));

    let res = warp::test::request()
        .method("POST")
        .path("/block_by_num")
        .header("Content-Type", "application/json")
        .json(&vec![0])
        .reply(&filter)
        .await;

    println!("res: {:?}", res);

    assert_eq!(res.status(), 200);
    assert_eq!(res.body(), "{}");
}
