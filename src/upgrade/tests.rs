use super::{get_upgrade_compute_db, old, upgrade_compute_db};
use crate::configurations::{DbMode, ExtraNodeParams};
use crate::constants::DB_VERSION_KEY;
use crate::db_utils::DB_COL_DEFAULT;
use crate::db_utils::{new_db_no_version_check, SimpleDb};
use crate::test_utils::{
    init_arc_node, init_instance_info, remove_all_node_dbs_in_info, NetworkConfig,
    NetworkInstanceInfo, NodeType,
};
use std::collections::BTreeSet;

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_real_db() {
    let config = complete_network_config(20000, false);
    let info = init_instance_info(&config);
    remove_all_node_dbs_in_info(&info);

    upgrade_compute_common(config, info).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_in_memory() {
    let config = complete_network_config(20010, true);
    let info = init_instance_info(&config);
    upgrade_compute_common(config, info).await;
}

async fn upgrade_compute_common(config: NetworkConfig, info: NetworkInstanceInfo) {
    //
    // Arrange
    //
    let db_mode = info.node_infos.get("compute1").as_ref().unwrap().db_mode;
    let old_db = create_old_compute_db(db_mode).in_memory();

    //
    // Act
    //
    let extra = {
        let db = get_upgrade_compute_db(db_mode, old_db).unwrap();
        let db = upgrade_compute_db(db).unwrap();
        ExtraNodeParams {
            db: db.in_memory(),
            ..Default::default()
        }
    };
    let compute = init_arc_node("compute1", &config, &info, extra).await;
    let compute = compute.compute().unwrap().lock().await;
    let actual_req_list = compute.get_request_list();

    //
    // Assert
    //
    assert_eq!(actual_req_list, &BTreeSet::new());
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_real_db() {
    let config = complete_network_config(20100, false);
    let info = init_instance_info(&config);
    remove_all_node_dbs_in_info(&info);

    open_upgrade_started_compute_common(info).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_in_memory() {
    let config = complete_network_config(20110, true);
    let info = init_instance_info(&config);
    open_upgrade_started_compute_common(info).await;
}

async fn open_upgrade_started_compute_common(info: NetworkInstanceInfo) {
    //
    // Arrange
    //
    let db_mode = info.node_infos.get("compute1").as_ref().unwrap().db_mode;
    let old_db = create_old_compute_db(db_mode).in_memory();

    //
    // Act
    //
    let db = get_upgrade_compute_db(db_mode, old_db).unwrap().in_memory();
    let db = get_upgrade_compute_db(db_mode, db);

    //
    // Assert
    //
    db.unwrap();
}

fn create_old_compute_db(db_mode: DbMode) -> SimpleDb {
    let mut old_db = new_db_no_version_check(db_mode, &old::compute::DB_SPEC, true, None).unwrap();
    old_db.delete_cf(DB_COL_DEFAULT, DB_VERSION_KEY).unwrap();
    old_db
}

fn complete_network_config(initial_port: u16, in_memory_db: bool) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        compute_raft: false,
        storage_raft: false,
        in_memory_db,
        compute_partition_full_size: 1,
        compute_minimum_miner_pool_len: 1,
        nodes: vec![
            (NodeType::Miner, vec!["miner1".to_string()]),
            (NodeType::Compute, vec!["compute1".to_string()]),
            (NodeType::Storage, vec!["storage1".to_string()]),
            (NodeType::User, vec!["user1".to_string()]),
        ]
        .into_iter()
        .collect(),
        compute_seed_utxo: Default::default(),
        compute_genesis_tx_in: None,
        user_wallet_seeds: Default::default(),
        compute_to_miner_mapping: Default::default(),
        test_duration_divider: 1,
    }
}
