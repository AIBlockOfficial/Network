use super::tests_last_version_db::{self, DbEntryType};
use super::{
    get_upgrade_compute_db, get_upgrade_storage_db, old, upgrade_compute_db, upgrade_storage_db,
    UpgradeError,
};
use crate::configurations::{DbMode, ExtraNodeParams};
use crate::constants::{DB_VERSION_KEY, NETWORK_VERSION_SERIALIZED};
use crate::db_utils::{
    new_db, new_db_with_version, SimpleDb, SimpleDbError, SimpleDbSpec, DB_COL_DEFAULT,
};
use crate::interfaces::BlockStoredInfo;
use crate::test_utils::{
    init_arc_node, init_instance_info, remove_all_node_dbs_in_info, NetworkConfig,
    NetworkInstanceInfo, NetworkNodeInfo, NodeType,
};
use crate::{compute, storage};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use tracing::info;

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_real_db() {
    let config = real_db(complete_network_config(20000));
    let info = init_instance_info(&config);
    remove_all_node_dbs_in_info(&info);

    upgrade_common(config, info, "compute1").await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_in_memory() {
    let config = complete_network_config(20010);
    let info = init_instance_info(&config);
    upgrade_common(config, info, "compute1").await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_storage_in_memory() {
    let config = complete_network_config(20020);
    let info = init_instance_info(&config);
    upgrade_common(config, info, "storage1").await;
}

async fn upgrade_common(config: NetworkConfig, info: NetworkInstanceInfo, name: &str) {
    test_step_start();

    //
    // Arrange
    //
    let n_info = info.node_infos.get(name).unwrap();
    let db = create_old_node_db(n_info);

    //
    // Act
    //
    let db = get_upgrade_node_db(n_info, db.in_memory()).unwrap();
    let db = upgrade_node_db(n_info, db).unwrap();
    let db = open_as_new_node_db(n_info, db.in_memory()).unwrap();

    let node = {
        let extra = ExtraNodeParams {
            db: db.in_memory(),
            ..Default::default()
        };
        init_arc_node(name, &config, &info, extra).await
    };

    //
    // Assert
    //
    match n_info.node_type {
        NodeType::Compute => {
            let compute = node.compute().unwrap().lock().await;

            let expected_req_list: BTreeSet<SocketAddr> =
                std::iter::once("127.0.0.1:12340".parse().unwrap()).collect();
            assert_eq!(compute.get_request_list(), &expected_req_list);
        }
        NodeType::Storage => {
            let storage = node.storage().unwrap().lock().await;

            {
                let mut expected = Vec::new();
                let mut actual = Vec::new();
                for (_, k, v) in tests_last_version_db::STORAGE_DB_V0_2_0 {
                    expected.push(Some(v.to_vec()));
                    actual.push(storage.get_stored_value(k));
                }
                assert_eq!(actual, expected);
                assert_eq!(storage.get_stored_values_count(), expected.len());
                assert_eq!(
                    storage.get_last_block_stored(),
                    &Some(get_expected_last_block_stored())
                );
            }
        }
        _ => unimplemented!(),
    }
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_real_db() {
    let config = real_db(complete_network_config(20100));
    let info = init_instance_info(&config);
    remove_all_node_dbs_in_info(&info);

    open_upgrade_started_compute_common(info, "compute1").await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_in_memory() {
    let config = complete_network_config(20110);
    let info = init_instance_info(&config);
    open_upgrade_started_compute_common(info, "compute1").await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_storage_in_memory() {
    let config = complete_network_config(20120);
    let info = init_instance_info(&config);
    open_upgrade_started_compute_common(info, "storage1").await;
}

async fn open_upgrade_started_compute_common(info: NetworkInstanceInfo, name: &str) {
    test_step_start();

    //
    // Arrange
    //
    let n_info = info.node_infos.get(name).unwrap();
    let db = create_old_node_db(n_info);

    //
    // Act
    //
    let err_new_1 = open_as_new_node_db(n_info, db.cloned_in_memory()).err();
    let db = open_as_old_node_db(n_info, db.in_memory()).unwrap();

    let db = get_upgrade_node_db(n_info, db.in_memory()).unwrap();
    let db = open_as_old_node_db(n_info, db.in_memory()).unwrap();
    let db = get_upgrade_node_db(n_info, db.in_memory()).unwrap();

    let err_new_2 = open_as_new_node_db(n_info, db.cloned_in_memory()).err();

    //
    // Assert
    //
    assert!(err_new_1.is_some());
    assert!(err_new_2.is_some());
}

fn create_old_node_db(info: &NetworkNodeInfo) -> SimpleDb {
    let (spec, entries) = match info.node_type {
        NodeType::Compute => (
            &old::compute::DB_SPEC,
            &tests_last_version_db::COMPUTE_DB_V0_2_0,
        ),
        NodeType::Storage => (
            &old::storage::DB_SPEC,
            &tests_last_version_db::STORAGE_DB_V0_2_0,
        ),
        _ => unimplemented!(),
    };
    create_old_db(spec, info.db_mode, entries)
}

fn create_old_db(spec: &SimpleDbSpec, db_mode: DbMode, entries: &[DbEntryType]) -> SimpleDb {
    let mut db = new_db(db_mode, spec, None);

    let mut batch = db.batch_writer();
    batch.delete_cf(DB_COL_DEFAULT, DB_VERSION_KEY);
    for (_column, key, value) in entries {
        batch.put_cf(DB_COL_DEFAULT, key, value);
    }
    let batch = batch.done();
    db.write(batch).unwrap();

    db
}

fn open_as_old_node_db(
    info: &NetworkNodeInfo,
    old_db: Option<SimpleDb>,
) -> Result<SimpleDb, SimpleDbError> {
    let version = old::constants::NETWORK_VERSION_SERIALIZED;
    let spec = match info.node_type {
        NodeType::Compute => &old::compute::DB_SPEC,
        NodeType::Storage => &old::storage::DB_SPEC,
        _ => unimplemented!(),
    };
    new_db_with_version(info.db_mode, spec, version, old_db)
}

fn open_as_new_node_db(
    info: &NetworkNodeInfo,
    old_db: Option<SimpleDb>,
) -> Result<SimpleDb, SimpleDbError> {
    let version = Some(NETWORK_VERSION_SERIALIZED);
    let spec = match info.node_type {
        NodeType::Compute => &compute::DB_SPEC,
        NodeType::Storage => &storage::DB_SPEC,
        _ => unimplemented!(),
    };
    new_db_with_version(info.db_mode, spec, version, old_db)
}

pub fn get_upgrade_node_db(
    info: &NetworkNodeInfo,
    old_db: Option<SimpleDb>,
) -> Result<SimpleDb, UpgradeError> {
    match info.node_type {
        NodeType::Compute => get_upgrade_compute_db(info.db_mode, old_db),
        NodeType::Storage => get_upgrade_storage_db(info.db_mode, old_db),
        _ => unimplemented!(),
    }
}

pub fn upgrade_node_db(info: &NetworkNodeInfo, db: SimpleDb) -> Result<SimpleDb, UpgradeError> {
    match info.node_type {
        NodeType::Compute => upgrade_compute_db(db),
        NodeType::Storage => upgrade_storage_db(db),
        _ => unimplemented!(),
    }
}

//
// Test helpers
//

fn test_step_start() {
    let _ = tracing_subscriber::fmt::try_init();
    info!("Test Step start");
}

fn complete_network_config(initial_port: u16) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        compute_raft: false,
        storage_raft: false,
        in_memory_db: true,
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

fn real_db(mut config: NetworkConfig) -> NetworkConfig {
    config.in_memory_db = false;
    config
}

fn get_static_column(spec: SimpleDbSpec, name: &str) -> &'static str {
    [DB_COL_DEFAULT]
        .iter()
        .chain(spec.columns.iter())
        .find(|sn| **sn == name)
        .unwrap()
}

fn get_expected_last_block_stored() -> BlockStoredInfo {
    use naom::primitives::{
        asset::{Asset, TokenAmount},
        transaction::{Transaction, TxIn, TxOut},
    };
    use naom::script::{lang::Script, StackEntry};

    BlockStoredInfo {
        block_hash: "f628017bb00472a33a5070bce18ef68320c558f999350e1a3164f319ba9b5c00".to_owned(),
        block_num: 2,
        nonce: Vec::new(),
        merkle_hash: "24c87c26cf5233f59ffe9b3f8f19cd7e1cdcf871dafb2e3e800e15cf155da944".to_owned(),
        mining_transactions: std::iter::once((
            "g567775cc21b9647014a6b7959919911".to_owned(),
            Transaction {
                inputs: vec![TxIn {
                    previous_out: None,
                    script_signature: Script {
                        stack: vec![StackEntry::Num(2)],
                    },
                }],
                outputs: vec![TxOut {
                    value: Asset::Token(TokenAmount(7510184)),
                    locktime: 0,
                    drs_block_hash: None,
                    drs_tx_hash: None,
                    script_public_key: Some("6e8f40e652d5f26c6e65180602a289a2".to_owned()),
                }],
                version: 0,
                druid_info: None,
            },
        ))
        .collect(),
        shutdown: false,
    }
}
