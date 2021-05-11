use super::tests_last_version_db::{self, DbEntryType};
use super::tests_last_version_db_no_block;
use super::{
    get_upgrade_compute_db, get_upgrade_storage_db, get_upgrade_wallet_db, old, upgrade_compute_db,
    upgrade_storage_db, upgrade_wallet_db, DbCfg, UpgradeCfg, UpgradeError,
};
use crate::configurations::{DbMode, ExtraNodeParams, UserAutoGenTxSetup, WalletTxSpec};
use crate::constants::{DB_VERSION_KEY, LAST_BLOCK_HASH_KEY, NETWORK_VERSION_SERIALIZED};
use crate::db_utils::{
    new_db, new_db_with_version, SimpleDb, SimpleDbError, SimpleDbSpec, DB_COL_DEFAULT,
};
use crate::interfaces::{BlockStoredInfo, Response};
use crate::test_utils::{
    node_join_all_checked, remove_all_node_dbs, Network, NetworkConfig, NetworkNodeInfo, NodeType,
};
use crate::{compute, compute_raft, storage, storage_raft, wallet};
use naom::primitives::asset::TokenAmount;
use std::future::Future;
use std::time::Duration;
use tracing::info;

const WALLET_PASSWORD: &str = "TestPassword";
const LAST_BLOCK_STORED_NUM: u64 = 2;
const BLOCK_HASHES: &[&str] = &[
    "abd4012af0eb398f7fed06cdc633c506d374bb8e38174035c0fb9910d3d7a7f6",
    "04d6006a3923d06c00be1c9f26e38142e1defbe0d5a57ea60d94255c20a59a04",
    "7825220b591e99ad654acd0268f9ec0a5e08d3f46929f93cd4195ce943bb9f5c",
];
const BLOCK_INDEXES: &[&str] = &[
    "nIndexedBlockHashKey_0000000000000000",
    "nIndexedBlockHashKey_0000000000000001",
    "nIndexedBlockHashKey_0000000000000002",
];
const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);

enum Specs {
    Db(SimpleDbSpec, SimpleDbSpec),
    Wallet(SimpleDbSpec),
}

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_real_db() {
    let config = real_db(complete_network_config(20000));
    remove_all_node_dbs(&config);
    upgrade_common(config, "compute1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_in_memory() {
    let config = complete_network_config(20010);
    upgrade_common(config, "compute1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_compute_no_block_in_memory() {
    let config = complete_network_config(20015);
    let upgrade_cfg = cfg_upgrade_no_block();
    upgrade_common(config, "compute1", upgrade_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_storage_in_memory() {
    let config = complete_network_config(20020);
    upgrade_common(config, "storage1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_miner_in_memory() {
    let config = complete_network_config(20030);
    upgrade_common(config, "miner1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_user_in_memory() {
    let config = complete_network_config(20040);
    upgrade_common(config, "user1", cfg_upgrade()).await;
}

async fn upgrade_common(config: NetworkConfig, name: &str, upgrade_cfg: UpgradeCfg) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_stopped_from_config(&config);
    let n_info = network.get_node_info(name).unwrap().clone();
    let db = create_old_node_db(&n_info, upgrade_cfg.db_cfg);

    //
    // Act
    //
    let db = get_upgrade_node_db(&n_info, in_memory(db)).unwrap();
    let db = upgrade_node_db(&n_info, db, &upgrade_cfg).unwrap();
    let db = open_as_new_node_db(&n_info, in_memory(db)).unwrap();

    network.add_extra_params(name, in_memory(db));
    network.re_spawn_dead_nodes().await;
    raft_node_handle_event(&mut network, name, "Snapshot applied").await;

    //
    // Assert
    //
    match n_info.node_type {
        NodeType::Compute => {
            let (expected_mining_b_num, expected_b_num) =
                if upgrade_cfg.db_cfg == DbCfg::ComputeBlockToMine {
                    let expected = Some(LAST_BLOCK_STORED_NUM + 1);
                    (expected, expected)
                } else {
                    (None, Some(LAST_BLOCK_STORED_NUM))
                };

            let compute = network.compute(name).unwrap().lock().await;

            let block = compute.get_mining_block();
            assert_eq!(
                block.as_ref().map(|bs| bs.header.b_num),
                expected_mining_b_num
            );

            let b_num = compute.get_committed_current_block_num();
            assert_eq!(b_num, expected_b_num);
            assert_eq!(compute.get_request_list(), &Default::default());
        }
        NodeType::Storage => {
            let storage = network.storage(name).unwrap().lock().await;

            {
                let mut expected_blocks = vec![Some(vec![]); BLOCK_HASHES.len()];
                let mut actual_blocks = vec![None; BLOCK_HASHES.len()];

                let mut expected = Vec::new();
                let mut actual = Vec::new();
                for (_, k, v) in tests_last_version_db::STORAGE_DB_V0_2_0 {
                    if let Some(b_num) = BLOCK_HASHES.iter().position(|v| *k == v.as_bytes()) {
                        expected_blocks[b_num] = Some(v.to_vec());
                        actual_blocks[b_num] = storage.get_stored_value(BLOCK_INDEXES[b_num]);
                    }
                    expected.push(Some(v.to_vec()));
                    actual.push(storage.get_stored_value(k));
                }
                assert_eq!(actual, expected);
                assert_eq!(actual_blocks, expected_blocks);
                assert_eq!(storage.get_stored_values_count(), expected.len());
                assert_eq!(
                    storage.get_stored_value(LAST_BLOCK_HASH_KEY),
                    expected_blocks[LAST_BLOCK_STORED_NUM as usize]
                );
                assert_eq!(
                    storage.get_last_block_stored(),
                    &Some(get_expected_last_block_stored())
                );
            }
        }
        NodeType::User => {
            let user = network.user(name).unwrap().lock().await;
            let wallet = user.get_wallet_db();
            let payment = wallet.fetch_inputs_for_payment(TokenAmount(123)).await;
            assert_eq!(
                (payment.0.len(), payment.1, payment.2.len()),
                (1, TokenAmount(123), 1)
            );
        }
        NodeType::Miner => {
            let miner = network.miner(name).unwrap().lock().await;
            let wallet = miner.get_wallet_db();
            let payment = wallet.fetch_inputs_for_payment(TokenAmount(15020370)).await;
            assert_eq!(
                (payment.0.len(), payment.1, payment.2.len()),
                (2, TokenAmount(15020370), 2)
            );
        }
    }

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_real_db() {
    let config = real_db(complete_network_config(20100));
    remove_all_node_dbs(&config);
    open_upgrade_started_compute_common(config, "compute1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_in_memory() {
    let config = complete_network_config(20110);
    open_upgrade_started_compute_common(config, "compute1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_compute_no_block_in_memory() {
    let config = complete_network_config(20115);
    let upgrade_cfg = cfg_upgrade_no_block();
    open_upgrade_started_compute_common(config, "compute1", upgrade_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_storage_in_memory() {
    let config = complete_network_config(20120);
    open_upgrade_started_compute_common(config, "storage1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_miner_in_memory() {
    let config = complete_network_config(20130);
    open_upgrade_started_compute_common(config, "miner1", cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn open_upgrade_started_user_in_memory() {
    let config = complete_network_config(20140);
    open_upgrade_started_compute_common(config, "user1", cfg_upgrade()).await;
}

async fn open_upgrade_started_compute_common(
    config: NetworkConfig,
    name: &str,
    upgrade_cfg: UpgradeCfg,
) {
    test_step_start();

    //
    // Arrange
    //
    let mut network = Network::create_stopped_from_config(&config);
    let n_info = network.get_node_info(name).unwrap().clone();
    let db = create_old_node_db(&n_info, upgrade_cfg.db_cfg);

    //
    // Act
    //
    let err_new_1 = open_as_new_node_db(&n_info, cloned_in_memory(&db)).err();
    let db = open_as_old_node_db(&n_info, in_memory(db)).unwrap();

    let db = get_upgrade_node_db(&n_info, in_memory(db)).unwrap();
    let db = open_as_old_node_db(&n_info, in_memory(db)).unwrap();
    let db = get_upgrade_node_db(&n_info, in_memory(db)).unwrap();

    let err_new_2 = open_as_new_node_db(&n_info, cloned_in_memory(&db)).err();

    //
    // Assert
    //
    assert!(err_new_1.is_some());
    assert!(err_new_2.is_some());

    test_step_complete(network).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_restart_network_real_db() {
    let config = real_db(complete_network_config(20200));
    remove_all_node_dbs(&config);
    upgrade_restart_network_common(config, cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_restart_network_in_memory() {
    let config = complete_network_config(20210);
    upgrade_restart_network_common(config, cfg_upgrade()).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_restart_network_compute_no_block_in_memory() {
    let config = complete_network_config(20215);
    let upgrade_cfg = cfg_upgrade_no_block();
    upgrade_restart_network_common(config, upgrade_cfg).await;
}

#[tokio::test(basic_scheduler)]
async fn upgrade_restart_network_compute_no_block_raft_2_in_memory() {
    // Create 2 identical copy of the database in memory for each node in raft grup.
    // Upgrade applying the configuration data and run.
    let raft_len = 2;

    let config = complete_network_config(20220).with_groups(raft_len, raft_len);
    let mut upgrade_cfg = cfg_upgrade_no_block();
    upgrade_cfg.raft_len = raft_len;
    upgrade_restart_network_common(config, upgrade_cfg).await;
}

async fn upgrade_restart_network_common(mut config: NetworkConfig, upgrade_cfg: UpgradeCfg) {
    test_step_start();

    //
    // Arrange
    //
    config.user_test_auto_gen_setup = get_test_auto_gen_setup(Some(0));
    let mut network = Network::create_stopped_from_config(&config);
    let compute_nodes = &config.nodes[&NodeType::Compute];
    let extra_blocks = 2usize;
    let expected_block_num = LAST_BLOCK_STORED_NUM + extra_blocks as u64;

    for name in network.dead_nodes().clone() {
        let n_info = network.get_node_info(&name).unwrap();
        let db = create_old_node_db(n_info, upgrade_cfg.db_cfg);
        let db = get_upgrade_node_db(n_info, in_memory(db)).unwrap();
        let db = upgrade_node_db(n_info, db, &upgrade_cfg).unwrap();
        network.add_extra_params(&name, in_memory(db));
    }

    //
    // Act
    //
    network.re_spawn_dead_nodes().await;
    for node_name in compute_nodes {
        node_send_coordinated_shutdown(&mut network, &node_name, expected_block_num).await;
    }

    let handles = network
        .spawn_main_node_loops(TIMEOUT_TEST_WAIT_DURATION)
        .await;
    node_join_all_checked(handles, &"").await.unwrap();

    //
    // Assert
    //
    {
        let compute = network.compute("compute1").unwrap().lock().await;
        let b_num = compute.get_committed_current_block_num();
        assert_eq!(b_num, Some(expected_block_num));
    }
    {
        let storage = network.storage("storage1").unwrap().lock().await;
        let count = storage.get_stored_values_count();
        let mining_txs_per_block = upgrade_cfg.raft_len;
        let expected_count = tests_last_version_db::STORAGE_DB_V0_2_0.len()
            + extra_blocks * (mining_txs_per_block + 1);

        assert_eq!(count, expected_count);

        let block_stored = storage.get_last_block_stored().as_ref();
        assert_eq!(block_stored.map(|b| b.block_num), Some(expected_block_num));
    }

    test_step_complete(network).await;
}

//
// Test helpers
//

fn create_old_node_db(info: &NetworkNodeInfo, db_cfg: DbCfg) -> ExtraNodeParams {
    match info.node_type {
        NodeType::Compute => ExtraNodeParams {
            db: Some(create_old_db(
                &old::compute::DB_SPEC,
                info.db_mode,
                if db_cfg == DbCfg::ComputeBlockToMine {
                    &tests_last_version_db::COMPUTE_DB_V0_2_0
                } else {
                    &tests_last_version_db_no_block::COMPUTE_DB_V0_2_0
                },
            )),
            raft_db: Some(create_old_db(
                &old::compute_raft::DB_SPEC,
                info.db_mode,
                if db_cfg == DbCfg::ComputeBlockToMine {
                    &tests_last_version_db::COMPUTE_RAFT_DB_V0_2_0
                } else {
                    &tests_last_version_db_no_block::COMPUTE_RAFT_DB_V0_2_0
                },
            )),
            ..Default::default()
        },
        NodeType::Storage => ExtraNodeParams {
            db: Some(create_old_db(
                &old::storage::DB_SPEC,
                info.db_mode,
                &tests_last_version_db::STORAGE_DB_V0_2_0,
            )),
            raft_db: Some(create_old_db(
                &old::storage_raft::DB_SPEC,
                info.db_mode,
                &tests_last_version_db::STORAGE_RAFT_DB_V0_2_0,
            )),
            ..Default::default()
        },
        NodeType::User => ExtraNodeParams {
            wallet_db: Some(create_old_db(
                &old::wallet::DB_SPEC,
                info.db_mode,
                &tests_last_version_db::USER_DB_V0_2_0,
            )),
            ..Default::default()
        },
        NodeType::Miner => ExtraNodeParams {
            wallet_db: Some(create_old_db(
                &old::wallet::DB_SPEC,
                info.db_mode,
                &tests_last_version_db::MINER_DB_V0_2_0,
            )),
            ..Default::default()
        },
    }
}

fn test_step_start() {
    let _ = tracing_subscriber::fmt::try_init();
    info!("Test Step start");
}

async fn test_step_complete(network: Network) {
    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
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
    old_dbs: ExtraNodeParams,
) -> Result<ExtraNodeParams, SimpleDbError> {
    let version = old::constants::NETWORK_VERSION_SERIALIZED;
    let specs = match info.node_type {
        NodeType::Compute => Specs::Db(old::compute::DB_SPEC, old::compute_raft::DB_SPEC),
        NodeType::Storage => Specs::Db(old::storage::DB_SPEC, old::storage_raft::DB_SPEC),
        NodeType::User => Specs::Wallet(old::wallet::DB_SPEC),
        NodeType::Miner => Specs::Wallet(old::wallet::DB_SPEC),
    };
    open_as_version_node_db(info, &specs, version, old_dbs)
}

fn open_as_new_node_db(
    info: &NetworkNodeInfo,
    old_dbs: ExtraNodeParams,
) -> Result<ExtraNodeParams, SimpleDbError> {
    let version = Some(NETWORK_VERSION_SERIALIZED);
    let specs = match info.node_type {
        NodeType::Compute => Specs::Db(compute::DB_SPEC, compute_raft::DB_SPEC),
        NodeType::Storage => Specs::Db(storage::DB_SPEC, storage_raft::DB_SPEC),
        NodeType::User => Specs::Wallet(wallet::DB_SPEC),
        NodeType::Miner => Specs::Wallet(wallet::DB_SPEC),
    };
    open_as_version_node_db(info, &specs, version, old_dbs)
}

fn open_as_version_node_db(
    info: &NetworkNodeInfo,
    specs: &Specs,
    version: Option<&[u8]>,
    old_dbs: ExtraNodeParams,
) -> Result<ExtraNodeParams, SimpleDbError> {
    match specs {
        Specs::Db(spec, raft_spec) => {
            let db = new_db_with_version(info.db_mode, spec, version, old_dbs.db)?;
            let raft_db = new_db_with_version(info.db_mode, raft_spec, version, old_dbs.raft_db)?;
            Ok(ExtraNodeParams {
                db: Some(db),
                raft_db: Some(raft_db),
                ..Default::default()
            })
        }
        Specs::Wallet(spec) => {
            let wallet_db = new_db_with_version(info.db_mode, spec, version, old_dbs.wallet_db)?;
            Ok(ExtraNodeParams {
                wallet_db: Some(wallet_db),
                ..Default::default()
            })
        }
    }
}

pub fn get_upgrade_node_db(
    info: &NetworkNodeInfo,
    old_dbs: ExtraNodeParams,
) -> Result<ExtraNodeParams, UpgradeError> {
    match info.node_type {
        NodeType::Compute => get_upgrade_compute_db(info.db_mode, old_dbs),
        NodeType::Storage => get_upgrade_storage_db(info.db_mode, old_dbs),
        NodeType::User => get_upgrade_wallet_db(info.db_mode, old_dbs),
        NodeType::Miner => get_upgrade_wallet_db(info.db_mode, old_dbs),
    }
}

pub fn upgrade_node_db(
    info: &NetworkNodeInfo,
    dbs: ExtraNodeParams,
    upgrade_cfg: &UpgradeCfg,
) -> Result<ExtraNodeParams, UpgradeError> {
    match info.node_type {
        NodeType::Compute => upgrade_compute_db(dbs, upgrade_cfg),
        NodeType::Storage => upgrade_storage_db(dbs, upgrade_cfg),
        NodeType::User => upgrade_wallet_db(dbs, upgrade_cfg),
        NodeType::Miner => upgrade_wallet_db(dbs, upgrade_cfg),
    }
}

fn complete_network_config(initial_port: u16) -> NetworkConfig {
    NetworkConfig {
        initial_port,
        compute_raft: true,
        storage_raft: true,
        in_memory_db: true,
        compute_partition_full_size: 1,
        compute_minimum_miner_pool_len: 1,
        nodes: vec![(NodeType::User, vec!["user1".to_string()])]
            .into_iter()
            .collect(),
        compute_seed_utxo: Default::default(),
        compute_genesis_tx_in: None,
        user_wallet_seeds: Default::default(),
        compute_to_miner_mapping: Default::default(),
        test_duration_divider: 1,
        passphrase: Some(WALLET_PASSWORD.to_owned()),
        user_test_auto_gen_setup: Default::default(),
    }
    .with_groups(1, 1)
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

fn cfg_upgrade() -> UpgradeCfg {
    UpgradeCfg {
        raft_len: 1,
        passphrase: WALLET_PASSWORD.to_owned(),
        db_cfg: DbCfg::ComputeBlockToMine,
    }
}

fn cfg_upgrade_no_block() -> UpgradeCfg {
    UpgradeCfg {
        raft_len: 1,
        passphrase: WALLET_PASSWORD.to_owned(),
        db_cfg: DbCfg::ComputeBlockInStorage,
    }
}

fn get_expected_last_block_stored() -> BlockStoredInfo {
    use naom::primitives::{
        asset::Asset,
        transaction::{Transaction, TxIn, TxOut},
    };
    use naom::script::{lang::Script, StackEntry};

    BlockStoredInfo {
        block_hash: BLOCK_HASHES[LAST_BLOCK_STORED_NUM as usize].to_owned(),
        block_num: LAST_BLOCK_STORED_NUM,
        nonce: Vec::new(),
        merkle_hash: "24c87c26cf5233f59ffe9b3f8f19cd7e1cdcf871dafb2e3e800e15cf155da944".to_owned(),
        mining_transactions: std::iter::once((
            "g27fac41a2d62a56c8962e3d360838c8".to_owned(),
            Transaction {
                inputs: vec![TxIn {
                    previous_out: None,
                    script_signature: Script {
                        stack: vec![StackEntry::Num(LAST_BLOCK_STORED_NUM as usize)],
                    },
                }],
                outputs: vec![TxOut {
                    value: Asset::Token(TokenAmount(7510184)),
                    locktime: 0,
                    drs_block_hash: None,
                    drs_tx_hash: None,
                    script_public_key: Some("79609a5b997a265ab3f370c4abef00ad".to_owned()),
                }],
                version: old::constants::NETWORK_VERSION as usize,
                druid_info: None,
            },
        ))
        .collect(),
        shutdown: false,
    }
}

fn get_test_auto_gen_setup(count_override: Option<usize>) -> UserAutoGenTxSetup {
    let user1_tx = vec![
        WalletTxSpec {
            out_point:  "0-000000".to_owned(),
            secret_key: "e2fa624994ec5c6f46e9a991ed8e8791c4d2ce2d7ed05a827bd45416e5a19555f4f0c1a951959e88fe343de5a2ebe7efbcb15422090b3549577f424db6851ca5".to_owned(),
            public_key: "f4f0c1a951959e88fe343de5a2ebe7efbcb15422090b3549577f424db6851ca5".to_owned(),
            amount: 2
        },
        WalletTxSpec {
            out_point: "0-000001".to_owned(),
            secret_key: "09784182e825fbd7e53333aa6b5f1d55bc19a992d5cf71253212264825bc89c8a80fc230590e38bd648dc6bc4b6019d39e841f78657ad5138f351a70b6165c43".to_owned(),
            public_key: "a80fc230590e38bd648dc6bc4b6019d39e841f78657ad5138f351a70b6165c43".to_owned(),
            amount: 5
        }
    ];

    UserAutoGenTxSetup {
        user_initial_transactions: vec![user1_tx],
        user_setup_tx_chunk_size: Some(5),
        user_setup_tx_in_per_tx: Some(3),
        user_setup_tx_max_count: count_override.unwrap_or(100000),
    }
}

fn in_memory(dbs: ExtraNodeParams) -> ExtraNodeParams {
    ExtraNodeParams {
        db: dbs.db.and_then(|v| v.in_memory()),
        raft_db: dbs.raft_db.and_then(|v| v.in_memory()),
        wallet_db: dbs.wallet_db.and_then(|v| v.in_memory()),
    }
}

fn cloned_in_memory(dbs: &ExtraNodeParams) -> ExtraNodeParams {
    ExtraNodeParams {
        db: dbs.db.as_ref().and_then(|v| v.cloned_in_memory()),
        raft_db: dbs.raft_db.as_ref().and_then(|v| v.cloned_in_memory()),
        wallet_db: dbs.wallet_db.as_ref().and_then(|v| v.cloned_in_memory()),
    }
}

fn test_timeout() -> impl Future<Output = &'static str> + Unpin {
    Box::pin(async move {
        tokio::time::delay_for(TIMEOUT_TEST_WAIT_DURATION).await;
        "Test timeout elapsed"
    })
}

async fn raft_node_handle_event(network: &mut Network, node: &str, reason_val: &str) {
    if let Some(n) = network.compute(node) {
        let mut n = n.lock().await;
        match n.handle_next_event(&mut test_timeout()).await {
            Some(Ok(Response { success, reason })) if success && reason == reason_val => {}
            other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
        }
    } else if let Some(n) = network.storage(node) {
        let mut n = n.lock().await;
        match n.handle_next_event(&mut test_timeout()).await {
            Some(Ok(Response { success, reason })) if success && reason == reason_val => {}
            other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
        }
    }
}

async fn node_send_coordinated_shutdown(network: &mut Network, node: &str, at_block: u64) {
    use crate::utils::LocalEvent;
    let mut event_tx = network.get_local_event_tx(&node).await.unwrap();
    let event = LocalEvent::CoordinatedShutdown(at_block);
    event_tx.send(event, "test shutdown").await.unwrap();
}
