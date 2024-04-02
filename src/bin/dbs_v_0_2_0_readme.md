# Instruction to generate:
 * src/bin/dbs_v_0_2_0_new_block.tar.gz
 * src/bin/dbs_v_0_2_0_no_new_block.tar.gz
 * src/upgrade/tests_last_version_db_no_block.rs
 * src/upgrade/tests_last_version_db.rs

## In a separate clone of znp and noam:
 * checkout ZNP at tag v0.2.0
 * checkout NAOM at tag v0.2.0
 * Make sure znp cargo.toml point to corresponding tw_chain

In `src/bin/node_settings.toml`, remove all nodes not used in the network. There should only remain first of each mempool/storage/miner/user.

Also update as follow, to use raft and leave 45 second to execute instructions:
```toml
mempool_raft = 1
storage_raft = 1
storage_block_timeout = 45000
```

In `src/bin/node_settings_run.sh`, comment out the line launching mempool, miner and user.

## Generate database and snapshots in v0.2.0 clone

These instructions will be easier to follow with 5 tabs ready to run the following:
 * `rm -rf src/db/db src/wallet/wallet; src/bin/node_settings_run.sh set_log info`
 * `RUST_LOG="info" target/release/node mempool`
 * `RUST_LOG="info" target/release/node user`
 * `RUST_LOG="info" target/release/node miner`
 * `tar -czf db_mempool_before_block.tar src/db/db/test.mempool*`

The execution than proceed:
 * Start all network nodes in the same order described above.
    * Initial block -> block 0 created -> user tx generated
    * Block 0 send to storage
    * -----
    * 45 sec => Blockstored 0 send to mempool -> block 1 created -> user tx generated
    * Block 1 send to storage (with tx)
 * Kill `user` to prevent user tx on last block
    * (After generating transaction for block 0 and 1: kill user (see user output: kill after 2 "New Generated txs sent")
    * -----
    * 30 sec => Blockstored 1 send to mempool -> block 2 created -> !!NO user TXs!!
    * Block 2 send to storage (with tx)
 * `tar -czf db_mempool_before_block.tar src/db/db/test.mempool*`
 * Kill `miner` to prevent more block stored
    * (Generate db snapshot as if we killed mempool node then (see mempool ouptput: tar/kill miner after 3 "Send Block to storage"  (0,1,2))
    * -----
    * 30 sec => Blockstored 2 send to mempool -> block 3 created !!!NO TXs!!!
* Kill `mempool` & `storage` node in stalled network

Build the databases snapshoot:
* `tar -czf dbs_v_0_2_0_new_block.tar.gz src/db/db src/wallet/wallet`
    * Build tar.gz with block stored in storage and next block created in mempool
* `rm -rf src/db/db/test.mempool*; tar -xzf db_mempool_before_block.tar`
* `tar -czf dbs_v_0_2_0_no_new_block.tar.gz src/db/db src/wallet/wallet`
    * Build tar.gz with block stored in storage and next block not created in mempool.

## Add snapshot to repo

Copy the snapshots to V3 ZNP repo to:
 * src/bin/dbs_v_0_2_0_new_block.tar.gz
 * src/bin/dbs_v_0_2_0_no_new_block.tar.gz

Update rust dumps:

```
rm -rf src/db/db/test.* src/wallet/wallet/test.*; tar -xzf src/bin/dbs_v_0_2_0_new_block.tar.gz 
target/debug/upgrade --config=src/bin/node_settings_local_raft_1.toml --type all --processing read > src/upgrade/tests_last_version_db.rs

rm -rf src/db/db/test.* src/wallet/wallet/test.*; tar -xzf src/bin/dbs_v_0_2_0_no_new_block.tar.gz
target/debug/upgrade  --config=src/bin/node_settings_local_raft_1.toml --type mempool --processing read > src/upgrade/tests_last_version_db_no_block.rs
```
