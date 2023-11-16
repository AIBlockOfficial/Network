# Instruction to generate:
 * src/bin/dbs_v_0_5_0_upgraded.tar.gz
 * src/bin/dbs_v_0_6_0_with_old.tar.gz
 * src/upgrade/tests_last_version_db.rs

## In a separate clone of znp and noam:
 * checkout ZNP at tag v1.1.2 tag | commit ID 20f7df91
 * checkout NAOM at commit ID 33a41339
 * Make sure znp cargo.toml point to corresponding a_block_chain

## Generate database and snapshots in v0.5.0 clone

These instructions will be easier to follow with 5 tabs ready to run the following:
 * `cargo build --bins --release`
 * `rm -rf src/db/db/test.* src/wallet/wallet/test.*; tar -xzf src/bin/dbs_v_0_5_0_with_old.tar.gz`
 * `target/release/upgrade --config=src/bin/node_settings_upgraded.toml --type=all --processing=upgrade --passphrase=TestPassword`
 * `tar -czf src/bin/dbs_v_0_5_0_upgraded.tar.gz src/db/db src/wallet/wallet`

 Then:
 * `echo 22 > shutdown_coordinated`
 * `rm -rf src/db/db/test.* src/wallet/wallet/test.*; tar -xzf src/bin/dbs_v_0_5_0_upgraded.tar.gz ; src/bin/node_settings_upgraded_run.sh set_log debug debug debug debug`
 * Wait for shutdown.
 * `tar -czf src/bin/dbs_v_0_6_0_with_old.tar.gz src/db/db src/wallet/wallet`

## Add snapshot to repo

Copy the snapshots to V6 ZNP repo to:
 * src/bin/dbs_v_0_6_0_with_old.tar.gz

Update rust dumps:

```
rm -rf src/db/db/test.* src/wallet/wallet/test.*; tar -xzf src/bin/dbs_v_0_6_0_with_old.tar.gz
target/release/upgrade --config=src/bin/node_settings_upgraded.toml --type all --processing read > src/upgrade/tests_last_version_db.rs
```

## Notes for next DB upgrade
* `LOCKED_COINBASE_KEY` column will need deserialization and conversion if structure changes with next DB upgrade.
* `new_create_asset` will result in a different `Script` value because of `OP_DROP` opcode being added. Although, this type of script might not form part of any transactions on current block on compute consensused, so conversion might not be necessary.
