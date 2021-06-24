#!/bin/sh

echo " "
echo "//-----------------------------//"
echo "Building nodes"
echo "//-----------------------------//"
echo " "
cargo build --bins --release
if [ "$?" != "0" ]
then
    exit 1
fi
echo " "
echo "//-----------------------------//"
echo "Delete, Extract databases"
echo "//-----------------------------//"
echo " "
if [ "$1" != "no_rm" ]
then
    echo "delete dbs"
    rm -rf src/db/db/test.* src/wallet/wallet/test.*

    echo "extract dbs"
    tar -xzf src/bin/dbs_v_0_2_0_no_new_block.tar.gz
fi
echo " "
echo "//-----------------------------//"
echo "Running upgrade for node_settings_local_raft_3.toml"
echo "Launch network afterward manually for node_settings_local_raft_3.toml"
echo "//-----------------------------//"
echo " "

if [ "$1" = "set_log" ]
then
    echo set log upgrae: $2, pre_launch: $3,
    UPGRADE_LOG=$2
    PRE_LAUNCH_LOG=$3
    MINER_LOG=$4
    USER_LOG=$5
    UPGRADE_LOG=$6
else
    UPGRADE_LOG=debug
    PRE_LAUNCH_LOG=debug
fi

echo " "
echo "//-----------------------------//"
echo "Running upgrade for node_settings_local_raft_3.toml"
echo "//-----------------------------//"
echo " "
RUST_LOG="$UPGRADE_LOG" target/release/upgrade --config=src/bin/node_settings_local_raft_3.toml --type=all --processing=upgrade --compute_block=discard --ignore=storage.1,storage.2,compute.1,compute.2,miner.1,miner.2,miner.3,miner.4,miner.5,miner.6,miner.7,miner.8,miner.9,user.1> upgrade_all.log 2>&1
cat upgrade_all.log

echo " "
echo "//-----------------------------//"
echo "Launch network afterward manually for node_settings_local_raft_3.toml"
echo "//-----------------------------//"
echo " "
RUST_LOG="$PRE_LAUNCH_LOG" target/release/pre_launch --config=src/bin/node_settings_local_raft_3.toml --type=storage --index=2 > pre_launch_storage_2.log 2>&1 &
s2=$!
RUST_LOG="$PRE_LAUNCH_LOG" target/release/pre_launch --config=src/bin/node_settings_local_raft_3.toml --type=storage --index=1 > pre_launch_storage_1.log 2>&1 &
s1=$!
RUST_LOG="$PRE_LAUNCH_LOG" target/release/pre_launch --config=src/bin/node_settings_local_raft_3.toml --type=storage > pre_launch_storage_0.log 2>&1 &
s0=$!

RUST_LOG="$PRE_LAUNCH_LOG" target/release/pre_launch --config=src/bin/node_settings_local_raft_3.toml --type=compute --index=2 > pre_launch_compute_2.log 2>&1 &
c2=$!
RUST_LOG="$PRE_LAUNCH_LOG" target/release/pre_launch --config=src/bin/node_settings_local_raft_3.toml --type=compute --index=1 > pre_launch_compute_1.log 2>&1 &
c1=$!
RUST_LOG="$PRE_LAUNCH_LOG" target/release/pre_launch --config=src/bin/node_settings_local_raft_3.toml --type=compute > pre_launch_compute_0.log 2>&1 &
c0=$!

echo $s2 $s1 $s0 $c2 $c1 $c0
trap 'echo Kill All $s2 $s1 $s0 $c2 $c1 $c0; kill $s2 $s1 $s0 $c2 $c1 $c0' INT

echo Wait for all processes to complete: $s2 $s1 $s0 $c2 $c1 $c0
wait $s2 $s1 $s0 $c2 $c1 $c0
echo all processes to complete: $s2 $s1 $s0 $c2 $c1 $c0

echo " "
echo "//-----------------------------//"
echo "cat pre_launch_storage_0.log"
echo "//-----------------------------//"
echo " "
cat pre_launch_storage_0.log

echo " "
echo "//-----------------------------//"
echo "cat pre_launch_storage_1.log"
echo "//-----------------------------//"
echo " "
cat pre_launch_storage_1.log
