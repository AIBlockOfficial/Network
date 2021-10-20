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
echo "Delete databases"
echo "//-----------------------------//"
echo " "
if [ "$1" != "no_rm" ]
then
    echo "delete dbs"
    rm -rf src/db/db/test.* src/wallet/wallet/test.*
fi
echo " "
echo "//-----------------------------//"
echo "Running nodes for node_settings_local_raft_2.toml"
echo "//-----------------------------//"
echo " "

if [ "$1" = "set_log" ]
then
    echo set log storage: $2, compute: $3, miner: $4, user: $5.
    STORAGE_LOG=$2
    COMPUTE_LOG=$3
    MINER_LOG=$4
    USER_LOG=$5
else
    STORAGE_LOG=debug
    COMPUTE_LOG=warn
    MINER_LOG=warn
    USER_LOG=debug
fi

RUST_LOG="$STORAGE_LOG,raft=warn" target/release/node storage --config=src/bin/node_settings_local_raft_2.toml --api_port=3001 --index=1 > storage_1.log 2>&1 &
s1=$!
RUST_LOG="$STORAGE_LOG,raft=warn" target/release/node storage --config=src/bin/node_settings_local_raft_2.toml --api_port=3002 > storage_0.log 2>&1 &
s0=$!
RUST_LOG="$COMPUTE_LOG" target/release/node compute --config=src/bin/node_settings_local_raft_2.toml --index=1 --api_port=3003 > compute_1.log 2>&1 &
c1=$!
RUST_LOG="$COMPUTE_LOG" target/release/node compute --config=src/bin/node_settings_local_raft_2.toml --api_port=3004 > compute_0.log 2>&1 &
c0=$!
RUST_LOG="$MINER_LOG" target/release/node miner --config=src/bin/node_settings_local_raft_2.toml --index=5 --api_port=3005 --compute_index=1 > miner_5.log 2>&1 &
m5=$!
RUST_LOG="$MINER_LOG" target/release/node miner --config=src/bin/node_settings_local_raft_2.toml --index=4 --api_port=3006 --compute_index=0 > miner_4.log 2>&1 &
m4=$!
RUST_LOG="$MINER_LOG" target/release/node miner --config=src/bin/node_settings_local_raft_2.toml --index=3 --api_port=3007 --compute_index=1 > miner_3.log 2>&1 &
m3=$!
RUST_LOG="$MINER_LOG" target/release/node miner --config=src/bin/node_settings_local_raft_2.toml --index=2 --api_port=3008 --compute_index=0 > miner_2.log 2>&1 &
m2=$!
RUST_LOG="$MINER_LOG" target/release/node miner --config=src/bin/node_settings_local_raft_2.toml --index=1 --api_port=3009 --compute_index=1 > miner_1.log 2>&1 &
m1=$!
RUST_LOG="$MINER_LOG" target/release/node miner --config=src/bin/node_settings_local_raft_2.toml --with_user_index=1 --api_port=3010 > miner_0.log 2>&1 &
m0=$!
RUST_LOG="$USER_LOG" target/release/node user --config=src/bin/node_settings_local_raft_2.toml > user_0.log 2>&1 &
u0=$!

echo $s1 $s0 $c1 $c0 $m5 $m4 $m3 $m2 $m1 $m0 $u0
trap 'echo Kill All $s1 $s0 $c1 $c0 $m5 $m4 $m3 $m2 $m1 $m0 $u0; kill $s1 $s0 $c1 $c0 $m5 $m4 $m3 $m2 $m1 $m0 $u0' INT
tail -f storage_1.log
