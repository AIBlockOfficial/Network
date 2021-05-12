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
echo "Running nodes for node_settings_local_raft_3.toml"
echo "//-----------------------------//"
echo " "

# Using https://unix.stackexchange.com/questions/68956/block-network-access-of-a-process
# create groups:
# > sudo groupadd test_s2
# > sudo groupadd test_c2
# > sudo usermod -a -G test_s2 $USER
# > sudo usermod -a -G test_c2 $USER
# > sudo groups $USER
if [ "$1" = "use_test_groups" ]
then
    sg test_s2 "echo use test_s2 group for storage 2"
    sg test_c2 "echo use test_c2 group for storage 2"
    USE_TEST_GROUPS=1
fi

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

if [ "$USE_TEST_GROUPS" = "1" ]
then
    RUST_LOG="$STORAGE_LOG,raft=warn" sg test_s2 "target/release/storage --config=src/bin/node_settings_local_raft_3.toml --index=2" > storage_2.log 2>&1 &
else
    RUST_LOG="$STORAGE_LOG,raft=warn" target/release/storage --config=src/bin/node_settings_local_raft_3.toml --api_port=3001 --index=2 > storage_2.log 2>&1 &
    s2=$!
fi
RUST_LOG="$STORAGE_LOG,raft=warn" target/release/storage --config=src/bin/node_settings_local_raft_3.toml --api_port=3002 --index=1 > storage_1.log 2>&1 &
s1=$!
RUST_LOG="$STORAGE_LOG,raft=warn" target/release/storage --config=src/bin/node_settings_local_raft_3.toml --api_port=3003 > storage_0.log 2>&1 &
s0=$!

if [ "$USE_TEST_GROUPS" = "1" ]
then
    RUST_LOG="$COMPUTE_LOG" sg test_c2 "target/release/compute --config=src/bin/node_settings_local_raft_3.toml --index=2" > compute_2.log 2>&1 &
else
    RUST_LOG="$COMPUTE_LOG" target/release/compute --config=src/bin/node_settings_local_raft_3.toml --index=2 > compute_2.log 2>&1 &
    c2=$!
fi
RUST_LOG="$COMPUTE_LOG" target/release/compute --config=src/bin/node_settings_local_raft_3.toml --index=1 > compute_1.log 2>&1 &
c1=$!
RUST_LOG="$COMPUTE_LOG" target/release/compute --config=src/bin/node_settings_local_raft_3.toml > compute_0.log 2>&1 &
c0=$!

RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=8 --compute_index=2 > miner_8.log 2>&1 &
m8=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=7 --compute_index=2 > miner_7.log 2>&1 &
m7=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=6 --compute_index=2 > miner_6.log 2>&1 &
m6=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=5 --compute_index=1 > miner_5.log 2>&1 &
m5=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=4 --compute_index=1 > miner_4.log 2>&1 &
m4=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=3 --compute_index=1 > miner_3.log 2>&1 &
m3=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=2 --compute_index=0 > miner_2.log 2>&1 &
m2=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  --index=1 --compute_index=0 > miner_1.log 2>&1 &
m1=$!
RUST_LOG="$MINER_LOG" target/release/miner --config=src/bin/node_settings_local_raft_3.toml  > miner_0.log 2>&1 &
m0=$!

RUST_LOG="$USER_LOG" target/release/user  --config=src/bin/node_settings_local_raft_3.toml > user_0.log 2>&1 &
u0=$!

echo $s2 $s1 $s0 $c2 $c1 $c0 $m8 $m7 $m6 $m5 $m4 $m3 $m2 $m1 $m0 $u0
trap 'echo Kill All $s2 $s1 $s0 $c2 $c1 $c0 $m8 $m7 $m6 $m5 $m4 $m3 $m2 $m1 $m0 $u0; kill $s2 $s1 $s0 $c2 $c1 $c0 $m8 $m7 $m6 $m5 $m4 $m3 $m2 $m1 $m0 $u0' INT
tail -f storage_1.log
