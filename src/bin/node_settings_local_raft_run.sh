#!/bin/sh

echo " "
echo "//-----------------------------//"
echo "Building nodes"
echo "//-----------------------------//"
echo " "
cargo build --bins --release
echo " "
echo "//-----------------------------//"
echo "Running nodes for node_settings_local_raft.toml"
echo "//-----------------------------//"
echo " "
RUST_LOG="debug,raft=warn" target/release/storage --config=src/bin/node_settings_local_raft.toml --index=1 > storage_1.log 2>&1 &
c1=$!
RUST_LOG="debug,raft=warn" target/release/storage --config=src/bin/node_settings_local_raft.toml > storage_0.log 2>&1 &
c2=$!
target/release/compute --config=src/bin/node_settings_local_raft.toml --index=1 > compute_1.log 2>&1 &
c3=$!
target/release/compute --config=src/bin/node_settings_local_raft.toml > compute_0.log 2>&1 &
c4=$!
target/release/miner --config=src/bin/node_settings_local_raft.toml  --index=5 --compute_index=1 --compute_connect > miner_5.log 2>&1 &
c5=$!
target/release/miner --config=src/bin/node_settings_local_raft.toml  --index=4 --compute_index=0 --compute_connect > miner_4.log 2>&1 &
c6=$!
target/release/miner --config=src/bin/node_settings_local_raft.toml  --index=3 --compute_index=1 --compute_connect > miner_3.log 2>&1 &
c7=$!
target/release/miner --config=src/bin/node_settings_local_raft.toml  --index=2 --compute_index=0 --compute_connect > miner_2.log 2>&1 &
c8=$!
target/release/miner --config=src/bin/node_settings_local_raft.toml  --index=1 --compute_index=1 --compute_connect > miner_1.log 2>&1 &
c9=$!
target/release/miner --config=src/bin/node_settings_local_raft.toml  --compute_connect > miner_0.log 2>&1 &
c10=$!
RUST_LOG="debug" target/release/user  --config=src/bin/node_settings_local_raft.toml --compute_connect > user_0.log 2>&1 &
c11=$!

trap 'echo Kill All $c1 $c2 $c3 $c4 $c5 $c6 $c7 $c8 $c9 $c10 $c11; kill $c1 $c2 $c3 $c4 $c5 $c6 $c7 $c8 $c9 $c10 $c11' INT
tail -f storage_1.log
