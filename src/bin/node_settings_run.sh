#!/bin/sh

echo " "
echo "//-----------------------------//"
echo "Building nodes"
echo "//-----------------------------//"
echo " "
cargo build --bins --release
echo " "
echo "//-----------------------------//"
echo "Delete databases"
echo "//-----------------------------//"
echo " "
rm -rf src/db/db/test.* src/wallet/wallet/test.*
echo " "
echo "//-----------------------------//"
echo "Running nodes for node_settings_local_raft.toml"
echo "//-----------------------------//"
echo " "
RUST_LOG="debug,raft=warn" target/release/storage > storage_0.log 2>&1 &
c1=$!
target/release/compute > compute_0.log 2>&1 &
c2=$!
target/release/miner --compute_connect > miner_1.log 2>&1 &
c3=$!
RUST_LOG="debug" target/release/user --compute_connect > user_0.log 2>&1 &
c4=$!

trap 'echo Kill All $c1 $c2 $c3 $c4; kill $c1 $c2 $c3 $c4' INT
tail -f storage_0.log
