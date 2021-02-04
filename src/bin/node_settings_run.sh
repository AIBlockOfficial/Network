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
s0=$!
RUST_LOG="warn" target/release/compute > compute_0.log 2>&1 &
c0=$!
RUST_LOG="warn" target/release/miner --compute_connect > miner_1.log 2>&1 &
m0=$!
RUST_LOG="debug" target/release/user --compute_connect > user_0.log 2>&1 &
u0=$!

echo $s0 $c0 $m0 $u0
trap 'echo Kill All $s0 $c0 $m0 $u0; kill $s0 $c0 $m0 $u0' INT
tail -f storage_0.log
