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
RUST_LOG="debug,raft=warn" target/release/storage > storage_0.log 2>&1 &
c1=$!
target/release/compute > compute_0.log 2>&1 &
c2=$!
target/release/miner --compute_connect > miner_1.log 2>&1 &
c3=$!

trap 'echo Kill All $c1 $c2 $c3; kill $c1 $c2 $c3' INT
tail -f storage_0.log
