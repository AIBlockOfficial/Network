#!/bin/sh

set -x
NODE_TYPE=$1
INDEX=$2
COMPUTE_INDEX=$3
LOG_LEVEL=$4
PACKAGE_NAME=$5
START_CLEAN_DB=$6
NODE_CONFIG=src/bin/node_settings_aws.toml
NODE_INIT_BLOCK=src/bin/initial_block.json
set +x

echo " "
echo "//-----------------------------//"
echo "Kill existing node"
echo "//-----------------------------//"
echo " "
set -v
sh ./znp/kill_node.sh
set +v

echo " "
echo "//-----------------------------//"
echo "Check if need start clean db $START_CLEAN_DB==start_with_clean_db"
echo "Check if need start folder $START_CLEAN_DB==start_with_wipe_znp"
echo "//-----------------------------//"
echo " "
if [ "$START_CLEAN_DB" = "start_with_clean_db" ]
then
    set -v
    echo "Delete databases"
    rm -rf ./znp/src/db/db/* ./znp/src/wallet/wallet/*
    set +v
elif [ "$START_CLEAN_DB" = "start_with_wipe_znp" ]
then
    set -v
    echo "Delete all znp"
    rm -rf ./znp
    set +v
fi

echo " "
echo "//-----------------------------//"
echo "Extract package if $PACKAGE_NAME != keep_file_restart_only"
echo "//-----------------------------//"
echo " "
if [ "$PACKAGE_NAME" != "keep_file_restart_only" ]
then
    set -v
    mkdir ./znp
    tar -xvzf $PACKAGE_NAME -C znp
    set +v
fi

echo " "
echo "//-----------------------------//"
echo "Move to znp path"
echo "//-----------------------------//"
echo " "
set -v
cd znp
set +v

echo " "
echo "//-----------------------------//"
echo "Running $NODE_TYPE node $INDEX for $NODE_CONFIG"
echo "//-----------------------------//"
echo " "

if [ "$NODE_TYPE" = "storage" ]
then
    set -x
    RUST_LOG="$LOG_LEVEL" target/release/storage --config=$NODE_CONFIG --index=$INDEX > storage_$INDEX.log 2>&1 &
    n0=$!
    set +x
elif [ "$NODE_TYPE" = "compute" ]
then
    set -x
    RUST_LOG="$LOG_LEVEL" target/release/compute --config=$NODE_CONFIG --initial_block_config=$NODE_INIT_BLOCK --index=$INDEX > compute_$INDEX.log 2>&1 &
    n0=$!
    set +x
elif [ "$NODE_TYPE" = "miner" ]
then
    set -x
    RUST_LOG="$LOG_LEVEL" target/release/miner --config=$NODE_CONFIG --index=$INDEX --compute_index=$COMPUTE_INDEX > miner_$INDEX.log 2>&1 &
    n0=$!
    set +x
elif [ "$NODE_TYPE" = "user" ]
then
    set -x
    RUST_LOG="$LOG_LEVEL" target/release/user --config=$NODE_CONFIG --initial_block_config=$NODE_INIT_BLOCK --index=$INDEX --compute_index=$COMPUTE_INDEX > user_$INDEX.log 2>&1 &
    n0=$!
    set +x
fi

echo "kill $n0" > kill_$NODE_TYPE.sh
echo "kill $n0" > kill_node.sh
echo "Started $NODE_TYPE node at $n0, kill with kill_$NODE_TYPE.sh or kill_node.sh"
set +x
set +v

# Make sure the command do not complete so node process not killed
# when screen exit
echo "wait $n0" 
wait $n0
