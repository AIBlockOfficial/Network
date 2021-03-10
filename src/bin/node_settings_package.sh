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
echo "Create tar.gz file"
echo "//-----------------------------//"
echo " "

COMMIT=`git rev-parse HEAD`
tar -cvzf target/release/znp-$COMMIT.tar.gz target/release/compute  target/release/miner  target/release/storage  target/release/user src/bin/*.toml src/bin/*.json src/bin/*.sh
tar -cvzf target/release/znp-$COMMIT.configs.tar.gz src/bin/*.toml src/bin/*.json src/bin/*.sh
tar -cvzf target/release/znp-$COMMIT.light-config.tar.gz src/bin/*.toml src/bin/*.sh

echo "Complete package at target/release/znp-$COMMIT.tar.gz"
echo "Complete config package at target/release/znp-$COMMIT.configs.tar.gz"
echo "Complete light config package at target/release/znp-$COMMIT.light-config.tar.gz"
