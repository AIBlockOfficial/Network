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
echo "Get configuration files"
echo "//-----------------------------//"
echo " "
rm -rf target/release/src/bin
mkdir -p target/release/src/bin
cp src/bin/*.toml src/bin/*.json src/bin/*.sh target/release/src/bin
echo " "
echo "//-----------------------------//"
echo "Create tar.gz file"
echo "//-----------------------------//"
echo " "

COMMIT=`git rev-parse HEAD`
cd target/release
tar -cvzf znp-$COMMIT.tar.gz compute  miner  storage  user src

echo "Complete package at target/release/znp-$COMMIT.tar.gz"
