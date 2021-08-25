#!/bin/sh

echo $1
echo $2
echo --param1="$(<src/bin/tls_data/user101.bundle.pem)"
IFS= read -r some_data <"src/bin/tls_data/user101.bundle.pem"
echo $IFS
echo $some_data
TEST= <"src/bin/tls_data/user101.bundle.pem"
echo $TEST
TEST=$(cat <"src/bin/tls_data/user101.bundle.pem")
echo  bb $TEST

echo $(cat <"src/bin/tls_data/user101.bundle.pem")
