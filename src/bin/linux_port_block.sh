#!/bin/sh

echo "linux_port_block $1 $2"
if [ "$1" = "blockp" ]
then
    echo "Drop port $2"
    iptables -A INPUT -p tcp --sport $2 -j DROP
    iptables -A OUTPUT -p tcp --dport $2 -j DROP
fi

if [ "$1" = "removep" ]
then
    echo "Remove rule for port $2"
    iptables -D INPUT -p tcp --sport $2 -j DROP
    iptables -D OUTPUT -p tcp --dport $2 -j DROP
fi

if [ "$1" = "blocka" ]
then
    echo "Drop addr $2"
    iptables -A INPUT -p tcp -s $2 -j DROP
    iptables -A OUTPUT -p tcp -d $2 -j DROP
fi

# Using https://unix.stackexchange.com/questions/68956/block-network-access-of-a-process
if [ "$1" = "removea" ]
then
    echo "Remove rule for addr $2"
    iptables -D INPUT -p tcp -s $2 -j DROP
    iptables -D OUTPUT -p tcp -d $2 -j DROP
fi

if [ "$1" = "blockg" ]
then
    echo "Drop group $2 (Use 'sg $2 \"command\"')"
    iptables -A OUTPUT -m owner --gid-owner $2 -j DROP
fi

if [ "$1" = "removeg" ]
then
    echo "Remove rule for group $2"
    iptables -D OUTPUT -m owner --gid-owner $2 -j DROP
fi

if [ "$1" = "remove_first" ]
then
    echo "Remove first rule"
    iptables -D INPUT 1
    iptables -D OUTPUT 1
fi

if [ "$1" = "display" ]
    then
    iptables -L -n -v
fi
