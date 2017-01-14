#!/bin/bash
pid=`ps -ef | grep Orangedb| awk '{print $2}'`
sudo kill `echo $pid`
go build

rm -rf log
rm -rf node0 node1 node2


./datasever  node0  log0 &
sleep 2
./datasever -haddr 127.0.0.1:11001 -raddr 127.0.0.1:12001  -respaddr 127.0.0.1:56001  -join 127.0.0.1:11000 node1 log1 &
sleep 2
./datasever -haddr 127.0.0.1:11002 -raddr 127.0.0.1:12002   -respaddr 127.0.0.1:56002 -join 127.0.0.1:11000 node2 log2 &