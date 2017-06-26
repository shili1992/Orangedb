#/bin/bash
username=`whoami`

if [ $1 -eq 0 ];then
        ps -ef -u $username | grep dataserver|grep node| awk '{print $2,$8,$9,$10}'
fi


if [ $1 -eq 1 ];then
        ./ledis-cli  -p 56000
fi

if [ $1 -eq 3 ];then
        echo "kill all dataserver"
        pkill -9  dataserver
fi

if [ $1 -eq 4 ];then
        cat /dev/null > nohup.out
        rm -rf log
        rm -rf node0 node1 node2
        mkdir log

        nohup  ./dataserver -haddr 127.0.0.1:11000 -raddr 127.0.0.1:12000  -respaddr 127.0.0.1:56000  -tranaddr 127.0.0.1:37000  node0  log0 >log/0.log &
        sleep 2
        nohup  ./dataserver -haddr 127.0.0.1:11001 -raddr 127.0.0.1:12001  -respaddr 127.0.0.1:56001  -tranaddr 127.0.0.1:37001  -join 127.0.0.1:11000  node1 log1  >log/1.log &
        sleep 2
        nohup  ./dataserver -haddr 127.0.0.1:11002 -raddr 127.0.0.1:12002   -respaddr 127.0.0.1:56002 -tranaddr 127.0.0.1:37002 -join 127.0.0.1:11000 node2 log2  >log/2.log &
fi

if [ $1 -eq 5 ];then
        cat /dev/null > nohup.out
        cat /dev/null > log/log0
        cat /dev/null > log/log1
        cat /dev/null > log/log2
        rm -rf node0 node1 node2
fi

if [ $1 -eq 6 ];then
        go build
        echo "build dataserver success"
fi

if [ $1 -eq 7 ];then
        cat /dev/null > nohup.out
        rm -rf log
        rm -rf node0 node1 node2
        mkdir log

        nohup  ./dataserver -haddr 127.0.0.1:11000 -raddr 127.0.0.1:12000  -respaddr 127.0.0.1:56000  -tranaddr 127.0.0.1:37000 -conf 3part_1cluster.conf  -C 0  -G 3 -g 0 node0  log0 >log/0.log &
        sleep 1
        nohup  ./dataserver -haddr 127.0.0.1:11001 -raddr 127.0.0.1:12001  -respaddr 127.0.0.1:56001  -tranaddr 127.0.0.1:37001 -conf 3part_1cluster.conf  -C 0  -G 3 -g 1 node1 log1  >log/1.log &
        sleep 1
        nohup  ./dataserver -haddr 127.0.0.1:11002 -raddr 127.0.0.1:12002   -respaddr 127.0.0.1:56002 -tranaddr 127.0.0.1:37002 -conf 3part_1cluster.conf  -C 0 -G 3 -g 2  node2 log2  >log/2.log &
fi


if [ $1 -eq 8 ];then
        cat /dev/null > nohup.out
        rm -rf log
        rm -rf node0 node1 node2 node3
        mkdir log

        nohup  ./dataserver -haddr 127.0.0.1:11000 -raddr 127.0.0.1:12000  -respaddr 127.0.0.1:56000 -tranaddr 127.0.0.1:37000 -conf 2part_2cluster.conf  -C 0  -G 2 -g 0  node0 log0 >log/0.log &
        sleep 1
        nohup  ./dataserver -haddr 127.0.0.1:11001 -raddr 127.0.0.1:12001  -respaddr 127.0.0.1:56001 -tranaddr 127.0.0.1:37001 -conf 2part_2cluster.conf  -C 0  -G 2 -g 1  node1 log1 >log/1.log &
        sleep 1
        nohup  ./dataserver -haddr 127.0.0.1:11002 -raddr 127.0.0.1:12002  -respaddr 127.0.0.1:56002 -tranaddr 127.0.0.1:37002 -conf 2part_2cluster.conf  -join 127.0.0.1:11000 -C 1  -G 2 -g 0  node2 log2 >log/2.log &
        sleep 1
        nohup  ./dataserver -haddr 127.0.0.1:11003 -raddr 127.0.0.1:12003  -respaddr 127.0.0.1:56003 -tranaddr 127.0.0.1:37003 -conf 2part_2cluster.conf  -join 127.0.0.1:11001 -C 1  -G 2 -g 1  node3 log3 >log/3.log &

fi