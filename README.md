# Orangedb
calvin 系统实现
### References
+ Calvin: Fast Distributed Transactions for Partitioned Database Systems
+ The Case for Determinism in Database Systems

&nbsp;      
## Features      
+ 客户端兼容 redis-cli    ledis-cli
+ 使用raft 支持 多副本

&nbsp;
##Cient tool
   客户端使用ledisdb的客户端工具 ledis-cli,使用方法如下：
>  Usage of  ledis-cli:
  -h string
   &emsp;&emsp;data  server ip (default 127.0.0.1) (default "127.0.0.1")
  -p int
   &emsp;&emsp;data  server port (default 56000) 
   
##Build and install
```bash
mkdir $WORKSPACE
cd $WORKSPACE
git clone git@github.com:shili1992/Orangedb.git
cd src/github.com/shili1992/Orangedb/cmd/dataserver
go build
```

##Running servers
+ **开启一个dataserver**
```bash
cd src/github.com/shili1992/Orangedb/cmd/dataserver
./dataserver  node0  log0 &
``` 
+ **开启一个集群**
```bash
cd src/github.com/shili1992/Orangedb/cmd/dataserver
./dataserver  node0  log0 &
sleep 2
./dataserver -haddr 127.0.0.1:11001 -raddr 127.0.0.1:12001  -respaddr 127.0.0.1:56001  -join 127.0.0.1:11000 node1 log1 &
sleep 2
./dataserver -haddr 127.0.0.1:11002 -raddr 127.0.0.1:12002   -respaddr 127.0.0.1:56002 -join 127.0.0.1:11000 node2 log2 &
``` 

##Todo
+ 实现Calvin 确定化调度
+ 提供事务支撑（rc 隔离级别）
+ 实现分布式的calvin 系统 
+ 提供依赖事务的支持
+ 实现 Mvcc架构的存储层
+ 实现快照或者可串行化隔离级别
+ 提供多种数据结构支持


