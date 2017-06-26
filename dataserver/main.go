package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"github.com/shili1992/Orangedb"
	"github.com/laohanlinux/go-logger/logger"
)

// Command line defaults
const (
	DefaultHTTPAddr = "127.0.0.1:11000"
	DefaultRESPAddr = "127.0.0.1:56000"
	DefaultRaftAddr = "127.0.0.1:12000"
	DefaultTranPortAddr = "127.0.0.1:37000"
	DefaultConfFile = "./node.conf"
	DefaultClusterID  =  0
	DefaultPartitionID  = 0
	DefaultPartitionNum  = 0
)

// Command line parameters
var clusterID    int
var partitionID  int
var partitionNum   int
var httpAddr string
var respAddr string
var raftAddr string
var joinAddr string
var tranAddr string
var confFile string


func init() {
	flag.IntVar(&clusterID,"C",DefaultClusterID,"Set  cluster id")  //本机器所在集群的id
	flag.IntVar(&partitionNum,"G",DefaultPartitionNum,"Set  partition  num")  //本机器所在partition 的数量
	flag.IntVar(&partitionID,"g",DefaultPartitionID,"Set  partiton  id")  //本机器所在partition 的id
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")  //http 请求接口，可以接受raft 请求
	flag.StringVar(&respAddr, "respaddr", DefaultRESPAddr, "Set the RESP bind address")  //redis 客户端地址
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")     //raft 之间通信地址
	flag.StringVar(&tranAddr, "tranaddr", DefaultTranPortAddr, "Set transport bind address") //系统内部进行数据传递地址
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")   //raft注册的地址
	flag.StringVar(&confFile,"conf",DefaultConfFile,"set configure file")   //集群配置文件

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	raftDir := flag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	// Init log confiure
	logfile := flag.Arg(1)
	if logfile == "" {
		fmt.Fprintf(os.Stderr, "No logDir storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll("log", 0700)
	os.Create("log/" + logfile)
	logger.SetConsole(true)
	err := logger.SetRollingDaily("log", logfile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "No log  directory specified\n")
		os.Exit(1)
	}

	fsmStore := Orangedb.NewFsmStore()
	fsmStore.RaftDir = raftDir
	fsmStore.RaftBind = raftAddr
	if err := fsmStore.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	//start  listen http service
	h := Orangedb.NewService(httpAddr, respAddr,tranAddr,fsmStore,partitionID,partitionNum,clusterID,confFile)
	if err := h.StartService(); err != nil {
		log.Fatalf("failed to start  service: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	log.Println("Orangedb started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("Orangedb exiting")
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
