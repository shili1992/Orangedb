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

	"github.com/laohanlinux/go-logger/logger"
	"github.com/shili1992/Orangedb/service"
	"github.com/shili1992/Orangedb/store"
)

// Command line defaults
const (
	DefaultHTTPAddr = "127.0.0.1:11000"
	DefaultRESPAddr = "127.0.0.1:56000"
	DefaultRaftAddr = "127.0.0.1:12000"
)

// Command line parameters
var httpAddr string
var respAddr string
var raftAddr string
var joinAddr string

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&respAddr, "respaddr", DefaultRESPAddr, "Set the RESP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
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

	s := store.New()
	s.RaftDir = raftDir
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	//start  listen http service
	h := service.New(httpAddr, respAddr, s)
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