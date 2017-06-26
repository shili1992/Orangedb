// Package service provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package Orangedb

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"github.com/laohanlinux/go-logger/logger"
	"sync"
	"github.com/hashicorp/raft"
	"fmt"
	"github.com/widuu/goini"
	"strconv"
)

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) (string, error)

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error
}

// Service provides HTTP  and  RESP service.
type Service struct {
	httpAddr     	string
	httpListener 	net.Listener

	respAddr     	string
	respListener 	net.Listener

	tranportAddr	string
	transport 	*Transport
	rpcCh <-chan RPC   //处理网络传输层 中的命令，并将处理信息回复给调用者

	respLock      	sync.Mutex //互斥锁
	respClients   	map[*RespClient] struct{}

	store        	*FsmStore

	preProcessor   	*PreProcessor  //预处理模块
	sequencerWriter *SequencerWriter
	sequencerReader	*SequencerReader
	scheduler	*Scheduler
	db		*DB
	scheRpcCh	chan RPC

	partitionPeerLock      	sync.Mutex //互斥锁
	partitionPeers	map[Server] struct{}
	chooser		*HashChooser    //partition  chooser
	conf            *Config
	raft 		*raft.Raft
	txnMgr		*TxnMgr     //txn mananger

        clusterID    int	//集群信息
        partitionID  int       //本节点的 partition id
        partitionNum   int

	configureFile	string
}



// New returns an uninitialized   service.  构造函数
func NewService(httpAddr string,respAddr string,tranAddr string , store *FsmStore,
		partID int,partNum int,clusterID int,conffile string) *Service {
	Register()
	service:= &Service{
		httpAddr:  	httpAddr,
		respAddr: 	respAddr,
		tranportAddr:	tranAddr,
		store: 		store,
		respClients:	make(map[*RespClient]struct{}),
		partitionPeers: make(map[Server] struct{}),
		conf:		DefaultConfig(),
		raft:		store.Raft,
		db:		NewDB(),
		partitionID:partID,
		partitionNum: partNum,
		clusterID:clusterID,
		chooser:  NewHashChooser(partNum),
		configureFile: conffile,
	}
	service.txnMgr= NewTxnMgr(service.chooser)
	service.db.Chooser = service.chooser
	service.db.partitionID = service.partitionID

	return service
}

func   (s * Service)deleteReadTxn(id int){
	s.txnMgr.DeleteReadTxn(id)
}
//只在sequencer writer 中添加read txn
func  (s * Service)AddReadTxn(txn *Txn){
	s.txnMgr.AddReadTxn(txn)
}


func   (s * Service)SetPreprocessor( pre *PreProcessor){
	s.preProcessor= pre
}


func (s * Service) addRespClient(client *RespClient)  {
	s.respLock.Lock()
	s.respClients[client]=struct {}{}
	s.respLock.Unlock()
}

/*todo(shili)  partition   temp function*/
func   (s * Service) addPartitionPeer(peer  Server ){
	s.partitionPeerLock.Lock()
	s.partitionPeers[peer]=struct {}{}
	s.partitionPeerLock.Unlock()
}

func (s *Service) PushTxnTask(txn  *Txn)  {
	s.preProcessor.Push(txn)
}



func (s * Service) delRespClient(client *RespClient)  {
	s.respLock.Lock()
	delete(s.respClients,client)
	s.respLock.Unlock()
}

//start  http  and RESP service
func (s * Service) StartService() error{
	err:= s.HttpServiceStart()
	if err!= nil{
		return err
	}
	logger.Infof("start http service")

	go func(){
		err = s.RESPServiceStart()
		if err!= nil{
			logger.Warnf("start resp service  fail")
		}
	}()


	err = s.TransportServiceStart()
	if err!= nil{
		logger.Warnf("start transport  service  fail,error:%v",err)
		return err
	}

	err = s.SystemServiceStart()
	if err!= nil{
		logger.Warnf("start system service  fail,error:%v",err)
		return err
	}

	go s.serviceLoop()
	return  nil
}


// Start start the  RESP service.
func (s *Service) RESPServiceStart() error {

	ln, err := net.Listen("tcp", s.respAddr)
	if err != nil {
		logger.Warnf("lisent  respAddr fail,respAddr=%s,err=%s",s.respAddr,err)
		return err
	}

	s.respListener = ln
	logger.Infof("start resp service")

	for {
		c, err := s.respListener.Accept()
		if err != nil {
			logger.Warnf("accept error:%s", err)
			break
		}
		// start a new goroutine to handle
		// the new connection.
		NewRespClient(c,s)
	}

	return nil
}

// Start starts the http service.
func (s *Service) HttpServiceStart() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		logger.Warnf("lisent  http fail,httpAddr=%s,err=%s",s.respAddr,err)
		return err
	}
	s.httpListener = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.httpListener)
		if err != nil {
			logger.Warn("HTTP serve: %s", err)
		}
	}()

	return nil
}



func (s *Service)TransportServiceStart() error{
	logger.Infof("TransportService Start,service addr %s",s.tranportAddr)
	trans, err := NewTCPTransport(s.tranportAddr, nil, s.conf.MaxConnPoolNum,s.conf.ConnectTimeout, nil)
	if err != nil {
		logger.Warnf("new tcp Transport fail,err:%v",err)
		return err
	}
	s.transport = trans
	s.rpcCh = trans.Consumer()
	conf := goini.SetConfig(s.configureFile)
	node_list := conf.ReadList()
	for _,node := range node_list{
		var server Server
		for  _,v := range node{
			server.Addr = v["addr"]
			server.clusterID,_ = strconv.Atoi(v["clusterid"])
			server.partitionID,_ = strconv.Atoi(v["partitionid"])
		}
		logger.Infof("load one server %s", ToString(server))
		s.addPartitionPeer(server)
	}
	return nil
}

/*@berif 启动系统各种线程 */
func (s *Service) SystemServiceStart() error{
	tmpPreProcessor:=NewPreProcessor() 			//启动  preProcessor协程
	s.SetPreprocessor(tmpPreProcessor)

	tmpSequencerWriter := NewSequencerWriter(s.conf) 	//启动  sequencerWriter
	s.sequencerWriter  =  tmpSequencerWriter
	tmpSequencerWriter.setRaft(s.raft)


	tmpSequencerReader := NewSequencerReader(s.conf)		//启动  sequencerReader
	s.sequencerReader = tmpSequencerReader
	tmpSequencerReader.Trans = s.transport
	tmpSequencerReader.PartitionPeers = s.partitionPeers
	tmpSequencerReader.LocalPeer = s.tranportAddr
	tmpSequencerReader.Chooser = s.chooser
	tmpSequencerReader.ClusterID = s.clusterID

	tmpPreProcessor.setSequencerCh(tmpSequencerWriter.ApplyCh)  //设置传输的通道
	s.store.BatchSyncCh = tmpSequencerReader.ApplyCh

	tmpScheduler := NewScheduler(s.conf,s.partitionID,s.chooser)  //开启scheduler
	tmpScheduler.DataBase = s.db
	tmpScheduler.Trans = s.transport
	tmpScheduler.PartitionPeers = s.partitionPeers
	tmpScheduler.setService(s)

	s.scheduler=tmpScheduler
	s.scheRpcCh=tmpScheduler.GetRpcCh()
	return nil
}


func (s *Service)serviceLoop(){
	for {
		select {
		case rpc := <-s.rpcCh:
			logger.Infof("serviceLoop,get one rpc")
			s.processRPC(rpc)
		}
	}
}


// processRPC is called to handle an incoming RPC request.
// 各个节点 处理rpc请求
func (s *Service) processRPC(rpc RPC) {
	switch  rpc.Command.(type) {
	case *TransBatchTxnRequest: 	// 从sequencer reader 拷贝数据到 scheduler
		s.transBatchTxn(rpc)
	case *	RetReadResultRequest:
		s.handleRetResultReq(rpc)
	default:
		logger.Infof("Got unexpected command: %#v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

/*向 scheduler  通道发送一个 rpc*/
func (s *Service) transBatchTxn(rpc RPC) {
	s.scheRpcCh<-rpc
}

func (s *Service)handleRetResultReq(rpc RPC){
	if s.raft.State() == raft.Leader { //只有是leader 才处理该数据
		var ret error
		logger.Infof("handle  handleRetResultReq")
		req := rpc.Command.(*RetReadResultRequest)
		resp := &RetReadResultResponse{
			Ok: true,
		}
		var rpcErr error
		defer func() {
			rpc.Respond(resp, rpcErr) //response
		}()
		readKey := req.ReadKey
		readResult:= req.ReadResult
		logger.Infof("req.TxnId:%d  ,runningReadTxn:%d",req.TxnId,s.txnMgr.RunningReadTxnLen())

		txn,exist:= s.txnMgr.GetReadTxn(req.TxnId)

		if !exist{
			ret = ErrNotFindKey
			logger.Infof("can't find target txn:%p,ret=%v",txn,ret)
		} else {
			s.txnMgr.DecReadRef(req.TxnId,1)
			if  ref,_:=s.txnMgr.GetReadTxnRef(req.TxnId);ref > 0{
				logger.Infof("txn don't receive enought response",ToString(txn))
				s.txnMgr.CacheReadResult(req.TxnId,readKey,readResult)
			}else{
				s.txnMgr.CacheReadResult(req.TxnId,readKey,readResult)
				cache,exist:=s.txnMgr.GetCacheReadResult(req.TxnId)
				if !exist{
					ret = ErrNotFindKey
					logger.Infof("txn:%s cache not exist,ret=%v",ToString(txn),ret)
				}else{
					result,ok:=txn.SortReadResult(cache)
					if ok!=nil {
						ret = ok
						logger.Infof("sort result fail,ret=%v",ToString(txn),ret)
						txn.GetClient().WriteError(ok)
					}else{
						txn.GetClient().WriteStringArray(result)  //respone to client
						txn.GetClient().Flush()
						s.deleteReadTxn(req.TxnId)
					}
				}
			}
		}
		if( nil!=ret ){
			txn.GetClient().WriteError(ret)
			txn.GetClient().Flush()
		}
	}else { //follower
		//do nothing 不需要删除 read txn等，因为follower 不会添加
	}
}


// Close closes the service.
func (s *Service) Close() {
	s.httpListener.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	switch r.Method {
	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		v, err := s.store.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, err := json.Marshal(map[string]string{k: v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		io.WriteString(w, string(b))

	case "POST":
		// Read the value from the POST body.
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.store.Set(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	case "DEL":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.store.Delete(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.store.Delete(k)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.httpListener.Addr()
}
