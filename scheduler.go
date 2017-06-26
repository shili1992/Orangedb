package Orangedb

import (
	"github.com/laohanlinux/go-logger/logger"
	"fmt"
	"github.com/hashicorp/raft"
)

type Scheduler struct {
	readyCh        chan struct{} //已经有 txn 获得了 锁，通知 docheck 线程来取
	releaseCh      chan *Txn     //用于传递 处理完毕的txn
	rpcCh          chan RPC      //用于接收 远程传送的BatchTxn

	Trans          *Transport
	readyTxnQueue  *Queue        //已经获得所有的需要的锁， 就绪执行的txn
	conf           *Config
	lockManager    *LockManager  //锁管理器
	workqueue      *QueueThread  //workers 执行的队列
	DataBase       *DB           // database

	PartitionPeers map[Server] struct{}  //集群中的节点
	Chooser        *HashChooser   //分区方法
	partitionID    int

	service 	*Service
}

func NewScheduler(config  *Config,partid int,chooser *HashChooser) *Scheduler {
	logger.Infof("start scheduler ")
	q:= &Scheduler{
		readyCh: make(chan struct{}),
		releaseCh: make(chan  *Txn,8),
		rpcCh:	make(chan RPC),
		readyTxnQueue: NewQueue(),
		conf:		config,
		partitionID: partid,
	}
	q.workqueue=NewQueueThread(q.WorkerHandler,config.MaxSchedulerWorkerNum)
	q.lockManager = NewLockManager()
	q.lockManager.ReadyTxnQueue = q.readyTxnQueue
	q.lockManager.ReadyCh = q.readyCh
	q.lockManager.LocalPartitionID = partid
	q.lockManager.Chooser = chooser

	go q.LockManagerRun()
	go q.DoCheckRun()
	return q
}

func (s *Scheduler)GetRpcCh() chan RPC{
	return s.rpcCh
}

func (s *Scheduler)setService(ser *Service) {
	s.service = ser
}

func (s *Scheduler) processRPC(rpc RPC) {
	switch cmd := rpc.Command.(type) {
	case *TransBatchTxnRequest: 	// 日志复制 请求
		s.LockBatchTxns(rpc, cmd)

	default:
		logger.Error("Got unexpected command: %v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}


/*@berif  lockmanager lock all txns in  batch*/
func (s *Scheduler) LockBatchTxns(rpc RPC, t *TransBatchTxnRequest) {
	logger.Infof("scheduler get one batchTxn:%s", ToString(t.TransBatch))
	resp := &TransBatchTxnResponse{
		Success: true,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr) //回复 操作信息
	}()
	batch:=t.TransBatch
	sourceAddr := t.SourceAddr
	for _,txn:= range batch.Txns{
		txn.SetSourceAddr(sourceAddr)
		s.lockManager.Lock(txn)
	}
}

//检查是否 已经获得所有需要的锁的txn
func (s *Scheduler)DoCheckRun(){
	logger.Infof("start Scheduler  do  check  thread")
	for {
		select {
		case  <-s.readyCh:
			for !s.readyTxnQueue.Empty(){
				txn:=s.readyTxnQueue.Pop()
				s.workqueue.Push(txn)   //push txn to work thread
				logger.Infof("CheckRun,one txn is alreaddy:%s",ToString(txn.(*Txn)))
			}
		}
	}
}


/*工作处理线程*/
func  (s *Scheduler) WorkerHandler(task interface{})  {
	txn:= task.(*Txn)
	logger.Infof("WorkerHandler,get one txn,%s",ToString(txn))
	s.DataBase.Execute(txn)
	s.releaseCh<- txn    //notify  lockmanger thread release  lock of txn
}

/*@berif lockmanager  运行线程*/
func  (s *Scheduler) LockManagerRun() {
	logger.Infof("start Scheduler  Lock manager thread")
	for {
		select {
		case newRpc := <-s.rpcCh:
			s.processRPC(newRpc)
		case  txn := <-s.releaseCh:   //释放 已经执行完毕的锁
			s.lockManager.UnLock(txn)
			if len(txn.ReadSet) >0  && s.service.raft.State()== raft.Leader{//只有leader返回读取的数据
				s.SendReadResult(txn)
			}
		}
	}
}


//return all read result after unlock the lock of txn
func (s *Scheduler) SendReadResult(txn *Txn) {
	if len(txn.ReadSet) >0 {
		logger.Infof("process one  read txn :%s", ToString(txn))
		readKey,readResult := txn.GetReadResult()
		req := RetReadResultRequest{
			TxnId:	txn.TxnId,
			ReadKey:readKey,
			ReadResult:readResult,
		}
		var resp RetReadResultResponse

		logger.Infof("return read result,tcp trans batch to scheduler,peer=%s",txn.GetSourceAddr())
		if err:=s.Trans.RetReadResult(txn.GetSourceAddr(),&req,&resp);nil!=err{
			logger.Infof("TransBatchTxn fail,error:%v",err)
		}
	}
}

