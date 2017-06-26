package Orangedb

import (
	"github.com/laohanlinux/go-logger/logger"
	"time"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/go-msgpack/codec"
	"fmt"
	"bytes"
	"encoding/gob"
)


type BatchTxn struct {
	BatchNumber	int
	Txns		[]*Txn
}

func (this *BatchTxn)  ToString() string{
	result:=""
	result+=fmt.Sprintf("BatchTxnNum:%d  [ ",this.BatchNumber)
	for i,v:= range this.Txns{
		tmpStr:=fmt.Sprintf("<%d, %s> ,",i,v.ToString())
		result+=tmpStr
	}

	result +=" ]"
	return result
}


type SequencerWriter struct {
	batchNumber int
	txnNumber   int
	ApplyCh     chan *Txn

	conf        *Config
	Raft        *raft.Raft // The consensus mechanism
}

func NewSequencerWriter(config *Config) *SequencerWriter {
	logger.Infof("start sequencer Writer")
	q:= &SequencerWriter{
		batchNumber:		0,
		txnNumber:		0,
		ApplyCh:		make(chan *Txn),
		conf:			config,
	}

	go  q.run()
	return q
}

func (s *SequencerWriter) setRaft(r *raft.Raft){
	s.Raft=r
}


func (s *SequencerWriter) MsgpackEncode(ts interface{}) (bs []byte, err error) {
	err = codec.NewEncoderBytes(&bs, &codec.MsgpackHandle{}).Encode(ts)
	return
}

func (s *SequencerWriter) MsgpackDecode(buf []byte, ts interface{}) error {
	return codec.NewDecoderBytes(buf, &codec.MsgpackHandle{}).Decode(ts)
}


func (s *SequencerWriter) GobEncode(ts interface{}) (bs []byte, err error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(ts)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func (s *SequencerWriter) GobDecode(data []byte, ts interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(ts)
}

func (s *SequencerWriter)testGob(batch *BatchTxn){
	logger.Infof("before encode testBatch,%s",ToString(batch))
	bs,err:= s.GobEncode(batch)
	if nil!=err{
		logger.Warnf("encode  batchTxn fail,err=%v",err)
		return
	}


	var testBatchTxn BatchTxn
	s.GobDecode(bs,&testBatchTxn)
	logger.Infof("after decode testBatch,%s",ToString(&testBatchTxn))

}


//copy TxnBatch to all follwers
func (s *SequencerWriter) SubmitBatch(batch *BatchTxn) error{
	if s.Raft.State() != raft.Leader {
		logger.Infof("not leader")
		return ErrNotLeader
	}
	logger.Infof("submit   one batch ")
	batchCmd:=&batchSubCommand{
		BatchTxn:	batch,
	}
	bs,err:=s.MsgpackEncode(batchCmd);
	if nil!=err{
		logger.Warnf("encode  batchTxn fail,err=%v",err)
		return err
	}

	c := &Command{
		Type:  BatchSubCommand,
		Data:  bs,
	}

	b,err:=s.MsgpackEncode(c);
	if nil!=err{
		logger.Warnf("encode  batchTxn fail,err=%v",err)
		return err
	}
	logger.Infof("sequencer writer deliver one batch,%s",ToString(batch))
	f := s.Raft.Apply(b, raftTimeout)
	return f.Error()
}

/*SequencerWriter 处理函数*/
func (this *SequencerWriter) run() {
	logger.Infof("start sequencer writer")

	for {
		if this.Raft.State() == raft.Leader {
			logger.Infof("this node is leader ")
			select {
			case newTxn := <-this.ApplyCh:
				logger.Infof("get one  txn,%s",ToString(newTxn))
				newTxn.TxnId = this.txnNumber
				if newTxn.HasRead(){
					newTxn.GetClient().Service.AddReadTxn(newTxn)
				}
				this.txnNumber++
				var batchTxn BatchTxn
				batchTxn.BatchNumber = this.batchNumber
				batch := []*Txn{newTxn}
				epochCount := 0
				stop := false
				//收集一批  txn
				epoch := time.After(this.conf.EpochDuration)
				for !stop && epochCount <= this.conf.MaxAppendEntries {
					select {
					case newTxn := <-this.ApplyCh:
						newTxn.TxnId = this.txnNumber
						if newTxn.HasRead(){
							newTxn.GetClient().Service.AddReadTxn(newTxn)
						}
						this.txnNumber++
						epochCount++;
						batch = append(batch, newTxn)
					case <-epoch:
					//时间超时
						stop = true
					}
				}
				batchTxn.Txns = batch
				this.batchNumber++;
				logger.Infof("get one batch success,batchNumber:%d", batchTxn.BatchNumber)

				if err:=this.SubmitBatch(&batchTxn);nil!=err{  //复制一批数据到副本中
					logger.Warnf("sync batch txn fail,batchNumber:%d,error:%v", batchTxn.BatchNumber,err)
				}else{
					logger.Infof("sync batch txn Success,batchNumber:%d", batchTxn.BatchNumber)
				}
			}//end select
		}else{
			time.Sleep(1*time.Second)
		}
	}
}







type SequencerReader struct {
	ApplyCh     chan *BatchTxn
	conf        *Config
	Trans	    *Transport
	LocalPeer	string
	PartitionPeers	map[Server] struct{}
	Chooser     *HashChooser
	ClusterID    int
}

func NewSequencerReader(config  *Config) *SequencerReader {
	logger.Infof("start sequencer reader")
	q:= &SequencerReader{
		ApplyCh:	make(chan *BatchTxn),
		conf:		config,
	}

	go  q.run()
	return q
}

//find the server  using partition  id
func  (s *SequencerReader)  findDescPeer(partid  int,clusterid int) (string,error){
	for  peer,_ := range s.PartitionPeers{
		if peer.partitionID == partid &&peer.clusterID == clusterid{
			return peer.Addr,nil
		}
	}
	return "",ErrNotFindPeer
}


/*SequencerReader 处理函数*/
func (this *SequencerReader) run() {
	logger.Infof("start sequencer reader")

	for {
		select {
		case newBatchTxn := <-this.ApplyCh:

			logger.Infof("sequencer reader get one  BatchTxn:%s", ToString(newBatchTxn))
			batchTxnMap := make(map[int]*BatchTxn)

			for   _,ctx := range newBatchTxn.Txns{//遍历batch 中每一个txn
				partitionList := make(map[int]struct{})
				for key,_ := range ctx.ReadSet{
					partitionID := this.Chooser.Choose(key)
					ctx.Readers[partitionID] = struct {}{}
					partitionList[partitionID] = struct {}{}
				}
				for key,_ := range ctx.WriteSet{
					partitionID := this.Chooser.Choose(key)
					ctx.Writers[partitionID] = struct {}{}
					partitionList[partitionID] = struct {}{}
				}
				logger.Infof("partition get one txn:%s",ToString(ctx))

				for partID,_:= range partitionList{ //将txn 添加到 相关的分区 batch
					if v, ok := batchTxnMap[partID]; ok {
						v.Txns = append(v.Txns,ctx)
					} else {
						batchTxnMap[partID] = new(BatchTxn)
						batchTxnMap[partID].Txns = append(batchTxnMap[partID].Txns,ctx)
					}
				}
			}

			for k, v := range batchTxnMap {
				logger.Infof("partition:%d  clusterID:%d batch:%s", k,this.ClusterID,ToString(v))
				req := TransBatchTxnRequest{
					SourceAddr:this.LocalPeer,
					TransBatch:v,
				}
				var resp TransBatchTxnResponse
				desAddr,ok:= this.findDescPeer(k,this.ClusterID)
				if nil!= ok{
					logger.Warnf("find desc peer fail,err=[%v]",ok)
					return
				}

				logger.Infof("tcp trans batch to scheduler,peer=%s",desAddr)
				if err := this.Trans.TransBatchTxn(desAddr, &req, &resp); nil != err {
					logger.Infof("TransBatchTxn fail,error:%v", err)
				}
			}
		}//end select
	}
}
