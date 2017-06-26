package Orangedb

import (
	"fmt"
	"github.com/laohanlinux/go-logger/logger"
)


var(
	InitCmdQueueLen  int = 16
)

const(
	AlreadyInTxnState = iota
	NormalState
)

type  RespCommand struct{
	Cmd  string
	Args [][]byte
}


type TxnMgr struct {
	runningReadTxn map[int] *Txn //读事务
	readTxnRef     map[int] int  //读事务 还未接受到的分片数量,和runningReadTxn 一一对应
	readResult      map[int] map[string]string  //缓存接受到分片的结果
	chooser        *HashChooser  //partition  chooser

}

func  NewTxnMgr(chooser *HashChooser) *TxnMgr{
	txnmgr:= &TxnMgr{
		runningReadTxn: make(map[int] *Txn),
		readTxnRef: make(map[int] int),
		readResult:make(map[int] map[string]string ),
		chooser: chooser,
	}
	return txnmgr
}

//@berif 删除正在运行中的 read_txn
func   (t * TxnMgr)DeleteReadTxn(id int){
	delete(t.runningReadTxn,id)
	delete(t.readTxnRef,id)
	delete(t.readResult,id)
}

func (t * TxnMgr)DecReadRef(txnid int,num int){
	t.readTxnRef[txnid] = t.readTxnRef[txnid]- num
}

//cache all response  read result
func (t * TxnMgr)CacheReadResult(txnid int,keyList []string,valueList []string){
	result,exist :=t.readResult[txnid]
	if !exist{
		result = make(map[string]string)
		t.readResult[txnid] = result
	}
	for index,key := range keyList{
		result[key] = valueList[index]
	}
}

//
func (t * TxnMgr)GetCacheReadResult(txnid int)(cache map[string]string, exist bool) {
	cache,exist = t.readResult[txnid]
	return
}

//save running  read txn
func  (t * TxnMgr)AddReadTxn(txn *Txn){
	partitionList:= make(map[int]struct{})
	for key,_ := range txn.ReadSet{
		partitionID := t.chooser.Choose(key)
		partitionList[partitionID] = struct {}{}
	}
	t.runningReadTxn[txn.TxnId]= txn
	t.readTxnRef[txn.TxnId] = len(partitionList)
}

//running read  txn  number
func (t * TxnMgr)RunningReadTxnLen()int {
	return len(t.runningReadTxn)
}


//running read  txn  number
func (t * TxnMgr)GetReadTxnRef(txnid int) (ref int, exist bool) {
	ref, exist = t.readTxnRef[txnid]
	return
}

//running read  txn  number
func (t * TxnMgr)GetReadTxn(txnid int)(txn *Txn,exist bool) {
	txn, exist = t.runningReadTxn[txnid]
	return
}



type Txn struct {
	TxnId           int
	client          *RespClient
	ReadSet         map[string]struct{}
	WriteSet        map[string]struct{}

	Cmds            []*RespCommand
	readResult      []string //read result Readkey
	readKey         []string

	isolation_level int
	Readers         map[int]struct{}
	Writers         map[int]struct{}
	sourceAddr      string
}

func  (t *Txn)AppendReadResult(key string ,value string){
	t.readKey = append(t.readKey,key)
	t.readResult = append(t.readResult,value)
}

func  (t *Txn)GetReadResult()( []string , []string) {
	return t.readKey,t.readResult
}


func  (t *Txn)SortReadResult(cache map[string]string) (result []string,ret error){
	for _,c := range t.Cmds {
		switch c.Cmd {
		case "get":
			key := string(c.Args[0])
			value,exist:= cache[key]
			if !exist{
				logger.Warnf("key:%s not exist",key)
				ret = ErrNotFindKey
				return
			}else{
				result =append(result,value)
			}
		case "mget":
			for _, arg := range c.Args {
				key := string(arg)
				value,exist:= cache[key]
				if !exist{
					logger.Warnf("key:%s not exist",key)
					return result,ErrNotFindKey
				}else{
					result =append(result,value)
				}
			}
		}
	}
	ret = nil
	return
}


func  (this *Txn)HasRead() bool{
	return len(this.ReadSet)>0
}

func (this *Txn) GetClient()*RespClient{
	return this.client
}

func (this *Txn) SetSourceAddr(args string){
	this.sourceAddr = args
}

func (this *Txn) GetSourceAddr() (args string){
	args = this.sourceAddr
	return
}


func (this *Txn) AddReadSet(args string){
	this.ReadSet[args]= struct {}{}
}

func (this *Txn) AddWriteSet(args string){
	this.WriteSet[args]= struct {}{}
}


func (this * Txn)ToString() string{
	result:=fmt.Sprintf("Txn id:%d>",this.TxnId)
	result+=",readSet ["
	if len(this.ReadSet) >0{
		for i,_ := range this.ReadSet{
			tmpStr:=fmt.Sprintf("<%s> ,",i)
			result+=tmpStr
		}
		result+="]"
	}else{
		result+="]"
	}


	result+=",writerSet ["
	if len(this.WriteSet) >0{
		for i,_ := range this.WriteSet{
			tmpStr:=fmt.Sprintf("< %s> ,",i)
			result+=tmpStr
		}
		result+="]"
	}else{
		result+="]"
	}

	result+=",readers["
	if len(this.Readers) >0{
		for i,_ := range this.Readers{
			tmpStr:=fmt.Sprintf("< %d> ,",i)
			result+=tmpStr
		}
		result+="]"
	}else{
		result+="]"
	}


	result+=",writers["
	if len(this.Writers) >0{
		for i,_ := range this.Writers{
			tmpStr:=fmt.Sprintf("< %d> ,",i)
			result+=tmpStr
		}
		result+="]"
	}else{
		result+="]"
	}
	return result
}


func (txn *Txn) addCmd(cmd string,args [][]byte )error{
	return nil
}


/*@berif 使用 c 来初始化  txn*/
func NewTxn(c *RespClient)  *Txn{
	txn:= &Txn{client:c,
		ReadSet: make(map[string] struct{}),
		WriteSet: make(map[string] struct{}),
		Readers: make(map[int] struct{}),
		Writers: make(map[int] struct{}),
		}
	cmd:=&RespCommand{Args:c.Args, Cmd:c.Cmd}
	txn.Cmds = append(txn.Cmds,cmd)
	return txn
}
