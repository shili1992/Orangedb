package Orangedb


import (
	"github.com/laohanlinux/go-logger/logger"
)

type CmdType uint8
const (
	// LogCommand is applied to a user FSM.
	ApplyCommand CmdType = iota
	BatchSubCommand
)

type applyCommand struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type batchSubCommand struct {
	BatchTxn *BatchTxn
}


type Command struct {
	// Type holds the type of the log entry.
	Type CmdType

	// Data holds the log entry's type-specific data.
	Data []byte
}


type CommandFunc func(c *RespClient) error

var txnPushFuncMap = map[string]CommandFunc{}


func DoGet(c *RespClient) error {
	if len(c.Args) < 1 {
		return ErrInvalidArgument
	}
	txn := NewTxn(c)
	txn.AddReadSet(string(c.Args[0]))
	logger.Debugf("get  a key [%s]", string(c.Args[0]))
	c.Service.PushTxnTask(txn)
	return nil
}

func DoSet(c *RespClient) error {
	if len(c.Args) < 1 {
		return ErrInvalidArgument
	}
	txn := NewTxn(c)
	txn.AddWriteSet(string(c.Args[0]))
	logger.Debugf("set  a key", string(c.Args[0]))
	c.Service.PushTxnTask(txn)

	c.respWriter.writeStatus(ResponseOK)
	c.respWriter.flush()
	return nil
}


func DoDel(c *RespClient) error {
	if len(c.Args) < 1 {
		return ErrInvalidArgument
	}
	txn := NewTxn(c)
	txn.AddWriteSet(string(c.Args[0]))

	logger.Debugf("delete  a key", string(c.Args[0]))
	c.Service.PushTxnTask(txn)

	c.respWriter.writeStatus(ResponseOK)
	c.respWriter.flush()
	return nil
}


func DoMget(c *RespClient) error{
	if len(c.Args) < 1 {
		return ErrInvalidArgument
	}
	txn := NewTxn(c)
	for _,arg := range c.Args{
		txn.AddReadSet(string(arg))

	}
	logger.Debugf("get  keys array [%v]",txn.ReadSet)
	c.Service.PushTxnTask(txn)
	return nil
}

func DoMset(c *RespClient) error{
	if len(c.Args) < 2 {
		return ErrInvalidArgument
	}
	txn := NewTxn(c)
	for index:=0; index <len(c.Args);index+=2{
		txn.AddWriteSet(string(c.Args[index]))
	}
	logger.Debugf("get  set key array [%v]",txn.WriteSet)
	c.Service.PushTxnTask(txn)
	c.respWriter.writeStatus(ResponseOK)
	c.respWriter.flush()
	return nil
}

/*@berif 注册各种回调函数*/
func Register() {
	txnPushFuncMap["get"] = DoGet
	txnPushFuncMap["set"] = DoSet
	txnPushFuncMap["del"] = DoDel
	txnPushFuncMap["mget"] = DoMget
	txnPushFuncMap["mset"] = DoMset
}









type TransBatchTxnRequest struct {
	SourceAddr	string
	TransBatch 	*BatchTxn
}


type TransBatchTxnResponse struct {
	Success 	bool
}


type  RetReadResultRequest struct {
	TxnId		int
	ReadKey		[] string
	ReadResult	[] string  //和 readkey 一一对应
}

type  RetReadResultResponse struct {
	Ok             bool
}

