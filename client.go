package Orangedb

import (
	"bufio"
	"github.com/laohanlinux/go-logger/logger"
	"github.com/siddontang/go/hack"
	"github.com/siddontang/go/num"
	"github.com/siddontang/goredis"
	"net"
	"runtime"
	"strconv"
	"strings"
	"fmt"
)


type KVPair struct {
	Field []byte
	Value []byte
}

var (
	Delims = []byte("\r\n")

	NullBulk = []byte("-1")
	NullArray = []byte("-1")
)

const (
	ReaderBufferSize = 4096
	WriterBufferSize = 4096
)

type Client struct {
	Service    *Service
	remoteAddr string
}

func (c *Client) close() {

}

//RespClient 继承于  Client
type RespClient struct {
	*Client
	conn       net.Conn
	respReader *goredis.RespReader //用于读取 客户端的内容
	respWriter *RespWriter
	txn        *Txn
	txnState   int
	Cmd        string
	Args       [][]byte
}


func (c *RespClient) DoRequest() {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			logger.Warnf("client run panic %s:%v", buf, e)
		}

		c.Client.close()
		c.conn.Close()
		c.Service.delRespClient(c)
	}()
	for {
		c.Cmd = ""
		c.Args = nil

		reqData, err := c.respReader.ParseRequest() //获取请求
		if err == nil {
			err = c.handleRequest(reqData) //处理请求
		}

		if err != nil {
			logger.Warnf("handle request fail %s", err)
			return
		}
	}
}

func (c *RespClient) handleRequest(reqData [][]byte) error {
	if len(reqData) == 0 {
		c.Cmd = ""
		c.Args = reqData[0:0]
	} else {
		c.Cmd = strings.ToLower(hack.String(reqData[0])) //命令
		c.Args = reqData[1:]                             //参数
	}

	argsLen := len(c.Args)
	argStrs := make([]string, argsLen)
	for i := 0; i < argsLen; i++ {
		argStrs[i] = string(c.Args[i])
	}
	if c.isSystemCmd(c.Cmd) {
		if err := c.handleSysCommand(); err != nil {
			c.respWriter.writeError(err) //回复错误信息
			c.respWriter.flush()
			return err
		}
	} else {
		if err := c.handleCommand(); err != nil {
			c.respWriter.writeError(err) //回复错误信息
			c.respWriter.flush()
			return err
		}
	}
	return nil
}


func (c *RespClient)WriteKvArray(array map[string]string){
	c.respWriter.writeKVarray(array);
}

func (c *RespClient)WriteStringArray(array []string){
	c.respWriter.writeStringArray(array);
}

func (c *RespClient)WriteError(err error){
	c.respWriter.writeError(err) //回复错误信息
}


func (c *RespClient)Flush(){
	c.respWriter.flush()
}


func (c *RespClient) isSystemCmd(cmd string) bool {
	return (cmd == "xselect" || cmd == "select" || cmd == "quit" || cmd == "ping"|| cmd == "shutdown")
}

func (c *RespClient) isTransCmd(cmd string) bool {
	return (cmd == "exec" || cmd == "discard" || cmd == "multi")
}

func (c *RespClient) handleSysCommand() error {

	if c.Cmd == "quit" {
	} else if c.Cmd == "shutdown" {
	}
	c.respWriter.writeStatus("OK")
	c.respWriter.flush()
	return nil
}


/*@berif 处理  multi,discard,exec 命令*/
func (c *RespClient) handleTransCommand(in_trans bool) error {
	switch c.Cmd {
	case "multi":
		if in_trans {
			return ErrAlreadyInTransaction
		} else {
			//c.txn = NewTxn(c)
			//c.txnState = AlreadyInTxnState
		}
	case "discard":
		if !in_trans {
			return ErrNotInTransaction
		} else {
			//c.txnState = NormalState
		}
	case "exec":
		if !in_trans {
			return ErrNotInTransaction
		} else {

		}
	}
	return nil
}

/*@berif  处理  del  set  get 等 命令*/
func (c *RespClient) handleNormalCommand() error {
	if cmdfunc, ok := txnPushFuncMap[c.Cmd]; !ok {
		return ErrNotFound
	} else if err := cmdfunc(c); err != nil {
		return err
	}
	return nil
}

func (c *RespClient) handleCommandv2() error {
	if cmdfunc, ok := txnPushFuncMap[c.Cmd]; !ok {
		return ErrNotFound
	} else if err := cmdfunc(c); err != nil {
		return err
	}
	return nil
}

func (c *RespClient) handleCommand() error {
	if err := c.handleNormalCommand(); err != nil {
		return err
	}
	return nil
}





type RespWriter struct {
	buffer *bufio.Writer
}

func (w *RespWriter) writeError(err error) {
	w.buffer.Write(hack.Slice("-"))
	if err != nil {
		w.buffer.WriteByte(' ')
		w.buffer.Write(hack.Slice(err.Error()))
	}
	w.buffer.Write(Delims)
}

func (w *RespWriter)writeStringArray(array []string){
	var result  [][]byte
	for _,v:= range array{
		value:=  hack.Slice(v)
		result = append(result,value)
	}
	w.writeSliceArray(result)
}

func (w *RespWriter)writeKVarray(array map[string]string){
	var valueArray []KVPair
	for k,v:= range array{
		var value  KVPair
		tmpkey:= hack.Slice(k)
		tmpValue:=  hack.Slice(v)
		value.Field = tmpkey
		value.Value = tmpValue
		valueArray = append(valueArray,value)
	}
	w.writeKVPairArray(valueArray)
}


func (w *RespWriter) writeStatus(status string) {
	w.buffer.WriteByte('+')
	w.buffer.Write(hack.Slice(status))
	w.buffer.Write(Delims)
}

func (w *RespWriter) writeInteger(n int64) {
	w.buffer.WriteByte(':')
	w.buffer.Write(num.FormatInt64ToSlice(n))
	w.buffer.Write(Delims)
}

func (w *RespWriter) writeBulk(b []byte) {
	w.buffer.WriteByte('$')
	if b == nil {
		w.buffer.Write(NullBulk)
	} else {
		w.buffer.Write(hack.Slice(strconv.Itoa(len(b))))
		w.buffer.Write(Delims)
		w.buffer.Write(b)
	}

	w.buffer.Write(Delims)
}

func (w *RespWriter) writeKVPairArray(lst [] KVPair) {
	w.buffer.WriteByte('*')
	if lst == nil {
		w.buffer.Write(NullArray)
		w.buffer.Write(Delims)
	} else {
		w.buffer.Write(hack.Slice(strconv.Itoa(len(lst) * 2)))
		w.buffer.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.writeBulk(lst[i].Field)
			w.writeBulk(lst[i].Value)
		}
	}
}


func (w *RespWriter) writeArray(lst []interface{}) {
	w.buffer.WriteByte('*')
	if lst == nil {
		w.buffer.Write(NullArray)
		w.buffer.Write(Delims)
	} else {
		w.buffer.Write(hack.Slice(strconv.Itoa(len(lst))))
		w.buffer.Write(Delims)

		for i := 0; i < len(lst); i++ {
			switch v := lst[i].(type) {
			case []interface{}:
				w.writeArray(v)
			case [][]byte:
				w.writeSliceArray(v)
			case []byte:
				w.writeBulk(v)
			case nil:
				w.writeBulk(nil)
			case int64:
				w.writeInteger(v)
			case string:
				w.writeStatus(v)
			case error:
				w.writeError(v)
			default:
				panic(fmt.Sprintf("invalid array type %T %v", lst[i], v))
			}
		}
	}
}

func (w *RespWriter) writeSliceArray(lst [][]byte) {
	w.buffer.WriteByte('*')
	if lst == nil {
		w.buffer.Write(NullArray)
		w.buffer.Write(Delims)
	} else {
		w.buffer.Write(hack.Slice(strconv.Itoa(len(lst))))
		w.buffer.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.writeBulk(lst[i])
		}
	}
}

func (w *RespWriter) flush() {
	w.buffer.Flush()
}

func newClient(s *Service) *Client {
	c := new(Client)
	c.Service = s
	return c
}

func NewRespWriter(conn net.Conn, size int) *RespWriter {
	writer := new(RespWriter)
	writer.buffer = bufio.NewWriterSize(conn, size)
	return writer
}

func NewRespClient(conn net.Conn, s *Service) {
	c := new(RespClient)
	c.Client = newClient(s)
	c.conn = conn

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetReadBuffer(ReaderBufferSize)
		tcpConn.SetWriteBuffer(WriterBufferSize)
	}
	reader := bufio.NewReaderSize(conn, ReaderBufferSize)
	c.respReader = goredis.NewRespReader(reader)

	c.respWriter = NewRespWriter(conn, WriterBufferSize)

	c.remoteAddr = conn.RemoteAddr().String()
	s.addRespClient(c)
	logger.Info("new  resp client")
	go c.DoRequest()
}
