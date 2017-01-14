package service

import (
	"net"
	"github.com/siddontang/goredis"
	"bufio"
	"github.com/siddontang/go/hack"
	"strings"
	"github.com/laohanlinux/go-logger/logger"
	"runtime"
	"strconv"
	"github.com/siddontang/go/num"
	"github.com/shili/Orangedb/common"
)

var (
	Delims = []byte("\r\n")

	NullBulk = []byte("-1")
	NullArray = []byte("-1")

	PONG = "PONG"
	OK = "OK"
	NOKEY = "NOKEY"
)

const (
	ReaderBufferSize = 4096
	WriterBufferSize = 4096
)

type  Client struct {
	service    *Service
	remoteAddr string
}

func (c *Client) close() {

}


//RespClient 继承子  Client
type RespClient struct {
	* Client
	conn       net.Conn
	respReader *goredis.RespReader //用于读取 客户端的内容
	respWriter *RespWriter
	cmd        string
	args       [][]byte
}

func (c *RespClient )  DoRequest() {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			logger.Warnf("client run panic %s:%v", buf, e)
		}

		c.Client.close()
		c.conn.Close()
		c.service.delRespClient(c)
	}()
	for {
		c.cmd = ""
		c.args = nil

		reqData, err := c.respReader.ParseRequest() //获取请求
		if err == nil {
			err = c.handleRequest(reqData)
		}

		if err != nil {
			return
		}
	}
}

func (c *RespClient) handleRequest(reqData [][]byte) error {
	if len(reqData) == 0 {
		c.cmd = ""
		c.args = reqData[0:0]
	} else {
		c.cmd = strings.ToLower(hack.String(reqData[0]))
		c.args = reqData[1:]
	}

	argsLen := len(c.args)
	argStrs := make([]string,argsLen)
	for i:=0;i<argsLen;i++{
		argStrs[i] = string(c.args[i])
	}

	if c.cmd == "select"||c.cmd == "xselect"||c.cmd == "ping" {
		c.respWriter.writeStatus("OK")
		c.respWriter.flush()
		return nil
	}/*else if c.cmd == "get" {
		logger.Infof("cmd:%s, args:%v",c.cmd,argStrs)
		c.respWriter.writeStatus("OK")
		c.respWriter.flush()
		return nil
	}else if c.cmd == "set" {
		logger.Infof("cmd:%s, args:%v",c.cmd,argStrs)
		c.respWriter.writeStatus("OK")
		c.respWriter.flush()
		return nil
	}else if c.cmd == "delete" {
		logger.Infof("cmd:%s, args:%v",c.cmd,argStrs)
		c.respWriter.writeStatus("OK")
		c.respWriter.flush()
		return nil
	}else{
		c.respWriter.writeStatus("OK")
		c.respWriter.flush()
		return nil
	}*/
	if err :=c.handleCommand();err!=nil{
		c.respWriter.writeError(err)  //回复错误信息
		c.respWriter.flush()
		return err
	}
	return nil
}

func (c *RespClient) handleCommand() error {
	if cmdfunc,ok:=commandFuncMap[c.cmd];!ok{
		return common.ErrNotFound
	}else if err:= cmdfunc(c);err!=nil{
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

func (w *RespWriter) flush() {
	w.buffer.Flush()
}

func newClient(s *Service) *Client {
	c := new(Client)
	c.service = s
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