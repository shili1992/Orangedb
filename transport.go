package Orangedb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/laohanlinux/go-logger/logger"
)

const (
	rpcAppendEntries uint8 = iota    //日志复制请求
	rpcTransPortBatch
	rpcRetReadResult

	// DefaultTimeoutScale is the default TimeoutScale in a Transport.
	DefaultTimeoutScale = 256 * 1024 // 256KB
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP = errors.New("local address is not a TCP address")
)

// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}         //rpc 请求（req_type +  args）
	Reader   io.Reader           // Set only for InstallSnapshot
	RespChan chan <- RPCResponse //用于回复的通道
}

// Respond is used to respond with a response, error or both
//回复请求信息
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}


//实现了  Transport 接口， 用于网络通信 ，用于与raft中其他节点通信
type Transport struct {
	connPool        map[string][]*netConn
	connPoolLock    sync.Mutex

	consumeCh       chan RPC

	heartbeatFn     func(RPC)
	heartbeatFnLock sync.Mutex

	logger          *log.Logger

	maxPool         int

	shutdown        bool
	shutdownCh      chan struct{}
	shutdownLock    sync.Mutex

	stream          StreamLayer

	timeout         time.Duration
	TimeoutScale    int
}

// StreamLayer is used with Transport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

type netConn struct {
	target string
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder //encode  的buffer 为  netConn.w
}

func (n *netConn) Release() error {
	return n.conn.Close()
}


// NewTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewTransport(
stream StreamLayer,
maxPool int,
timeout time.Duration,
logOutput io.Writer,
) *Transport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewTransportWithLogger(stream, maxPool, timeout, log.New(logOutput, "", log.LstdFlags))
}

// NewTransportWithLogger creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewTransportWithLogger(
stream StreamLayer,
maxPool int,
timeout time.Duration,
logger *log.Logger,
) *Transport {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	trans := &Transport{
		connPool:     make(map[string][]*netConn),
		consumeCh:    make(chan RPC),
		logger:       logger,
		maxPool:      maxPool,
		shutdownCh:   make(chan struct{}),
		stream:       stream,
		timeout:      timeout,
		TimeoutScale: DefaultTimeoutScale,
	}
	go trans.listen()
	return trans
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (n *Transport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// Close is used to stop the network transport.
func (n *Transport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.stream.Close()
		n.shutdown = true
	}
	return nil
}

// Consumer implements the Transport interface.
func (n *Transport) Consumer() <-chan RPC {
	return n.consumeCh
}

// LocalAddr implements the Transport interface.
func (n *Transport) LocalAddr() string {
	return n.stream.Addr().String()
}

// IsShutdown is used to check if the transport is shutdown.
func (n *Transport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
func (n *Transport) getPooledConn(target string) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num - 1] = conns[num - 1], nil
	n.connPool[target] = conns[:num - 1]
	return conn
}

// getConn is used to get a connection from the pool.
func (n *Transport) getConn(target string) (*netConn, error) {
	// Check for a pooled conn
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		r:      bufio.NewReader(conn),
		w:      bufio.NewWriter(conn),
	}

	// Setup encoder/decoders
	netConn.dec = codec.NewDecoder(netConn.r, &codec.MsgpackHandle{})
	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	// Done
	return netConn, nil
}

// returnConn returns a connection back to the pool.
func (n *Transport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns, _ := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

func (n *Transport) TransBatchTxn(target string, args *TransBatchTxnRequest, resp *TransBatchTxnResponse) error {
	return n.genericRPC(target, rpcTransPortBatch, args, resp)
}

func (n *Transport) RetReadResult(target string, args *RetReadResultRequest, resp *RetReadResultResponse) error {
	return n.genericRPC(target, rpcRetReadResult, args, resp)
}

// genericRPC handles a simple request/response RPC.
/*@param rpcType[in]  rpc调用的类型  */
func (n *Transport) genericRPC(target string, rpcType uint8, args interface{}, resp interface{}) error {
	// Get a conn
	conn, err := n.getConn(target)
	if err != nil {
		return err
	}

	// Set a deadline
	if n.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		n.returnConn(conn)
	}
	return err
}


// EncodePeer implements the Transport interface.
func (n *Transport) EncodePeer(p string) []byte {
	return []byte(p)
}

// DecodePeer implements the Transport interface.
func (n *Transport) DecodePeer(buf []byte) string {
	return string(buf)
}

// listen is used to handling incoming connections.
func (n *Transport) listen() {
	for {
		// Accept incoming connections
		conn, err := n.stream.Accept()
		if err != nil {
			if n.IsShutdown() {
				return
			}
			n.logger.Printf("[ERR] raft-net: Failed to accept connection: %v", err)
			continue
		}
		n.logger.Printf("[DEBUG] raft-net: %v accepted connection from: %v", n.LocalAddr(), conn.RemoteAddr())

		// Handle the connection in dedicated routine
		go n.handleConn(conn)
	}
}

// handleConn is used to handle an inbound connection for its lifespan.
//当有连接过来，开启一个线程来处理该请求
func (n *Transport) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})
	logger.Infof("request:%s",conn.RemoteAddr().String())
	for {
		if err := n.handleCommand(r, dec, enc); err != nil {
			//处理命令
			if err != io.EOF {
				n.logger.Printf("[ERR] raft-net: Failed to decode incoming command: %v", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			//给远程节点返回结果
			n.logger.Printf("[ERR] raft-net: Failed to flush response: %v", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command.
//处理请求命令（ rcptype + args），并将要回复的内容 填到enc 中
//dec  [in]  需要处理的内容
//enc  [out]  回复的内容
func (n *Transport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// Create the RPC object
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	// Decode the command
	switch rpcType {
	case rpcTransPortBatch:  //日志复制请求
		var req TransBatchTxnRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		logger.Info("get one  command: rpcTransPortBatch")
	case rpcRetReadResult:
		var req RetReadResultRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		logger.Info("get one command: rpcRetReadResult")
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	// Dispatch the RPC  发送rpc的请求到其他线程处理
	select {
	case n.consumeCh <- rpc:   //交给 service处理
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	select {
	case resp := <-respCh:
	//等待回复
	// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

	// Send the response  往缓存中需要返回的内容
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

// sendRPC is used to encode and send the RPC.
//先序列化 一个 类型  然后在序列化相关的参数
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// Write the request type
	if err := conn.w.WriteByte(rpcType); err != nil {
		conn.Release()
		return err
	}

	// Send the request
	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}


















// TCPStreamLayer implements StreamLayer interface for plain TCP.
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewTCPTransport returns a Transport that is built on top of
// a TCP streaming transport layer.
func NewTCPTransport(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, logOutput io.Writer, ) (*Transport, error) {
	return newTCPTransport(bindAddr, advertise, maxPool, timeout, func(stream StreamLayer) *Transport {
		return NewTransport(stream, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger returns a Transport that is built on top of
// a TCP streaming transport layer, with log output going to the supplied Logger
func NewTCPTransportWithLogger(
bindAddr string,
advertise net.Addr,
maxPool int,
timeout time.Duration,
logger *log.Logger,
) (*Transport, error) {
	return newTCPTransport(bindAddr, advertise, maxPool, timeout, func(stream StreamLayer) *Transport {
		return NewTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

func newTCPTransport(bindAddr string,
advertise net.Addr,
maxPool int,
timeout time.Duration,
transportCreator func(stream StreamLayer) *Transport) (*Transport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	// Verify that we have a usable advertise address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	if addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}

	// Create the network transport
	trans := transportCreator(stream)
	return trans, nil
}

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address, timeout)
}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
