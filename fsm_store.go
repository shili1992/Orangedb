// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package Orangedb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/laohanlinux/go-logger/logger"
	"github.com/hashicorp/go-msgpack/codec"
)

const (
	retainSnapshotCount = 2
	raftTimeout = 10 * time.Second
)



// Store is a simple key-value store, where all changes are made via Raft consensus.
//实现了   service.Store interface   and raft.FSM interface
type FsmStore struct {
	RaftDir     string
	RaftBind    string

	mu          sync.Mutex
	m           map[string]string // The key-value store for the system.

	Raft        *raft.Raft        // The consensus mechanism  //todo(shili )raft 功能放置的位置不好

	BatchSyncCh chan *BatchTxn    //该通道用于接收batchTxn，并拷贝到follower
}

// New returns a new Store.
func NewFsmStore() *FsmStore {
	return &FsmStore{
		m:      make(map[string]string),
	}
}

//返回raft 服务的结构
func (s *FsmStore) GetRaftSevice() *raft.Raft {
	return s.Raft
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *FsmStore) Open(enableSingle bool) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.RaftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		logger.Infof("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10 * time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create peer storage.
	peerStore := raft.NewJSONPeers(s.RaftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*FsmStore)(s), logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.Raft = ra
	return nil
}




// Get returns the value for the given key. 直接本地设置
func (s *FsmStore) Get(key string) (string, error) {
	s.mu.Lock()
	logger.Info("Get a value by ", string(key))
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.  发送master raft ,然后在进行set
func (s *FsmStore) Set(key, value string) error {
	if s.Raft.State() != raft.Leader {
		logger.Infof("not leader")
		return ErrNotLeader
	}

	applyCmd := &applyCommand{
		Op:    "set",
		Key:   key,
		Value: value,
	}

	bs, err := s.MsgpackEncode(applyCmd);
	if nil != err {
		logger.Warnf("encode  batchTxn fail,err=%v", err)
		return err
	}
	c := &Command{
		Type:  ApplyCommand,
		Data:  bs,
	}
	b, err := s.MsgpackEncode(c);
	if err != nil {
		return err
	}
	f := s.Raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *FsmStore) MsgpackEncode(ts interface{}) (bs []byte, err error) {
	err = codec.NewEncoderBytes(&bs, &codec.MsgpackHandle{}).Encode(ts)
	return
}



// Delete deletes the given key.  发送master raft ,然后在进行删除
func (s *FsmStore) Delete(key string) error {
	if s.Raft.State() != raft.Leader {
		logger.Infof("not leader")
		return ErrNotLeader
	}

	applyCmd := &applyCommand{
		Op:  "del",
		Key: key,
	}

	bs, err := s.MsgpackEncode(applyCmd);
	if nil != err {
		logger.Warnf("encode  batchTxn fail,err=%v", err)
		return err
	}

	c := &Command{
		Type:  ApplyCommand,
		Data:  bs,
	}

	b, err := s.MsgpackEncode(c);
	if err != nil {
		return err
	}

	f := s.Raft.Apply(b, raftTimeout)
	return f.Error()
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *FsmStore) Join(addr string) error {
	logger.Infof("received join request for remote node as %s", addr)

	f := s.Raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	logger.Infof("node at %s joined successfully", addr)
	return nil
}


func (s *FsmStore) MsgpackDecode(buf []byte, ts interface{}) error {
	return codec.NewDecoderBytes(buf, &codec.MsgpackHandle{}).Decode(ts)
}
//raft.FSM接口
// Apply applies a Raft log entry to the key-value store.
func (f *FsmStore) Apply(l *raft.Log) interface{} {
	var c Command
	if err := f.MsgpackDecode(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Type {
	case ApplyCommand:
		var applyCmd applyCommand
		if err := f.MsgpackDecode(c.Data, &applyCmd); err != nil {
			panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
		}

		switch applyCmd.Op {
		case "set":
			logger.Debug("set a key:value ", string(applyCmd.Key), string(applyCmd.Value))
			return f.applySet(applyCmd.Key, applyCmd.Value)
		case "del":
			logger.Debug("delete  a key ", string(applyCmd.Key))
			return f.applyDelete(applyCmd.Key)
		default:
			panic(fmt.Sprintf("unrecognized command op: %s", applyCmd.Op))
		}
	case BatchSubCommand:
		var batchCmd batchSubCommand
		if err := f.MsgpackDecode(c.Data, &batchCmd); err != nil {
			panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
		}
		f.BatchSyncCh<- batchCmd.BatchTxn    //像 reader 传递一个信息
	}
	return nil
}

//raft.FSM接口
// Snapshot returns a snapshot of the key-value store.
func (f *FsmStore) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

//raft.FSM接口
// Restore stores the key-value store to a previous state.
func (f *FsmStore) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *FsmStore) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	//logger.Info("set a key:value ", string(key),string(value))
	f.m[key] = value
	return nil
}

func (f *FsmStore) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	//logger.Info("delete a value by ", string(key))
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}

func readPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}
