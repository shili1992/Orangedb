package Orangedb

import (
	"sync"
	"github.com/laohanlinux/go-logger/logger"
)

type DB struct {
	mu          sync.RWMutex
	m           map[string]string // The key-value store for the system.

	Chooser        *HashChooser   //分区方法
	partitionID    int
}


// New returns a new Store.
func NewDB() *DB {
	return &DB{
		m:      make(map[string]string),
	}
}


func  (d *DB)isLocal(key string) bool{
	return d.Chooser.Choose(key) == d.partitionID
}


func (d *DB)Execute(txn * Txn){
	for _,c := range txn.Cmds{
		switch c.Cmd {
		case "set":
			key:= string(c.Args[0])
			if d.isLocal(key){
				d.Set(key, string(c.Args[1]))
			}
		case "del":
			key:= string(c.Args[0])
			if d.isLocal(key) {
				d.Delete(string(c.Args[0]))
			}
		case "get":
			key:= string(c.Args[0])
			if d.isLocal(key) {
				value,_:=d.Get(key)
				txn.AppendReadResult(key,value)
			}

		case "mget":
			for _,arg:= range c.Args{
				key:= string(arg)
				if d.isLocal(key) {
					value,_:=d.Get(key)
					txn.AppendReadResult(key,value)
				}
			}
		case "mset":
			for index:=0; index <len(c.Args);index+=2{
				key:= string(c.Args[index])
				if d.isLocal(key) {
					d.Set(key, string(c.Args[index + 1]))
				}
			}
		}
	}
}


// Get returns the value for the given key. 直接本地设置
func (s *DB) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	logger.Infof("Get a value, key:%s ", string(key))
	return s.m[key], nil
}

// Set sets the value for the given key.  发送master raft ,然后在进行set
func (s *DB) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key]= value
	logger.Infof("set a key,value  [%s,%s] ", key,value)
	return nil
}

// Delete del the given key.  发送master raft ,然后在进行删除
func (s *DB) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m,key)
	logger.Infof("delete  a key  ", string(key))
	return nil
}


