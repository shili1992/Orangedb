package db

import "sync"

type Transaction struct {
	db        *DB
	lk        sync.RWMutex
	closed    bool
}



