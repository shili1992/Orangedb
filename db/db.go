package db

import (
	"github.com/shili/Orangedb/store"
)

type DB struct {
	storage * store.Store
}


func (db *DB) OpenTransaction() (*Transaction, error) {

}

