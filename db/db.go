package db

import (
	"github.com/shili1992/Orangedb/store"
)

type DB struct {
	storage *store.Store
}

func (db *DB) OpenTransaction() (*Transaction, error) {
	return nil
}
