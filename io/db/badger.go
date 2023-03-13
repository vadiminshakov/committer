package db

import (
	badger "github.com/dgraph-io/badger/v2"
)

type BadgerDB struct {
	db *badger.DB
}

func New(path string) (Repository, error) {
	db, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	return &BadgerDB{db}, nil
}

func (b *BadgerDB) Put(key string, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

func (b *BadgerDB) Get(key string) ([]byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		return err
	})
	return value, err
}

func (b *BadgerDB) Close() error {
	return b.db.Close()
}
