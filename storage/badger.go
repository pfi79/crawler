/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storage

import (
	"context"
	badger "github.com/dgraph-io/badger/v2"
	"os"
	"strconv"
	"strings"
)

type Badger struct {
	db *badger.DB
}

func NewBadger(path string) (*Badger, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		if strings.Contains(err.Error(), "Another process is using this Badger database") {
			db, err = badger.Open(badger.DefaultOptions(path + strconv.Itoa(os.Getpid())))
		} else {
			return nil, err
		}
	}

	return &Badger{db}, nil
}

func (b *Badger) InitChannelsStorage(channels []string) error {
	return nil
}

// Put saves value by key
func (b *Badger) Put(key string, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

// Get retrieves data from BadgerDB using key
func (b *Badger) Get(_ context.Context, key string) ([]byte, error) {
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

func (b *Badger) Delete(key string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (b *Badger) Close() error {
	return b.db.Close()
}

func (b *Badger) GetStream(_ context.Context, key string) (<-chan []byte, <-chan error) {
	return nil, nil
}
