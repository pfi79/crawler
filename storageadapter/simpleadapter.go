/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storageadapter

import (
	"context"
	"errors"
	"github.com/newity/crawler/parser"
	"github.com/newity/crawler/storage"
	"strconv"
)

type SimpleAdapter struct {
	storage storage.Storage
}

func NewSimpleAdapter(stor storage.Storage) *SimpleAdapter {
	return &SimpleAdapter{stor}
}

func (s *SimpleAdapter) Inject(data *parser.Data) error {
	encoded, err := Encode(data)
	if err != nil {
		return err
	}
	return s.storage.Put(strconv.Itoa(int(data.BlockNumber)), encoded)
}

func (s *SimpleAdapter) InjectBatch(data []parser.Data) error {
	if len(data) == 0 {
		return errors.New("empty slice passed to InjectBatch")
	}
	encoded, err := EncodeBatch(data)
	if err != nil {
		return err
	}
	return s.storage.Put(strconv.Itoa(int(data[len(data)-1].BlockNumber)), encoded)
}

func (s *SimpleAdapter) Retrieve(blocknum string) (*parser.Data, error) {
	value, err := s.storage.Get(context.Background(), blocknum)
	if err != nil {
		return nil, err
	}
	return Decode(value)
}

func (s *SimpleAdapter) ReadStream(_ context.Context, blocknum string) (<-chan *parser.Data, <-chan error) {
	return nil, nil
}
