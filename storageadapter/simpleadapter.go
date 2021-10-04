/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storageadapter

import (
	"strconv"

	"github.com/newity/crawler/parser"
	"github.com/newity/crawler/storage"
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

func (s *SimpleAdapter) Retrieve(blocknum string) (*parser.Data, error) {
	value, err := s.storage.Get(blocknum)
	if err != nil {
		return nil, err
	}

	return Decode(value)
}

func (s *SimpleAdapter) ReadStream(_ string) (<-chan *parser.Data, <-chan error) { // blocknum
	return nil, nil
}
