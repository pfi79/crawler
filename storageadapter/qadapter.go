/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storageadapter

import (
	"context"
	"sync"

	"github.com/newity/crawler/parser"
	"github.com/newity/crawler/storage"
)

// QueueAdapter is a general storage adapter for the message brokers.
type QueueAdapter struct {
	storage storage.Storage
}

func NewQueueAdapter(stor storage.Storage) *QueueAdapter {
	return &QueueAdapter{stor}
}

func (s *QueueAdapter) Inject(data *parser.Data) error {
	encoded, err := Encode(data)
	if err != nil {
		return err
	}

	return s.storage.Put(data.Channel, encoded)
}

func (s *QueueAdapter) Retrieve(topic string) (*parser.Data, error) {
	value, err := s.storage.Get(context.Background(), topic)
	if err != nil {
		return nil, err
	}

	return Decode(value)
}

func (s *QueueAdapter) ReadStream(
	ctx context.Context,
	topic string,
) (<-chan *parser.Data, <-chan error) {
	var (
		out        = make(chan *parser.Data)
		errOutChan = make(chan error)
		wg         sync.WaitGroup
	)

	stream, errChan := s.storage.GetStream(ctx, topic)

	wg.Add(1)

	go func() {
		wg.Done()

		for {
			select {
			case msg := <-stream:
				decodedMsg, err := Decode(msg)
				if err != nil {
					errOutChan <- err

					continue
				}
				out <- decodedMsg
			case err := <-errChan:
				errOutChan <- err
			}
		}
	}()

	wg.Wait()

	return out, errOutChan
}
