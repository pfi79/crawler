/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storage

import "context"

// Storage interface is a contract for storage implementations
type Storage interface {
	// init storage (initial setup of storage and connection create operations)
	InitChannelsStorage(channels []string) error
	// put value by key
	Put(key string, value []byte) error
	// get data from storage by specified key
	Get(ctx context.Context, key string) ([]byte, error)
	// get channel with some data from storage (for message broker storage implementations)
	GetStream(ctx context.Context, key string) (<-chan []byte, <-chan error)
	// remove parser.Data from storage
	Delete(key string) error
	// close connection to storage (network connections, file descriptors, goroutines)
	Close() error
}
