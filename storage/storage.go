/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storage

// Storage interface is a contract for storage implementations.
type Storage interface {
	// InitChannelsStorage init storage (initial setup of
	// storage and connection create operations)
	InitChannelsStorage(channels []string) error
	// Put - put value by key
	Put(key string, value []byte) error
	// Get - get data from storage by specified key
	Get(key string) ([]byte, error)
	// GetStream - get channel with some data from storage
	// (for message broker storage implementations)
	GetStream(key string) (<-chan []byte, <-chan error)
	// Delete - remove parser.Data from storage
	Delete(key string) error
	// Close - close connection to storage (network connections, file descriptors, goroutines)
	Close() error
}
