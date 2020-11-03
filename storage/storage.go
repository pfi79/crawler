/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storage

// Storage interface is a contract for storage implementations
type Storage interface {
	// init storage (initial setup of storage and connection create operations)
	InitChannelsStorage(channels []string)
	// put value by key
	Put(key string, value []byte) error
	// get data from storage by specified key
	Get(key string) ([]byte, error)
	// remove parser.Data from storage
	Delete(key string) error
	// close connection to storage (network connections, file descriptors, goroutines)
	Close() error
}
