/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package storageadapter

import (
	"context"
	"github.com/newity/crawler/parser"
)

type StorageAdapter interface {
	Inject(data *parser.Data) error
	InjectBatch(data []parser.Data) error
	Retrieve(key string) (*parser.Data, error)
	ReadStream(ctx context.Context, key string) (<-chan *parser.Data, <-chan error)
}
