/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package crawler

import (
	"os"
	"path"

	"github.com/hyperledger/fabric-sdk-go/pkg/client/channel"
	"github.com/hyperledger/fabric-sdk-go/pkg/client/event"
	contextApi "github.com/hyperledger/fabric-sdk-go/pkg/common/providers/context"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/core"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/fab"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/msp"
	"github.com/hyperledger/fabric-sdk-go/pkg/core/config"
	"github.com/hyperledger/fabric-sdk-go/pkg/fab/events/deliverclient/seek"
	"github.com/hyperledger/fabric-sdk-go/pkg/fabsdk"
	"github.com/newity/crawler/parser"
	"github.com/newity/crawler/storage"
	"github.com/newity/crawler/storageadapter"
	"github.com/sirupsen/logrus"
)

// Crawler is responsible for fetching info from blockchain.
type Crawler struct {
	sdk             *fabsdk.FabricSDK
	chCli           map[string]*channel.Client
	eventCli        map[string]*event.Client
	channelProvider contextApi.ChannelProvider
	notifiers       map[string]<-chan *fab.BlockEvent
	registrations   map[string]fab.Registration
	parser          parser.Parser
	adapter         storageadapter.StorageAdapter
	storage         storage.Storage
	configProvider  core.ConfigProvider
}

// New creates Crawler instance from HLF connection profile and returns pointer to it.
// "connectionProfile" is a path to HLF connection profile.
func New(connectionProfile string, opts ...Option) (*Crawler, error) {
	crawl := &Crawler{
		chCli:         make(map[string]*channel.Client),
		eventCli:      make(map[string]*event.Client),
		notifiers:     make(map[string]<-chan *fab.BlockEvent),
		registrations: make(map[string]fab.Registration),
	}

	var err error
	for _, opt := range opts {
		if err = opt(crawl); err != nil {
			return nil, err
		}
	}

	if crawl.sdk == nil {
		if crawl.configProvider == nil {
			crawl.configProvider = config.FromFile(connectionProfile)
		}

		crawl.sdk, err = fabsdk.New(crawl.configProvider)
		if err != nil {
			return nil, err
		}
	}

	// if no parser is specified, use the default parser ParserImpl
	if crawl.parser == nil {
		crawl.parser = parser.New()
	}

	// if no storage is specified, use the default storage Badger
	if crawl.storage == nil {
		home := os.Getenv("HOME")

		crawl.storage, err = storage.NewBadger(path.Join(home, ".crawler-storage"))
		if err != nil {
			return nil, err
		}
	}

	// if no storage adapter is specified, use the default SimpleAdapter
	if crawl.adapter == nil {
		crawl.adapter = storageadapter.NewSimpleAdapter(crawl.storage)
	}

	return crawl, nil
}

// SDK returns fabsdk.FabricSDK instance.
func (c *Crawler) SDK() *fabsdk.FabricSDK {
	return c.sdk
}

// ConfigProvider returns core.ConfigProvider instance.
func (c *Crawler) ConfigProvider() core.ConfigProvider {
	return c.configProvider
}

// Connect connects crawler to channel 'ch' as identity specified
// in 'username' from organization with name 'org'.
func (c *Crawler) Connect(ch, username, org string) error {
	var err error

	c.channelProvider = c.sdk.ChannelContext(
		ch,
		fabsdk.WithUser(username),
		fabsdk.WithOrg(org),
	)
	c.chCli[ch], err = channel.New(c.channelProvider)

	return err
}

// ConnectWithIdentity connects crawler to channel 'ch' as
// passed signing identity from specified organization.
func (c *Crawler) ConnectWithIdentity(
	ch, org string,
	identity msp.SigningIdentity,
) error {
	var err error

	c.channelProvider = c.sdk.ChannelContext(
		ch,
		fabsdk.WithIdentity(identity),
		fabsdk.WithOrg(org),
	)
	c.chCli[ch], err = channel.New(c.channelProvider)

	return err
}

// Listen starts blocks listener starting from block with num 'from'.
// All consumed blocks will be hadled by the provided parser (or default parser ParserImpl).
func (c *Crawler) Listen(opts ...ListenOpt) error {
	var (
		err        error
		listenType string
		fromBlock  uint64
		clientOpts []event.ClientOption
	)

	for _, opt := range opts {
		switch v := opt().(type) {
		case string:
			listenType = v
		case int:
			fromBlock = uint64(v)
		}
	}

	switch listenType {
	case ListenFrom:
		clientOpts = append(clientOpts,
			event.WithBlockEvents(),
			event.WithSeekType(seek.FromBlock),
			event.WithBlockNum(fromBlock),
		)
	case ListenNewest:
		clientOpts = append(clientOpts,
			event.WithBlockEvents(),
			event.WithSeekType(seek.Newest),
		)
	case ListenOldest:
		clientOpts = append(clientOpts,
			event.WithBlockEvents(),
			event.WithSeekType(seek.Oldest),
		)
	}

	for ch := range c.chCli {
		c.eventCli[ch], err = event.New(
			c.channelProvider,
			clientOpts...,
		)
		if err != nil {
			return err
		}

		c.registrations[ch], c.notifiers[ch], err = c.eventCli[ch].RegisterBlockEvent()

		return err
	}

	return err
}

func (c *Crawler) ListenerForChannel(channel string) <-chan *fab.BlockEvent {
	return c.notifiers[channel]
}

// StopListenChannel removes the registration for
// block events from channel and closes the channel.
func (c *Crawler) StopListenChannel(channel string) {
	for ch, eventcli := range c.eventCli {
		if channel == ch {
			eventcli.Unregister(c.registrations[ch])
		}
	}
}

// StopListenAll removes the registration for
// block events from all channels and closes these channels.
func (c *Crawler) StopListenAll() {
	for ch, eventcli := range c.eventCli {
		eventcli.Unregister(c.registrations[ch])
	}
}

// Run starts parsing blocks and saves them to storage.
// The parsing strategy is determined by the implementation of the parser.
// What and in what form will be stored in the storage is determined by the storage adapter implementation.
func (c *Crawler) Run() {
	for _, notifier := range c.notifiers {
		for blockevent := range notifier {
			data, err := c.parser.Parse(blockevent.Block)
			if err != nil {
				logrus.Error(err)

				continue
			}

			if data == nil {
				continue
			}

			if err = c.adapter.Inject(data); err != nil {
				logrus.Error(err)
			}
		}
	}
}

// GetFromStorage retrieves specified data from a storage by specified key
// and returns it in the form of parser.Data.
func (c *Crawler) GetFromStorage(key string) (*parser.Data, error) {
	return c.adapter.Retrieve(key)
}

func (c *Crawler) ReadStreamFromStorage(key string) (<-chan *parser.Data, <-chan error) {
	return c.adapter.ReadStream(key)
}
