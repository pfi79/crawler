/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"github.com/newity/crawler"
	"github.com/newity/crawler/blocklib"
	"github.com/sirupsen/logrus"
)

const (
	CHANNEL = "mychannel"
	USER    = "User1"
	ORG     = "Org1"
)

func main() {
	engine, err := crawler.New(
		"connection.yaml",
		crawler.WithAutoConnect(USER, ORG),
	)
	if err != nil {
		logrus.Error(err)
	}

	err = engine.Listen(crawler.FromBlock(), crawler.WithBlockNum(0))
	if err != nil {
		logrus.Error(err)
	}

	listener := engine.ListenerForChannel(CHANNEL)

	for blockevent := range listener {
		block, err := blocklib.FromFabricBlock(blockevent.Block)
		if err != nil {
			logrus.Error(err)
		}

		index, err := block.LastConfig()
		if err != nil {
			logrus.Error(err)
		}

		logrus.Infof(
			"[%s] Last config index for block %d is %d\n",
			CHANNEL,
			block.Number(),
			index,
		)
	}

	engine.StopListenAll()
}
