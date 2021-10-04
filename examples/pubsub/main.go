/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"encoding/hex"
	"fmt"

	"github.com/newity/crawler"
	"github.com/newity/crawler/storage"
	"github.com/newity/crawler/storageadapter"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

const (
	CHANNEL   = "mychannel"
	USER      = "User1"
	ORG       = "Org1"
	CREDSFILE = "/path/to/creds.json"
)

func main() {
	pubsub, err := storage.NewPubSub(
		"hlf-newity",
		option.WithCredentialsFile(CREDSFILE),
	)
	if err != nil {
		logrus.Fatal(err)
	}

	err = pubsub.InitChannelsStorage([]string{CHANNEL})
	if err != nil {
		logrus.Fatal(err)
	}

	engine, err := crawler.New(
		"connection.yaml",
		crawler.WithStorage(pubsub),
		crawler.WithStorageAdapter(storageadapter.NewQueueAdapter(pubsub)),
	)
	if err != nil {
		logrus.Fatal(err)
	}

	err = engine.Connect(CHANNEL, USER, ORG)
	if err != nil {
		logrus.Fatal(err)
	}

	err = engine.Listen(crawler.FromBlock(), crawler.WithBlockNum(0))
	if err != nil {
		logrus.Fatal(err)
	}

	go engine.Run()

	readFromQueue(engine, CHANNEL)
	engine.StopListenAll()
}

func readFromQueue(engine *crawler.Crawler, topic string) {
	dataChan, errChan := engine.ReadStreamFromStorage(topic)

	for {
		select {
		case data := <-dataChan:
			logrus.Infof(
				"block %d with hash %s and previous hash %s\n\nOrderers signed:\n",
				data.BlockNumber,
				hex.EncodeToString(data.Datahash),
				hex.EncodeToString(data.Prevhash),
			)

			for _, signature := range data.BlockSignatures {
				fmt.Printf(
					"MSP ID: %s\nSignature: %s\nCertificate:\n%s\n",
					signature.MSPID,
					hex.EncodeToString(signature.Signature),
					string(signature.Cert),
				)
			}

			for _, tx := range data.Txs {
				t, err := tx.Timestamp()
				if err != nil {
					logrus.Error("failed to get timestamp", err)
				}

				txid, err := tx.TxId()
				if err != nil {
					logrus.Error("failed to get tx ID", err)
				}

				fmt.Printf(
					"Tx ID: %s\nCreation time: %s\n",
					txid,
					t.String(),
				)
			}
		case err := <-errChan:
			logrus.Error(err)
		}
	}
}
