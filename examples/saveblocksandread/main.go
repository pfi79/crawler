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
	"os"
	"path"
	"strconv"
	"time"
)

const (
	USER = "User1"
	ORG  = "Org1"
)

func main() {
	home := os.Getenv("HOME")
	stor, err := storage.NewBadger(path.Join(home, ".crawler-storage"))
	if err != nil {
		logrus.Fatal(err)
	}

	engine, err := crawler.New("connection.yaml", crawler.WithAutoConnect(USER, ORG), crawler.WithStorage(stor), crawler.WithStorageAdapter(storageadapter.NewSimpleAdapter(stor)))
	if err != nil {
		logrus.Error(err)
	}

	err = engine.Listen(crawler.FromBlock(), crawler.WithBlockNum(0))
	if err != nil {
		logrus.Error(err)
	}

	go engine.Run()

	// wait for pulling blocks to storage
	time.Sleep(1 * time.Second)

	readBlock(engine, 2)
	engine.StopListenAll()
}

func readBlock(engine *crawler.Crawler, num int) {
	data, err := engine.GetFromStorage(strconv.Itoa(num))
	if err != nil {
		logrus.Error(err)
	}

	logrus.Infof("block %d with hash %s and previous hash %s\n\nOrderers signed:\n", data.BlockNumber,
		hex.EncodeToString(data.Datahash),
		hex.EncodeToString(data.Prevhash))
	for _, signature := range data.BlockSignatures {
		fmt.Printf("MSP ID: %s\nSignature: %s\nCertificate:\n%s\n", signature.MSPID, hex.EncodeToString(signature.Signature), string(signature.Cert))
	}
	fmt.Println("Transactions")
	for _, tx := range data.Txs {
		t, err := tx.Timestamp()
		if err != nil {
			logrus.Error(err)
		}
		txid, err := tx.TxId()
		if err != nil {
			logrus.Error(err)
		}

		fmt.Printf("Tx ID: %s\nCreation time: %s\n", txid, t.String())

		// print rwset
		actions, err := tx.Actions()
		if err != nil {
			panic(err)
		}

		for _, action := range actions {
			rwsets, err := action.RWSets()
			if err != nil {
				panic(err)
			}

			for _, rw := range rwsets {
				fmt.Println("readset", rw.KVRWSet.Reads)
				fmt.Println("writeset", rw.KVRWSet.Writes)
			}
		}
	}
}
