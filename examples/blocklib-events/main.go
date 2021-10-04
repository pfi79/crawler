/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/newity/crawler/blocklib"
	log "github.com/sirupsen/logrus"
)

func main() {
	fabBlock, err := getBlock("../../blocklib/mock/withevents.pb")
	if err != nil {
		panic(err)
	}

	block, err := blocklib.FromFabricBlock(fabBlock)
	if err != nil {
		panic(err)
	}

	txs, err := block.Txs()
	if err != nil {
		log.Error(err)
	}

	for _, tx := range txs {
		actions, err := tx.Actions()
		if err != nil {
			log.Error(err)
		}

		for _, act := range actions {
			event, err := act.ChaincodeEvent()
			if err != nil {
				log.Error(err)
			}

			fmt.Printf("Event\nTx ID: %s\nEvent name: %s\nChaincode ID: %s\nPayload: %v\n",
				event.TxId,
				event.EventName,
				event.ChaincodeId,
				strings.Split(string(event.Payload), "\t"))
		}
	}
}

func getBlock(pathToBlock string) (*common.Block, error) {
	file, err := ioutil.ReadFile(pathToBlock)
	if err != nil {
		return nil, err
	}

	fabBlock := &common.Block{}

	err = proto.Unmarshal(file, fabBlock)

	return fabBlock, err
}
