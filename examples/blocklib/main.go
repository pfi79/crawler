/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/newity/crawler/blocklib"
)

func main() {
	fabBlock, err := getBlock("../../blocklib/mock/config.pb")
	if err != nil {
		panic(err)
	}

	block, err := blocklib.FromFabricBlock(fabBlock)
	if err != nil {
		panic(err)
	}

	fmt.Println("Config?", block.IsConfig())
	fmt.Println("hash", hex.EncodeToString(block.Hash()))
	fmt.Println("previous hash", hex.EncodeToString(block.PreviousHash()))
	fmt.Println("block number", block.Number())
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
