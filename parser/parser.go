/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package parser

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/newity/crawler/blocklib"
	"github.com/sirupsen/logrus"
)

type ParserImpl struct{}

func New() *ParserImpl {
	return &ParserImpl{}
}

func (p *ParserImpl) Parse(block *common.Block) (*Data, error) {
	b, err := blocklib.FromFabricBlock(block)
	if err != nil {
		return nil, err
	}

	txs, err := b.Txs()
	if err != nil {
		return nil, err
	}

	var (
		selectedTransactions []blocklib.Tx
		selectedEvents       []*peer.ChaincodeEvent
	)

	if !b.IsConfig() {
		for _, tx := range txs {
			selectedTransactions = append(selectedTransactions, tx)

			actions, err := tx.Actions()
			if err != nil {
				logrus.Errorf("failed to actions from transaction: %s", err)

				continue
			}

			for _, action := range actions {
				ccEvent, err := action.ChaincodeEvent()
				if err != nil {
					logrus.Errorf("failed to extract chaincode events: %s", err)

					continue
				}

				selectedEvents = append(selectedEvents, ccEvent)
			}
		}
	}

	header, err := txs[0].ChannelHeader()
	if err != nil {
		return nil, err
	}

	return &Data{
		BlockNumber:     block.Header.Number,
		Prevhash:        block.Header.PreviousHash,
		Datahash:        block.Header.DataHash,
		BlockSignatures: b.OrderersSignatures(),
		Channel:         header.ChannelId,
		Txs:             selectedTransactions,
		Events:          selectedEvents,
	}, nil
}
