/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package blocklib

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/stretchr/testify/assert"
)

func getBlock(pathToBlock string) (*common.Block, error) {
	file, err := ioutil.ReadFile(pathToBlock)
	if err != nil {
		return nil, err
	}

	fabBlock := &common.Block{}
	err = proto.Unmarshal(file, fabBlock)

	return fabBlock, err
}

func TestFromFabricBlock(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/genesis.pb")
	assert.NoError(t, err)

	block, err := FromFabricBlock(fabBlock)
	assert.NoError(t, err)
	assert.NotNil(t, block.Data)
	assert.NotNil(t, block.Metadata)
	assert.NotNil(t, block.OrderersSignatures)
}

func TestBlockSignatures(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/genesis.pb")
	assert.NoError(t, err)

	block, err := FromFabricBlock(fabBlock)
	assert.NoError(t, err)

	sigs := block.OrderersSignatures()

	for _, sig := range sigs {
		assert.Equal(t, uint64(12652116863344733010), binary.BigEndian.Uint64(sig.Nonce))
		assert.Equal(t, "OrdererMSP", sig.MSPID)

		certhash := sha256.New()
		_, err = certhash.Write(sig.Cert)
		assert.NoError(t, err)

		sighash := sha256.New()
		_, err = sighash.Write(sig.Signature)

		assert.NoError(t, err)
		assert.Equal(
			t,
			"9fa97f0795f3ade55fbf89419e7c8afaf2135b58bdf8ad39728a173ec498bfaf",
			hex.EncodeToString(certhash.Sum(nil)),
		)
		assert.Equal(
			t,
			"3d82fb80a17f2c189d088330d2459efe66e7a409ebd4ccbf2f32f5a768059345",
			hex.EncodeToString(sighash.Sum(nil)),
		)
	}
}

func TestTxs(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		fabBlock, err := getBlock("./mock/sampleblock.pb")
		assert.NoError(t, err)

		block, err := FromFabricBlock(fabBlock)
		assert.NoError(t, err)

		txs, err := block.Txs()
		assert.NoError(t, err)

		for _, tx := range txs {
			assert.NotNil(t, tx.Data)
			assert.Equal(t, int32(0), tx.ValidationCode())
			assert.Equal(t, "VALID", tx.ValidationStatus())
		}
	})

	t.Run("with MVCC_READ_CONFLICT", func(t *testing.T) {
		t.Parallel()

		fabBlock, err := getBlock("./mock/mvcc_read_conflict.pb")
		assert.NoError(t, err)

		block, err := FromFabricBlock(fabBlock)
		assert.NoError(t, err)

		txs, err := block.Txs()
		assert.NoError(t, err)

		for _, tx := range txs {
			assert.NotNil(t, tx.Data)
			assert.Equal(t, int32(11), tx.ValidationCode())
			assert.Equal(t, "MVCC_READ_CONFLICT", tx.ValidationStatus())
		}
	})
}

func TestLastConfig(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	block, _ := FromFabricBlock(fabBlock)
	lastConfig, _ := block.LastConfig()
	assert.Equal(t, uint64(0), lastConfig)
}

func TestNumber(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	block, _ := FromFabricBlock(fabBlock)
	number := block.Number()
	assert.Equal(t, uint64(7), number)
}

func TestPreviousHash(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	block, _ := FromFabricBlock(fabBlock)
	prevhash := block.PreviousHash()
	assert.Equal(
		t,
		"3cc69f358eacc13a378045bbfe5c516059a8969b3a77d6b80ed67c786d47e5ad",
		hex.EncodeToString(prevhash),
	)
}

func TestDataHash(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	block, _ := FromFabricBlock(fabBlock)
	hash := block.DataHash()
	assert.Equal(
		t,
		"db7a04bfca3b18b7cc6f6544863bec7f6b8d863bf8488bd92e25c71ffe04769b",
		hex.EncodeToString(hash),
	)
}

func TestHeaderHash(t *testing.T) {
	t.Parallel()

	fabBlock, err := getBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	block, _ := FromFabricBlock(fabBlock)
	hash := block.HeaderHash()
	assert.Equal(
		t,
		"1652fcac96482da896909760e3df4758195fcad4672a54123e586c9a26afde0e",
		hex.EncodeToString(hash),
	)
}
