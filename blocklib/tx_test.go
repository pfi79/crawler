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
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/prometheus/common/log"
	"github.com/stretchr/testify/assert"
)

var (
	tx             Tx
	invalidtx      Tx
	configtx       Tx
	configUpdateTx Tx
	block1         *Block
	block2         *Block
	block3         *Block
)

func TestMain(m *testing.M) {
	txsvalid, err := readTxsFromBlock("./mock/sampleblock.pb")
	if err != nil {
		log.Error(err)
	}

	tx = txsvalid[0]

	txsinvalid, err := readTxsFromBlock("./mock/mvcc_read_conflict.pb")
	if err != nil {
		log.Error(err)
	}

	invalidtx = txsinvalid[0]

	conftxs, err := readTxsFromBlock("./mock/config.pb")
	if err != nil {
		log.Error(err)
	}

	configtx = conftxs[0]

	confUpdTxs, err := readTxsFromBlock("./mock/configUpdate.pb")
	if err != nil {
		log.Error(err)
	}

	configUpdateTx = confUpdTxs[0]

	block1, err = getBlocklibBlock("./mock/forIntegrityCheck.pb")
	if err != nil {
		log.Error(err)
	}

	block2, err = getBlocklibBlock("./mock/configUpdate.pb")
	if err != nil {
		log.Error(err)
	}

	block3, err = getBlocklibBlock("./mock/mvcc_read_conflict.pb")
	if err != nil {
		log.Error(err)
	}

	os.Exit(m.Run())
}

func readTxsFromBlock(pathToBlock string) ([]Tx, error) {
	file, err := ioutil.ReadFile(pathToBlock)
	if err != nil {
		return nil, err
	}

	fabBlock := &common.Block{}

	err = proto.Unmarshal(file, fabBlock)
	if err != nil {
		return nil, err
	}

	block, err := FromFabricBlock(fabBlock)
	if err != nil {
		return nil, err
	}

	txs, err := block.Txs()

	return txs, err
}

func getBlocklibBlock(pathToBlock string) (*Block, error) {
	file, err := ioutil.ReadFile(pathToBlock)
	if err != nil {
		return nil, err
	}

	fabBlock := &common.Block{}

	err = proto.Unmarshal(file, fabBlock)
	if err != nil {
		return nil, err
	}

	return FromFabricBlock(fabBlock)
}

// func readConfigBlock(pathToBlock string) ([]Tx, error) {
// 	file, err := ioutil.ReadFile(pathToBlock)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	fabBlock := &common.Block{}
//
// 	err = proto.Unmarshal(file, fabBlock)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	block, err := FromFabricBlock(fabBlock)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	txs, err := block.Txs()
//
// 	return txs, err
// }

func TestCheckIntegrity(t *testing.T) {
	t.Parallel()

	t.Run("check valid", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, true, CheckIntegrity(block1, block2))
	})
	t.Run("check invalid", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, false, CheckIntegrity(block1, block3))
	})
}

func TestIsValid(t *testing.T) {
	t.Parallel()

	t.Run("check valid", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, true, tx.IsValid())
	})
	t.Run("check invalid", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, false, invalidtx.IsValid())
	})
}

func TestValidationCode(t *testing.T) {
	t.Parallel()

	t.Run("check code 0", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, int32(0), tx.ValidationCode())
	})
	t.Run("check code 11", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, int32(11), invalidtx.ValidationCode())
	})
}

func TestValidationStatus(t *testing.T) {
	t.Parallel()

	t.Run("check VALID status", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "VALID", tx.ValidationStatus())
	})
	t.Run("check MVCC_READ_CONFLICT status", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "MVCC_READ_CONFLICT", invalidtx.ValidationStatus())
	})
}

func TestEnvelope(t *testing.T) {
	t.Parallel()

	envelope, err := tx.Envelope()
	assert.NoError(t, err)
	assert.NotNil(t, envelope.Payload)
	assert.NotNil(t, envelope.Signature)
}

func TestPayload(t *testing.T) {
	t.Parallel()

	payload, err := tx.Payload()
	assert.NoError(t, err)
	assert.NotNil(t, payload.Header)
	assert.NotNil(t, payload.Data)
}

func TestChannelHeader(t *testing.T) {
	t.Parallel()

	channelHeader, err := tx.ChannelHeader()
	assert.NoError(t, err)
	assert.NotNil(t, channelHeader)
}

func TestSignatureHeader(t *testing.T) {
	t.Parallel()

	signatureHeader, err := tx.SignatureHeader()
	assert.NoError(t, err)
	assert.NotNil(t, signatureHeader)
}

func TestCreator(t *testing.T) {
	t.Parallel()

	mspid, cert, err := tx.Creator()
	assert.NoError(t, err)

	certHash := sha256.New()
	certHash.Write(cert)
	assert.Equal(t, "Org1MSP", mspid)
	assert.Equal(
		t,
		"41202425b7c240ef2bfc3e9d48c457257b4d1fd5187a9943e3824be5c270f979",
		hex.EncodeToString(certHash.Sum(nil)),
	)
}

func TestCreatorSignatureBytes(t *testing.T) {
	t.Parallel()

	sigBytes, err := tx.CreatorSignatureBytes()
	assert.NoError(t, err)
	assert.Equal(
		t,
		"30440220629f5638a935d6d2be318852da6d4dc779e5acfb3e9fae175f1f496dcc10350a022054e918b3a4fa001c0ce56a2992b8bc7bcb4733b3ea1e122393fc5e0fd92fde30",
		hex.EncodeToString(sigBytes),
	)
}

func TestCreatorSignatureHexString(t *testing.T) {
	t.Parallel()

	sigHex, err := tx.CreatorSignatureHexString()
	assert.NoError(t, err)
	assert.Equal(
		t,
		"30440220629f5638a935d6d2be318852da6d4dc779e5acfb3e9fae175f1f496dcc10350a022054e918b3a4fa001c0ce56a2992b8bc7bcb4733b3ea1e122393fc5e0fd92fde30",
		sigHex,
	)
}

func TestChaincodeId(t *testing.T) {
	t.Parallel()

	id, err := tx.ChaincodeID()
	assert.NoError(t, err)
	assert.Equal(t, "fabcar", id.Name)
}

func TestEpoch(t *testing.T) {
	t.Parallel()

	epoch, err := tx.Epoch()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), epoch)
}

func TestTimestamp(t *testing.T) {
	t.Parallel()

	timestamp, err := tx.Timestamp()
	assert.NoError(t, err)
	assert.Equal(t, int64(1603659829097237404), timestamp.UnixNano())
}

func TestTxId(t *testing.T) {
	t.Parallel()

	txid, err := tx.TxID()
	assert.NoError(t, err)
	assert.Equal(t, "23e7c409b6849a71e6b5d7767a4e6c7efcd4bafba02b932ca5e6559e4d050dea", txid)
}

func TestPeerTransaction(t *testing.T) {
	t.Parallel()

	peerTransaction, err := tx.PeerTransaction()
	assert.NoError(t, err)
	assert.NotNil(t, peerTransaction.Actions)
}

func TestActions(t *testing.T) {
	t.Parallel()

	actions, err := tx.Actions()
	assert.NoError(t, err)

	for _, action := range actions {
		payloadHash := sha256.New()
		payloadHash.Write(action.Payload.ChaincodeProposalPayload)

		creatorHash := sha256.New()
		creatorHash.Write(action.SignatureHeader.Creator)
		assert.Equal(
			t,
			"51353a437c811a1ec7d4ffe061d1f38907ad443c5c1847b4877c4a65c5efa24e",
			hex.EncodeToString(payloadHash.Sum(nil)),
		)
		assert.Equal(
			t,
			"3b2106648e7b0773db03d160dbfef48a514f0871f8e18524a10a2de19fb21dd9",
			hex.EncodeToString(creatorHash.Sum(nil)),
		)
		assert.Equal(t, uint64(2779780183085072792), binary.BigEndian.Uint64(action.SignatureHeader.Nonce))
	}
}

func TestConfigUpdate(t *testing.T) {
	t.Parallel()

	update, err := configUpdateTx.ConfigUpdate()
	assert.NoError(t, err)
	assert.Equal(t, "mychannel", update.ChannelId)
	assert.Equal(
		t,
		"0a1b0a1670656572302e6f7267322e6578616d706c652e636f6d10db46",
		hex.EncodeToString(update.WriteSet.Groups["Application"].
			Groups["Org2MSP"].
			Values["AnchorPeers"].Value),
	)
}

func TestConfigEnvelope(t *testing.T) {
	t.Parallel()

	envelope, err := configtx.ConfigEnvelope()
	assert.NoError(t, err)
	assert.NotNil(t, envelope.Config)
	assert.NotNil(t, envelope.LastUpdate)
}

func TestConfigSequence(t *testing.T) {
	t.Parallel()

	sequence, err := configtx.ConfigSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), sequence)
}

func TestConfigEnvelopeLastUpdatePayload(t *testing.T) {
	t.Parallel()

	payload, err := configtx.ConfigEnvelopeLastUpdatePayload()
	assert.NoError(t, err)
	assert.NotNil(t, payload.Data)
	assert.NotNil(t, payload.Header)
}

func TestConfigGroup(t *testing.T) {
	t.Parallel()

	configGroup, err := configtx.ConfigGroup()
	assert.NoError(t, err)
	assert.NotNil(t, configGroup)
}

func TestCfgEnvLastUpdateCreatorSignatureBytes(t *testing.T) {
	t.Parallel()

	sigbytes, err := configtx.CfgEnvLastUpdateCreatorSignatureBytes()
	assert.NoError(t, err)
	assert.Equal(
		t,
		"3044022010972f8b345756ab1a6da4e869a7465a8b5bb7fe60df04ff0f22711d1343f1fa022035153835fde7606448c2cda034d359d52c0b9b80540d718d8fafeb4170fbff5f",
		hex.EncodeToString(sigbytes),
	)
}

func TestCfgEnvLastUpdateCreatorSignatureHex(t *testing.T) {
	t.Parallel()

	sighex, err := configtx.CfgEnvLastUpdateCreatorSignatureHex()
	assert.NoError(t, err)
	assert.Equal(
		t,
		"3044022010972f8b345756ab1a6da4e869a7465a8b5bb7fe60df04ff0f22711d1343f1fa022035153835fde7606448c2cda034d359d52c0b9b80540d718d8fafeb4170fbff5f",
		sighex,
	)
}
