/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package blocklib

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/stretchr/testify/assert"
)

func GetActionFromBlock(pathToBlock string) (*Action, error) {
	txsvalid, err := readTxsFromBlock(pathToBlock)
	if err != nil {
		return nil, err
	}

	t := txsvalid[0]

	actions, err := t.Actions()
	if err != nil {
		return nil, err
	}

	return &actions[0], nil
}

func TestChaincodeActionPayload(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	ccActPayload := action.ChaincodeActionPayload()
	assert.NotNil(t, ccActPayload.Action)
	assert.NotNil(t, ccActPayload.ChaincodeProposalPayload)
}

func TestEndorsements(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)

	endorsements := action.Endorsements()

	identity1 := &msp.SerializedIdentity{}
	assert.NoError(t, proto.Unmarshal(endorsements[0].Endorser, identity1))
	assert.Equal(t, "Org1MSP", identity1.Mspid)
	assert.Equal(
		t,
		"30450221008b06e091f41a1e49d0876fca1c4afbbde7f3b4ae18fbca7a64502f4c7bea074a02206feb4a205e50203e214fccf1b8621c52d78340cdceb92a9f3c826c25174d4f44",
		hex.EncodeToString(endorsements[0].Signature),
	)

	identity2 := &msp.SerializedIdentity{}
	assert.NoError(t, proto.Unmarshal(endorsements[1].Endorser, identity2))
	assert.Equal(t, "Org2MSP", identity2.Mspid)
	assert.Equal(
		t,
		"3045022100989c1757cbf822723a670e82e9e1a3af2da33e1b6831b8521a36dc612347d50802207ccab24b18c1817746c683026a1d80553476f2da01fea1dcf3e3e7f678a00ad7",
		hex.EncodeToString(endorsements[1].Signature),
	)
}

func TestCreatorMSPID(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	creatorMSPID, err := action.CreatorMSPID()
	assert.NoError(t, err)
	assert.Equal(t, "Org1MSP", creatorMSPID)
}

func TestCreatorCertBytes(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	certBytes, err := action.CreatorCertBytes()
	assert.NoError(t, err)

	certHash := sha256.New()
	certHash.Write(certBytes)

	assert.Equal(
		t,
		"41202425b7c240ef2bfc3e9d48c457257b4d1fd5187a9943e3824be5c270f979",
		hex.EncodeToString(certHash.Sum(nil)),
	)
}

func TestCreatorCertHashHex(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	cert, err := action.CreatorCertHashHex()
	assert.NoError(t, err)
	assert.Equal(t, "41202425b7c240ef2bfc3e9d48c457257b4d1fd5187a9943e3824be5c270f979", cert)
}

func TestChaincodeProposalPayload(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	ccPropRespPayload, err := action.ChaincodeProposalPayload()
	assert.NoError(t, err)
	assert.NotNil(t, ccPropRespPayload.Input)
}

func TestChaincodeInput(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	input, err := action.ChaincodeInput()
	assert.NoError(t, err)
	assert.Equal(t, []string{"createCar", "CAR11", "VW", "Polo", "Grey", "Mary"}, input)
}

func TestDecorations(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	decor, err := action.Decorations()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]byte(nil), decor)
}

func TestIsInit(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	isinit, err := action.IsInit()
	assert.NoError(t, err)
	assert.Equal(t, false, isinit)
}

func TestProposalResponsePayload(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	propRespPayload, err := action.ProposalResponsePayload()
	assert.NoError(t, err)
	assert.NotNil(t, propRespPayload.Extension)
	assert.NotNil(t, propRespPayload.ProposalHash)
}

func TestProposalHash(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/sampleblock.pb")
	assert.NoError(t, err)
	hash, err := action.ProposalHash()
	assert.NoError(t, err)
	assert.Equal(
		t,
		"d61815db10b7b0f61ae13873c81076481d573aa8265457f85abea0332f5e007b",
		hex.EncodeToString(hash),
	)
}

func TestChaincodeAction(t *testing.T) {
	t.Parallel()

	action, err := GetActionFromBlock("./mock/genesis.pb")
	assert.NoError(t, err)
	ccAction, err := action.ChaincodeAction()
	assert.NoError(t, err)
	assert.Equal(t, "lscc", ccAction.ChaincodeId.Name)
	assert.NotNil(t, ccAction.Response.Payload)
	assert.NotNil(t, ccAction.Results)
}
