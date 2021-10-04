/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package blocklib

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
)

type RwSet struct {
	NameSpace                    string                                `json:"namespace"`
	KVRWSet                      kvrwset.KVRWSet                       `json:"kv_rw_set"`
	CollectionHashedReadWriteSet []*rwset.CollectionHashedReadWriteSet `json:"collection_hashed_rw_set"`
}

type Action struct {
	Payload         *peer.ChaincodeActionPayload
	SignatureHeader *common.SignatureHeader
}

// ChaincodeActionPayload returns pointer to peer.ChaincodeActionPayload.
// ChaincodeActionPayload is the message to be used for the TransactionAction's
// payload when the Header's type is set to CHAINCODE.
// It carries the chaincodeProposalPayload and an endorsed action to apply to the ledger.
func (a *Action) ChaincodeActionPayload() *peer.ChaincodeActionPayload {
	return a.Payload
}

// Endorsements returns a slice of pointers to peer.Endorsement.
// An endorsement is a signature of an endorser over a proposal response.
// By producing an endorsement message, an endorser implicitly "approves"
// that proposal response and the actions contained therein.
// When enough endorsements have been collected, a transaction
// can be generated out of a set of proposal responses.
// Note that this message only contains an identity and a signature but no signed payload.
// This is intentional because endorsements are supposed to be collected in
// a transaction, and they are all expected to endorse a single proposal
// response/action (many endorsements over a single proposal response).
func (a *Action) Endorsements() []*peer.Endorsement {
	return a.Payload.Action.Endorsements
}

// CreatorMSPID returns MSP ID of the organization the creator belongs to.
func (a *Action) CreatorMSPID() (string, error) {
	creator := &msp.SerializedIdentity{}
	err := proto.Unmarshal(a.SignatureHeader.Creator, creator)

	return creator.Mspid, err
}

// CreatorCertBytes returns pem-encoded certificate of transaction creator.
func (a *Action) CreatorCertBytes() ([]byte, error) {
	creator := &msp.SerializedIdentity{}
	err := proto.Unmarshal(a.SignatureHeader.Creator, creator)

	return creator.IdBytes, err
}

// CreatorCertHashHex returns hex-encoded SHA256 hash of pem-encoded certificate of transaction creator.
func (a *Action) CreatorCertHashHex() (string, error) {
	creator := &msp.SerializedIdentity{}
	if err := proto.Unmarshal(a.SignatureHeader.Creator, creator); err != nil {
		return "", err
	}

	certHash := sha256.New()
	certHash.Write(creator.IdBytes)

	return hex.EncodeToString(certHash.Sum(nil)), nil
}

// ChaincodeProposalPayload returns a pointer to peer.ChaincodeProposalPayload.
// This method should be used for retrieving input or transient data of the chaincode invocation.
func (a *Action) ChaincodeProposalPayload() (*peer.ChaincodeProposalPayload, error) {
	ccProposalPayload := &peer.ChaincodeProposalPayload{}
	err := proto.Unmarshal(a.Payload.ChaincodeProposalPayload, ccProposalPayload)

	return ccProposalPayload, err
}

// ChaincodeInput retrieves chaincode input, format and returns it as string slice.
func (a *Action) ChaincodeInput() ([]string, error) {
	ccProposalPayload, err := a.ChaincodeProposalPayload()
	if err != nil {
		return nil, err
	}

	input := &peer.ChaincodeInvocationSpec{}
	err = proto.Unmarshal(ccProposalPayload.Input, input)
	args := make([]string, 0, len(input.ChaincodeSpec.Input.Args))

	for _, arg := range input.ChaincodeSpec.Input.Args {
		args = append(args, string(arg))
	}

	return args, err
}

// IsInit returns true (is Init invoked) or false (not Init invoked).
// is_init is used for the application to signal that an invocation is
// to be routed to the legacy 'Init' function for compatibility
// with chaincodes which handled Init in the old way.
// New applications should manage their initialized state themselves.
func (a *Action) IsInit() (bool, error) {
	ccProposalPayload, err := a.ChaincodeProposalPayload()
	if err != nil {
		return false, err
	}

	input := &peer.ChaincodeInvocationSpec{}
	err = proto.Unmarshal(ccProposalPayload.Input, input)

	return input.ChaincodeSpec.Input.IsInit, err
}

// TransientMap contains data (e.g. cryptographic material) that might
// be used to implement some form of application-level confidentiality.
// The contents of this field are supposed to always be omitted
// from the transaction and excluded from the ledger.
func (a *Action) TransientMap() (map[string][]byte, error) {
	ccProposalPayload, err := a.ChaincodeProposalPayload()
	if err != nil {
		return nil, err
	}

	return ccProposalPayload.TransientMap, nil
}

// Decorations returns additional data (if applicable) about the proposal
// that originated from the peer. This data is set by the decorators of the
// peer, which append or mutate the chaincode input passed to the chaincode.
//
// unfortunately decorations are always nil in the current HLF versions
// https://github.com/hyperledger/fabric/blob/master/core/endorser/support.go#L121
func (a *Action) Decorations() (map[string][]byte, error) {
	ccProposalPayload, err := a.ChaincodeProposalPayload()
	if err != nil {
		return nil, err
	}

	input := &peer.ChaincodeInvocationSpec{}

	err = proto.Unmarshal(ccProposalPayload.Input, input)
	if err != nil {
		return nil, err
	}

	return input.ChaincodeSpec.Input.Decorations, nil
}

// ProposalResponsePayload returns a pointer to peer.ProposalResponsePayload.
// ProposalResponsePayload is the payload of a proposal response.
// This message is the "bridge" between the client's request and
// the endorser's action in response to that request.
// Concretely, for chaincodes, it contains a hashed representation of
// the proposal (proposalHash) and a representation of the chaincode
// state changes and events inside the extension field.
func (a *Action) ProposalResponsePayload() (*peer.ProposalResponsePayload, error) {
	proposalResponsePayload := &peer.ProposalResponsePayload{}
	err := proto.Unmarshal(
		a.Payload.Action.ProposalResponsePayload,
		proposalResponsePayload,
	)

	return proposalResponsePayload, err
}

// ProposalHash returns SHA256 hash of common.ChannelHeader,
// common.SignatureHeader and peer.ProposalResponsePayload.
func (a *Action) ProposalHash() ([]byte, error) {
	proposalResponsePayload, err := a.ProposalResponsePayload()
	if err != nil {
		return nil, err
	}

	return proposalResponsePayload.ProposalHash, nil
}

// ChaincodeAction returns a pointer to peer.ChaincodeAction that contains and
// actions the events generated by the execution of the chaincode.
func (a *Action) ChaincodeAction() (*peer.ChaincodeAction, error) {
	proposalResponsePayload, err := a.ProposalResponsePayload()
	if err != nil {
		return nil, err
	}

	chaincodeAction := &peer.ChaincodeAction{}
	err = proto.Unmarshal(proposalResponsePayload.Extension, chaincodeAction)

	return chaincodeAction, err
}

// ChaincodeEvent returns a pointer to peer.ChaincodeEvent that contains events info.
func (a *Action) ChaincodeEvent() (*peer.ChaincodeEvent, error) {
	chaincodeAction, err := a.ChaincodeAction()
	if err != nil {
		return nil, err
	}

	chaincodeEvent := &peer.ChaincodeEvent{}
	err = proto.Unmarshal(chaincodeAction.Events, chaincodeEvent)

	return chaincodeEvent, err
}

// RWSets returns to a read-write sets slice.
func (a *Action) RWSets() ([]RwSet, error) {
	chaincodeAction, err := a.ChaincodeAction()
	if err != nil {
		return nil, err
	}

	txReadWriteSet := &rwset.TxReadWriteSet{}
	if err = proto.Unmarshal(
		chaincodeAction.Results,
		txReadWriteSet,
	); err != nil {
		return nil, err
	}

	result := make([]RwSet, len(txReadWriteSet.NsRwset))
	for i, rwSet := range txReadWriteSet.NsRwset {
		result[i].NameSpace = rwSet.Namespace
		if err = proto.Unmarshal(rwSet.Rwset, &result[i].KVRWSet); err != nil {
			return nil, err
		}

		result[i].CollectionHashedReadWriteSet = rwSet.CollectionHashedRwset
	}

	return result, err
}
