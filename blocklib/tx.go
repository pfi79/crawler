/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package blocklib

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
)

type TxValidationFlags []uint8

type Tx struct {
	Data             []byte
	validationCode   int32
	validationStatus string
}

// IsValid checks if transaction with specified number (txNumber int)
// in block (block *common.Block) is valid
// or not and returns corresponding bool value.
func (tx *Tx) IsValid() bool {
	return tx.validationCode == 0
}

// ValidationCode returns validation code for transaction
// with specified number (txNumber int) in block (block *common.Block).
func (tx *Tx) ValidationCode() int32 {
	return tx.validationCode
}

// ValidationStatus returns string representation of validation code
// for transaction with specified number (txNumber int)
// in block (block *common.Block).
func (tx *Tx) ValidationStatus() string {
	return tx.validationStatus
}

// Envelope returns pointer to common.Envelope.
// common.Envelope contains payload with a signature.
func (tx *Tx) Envelope() (*common.Envelope, error) {
	envelope := &common.Envelope{}
	if err := proto.Unmarshal(tx.Data, envelope); err != nil {
		return nil, err
	}

	return envelope, nil
}

// ConfigUpdate extracts configuration transaction info from raw transaction.
func (tx *Tx) ConfigUpdate() (*common.ConfigUpdate, error) {
	env, err := tx.ConfigEnvelope()
	if err != nil {
		return nil, err
	}

	payload := &common.Payload{}
	if err = proto.Unmarshal(env.LastUpdate.Payload, payload); err != nil {
		return nil, err
	}

	updateEnv := &common.ConfigUpdateEnvelope{}
	if err = proto.Unmarshal(payload.Data, updateEnv); err != nil {
		return nil, err
	}

	update := &common.ConfigUpdate{}
	if err = proto.Unmarshal(updateEnv.ConfigUpdate, update); err != nil {
		return nil, err
	}

	return update, nil
}

// ConfigEnvelope returns pointer to common.ConfigEnvelope.
// common.Envelope contains config and last update payload.
func (tx *Tx) ConfigEnvelope() (*common.ConfigEnvelope, error) {
	envelope, err := tx.Envelope()
	if err != nil {
		return nil, err
	}

	payload := &common.Payload{}
	if err = proto.Unmarshal(envelope.Payload, payload); err != nil {
		return nil, err
	}

	configEnvelope := &common.ConfigEnvelope{}
	if err = proto.Unmarshal(payload.Data, configEnvelope); err != nil {
		return nil, err
	}

	return configEnvelope, nil
}

// ConfigSequence returns sequence number of config.
func (tx *Tx) ConfigSequence() (uint64, error) {
	configEnvelope, err := tx.ConfigEnvelope()
	if err != nil {
		return 0, err
	}

	return configEnvelope.Config.Sequence, nil
}

// ConfigGroup returns a pointer to common.ConfigGroup.
// ConfigGroup is the hierarchical data structure for holding config.
func (tx *Tx) ConfigGroup() (*common.ConfigGroup, error) {
	configEnvelope, err := tx.ConfigEnvelope()
	if err != nil {
		return nil, err
	}

	return configEnvelope.Config.ChannelGroup, nil
}

// ConfigEnvelopeLastUpdatePayload payload of last config update.
func (tx *Tx) ConfigEnvelopeLastUpdatePayload() (*common.Payload, error) {
	configEnvelope, err := tx.ConfigEnvelope()
	if err != nil {
		return nil, err
	}

	payload := &common.Payload{}

	err = proto.Unmarshal(configEnvelope.LastUpdate.Payload, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// CfgEnvLastUpdateCreatorSignatureBytes extracts signature
// of transaction creator as bytes slice.
func (tx *Tx) CfgEnvLastUpdateCreatorSignatureBytes() ([]byte, error) {
	configEnvelope, err := tx.ConfigEnvelope()
	if err != nil {
		return nil, err
	}

	return configEnvelope.LastUpdate.Signature, nil
}

// CfgEnvLastUpdateCreatorSignatureHex extracts signature
// of transaction creator as hex-encoded string.
func (tx *Tx) CfgEnvLastUpdateCreatorSignatureHex() (string, error) {
	configEnvelope, err := tx.ConfigEnvelope()
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(configEnvelope.LastUpdate.Signature), nil
}

// Payload returns pointer to common.Payload.
// common.Payload is the message contents (and header to allow for signing).
func (tx *Tx) Payload() (*common.Payload, error) {
	envelope, err := tx.Envelope()
	if err != nil {
		return nil, err
	}

	payload := &common.Payload{}

	err = proto.Unmarshal(envelope.Payload, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// ChannelHeader returns pointer to common.ChannelHeader of the transaction.
func (tx *Tx) ChannelHeader() (*common.ChannelHeader, error) {
	payload, err := tx.Payload()
	if err != nil {
		return nil, err
	}

	chdr := &common.ChannelHeader{}

	err = proto.Unmarshal(payload.Header.ChannelHeader, chdr)
	if err != nil {
		return nil, err
	}

	return chdr, nil
}

// SignatureHeader returns pointer to common.SignatureHeader
// that contains nonce and creator (msp.SerializedIdentity).
func (tx *Tx) SignatureHeader() (*common.SignatureHeader, error) {
	payload, err := tx.Payload()
	if err != nil {
		return nil, err
	}

	sighdr := &common.SignatureHeader{}

	err = proto.Unmarshal(payload.Header.SignatureHeader, sighdr)
	if err != nil {
		return nil, err
	}

	return sighdr, nil
}

// Creator can be used to extract tx creator's MSP ID and PEM-encoded certificate.
func (tx *Tx) Creator() (string, []byte, error) {
	sighdr, err := tx.SignatureHeader()
	if err != nil {
		return "", nil, err
	}

	identity := &msp.SerializedIdentity{}
	err = proto.Unmarshal(sighdr.Creator, identity)

	return identity.Mspid, identity.IdBytes, err
}

// CreatorSignatureBytes extracts signature of transaction creator as bytes slice.
func (tx *Tx) CreatorSignatureBytes() ([]byte, error) {
	envelope, err := tx.Envelope()
	if err != nil {
		return nil, err
	}

	return envelope.Signature, err
}

// CreatorSignatureHexString extracts signature
// of transaction creator as hex-encoded string.
func (tx *Tx) CreatorSignatureHexString() (string, error) {
	envelope, err := tx.Envelope()
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(envelope.Signature), err
}

// ChaincodeID returns peer.ChaincodeID (name, version and path)
// of the target chaincode
// (only if it's type is common.HeaderType_ENDORSER_TRANSACTION).
func (tx *Tx) ChaincodeID() (*peer.ChaincodeID, error) {
	chhdr, err := tx.ChannelHeader()
	if err != nil {
		return nil, err
	}

	if chhdr.Type == int32(common.HeaderType_ENDORSER_TRANSACTION) {
		ccHeaderExt := &peer.ChaincodeHeaderExtension{}
		err = proto.Unmarshal(chhdr.Extension, ccHeaderExt)

		return ccHeaderExt.ChaincodeId, err
	}

	return nil, fmt.Errorf(
		"not an endorser transaction type (type code %d)",
		chhdr.Type,
	)
}

// Epoch returns the epoch in which this header was generated,
// where epoch is defined based on block height
// Epoch in which the response has been generated. This field identifies a
// logical window of time. A proposal response is accepted by a peer only if
// two conditions hold:
// 1. the epoch specified in the message is the current epoch
// 2. this message has been only seen once during this epoch
// (i.e. it hasn't been replayed)
//
// Always equals to 0 because of this reason:
// https://github.com/hyperledger/fabric/blob/release-2.1/core/common/validation/msgvalidation.go#L110
func (tx *Tx) Epoch() (uint64, error) {
	chhdr, err := tx.ChannelHeader()

	return chhdr.Epoch, err
}

// Timestamp returns the local time when the message was created by the sender.
func (tx *Tx) Timestamp() (time.Time, error) {
	chhdr, err := tx.ChannelHeader()
	if err != nil {
		return time.Time{}, err
	}

	return ptypes.Timestamp(chhdr.Timestamp)
}

// TLSCertHash returns hash of the client's TLS certificate
// (if mutual TLS is employed).
func (tx *Tx) TLSCertHash() ([]byte, error) {
	chhdr, err := tx.ChannelHeader()

	return chhdr.TlsCertHash, err
}

// TxID returns transaction ID.
func (tx *Tx) TxID() (string, error) {
	chhdr, err := tx.ChannelHeader()

	return chhdr.TxId, err
}

// PeerTransaction returns pointer to peer.Transaction.
// The transaction to be sent to the ordering service.
// A transaction contains one or more TransactionAction.
// Each TransactionAction binds a proposal to potentially multiple actions.
// The transaction is atomic meaning that either all actions in
// the transaction will be committed or none will.
// Note that while a Transaction might include more than one Header,
// the Header.creator field must be the same in each.
// A single client is free to issue a number of independent Proposal,
// each with their header (Header) and request payload (ChaincodeProposalPayload).
// Each proposal is independently endorsed generating an action
// (ProposalResponsePayload) with one signature per Endorser.
// Any number of independent proposals (and their action) might be included in
// a transaction to ensure that they are treated atomically.
func (tx *Tx) PeerTransaction() (*peer.Transaction, error) {
	payload, err := tx.Payload()
	if err != nil {
		return nil, err
	}

	transaction := &peer.Transaction{}

	if err = proto.Unmarshal(payload.Data, transaction); err != nil {
		return nil, err
	}

	return transaction, nil
}

// Actions - GetTransaction returns slice of the Action structs.
func (tx *Tx) Actions() ([]Action, error) {
	transaction, err := tx.PeerTransaction()
	if err != nil {
		return nil, err
	}

	actions := make([]Action, 0, len(transaction.Actions))

	for _, act := range transaction.Actions {
		ccActionPayload := &peer.ChaincodeActionPayload{}
		if err = proto.Unmarshal(act.Payload, ccActionPayload); err != nil {
			return nil, err
		}

		ccActionSignatureHeader := &common.SignatureHeader{}
		if err = proto.Unmarshal(act.Header, ccActionSignatureHeader); err != nil {
			return nil, err
		}

		actions = append(actions, Action{Payload: ccActionPayload, SignatureHeader: ccActionSignatureHeader})
	}

	return actions, nil
}
