/*
Copyright LLC Newity. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package blocklib

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"fmt"
	"math/big"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric-sdk-go/pkg/client/ledger"
	"github.com/hyperledger/fabric-sdk-go/pkg/fab/resource"
	"github.com/newity/crawler/blocklib/smartbft"
	bftcommon "github.com/newity/crawler/blocklib/smartbft/common"
	"github.com/pkg/errors"
)

// Block contains all the necessary information about the blockchain block.
type Block struct {
	Data       [][]byte
	number     uint64
	signatures []BlockSignature
	prevhash   []byte
	datahash   []byte
	headerhash []byte
	Metadata   [][]byte
	txsFilter  []uint8
	isconfig   bool
}

// BlockSignature contains nonce, cert, MSP ID and
// signature of the orderer which signed the block.
type BlockSignature struct {
	Cert      []byte // pem-encoded
	MSPID     string
	Signature []byte
	Nonce     []byte
}

type BFTSerializedIdentity struct {
	ConsenterID uint64
	Identity    msp.SerializedIdentity
}

// FromFabricBlock converts common.Block to blocklib.Block.
// Such conversion is necessary for further comfortable work with information from the block.
func FromFabricBlock(block *common.Block) (*Block, error) {
	metadata := &common.Metadata{}

	err := proto.Unmarshal(
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES],
		metadata,
	)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"error unmarshaling metadata from block at index [%s]",
			common.BlockMetadataIndex_SIGNATURES,
		)
	}

	blockSignatures := make([]BlockSignature, 0, len(metadata.Signatures))

	for _, metadataSignature := range metadata.Signatures {
		sigHdr := &common.SignatureHeader{}
		if err = proto.Unmarshal(metadataSignature.SignatureHeader, sigHdr); err != nil {
			return nil, errors.Wrap(err, "error unmarshaling SignatureHeader")
		}

		creator := &msp.SerializedIdentity{}
		if err = proto.Unmarshal(sigHdr.Creator, creator); err != nil {
			return nil, err
		}

		blockSignatures = append(blockSignatures,
			BlockSignature{
				Cert:      creator.IdBytes,
				MSPID:     creator.Mspid,
				Signature: metadataSignature.Signature,
				Nonce:     sigHdr.Nonce,
			},
		)
	}

	envelope := &common.Envelope{}
	if err = proto.Unmarshal(block.Data.Data[0], envelope); err != nil {
		return nil, err
	}

	payload := &common.Payload{}

	err = proto.Unmarshal(envelope.Payload, payload)
	if err != nil {
		return nil, err
	}

	hdr := &common.ChannelHeader{}

	err = proto.Unmarshal(payload.Header.ChannelHeader, hdr)
	if err != nil {
		return nil, err
	}

	headerHash := sha256.Sum256(BlockHeaderBytes(block.Header))

	filter := block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
	if filter == nil {
		filter = block.Metadata.Metadata[1] // for HLF 1.x compatibility
	}

	return &Block{
		Data:       block.Data.Data,
		number:     block.Header.Number,
		signatures: blockSignatures,
		Metadata:   block.Metadata.Metadata,
		prevhash:   block.Header.PreviousHash,
		datahash:   block.Header.DataHash,
		headerhash: headerHash[:],
		txsFilter:  filter,
		isconfig: common.HeaderType(hdr.Type) == common.HeaderType_CONFIG ||
			common.HeaderType(hdr.Type) == common.HeaderType_ORDERER_TRANSACTION,
	}, nil
}

// CheckIntegrity checks that two blocks are 'connected'.
// This func compares current block header 'PreviousHash'
// (hash of block number, data and PreviousHash -
// think of it as a link to the previous block)
// with actual previous block header hash.
func CheckIntegrity(previousblock, currentblock *Block) bool {
	return bytes.Equal(previousblock.headerhash, currentblock.prevhash)
}

// FromBFTFabricBlock converts common.Block produced by BFT-orderer to blocklib.Block.
func FromBFTFabricBlock(cli *ledger.Client, block *common.Block) (*Block, error) {
	metadata := &common.Metadata{}

	err := proto.Unmarshal(
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES],
		metadata,
	)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"error unmarshaling metadata from block at index [%s]",
			common.BlockMetadataIndex_SIGNATURES,
		)
	}

	identities, err := GetBFTOrderersIdentities(cli, block)
	if err != nil {
		return nil, err
	}

	var blockSignatures []BlockSignature

	for _, metadataSignature := range metadata.Signatures {
		sigHdr := &common.SignatureHeader{}
		if err = proto.Unmarshal(metadataSignature.SignatureHeader, sigHdr); err != nil {
			return nil, errors.Wrap(err, "error unmarshaling SignatureHeader")
		}

		creator := &msp.SerializedIdentity{}

		if err = proto.Unmarshal(sigHdr.Creator, creator); err != nil {
			return nil, err
		}

		for _, identity := range identities {
			blockSignatures = append(blockSignatures,
				BlockSignature{
					Cert:      identity.Identity.IdBytes,
					MSPID:     identity.Identity.Mspid,
					Signature: metadataSignature.Signature,
					Nonce:     sigHdr.Nonce,
				},
			)
		}
	}

	envelope := &common.Envelope{}
	if err = proto.Unmarshal(block.Data.Data[0], envelope); err != nil {
		return nil, err
	}

	payload := &common.Payload{}

	err = proto.Unmarshal(envelope.Payload, payload)
	if err != nil {
		return nil, err
	}

	hdr := &common.ChannelHeader{}

	err = proto.Unmarshal(payload.Header.ChannelHeader, hdr)
	if err != nil {
		return nil, err
	}

	headerHash := sha256.Sum256(BlockHeaderBytes(block.Header))

	filter := block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
	if filter == nil {
		filter = block.Metadata.Metadata[1] // for HLF 1.x compatibility
	}

	return &Block{
		Data:       block.Data.Data,
		number:     block.Header.Number,
		signatures: blockSignatures,
		Metadata:   block.Metadata.Metadata,
		prevhash:   block.Header.PreviousHash,
		datahash:   block.Header.DataHash,
		headerhash: headerHash[:],
		txsFilter:  filter,
		isconfig: common.HeaderType(hdr.Type) == common.HeaderType_CONFIG ||
			common.HeaderType(hdr.Type) == common.HeaderType_ORDERER_TRANSACTION,
	}, nil
}

func FromBFTFabricBlockWithoutOrdererIdenities(block *common.Block) (*Block, error) {
	metadata := &common.Metadata{}

	err := proto.Unmarshal(
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES],
		metadata,
	)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"error unmarshaling metadata from block at index [%s]",
			common.BlockMetadataIndex_SIGNATURES,
		)
	}

	blockSignatures := make([]BlockSignature, len(metadata.Signatures))

	for _, metadataSignature := range metadata.Signatures {
		sigHdr := &common.SignatureHeader{}
		if err = proto.Unmarshal(metadataSignature.SignatureHeader, sigHdr); err != nil {
			return nil, errors.Wrap(err, "error unmarshaling SignatureHeader")
		}

		creator := &msp.SerializedIdentity{}
		if err = proto.Unmarshal(sigHdr.Creator, creator); err != nil {
			return nil, err
		}

		blockSignatures = append(blockSignatures,
			BlockSignature{
				Cert:      nil,
				MSPID:     "",
				Signature: metadataSignature.Signature,
				Nonce:     sigHdr.Nonce,
			},
		)
	}

	envelope := &common.Envelope{}
	if err = proto.Unmarshal(block.Data.Data[0], envelope); err != nil {
		return nil, err
	}

	payload := &common.Payload{}

	err = proto.Unmarshal(envelope.Payload, payload)
	if err != nil {
		return nil, err
	}

	hdr := &common.ChannelHeader{}

	err = proto.Unmarshal(payload.Header.ChannelHeader, hdr)
	if err != nil {
		return nil, err
	}

	headerHash := sha256.Sum256(BlockHeaderBytes(block.Header))

	filter := block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
	if filter == nil {
		filter = block.Metadata.Metadata[1] // for HLF 1.x compatibility
	}

	return &Block{
		Data:       block.Data.Data,
		number:     block.Header.Number,
		signatures: blockSignatures,
		Metadata:   block.Metadata.Metadata,
		prevhash:   block.Header.PreviousHash,
		datahash:   block.Header.DataHash,
		headerhash: headerHash[:],
		txsFilter:  filter,
		isconfig: common.HeaderType(hdr.Type) == common.HeaderType_CONFIG ||
			common.HeaderType(hdr.Type) == common.HeaderType_ORDERER_TRANSACTION,
	}, nil
}

func GetBFTOrderersIdentities(
	cli *ledger.Client,
	blk *common.Block,
) ([]BFTSerializedIdentity, error) {
	if blk.Metadata == nil {
		return nil, errors.New("no metadata in block")
	}

	index := int(common.BlockMetadataIndex_SIGNATURES)

	if len(blk.Metadata.Metadata) <= index {
		return nil, fmt.Errorf("no metadata at index [%d]", index)
	}

	md := new(bftcommon.BFTMetadata)
	if err := proto.Unmarshal(blk.Metadata.Metadata[index], md); err != nil {
		return nil, fmt.Errorf("couldn't unmarshal block metadata: %w", err)
	}

	lc := common.LastConfig{}

	if err := proto.Unmarshal(md.Value, &lc); err == nil {
		return nil, err
	}

	cfgBlock, err := cli.QueryBlock(lc.Index)
	if err != nil {
		return nil, err
	}

	configEnvelope, err := resource.CreateConfigEnvelope(cfgBlock.Data.Data[0])
	if err != nil {
		return nil, err
	}

	identities, err := getOrderersIdentities(configEnvelope)
	if err != nil {
		return nil, fmt.Errorf("couldn't extract orderers identities: %w", err)
	}

	// filter 'identities' slice (all channel orderers) to find those which signed this block
	var filteredIdentities []BFTSerializedIdentity

	for _, identity := range identities {
		for _, signature := range md.Signatures {
			if identity.ConsenterID == signature.SignerId {
				filteredIdentities = append(filteredIdentities, identity)
			}
		}
	}

	return filteredIdentities, nil
}

func getOrderersIdentities(envelope *common.ConfigEnvelope) ([]BFTSerializedIdentity, error) {
	consensusType := envelope.Config.
		ChannelGroup.Groups["Orderer"].
		Values["ConsensusType"].Value

	ct := &orderer.ConsensusType{}

	err := proto.Unmarshal(consensusType, ct)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshaling consensus type from consensusType: %w", err)
	}

	m := &smartbft.ConfigMetadata{}

	err = proto.Unmarshal(ct.Metadata, m)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshaling config metadata from metadata: %w", err)
	}

	identity := msp.SerializedIdentity{}
	identities := make([]BFTSerializedIdentity, 0, len(m.Consenters))

	for _, consenter := range m.Consenters {
		if err = proto.Unmarshal(consenter.Identity, &identity); err != nil {
			return nil, err
		}

		identities = append(
			identities,
			BFTSerializedIdentity{
				consenter.ConsenterId,
				identity,
			},
		)
	}

	return identities, nil
}

// IsConfig returns a boolean value that indicates whether the block is a configuration block.
func (b *Block) IsConfig() bool {
	return b.isconfig
}

// Number returns a block number.
func (b *Block) Number() uint64 {
	return b.number
}

// PreviousHash returns hash of the previous block.
func (b *Block) PreviousHash() []byte {
	return b.prevhash
}

// DataHash returns hash of this block's data.
func (b *Block) DataHash() []byte {
	return b.datahash
}

// HeaderHash returns hash of this block's header.
func (b *Block) HeaderHash() []byte {
	return b.headerhash
}

// OrderersSignatures returns signatures of orderers, their cert, MSP ID and nonce.
func (b *Block) OrderersSignatures() []BlockSignature {
	return b.signatures
}

func (b *Block) Txs() ([]Tx, error) {
	var (
		validationCode   int32 = 255
		validationStatus       = "INVALID_OTHER_REASON"
	)

	txs := make([]Tx, 0, len(b.Data))

	for txNumber, data := range b.Data {
		if b.IsConfig() {
			txs = append(txs, Tx{Data: data, validationCode: 0, validationStatus: ""})
		} else {
			for _, code := range peer.TxValidationCode_value {
				if b.txsFilter[txNumber] == uint8(code) {
					validationCode = code
					validationStatus = peer.TxValidationCode_name[code]
				}
			}
			txs = append(txs, Tx{Data: data, validationCode: validationCode, validationStatus: validationStatus})
		}
	}

	return txs, nil
}

// TxsFromOrdererBlock returns transactions from orderer block
// without validation code and validation status,
// because odrerer ledger does not contain them.
func (b *Block) TxsFromOrdererBlock() ([]Tx, error) {
	txs := make([]Tx, 0, len(b.Data))

	for _, data := range b.Data {
		txs = append(txs, Tx{Data: data})
	}

	return txs, nil
}

// LastConfig returns last configuration block index for provided block.
func (b *Block) LastConfig() (uint64, error) {
	lastConfig := &common.LastConfig{}
	err := proto.Unmarshal(b.Metadata[common.BlockMetadataIndex_LAST_CONFIG], lastConfig)

	return lastConfig.Index, err
}

func GetTx(block *common.Block, txNumber int) *Tx {
	txsFilter := TxValidationFlags(
		block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER],
	)

	var (
		validationCode   int32 = 255
		validationStatus       = "INVALID_OTHER_REASON"
	)

	for _, code := range peer.TxValidationCode_value {
		if txsFilter[txNumber] == uint8(code) {
			validationCode = code
			validationStatus = peer.TxValidationCode_name[code]
		}
	}

	return &Tx{
		Data:             block.Data.Data[txNumber],
		validationCode:   validationCode,
		validationStatus: validationStatus,
	}
}

type asn1Header struct {
	Number       *big.Int
	PreviousHash []byte
	DataHash     []byte
}

func BlockHeaderBytes(b *common.BlockHeader) []byte {
	ah := asn1Header{
		PreviousHash: b.PreviousHash,
		DataHash:     b.DataHash,
		Number:       new(big.Int).SetUint64(b.Number),
	}

	result, err := asn1.Marshal(ah)
	if err != nil {
		// Errors should only arise for types which cannot be encoded, since the
		// BlockHeader type is known a-priori to contain only encodable types, an
		// error here is fatal and should not be propogated
		panic(err)
	}

	return result
}
