package serdes

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
)

// ProtobufDeserializer using the schema registry client
type ProtobufDeserializer struct {
}

// NewProtobufDeserializer returns a new ProtobufDeserializer
func NewProtobufDeserializer() *ProtobufDeserializer {
	return &ProtobufDeserializer{}
}

// Deserialize using the Confluent Schema Registry wire format
func (ps *ProtobufDeserializer) Deserialize(bytes []byte, pb proto.Message) error {
	const (
		wireFormatLen = 5 // magic byte + schema ID
		minBytesLen   = 6 // SR wire protocol + msg_index length
		magicByte     = byte(0)
	)

	if len(bytes) < minBytesLen {
		return fmt.Errorf("message too small. This message was not produced with a Confluent Schema Registry serializer")
	}

	if bytes[0] != magicByte {
		return fmt.Errorf("unknown magic byte. This message was not produced with a Confluent Schema Registry serializer")
	}

	// decode the number of elements in the array of message indexes
	arrayLen, bytesRead := binary.Varint(bytes[wireFormatLen:])
	const msgIndexErrMsg = "unable to decode message index array"
	if arrayLen < 0 {
		return fmt.Errorf(msgIndexErrMsg)
	}
	if bytesRead <= 0 {
		return fmt.Errorf(msgIndexErrMsg)
	}
	totalBytesRead := bytesRead
	msgIndexArray := make([]int64, arrayLen)
	// iterate arrayLen times, decoding another varint
	for i := int64(0); i < arrayLen; i++ {
		idx, bytesRead := binary.Varint(bytes[wireFormatLen+totalBytesRead:])
		if bytesRead <= 0 {
			err := fmt.Errorf("unable to decode value in message index array")
			return err
		}
		totalBytesRead += bytesRead
		msgIndexArray[i] = idx
	}
	// Protobuf Messages are self-describing; no need to query schema
	// Move the reader cursor past the index
	err := proto.Unmarshal(bytes[wireFormatLen+totalBytesRead:], pb)
	if err != nil {
		return err
	}
	return nil
}
