package serdes

import (
	"github.com/finncolman/kafka-go-serdes/internal/message"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"testing"
)

func TestProtobufDeserializer_Deserialize(t *testing.T) {
	nest1 := &message.Nested1{MessageId: 232}
	nest2 := &message.Nested2{Id: "sdcvcsdsd"}

	msgData := &message.MessageData{Nest1: nest1, Nest2: nest2}
	payload, err := proto.Marshal(msgData)
	if err != nil {
		t.Fatalf("unexpected error on proto.Marshal: %s", err.Error())
	}

	cases := []struct {
		name        string
		schemaBytes []byte
		want        string
	}{
		{
			"index 0 case",
			[]byte{0, 0, 0, 0, 0, 0},
			msgData.String(),
		},
		{
			"second element at top level",
			[]byte{0, 0, 0, 0, 0, 2, 2},
			msgData.String(),
		},
		{
			"third element at top level",
			[]byte{0, 0, 0, 0, 0, 2, 4},
			msgData.String(),
		},
		{
			"first nested element under first top level element",
			[]byte{0, 0, 0, 0, 0, 4, 0, 0},
			msgData.String(),
		},
		{
			"third nested element under first top level element",
			[]byte{0, 0, 0, 0, 0, 4, 0, 4},
			msgData.String(),
		},
		{
			"third nested element under third top level element",
			[]byte{0, 0, 0, 0, 0, 4, 4, 4},
			msgData.String(),
		},
		{
			"2nd nested element under 3rd level element under fourth top level element",
			[]byte{0, 0, 0, 0, 0, 6, 2, 4, 6},
			msgData.String(),
		},
		{
			"large index to test more than one byte",
			[]byte{0, 0, 0, 0, 0, 4, 2, 144, 3},
			msgData.String(),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// we should be able to extract the payload after the first few wire format bytes
			data := append(c.schemaBytes, payload...)
			pd := NewProtobufDeserializer()
			result := &message.MessageData{}
			err = pd.Deserialize(data, result)
			if err != nil {
				t.Fatalf("unexpected error on Deserialize: %s", err.Error())
			}
			got := result.String()
			if !cmp.Equal(got, c.want) {
				diff := cmp.Diff(got, c.want)
				t.Fatalf("ps.Deserialize(%v, %v) == %v, want %v, diff %v", data, got, got, c.want, diff)
			}
		})
	}
}

func TestProtobufDeserializer_DeserializeErrors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{
			"message too small",
			[]byte{0, 0, 0, 0, 0},
			"message too small. This message was not produced with a Confluent Schema Registry serializer",
		},
		{
			"magic byte missing",
			[]byte{1, 0, 0, 0, 0, 0},
			"unknown magic byte. This message was not produced with a Confluent Schema Registry serializer",
		},
		{
			"message index array length only",
			[]byte{0, 0, 0, 0, 0, 2},
			"unable to decode value in message index array",
		},
		{
			"message index array missing byte",
			[]byte{0, 0, 0, 0, 0, 4, 0},
			"unable to decode value in message index array",
		},
		{
			"invalid message index array length",
			[]byte{0, 0, 0, 0, 0, 3, 0},
			"unable to decode message index array",
		},
		{
			"buffer too small",
			[]byte{0, 0, 0, 0, 0, 254, 254},
			"unable to decode message index array",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// the wire format bytes in these tests are going to be invalid
			pd := NewProtobufDeserializer()
			result := &message.MessageData{}
			err := pd.Deserialize(c.data, result)
			if err == nil {
				t.Fatalf("expected error on Deserialize, got none")
			}
			if err.Error() != c.want {
				t.Fatalf("ps.Deserialize(%v, %v) == %v, want %v", c.data, err, err, c.want)
			}
		})
	}
}
