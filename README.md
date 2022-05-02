# kafka-go-serdes

![Build Status](https://github.com/finncolman/kafka-go-serdes/actions/workflows/go.yml/badge.svg?event=push)

Serdes for confluent-kafka-go

**kafka-go-serdes** is a Serilializer/Deserializer (Serdes) designed to be used with Go and the Confluent Schema Registry.
It will serialize records and decorate the serialized records with the schema ID used in Confluent Schema Registry.
Currently only [Protobuf](https://developers.google.com/protocol-buffers) format is supported, due to Protobuf having the best library support with Go.

The confluent-kafka-go library allows Go to produce and consume from Kafka but has no Schema Registry support built in.
There is also the srclient library for Go that allows Go programmers to work with Confluent Schema Registry but has no Serdes support and does not handle the wire format needed for serialization.
This library fills in that gap. It is meant to be used in conjunction with github.com/confluentinc/confluent-kafka-go/kafka and github.com/riferrei/srclient.

## Features

* **Simple to Use** - You can continue to use the confluent-kafka-go lib as is. 
This library will just help to generated protobuf serialized data that conforms with the required wire format with the prepended schema ID for working with Schema Registry.
The Value you pass to the produce call via a kafka.Message is still a byte slice.

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)

Module install:

This client is a Go module, therefore you can have it simply by adding the following import to your code:

```go
import "github.com/finncolman/kafka-go-serdes/serdes"
```

Then run a build to have this client automatically added to your go.mod file as a dependency.

Manual install:

```bash
go get -u github.com/finncolman/kafka-go-serdes/serdes
```

## Examples

In the example below msgData is an example protobuf object, you can substitute your own protobuf object for this. 
Your protobuf code should have been generated using a reasonably recent version of protoc as the newer versions use 'ProtoReflect' to get the descriptor. 
### Serializer
```go
package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/finncolman/kafka-go-serdes/internal/message"
	"github.com/finncolman/kafka-go-serdes/serdes"
	"github.com/riferrei/srclient"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	topic := "myTopic"
	msgData := message.MessageData{}
	md := msgData.ProtoReflect().Descriptor()
	sc := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	ps, err := serdes.NewProtobufSerializer(md, sc, serdes.ProtobufSerializerConfig{serdes.AutoRegisterSchemas: true, serdes.UseLatestVersion: true})
	if err != nil {
		panic(fmt.Sprintf("failed to get the NewProtobufSerializer %s", err))
	}

	protoNested1 := &message.Nested1{MessageId: 1, Test: "blah", Test_2: "blah2"}
	protoNested2 := &message.Nested2{Id: "1234", Another: "blah", AnotherPart: "test"}
	protoMsgData := &message.MessageData{Nest1: protoNested1, Nest2: protoNested2}
	//protoMsgData :=
	serializationContext := serdes.SerializationContext{Topic: topic, Field: serdes.MessageFieldValue}
	data, err := ps.Serialize(protoMsgData, serializationContext)
	if err != nil {
		panic(fmt.Sprintf("could not marshal Kafka msgData %s", err))
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, deliveryChan)
	if err != nil {
		panic(fmt.Sprintf("could not send to topic %s", err))
	}
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		panic(fmt.Sprintf("topic partition error %s", m.TopicPartition.Error))
	}
}
```

### Deserializer
```go
pd := serdes.NewProtobufDeserializer()
msgData := &message.MessageData{}
err := bkt.pd.Deserialize(msg.Value, msgData)
if err != nil {
    logger.Error().Err(err).Msg("Error trying to unmarshal the message from Kafka")
}
```