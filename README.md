# kafka-go-serdes

![Build Status](https://github.com/finncolman/kafka-go-serdes/actions/workflows/go.yml/badge.svg?event=push)

Serdes for confluent-kafka-go

**kafka-go-serdes** is a Serilializer/Deserializer (Serdes) designed to be used with Go and the Confluent Schema Registry. It will serialize records and decorate the serialized records with the schema ID used in Confluent Schema Registry.
Currently only Protobuf format is supported [Protobuf](https://developers.google.com/protocol-buffers), due to Protobuf having the best library support with Go.

The confluent-kafka-go library allows Go to produce and consume from Kafka but has no Schema Registry support built in. There is also the srclient library for Go that allows Go programmers to work with Schema Reistry but has no Serdes support and does not handle the wire format needed for serialization.
This library fills in that gap. It is meant to be used in conjunction with github.com/confluentinc/confluent-kafka-go/kafka and github.com/riferrei/srclient.

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

In the example below msgData is a protobuf object
### Serializer
```go
md := msgData.ProtoReflect().Descriptor()
sc := srclient.CreateSchemaRegistryClient("http://localhost:8081")
ps, err := serdes.NewProtobufSerializer(md, sc, serdes.ProtobufSerializerConfig{serdes.AutoRegisterSchemas: true, serdes.UseLatestVersion: true})
if err != nil {
	logger.Fatal().Err(err).Msg("failed to get the NewProtobufSerializer")
}

serializationContext := serdes.SerializationContext{Topic: topic, Field: serdes.MessageFieldValue}
data, err := ps.Serialize(msgData, serializationContext)
if err != nil {
    logger.Error().Err(err).Int64(messageIDKey, msg.MessageID.ValueOrZero()).Msg("Could not marshal Kafka msgData")
}

err = p.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          data,
    Headers:        headers,
    }, deliveryChan)
if err != nil {
    logger.Error().Err(err).Int64(messageIDKey, msg.MessageID.ValueOrZero()).Str("topicName", topic).Msg("Could not send to topic")
}
e := <-deliveryChan
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