package serdes

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	defaultIndex = 0
	// MessageFieldKey message field is key
	MessageFieldKey = "key"
	// MessageFieldValue  message field is value
	MessageFieldValue = "value"
	// AutoRegisterSchemas auto register schemas
	AutoRegisterSchemas = "auto.register.schemas"
	// UseLatestVersion use latest version of schema
	UseLatestVersion = "use.latest.version"
	// SkipKnownTypes skips known types for schema references
	SkipKnownTypes = "skip.known.types"
	// SubjectNameStrategyImpl the implementation to use for determining subject naming strategy
	SubjectNameStrategyImpl = "subject.name.strategy"
	// ReferenceSubjectNameStrategyImpl the implementation to use for determining the subject naming strategy for references
	ReferenceSubjectNameStrategyImpl = "reference.subject.name.strategy"
)

// ProtobufSerializerConfigValue config values for protobuf serialization
type ProtobufSerializerConfigValue interface{}

// ProtobufSerializerConfig map of string to ProtobufSerializerConfigValue
type ProtobufSerializerConfig map[string]ProtobufSerializerConfigValue

// SerializationContext extra context information to use for serialization
type SerializationContext struct {
	Topic string
	Field string // either key or value
}

// SubjectNameStrategy how the subject is named, this will usually be based on the topic name
type SubjectNameStrategy interface {
	Subject(ctx SerializationContext, recordName string) string
}

// TopicSubjectNameStrategy uses the topic name for the subject name
type TopicSubjectNameStrategy struct{}

// Subject for TopicSubjectNameStrategy
func (TopicSubjectNameStrategy) Subject(ctx SerializationContext, _ string) string {
	return ctx.Topic + "-" + ctx.Field
}

// TopicRecordSubjectNameStrategy uses the topic name and record name for the subject name
type TopicRecordSubjectNameStrategy struct{}

// Subject for TopicRecordSubjectNameStrategy
func (TopicRecordSubjectNameStrategy) Subject(ctx SerializationContext, recordName string) string {
	return ctx.Topic + "-" + recordName
}

// RecordSubjectNameStrategy uses the record name for the subject name
type RecordSubjectNameStrategy struct{}

// Subject for RecordSubjectNameStrategy
func (RecordSubjectNameStrategy) Subject(_ SerializationContext, recordName string) string {
	return recordName
}

// SubjectNameStrategyForReferences how the subject is named for references, this will usually just be the reference name
type SubjectNameStrategyForReferences interface {
	Subject(ctx SerializationContext, schemaRef protoreflect.FileImport) string
}

// ReferenceSubjectNameStrategy use the reference name for the subject name for references
type ReferenceSubjectNameStrategy struct{}

// Subject for ReferenceSubjectNameStrategy
func (ReferenceSubjectNameStrategy) Subject(_ SerializationContext, schemaRef protoreflect.FileImport) string {
	return schemaRef.Path()
}

// ProtobufSerializer using the schema registry client
type ProtobufSerializer struct {
	client                       srclient.ISchemaRegistryClient
	msgIndexBytes                []byte
	autoRegisterSchemas          bool
	useLatestVersion             bool
	skipKnownTypes               bool
	knownSubjects                map[string]int // map from subject name to associated schema ID
	knownSubjectsLock            sync.RWMutex
	subjectNameStrategy          SubjectNameStrategy
	referenceSubjectNameStrategy SubjectNameStrategyForReferences
}

func createMsgIndex(md protoreflect.MessageDescriptor) []int {
	msgIndex := []int{}
	var current protoreflect.Descriptor
	current = md

	// Traverse tree upwardly until it's root
	for {
		// prepend, but only if current is a MessageDescriptor
		_, ok := current.(protoreflect.MessageDescriptor)
		if ok {
			msgIndex = append([]int{current.Index()}, msgIndex...)
		}
		parent := current.Parent()
		if parent == nil {
			break
		}
		current = parent
	}

	return msgIndex
}

func createMsgIndexBytes(msgIndex []int) []byte {
	if len(msgIndex) == 1 && msgIndex[0] == defaultIndex {
		// optimization, just 0 in this case
		varBytes := make([]byte, 1)
		length := binary.PutVarint(varBytes, int64(defaultIndex))
		return varBytes[:length]
	}

	msgIndexLength := len(msgIndex)
	// make a sufficiently large buffer
	varBytes := make([]byte, 100)

	length := binary.PutVarint(varBytes, int64(msgIndexLength))
	for _, index := range msgIndex {
		length += binary.PutVarint(varBytes[length:], int64(index))
	}
	return varBytes[:length]
}

// NewProtobufSerializer returns a new ProtobufSerializer
func NewProtobufSerializer(md protoreflect.MessageDescriptor, schemaRegistryClient srclient.ISchemaRegistryClient, config ProtobufSerializerConfig) (*ProtobufSerializer, error) {
	msgIndex := createMsgIndex(md)

	msgIndexBytes := createMsgIndexBytes(msgIndex)

	knownSubjects := make(map[string]int)

	ps := &ProtobufSerializer{
		client:        schemaRegistryClient,
		msgIndexBytes: msgIndexBytes,
		knownSubjects: knownSubjects,
	}

	// set all the defaults
	configToUse := ProtobufSerializerConfig{
		AutoRegisterSchemas:              true,
		UseLatestVersion:                 false,
		SkipKnownTypes:                   false,
		SubjectNameStrategyImpl:          TopicSubjectNameStrategy{},     // TopicSubjectNameStrategy is the default
		ReferenceSubjectNameStrategyImpl: ReferenceSubjectNameStrategy{}, // ReferenceSubjectNameStrategy is the default
	}

	// handle configuration
	// update the defaults in configToUse with the values from the passed in config
	if config != nil {
		for key, value := range config {
			configToUse[key] = value
		}
	}

	err := ps.SetAutoRegisterSchemas(configToUse)
	if err != nil {
		return nil, err
	}

	err = ps.SetUseLatestVersion(configToUse)
	if err != nil {
		return nil, err
	}

	err = ps.SetSkipKnownTypes(configToUse)
	if err != nil {
		return nil, err
	}

	err = ps.SetSubjectNameStrategy(configToUse)
	if err != nil {
		return nil, err
	}

	err = ps.SetReferenceSubjectNameStrategy(configToUse)
	if err != nil {
		return nil, err
	}

	// check for left over unrecognized properties
	if len(configToUse) > 0 {
		var keys []string
		for key := range configToUse {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("unrecognized properties: %s", strings.Join(keys, ", "))
	}

	return ps, nil
}

// SetAutoRegisterSchemas using the supplied ProtobufSerializerConfig
func (ps *ProtobufSerializer) SetAutoRegisterSchemas(config ProtobufSerializerConfig) error {
	autoRegisterSchemasConf, ok := config[AutoRegisterSchemas]
	if ok {
		autoRegisterSchemas, okTypeCast := autoRegisterSchemasConf.(bool)
		if !okTypeCast {
			return fmt.Errorf("%s must be a boolean value", AutoRegisterSchemas)
		}
		ps.autoRegisterSchemas = autoRegisterSchemas
		delete(config, AutoRegisterSchemas)
	}
	return ps.isUseLatestVersionAndAutoRegisterSchemas()
}

// SetUseLatestVersion using the supplied ProtobufSerializerConfig
func (ps *ProtobufSerializer) SetUseLatestVersion(config ProtobufSerializerConfig) error {
	useLatestVersionConf, ok := config[UseLatestVersion]
	if ok {
		useLatestVersion, okTypeCast := useLatestVersionConf.(bool)
		if !okTypeCast {
			return fmt.Errorf("%s must be a boolean value", UseLatestVersion)
		}
		ps.useLatestVersion = useLatestVersion
		delete(config, UseLatestVersion)
	}
	return ps.isUseLatestVersionAndAutoRegisterSchemas()
}

func (ps *ProtobufSerializer) isUseLatestVersionAndAutoRegisterSchemas() error {
	if ps.useLatestVersion && ps.autoRegisterSchemas {
		return fmt.Errorf("cannot enable both %s and %s", UseLatestVersion, AutoRegisterSchemas)
	}
	return nil
}

// SetSkipKnownTypes using the supplied ProtobufSerializerConfig
func (ps *ProtobufSerializer) SetSkipKnownTypes(config ProtobufSerializerConfig) error {
	skipKnownTypesConf, ok := config[SkipKnownTypes]
	if ok {
		skipKnownTypes, okTypeCast := skipKnownTypesConf.(bool)
		if !okTypeCast {
			return fmt.Errorf("%s must be a boolean value", SkipKnownTypes)
		}
		ps.skipKnownTypes = skipKnownTypes
		delete(config, SkipKnownTypes)
	}
	return nil
}

// SetSubjectNameStrategy using the supplied ProtobufSerializerConfig
func (ps *ProtobufSerializer) SetSubjectNameStrategy(config ProtobufSerializerConfig) error {
	subjectNameStrategyConf, ok := config[SubjectNameStrategyImpl]
	if ok {
		subjectNameStrategy, okTypeCast := subjectNameStrategyConf.(SubjectNameStrategy)
		if !okTypeCast {
			return fmt.Errorf("%s must be a SubjectNameStrategy", SubjectNameStrategyImpl)
		}
		ps.subjectNameStrategy = subjectNameStrategy
		delete(config, SubjectNameStrategyImpl)
	}
	return nil
}

// SetReferenceSubjectNameStrategy using the supplied ProtobufSerializerConfig
func (ps *ProtobufSerializer) SetReferenceSubjectNameStrategy(config ProtobufSerializerConfig) error {
	referenceSubjectNameStrategyConf, ok := config[ReferenceSubjectNameStrategyImpl]
	if ok {
		referenceSubjectNameStrategy, okTypeCast := referenceSubjectNameStrategyConf.(SubjectNameStrategyForReferences)
		if !okTypeCast {
			return fmt.Errorf("%s must be a SubjectNameStrategyForReferences", ReferenceSubjectNameStrategyImpl)
		}
		ps.referenceSubjectNameStrategy = referenceSubjectNameStrategy
		delete(config, ReferenceSubjectNameStrategyImpl)
	}
	return nil
}

// Serialize using the Confluent Schema Registry wire format
func (ps *ProtobufSerializer) Serialize(pb proto.Message, ctx SerializationContext) ([]byte, error) {
	md := pb.ProtoReflect().Descriptor()

	subject := ps.subjectNameStrategy.Subject(ctx, string(md.FullName()))

	schemaID, err := ps.getSchemaID(ctx, md, subject)
	if err != nil {
		return nil, err
	}

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))

	bytes, err := proto.Marshal(pb)
	if err != nil {
		//fmt.Printf("failed serialize: %v", err)
		return nil, err
	}

	var msgBytes []byte
	// schema serialization protocol version number
	msgBytes = append(msgBytes, byte(0))
	// schema id
	msgBytes = append(msgBytes, schemaIDBytes...)
	// zig zag encoded array of message indexes preceded by length of array
	msgBytes = append(msgBytes, ps.msgIndexBytes...)

	msgBytes = append(msgBytes, bytes...)

	return msgBytes, nil
}

// resolveDependencies resolves and optionally registers schema references recursively.
func (ps *ProtobufSerializer) resolveDependencies(ctx SerializationContext, fd protoreflect.FileDescriptor) ([]srclient.Reference, error) {
	var schemaRefs []srclient.Reference
	fileImports := fd.Imports()
	for i := 0; i < fileImports.Len(); i++ {
		fileImport := fileImports.Get(i)
		if ps.skipKnownTypes && strings.HasPrefix(fileImport.Path(), "google/protobuf/") {
			continue
		}
		// make recursive call
		depRefs, err := ps.resolveDependencies(ctx, fileImport.FileDescriptor)
		if err != nil {
			return nil, err
		}
		subject := ps.referenceSubjectNameStrategy.Subject(ctx, fileImport)
		schemaString, err := fileDescriptorToString(fileImport.FileDescriptor)
		if err != nil {
			return nil, err
		}
		if ps.autoRegisterSchemas {
			_, err = ps.client.CreateSchema(subject, schemaString, srclient.Protobuf, depRefs...)
			if err != nil {
				return nil, err
			}
		}
		reference, err := ps.client.LookupSchema(subject, schemaString, srclient.Protobuf, depRefs...)
		if err != nil {
			return nil, err
		}
		// schemaRefs are per file descriptor
		schemaRef := srclient.Reference{Name: fileImport.Path(), Subject: subject, Version: reference.Version()}
		schemaRefs = append(schemaRefs, schemaRef)
	}
	return schemaRefs, nil
}

func (ps *ProtobufSerializer) getSchemaID(ctx SerializationContext, md protoreflect.MessageDescriptor, subject string) (int, error) {
	ps.knownSubjectsLock.RLock()
	schemaID, ok := ps.knownSubjects[subject]
	ps.knownSubjectsLock.RUnlock()
	if ok {
		return schemaID, nil
	}

	if ps.useLatestVersion {
		theSchema, err := ps.client.GetLatestSchema(subject)
		if err != nil {
			return 0, err
		}
		schemaID = theSchema.ID()
	} else {
		fd := md.ParentFile()
		schemaRefs, err := ps.resolveDependencies(ctx, fd)
		if err != nil {
			return 0, err
		}
		schemaString, err := fileDescriptorToString(fd)
		if err != nil {
			return 0, err
		}
		if ps.autoRegisterSchemas {
			theSchema, err := ps.client.CreateSchema(subject, schemaString, srclient.Protobuf, schemaRefs...)
			if err != nil {
				return 0, err
			}
			schemaID = theSchema.ID()
		} else {
			theSchema, err := ps.client.LookupSchema(subject, schemaString, srclient.Protobuf, schemaRefs...)
			if err != nil {
				return 0, err
			}
			schemaID = theSchema.ID()
		}
	}

	ps.knownSubjectsLock.Lock()
	ps.knownSubjects[subject] = schemaID
	ps.knownSubjectsLock.Unlock()

	return schemaID, nil
}

// fileDescriptorToString converts fd to a file descriptor proto, then marshals it and base64 encodes it for Schema Registry
func fileDescriptorToString(fd protoreflect.FileDescriptor) (string, error) {
	fileDescProto := protodesc.ToFileDescriptorProto(fd)
	data, err := proto.Marshal(fileDescProto)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}
