package serdes

import (
	"fmt"
	"github.com/finncolman/kafka-go-serdes/internal/message"
	"github.com/finncolman/kafka-go-serdes/internal/messagerefs"
	"github.com/riferrei/srclient"
	"reflect"
	"testing"
	"time"
)

func TestProtobufSerializer_createMsgIndexBytes(t *testing.T) {
	cases := []struct {
		name     string
		msgIndex []int
		want     []byte
	}{
		{
			"default case",
			[]int{0},
			[]byte{0},
		},
		{
			"second element at top level",
			[]int{1},
			[]byte{2, 2},
		},
		{
			"third element at top level",
			[]int{2},
			[]byte{2, 4},
		},
		{
			"first nested element under first top level element",
			[]int{0, 0},
			[]byte{4, 0, 0},
		},
		{
			"third nested element under first top level element",
			[]int{0, 2},
			[]byte{4, 0, 4},
		},
		{
			"third nested element under third top level element",
			[]int{2, 2},
			[]byte{4, 4, 4},
		},
		{
			"2nd nested element under 3rd level element under fourth top level element",
			[]int{1, 2, 3},
			[]byte{6, 2, 4, 6},
		},
		{
			"large index to test more than one byte",
			[]int{1, 200},
			[]byte{4, 2, 144, 3},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := createMsgIndexBytes(c.msgIndex)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("createMsgIndexBytes(%v) == %v, want %v", c.msgIndex, got, c.want)
			}
		})
	}
}

type mockSchemaRegistryClient struct {
}

func (*mockSchemaRegistryClient) GetSubjects() ([]string, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) GetSchema(int) (*srclient.Schema, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) GetLatestSchema(string) (*srclient.Schema, error) {
	return &srclient.Schema{}, nil
}

func (*mockSchemaRegistryClient) GetSchemaVersions(string) ([]int, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) GetSchemaByVersion(string, int) (*srclient.Schema, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) CreateSchema(string, string, srclient.SchemaType, ...srclient.Reference) (*srclient.Schema, error) {
	return &srclient.Schema{}, nil
}

func (*mockSchemaRegistryClient) DeleteSubject(string, bool) error {
	return nil
}

func (*mockSchemaRegistryClient) DeleteSubjectByVersion(string, int, bool) error {
	return nil
}

func (*mockSchemaRegistryClient) SetCredentials(string, string) {
}

func (*mockSchemaRegistryClient) SetTimeout(time.Duration) {
}

func (*mockSchemaRegistryClient) CachingEnabled(bool) {
}

func (*mockSchemaRegistryClient) CodecCreationEnabled(bool) {
}

func (*mockSchemaRegistryClient) IsSchemaCompatible(string, string, string, srclient.SchemaType) (bool, error) {
	return true, nil
}

func (*mockSchemaRegistryClient) ChangeSubjectCompatibilityLevel(subject string, compatibility srclient.CompatibilityLevel) (*srclient.CompatibilityLevel, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) GetCompatibilityLevel(subject string, defaultToGlobal bool) (*srclient.CompatibilityLevel, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) GetGlobalCompatibilityLevel() (*srclient.CompatibilityLevel, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) GetSubjectsIncludingDeleted() ([]string, error) {
	return nil, nil
}

func (*mockSchemaRegistryClient) LookupSchema(subject string, schema string, schemaType srclient.SchemaType, references ...srclient.Reference) (*srclient.Schema, error) {
	return &srclient.Schema{}, nil
}

func (*mockSchemaRegistryClient) ResetCache() {
}

func TestProtobufSerializer_Serialize(t *testing.T) {
	msrc := &mockSchemaRegistryClient{}
	ctx := SerializationContext{Topic: "test", Field: MessageFieldValue}

	msgData := &message.MessageData{}
	msgDescriptor := msgData.ProtoReflect().Descriptor()

	cases := []struct {
		name string
		want []byte
	}{
		{
			"default case",
			[]byte{0, 0, 0, 0, 0, 2, 4}, // magic byte + all zeros schema id + third element at top level msg index
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ps, err := NewProtobufSerializer(msgDescriptor, msrc, nil)
			if err != nil {
				t.Fatalf("unexpected error on NewProtobufSerializer: %s", err.Error())
			}
			got, err := ps.Serialize(msgData, ctx)
			if err != nil {
				t.Fatalf("unexpected error on Serialize: %s", err.Error())
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("ps.Serialize(%v) == %v, want %v", msgData, got, c.want)
			}
		})
	}
}

func TestProtobufSerializer_NewProtobufSerializer(t *testing.T) {
	msrc := &mockSchemaRegistryClient{}

	msgData := &message.MessageData{}
	msgDescriptor := msgData.ProtoReflect().Descriptor()

	knownSubjects := make(map[string]int)

	cases := []struct {
		name   string
		config ProtobufSerializerConfig
		want   *ProtobufSerializer
	}{
		{
			"nil ProtobufSerializerConfig",
			nil,
			&ProtobufSerializer{
				client:                       msrc,
				msgIndexBytes:                []byte{2, 4},
				autoRegisterSchemas:          true,
				useLatestVersion:             false,
				skipKnownTypes:               false,
				knownSubjects:                knownSubjects,
				subjectNameStrategy:          TopicSubjectNameStrategy{},
				referenceSubjectNameStrategy: ReferenceSubjectNameStrategy{},
			},
		},
		{
			"empty ProtobufSerializerConfig",
			ProtobufSerializerConfig{},
			&ProtobufSerializer{
				client:                       msrc,
				msgIndexBytes:                []byte{2, 4},
				autoRegisterSchemas:          true,
				useLatestVersion:             false,
				skipKnownTypes:               false,
				knownSubjects:                knownSubjects,
				subjectNameStrategy:          TopicSubjectNameStrategy{},
				referenceSubjectNameStrategy: ReferenceSubjectNameStrategy{},
			},
		},
		{
			fmt.Sprintf("%s ProtobufSerializerConfig", UseLatestVersion),
			ProtobufSerializerConfig{
				AutoRegisterSchemas: false,
				UseLatestVersion:    true,
			},
			&ProtobufSerializer{
				client:                       msrc,
				msgIndexBytes:                []byte{2, 4},
				autoRegisterSchemas:          false,
				useLatestVersion:             true,
				skipKnownTypes:               false,
				knownSubjects:                knownSubjects,
				subjectNameStrategy:          TopicSubjectNameStrategy{},
				referenceSubjectNameStrategy: ReferenceSubjectNameStrategy{},
			},
		},
		{
			"TopicRecordSubjectNameStrategy ProtobufSerializerConfig",
			ProtobufSerializerConfig{
				SubjectNameStrategyImpl: TopicRecordSubjectNameStrategy{},
			},
			&ProtobufSerializer{
				client:                       msrc,
				msgIndexBytes:                []byte{2, 4},
				autoRegisterSchemas:          true,
				useLatestVersion:             false,
				skipKnownTypes:               false,
				knownSubjects:                knownSubjects,
				subjectNameStrategy:          TopicRecordSubjectNameStrategy{},
				referenceSubjectNameStrategy: ReferenceSubjectNameStrategy{},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := NewProtobufSerializer(msgDescriptor, msrc, c.config)
			if err != nil {
				t.Fatalf("unexpected error on NewProtobufSerializer: %s", err.Error())
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("NewProtobufSerializer(%v, %v, %v) == %v, want %v", msgDescriptor, msrc, c.config, got, c.want)
			}
		})
	}
}

func TestProtobufSerializer_NewProtobufSerializerErrors(t *testing.T) {
	msrc := &mockSchemaRegistryClient{}

	msgData := &message.MessageData{}
	msgDescriptor := msgData.ProtoReflect().Descriptor()

	cases := []struct {
		name   string
		config ProtobufSerializerConfig
		want   error
	}{
		{
			fmt.Sprintf("wrong type for %s", AutoRegisterSchemas),
			ProtobufSerializerConfig{
				AutoRegisterSchemas: "true",
			},
			fmt.Errorf("%s must be a boolean value", AutoRegisterSchemas),
		},
		{
			fmt.Sprintf("wrong type for %s", UseLatestVersion),
			ProtobufSerializerConfig{
				AutoRegisterSchemas: false,
				UseLatestVersion:    "true",
			},
			fmt.Errorf("%s must be a boolean value", UseLatestVersion),
		},
		{
			fmt.Sprintf("wrong type for %s", SkipKnownTypes),
			ProtobufSerializerConfig{
				SkipKnownTypes: "true",
			},
			fmt.Errorf("%s must be a boolean value", SkipKnownTypes),
		},
		{
			fmt.Sprintf("cannot enable both %s and %s", UseLatestVersion, AutoRegisterSchemas),
			ProtobufSerializerConfig{
				AutoRegisterSchemas: true,
				UseLatestVersion:    true,
			},
			fmt.Errorf("cannot enable both %s and %s", UseLatestVersion, AutoRegisterSchemas),
		},
		{
			fmt.Sprintf("cannot enable both %s and %s, need to explicitly set '%s' to false", UseLatestVersion, AutoRegisterSchemas, AutoRegisterSchemas),
			ProtobufSerializerConfig{
				UseLatestVersion: true,
			},
			fmt.Errorf("cannot enable both %s and %s", UseLatestVersion, AutoRegisterSchemas),
		},
		{
			fmt.Sprintf("wrong type for %s", SubjectNameStrategyImpl),
			ProtobufSerializerConfig{
				SubjectNameStrategyImpl: true,
			},
			fmt.Errorf("%s must be a SubjectNameStrategy", SubjectNameStrategyImpl),
		},
		{
			fmt.Sprintf("wrong type for %s", ReferenceSubjectNameStrategyImpl),
			ProtobufSerializerConfig{
				ReferenceSubjectNameStrategyImpl: true,
			},
			fmt.Errorf("%s must be a SubjectNameStrategyForReferences", ReferenceSubjectNameStrategyImpl),
		},
		{
			"unrecognized properties",
			ProtobufSerializerConfig{
				"made.this.up": true,
				"also.made.up": false,
			},
			fmt.Errorf("unrecognized properties: also.made.up, made.this.up"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewProtobufSerializer(msgDescriptor, msrc, c.config)
			if err == nil {
				t.Fatalf("expected error on NewProtobufSerializer but got none")
			}
			if !reflect.DeepEqual(err, c.want) {
				t.Fatalf("NewProtobufSerializer(%v, %v, %v) == %v, want %v", msgDescriptor, msrc, c.config, err, c.want)
			}
		})
	}
}

func TestProtobufSerializer_resolveDependencies(t *testing.T) {
	msrc := &mockSchemaRegistryClient{}

	msgData := &messagerefs.MessageData{}
	msgDescriptor := msgData.ProtoReflect().Descriptor()

	cases := []struct {
		name   string
		config ProtobufSerializerConfig
		want   []srclient.Reference
	}{
		{
			"AutoRegisterSchemas true",
			ProtobufSerializerConfig{
				AutoRegisterSchemas: true,
				UseLatestVersion:    false,
			},
			[]srclient.Reference{{Name: "nested1.proto", Subject: "nested1.proto", Version: 0}, {Name: "nested2.proto", Subject: "nested2.proto", Version: 0}},
		},
		{
			"UseLatestVersion true",
			ProtobufSerializerConfig{
				AutoRegisterSchemas: false,
				UseLatestVersion:    true,
			},
			[]srclient.Reference{{Name: "nested1.proto", Subject: "nested1.proto", Version: 0}, {Name: "nested2.proto", Subject: "nested2.proto", Version: 0}},
		},
		{
			"AutoRegisterSchemas and UseLatestVersion both false",
			ProtobufSerializerConfig{
				AutoRegisterSchemas: false,
				UseLatestVersion:    false,
			},
			[]srclient.Reference{{Name: "nested1.proto", Subject: "nested1.proto", Version: 0}, {Name: "nested2.proto", Subject: "nested2.proto", Version: 0}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ps, err := NewProtobufSerializer(msgDescriptor, msrc, c.config)
			if err != nil {
				t.Fatalf("unexpected error on NewProtobufSerializer: %s", err.Error())
			}

			got, err := ps.resolveDependencies(SerializationContext{}, msgDescriptor.ParentFile())
			if err != nil {
				t.Fatalf("unexpected error on ps.resolveDependencies: %s", err.Error())
			}

			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("ps.resolveDependencies(%v, %v) == %v, want %v", SerializationContext{}, msgDescriptor.ParentFile(), got, c.want)
			}
		})
	}
}
