package legacywriter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decimalValue(t *testing.T, dataType types.DataType, text string) typeutils.Decimal {
	t.Helper()
	value, err := typeutils.ReformatDecimal(dataType, text)
	require.NoError(t, err)
	return value
}

func TestToProtoFieldValue(t *testing.T) {
	sample := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	raw := []byte{0xff, 0x00, 0x80, 0x41}

	testCases := []struct {
		name    string
		iceType string
		value   any
		want    any
		wantErr bool
	}{
		{
			name:    "boolean",
			iceType: "boolean",
			value:   true,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BoolValue{BoolValue: true},
		},
		{
			name:    "int",
			iceType: "int",
			value:   int32(7),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_IntValue{IntValue: 7},
		},
		{
			name:    "long",
			iceType: "long",
			value:   int64(42),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: 42},
		},
		{
			name:    "float",
			iceType: "float",
			value:   float32(1.5),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_FloatValue{FloatValue: 1.5},
		},
		{
			name:    "double",
			iceType: "double",
			value:   2.5,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: 2.5},
		},
		{
			name:    "timestamptz travels as epoch micros",
			iceType: "timestamptz",
			value:   sample,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_LongValue{LongValue: sample.UnixMicro()},
		},
		{
			name:    "decimal travels as its exact text",
			iceType: "decimal(38, 10)",
			value:   decimalValue(t, types.DecimalOf(38, 10), "-1234567890123456789012345678.0123456789"),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "-1234567890123456789012345678.0123456789"},
		},
		{
			name:    "decimal into a double column",
			iceType: "double",
			value:   decimalValue(t, types.DecimalOf(10, 2), "2.50"),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_DoubleValue{DoubleValue: 2.5},
		},
		{
			name:    "bytes into a binary column",
			iceType: "binary",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: raw},
		},
		{
			name:    "text into a binary column travels as its bytes",
			iceType: "binary",
			value:   "abc",
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: []byte("abc")},
		},
		{
			name:    "bytes of the exact width",
			iceType: "fixed[4]",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: raw},
		},
		{
			name:    "a value narrower than the column is rejected",
			iceType: "fixed[6]",
			value:   raw,
			wantErr: true,
		},
		{
			name:    "a value wider than the column is rejected",
			iceType: "fixed[2]",
			value:   raw,
			wantErr: true,
		},
		{
			name:    "a number into a binary column travels as its text",
			iceType: "binary",
			value:   42,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_BytesValue{BytesValue: []byte("42")},
		},
		{
			name:    "utf-8 bytes into a string column become text",
			iceType: "string",
			value:   []byte("abc"),
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: fmt.Sprintf("%v", []byte("abc"))},
		},
		{
			name:    "a number into a string column becomes text",
			iceType: "string",
			value:   42,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "42"},
		},
		{
			name:    "a type olake does not model becomes text",
			iceType: "date",
			value:   "2024-01-02",
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "2024-01-02"},
		},
		{
			name:    "bytes that are not utf-8",
			iceType: "string",
			value:   raw,
			want:    &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: fmt.Sprintf("%v", raw)},
		},
		{
			name:    "a value that is not a boolean is rejected",
			iceType: "boolean",
			value:   map[string]any{"a": 1},
			wantErr: true,
		},
		{
			name:    "a value that is not an int is rejected",
			iceType: "int",
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a long is rejected",
			iceType: "long",
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a float is rejected",
			iceType: "float",
			value:   "abc",
			wantErr: true,
		},
		{
			name:    "a value that is not a double is rejected",
			iceType: "double",
			value:   "abc",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fv, err := toProtoFieldValue(tc.iceType, tc.value)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, fv.GetValue())
		})
	}
}

func TestRawDataColumnBuffer(t *testing.T) {
	record := types.RawRecord{
		Data:         map[string]any{"blob": []byte{0xff, 0x00}},
		OlakeColumns: map[string]any{constants.OlakeID: "abc"},
	}
	protoSchema := []*proto.IcebergPayload_SchemaField{
		{Key: constants.OlakeID, IceType: "string"},
		{Key: constants.StringifiedData, IceType: "string"},
		{Key: "absent", IceType: "string"},
	}

	values, err := RawDataColumnBuffer(record, protoSchema)
	require.NoError(t, err)
	require.Len(t, values, 3)

	assert.Equal(t, &proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: "abc"}, values[0].GetValue())
	assert.Equal(t,
		&proto.IcebergPayload_IceRecord_FieldValue_StringValue{StringValue: `{"blob":"/wA="}`},
		values[1].GetValue(),
		"the denormalized column is json, so raw bytes travel base64 encoded and stay valid utf-8")
	assert.Nil(t, values[2], "a schema field the record does not carry is sent unset")
}

type committedIndex struct{ types.StreamIndex }

func (committedIndex) Lookup(_ string) (types.RowLocation, bool, error) {
	return types.RowLocation{}, false, nil
}
func (committedIndex) LastCommittedSnapshot() (int64, error)         { return 0, nil }
func (committedIndex) Commit(*types.StreamIndexThread, *int64) error { return nil }

type mockServer struct {
	sentPayloads []*proto.IcebergPayload
	currentFile  string
	nextPosition int64
}

func (m *mockServer) SendClientRequest(_ context.Context, reqPayload interface{}) (interface{}, error) {
	req := reqPayload.(*proto.IcebergPayload)
	m.sentPayloads = append(m.sentPayloads, req)

	ranges := make([]*proto.FilePositionMap_Range, 0, len(req.Records))
	for i := range req.Records {
		ranges = append(ranges, &proto.FilePositionMap_Range{
			BatchStartIdx: int32(i), //nolint:gosec // test index fits within int32
			StartPosition: m.nextPosition,
			Count:         1,
		})
		m.nextPosition++
	}

	return &proto.RecordIngestResponse{
		Result: "ok",
		FilePositionMaps: []*proto.FilePositionMap{
			{
				FilePath: m.currentFile,
				Ranges:   ranges,
			},
		},
	}, nil
}

func newTestLegacyWriter(t *testing.T, hasPK bool) (*LegacyWriter, *mockServer, *types.StreamIndexThread) {
	t.Helper()
	server := &mockServer{currentFile: "test_data.parquet"}
	index := types.NewStreamIndexThread(committedIndex{})
	pkSet := types.NewSet[string]()
	if hasPK {
		pkSet.Insert("id")
	}
	stream := &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:                    "t",
			Namespace:               "n",
			SourceDefinedPrimaryKey: pkSet,
		},
	}
	schema := map[string]string{
		constants.OlakeID: "string",
		constants.OpType:  "string",
		"payload":         "string",
	}
	w := New(&destination.Options{ThreadID: "th"}, schema, stream, server, true, index)
	return w, server, index
}

func testLegacyRecord(id, op, payload string) types.RawRecord {
	return types.RawRecord{
		Data: map[string]any{
			constants.OlakeID: id,
			constants.OpType:  op,
			"payload":         payload,
		},
		OlakeColumns: map[string]any{
			constants.OlakeID:        id,
			constants.OpType:         op,
			constants.OlakeTimestamp: time.Now().UTC(),
		},
	}
}

// TestLegacyWriterReinsertDeletedPrimaryKeyWithPK verifies issue #1286 in legacy-writer:
// When a PK stream deletes a row and re-inserts it ('c'), the legacy writer emits a positional
// delete (DeleteFilePath and DeletePosition set) pointing to the tombstone row.
func TestLegacyWriterReinsertDeletedPrimaryKeyWithPK(t *testing.T) {
	ctx := context.Background()
	w, server, _ := newTestLegacyWriter(t, true)

	// 1. Insert key "1"
	require.NoError(t, w.Write(ctx, []types.RawRecord{testLegacyRecord("1", "c", "v1")}))
	require.Len(t, server.sentPayloads, 1)
	req1 := server.sentPayloads[0]
	require.Len(t, req1.Records, 1)
	assert.Nil(t, req1.Records[0].DeleteFilePath, "initial insert should not have delete file path")

	// 2. Delete key "1" (writes tombstone at pos 1, deletes pos 0)
	require.NoError(t, w.Write(ctx, []types.RawRecord{testLegacyRecord("1", "d", "v1")}))
	require.Len(t, server.sentPayloads, 2)
	req2 := server.sentPayloads[1]
	require.Len(t, req2.Records, 1)
	require.NotNil(t, req2.Records[0].DeleteFilePath)
	assert.Equal(t, "test_data.parquet", *req2.Records[0].DeleteFilePath)
	require.NotNil(t, req2.Records[0].DeletePosition)
	assert.Equal(t, int64(0), *req2.Records[0].DeletePosition)

	// 3. Re-insert key "1" ('c')
	// On staging before fix: DeleteFilePath was nil (skipped lookup for 'c').
	// With fix: DeleteFilePath and DeletePosition point to tombstone at pos 1!
	require.NoError(t, w.Write(ctx, []types.RawRecord{testLegacyRecord("1", "c", "v2")}))
	require.Len(t, server.sentPayloads, 3)
	req3 := server.sentPayloads[2]
	require.Len(t, req3.Records, 1)
	require.NotNil(t, req3.Records[0].DeleteFilePath, "re-insert of deleted PK must emit positional delete for the tombstone")
	assert.Equal(t, "test_data.parquet", *req3.Records[0].DeleteFilePath)
	require.NotNil(t, req3.Records[0].DeletePosition)
	assert.Equal(t, int64(1), *req3.Records[0].DeletePosition)
}

// TestLegacyWriterPKLessStreamPreservesDuplicateInserts verifies that tables without a primary key
// do not look up index for 'c' records, emitting no positional deletes.
func TestLegacyWriterPKLessStreamPreservesDuplicateInserts(t *testing.T) {
	ctx := context.Background()
	w, server, _ := newTestLegacyWriter(t, false)

	// Insert duplicate records
	require.NoError(t, w.Write(ctx, []types.RawRecord{testLegacyRecord("hash1", "c", "dup")}))
	require.NoError(t, w.Write(ctx, []types.RawRecord{testLegacyRecord("hash1", "c", "dup")}))

	require.Len(t, server.sentPayloads, 2)
	assert.Nil(t, server.sentPayloads[0].Records[0].DeleteFilePath)
	assert.Nil(t, server.sentPayloads[1].Records[0].DeleteFilePath, "PK-less duplicate insert should not emit positional delete")
}
