package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

// committedFile is a file an earlier sync committed, holding row 0.
const committedFile = "s3://b/t/data/committed.parquet"

// committedIndex is the index as the last sync committed it.
type committedIndex struct{ types.StreamIndex }

func (committedIndex) Lookup(id string) (types.RowLocation, bool, error) {
	if id == "0" {
		return types.RowLocation{FilePath: committedFile}, true, nil
	}
	return types.RowLocation{}, false, nil
}
func (committedIndex) LastCommittedSnapshot() (int64, error)         { return 0, nil }
func (committedIndex) Commit(*types.StreamIndexThread, *int64) error { return nil }

// uploadServer stands in for the Java server: it hands out file paths and keeps the files uploaded.
type uploadServer struct {
	paths           int
	uploaded        map[string][]byte
	deletionVectors []*proto.ArrowPayload_DeletionVectorBatch
}

func (s *uploadServer) SendClientRequest(_ context.Context, req any) (any, error) {
	payload := req.(*proto.ArrowPayload)
	switch payload.GetType() {
	case proto.ArrowPayload_JSONSCHEMA:
		return &proto.ArrowIngestResponse{IcebergSchemas: map[string]string{
			fileTypeData:             `{"fields":[{"id":1,"name":"_olake_id"},{"id":2,"name":"_op_type"},{"id":3,"name":"_olake_timestamp"},{"id":4,"name":"payload"}]}`,
			fileTypeEqualityDelete:   `{"fields":[{"id":1,"name":"_olake_id"}]}`,
			fileTypePositionalDelete: `{"fields":[]}`,
		}}, nil
	case proto.ArrowPayload_FILEPATH:
		s.paths++
		return &proto.ArrowIngestResponse{Result: fmt.Sprintf("s3://b/t/data/f%d.parquet", s.paths)}, nil
	case proto.ArrowPayload_UPLOAD_FILE:
		up := payload.GetMetadata().GetFileUpload()
		s.uploaded[up.GetFilePath()] = append([]byte(nil), up.GetFileData()...)
		return &proto.ArrowIngestResponse{}, nil
	case proto.ArrowPayload_REGISTER_AND_COMMIT:
		return &proto.ArrowIngestResponse{}, nil
	case proto.ArrowPayload_DELETION_VECTORS:
		s.deletionVectors = append(s.deletionVectors, payload.GetMetadata().GetDeletionVectors())
		return &proto.ArrowIngestResponse{}, nil
	}
	return nil, fmt.Errorf("unexpected %v", payload.GetType())
}

// newTestWriter is a pos mode writer on an uploadServer, with the index thread it fills.
func newTestWriter(t *testing.T, partitionInfo []internal.PartitionInfo) (*ArrowWriter, *uploadServer, *types.StreamIndexThread) {
	t.Helper()

	server := &uploadServer{uploaded: map[string][]byte{}}
	updateType := "pos"
	stream := &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:                    "t",
			Namespace:               "n",
			SourceDefinedPrimaryKey: types.NewSet("id"),
		},
		StreamMetadata: types.StreamMetadata{UpdateType: &updateType},
	}
	index := types.NewStreamIndexThread(committedIndex{})
	schema := map[string]string{"_olake_id": "string", "_op_type": "string", "_olake_timestamp": "timestamptz", "payload": "string"}
	w, err := New(context.Background(), &destination.Options{ThreadID: "th", TableIndex: committedIndex{}}, partitionInfo, schema, stream, server, true, index)
	require.NoError(t, err)
	return w, server, index
}

func testRecord(id, op, payload string) types.RawRecord {
	return types.RawRecord{Data: map[string]any{"payload": payload}, OlakeColumns: map[string]any{
		constants.OlakeID: id, constants.OpType: op, constants.OlakeTimestamp: time.Now().UTC()}}
}

// parquetRows reads the named columns of an uploaded parquet file as text, one slice per row.
func parquetRows(t *testing.T, data []byte, columns ...string) [][]string {
	t.Helper()

	table, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()

	rows := make([][]string, table.NumRows())
	for _, name := range columns {
		indices := table.Schema().FieldIndices(name)
		require.Len(t, indices, 1, "column %s", name)
		row := 0
		for _, chunk := range table.Column(indices[0]).Data().Chunks() {
			for idx := range chunk.Len() {
				rows[row] = append(rows[row], chunk.ValueStr(idx))
				row++
			}
		}
	}
	return rows
}

// assertPageIndex checks every column chunk of an uploaded parquet file has the page index Java
// reads single rows by.
func assertPageIndex(t *testing.T, data []byte) {
	t.Helper()

	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	for group := range reader.NumRowGroups() {
		rowGroup := reader.MetaData().RowGroup(group)
		for column := range rowGroup.NumColumns() {
			chunk, err := rowGroup.ColumnChunk(column)
			require.NoError(t, err)
			assert.NotNil(t, chunk.GetOffsetIndexLocation(), "offset index of row group %d column %d", group, column)
			assert.NotNil(t, chunk.GetColumnIndexLocation(), "column index of row group %d column %d", group, column)
		}
	}
}

// Every location the index hands out holds its row's latest version, whether or not a read closed
// the open file between two batches, and the positional deletes point at the versions replaced.
func TestEnsureReadableKeepsPositions(t *testing.T) {
	testCases := []struct {
		name          string
		read          string           // the row a read asks for between the two batches
		wantFiles     int              // data files at Close
		wantPositions map[string]int64 // each row's position in its file at Close
	}{
		{
			name:          "reading a row in the open file closes it, and the next batch starts a new one",
			read:          "1",
			wantFiles:     2,
			wantPositions: map[string]int64{"1": 0, "2": 1, "3": 2, "4": 2},
		},
		{
			name:          "reading a row in a committed file leaves the open file taking the next batch",
			read:          "0",
			wantFiles:     1,
			wantPositions: map[string]int64{"1": 3, "2": 4, "3": 2, "4": 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w, server, index := newTestWriter(t, nil)
			require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "a"), testRecord("2", "c", "b"), testRecord("3", "c", "c")}))
			read, found, err := index.Lookup(tc.read)
			require.NoError(t, err)
			require.True(t, found, "row %s is indexed", tc.read)
			require.NoError(t, w.EnsureReadable(ctx, []string{read.FilePath}))
			require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "u", "a2"), testRecord("2", "u", "b2"), testRecord("4", "c", "d")}))

			require.NoError(t, w.Close(ctx, nil))

			var dataFiles, deleteFiles []string
			for _, files := range w.createdFiles {
				for _, data := range files.DataFiles {
					dataFiles = append(dataFiles, data.FilePath)
				}
				for _, deletes := range files.PosDeleteFiles {
					deleteFiles = append(deleteFiles, deletes.FilePath)
				}
			}
			assert.Len(t, dataFiles, tc.wantFiles)
			stored := map[types.RowLocation][]string{} // each data file row's _olake_id and payload
			for _, path := range dataFiles {
				assertPageIndex(t, server.uploaded[path])
				for position, row := range parquetRows(t, server.uploaded[path], constants.OlakeID, "payload") {
					stored[types.RowLocation{FilePath: path, Position: int64(position)}] = row
				}
			}
			for id, payload := range map[string]string{"1": "a2", "2": "b2", "3": "c", "4": "d"} {
				location, found, err := index.Lookup(id)
				require.NoError(t, err)
				require.True(t, found, "row %s is indexed", id)
				assert.Equal(t, tc.wantPositions[id], location.Position, "position of row %s", id)
				assert.Equal(t, []string{id, payload}, stored[location], "row %s at its location", id)
			}
			var deleted [][]string
			for _, path := range deleteFiles {
				for _, row := range parquetRows(t, server.uploaded[path], "file_path", "pos") {
					position, err := strconv.ParseInt(row[1], 10, 64)
					require.NoError(t, err)
					deleted = append(deleted, stored[types.RowLocation{FilePath: row[0], Position: position}])
				}
			}
			assert.ElementsMatch(t, [][]string{{"1", "a"}, {"2", "b"}}, deleted, "the versions rows 1 and 2 replaced")
		})
	}
}

// Only the open files a read names are uploaded: other partitions keep writing to theirs, and a
// file that is not open, or a partition left without one by schema evolution, uploads nothing.
func TestEnsureReadableUploadsNamedOpenFiles(t *testing.T) {
	testCases := []struct {
		name         string
		evolve       bool     // schema evolution uploads every open file before the read
		read         []string // ids of the rows the read asks for
		wantUploaded []string // ids of the rows whose files are uploaded once EnsureReadable returns
	}{
		{
			name:         "only the named partition's file is uploaded",
			read:         []string{"1"},
			wantUploaded: []string{"1"},
		},
		{
			name:         "every named file is uploaded",
			read:         []string{"1", "2"},
			wantUploaded: []string{"1", "2"},
		},
		{
			name: "a row in a file an earlier sync committed uploads nothing",
			read: []string{"0"},
		},
		{
			name:         "a partition schema evolution left without an open file is skipped",
			evolve:       true,
			read:         []string{"1"},
			wantUploaded: []string{"1", "2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w, server, index := newTestWriter(t, []internal.PartitionInfo{{Field: "payload", SchemaField: "payload", Transform: "identity"}})
			require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "a"), testRecord("2", "c", "b")}))
			filesOf := func(ids []string) []string {
				var paths []string
				for _, id := range ids {
					location, found, err := index.Lookup(id)
					require.NoError(t, err)
					require.True(t, found, "row %s is indexed", id)
					paths = append(paths, location.FilePath)
				}
				return paths
			}
			if tc.evolve {
				require.NoError(t, w.EvolveSchema(ctx, w.schema))
			}

			require.NoError(t, w.EnsureReadable(ctx, filesOf(tc.read)))

			assert.ElementsMatch(t, filesOf(tc.wantUploaded), slices.Collect(maps.Keys(server.uploaded)))
		})
	}
}

func collectLiveRowsFromWriters(t *testing.T, server *uploadServer, writers ...*ArrowWriter) [][]string {
	t.Helper()

	var dataFiles, deleteFiles []string
	for _, w := range writers {
		for _, files := range w.createdFiles {
			for _, data := range files.DataFiles {
				dataFiles = append(dataFiles, data.FilePath)
			}
			for _, deletes := range files.PosDeleteFiles {
				deleteFiles = append(deleteFiles, deletes.FilePath)
			}
		}
	}

	stored := map[types.RowLocation][]string{}
	for _, path := range dataFiles {
		for position, row := range parquetRows(t, server.uploaded[path], constants.OlakeID, constants.OpType, "payload") {
			stored[types.RowLocation{FilePath: path, Position: int64(position)}] = row
		}
	}

	var deletedLocations []types.RowLocation
	for _, path := range deleteFiles {
		for _, row := range parquetRows(t, server.uploaded[path], "file_path", "pos") {
			position, err := strconv.ParseInt(row[1], 10, 64)
			require.NoError(t, err)
			deletedLocations = append(deletedLocations, types.RowLocation{FilePath: row[0], Position: position})
		}
	}

	var liveRows [][]string
	for loc, row := range stored {
		if !slices.Contains(deletedLocations, loc) {
			liveRows = append(liveRows, row)
		}
	}
	return liveRows
}

func collectLiveRows(t *testing.T, w *ArrowWriter, server *uploadServer) [][]string {
	t.Helper()
	return collectLiveRowsFromWriters(t, server, w)
}

// TestReinsertDeletedPrimaryKeyLeavesNoTombstoneLive reproduces and verifies the fix for issue #1286:
// When a primary key is deleted, a tombstone row (_op_type = 'd') is written and indexed.
// When that same primary key is re-inserted ('c'), the writer must emit a positional delete
// against the tombstone row so only the re-inserted row remains live.
func TestReinsertDeletedPrimaryKeyLeavesNoTombstoneLive(t *testing.T) {
	t.Run("successive batches", func(t *testing.T) {
		ctx := context.Background()
		w, server, _ := newTestWriter(t, nil)

		// 1. Insert key "1"
		require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v1")}))
		// 2. Delete key "1" (writes tombstone at pos 1, deletes pos 0)
		require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "d", "v1")}))
		// 3. Re-insert key "1" (should delete tombstone at pos 1, write new row at pos 2)
		require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v2")}))

		require.NoError(t, w.Close(ctx, nil))

		liveRows := collectLiveRows(t, w, server)
		assert.Len(t, liveRows, 1, "expected exactly 1 live row after re-insert, but found soft-deleted tombstone still live")
		if len(liveRows) == 1 {
			assert.Equal(t, []string{"1", "c", "v2"}, liveRows[0])
		}
	})

	t.Run("same batch", func(t *testing.T) {
		ctx := context.Background()
		w, server, _ := newTestWriter(t, nil)

		// c -> d -> c all within a single Write batch
		require.NoError(t, w.Write(ctx, []types.RawRecord{
			testRecord("1", "c", "v1"),
			testRecord("1", "d", "v1"),
			testRecord("1", "c", "v2"),
		}))

		require.NoError(t, w.Close(ctx, nil))

		liveRows := collectLiveRows(t, w, server)
		assert.Len(t, liveRows, 1, "expected exactly 1 live row after re-insert in single batch")
		if len(liveRows) == 1 {
			assert.Equal(t, []string{"1", "c", "v2"}, liveRows[0])
		}
	})
}

// TestReinsertDeletedPrimaryKeyAcrossWriters verifies that when a delete occurs in one sync cycle / writer
// and re-insert occurs in a subsequent writer sharing the index, the tombstone in the earlier file is deleted.
func TestReinsertDeletedPrimaryKeyAcrossWriters(t *testing.T) {
	ctx := context.Background()
	server := &uploadServer{uploaded: map[string][]byte{}}
	updateType := "pos"
	stream := &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:                    "t",
			Namespace:               "n",
			SourceDefinedPrimaryKey: types.NewSet("id"),
		},
		StreamMetadata: types.StreamMetadata{UpdateType: &updateType},
	}
	index := types.NewStreamIndexThread(committedIndex{})
	schema := map[string]string{"_olake_id": "string", "_op_type": "string", "_olake_timestamp": "timestamptz", "payload": "string"}

	// Writer 1: inserts then deletes key "1", then closes
	w1, err := New(ctx, &destination.Options{ThreadID: "th1", TableIndex: committedIndex{}}, nil, schema, stream, server, true, index)
	require.NoError(t, err)
	require.NoError(t, w1.Write(ctx, []types.RawRecord{testRecord("1", "c", "v1")}))
	require.NoError(t, w1.Write(ctx, []types.RawRecord{testRecord("1", "d", "v1")}))
	require.NoError(t, w1.Close(ctx, nil))

	// Writer 2: re-inserts key "1" in a new sync cycle using the shared index thread
	w2, err := New(ctx, &destination.Options{ThreadID: "th2", TableIndex: committedIndex{}}, nil, schema, stream, server, true, index)
	require.NoError(t, err)
	require.NoError(t, w2.Write(ctx, []types.RawRecord{testRecord("1", "c", "v2")}))
	require.NoError(t, w2.Close(ctx, nil))

	liveRows := collectLiveRowsFromWriters(t, server, w1, w2)
	assert.Len(t, liveRows, 1, "expected exactly 1 live row across writer cycles")
	if len(liveRows) == 1 {
		assert.Equal(t, []string{"1", "c", "v2"}, liveRows[0])
	}
}

// TestMultipleReinsertsOfDeletedPrimaryKey verifies c -> d -> c -> c leaves only the latest row live.
func TestMultipleReinsertsOfDeletedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	w, server, _ := newTestWriter(t, nil)

	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v1")}))
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "d", "v1")}))
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v2")}))
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v3")}))
	require.NoError(t, w.Close(ctx, nil))

	liveRows := collectLiveRows(t, w, server)
	assert.Len(t, liveRows, 1, "expected exactly 1 live row after multiple re-inserts")
	if len(liveRows) == 1 {
		assert.Equal(t, []string{"1", "c", "v3"}, liveRows[0])
	}
}

// TestPKLessStreamPreservesDuplicateInserts verifies that tables without a primary key do not
// emit positional deletes for 'c' records, preserving SQL multiset (duplicate insert) semantics.
func TestPKLessStreamPreservesDuplicateInserts(t *testing.T) {
	ctx := context.Background()
	server := &uploadServer{uploaded: map[string][]byte{}}
	updateType := "pos"
	// Stream without primary key
	stream := &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:                    "t",
			Namespace:               "n",
			SourceDefinedPrimaryKey: types.NewSet[string](),
		},
		StreamMetadata: types.StreamMetadata{UpdateType: &updateType},
	}
	index := types.NewStreamIndexThread(committedIndex{})
	schema := map[string]string{"_olake_id": "string", "_op_type": "string", "_olake_timestamp": "timestamptz", "payload": "string"}
	w, err := New(ctx, &destination.Options{ThreadID: "th", TableIndex: committedIndex{}}, nil, schema, stream, server, true, index)
	require.NoError(t, err)

	// Three identical inserts
	require.NoError(t, w.Write(ctx, []types.RawRecord{
		testRecord("hash1", "c", "dup"),
		testRecord("hash1", "c", "dup"),
		testRecord("hash1", "c", "dup"),
	}))
	require.NoError(t, w.Close(ctx, nil))

	liveRows := collectLiveRows(t, w, server)
	// All 3 duplicate inserts must remain live (no deletes emitted)
	assert.Len(t, liveRows, 3, "PK-less stream must preserve duplicate inserts as separate live rows")
	for _, files := range w.createdFiles {
		assert.Empty(t, files.PosDeleteFiles, "PK-less stream should not emit positional delete files for 'c' records")
	}
}

// TestReplayedInsertOnLiveKeyDeduplicates verifies that if a 'c' record is replayed against an
// existing live key without an intervening delete (e.g. CDC replay), the prior row is deleted.
func TestReplayedInsertOnLiveKeyDeduplicates(t *testing.T) {
	ctx := context.Background()
	w, server, _ := newTestWriter(t, nil)

	// 1. Initial insert
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v1")}))
	// 2. Replayed insert of the same key without a delete
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v2")}))
	require.NoError(t, w.Close(ctx, nil))

	liveRows := collectLiveRows(t, w, server)
	assert.Len(t, liveRows, 1, "replayed insert on live key must supersede prior row")
	if len(liveRows) == 1 {
		assert.Equal(t, []string{"1", "c", "v2"}, liveRows[0])
	}
}

// TestEqualityDeleteModeUnchanged verifies that equality delete mode (when indexThread is nil)
// behaves unchanged and produces equality delete files.
func TestEqualityDeleteModeUnchanged(t *testing.T) {
	ctx := context.Background()
	server := &uploadServer{uploaded: map[string][]byte{}}
	updateType := "pos"
	stream := &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:                    "t",
			Namespace:               "n",
			SourceDefinedPrimaryKey: types.NewSet("id"),
		},
		StreamMetadata: types.StreamMetadata{UpdateType: &updateType},
	}
	schema := map[string]string{"_olake_id": "string", "_op_type": "string", "_olake_timestamp": "timestamptz", "payload": "string"}
	// indexThread == nil triggers equality delete path
	w, err := New(ctx, &destination.Options{ThreadID: "th", TableIndex: committedIndex{}}, nil, schema, stream, server, true, nil)
	require.NoError(t, err)

	require.NoError(t, w.Write(ctx, []types.RawRecord{
		testRecord("1", "c", "v1"),
		testRecord("1", "d", "v1"),
		testRecord("1", "c", "v2"),
	}))
	require.NoError(t, w.Close(ctx, nil))

	var eqDeleteFiles []string
	for _, files := range w.createdFiles {
		for _, eq := range files.EqDeleteFiles {
			eqDeleteFiles = append(eqDeleteFiles, eq.FilePath)
		}
	}
	assert.NotEmpty(t, eqDeleteFiles, "equality delete mode should produce equality delete files")
}

// TestDeletionVectorModeReinsertDeletedPrimaryKey verifies that in deletion vector mode ("dv"),
// re-inserting a deleted primary key queues and sends a deletion vector for the tombstone position.
func TestDeletionVectorModeReinsertDeletedPrimaryKey(t *testing.T) {
	ctx := context.Background()
	server := &uploadServer{uploaded: map[string][]byte{}}
	updateType := string(types.UpdateTypeDeletionVector)
	stream := &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:                    "t",
			Namespace:               "n",
			SourceDefinedPrimaryKey: types.NewSet("id"),
		},
		StreamMetadata: types.StreamMetadata{UpdateType: &updateType},
	}
	index := types.NewStreamIndexThread(committedIndex{})
	schema := map[string]string{"_olake_id": "string", "_op_type": "string", "_olake_timestamp": "timestamptz", "payload": "string"}
	w, err := New(ctx, &destination.Options{ThreadID: "th", TableIndex: committedIndex{}}, nil, schema, stream, server, true, index)
	require.NoError(t, err)

	// 1. Insert key "1" (pos 0)
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v1")}))
	// 2. Delete key "1" (pos 1, queues DV for pos 0)
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "d", "v1")}))
	// 3. Re-insert key "1" (pos 2, queues DV for pos 1)
	require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "v2")}))

	require.NoError(t, w.Close(ctx, nil))

	// Verify deletion vectors sent to server
	require.NotEmpty(t, server.deletionVectors, "deletion vectors must be sent to server in dv mode")
	var deletedPositions []int64
	for _, batch := range server.deletionVectors {
		for _, entry := range batch.GetEntries() {
			deletedPositions = append(deletedPositions, entry.GetPositions()...)
		}
	}
	// Positions 0 (original insert) and 1 (tombstone) must both be deleted!
	assert.Contains(t, deletedPositions, int64(0), "original insert position 0 must be deleted")
	assert.Contains(t, deletedPositions, int64(1), "tombstone position 1 must be deleted by the re-insert")
}
