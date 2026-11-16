package testhandler

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/datazip-inc/olake/types"
)

// ParquetLocal writes Parquet files under a local folder.
var ParquetLocal = &DestinationKind{
	name:   "parquet-local",
	writer: types.Parquet,
	open: func(h *Handler) (map[string]any, func(context.Context) error) {
		root := filepath.Join(os.TempDir(), "olake-testhandler", "parquet")
		drop := func(context.Context) error {
			return os.RemoveAll(filepath.Join(root, h.id))
		}
		return map[string]any{"local_path": root}, drop
	},
}

// ParquetMinIO writes Parquet files to an S3 bucket served by a MinIO container.
var ParquetMinIO = &DestinationKind{
	name:   "parquet-minio",
	writer: types.Parquet,
	open: func(h *Handler) (map[string]any, func(context.Context) error) {
		minio := h.start(minioService)
		config := map[string]any{
			"s3_bucket":     bucket,
			"s3_region":     region,
			"s3_access_key": serverUser,
			"s3_secret_key": serverKey,
			"s3_endpoint":   "http://" + minio.addr(),
			"s3_path":       "parquet",
		}
		drop := func(ctx context.Context) error {
			return removePrefix(ctx, minio, "parquet/"+h.id)
		}
		return config, drop
	},
}

// parquetPin remembers the first Parquet kind this process handed out.
var parquetPin struct {
	sync.Mutex
	kind *DestinationKind
	test string
}

// pinParquet fails a test that asks for a second Parquet kind in one test binary. The Parquet writer
// keeps its first config for the life of the process, so the second kind would write to the first.
func (h *Handler) pinParquet(kind *DestinationKind) {
	h.t.Helper()
	parquetPin.Lock()
	defer parquetPin.Unlock()
	if parquetPin.kind == nil {
		parquetPin.kind, parquetPin.test = kind, h.t.Name()
	}
	if parquetPin.kind != kind {
		h.t.Fatalf("testhandler: %s already uses %s in this test binary, and the Parquet writer keeps its first config for the whole process; use %s from another package",
			parquetPin.test, parquetPin.kind.name, kind.name)
	}
}
