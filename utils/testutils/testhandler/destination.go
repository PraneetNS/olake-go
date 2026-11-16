package testhandler

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

// DestinationKind is one destination a test can ask for. The kinds of this package are the only ones.
type DestinationKind struct {
	name   string
	writer types.DestinationType
	// open starts what the kind needs and returns its writer config and the drop of one test's data.
	open func(h *Handler) (config map[string]any, drop func(ctx context.Context) error)
}

// Destination is a writer config and the destination database that isolates one test in it.
type Destination struct {
	// Config is the same for every test of a kind, so it survives the writers' per-process caches.
	Config *types.WriterConfig
	// Database must be the destination database of every stream the test writes.
	// It is the Iceberg namespace or the Parquet folder that cleanup removes.
	Database string
}

// Destination returns the writer config of a kind and this test's own destination database in it.
// Cleanup removes that database: its catalog entries and its files.
func (h *Handler) Destination(kind *DestinationKind) *Destination {
	h.t.Helper()
	return once(h, kind.name, func() *Destination {
		if kind.writer == types.Iceberg {
			h.leaseIcebergWriter()
		}
		config, drop := kind.open(h)
		if kind.writer == types.Parquet {
			h.pinParquet(kind)
		}
		h.cleanup(kind.name+" destination", drop)
		return &Destination{
			Config:   &types.WriterConfig{Type: kind.writer, WriterConfig: config},
			Database: h.id,
		}
	})
}
