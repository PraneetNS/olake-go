package testhandler

import (
	"context"
	"time"
)

const (
	bucket = "warehouse"
	region = "us-east-1"
)

// minioService is the object store behind Parquet on S3 and behind every Iceberg warehouse.
var minioService = &service{
	name:    "minio",
	image:   "olakego/minio:RELEASE.2025-04-03T14-56-28Z",
	port:    9000,
	env:     []string{"MINIO_ROOT_USER=" + serverUser, "MINIO_ROOT_PASSWORD=" + serverKey},
	args:    []string{"server", "/data"},
	startup: time.Minute,
	ready: func(ctx context.Context, c *container) error {
		_, err := mc(ctx, c, "mb", "--ignore-existing", "local/"+bucket)
		return err
	},
}

// mc runs the MinIO client bundled in the server image against the server itself.
func mc(ctx context.Context, c *container, args ...string) (string, error) {
	flags := []string{"--env", "MC_HOST_local=http://" + serverUser + ":" + serverKey + "@127.0.0.1:9000"}
	return c.exec(ctx, flags, append([]string{"mc"}, args...)...)
}

// removePrefix deletes every object under a key prefix of the shared bucket.
func removePrefix(ctx context.Context, minio *container, prefix string) error {
	_, err := mc(ctx, minio, "rm", "--recursive", "--force", "local/"+bucket+"/"+prefix+"/")
	return err
}
