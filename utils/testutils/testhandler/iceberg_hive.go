package testhandler

import (
	"context"
	"errors"
	"time"

	"github.com/datazip-inc/olake/types"
)

// hiveMetastoreService is a standalone Hive metastore on embedded Derby. It writes namespace
// folders to MinIO itself, so it gets the S3A jars and endpoint; path-style access has no env switch.
var hiveMetastoreService = &service{
	name:  "hive-metastore",
	image: "apache/hive:4.2.0",
	port:  9083,
	env: []string{
		"SERVICE_NAME=metastore",
		"HADOOP_OPTIONAL_TOOLS=hadoop-aws",
		"S3_ENDPOINT_URL=http://" + minioService.containerName() + ":9000",
		"AWS_ACCESS_KEY_ID=" + serverUser,
		"AWS_SECRET_ACCESS_KEY=" + serverKey,
	},
	entrypoint: "bash",
	args: []string{"-c", `sed -i "s|</configuration>|<property><name>fs.s3a.path.style.access</name><value>true</value></property></configuration>|" ` +
		`/opt/hive/conf/core-site.xml.template && exec /entrypoint.sh`},
	needs:   []*service{minioService},
	startup: 5 * time.Minute,
	// Probed inside the container: Docker's port proxy accepts host connections before the server listens.
	ready: func(ctx context.Context, c *container) error {
		_, err := c.exec(ctx, nil, "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9083")
		return err
	},
}

// IcebergHive writes Iceberg tables through a Hive metastore.
var IcebergHive = &DestinationKind{
	name:   "iceberg-hive",
	writer: types.Iceberg,
	open: func(h *Handler) (map[string]any, func(context.Context) error) {
		minio, metastore := h.start(minioService), h.start(hiveMetastoreService)
		// hive_clients must be set: the writer sends 0 otherwise and then never reaches the metastore.
		config := icebergConfig(minio, "s3a://"+bucket+"/iceberg_hive", map[string]any{
			"catalog_type":      "hive",
			"hive_uri":          "thrift://" + metastore.addr(),
			"hive_clients":      5,
			"hive_sasl_enabled": false,
		})
		drop := func(ctx context.Context) error {
			_, err := beeline(ctx, metastore, "DROP DATABASE IF EXISTS "+h.id+" CASCADE")
			return errors.Join(err, removePrefix(ctx, minio, "iceberg_hive/"+h.id+".db"))
		}
		return config, drop
	},
}

// beeline runs one statement on the metastore with the beeline bundled in its image.
// Beeline needs a terminal, and it must be pointed at the running metastore or it opens its own Derby.
func beeline(ctx context.Context, metastore *container, statement string) (string, error) {
	flags := []string{"--tty", "--env", "HADOOP_CLIENT_OPTS=-Dhive.metastore.uris=thrift://localhost:9083"}
	return metastore.exec(ctx, flags, "beeline", "-u", "jdbc:hive2://", "-e", statement)
}
