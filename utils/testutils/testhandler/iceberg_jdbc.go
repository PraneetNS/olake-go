package testhandler

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/types"
)

const jdbcCatalogDatabase = "olake_testhandler_catalog"

// IcebergJDBC writes Iceberg tables through a JDBC catalog kept in the Postgres container.
var IcebergJDBC = &DestinationKind{
	name:   "iceberg-jdbc",
	writer: types.Iceberg,
	open: func(h *Handler) (map[string]any, func(context.Context) error) {
		minio, postgres := h.start(minioService), h.start(postgresService)
		ctx := h.t.Context()
		// Another process may create the catalog database at the same moment, so only its absence is an error.
		_, createErr := psql(ctx, postgres, "postgres", `CREATE DATABASE `+jdbcCatalogDatabase)
		found, err := psql(ctx, postgres, "postgres", `SELECT count(*) FROM pg_database WHERE datname = '`+jdbcCatalogDatabase+`'`)
		if err != nil || found != "1" {
			h.t.Fatalf("testhandler: no JDBC catalog database: %v", errors.Join(createErr, err))
		}
		config := icebergConfig(minio, "s3a://"+bucket+"/iceberg_jdbc", map[string]any{
			"catalog_type":  "jdbc",
			"jdbc_url":      fmt.Sprintf("jdbc:postgresql://%s/%s", postgres.addr(), jdbcCatalogDatabase),
			"jdbc_username": "postgres",
			"jdbc_password": serverKey,
		})
		drop := func(ctx context.Context) error {
			return errors.Join(dropJDBCNamespace(ctx, postgres, h.id), removePrefix(ctx, minio, "iceberg_jdbc/"+h.id))
		}
		return config, drop
	},
}

// dropJDBCNamespace removes a namespace and its tables from the catalog's two tables.
// The catalog creates those tables on first use, so they may not exist yet.
func dropJDBCNamespace(ctx context.Context, postgres *container, namespace string) error {
	_, err := psql(ctx, postgres, jdbcCatalogDatabase, `DO $$ BEGIN
		IF to_regclass('iceberg_tables') IS NOT NULL THEN
			DELETE FROM iceberg_tables WHERE table_namespace = '`+namespace+`';
		END IF;
		IF to_regclass('iceberg_namespace_properties') IS NOT NULL THEN
			DELETE FROM iceberg_namespace_properties WHERE namespace = '`+namespace+`';
		END IF;
	END $$`)
	return err
}
