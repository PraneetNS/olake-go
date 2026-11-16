package testhandler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/datazip-inc/olake/types"
)

// glueService is Moto, which emulates the AWS Glue API in memory and checks no signatures.
var glueService = &service{
	name:    "glue",
	image:   "motoserver/moto:5.2.5",
	port:    5000,
	startup: 2 * time.Minute,
	ready:   httpReady("/moto-api/"),
}

// IcebergGlue writes Iceberg tables through a Glue catalog served by an emulator, never by AWS.
var IcebergGlue = &DestinationKind{
	name:   "iceberg-glue",
	writer: types.Iceberg,
	open: func(h *Handler) (map[string]any, func(context.Context) error) {
		minio, glue := h.start(minioService), h.start(glueService)
		// Without glue_additional_config the writer ignores glue_endpoint and calls real AWS.
		config := icebergConfig(minio, "s3://"+bucket+"/iceberg_glue", map[string]any{
			"catalog_type":           "glue",
			"glue_additional_config": true,
			"glue_endpoint":          "http://" + glue.addr(),
			"glue_access_key":        serverUser,
			"glue_secret_key":        serverKey,
			"glue_region":            region,
		})
		drop := func(ctx context.Context) error {
			return errors.Join(dropGlueDatabase(ctx, glue, h.id), removePrefix(ctx, minio, "iceberg_glue/"+h.id+".db"))
		}
		return config, drop
	},
}

// dropGlueDatabase deletes a database from the emulator, which drops its tables with it.
func dropGlueDatabase(ctx context.Context, glue *container, database string) error {
	status, body, err := glueCall(ctx, glue, "DeleteDatabase", `{"Name":"`+database+`"}`)
	if err == nil && status != http.StatusOK && !strings.Contains(body, "EntityNotFoundException") {
		err = fmt.Errorf("glue DeleteDatabase %s returned status %d: %s", database, status, body)
	}
	return err
}

// glueCall sends one Glue API action to the emulator. The region in the credential scope selects its store.
func glueCall(ctx context.Context, glue *container, action, body string) (int, string, error) {
	return request(ctx, http.MethodPost, "http://"+glue.addr()+"/", map[string]string{
		"X-Amz-Target":  "AWSGlue." + action,
		"Content-Type":  "application/x-amz-json-1.1",
		"Authorization": "AWS4-HMAC-SHA256 Credential=" + serverUser + "/20260101/" + region + "/glue/aws4_request, SignedHeaders=host, Signature=0",
	}, body)
}
