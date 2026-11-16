package testhandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/datazip-inc/olake/types"
)

// restCatalogService is Apache Iceberg's REST catalog fixture. It reaches MinIO over the shared
// network, while the writer on the host uses MinIO's published port.
var restCatalogService = &service{
	name:  "iceberg-rest",
	image: "apache/iceberg-rest-fixture:1.10.1",
	port:  8181,
	env: []string{
		"CATALOG_WAREHOUSE=s3://" + bucket + "/iceberg_rest/",
		"CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
		"CATALOG_S3_ENDPOINT=http://" + minioService.containerName() + ":9000",
		"CATALOG_S3_PATH__STYLE__ACCESS=true",
		"AWS_ACCESS_KEY_ID=" + serverUser,
		"AWS_SECRET_ACCESS_KEY=" + serverKey,
		"AWS_REGION=" + region,
	},
	needs:   []*service{minioService},
	startup: 2 * time.Minute,
	ready:   httpReady("/v1/config"),
}

// IcebergREST writes Iceberg tables through a REST catalog.
var IcebergREST = &DestinationKind{
	name:   "iceberg-rest",
	writer: types.Iceberg,
	open: func(h *Handler) (map[string]any, func(context.Context) error) {
		minio, catalog := h.start(minioService), h.start(restCatalogService)
		// The fixture serves one warehouse and ignores the path a client names, but the writer needs one.
		config := icebergConfig(minio, "s3://"+bucket+"/iceberg_rest", map[string]any{
			"catalog_type":     "rest",
			"rest_catalog_url": "http://" + catalog.addr(),
		})
		drop := func(ctx context.Context) error {
			return errors.Join(dropRESTNamespace(ctx, catalog, h.id), removePrefix(ctx, minio, "iceberg_rest/"+h.id))
		}
		return config, drop
	},
}

// dropRESTNamespace purges every table of a namespace and then drops the namespace.
func dropRESTNamespace(ctx context.Context, catalog *container, namespace string) error {
	namespaceURL := "http://" + catalog.addr() + "/v1/namespaces/" + namespace
	status, body, err := request(ctx, http.MethodGet, namespaceURL+"/tables", nil, "")
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status != http.StatusOK {
		return fmt.Errorf("list tables of %s returned status %d: %s", namespace, status, body)
	}
	var listing struct {
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
	}
	if err := json.Unmarshal([]byte(body), &listing); err != nil {
		return fmt.Errorf("list tables of %s: %s: %w", namespace, body, err)
	}
	for _, table := range listing.Identifiers {
		if err := restDelete(ctx, namespaceURL+"/tables/"+table.Name+"?purgeRequested=true"); err != nil {
			return err
		}
	}
	return restDelete(ctx, namespaceURL)
}

func restDelete(ctx context.Context, url string) error {
	status, body, err := request(ctx, http.MethodDelete, url, nil, "")
	if err == nil && status >= http.StatusMultipleChoices {
		err = fmt.Errorf("DELETE %s returned status %d: %s", url, status, body)
	}
	return err
}
