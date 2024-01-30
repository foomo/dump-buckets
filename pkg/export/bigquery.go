package export

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const (
	bigqueryGCSURIFormat = "gs://%s/%s/%s/%d/*.parquet.gz"
)

type BigQueryExportConfig struct {
	BucketName  string
	DatasetName string
	Dataset     string
	ProjectID   string
	GCSLocation string
}

type BigQueryExport struct {
	config BigQueryExportConfig
	client *bigquery.Client
}

func NewBigQueryExport(ctx context.Context, config BigQueryExportConfig) (*BigQueryExport, error) {
	client, err := bigquery.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %v", err)
	}
	return &BigQueryExport{
		config: config,
		client: client,
	}, nil
}

func (bqe *BigQueryExport) Export(ctx context.Context) error {
	// Get all table names from the configured dataset
	tableIterator := bqe.client.Dataset(bqe.config.Dataset).Tables(ctx)
	var tables []*bigquery.Table
	for {
		t, err := tableIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		tables = append(tables, t)
	}

	exportTimestamp := time.Now().Unix()
	// Run exportTableAsCompressedParquet for all tables and log
	for _, t := range tables {
		gcsURI := fmt.Sprintf(bigqueryGCSURIFormat, bqe.config.BucketName, t.DatasetID, t.TableID, exportTimestamp)
		err := bqe.exportTableAsCompressedParquet(ctx, t, gcsURI)
		if err != nil {
			return fmt.Errorf("failed to export to %q :%w", gcsURI, err)
		}
	}

	return nil
}

// exportTableAsCompressedParquet demonstrates using an export job to
// write the contents of a table into Cloud Storage as compressed CSV.
func (bqe *BigQueryExport) exportTableAsCompressedParquet(ctx context.Context, table *bigquery.Table, gcsURI string) error {
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.Compression = bigquery.Gzip
	gcsRef.DestinationFormat = bigquery.Parquet

	extractor := bqe.client.DatasetInProject(table.ProjectID, table.DatasetID).Table(table.TableID).ExtractorTo(gcsRef)
	// You can choose to run the job in a specific location for more complex data locality scenarios.
	extractor.Location = bqe.config.GCSLocation

	job, err := extractor.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	return nil
}
