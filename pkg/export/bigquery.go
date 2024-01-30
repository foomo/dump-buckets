package export

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const (
	// bigqueryGCSURIFormat is a constant that represents the format of a Google Cloud Storage URI used for exporting BigQuery tables to compressed Parquet format.
	// The format of the URI is "gs://{bucket}/{dataset}/{table}/{timestamp}/*.parquet.gz", where:
	// - {bucket} is the name of the Google Cloud Storage bucket
	// - {dataset} is the name of the BigQuery dataset
	// - {table} is the name of the BigQuery table
	// - {timestamp} is the current Unix timestamp
	bigqueryGCSURIFormat = "gs://%s/%s/%d/%s/*.parquet.gz"
)

var (
	bigqueryTableDateSufixRegex = regexp.MustCompile(`.+_(\d+)$`)
	bigqueryTableDateFormat     = "20060102"
)

type BigQueryDatasetExportConfig struct {
	BucketName  string
	DatasetName string
	ProjectID   string
	GCSLocation string
	FilterAfter time.Time
}

type BigQueryDatasetExport struct {
	config BigQueryDatasetExportConfig
	client *bigquery.Client
}

func NewBigQueryExport(ctx context.Context, config BigQueryDatasetExportConfig) (*BigQueryDatasetExport, error) {
	client, err := bigquery.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %v", err)
	}

	return &BigQueryDatasetExport{
		config: config,
		client: client,
	}, nil
}

func (bqe *BigQueryDatasetExport) Export(ctx context.Context) (string, error) {
	// Get all table names from the configured dataset
	tableIterator := bqe.client.Dataset(bqe.config.DatasetName).Tables(ctx)
	var tables []*bigquery.Table
	for {
		t, err := tableIterator.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			} else {
				return "", fmt.Errorf("failed to iterate dataset %w", err)
			}
		}
		if isTableFiltered(t.TableID, bqe.config.FilterAfter) {
			continue
		}
		tables = append(tables, t)
	}

	exportTimestamp := time.Now().Unix()
	g, gctx := errgroup.WithContext(ctx)
	// Run exportTableAsCompressedParquet for all tables and log
	for _, t := range tables {
		table := t
		g.Go(func() error {
			gcsURI := fmt.Sprintf(bigqueryGCSURIFormat, bqe.config.BucketName, table.DatasetID, exportTimestamp, table.TableID)
			err := bqe.exportTableAsCompressedParquet(gctx, table, gcsURI)
			if err != nil {
				return fmt.Errorf("failed to export to table %q with URI %q :%w", table.TableID, gcsURI, err)
			}
			return nil
		})
	}
	return fmt.Sprintf(bigqueryGCSURIFormat, bqe.config.BucketName, bqe.config.DatasetName, exportTimestamp, "*"), g.Wait()
}

// exportTableAsCompressedParquet demonstrates using an export job to
// write the contents of a table into Cloud Storage as compressed CSV.
func (bqe *BigQueryDatasetExport) exportTableAsCompressedParquet(ctx context.Context, table *bigquery.Table, gcsURI string) error {
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

func isTableFiltered(tableName string, filterAfter time.Time) bool {
	dateSuffix := bigqueryTableDateSufixRegex.FindStringSubmatch(tableName)
	if len(dateSuffix) > 0 {
		tableDate, err := time.Parse(bigqueryTableDateFormat, dateSuffix[1])
		if err != nil {
			return false
		}

		if tableDate.Before(filterAfter) {
			return true
		}
	}
	return false
}
