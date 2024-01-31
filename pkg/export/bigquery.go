package export

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
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
	bigqueryTableDateSufixRegex = regexp.MustCompile(`^.+_(\d{8})$`)
	bigqueryTableDateFormat     = "20060102"
)

type BigQueryDatasetExportConfig struct {
	BucketName      string
	ProjectID       string
	GCSLocation     string
	FilterAfter     time.Time
	ExcludePatterns []string
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

func (bqe *BigQueryDatasetExport) Export(ctx context.Context, l *slog.Logger) (string, error) {
	if l == nil {
		l = slog.Default()
	}
	// get all datasets
	datasetIterator := bqe.client.Datasets(ctx)
	var outputPaths []string
	for {
		dataset, err := datasetIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to iterate datasets: %w", err)
		}
		l := l.With("dataset", dataset.DatasetID)
		outputPath, err := bqe.exportDataset(ctx, l, dataset)
		if err != nil {
			// Continue exporting other datasets
			l.Error("Failed to export dataset, continuing dump...", slog.Any("error", err), slog.String("dataset", dataset.DatasetID))
			continue
		}
		outputPaths = append(outputPaths, outputPath)
	}
	return "", nil
}

func (bqe *BigQueryDatasetExport) exportDataset(ctx context.Context, l *slog.Logger, dataset *bigquery.Dataset) (string, error) {
	tableIterator := dataset.Tables(ctx)

	var tables []*bigquery.Table
	var tableNames []string
	for {
		t, err := tableIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to iterate dataset %w", err)
		}
		excluded, err := isTableExcluded(t, bqe.config.ExcludePatterns, bqe.config.FilterAfter)
		if err != nil {
			return "", fmt.Errorf("failed to check if table is excluded: %w", err)
		}
		if excluded == true {
			continue
		}
		md, err := t.Metadata(ctx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
		if err != nil {
			return "", fmt.Errorf("failed to get table metadata: %w", err)
		}
		if md.Type != bigquery.RegularTable {
			continue
		}
		tables = append(tables, t)
		tableNames = append(tableNames, t.TableID)
	}

	if len(tables) == 0 {
		l.Info("No tables on dataset or all tables are excluded")
		return "", nil
	}
	l.Info("Starting export...", slog.Any("tables", strings.Join(tableNames, ", ")))

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
	return fmt.Sprintf(bigqueryGCSURIFormat, bqe.config.BucketName, dataset.DatasetID, exportTimestamp, "*"), g.Wait()
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

func isTableExcluded(t *bigquery.Table, patterns []string, excludeBefore time.Time) (bool, error) {
	tableKey := fmt.Sprintf("%s.%s", t.DatasetID, t.TableID)
	for _, p := range patterns {
		matched, err := filepath.Match(p, tableKey)
		if err != nil {
			return false, fmt.Errorf("pattern %s is malformed: %w", p, err)
		}

		if matched {
			return true, nil
		}
	}

	dateSuffix := bigqueryTableDateSufixRegex.FindStringSubmatch(t.TableID)
	if len(dateSuffix) > 0 {
		tableDate, err := time.Parse(bigqueryTableDateFormat, dateSuffix[1])
		if err != nil {
			return false, fmt.Errorf("table date format is invalid: %w", err)
		}

		if tableDate.Before(excludeBefore) {
			return true, nil
		}
	}
	return false, nil
}
