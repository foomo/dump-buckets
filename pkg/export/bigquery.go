package export

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
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
	// The format of the URI is "gs://{bucket}/{timestamp}/{dataset}/{table}//*.parquet.gz", where:
	bigqueryGCSURIPrefix       = "gs://%s/%s"
	bigqueryQueryDataSetSchema = "SELECT * FROM region-%s.INFORMATION_SCHEMA.SCHEMATA"
	bigqueryQueryTableSchema   = "SELECT * FROM %s.%s.INFORMATION_SCHEMA.TABLES"
)

var (
	bigqueryTableDateSuffixRegex = regexp.MustCompile(`^.+_(\d{8})$`)
	bigqueryTableDateFormat      = "20060102"
)

type BigQueryDatasetExportConfig struct {
	BucketName      string
	ProjectID       string
	GCSLocation     string
	FilterAfter     time.Time
	ExcludePatterns []string
	Storage         Storage
}

type BigQueryDatasetExport struct {
	config BigQueryDatasetExportConfig
	client *bigquery.Client
}

func NewBigQueryExport(ctx context.Context, config BigQueryDatasetExportConfig) (*BigQueryDatasetExport, error) {
	client, err := bigquery.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
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
	exportTimestamp := time.Now().Format(TimestampFormat)
	bigqueryGCSURIPrefix := fmt.Sprintf(bigqueryGCSURIPrefix, bqe.config.BucketName, exportTimestamp)

	l.With(
		slog.Any("filterAfter", bqe.config.FilterAfter),
		slog.Any("excludePatterns", bqe.config.ExcludePatterns),
	).Info("Starting export")

	// Export Information Schemata
	schemaPath := path.Join(exportTimestamp, "INFORMATION_SCHEMA.SCHEMATA.json.gz")
	err := bqe.storeQueryResultAsGzippedJSON(ctx, schemaPath, fmt.Sprintf(bigqueryQueryDataSetSchema, bqe.config.GCSLocation))
	if err != nil {
		return "", fmt.Errorf("failed to store schemas: %w", err)
	}
	l.Info("Schema export complete", "path", schemaPath)
	// Region
	// get all datasets
	datasetIterator := bqe.client.Datasets(ctx)
	for {
		dataset, err := datasetIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to iterate datasets: %w", err)
		}

		bigqueryGCSURIDataSetPrefix := fmt.Sprintf("%s/%s", bigqueryGCSURIPrefix, dataset.DatasetID)
		l := l.With(
			slog.String("dataset", dataset.DatasetID),
			slog.String("path", bigqueryGCSURIDataSetPrefix),
		)
		// Export Dataset Schema
		tableSchemaPath := path.Join(exportTimestamp, dataset.DatasetID, "INFORMATION_SCHEMA.TABLES.json.gz")
		err = bqe.storeQueryResultAsGzippedJSON(ctx, path.Join(exportTimestamp, dataset.DatasetID, "INFORMATION_SCHEMA.TABLES.json.gz"), fmt.Sprintf(bigqueryQueryTableSchema, dataset.ProjectID, dataset.DatasetID))
		if err != nil {
			return "", fmt.Errorf("failed to store schema for dataset %s: %w", dataset.DatasetID, err)
		}
		l.Info("Table schema export complete", "path", tableSchemaPath)

		// Export Dataset Data
		err = bqe.exportDataset(ctx, l, dataset, bigqueryGCSURIDataSetPrefix)
		if err != nil {
			// Continue exporting other datasets
			l.Error("Failed to export dataset, continuing dump...", slog.Any("error", err), slog.String("dataset", dataset.DatasetID))
			continue
		}
		l.Info("Dataset export complete")
	}
	return bigqueryGCSURIPrefix, nil
}

func (bqe *BigQueryDatasetExport) exportDataset(ctx context.Context, l *slog.Logger, dataset *bigquery.Dataset, bigqueryGCSURIDataSetPrefix string) error {
	tableIterator := dataset.Tables(ctx)

	var tables []*bigquery.Table
	var tableNames []string
	for {
		t, err := tableIterator.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to iterate dataset %w", err)
		}
		excluded, err := isTableExcluded(t, bqe.config.ExcludePatterns, bqe.config.FilterAfter)
		if err != nil {
			return fmt.Errorf("failed to check if table is excluded: %w", err)
		}
		if excluded {
			continue
		}
		md, err := t.Metadata(ctx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
		if err != nil {
			return fmt.Errorf("failed to get table metadata: %w", err)
		}
		if md.Type != bigquery.RegularTable {
			continue
		}

		tables = append(tables, t)
		tableNames = append(tableNames, t.TableID)
	}

	if len(tables) == 0 {
		l.Info("No tables on dataset or all tables are excluded")
		return nil
	}
	l.Info("Starting export...", slog.Any("tables", strings.Join(tableNames, ", ")))

	g, groupCtx := errgroup.WithContext(ctx)
	// Run exportTableAsCompressedParquet for all tables and log
	for _, t := range tables {
		table := t
		g.Go(func() error {
			gcsURI := fmt.Sprintf("%s/%s/*.parquet.gz", bigqueryGCSURIDataSetPrefix, table.TableID)
			err := bqe.exportTableAsCompressedParquet(groupCtx, table, gcsURI)
			if err != nil {
				return fmt.Errorf("failed to export to table %q with URI %q :%w", table.TableID, gcsURI, err)
			}
			return nil
		})
	}
	return g.Wait()
}

func (bqe *BigQueryDatasetExport) storeQueryResultAsGzippedJSON(ctx context.Context, storagePath string, query string) error {
	writer, err := bqe.config.Storage.NewWriter(ctx, storagePath)
	if err != nil {
		return fmt.Errorf("failed to initialize writer: %w", err)
	}
	defer writer.Close()

	gzipWriter := gzip.NewWriter(writer)
	defer gzipWriter.Close()

	return bqe.executeQueryAndWriteJSON(ctx, query, gzipWriter)
}

func (bqe *BigQueryDatasetExport) executeQueryAndWriteJSON(ctx context.Context, query string, writer io.Writer) error {
	it, err := bqe.client.Query(query).Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch schema exports: %w", err)
	}
	var rows []map[string]bigquery.Value
	for {
		data := map[string]bigquery.Value{}

		err := it.Next(&data)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to iterate dataset: %w", err)
		}
		rows = append(rows, data)
	}

	return json.NewEncoder(writer).Encode(rows)
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

	dateSuffix := bigqueryTableDateSuffixRegex.FindStringSubmatch(t.TableID)
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
