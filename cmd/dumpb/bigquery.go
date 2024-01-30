package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/spf13/cobra"
)

var (
	bigqueryDatasetName    string
	bigqueryProjectID      string
	bigqueryLocation       string
	bigqueryFilterDuration time.Duration
)

var bigQueryCmd = &cobra.Command{
	Use:   "bigquery",
	Short: "Dumps contents of bigquery via ",
	RunE: exportWrapper("BigQuery", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {

		config := export.BigQueryDatasetExportConfig{
			BucketName:  storageBucketName,
			DatasetName: bigqueryDatasetName,
			ProjectID:   bigqueryProjectID,
			GCSLocation: bigqueryLocation,
			FilterAfter: time.Now().Add(-bigqueryFilterDuration),
		}
		export, err := export.NewBigQueryExport(ctx, config)
		if err != nil {
			return "", err
		}

		return export.Export(ctx)
	}),
}

func init() {
	rootCmd.AddCommand(bigQueryCmd)
	bigQueryCmd.Flags().StringVar(&bigqueryDatasetName, "bigquery-dataset-name", os.Getenv("BIGQUERY_DATASET_NAME"), "specifies the bigquery dataset name")
	bigQueryCmd.Flags().StringVar(&bigqueryProjectID, "bigquery-project-id", os.Getenv("BIGQUERY_PROJECT_ID"), "specifies the bigquery project ID")
	bigQueryCmd.Flags().StringVar(&bigqueryLocation, "bigquery-location", os.Getenv("BIGQUERY_LOCATION"), "specifies the bigquery location")
	bigQueryCmd.Flags().DurationVar(&bigqueryFilterDuration, "bigquery-filter-duration", mustParseDuration(os.Getenv("globus-datahub\t")), "specifies the bigquery filter after duration")
}

func mustParseDuration(value string) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		panic(fmt.Errorf("failed to parse duration: %w", err))
	}
	return duration
}
