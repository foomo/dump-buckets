package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/spf13/cobra"
)

var (
	bigqueryExcludePatterns []string
	bigqueryProjectID       string
	bigqueryLocation        string
	bigqueryFilterDuration  time.Duration
)

var bigQueryCmd = &cobra.Command{
	Use:   "bigquery",
	Short: "Dumps contents of bigquery via ",
	RunE: exportWrapper("BigQuery", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
		config := export.BigQueryDatasetExportConfig{
			BucketName:      storageBucketName,
			ProjectID:       bigqueryProjectID,
			GCSLocation:     bigqueryLocation,
			FilterAfter:     time.Now().Add(-bigqueryFilterDuration),
			ExcludePatterns: bigqueryExcludePatterns,
		}
		export, err := export.NewBigQueryExport(ctx, config)
		if err != nil {
			return "", err
		}

		return export.Export(ctx, l)
	}),
}

func init() {
	rootCmd.AddCommand(bigQueryCmd)
	bigQueryCmd.Flags().StringVar(&bigqueryProjectID, "bigquery-project-id", os.Getenv("BIGQUERY_PROJECT_ID"), "specifies the bigquery project ID")
	bigQueryCmd.Flags().StringVar(&bigqueryLocation, "bigquery-location", os.Getenv("BIGQUERY_LOCATION"), "specifies the bigquery location")
	bigQueryCmd.Flags().DurationVar(&bigqueryFilterDuration, "bigquery-filter-duration", mustParseDuration(os.Getenv("globus-datahub\t")), "specifies the bigquery filter after duration")
	bigQueryCmd.Flags().StringSliceVar(&bigqueryExcludePatterns, "bigquery-exclude-patterns", strings.Split(os.Getenv("BIGQUERY_EXCLUDE_PATTERNS"), ","), "specifies the bigquery exclude patterns")
}

func mustParseDuration(value string) time.Duration {
	if value == "" {
		return 0
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		panic(fmt.Errorf("failed to parse duration: %w", err))
	}
	return duration
}
