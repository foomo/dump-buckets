package dumpb

import (
	"context"
	"log/slog"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/spf13/cobra"
)

var bigQueryCmd = &cobra.Command{
	Use:   "bigquery",
	Short: "Dumps contents of bigquery via ",
	RunE: exportWrapper("BigQuery", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
		config := export.BigQueryDatasetExportConfig{
			BucketName:  "",
			DatasetName: "",
			ProjectID:   "",
			GCSLocation: "",
			FilterAfter: time.Time{},
		}
		export, err := export.NewBigQueryExport(ctx, config)
		if err != nil {
			return "", err
		}

		err := export.Export(ctx)

		return "", nil
	}),
}
