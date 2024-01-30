package dumpb

import (
	"context"
	"log/slog"

	"github.com/spf13/cobra"
)

var bigQueryCmd = &cobra.Command{
	Use:   "bigquery",
	Short: "Dumps contents of bigquery via ",
	RunE: exportWrapper("Contentful", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
		return "", nil
	}),
}
