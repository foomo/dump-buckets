package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
)

type exporterHandler func(ctx context.Context, l *slog.Logger, storage storageWriter) (outputPath string, err error)

func exportWrapper(exporterName string, handler exporterHandler) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		start := time.Now()
		ctx := cmd.Context()
		l := slog.With(
			slog.String("exporterName", exporterName),
			slog.String("bucketName", storageBucketName),
			slog.String("bucketVendor", storageBucketVendor),
		)
		l.Info("Configuring storage for vendor provider...")
		vendorStorage, err := configuredStorage(ctx)
		if err != nil {
			return fmt.Errorf("failed in configuring storage: %w", err)
		}

		l.Info("Starting exporter...")
		path, err := handler(ctx, l, vendorStorage)
		if err != nil {
			return err
		}

		l.With(slog.String("path", path)).Info("Export complete", slog.Any("duration", time.Since(start).Seconds()))
		return nil
	}
}
