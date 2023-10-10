package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
)

type exporterHandler func(ctx context.Context, l *slog.Logger, storage storageWriter) error

func exportWrapper(exporterName string, handler exporterHandler) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {

		start := time.Now()
		ctx := cmd.Context()
		l := slog.With(
			slog.String("exporterName", exporterName),
			slog.String("bucketName", storageBucketName),
			slog.String("bucketVendor", storageBucketVendor),
		)
		vendorStorage, err := configuredStorage(ctx)
		if err != nil {
			return fmt.Errorf("failed in configuring storage: %w", err)
		}

		err = handler(ctx, l, vendorStorage)
		if err != nil {
			return err
		}

		l.Info("Export complete", slog.Any("duration", time.Since(start).Seconds()))
		return nil
	}
}
