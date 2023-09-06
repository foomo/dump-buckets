package dumpb

import (
	"compress/gzip"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/foomo/dump-buckets/pkg/storage"
	"github.com/spf13/cobra"
)

var (
	mongoURI string
)

var mongoCmd = &cobra.Command{
	Use:   "mongo",
	Short: "Dumps mongo into a bucket",
	RunE: func(cmd *cobra.Command, args []string) error {
		start := time.Now()
		l := slog.With(
			slog.String("bucketName", storageBucketName),
			slog.String("bucketVendor", storageBucketVendor),
		)
		ctx := cmd.Context()
		gcs, err := storage.NewGCSStorage(ctx, storageBucketName)
		if err != nil {
			return err
		}

		exporter, err := export.NewMongoExport(
			ctx,
			export.MongoExportConfig{MongoURI: mongoURI},
			gcs,
		)
		if err != nil {
			return err
		}

		exportName := fmt.Sprintf("%s.archive.gz", time.Now().Format(time.RFC3339))
		exportPath := filepath.Join(storageBucketPath, exportName)
		l = l.With(slog.String("path", exportPath))
		l.Info("MongoDB export started")
		writer, err := gcs.NewWriter(ctx, exportPath)
		if err != nil {
			return err
		}

		defer writer.Close()

		gzipWriter := gzip.NewWriter(writer)
		defer gzipWriter.Close()

		err = exporter.Export(ctx, gzipWriter)
		if err != nil {
			return err
		}
		l.Info("MongoDB export complete", slog.Any("duration", time.Since(start).Seconds()))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(mongoCmd)
	mongoCmd.Flags().StringVar(&mongoURI, "mongo-uri", "", "specifies the mongo uri dump")
}
