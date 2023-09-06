package dumpb

import (
	"compress/gzip"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
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
		ctx := cmd.Context()

		exporter, err := export.NewMongoExport(ctx, export.MongoExportConfig{MongoURI: mongoURI})
		if err != nil {
			return err
		}
		vendorStorage, err := configuredStorage(ctx)
		if err != nil {
			return err
		}

		l := slog.With(
			slog.String("bucketName", storageBucketName),
			slog.String("bucketVendor", storageBucketVendor),
		)

		exportName := fmt.Sprintf("%s.archive.gz", time.Now().Format(time.RFC3339))
		exportPath := filepath.Join(storageBucketPath, exportName)
		l = l.With(slog.String("path", exportPath))
		l.Info("MongoDB export started")
		writer, err := vendorStorage.NewWriter(ctx, exportPath)
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
	mongoCmd.Flags().StringVar(&mongoURI, "mongo-uri", os.Getenv("MONGO_URI"), "specifies the mongo uri dump")
}
