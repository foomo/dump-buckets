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
	mongoURI                    string
	mongoAuthenticationDatabase string
	mongoUsername               string
	mongoPassword               string
)

var mongoCmd = &cobra.Command{
	Use:   "mongo",
	Short: "Dumps mongo into a bucket",
	RunE: func(cmd *cobra.Command, args []string) error {
		start := time.Now()
		ctx := cmd.Context()

		config := export.MongoExportConfig{
			MongoURI:               mongoURI,
			Username:               mongoUsername,
			Password:               mongoPassword,
			AuthenticationDatabase: mongoAuthenticationDatabase,
		}
		exporter, err := export.NewMongoExport(ctx, config)
		if err != nil {
			return fmt.Errorf("failed in initializing mongo exporter: %w", err)
		}
		vendorStorage, err := configuredStorage(ctx)
		if err != nil {
			return fmt.Errorf("failed in configuring storage: %w", err)
		}

		l := slog.With(
			slog.String("bucketName", storageBucketName),
			slog.String("bucketVendor", storageBucketVendor),
		)

		exportName := fmt.Sprintf("%s.%s.archive.gz", backupName, time.Now().Format(time.RFC3339))
		if backupName != "" {
			exportName += fmt.Sprintf("%s.%s", backupName, exportName)
		}
		exportPath := filepath.Join(storageBucketPath, exportName)
		l = l.With(slog.String("path", exportPath))
		l.Info("MongoDB export started")
		writer, err := vendorStorage.NewWriter(ctx, exportPath)
		if err != nil {
			return fmt.Errorf("failed to initialize writer: %w", err)
		}

		defer writer.Close()

		gzipWriter := gzip.NewWriter(writer)
		defer gzipWriter.Close()

		err = exporter.Export(ctx, gzipWriter)
		if err != nil {
			return fmt.Errorf("failed to export mongo data: %w", err)
		}
		l.Info("MongoDB export complete", slog.Any("duration", time.Since(start).Seconds()))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(mongoCmd)
	mongoCmd.Flags().StringVar(&mongoURI, "mongo-uri", os.Getenv("MONGO_URI"), "specifies the mongo uri dump")
	mongoCmd.Flags().StringVar(&mongoUsername, "mongo-username", os.Getenv("MONGO_USERNAME"), "specifies the mongo username")
	mongoCmd.Flags().StringVar(&mongoPassword, "mongo-password", os.Getenv("MONGO_PASSWORD"), "specifies the mongo password")
	mongoCmd.Flags().StringVar(&mongoAuthenticationDatabase, "mongo-authentication-database", os.Getenv("MONGO_AUTHENTICATION_DATABASE"), "specifies the mongo authentication database")
	mongoCmd.Flags().StringVar(&mongoAuthenticationDatabase, "mongo-read-preference", os.Getenv("MONGO_READ_PREFERENCE"), "specifies the read preference from the mongo command")
}
