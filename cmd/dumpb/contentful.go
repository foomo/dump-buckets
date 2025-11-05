package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/foomo/dump-buckets/pkg/storage"
	"github.com/spf13/cobra"
)

var (
	contentfulManagementToken string
	contentfulSpaceID         string
)

var contentfulCmd = &cobra.Command{
	Use:   "contentful",
	Short: "Dumps contentful spaces in a specific bucket",
	RunE: exportWrapper("Contentful", func(ctx context.Context, l *slog.Logger, sw storageWriter) (string, error) {
		config := export.ContentfulExportConfig{
			ManagementToken: contentfulManagementToken,
			SpaceID:         contentfulSpaceID,
		}
		exporter, err := export.NewContentfulExport(ctx, config)
		if err != nil {
			return "", err
		}

		exportName := fmt.Sprintf("%s.json.gz", time.Now().Format(export.TimestampFormat))
		if backupName != "" {
			exportName = fmt.Sprintf("%s/%s", backupName, exportName)
		}
		exportPath := filepath.Join(storageBucketPath, exportName)

		writer, err := sw.NewWriter(
			ctx,
			exportPath,
			storage.WithContentType("application/json"),
			storage.WithContentEncoding("gzip"),
			storage.WithMetadata("SpaceID", contentfulSpaceID),
		)
		if err != nil {
			return "", fmt.Errorf("failed to initialize writer: %w", err)
		}
		defer writer.Close()

		return exportPath, exporter.Export(ctx, writer)
	}),
}

func init() {
	rootCmd.AddCommand(contentfulCmd)
	contentfulCmd.Flags().StringVar(&contentfulManagementToken, "contentful-management-token", os.Getenv("CONTENTFUL_MANAGEMENT_TOKEN"), "specifies the contentful management token")
	contentfulCmd.Flags().StringVar(&contentfulSpaceID, "contentful-space-id", os.Getenv("CONTENTFUL_SPACE_ID"), "specifies the contentful space ID")
}
