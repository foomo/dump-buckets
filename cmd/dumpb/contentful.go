package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/spf13/cobra"
)

var (
	contentfulManagementToken string
	contentfulSpaceID         string
)

var contentfulCmd = &cobra.Command{
	Use:   "contentful",
	Short: "Dumps contentful spaces in a specific bucket",
	RunE: exportWrapper("Contentful", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
		config := export.ContentfulExportConfig{
			ManagementToken: contentfulManagementToken,
			SpaceID:         contentfulSpaceID,
		}
		exporter, err := export.NewContentfulExport(ctx, config)
		if err != nil {
			return "", err
		}

		exportName := fmt.Sprintf("%s.%s.%s.tar.gz", githubOrganization, githubRepository, time.Now().Format(time.RFC3339))
		if backupName != "" {
			exportName += fmt.Sprintf("%s/%s", backupName, exportName)
		}
		exportPath := filepath.Join(storageBucketPath, exportName)

		writer, err := storage.NewWriter(ctx, exportPath)
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
