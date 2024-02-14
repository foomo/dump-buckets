package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/foomo/dump-buckets/pkg/export/bitbucket"
	"github.com/spf13/cobra"
)

var (
	bitbucketToken   string
	bitbucketAccount string
)

var bitbucketCmd = &cobra.Command{
	Use:   "bitbucket",
	Short: "Dumps bitbucket accounts in a destination bucket",
	RunE: exportWrapper("BitBucket", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
		config := bitbucket.Config{
			AccountName: bitbucketAccount,
			Token:       bitbucketToken,
		}
		exporter, err := bitbucket.NewExporter(ctx, config)
		if err != nil {
			return "", err
		}

		exportName := fmt.Sprintf("%s.%s.tar.gz", bitbucketAccount, time.Now().Format(export.TimestampFormat))
		if backupName != "" {
			exportName += fmt.Sprintf("%s/%s", backupName, exportName)
		}
		exportPath := filepath.Join(storageBucketPath, exportName)

		writer, err := storage.NewWriter(ctx, exportPath)
		if err != nil {
			return "", fmt.Errorf("failed to initialize writer: %w", err)
		}
		defer writer.Close()

		return exportPath, exporter.Export(ctx, l, writer)
	}),
}

func init() {
	rootCmd.AddCommand(bitbucketCmd)
	bitbucketCmd.Flags().StringVar(&bitbucketToken, "bitbucket-token", os.Getenv("BITBUCKET_TOKEN"), "specifies the bitbucket token")
	bitbucketCmd.Flags().StringVar(&bitbucketAccount, "bitbucket-account", os.Getenv("BITBUCKET_ACCOUNT"), "specifies the bitbucket account name ")
}
