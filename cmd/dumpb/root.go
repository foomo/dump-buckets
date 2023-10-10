package dumpb

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/foomo/dump-buckets/pkg/storage"
	"github.com/spf13/cobra"
)

var (
	storageBucketVendor string
	storageBucketName   string
	storageBucketPath   string
	backupName          string
)

var rootCmd = &cobra.Command{
	Use:   "dumpb",
	Short: "dumpb - a simple databse dump tool",
	// Validate Parameters
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&backupName, "backup-name", os.Getenv("BACKUP_NAME"), "specifies the name of the backup")
	rootCmd.PersistentFlags().StringVar(&storageBucketVendor, "storage-vendor", os.Getenv("STORAGE_VENDOR"), "specifies the vendor for the buckets")
	rootCmd.PersistentFlags().StringVar(&storageBucketName, "storage-bucket-name", os.Getenv("STORAGE_BUCKET_NAME"), "specifies the bucket name where to dump to")
	rootCmd.PersistentFlags().StringVar(&storageBucketPath, "storage-path", os.Getenv("STORAGE_PATH"), "specifies the path where to store the backups")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("Failed to execute command", slog.Any("error", err))
		os.Exit(1)
	}
}

type storageWriter interface {
	NewWriter(ctx context.Context, path string) (writer io.WriteCloser, err error)
}

func configuredStorage(ctx context.Context) (storageWriter, error) {
	switch storageBucketVendor {
	case "gcs":
		gcs, err := storage.NewGCSStorage(ctx, storageBucketName)
		if err != nil {
			return nil, err
		}
		return gcs, nil
	default:
		return nil, fmt.Errorf("vendor %q not supported", storageBucketVendor)
	}
}
