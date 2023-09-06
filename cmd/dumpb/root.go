package dumpb

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var (
	storageBucketVendor string
	storageBucketName   string
	storageBucketPath   string
)

var rootCmd = &cobra.Command{
	Use:   "dumpb",
	Short: "dumpb - a simple databse dump tool",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&storageBucketVendor, "storage-vendor", "gcs", "specifies the vendor for the buckets")
	rootCmd.PersistentFlags().StringVar(&storageBucketName, "storage-bucket-name", "", "specifies the bucket name where to dump to")
	rootCmd.PersistentFlags().StringVar(&storageBucketPath, "storage-path", "", "specifies the path where to store the backups")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("Failed to execute command", slog.Any("error", err))
		os.Exit(1)
	}
}
