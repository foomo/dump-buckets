package dumpb

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "Executes the specified command in the container",
	RunE: func(cmd *cobra.Command, args []string) error {
		wrapper := exportWrapper(
			"execute",
			func(ctx context.Context, l *slog.Logger, storage storageWriter) error {
				if len(os.Args) < 3 {
					return errors.New("insufficient number of arguments")
				}

				exportName := fmt.Sprintf("%s/%s.tar.gz", backupName, time.Now().Format(time.RFC3339))
				exportPath := filepath.Join(storageBucketPath, exportName)

				writer, err := storage.NewWriter(ctx, exportPath)
				if err != nil {
					return fmt.Errorf("failed to initialize writer: %w", err)
				}
				defer writer.Close()

				// GZIP Write Output
				gzipWriter := gzip.NewWriter(writer)
				defer gzipWriter.Close()

				// Execute the command, skip first 2 arguments
				cmd := exec.CommandContext(ctx, os.Args[2], os.Args[3:]...)
				cmd.Stdout = gzipWriter // only write to bucket since dump will be in stdoud
				cmd.Stderr = log.Writer()

				return cmd.Run()
			},
		)
		return wrapper(cmd, args)
	},
}

func init() {
	rootCmd.AddCommand(executeCmd)
}
