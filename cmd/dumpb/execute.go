package dumpb

import (
	"bufio"
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

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/spf13/cobra"
)

var (
	outputGzip bool
	outputExt  string
)

var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "Executes the specified command in the container",
	RunE: func(cmd *cobra.Command, args []string) error {
		dashIndex := cmd.ArgsLenAtDash()
		if dashIndex == -1 {
			return errors.New("invalid command, requires args after dash")
		}

		wrapper := exportWrapper(
			"execute",
			func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
				cmdArgs := args[dashIndex:]

				if len(cmdArgs) < 1 {
					return "", errors.New("insufficient number of arguments")
				}

				exportPath := filepath.Join(storageBucketPath, getExportName(time.Now()))

				writer, err := storage.NewWriter(ctx, exportPath)
				if err != nil {
					return "", fmt.Errorf("failed to initialize writer: %w", err)
				}
				defer writer.Close()

				buf := bufio.NewWriter(writer)
				defer func(buf *bufio.Writer) {
					if err := buf.Flush(); err != nil {
						l.With(slog.Any("error", err)).Error("Failed to flush buffered stream")
					}
				}(buf)

				// Execute the command, skip first 2 arguments
				cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
				cmd.Stderr = log.Writer()

				if outputGzip {
					// GZIP Write Output
					gzipWriter := gzip.NewWriter(buf)
					defer func() {
						if err := gzipWriter.Close(); err != nil {
							l.With(slog.Any("error", err)).Error("Failed to close gzip writer")
						}
					}()
					cmd.Stdout = gzipWriter // only write to bucket since dump will be in stdoud
				} else {
					cmd.Stdout = buf
				}

				return exportPath, cmd.Run()
			},
		)
		return wrapper(cmd, args)
	},
}

func getExportName(ts time.Time) string {
	exportName := fmt.Sprintf("%s/%s", backupName, ts.Format(export.TimestampFormat))
	if outputExt != "" {
		exportName += outputExt
	}
	if outputGzip {
		exportName += ".gz"
	}
	return exportName
}

func init() {
	rootCmd.AddCommand(executeCmd)
	executeCmd.PersistentFlags().BoolVar(&outputGzip, "output-gzip", os.Getenv("OUTPUT_GZIP") == "true", "specifies that the output should use gzip compression")
	executeCmd.PersistentFlags().StringVar(&outputExt, "output-ext", os.Getenv("OUTPUT_EXT"), "specifies the extension of the dump")
}
