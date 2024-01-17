package dumpb

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
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
			func(ctx context.Context, l *slog.Logger, storage storageWriter) error {
				cmdArgs := args[dashIndex:]
				fmt.Println(cmdArgs, dashIndex)
				if len(cmdArgs) < 1 {
					return errors.New("insufficient number of arguments")
				}

				exportName := fmt.Sprintf("%s/%s.tar.gz", backupName, time.Now().Format(time.RFC3339))
				exportPath := filepath.Join(storageBucketPath, exportName)

				writer, err := storage.NewWriter(ctx, exportPath)
				if err != nil {
					return fmt.Errorf("failed to initialize writer: %w", err)
				}
				defer writer.Close()

				buf := bufio.NewWriter(writer)
				defer buf.Flush()

				// GZIP Write Output
				gzipWriter := gzip.NewWriter(buf)
				defer gzipWriter.Close()

				// Execute the command, skip first 2 arguments
				cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
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
