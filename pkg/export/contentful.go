package export

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
)

type ContentfulExportConfig struct {
	ManagementToken string
	SpaceID         string
}

type ContentfulExport struct {
	config ContentfulExportConfig
}

func NewContentfulExport(_ context.Context, config ContentfulExportConfig) (*ContentfulExport, error) {
	return &ContentfulExport{config: config}, nil
}

// sh "contentful-cli space export --use-verbose-renderer --management-token='${token}' --space-id=${spaceID} --export-dir=/backups --max-allowed-limit=100"
func (ce *ContentfulExport) Export(ctx context.Context, writer io.Writer) error {
	tdir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp output dir: %w", err)
	}

	args := []string{
		"space",
		"export",
		"--use-verbose-renderer",
		"--management-token", ce.config.ManagementToken,
		"--space-id", ce.config.SpaceID,
		"--max-allowed-limit", "100",
		"--export-dir", tdir,
	}

	// Export
	cmd := exec.CommandContext(ctx, "contentful", args...)
	cmd.Stdout = log.Writer() // only write to bucket since dump will be in stdoud
	cmd.Stderr = log.Writer()
	err = cmd.Run()
	if err != nil {
		return err
	}
	// Tar Output Directory
	return Tar(ctx, tdir, writer)
}
