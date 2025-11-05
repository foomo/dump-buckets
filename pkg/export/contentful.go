package export

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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

// Export exports contentful space data as a gzipped JSON file
func (ce *ContentfulExport) Export(ctx context.Context, writer io.Writer) error {
	tdir, err := os.MkdirTemp("", "contentful-export-")
	if err != nil {
		return fmt.Errorf("failed to create temp output dir: %w", err)
	}
	defer os.RemoveAll(tdir)

	exportFile := filepath.Join(tdir, "contentful-export.json")

	args := []string{
		"space",
		"export",
		"--use-verbose-renderer",
		"--management-token", ce.config.ManagementToken,
		"--space-id", ce.config.SpaceID,
		"--max-allowed-limit", "100",
		"--export-dir", tdir,
		"--content-file", "contentful-export.json",
	}

	// Export
	cmd := exec.CommandContext(ctx, "contentful", args...)
	cmd.Stdout = log.Writer()
	cmd.Stderr = log.Writer()
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("contentful export failed: %w", err)
	}

	// Open the exported JSON file
	jsonFile, err := os.Open(exportFile)
	if err != nil {
		return fmt.Errorf("failed to open exported file: %w", err)
	}
	defer jsonFile.Close()

	// Create gzip writer and copy the JSON file content
	gzw := gzip.NewWriter(writer)
	defer gzw.Close()

	_, err = io.Copy(gzw, jsonFile)
	if err != nil {
		return fmt.Errorf("failed to gzip content: %w", err)
	}

	return nil
}
