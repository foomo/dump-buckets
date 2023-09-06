package export

import (
	"context"
	"io"
	"log"
	"os/exec"
)

type MongoExportConfig struct {
	Name     string // Required
	MongoURI string // Required
}

type MongoExport struct {
	config  MongoExportConfig
	storage Storage
}

func NewMongoExport(_ context.Context, config MongoExportConfig, storage Storage) (*MongoExport, error) {
	//TODO: Validate Connection
	return &MongoExport{config: config, storage: storage}, nil
}

func (export *MongoExport) Export(ctx context.Context, writer io.WriteCloser) error {
	cfg := export.config

	args := []string{
		"--uri", cfg.MongoURI,
		"--archive",
	}

	cmd := exec.CommandContext(ctx, "mongodump", args...)

	cmd.Stdout = writer // only write to bucket since dump will be in stdoud
	cmd.Stderr = log.Writer()

	return cmd.Run()
}
