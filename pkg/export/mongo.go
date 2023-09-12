package export

import (
	"context"
	"io"
	"log"
	"os/exec"
)

type MongoExportConfig struct {
	MongoURI               string // Required
	AuthenticationDatabase string
	Username               string
	Password               string
}

type MongoExport struct {
	config MongoExportConfig
}

func NewMongoExport(_ context.Context, config MongoExportConfig) (*MongoExport, error) {
	//TODO: Validate Connection
	return &MongoExport{config: config}, nil
}

func (export *MongoExport) Export(ctx context.Context, writer io.WriteCloser) error {
	cfg := export.config

	args := []string{
		"--uri", cfg.MongoURI,
		"--archive",
		"--quiet",
	}

	if cfg.AuthenticationDatabase != "" {
		args = append(args, "--authenticationDatabase", cfg.AuthenticationDatabase)
	}
	if cfg.Username != "" && cfg.Password != "" {
		args = append(args, "--username", cfg.Username, "--password", cfg.Password)
	}

	cmd := exec.CommandContext(ctx, "mongodump", args...)

	cmd.Stdout = writer // only write to bucket since dump will be in stdoud
	cmd.Stderr = log.Writer()

	return cmd.Run()
}
