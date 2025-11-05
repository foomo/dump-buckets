# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dump Buckets is a database cloud backup & retention solution - a Go-based CLI tool for backing up data from different sources and piping output to storage buckets. Built with Cobra for CLI framework and currently supports Google Cloud Storage (GCS).

## Development Commands

### Building
```bash
# Build Docker image
make build

# Or directly with docker buildx
docker buildx build -f Dockerfile -t dump-buckets:latest .

# Build Go binary locally
go build -o dumpb main.go
```

### Running
```bash
# Run with Docker (includes all runtime dependencies)
make run

# Run specific exporter
docker run --rm -it \
    -e BACKUP_NAME=example \
    -e STORAGE_VENDOR=gcs \
    -e STORAGE_BUCKET_NAME=bucket-name \
    -e STORAGE_PATH=path/to/store \
    -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
    dump-buckets:latest <exporter-command>
```

### Testing
```bash
# Run all tests
go test ./...

# Run specific package tests
go test ./pkg/export/...
go test ./cmd/dumpb/...

# Run single test
go test -run TestExporter_Export ./pkg/export/bitbucket/
```

## Architecture

### Core Components

1. **CLI Layer** (`cmd/dumpb/`):
   - Entry point through Cobra commands
   - Each exporter is a separate subcommand
   - Uses `exportWrapper` pattern for common functionality (logging, storage setup, timing)
   - Commands defined: `root.go`, `execute.go`, `mongo.go`, `github.go`, `bigquery.go`, `contentful.go`, `bitbucket.go`

2. **Export Layer** (`pkg/export/`):
   - Exporters follow a consistent interface pattern:
     - Config struct (e.g., `GitHubExportConfig`)
     - Exporter struct (e.g., `GitHubExport`)
     - Constructor function `New...Export(ctx, config)`
     - Export method: `Export(ctx context.Context, writer io.Writer) error`
   - Common utilities: `Tar()` function for tarring/compressing directories

3. **Storage Layer** (`pkg/storage/`):
   - Abstract interface: `storageWriter` with `NewWriter(ctx, path) (io.WriteCloser, error)`
   - Currently implemented: GCS (`gcs.go`)
   - Easy to extend for S3, Azure Blob, etc.

### Key Patterns

#### exportWrapper Pattern
Located in `cmd/dumpb/dumpb.go`, this wrapper provides common functionality for all exporters:
- Sets up structured logging with slog
- Configures storage backend based on flags/env vars
- Tracks execution timing
- Handles errors consistently
- Logs completion with path and duration

Usage:
```go
var myCmd = &cobra.Command{
    Use: "myexporter",
    RunE: exportWrapper("ExporterName", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
        // 1. Create exporter config
        // 2. Initialize exporter
        // 3. Create storage writer with path
        // 4. Call exporter.Export(ctx, writer)
        // 5. Return export path
    }),
}
```

#### Exporter Interface Pattern
All exporters implement:
```go
type XExport struct {
    config XExportConfig
}

func NewXExport(ctx context.Context, config XExportConfig) (*XExport, error) {
    // Initialize and validate
}

func (e *XExport) Export(ctx context.Context, writer io.Writer) error {
    // Export data to writer
}
```

#### Configuration Pattern
- All configuration via flags with environment variable fallbacks
- Root flags (persistent): `--backup-name`, `--storage-vendor`, `--storage-bucket-name`, `--storage-path`
- Each command defines specific flags (e.g., mongo: `--mongo-uri`, `--mongo-username`)
- Use `os.Getenv()` as default values in flag definitions

### Adding New Exporters

1. **Create exporter in `pkg/export/`**:
   ```go
   type NewExportConfig struct {
       // Required config fields
   }

   type NewExport struct {
       config NewExportConfig
   }

   func NewNewExport(ctx context.Context, config NewExportConfig) (*NewExport, error) {
       // Validate config, initialize dependencies
   }

   func (e *NewExport) Export(ctx context.Context, writer io.Writer) error {
       // Write export data to writer
   }
   ```

2. **Create command in `cmd/dumpb/`**:
   ```go
   var newCmd = &cobra.Command{
       Use: "new",
       Short: "Description",
       RunE: exportWrapper("New", func(ctx context.Context, l *slog.Logger, storage storageWriter) (string, error) {
           // Setup config from flags
           // Create exporter
           // Create storage writer with path
           // Call Export()
           // Return path
       }),
   }

   func init() {
       rootCmd.AddCommand(newCmd)
       // Add command-specific flags
   }
   ```

3. **Add tests in `pkg/export/` with `_test.go` suffix**

### Adding New Storage Backends

1. **Implement `storageWriter` interface in `pkg/storage/`**:
   ```go
   type NewStorage struct {
       // client, config fields
   }

   func NewNewStorage(ctx context.Context, config) (*NewStorage, error) {
       // Initialize client
   }

   func (s *NewStorage) NewWriter(ctx context.Context, path string) (io.WriteCloser, error) {
       // Return writer that uploads to storage at path
   }
   ```

2. **Add case in `configuredStorage()` in `cmd/dumpb/root.go`**:
   ```go
   case "new-vendor":
       return storage.NewNewStorage(ctx, bucketName)
   ```

## Testing

- Uses `github.com/stretchr/testify` for assertions
- Test files colocated with implementation (`_test.go` suffix)
- Examples: `execute_test.go`, `bigquery_test.go`, `github_test.go`, `bitbucket_test.go`

### Unit vs Integration Tests
- **Unit tests**: Run by default with `go test ./...`
- **Integration tests**: Require external services (GCS, BigQuery, GitHub, Bitbucket)
  - Skip integration tests with: `go test -short ./...`
  - Integration tests check `testing.Short()` and skip if true
  - Examples: BigQuery exports, GitHub API calls, Bitbucket API calls

### Running Tests
```bash
# Run unit tests only (recommended for CI)
go test -short ./...

# Run all tests including integration tests
go test ./...

# Run specific package tests
go test -short ./pkg/export/...
```

## Docker Runtime

- Base: Alpine 3.22.1 with timezone support (Europe/Zurich)
- Includes: mongodb-tools, postgresql-client, npm, contentful-cli
- Multi-stage build with Go 1.21+
- Binary location: `/dumpb`
- Entrypoint: `/dumpb`

## Error Handling

- Use structured logging with `log/slog`
- Wrap errors with context: `fmt.Errorf("context: %w", err)`
- Defer close operations with error logging:
  ```go
  defer func() {
      if err := resource.Close(); err != nil {
          l.With(slog.Any("error", err)).Error("Failed to close resource")
      }
  }()
  ```

## Export Naming Convention

Exports follow pattern: `{BACKUP_NAME}/{TIMESTAMP}{.ext}{.gz}`
- Timestamp format: `20060102T150405` (defined in `export.TimestampFormat`)
- Optional extension and gzip compression
- Example: `my-backup/20240213T143052.sql.gz`
