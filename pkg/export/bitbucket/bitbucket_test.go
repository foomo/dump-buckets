package bitbucket

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	bitBucketToken = os.Getenv("BITBUCKET_TOKEN")
)

func TestExporter_Export(t *testing.T) {
	tdir := t.TempDir()

	ctx := context.Background()
	e, err := NewExporter(ctx, Config{
		AccountName: "fargo3d",
		Token:       bitBucketToken,
	})
	require.NoError(t, err)

	test, err := os.Create(filepath.Join(tdir, "clone.tar.gz"))
	require.NoError(t, err)

	err = e.Export(ctx, slog.Default(), test)
	require.NoError(t, err)
	test.Close()

	assert.FileExists(t, test.Name())
}

func TestExporter_fetchAllRepositories(t *testing.T) {
	ctx := context.Background()
	e, err := NewExporter(ctx, Config{
		AccountName: "fargo3d",
		Token:       bitBucketToken,
	})
	require.NoError(t, err)
	repositories, err := e.fetchAllRepositories(ctx)
	require.NoError(t, err)
	require.NotNil(t, repositories)
}
