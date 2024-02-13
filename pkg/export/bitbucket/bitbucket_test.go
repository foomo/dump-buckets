package bitbucket

import (
	"context"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

var (
	bitBucketToken = os.Getenv("BITBUCKET_TOKEN")
)

func TestExporter_Export(t *testing.T) {
	ctx := context.Background()
	e, err := NewExporter(ctx, Config{
		AccountName: "globusdigital",
		Token:       bitBucketToken,
	})
	require.NoError(t, err)
	err = e.Export(ctx, nil)
	require.NoError(t, err)

	_, err = os.Create("/Users/smartinov/dump-buckets/dump.tar")
	require.NoError(t, err)
}

func TestExporter_fetchAllRepositories(t *testing.T) {
	t.Skip()
	ctx := context.Background()
	e, err := NewExporter(ctx, Config{
		AccountName: "globusdigital",
		Token:       bitBucketToken,
	})
	require.NoError(t, err)
	repositories, err := e.fetchAllRepositories(ctx)
	require.NoError(t, err)
	spew.Dump(repositories)
}
