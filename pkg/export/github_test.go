package export

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGithubExport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	tfile, err := os.Create(filepath.Join(t.TempDir(), "archive.tar"))
	require.NoError(t, err)
	defer tfile.Close()

	export, err := NewGitExport(context.Background(), GitHubExportConfig{
		Organization: "foomo",
		Repository:   "pagespeed_exporter",
		GithubToken:  "",
		Branch:       "main",
	})
	require.NoError(t, err)

	err = export.Export(context.Background(), tfile)
	require.NoError(t, err)
	require.FileExists(t, tfile.Name())
}
