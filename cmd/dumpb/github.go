package dumpb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/spf13/cobra"
)

var (
	githubToken        string
	githubOrganization string
	githubRepository   string
	githubBranch       string
)

var githubCmd = &cobra.Command{
	Use:   "github",
	Short: "Dumps github repositories in a destination bucket",
	RunE: exportWrapper("GitHub", func(ctx context.Context, l *slog.Logger, storage storageWriter) error {
		config := export.GitHubExportConfig{
			Organization: githubOrganization,
			Repository:   githubRepository,
			GithubToken:  githubToken,
			Branch:       githubBranch,
		}
		exporter, err := export.NewGitExport(ctx, config)
		if err != nil {
			return err
		}

		exportName := fmt.Sprintf("%s.%s.%s.tar.gz", githubOrganization, githubRepository, time.Now().Format(time.RFC3339))
		if backupName != "" {
			exportName += fmt.Sprintf("%s/%s", backupName, exportName)
		}
		exportPath := filepath.Join(storageBucketPath, exportName)

		writer, err := storage.NewWriter(ctx, exportPath)
		if err != nil {
			return fmt.Errorf("failed to initialize writer: %w", err)
		}
		defer writer.Close()

		return exporter.Export(ctx, writer)
	}),
}

func init() {
	rootCmd.AddCommand(githubCmd)
	githubCmd.Flags().StringVar(&githubToken, "github-token", os.Getenv("GITHUB_TOKEN"), "specifies the GITHUB token (Optional)")
	githubCmd.Flags().StringVar(&githubOrganization, "github-org", os.Getenv("GITHUB_ORG"), "specifies the github organization")
	githubCmd.Flags().StringVar(&githubRepository, "github-repo", os.Getenv("GITHUB_REPO"), "specifies the github repository")
	githubCmd.Flags().StringVar(&githubBranch, "github-branch", os.Getenv("GITHUB_BRANCH"), "specifies the github branch")
}
