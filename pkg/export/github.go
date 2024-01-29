package export

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

const (
	repositoryArchiveURL = "https://api.github.com/repos/%s/%s/tarball/%s"
)

type GitHubExportConfig struct {
	Organization string
	Repository   string
	GithubToken  string
	Branch       string
	Client       *http.Client
}

type GitHubExport struct {
	config GitHubExportConfig
}

func NewGitExport(_ context.Context, config GitHubExportConfig) (*GitHubExport, error) {
	if config.Client == nil {
		config.Client = http.DefaultClient
	}
	return &GitHubExport{config: config}, nil
}

// Exports Tarball
func (ge *GitHubExport) Export(ctx context.Context, writer io.Writer) error {
	repositoryURL := fmt.Sprintf(repositoryArchiveURL, ge.config.Organization, ge.config.Repository, ge.config.Branch)
	req, err := http.NewRequestWithContext(ctx, "GET", repositoryURL, nil)
	if err != nil {
		return err
	}
	if ge.config.GithubToken != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", ge.config.GithubToken))
	}
	resp, err := ge.config.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status code %d received", resp.StatusCode)
	}

	_, err = io.Copy(writer, resp.Body)
	return err
}
