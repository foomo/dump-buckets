package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/go-git/go-git/v5"
)

var (
	defaultAccountRepositoriesURL = "https://api.bitbucket.org/2.0/repositories/%s?pagelen=100&page=%d"
	defaultCloneURL               = "https://x-token-auth:%s@bitbucket.org/%s/%s"
)

type Exporter struct {
	config Config
}

func NewExporter(_ context.Context, config Config) (*Exporter, error) {
	return &Exporter{
		config: config,
	}, nil
}

type Config struct {
	AccountName string // GlobusDigital
	Token       string
}

func (e *Exporter) Export(ctx context.Context, writer io.Writer) error {
	tdir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp output dir: %w", err)
	}

	repos, err := e.fetchAllRepositories(ctx)
	if err != nil {
		return err
	}

	for _, repo := range repos {
		cloneURL := fmt.Sprintf(defaultCloneURL, e.config.Token, e.config.AccountName, repo.Slug)
		outputPath := filepath.Join(tdir, repo.Slug)
		_, err = git.PlainCloneContext(ctx, outputPath, false, &git.CloneOptions{
			URL:      cloneURL,
			Mirror:   true,
			Progress: os.Stdout,
		})
		if err != nil {
			return err
		}
	}

	return export.Tar(ctx, tdir, writer)
}

func (e *Exporter) fetchAllRepositories(ctx context.Context) ([]Repository, error) {
	index := 1 // because bitbucket :'(
	var allRepositories []Repository
	for {
		repos, hasNextPage, err := e.fetchRepositoryPage(ctx, index)
		if err != nil {
			return nil, err
		}
		allRepositories = append(allRepositories, repos...)
		if !hasNextPage {
			break
		}
		index++
	}
	return allRepositories, nil
}

func (e *Exporter) fetchRepositoryPage(ctx context.Context, index int) (repos []Repository, hasNextPage bool, err error) {
	cfg := e.config

	pageURI := fmt.Sprintf(defaultAccountRepositoriesURL, cfg.AccountName, index)
	req, err := http.NewRequestWithContext(ctx, "GET", pageURI, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	var data RepositoryResponse
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, false, err
	}

	return data.Values, data.Next != "", nil
}
