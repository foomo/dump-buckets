package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/foomo/dump-buckets/pkg/export"
	"github.com/go-git/go-git/v5"
)

var (
	defaultAccountRepositoriesURL = "https://api.bitbucket.org/2.0/repositories/%s?pagelen=100&page=%d"
	defaultCloneURL               = "https://bitbucket.org/%s/%s"
)

type Exporter struct {
	config     Config
	httpClient *http.Client
}

func NewExporter(_ context.Context, config Config) (*Exporter, error) {
	return &Exporter{
		config: config,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

type Config struct {
	AccountName string // GlobusDigital
	Token       string
}

func (e *Exporter) Export(ctx context.Context, l *slog.Logger, writer io.Writer) error {
	l.Info("Starting bitbucket account export")

	tdir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp output dir: %w", err)
	}
	defer os.RemoveAll(tdir)

	repos, err := e.fetchAllRepositories(ctx)
	if err != nil {
		return err
	}

	for _, repo := range repos {
		err := e.cloneGitRepository(ctx, l, tdir, repo.Slug)
		if err != nil {
			return fmt.Errorf("failed to clone git repository: %w", err)
		}
	}

	l.Info("Taring git dump directory")
	return export.Tar(ctx, tdir, true, writer)
}

func (e *Exporter) cloneGitRepository(ctx context.Context, l *slog.Logger, tdir string, repoSlug string) error {
	l.Info("Cloning git repository", "repository", repoSlug)

	cloneURL, err := url.Parse(fmt.Sprintf(defaultCloneURL, e.config.AccountName, repoSlug))
	if err != nil {
		return err
	}

	if e.config.Token != "" {
		cloneURL.User = url.UserPassword("x-token-auth", e.config.Token)
	}

	outputPath := filepath.Join(tdir, repoSlug)
	_, err = git.PlainCloneContext(ctx, outputPath, false, &git.CloneOptions{
		URL:      cloneURL.String(),
		Mirror:   true,
		Progress: os.Stdout,
	})

	return err
}

func (e *Exporter) fetchAllRepositories(ctx context.Context) ([]Repository, error) {
	index := 1 // because bitbucket :'(
	var allRepositories []Repository
	for {
		repos, hasNextPage, err := e.fetchRepositoryPage(ctx, index)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch repository page: %w", err)
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
	if e.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := e.httpClient.Do(req)
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
