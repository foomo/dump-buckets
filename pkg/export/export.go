package export

import (
	"context"
	"io"
)

type Storage interface {
	NewWriter(ctx context.Context, path string) (writer io.WriteCloser, err error)
}
