package storage

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type GCSBackup struct {
	client     *storage.Client
	bucketName string
}

func NewGCSStorage(ctx context.Context, bucketName string) (*GCSBackup, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSBackup{
		bucketName: bucketName,
		client:     client,
	}, nil
}

func (gcs *GCSBackup) NewWriter(ctx context.Context, path string, opts ...WriterOption) (writer io.WriteCloser, err error) {
	obj := gcs.client.Bucket(gcs.bucketName).Object(path)
	w := obj.NewWriter(ctx)

	// Apply writer options if provided
	if len(opts) > 0 {
		attrs := &WriterAttrs{}
		for _, opt := range opts {
			opt(attrs)
		}

		if attrs.ContentType != "" {
			w.ContentType = attrs.ContentType
		}
		if attrs.ContentEncoding != "" {
			w.ContentEncoding = attrs.ContentEncoding
		}
		if len(attrs.Metadata) > 0 {
			w.Metadata = attrs.Metadata
		}
	}

	return w, nil
}
