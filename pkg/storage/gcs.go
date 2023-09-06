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

func (gcs *GCSBackup) NewWriter(ctx context.Context, path string) (writer io.WriteCloser, err error) {
	obj := gcs.client.Bucket(gcs.bucketName).Object(path)
	return obj.NewWriter(ctx), nil
}
