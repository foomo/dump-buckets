package export

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

func Test_Export(t *testing.T) {
	ctx := context.Background()
	export, err := NewBigQueryExport(ctx, BigQueryDatasetExportConfig{
		BucketName:  "bigquery-backup-example",
		ProjectID:   "globus-datahub",
		GCSLocation: "europe-west6",
		FilterAfter: time.Now().Add(-8 * 24 * time.Hour),
		ExcludePatterns: []string{
			"SAS.GPredictiveScore*",
		},
	})
	require.NoError(t, err)

	_, err = export.Export(ctx, nil)
	require.NoError(t, err)
}

func TestIsTableExcluded(t *testing.T) {
	tests := []struct {
		name          string
		table         *bigquery.Table
		patterns      []string
		excludeBefore time.Time
		want          bool
		wantErr       bool
	}{
		{
			name:     "exclude wildcard match",
			table:    &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScore"},
			patterns: []string{"SAS.GPredictiveScore*"},
			want:     true,
			wantErr:  false,
		},
		{
			name:     "exclude wildcard match extended",
			table:    &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScoreSAP"},
			patterns: []string{"SAS.GPredictiveScore*"},
			want:     true,
			wantErr:  false,
		},
		{
			name:     "exclude exact match",
			table:    &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScore"},
			patterns: []string{"SAS.GPredictiveScore"},
			want:     true,
			wantErr:  false,
		},
		{
			name:     "include excluding exact match",
			table:    &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScoreSAP"},
			patterns: []string{"SAS.GPredictiveScore"},
			want:     false,
			wantErr:  false,
		},
		{
			name:          "exclude time match",
			table:         &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScoreSAP_20200101"},
			patterns:      []string{""},
			excludeBefore: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want:          true,
			wantErr:       false,
		},
		{
			name:          "include time match",
			table:         &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScoreSAP_20210101"},
			patterns:      []string{""},
			excludeBefore: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			want:          false,
			wantErr:       false,
		},
		{
			name:     "not excluded",
			table:    &bigquery.Table{DatasetID: "SAS", TableID: "GPredictiveScoreSAP_20210101"},
			patterns: []string{""},
			want:     false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isTableExcluded(tt.table, tt.patterns, tt.excludeBefore)
			if (err != nil) != tt.wantErr {
				t.Errorf("isTableExcluded() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isTableExcluded() = %v, want %v", got, tt.want)
			}
		})
	}
}
