package export

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Export(t *testing.T) {
	ctx := context.Background()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/smartinov/globus-infrastructure/.auth/globus-webshop-master-fc4e39ceee8f.json")
	export, err := NewBigQueryExport(ctx, BigQueryDatasetExportConfig{
		BucketName:  "bigquery-backup-example",
		DatasetName: "Akeneo",
		ProjectID:   "globus-datahub-dev",
		GCSLocation: "europe-west6",
		FilterAfter: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	err = export.Export(ctx)
	require.NoError(t, err)
}

func TestIsTableFiltered(t *testing.T) {
	type args struct {
		tableName   string
		filterAfter time.Time
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "TableWithDateSuffixBeforeFilter",
			args: args{
				tableName:   "table_20220301",
				filterAfter: time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "TableWithDateSuffixAfterFilter",
			args: args{
				tableName:   "table_20230501",
				filterAfter: time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "TableWithInvalidDateSuffix",
			args: args{
				tableName:   "table_20223001",
				filterAfter: time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "TableWithoutDateSuffix",
			args: args{
				tableName:   "table",
				filterAfter: time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "TableWithEmptyName",
			args: args{
				tableName:   "",
				filterAfter: time.Date(2023, 3, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTableFiltered(tt.args.tableName, tt.args.filterAfter); got != tt.want {
				t.Errorf("isTableFiltered() = %v, want %v", got, tt.want)
			}
		})
	}
}
