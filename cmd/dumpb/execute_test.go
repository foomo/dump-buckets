package dumpb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_getExportName(t *testing.T) {
	backupName = "backup"

	t.Run("plain", func(t *testing.T) {
		outputGzip = false
		outputExt = ""

		exportName := getExportName(time.Time{})
		require.Equal(t, "backup/0001-01-01T00:00:00Z", exportName)
	})
	t.Run("ext", func(t *testing.T) {
		outputGzip = false
		outputExt = ".data"

		exportName := getExportName(time.Time{})
		require.Equal(t, "backup/0001-01-01T00:00:00Z.data", exportName)
	})
	t.Run("gz", func(t *testing.T) {
		outputGzip = true
		outputExt = ""

		exportName := getExportName(time.Time{})
		require.Equal(t, "backup/0001-01-01T00:00:00Z.gz", exportName)
	})
}
