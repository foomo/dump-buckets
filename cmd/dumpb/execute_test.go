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
		require.Equal(t, "backup/00010101T000000", exportName)
	})
	t.Run("ext", func(t *testing.T) {
		outputGzip = false
		outputExt = ".data"

		exportName := getExportName(time.Time{})
		require.Equal(t, "backup/00010101T000000.data", exportName)
	})
	t.Run("gz", func(t *testing.T) {
		outputGzip = true
		outputExt = ""

		exportName := getExportName(time.Time{})
		require.Equal(t, "backup/00010101T000000.gz", exportName)
	})
}
