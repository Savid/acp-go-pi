package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/savid/acp-go-core/process"
	"github.com/stretchr/testify/require"
)

func TestSeedSettingsValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var seedErr *process.SeedFileError
	require.ErrorAs(t, WriteSeedFiles(dir, map[string]string{SettingsFileName: "{bad"}), &seedErr)
	require.NoError(t, WriteSeedFiles(dir, map[string]string{SettingsFileName: `{"a":1}`}))
	data, err := os.ReadFile(filepath.Join(dir, SettingsFileName))
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1}`, string(data))
}
