package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteSeedFiles(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "agent")
	require.NoError(t, WriteSeedFiles(dir, nil))

	files := map[string]string{"settings.json": `{"a":1}`, "extensions/x.ts": "export default () => {}"}
	require.NoError(t, WriteSeedFiles(dir, files))

	settings, err := os.ReadFile(filepath.Join(dir, SettingsFileName))
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(settings))

	manifest, err := os.ReadFile(filepath.Join(dir, seedManifestFileName))
	require.NoError(t, err)
	require.JSONEq(t, `["extensions/x.ts","settings.json"]`, string(manifest))

	files["settings.json"] = `{"a":2}`
	require.NoError(t, WriteSeedFiles(dir, files))

	backup, err := os.ReadFile(filepath.Join(dir, SettingsFileName+seedBackupSuffix))
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(backup))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "operator.json"), []byte("{}"), 0o600))

	var seedErr *SeedFileError

	require.ErrorAs(t, WriteSeedFiles(dir, map[string]string{"operator.json": "x"}), &seedErr)
	require.Equal(t, "operator.json", seedErr.Name)
	require.EqualError(t, seedErr, `invalid seed file "operator.json"`)

	require.ErrorAs(t, WriteSeedFiles(dir, map[string]string{"settings.json": "{bad"}), &seedErr)

	for _, bad := range []string{"", "/abs", "../up", "a/../b", "./x", ".seed-manifest.json", "x.seed.bak", "a\x00b"} {
		require.ErrorAs(t, WriteSeedFiles(dir, map[string]string{bad: "x"}), &seedErr, bad)
	}
}
