//go:build !windows

package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublishSharedExtensionsRestrictsWhatItPublishes pins the modes the store
// leaves behind where the platform has them: an entry no account may write and
// a directory only its owner may enter.
func TestPublishSharedExtensionsRestrictsWhatItPublishes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	published, err := publishSharedExtensions(root)
	require.NoError(t, err)

	for _, path := range []string{published.bridge, published.path, published.mcp} {
		info, statErr := os.Stat(path)
		require.NoError(t, statErr)
		require.Equal(t, os.FileMode(0o400), info.Mode().Perm())
	}

	dirInfo, err := os.Stat(filepath.Dir(published.bridge))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
}

// TestPublishSharedExtensionsRefusesLoosePermissions pins what "another
// account may write this" means where a POSIX mode says it. The Windows half
// of the same rule lives beside that platform's predicate.
func TestPublishSharedExtensionsRefusesLoosePermissions(t *testing.T) {
	t.Parallel()

	t.Run("entry", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		entry := storeEntryPath(root, BridgeExtensionFileName)
		require.NoError(t, os.MkdirAll(filepath.Dir(entry), 0o700))
		require.NoError(t, os.WriteFile(entry, bridgeExtensionSource, 0o600))
		require.NoError(t, os.Chmod(entry, 0o666))

		_, err := publishSharedExtensions(root)
		require.ErrorContains(t, err, "is writable by group or world")
	})

	t.Run("digest directory", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		dir := filepath.Dir(storeEntryPath(root, BridgeExtensionFileName))
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o777))

		_, err := publishSharedExtensions(root)
		require.ErrorContains(t, err, "is writable by group or world")
	})
}
