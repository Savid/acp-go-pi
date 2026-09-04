package piacp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScratchParent(t *testing.T) {
	t.Parallel()

	require.Equal(t, os.TempDir(), scratchParent(""))
	require.Equal(t, "/explicit/scratch", scratchParent("/explicit/scratch"))
}

func TestEnsureScratchParent(t *testing.T) {
	t.Parallel()

	t.Run("empty resolves to the system temp directory", func(t *testing.T) {
		t.Parallel()

		parent, err := ensureScratchParent("")
		require.NoError(t, err)
		require.Equal(t, os.TempDir(), parent)
	})

	t.Run("missing nested path is created 0700", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "nested", "scratch")
		parent, err := ensureScratchParent(dir)
		require.NoError(t, err)
		require.Equal(t, dir, parent)

		info, err := os.Stat(dir)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		requireRestrictedMode(t, dir, 0o700)
	})

	t.Run("regular-file parent is an error", func(t *testing.T) {
		t.Parallel()

		file := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

		_, err := ensureScratchParent(filepath.Join(file, "child"))
		require.Error(t, err)
	})
}
