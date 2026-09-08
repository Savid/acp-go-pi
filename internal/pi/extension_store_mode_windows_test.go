//go:build windows

package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublishSharedExtensionsAdmitsModesWindowsCannotDescribe is the Windows
// mode-compatibility proof. Go's synthetic writable mode bits must not refuse
// cache reuse. This test does not inspect ACLs or prove write exclusivity.
func TestPublishSharedExtensionsAdmitsModesWindowsCannotDescribe(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	published, err := publishSharedExtensions(root)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Dir(published.bridge))
	require.NoError(t, err)
	require.NotZero(t, info.Mode().Perm()&sharedExtensionLooseModeBits,
		"Windows reports synthetic group/world write bits")

	// Republishing re-runs the guard over what the first call left behind, so
	// a reused store is admitted rather than refused.
	again, err := publishSharedExtensions(root)
	require.NoError(t, err)
	require.Equal(t, published, again)
}
