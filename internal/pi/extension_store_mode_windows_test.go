//go:build windows

package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublishSharedExtensionsAdmitsModesWindowsCannotDescribe is the Windows
// half of the store's write rule. Go reports 0777 for every writable directory
// on this platform and 0666 for every writable file, so a group-and-world
// write-bit test refused every path the store creates, and since every session
// launch publishes these sources first, no session could open at all.
// Confinement here is the ACL the scratch parent inherits from the user
// profile, which is the standing the ownership check already has.
func TestPublishSharedExtensionsAdmitsModesWindowsCannotDescribe(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	published, err := publishSharedExtensions(root)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Dir(published.bridge))
	require.NoError(t, err)
	require.NotZero(t, info.Mode().Perm()&sharedExtensionLooseModeBits,
		"the mode this platform reports is what the removed comparison read")

	// Republishing re-runs the guard over what the first call left behind, so
	// a reused store is admitted rather than refused.
	again, err := publishSharedExtensions(root)
	require.NoError(t, err)
	require.Equal(t, published, again)
}
