//go:build !windows

package pi

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// foreignUID is an identity this process cannot be running as, so a store path
// it owns fails the ownership check against it.
const foreignUID = -1

// restoreSharedExtensionOwnerSeam puts the compared identity back after a test
// moves it.
func restoreSharedExtensionOwnerSeam(t *testing.T) {
	t.Helper()

	uid := sharedExtensionCurrentUID

	t.Cleanup(func() { sharedExtensionCurrentUID = uid })
}

// setForeignSharedExtensionUID makes every path this process owns look like
// another user's.
func setForeignSharedExtensionUID() {
	sharedExtensionCurrentUID = func() int { return foreignUID }
}

// foreignFileInfo carries no platform stat, which is the shape a filesystem
// that cannot report ownership hands back.
type foreignFileInfo struct{}

func (foreignFileInfo) Name() string       { return "acp-bridge.ts" }
func (foreignFileInfo) Size() int64        { return 0 }
func (foreignFileInfo) Mode() fs.FileMode  { return 0o400 }
func (foreignFileInfo) ModTime() time.Time { return time.Time{} }
func (foreignFileInfo) IsDir() bool        { return false }
func (foreignFileInfo) Sys() any           { return nil }

func TestPublishSharedExtensionsRefusesForeignOwner(t *testing.T) {
	restoreAgentDirSeams(t)
	restoreSharedExtensionOwnerSeam(t)

	// Every path the store just created is owned by this process, so moving
	// the identity it compares against is what proves the check runs.
	root := t.TempDir()
	_, err := publishSharedExtensions(root)
	require.NoError(t, err)

	setForeignSharedExtensionUID()

	_, err = publishSharedExtensions(root)
	require.ErrorContains(t, err, "want the current user")
}

func TestSharedExtensionOwnedByCaller(t *testing.T) {
	restoreSharedExtensionOwnerSeam(t)

	path := filepath.Join(t.TempDir(), "acp-bridge.ts")
	require.NoError(t, os.WriteFile(path, []byte("ours\n"), 0o400))

	info, err := os.Lstat(path)
	require.NoError(t, err)
	require.NoError(t, sharedExtensionOwnedByCaller(info))

	setForeignSharedExtensionUID()
	require.ErrorContains(t, sharedExtensionOwnedByCaller(info), "want the current user")

	// A filesystem that reports no ownership at all is refused rather than
	// assumed to be ours.
	require.ErrorContains(t, sharedExtensionOwnedByCaller(foreignFileInfo{}), "file ownership is unavailable")
}
