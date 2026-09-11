package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublishExtensions(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), ExtensionDigest())
	paths, err := PublishExtensions(dir)
	require.NoError(t, err)
	require.Equal(t, []string{paths.Bridge, paths.Path}, paths.Paths())
	require.True(t, paths.IsWrapperExtension(paths.Bridge))
	require.False(t, paths.IsWrapperExtension("/elsewhere.ts"))
	require.False(t, paths.IsWrapperExtension(""))

	bridge, err := os.ReadFile(paths.Bridge)
	require.NoError(t, err)
	require.Contains(t, string(bridge), PermissionTitleMarker)
	require.Contains(t, string(bridge), EnvPermissionMode)

	path, err := os.ReadFile(paths.Path)
	require.NoError(t, err)
	require.Contains(t, string(path), EnvExtraPathDirs)

	require.NoError(t, os.WriteFile(paths.Path, []byte("stale"), 0o600))
	_, err = PublishExtensions(dir)
	require.NoError(t, err)

	path, err = os.ReadFile(paths.Path)
	require.NoError(t, err)
	require.NotEqual(t, "stale", string(path))
	require.Len(t, ExtensionDigest(), 16)
}

func TestParsePermissionTitle(t *testing.T) {
	t.Parallel()

	prompt, ok := ParsePermissionTitle(PermissionTitleMarker + `{"toolCallId":"c","toolName":"bash","input":{"command":"ls"}}`)
	require.True(t, ok)
	require.Equal(t, "c", prompt.ToolCallID)
	require.JSONEq(t, `{"command":"ls"}`, string(prompt.Input))

	_, ok = ParsePermissionTitle("plain title")
	require.False(t, ok)
	_, ok = ParsePermissionTitle(PermissionTitleMarker + "{bad")
	require.False(t, ok)
	_, ok = ParsePermissionTitle(PermissionTitleMarker + `{"toolCallId":" "}`)
	require.False(t, ok)
}
