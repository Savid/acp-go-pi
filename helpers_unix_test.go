//go:build !windows

package piacp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// requireRestrictedMode asserts the permission bits a path this agent created
// carries. Confinement on this platform is the POSIX mode, so the mode is what
// the rule is stated in.
func requireRestrictedMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, want, info.Mode().Perm())
}

// requireRelaunchBrowserShim states what a rebuilt generation owes the login
// leg. Here a shim directory can shadow every launcher a native login would
// exec, so a relaunch that did not rebuild one would leave the next login able
// to open the operator's browser.
func requireRelaunchBrowserShim(t *testing.T, shim *pi.BrowserShim) {
	t.Helper()

	require.NotNil(t, shim)
}
