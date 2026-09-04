//go:build !windows

package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// requireRelaunchBrowserShim states what a rebuilt generation owes the login
// leg. Here a shim directory can shadow every launcher a native login would
// exec, so a relaunch that did not rebuild one would leave the next login able
// to open the operator's browser.
func requireRelaunchBrowserShim(t *testing.T, shim *pi.BrowserShim) {
	t.Helper()

	require.NotNil(t, shim)
}
