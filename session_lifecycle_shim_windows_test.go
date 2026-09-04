//go:build windows

package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// requireRelaunchBrowserShim states the same rule where no shim can shadow a
// launcher: CreateProcess resolves the URL openers out of the system directory
// ahead of PATH, so a relaunch builds nothing rather than something that only
// looks like containment, and the login leg refuses on the nil it leaves.
func requireRelaunchBrowserShim(t *testing.T, shim *pi.BrowserShim) {
	t.Helper()

	require.Nil(t, shim)
}
