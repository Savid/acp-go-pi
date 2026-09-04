//go:build windows

package piacp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSessionLaunchesPiWithNoBrowserShim is the Windows half of the shim rule.
// CreateProcess resolves cmd.exe, explorer.exe, and rundll32.exe out of the
// system directory ahead of every PATH entry, and the `start` that opens a URL
// is a cmd.exe builtin with no image to shadow, so a directory of no-op
// launchers on PATH neutralises nothing here. The session therefore launches
// with no shim at all rather than one that only looks like containment, and the
// login leg that would reach for a browser refuses on the nil this leaves.
func TestSessionLaunchesPiWithNoBrowserShim(t *testing.T) {
	scratch := t.TempDir()
	agent := newStubClientAgent(t, newStubPiClient(), WithScratchDir(scratch))

	session, err := agent.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

	require.Nil(t, session.browserShim)
	require.Empty(t, browserShimDirs(t, scratch))

	for _, entry := range session.launch.Environ() {
		key, value, found := strings.Cut(entry, "=")
		require.True(t, found)
		require.False(t, strings.EqualFold(key, "BROWSER"), "no shim launcher is advertised")

		if strings.EqualFold(key, "PATH") {
			require.NotContains(t, value, browserShimDirPrefix)
		}
	}
}

// TestSessionBrowserShimIsAlwaysNilHere pins the same refusal at the seam that
// produces it: no scratch parent, usable or not, yields a shim on this
// platform, so the nil the login leg refuses on is the only value there is.
func TestSessionBrowserShimIsAlwaysNilHere(t *testing.T) {
	t.Parallel()

	scratch := t.TempDir()
	agent := newStubClientAgent(t, newStubPiClient(), WithScratchDir(scratch))
	require.Nil(t, agent.newSessionBrowserShim(scratch))

	blocked := filepath.Join(scratch, "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o600))
	require.Nil(t, agent.newSessionBrowserShim(blocked))
}
