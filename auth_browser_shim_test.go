package piacp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// browserShimDirs lists the shim directories currently living under parent.
func browserShimDirs(t *testing.T, parent string) []string {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(parent, "acp-go-pi-browser-shim-*"))
	require.NoError(t, err)

	return matches
}

// TestSessionLaunchesPiBehindTheBrowserShim pins that the session process
// inherits a PATH whose first entry is a directory of no-op launchers, and that
// BROWSER names one of them.
func TestSessionLaunchesPiBehindTheBrowserShim(t *testing.T) {
	scratch := t.TempDir()
	agent := newStubClientAgent(t, newStubPiClient(), WithScratchDir(scratch))

	session, err := agent.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.NoError(t, err)

	shims := browserShimDirs(t, scratch)
	require.Len(t, shims, 1)

	env := make(map[string]string)
	for _, entry := range session.launch.Environ() {
		key, value, found := strings.Cut(entry, "=")
		require.True(t, found)
		env[key] = value
	}

	require.Equal(t, shims[0], strings.Split(env["PATH"], string(os.PathListSeparator))[0])
	require.Equal(t, filepath.Join(shims[0], "open"), env["BROWSER"])

	for _, name := range []string{"open", "xdg-open"} {
		info, statErr := os.Stat(filepath.Join(shims[0], name))
		require.NoError(t, statErr)
		require.NotZero(t, info.Mode().Perm()&0o100)
	}

	require.NoError(t, session.Close(t.Context()))
	require.Empty(t, browserShimDirs(t, scratch))
}

// TestAuthorizeRefusesOAuthWithoutABrowserShim pins the fail-closed outcome on a
// platform where no shim can shadow the launcher: the login that would open the
// operator's browser is refused before pi is ever asked to start it.
func TestAuthorizeRefusesOAuthWithoutABrowserShim(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	logins := 0

	harness.scriptBridge(func(_ context.Context, request pi.AuthRequest) error {
		if request.Op == pi.AuthOpLogin {
			logins++
		}

		return nil
	})

	harness.session.browserShim = nil

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCausePolicy)
	require.Zero(t, logins)
}
