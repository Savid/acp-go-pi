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

	var matches []string
	err := filepath.WalkDir(parent, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "acp-go-pi-browser-shim-") {
			matches = append(matches, path)

			return filepath.SkipDir
		}

		return nil
	})
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

// TestSessionBrowserShimIsNilWhenItCannotBeMaterialised pins the nil the
// refusal below is stated in terms of. The refusal test supplies that nil by
// hand, so without this the branch that actually produces it — a scratch parent
// no shim directory can be created beneath — is never run, and the one thing
// standing between a native login and the operator's desktop rests on a value
// only a test ever assigns.
func TestSessionBrowserShimIsNilWhenItCannotBeMaterialised(t *testing.T) {
	t.Parallel()

	scratch := t.TempDir()
	usable := newStubClientAgent(t, newStubPiClient(), WithScratchDir(scratch))

	shim := usable.newSessionBrowserShim(scratch)
	require.NotNil(t, shim)
	require.NoError(t, shim.Remove())

	// A regular file is a parent no directory can be created beneath.
	blocked := filepath.Join(scratch, "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o600))

	agent := newStubClientAgent(t, newStubPiClient(), WithScratchDir(blocked))
	require.Nil(t, agent.newSessionBrowserShim(blocked))
	require.Nil(t, agent.newOwnedSessionBrowserShim(blocked))
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
