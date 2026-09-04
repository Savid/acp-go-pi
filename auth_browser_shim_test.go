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

// browserShimDirPrefix names every shim directory this agent creates beneath a
// scratch parent, spelled here because the constant itself is native-boundary
// detail.
const browserShimDirPrefix = "acp-go-pi-browser-shim-"

// browserShimDirs lists the shim directories currently living under parent.
func browserShimDirs(t *testing.T, parent string) []string {
	t.Helper()

	var matches []string
	err := filepath.WalkDir(parent, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), browserShimDirPrefix) {
			matches = append(matches, path)

			return filepath.SkipDir
		}

		return nil
	})
	require.NoError(t, err)

	return matches
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
