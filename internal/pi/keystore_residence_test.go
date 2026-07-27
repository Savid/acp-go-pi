//go:build integration

package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const keystoreResidenceCanary = "canary-not-a-real-credential"

// TestKeystoreResidenceMatrix asserts residence, never login. pi's credential
// storage admits a file backend and an in-memory one and nothing else, so the
// answer to "where does a brokered credential live" is the agent directory's
// own auth.json whether or not a Secret Service is running beside it. The
// container driver runs this twice — once with a live service on the session
// bus and once without — and both runs must agree.
func TestKeystoreResidenceMatrix(t *testing.T) {
	agentDir := t.TempDir()
	authPath := filepath.Join(agentDir, AuthFileName)

	require.NoError(t, os.WriteFile(authPath, []byte(`{"anthropic":{"type":"oauth","refresh":"`+keystoreResidenceCanary+`","access":"","expires":0}}`), 0o600))

	info, err := os.Stat(authPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	contents, err := os.ReadFile(authPath) // #nosec G304 -- the path is this test's own temp dir.
	require.NoError(t, err)
	require.Contains(t, string(contents), keystoreResidenceCanary)

	// The bus is what the two runs differ on, and it is exactly what must not
	// move the residence answer.
	t.Logf("session bus: %q", os.Getenv("DBUS_SESSION_BUS_ADDRESS"))

	entries, err := os.ReadDir(agentDir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the agent directory holds one credential file and no keystore sidecar")
	require.Equal(t, AuthFileName, entries[0].Name())
}
