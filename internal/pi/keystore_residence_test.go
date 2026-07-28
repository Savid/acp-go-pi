//go:build integration

package pi

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	envRunIntegration = "ACP_GO_PI_RUN_INTEGRATION"
	envRunKeystore    = "ACP_GO_PI_RUN_KEYSTORE"

	// envSessionBus reaches the Secret Service. Whether the fixture exported one
	// is the whole difference between the two Linux configurations.
	envSessionBus = "DBUS_SESSION_BUS_ADDRESS"

	// keystoreFixtureMarker is written by the credential-residence fixture's
	// entrypoint. Seeding a live Secret Service is only safe inside that
	// container, so the Linux configurations run nowhere else.
	keystoreFixtureMarker = "/run/acp-go-pi-keystore/marker"

	// keystoreDarwinService is a service name this test owns end to end, so the
	// macOS third never reads, overwrites, or deletes a real login item.
	keystoreDarwinService = "acp-go-pi-residence-canary"

	// keystoreLinuxService is the name the keystore-present third seeds. pi's
	// credential storage admits a file backend and an in-memory one and nothing
	// else, so it documents no Secret Service name to reuse; this one is the
	// test's own, in a store the fixture container discards.
	keystoreLinuxService = "acp-go-pi-residence-canary"

	// keystoreResidenceAccount is the provider key a brokered credential carries
	// in auth.json, so the seeded item is filed under the name the read path
	// would have to answer with if residence ever moved into a keystore.
	keystoreResidenceAccount = "anthropic"
)

// The two canaries differ so the credential the read path answers from is
// distinguishable from the item seeded beside it. Both are canary material; no
// configuration plants a real credential.
const (
	keystoreResidenceCanary = "canary-not-a-real-credential"
	keystoreSeededCanary    = "canary-seeded-through-the-platform-keystore"
)

// TestKeystoreResidenceMatrix asserts residence, never login. pi's credential
// storage admits a file backend and an in-memory one and nothing else, so the
// answer to "where does a brokered credential live" is the agent directory's
// own auth.json in every configuration of this tier — keystore-absent Linux,
// keystore-present Linux, and macOS. Each configuration seeds the platform
// keystore it has, and the identity below must hold identically in all three.
// The directory is materialized through the session write path, so the
// filename, the mode and the resulting directory shape are the wrapper's
// behavior rather than this test's own choices.
func TestKeystoreResidenceMatrix(t *testing.T) {
	requireResidenceTier(t)
	seedResidenceKeystore(t)

	agentDir := t.TempDir()
	authPath := filepath.Join(agentDir, AuthFileName)

	require.NoError(t, AgentDir{
		Root:     agentDir,
		AuthJSON: []byte(`{"anthropic":{"type":"oauth","refresh":"` + keystoreResidenceCanary + `","access":"","expires":0}}`),
	}.Write())

	info, err := os.Stat(authPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	contents, err := os.ReadFile(authPath) // #nosec G304 -- the path is this test's own temp dir.
	require.NoError(t, err)
	require.Contains(t, string(contents), keystoreResidenceCanary)
	require.NotContains(t, string(contents), keystoreSeededCanary)

	entries, err := os.ReadDir(agentDir)
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	require.ElementsMatch(t, []string{AuthFileName, SettingsFileName, seedManifestFileName}, names,
		"the agent directory holds what the session write path wrote and no keystore sidecar")
}

// requireResidenceTier answers to both tier gates. On Linux it additionally
// requires the fixture container: planting a canary in a developer's live
// Secret Service is not something a test may do, and the container is where the
// driver runs this binary once per Linux configuration.
func requireResidenceTier(t *testing.T) {
	t.Helper()

	if os.Getenv(envRunIntegration) != "1" || os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 and %s=1 to run the credential-residence matrix",
			envRunIntegration, envRunKeystore)
	}

	if runtime.GOOS == "darwin" {
		return
	}

	if _, err := os.Stat(keystoreFixtureMarker); err != nil {
		t.Skipf("the Linux configurations run inside the keystore fixture container: %v", err)
	}
}

// seedResidenceKeystore plants canary material through the platform tool rather
// than through the read path, so what the matrix asserts is not one library's
// round trip against itself. The keystore-absent Linux configuration has no
// service to seed, which is that configuration rather than a caveat.
func seedResidenceKeystore(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "darwin" {
		seedDarwinKeychainCanary(t)

		return
	}

	if os.Getenv(envSessionBus) == "" {
		t.Log("keystore-absent configuration: no session bus is exported, so nothing is seeded")

		return
	}

	command := exec.CommandContext(t.Context(), "secret-tool", "store", "--label=pi-residence-canary",
		"service", keystoreLinuxService, "username", keystoreResidenceAccount)
	command.Stdin = strings.NewReader(keystoreSeededCanary)

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed keystore canary: %v: %s", err, output)
	}
}

// seedDarwinKeychainCanary writes into the real login keychain, because a
// keychain write under a scratch HOME blocks on an interactive authorization
// modal that never returns. The item carries this test's own service name and
// is deleted when the test ends.
func seedDarwinKeychainCanary(t *testing.T) {
	t.Helper()

	seed := exec.CommandContext(t.Context(), "security", "add-generic-password",
		"-U", "-s", keystoreDarwinService, "-a", keystoreResidenceAccount, "-w", keystoreSeededCanary)
	if output, err := seed.CombinedOutput(); err != nil {
		t.Fatalf("seed login keychain canary: %v: %s", err, output)
	}

	t.Cleanup(func() {
		remove := exec.Command("security", "delete-generic-password",
			"-s", keystoreDarwinService, "-a", keystoreResidenceAccount)
		if output, err := remove.CombinedOutput(); err != nil {
			t.Errorf("delete login keychain canary: %v: %s", err, output)
		}
	})
}
