package pi

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireNode resolves the runtime the bridge extension needs. A developer
// machine without node skips these two, but a CI run does not: the only reason
// the extension is exercised at all is that this suite runs it, and a silent
// skip there retires that coverage without anyone noticing.
func requireNode(t *testing.T) string {
	t.Helper()

	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("node is required to run the bridge extension: %v", err)
		}

		t.Skipf("node is required to run the bridge extension: %v", err)
	}

	return node
}

// bridgeProbeExtension writes the embedded bridge extension where a driver can
// load it, and answers the stub directory the drivers live in.
func bridgeProbeExtension(t *testing.T) (extension string, stubs string) {
	t.Helper()

	stubs, err := filepath.Abs(filepath.Join("testdata", "bridge-probe"))
	require.NoError(t, err)

	extension = filepath.Join(t.TempDir(), BridgeExtensionFileName)
	require.NoError(t, os.WriteFile(extension, bridgeExtensionSource, 0o600))

	return extension, stubs
}

// TestBridgeExtensionDeliversADeviceCodePresentation runs the real bridge
// extension against a fake extension host with a device-code login: one
// notify, then a poll that returns control only when the provider settles or
// the login aborts. Three of pi's seven oauth providers have exactly that
// shape, and the Go suite cannot see them, because it scripts the bridge's
// messages rather than running it. The three properties pinned here are the
// ones the legs depend on: the presentation reaches the wrapper while the login
// is still polling, answering the abort watch stops that poll, and the probe
// answers every provider it was asked about — an omitted entry would make key
// presence the residence signal, which no reader on the Go side can see it is
// relying on.
func TestBridgeExtensionDeliversADeviceCodePresentation(t *testing.T) {
	t.Parallel()

	node := requireNode(t)
	extension, stubs := bridgeProbeExtension(t)

	command := exec.Command(node, "--experimental-strip-types", filepath.Join(stubs, "driver.mjs"), extension)
	command.Env = append(os.Environ(), "ACP_GO_PI_PERMISSION=allow")

	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	lines := strings.Fields(strings.TrimSpace(string(output)))
	require.Contains(t, lines, "PRESENTED")
	require.Contains(t, lines, "AAAA-BBBB")
	require.Contains(t, lines, "WATCHING")
	require.Contains(t, lines, "ABORTED")
	require.Contains(t, lines, "true")
	require.Contains(t, string(output), `PROBED {"probe":"api_key","absent":""}`)
}

// TestBridgeExtensionStoresABrokeredCredential runs the real bridge extension
// through a login that settles and reads the file it left behind. The write
// into pi's own AuthStorage is the last act of a brokered login and the only
// one with an effect outside the extension: drop it and the extension still
// reports the same successful result, the probe still answers, the wrapper's
// ledger still records a connection — over a credential store that never
// received the credential. Only the resulting auth.json distinguishes the two.
func TestBridgeExtensionStoresABrokeredCredential(t *testing.T) {
	t.Parallel()

	node := requireNode(t)
	extension, stubs := bridgeProbeExtension(t)
	agentDir := t.TempDir()

	command := exec.Command(node, "--experimental-strip-types", filepath.Join(stubs, "login-write.mjs"), extension)
	command.Env = append(os.Environ(),
		"ACP_GO_PI_PERMISSION=allow",
		"PROBE_PACKAGE_DIR="+bridgeProbeInstallRoot(t, stubs),
		"PI_CODING_AGENT_DIR="+agentDir,
	)

	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "RESULT ok=true cause= type=oauth")

	stored, err := os.ReadFile(filepath.Join(agentDir, AuthFileName)) // #nosec G304 -- the path is this test's own temp dir.
	require.NoError(t, err, string(output))

	var record map[string]map[string]any
	require.NoError(t, json.Unmarshal(stored, &record))
	require.Equal(t, map[string]any{
		"type":    "oauth",
		"access":  "settled-access",
		"refresh": "settled-refresh",
		"expires": float64(4242),
	}, record["settle"])
}

// bridgeProbeInstallRoot assembles the fixture pi install root the extension
// resolves its storage module under. The layout is built here rather than
// tracked, because the path the extension composes runs through a "dist"
// segment this repository ignores; the package.json makes node read the module
// as ESM, which is what pi ships.
func bridgeProbeInstallRoot(t *testing.T, stubs string) string {
	t.Helper()

	root := t.TempDir()
	core := filepath.Join(root, "dist", "core")
	require.NoError(t, os.MkdirAll(core, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"type":"module"}`), 0o600))

	storage, err := os.ReadFile(filepath.Join(stubs, "auth-storage.mjs"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(core, "auth-storage.js"), storage, 0o600))

	return root
}
