package pi

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is required to run the bridge extension")
	}

	stubs, err := filepath.Abs(filepath.Join("testdata", "bridge-probe"))
	require.NoError(t, err)

	extension := filepath.Join(t.TempDir(), BridgeExtensionFileName)
	require.NoError(t, os.WriteFile(extension, bridgeExtensionSource, 0o600))

	command := exec.Command(node, "--experimental-strip-types", filepath.Join(stubs, "driver.mjs"), extension, stubs)
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
