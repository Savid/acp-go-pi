//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestPiCLIVersionProbe(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	version, err := pi.ProbeVersion(ctx, path)
	require.NoError(t, err)
	require.NotEmpty(t, version)
	require.NoError(t, pi.CheckMinimumVersion(version, pi.DefaultMinimumVersion),
		"installed pi %s is older than the supported minimum %s", version, pi.DefaultMinimumVersion)
}

// TestPiCLIBridgeExtensionLoads proves the wrapper-owned bridge extension
// loads in the real binary with the wrapper's exact launch posture: silent
// startup, no commands registered, clean stdin-EOF exit, empty stderr.
func TestPiCLIBridgeExtensionLoads(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := startHarness(t, ctx, path, true)

	state, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, state.SessionID)
	require.False(t, state.IsStreaming)

	commands, err := h.client.GetCommands(ctx)
	require.NoError(t, err)
	require.Empty(t, commands, "the bridge extension deliberately registers no commands")

	require.NoError(t, h.client.SetAutoRetry(ctx, false))

	start := time.Now()
	require.NoError(t, h.process.CloseStdin())

	select {
	case <-h.process.Exited():
		require.NoError(t, h.process.WaitErr())
		require.Less(t, time.Since(start), 5*time.Second, "stdin EOF must exit pi immediately")
	case <-time.After(10 * time.Second):
		t.Fatalf("pi did not exit on stdin EOF; stderr: %s", h.process.StderrTail())
	}

	require.Empty(t, h.process.StderrTail(), "startup and shutdown must be silent on stderr")
	require.Zero(t, h.client.DecodeFailures())
}
