package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProcessIsolationOptionIsRefusedOnWindows proves the option is rejected on
// Windows rather than silently accepted and ignored. Process isolation is the
// mechanism that drops the native agent to a separate account; a wrapper that
// took the option, reported no error, and then launched the agent under its own
// identity would hand a caller who asked for containment a completely
// uncontained agent.
func TestProcessIsolationOptionIsRefusedOnWindows(t *testing.T) {
	original := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = original })

	isolation := &ProcessIsolation{
		UID: 65534, GID: 65534,
		StandaloneOwnerID:   "windows-refusal",
		StandaloneStateRoot: "/srv/pi/state",
	}

	agentRuntimePlatform = windowsPlatform
	require.ErrorContains(
		t,
		validateProcessIsolationOption(isolation),
		"process isolation is unsupported on windows",
	)

	agentRuntimePlatform = darwinPlatform
	require.NoError(t, validateProcessIsolationOption(isolation))
}
