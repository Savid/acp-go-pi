package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowsEnvironmentRejectsFoldedDuplicate(t *testing.T) {
	require.Equal(
		t,
		ambiguousField(metaOptionPath(metaEnvKey)+".Provider_Key"),
		validateEnvironment(map[string]string{"Provider_Key": "session", "PROVIDER_KEY": "agent"}, metaOptionPath(metaEnvKey), blockedSessionEnvKey),
	)
}
