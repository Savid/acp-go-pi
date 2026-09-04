package piacp

import "testing"

func TestWindowsEnvironmentRejectsFoldedDuplicate(t *testing.T) {
	requireUnsupportedField(
		t,
		validateEnvironment(map[string]string{"Provider_Key": "session", "PROVIDER_KEY": "agent"}, metaOptionPath(metaEnvKey), blockedSessionEnvKey),
		metaOptionPath(metaEnvKey)+".Provider_Key",
	)
}
