package piacp

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type processIsolationCapabilityStub struct{}

func (processIsolationCapabilityStub) Duplicate() (*os.File, error) { return os.Open(os.DevNull) }

func TestStandaloneProcessIsolationValidation(t *testing.T) {
	original := agentRuntimePlatform
	agentRuntimePlatform = linuxPlatform
	t.Cleanup(func() { agentRuntimePlatform = original })

	capability := processIsolationCapabilityStub{}
	valid := &ProcessIsolation{
		UID: 1, GID: 2, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: "A._:@/-0zZ9", StandaloneStateRoot: "/srv/pi/state",
	}
	require.NoError(t, validateProcessIsolationOption(valid))

	tests := []struct {
		name      string
		isolation *ProcessIsolation
	}{
		{name: "identity only", isolation: &ProcessIsolation{UID: 1, GID: 2, IdentityLock: capability}},
		{name: "domain only", isolation: &ProcessIsolation{UID: 1, GID: 2, AuthorityDomain: capability}},
		{name: "borrowed with owner", isolation: &ProcessIsolation{UID: 1, GID: 2, IdentityLock: capability, AuthorityDomain: capability, StandaloneOwnerID: "owner"}},
		{name: "bad owner", isolation: &ProcessIsolation{UID: 1, GID: 2, StandaloneOwnerID: "-owner", StandaloneStateRoot: "/srv/pi/state"}},
		{name: "bad root", isolation: &ProcessIsolation{UID: 1, GID: 2, StandaloneOwnerID: "owner", StandaloneStateRoot: "relative"}},
	}
	for _, test := range tests {
		require.Error(t, validateProcessIsolationOption(test.isolation), test.name)
	}
	require.NoError(t, validateProcessIsolationOption(&ProcessIsolation{
		UID: 1, GID: 2, IdentityLock: capability, AuthorityDomain: capability,
	}))

	for _, ownerID := range []string{"", strings.Repeat("a", 257), "-owner", "owner id"} {
		require.False(t, validStandaloneOwnerID(ownerID), ownerID)
	}
	require.True(t, validStandaloneOwnerID("A._:@/-0zZ9"))

	for _, stateRoot := range []string{
		"", strings.Repeat("x", 4097), string([]byte{'/', 0xff}), "relative", "/srv/../state", "/",
		"/srv/state\x00", "/var/lib/acp-go/agent-identities", "/var/lib/acp-go/agent-identities/pi", "/srv/\nstate",
	} {
		require.False(t, validStandaloneStateRootPath(stateRoot), stateRoot)
	}
	require.True(t, validStandaloneStateRootPath("/srv/pi/state"))
}

func TestPathValidationHelpers(t *testing.T) {
	require.Error(t, validateRequiredAbsolutePath("cwd", ""))
	require.Error(t, validateRequiredAbsolutePath("cwd", "relative"))
	require.NoError(t, validateRequiredAbsolutePath("cwd", "/absolute"))
	require.NoError(t, validateOptionalAbsolutePath("cwd", nil))
	empty := ""
	require.NoError(t, validateOptionalAbsolutePath("cwd", &empty))
	relative := "relative"
	require.Error(t, validateOptionalAbsolutePath("cwd", &relative))
	absolute := "/absolute"
	require.NoError(t, validateOptionalAbsolutePath("cwd", &absolute))
	require.Error(t, validateAbsolutePaths("paths", []string{""}))
	require.Error(t, validateAbsolutePaths("paths", []string{"relative"}))
	require.NoError(t, validateAbsolutePaths("paths", []string{"/one", "/two"}))
	require.Error(t, validateSessionStartPaths("", nil))
	require.Error(t, validateSessionStartPaths("/cwd", []string{"relative"}))
	require.NoError(t, validateSessionStartPaths("/cwd", []string{"/also"}))
}
