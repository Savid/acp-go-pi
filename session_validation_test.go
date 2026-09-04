package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathValidationHelpers(t *testing.T) {
	require.Error(t, validateRequiredAbsolutePath("cwd", ""))
	require.Error(t, validateRequiredAbsolutePath("cwd", "relative"))
	require.NoError(t, validateRequiredAbsolutePath("cwd", absTestPath("absolute")))
	require.NoError(t, validateOptionalAbsolutePath("cwd", nil))
	empty := ""
	require.NoError(t, validateOptionalAbsolutePath("cwd", &empty))
	relative := "relative"
	require.Error(t, validateOptionalAbsolutePath("cwd", &relative))
	absolute := absTestPath("absolute")
	require.NoError(t, validateOptionalAbsolutePath("cwd", &absolute))
	require.Error(t, validateAbsolutePaths("paths", []string{""}))
	require.Error(t, validateAbsolutePaths("paths", []string{"relative"}))
	require.NoError(t, validateAbsolutePaths("paths", []string{absTestPath("one"), absTestPath("two")}))
	require.Error(t, validateSessionStartPaths("", nil))
	require.Error(t, validateSessionStartPaths(testCwd, []string{"relative"}))
	require.NoError(t, validateSessionStartPaths(testCwd, []string{absTestPath("also")}))
}

func TestConfigurationAndAdmissionEdges(t *testing.T) {
	_, err := resolveSessionConfiguration(PiOptions{}, sessionConfigurationPresence{}, sessionConfigurationRecord{
		Env: map[string]string{}, ExtraPathDirs: []string{"relative"},
	})
	require.Error(t, err)

	err = validateEnvironmentForPlatform(
		map[string]string{"Token": "one", "TOKEN": "two"},
		"env",
		func(string) bool { return false },
		true,
	)
	require.Error(t, err)
	require.NoError(t, validateEnvironmentForPlatform(
		map[string]string{"TOKEN": "one"},
		"env",
		func(string) bool { return false },
		true,
	))

	agent := NewAgent()
	agent.options.ProviderAuthRoot = "/provider-auth"
	agent.options.hostAuthoritySupplied = true
	require.NoError(t, configureProviderAuth(agent))

	agent.markNativeTreeBusy("")
	require.Empty(t, agent.nativeBusyRoots)
	path, err := agent.resolveExecutablePath()
	require.NoError(t, err)
	require.Equal(t, rawEventSourceValue, path)
}
