package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProcessIsolationCovResolveRefusesUnusablePolicySearchPath proves that
// executable resolution never falls back to the ambient PATH or to a
// relative search directory. A policy whose environment omits PATH must be
// refused outright, and a policy whose PATH carries a relative entry must be
// refused without ever probing that entry, even when a matching executable
// exists in the process working directory that the relative entry names.
func TestProcessIsolationCovResolveRefusesUnusablePolicySearchPath(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pi"), []byte("#!/bin/sh\n"), 0o700))
	t.Setenv("PATH", dir)
	t.Chdir(filepath.Dir(dir))

	missingPath := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{"HOME": "/nonexistent"},
		TestOnlyNoCredential: true,
	}
	require.Empty(t, environmentValue([]string{"HOME=/nonexistent"}, envPath))
	_, err := ResolveExecutable("pi", missingPath)
	require.ErrorContains(t, err, `executable "pi" cannot be resolved without policy PATH`)

	relativePath := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{envPath: filepath.Base(dir)},
		TestOnlyNoCredential: true,
	}
	_, err = ResolveExecutable("pi", relativePath)
	require.ErrorContains(t, err, "policy PATH entry")
	require.ErrorContains(t, err, "is not absolute")

	resolved, err := ResolveExecutable("pi", &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{envPath: dir}, TestOnlyNoCredential: true,
	})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "pi"), resolved)
}
