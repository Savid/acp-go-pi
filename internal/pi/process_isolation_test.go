package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessIsolationEnvironmentIdentityAndLookup(t *testing.T) {
	t.Setenv("AMBIENT_ISOLATION_CANARY", "must-not-leak")
	dir := t.TempDir()
	executable := filepath.Join(dir, "pi")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700))
	isolation := &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir, "BASE": "one"}}
	environment, err := isolationEnvironment(isolation, map[string]string{"BASE": "two", "EXPLICIT": "yes"})
	require.NoError(t, err)
	require.Contains(t, environment, "BASE=two")
	require.Contains(t, environment, "EXPLICIT=yes")
	require.NotContains(t, environment, "AMBIENT_ISOLATION_CANARY=must-not-leak")
	require.Equal(t, executable, mustResolveExecutable(t, "pi", isolation))
	_, err = ResolveExecutable("pi", &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}})
	require.Error(t, err)
	_, err = ResolveExecutable("relative/pi", isolation)
	require.Error(t, err)
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"BAD=KEY": "x"}}))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"ACP_GO_PI_INTERNAL_ISOLATION_TEST_ONLY": "true"}}))
	require.Error(t, validateProcessIsolation(nil))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 0, GID: 1}))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 0}))
	_, err = isolationEnvironment(isolation, map[string]string{"BAD=KEY": "x"})
	require.Error(t, err)
	_, err = isolationEnvironment(isolation, map[string]string{envIsolationTest: "true"})
	require.Error(t, err)
	_, err = isolationEnvironment(nil)
	require.Error(t, err)
	_, err = ResolveExecutable("", isolation)
	require.Error(t, err)
	_, err = ResolveExecutable("pi", &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"PATH": "relative"}})
	require.Error(t, err)
	_, err = ResolveExecutable("missing", isolation)
	require.Error(t, err)
	_, err = ResolveExecutable("pi", nil)
	require.Error(t, err)
	_, err = ResolveExecutable(dir, isolation)
	require.Error(t, err)
	nonExecutable := filepath.Join(dir, "non-executable")
	require.NoError(t, os.WriteFile(nonExecutable, []byte("x"), 0o600))
	_, err = ResolveExecutable(nonExecutable, isolation)
	require.Error(t, err)
	_, err = supervisorEnvironment(nil, nil, "MODE", "1")
	require.Error(t, err)
	supervisorEnv, err := supervisorEnvironment(
		[]string{"A=B", "MODE=old", envIsolationUID + "=old", envIsolationGID + "=old", envIsolationTest + "=old"},
		&ProcessIsolation{UID: 1, GID: 2, TestOnlyNoCredential: true}, "MODE", "1",
	)
	require.NoError(t, err)
	require.Contains(t, supervisorEnv, "A=B")
	require.Contains(t, supervisorEnv, "MODE=1")
	require.Contains(t, supervisorEnv, envIsolationTest+"=true")
}

func mustResolveExecutable(t *testing.T, name string, isolation *ProcessIsolation) string {
	t.Helper()
	path, err := ResolveExecutable(name, isolation)
	require.NoError(t, err)

	return path
}
