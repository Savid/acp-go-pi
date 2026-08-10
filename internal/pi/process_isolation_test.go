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
	isolation := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir, "BASE": "one"},
		TestOnlyNoCredential: true,
	}
	environment, err := isolationEnvironment(isolation, map[string]string{"BASE": "two", "EXPLICIT": "yes"})
	require.NoError(t, err)
	require.Contains(t, environment, "BASE=two")
	require.Contains(t, environment, "EXPLICIT=yes")
	require.NotContains(t, environment, "AMBIENT_ISOLATION_CANARY=must-not-leak")
	require.Equal(t, executable, mustResolveExecutable(t, "pi", isolation))
	_, err = ResolveExecutable("pi", &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}}, nil)
	require.Error(t, err)
	_, err = ResolveExecutable("relative/pi", isolation, nil)
	require.Error(t, err)
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"BAD=KEY": "x"}}))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"ACP_GO_PI_INTERNAL_ISOLATION_TEST_ONLY": "true"}}))
	require.Error(t, validateProcessIsolation(nil))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 1}))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 0, GID: 1}))
	require.Error(t, validateProcessIsolation(&ProcessIsolation{UID: 1, GID: 0}))
	_, err = isolationEnvironment(isolation, map[string]string{"BAD=KEY": "x"})
	require.Error(t, err)
	_, err = isolationEnvironment(isolation, map[string]string{envIsolationTest: "true"})
	require.Error(t, err)
	_, err = isolationEnvironment(nil)
	require.Error(t, err)
	_, err = ResolveExecutable("", isolation, nil)
	require.Error(t, err)
	_, err = ResolveExecutable("pi", &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"PATH": "relative"}}, nil)
	require.Error(t, err)
	_, err = ResolveExecutable("missing", isolation, nil)
	require.Error(t, err)
	_, err = ResolveExecutable("pi", nil, nil)
	require.Error(t, err)
	_, err = ResolveExecutable(dir, isolation, nil)
	require.Error(t, err)
	nonExecutable := filepath.Join(dir, "non-executable")
	require.NoError(t, os.WriteFile(nonExecutable, []byte("x"), 0o600))
	_, err = ResolveExecutable(nonExecutable, isolation, nil)
	require.Error(t, err)
	_, err = supervisorEnvironment(nil, nil, "MODE", "1")
	require.Error(t, err)
	supervisorEnv, err := supervisorEnvironment(
		[]string{"A=B", "MODE=old", envIsolationUID + "=old", envIsolationGID + "=old", envIsolationTest + "=old"},
		&ProcessIsolation{UID: 1, GID: 2, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}, "MODE", "1",
	)
	require.NoError(t, err)
	require.Contains(t, supervisorEnv, "A=B")
	require.Contains(t, supervisorEnv, "MODE=1")
	require.Contains(t, supervisorEnv, envIsolationTest+"=true")
}

func TestCaptureOrdinaryEnvironmentIsSanitizedAndStable(t *testing.T) {
	original := ordinaryEnvironmentEntries
	t.Cleanup(func() { ordinaryEnvironmentEntries = original })

	entries := []string{
		"PATH=relative/bin:/usr/bin",
		"HOME=/ordinary/home",
		"LC_ALL=C",
		"OPENAI_API_KEY=must-not-cross",
		"NODE_OPTIONS=--require=/tmp/inject.js",
		privateEnvPrefix + "CANARY=must-not-cross",
		"BAD=KEY=value",
	}
	ordinaryEnvironmentEntries = func() []string { return entries }

	captured := CaptureOrdinaryEnvironment()
	require.Equal(t, "relative/bin:/usr/bin", captured["PATH"])
	require.Equal(t, "/ordinary/home", captured["HOME"])
	require.Equal(t, "C", captured["LC_ALL"])
	require.NotContains(t, captured, "OPENAI_API_KEY")
	require.NotContains(t, captured, "NODE_OPTIONS")
	require.NotContains(t, captured, privateEnvPrefix+"CANARY")
	require.NotContains(t, captured, "BAD")

	entries[0] = "PATH=/mutated"
	require.Equal(t, "relative/bin:/usr/bin", captured["PATH"])
}

func TestOrdinaryExecutableLookupAcceptsOrdinaryPaths(t *testing.T) {
	dir := t.TempDir()
	workingDir, err := os.Getwd()
	require.NoError(t, err)
	relativeDir, err := filepath.Rel(workingDir, dir)
	require.NoError(t, err)
	executable := filepath.Join(dir, "pi")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700))

	resolved, err := ResolveExecutable("pi", nil, map[string]string{"PATH": relativeDir})
	require.NoError(t, err)
	require.Equal(t, executable, resolved)
	environment, err := ordinaryEnvironment(map[string]string{"PATH": relativeDir}, map[string]string{"EXPLICIT": "yes"})
	require.NoError(t, err)
	require.Contains(t, environment, "EXPLICIT=yes")

	resolved, err = ResolveExecutable(filepath.Join(relativeDir, "pi"), nil, map[string]string{})
	require.NoError(t, err)
	require.Equal(t, executable, resolved)

	_, err = ResolveExecutable("pi", &ProcessIsolation{
		UID: 1, GID: 1, BaseEnvironment: map[string]string{"PATH": relativeDir},
		StandaloneOwnerID:   "relative-policy-path-test",
		StandaloneStateRoot: "/var/tmp/acp-go-pi-relative-policy-path-test",
	}, nil)
	require.ErrorContains(t, err, "not absolute")

	_, err = ResolveExecutable("", nil, map[string]string{"PATH": relativeDir})
	require.ErrorContains(t, err, "empty")
	_, err = ResolveExecutable("pi", nil, map[string]string{"PATH": relativeDir}, map[string]string{privateEnvPrefix + "BAD": "x"})
	require.ErrorContains(t, err, "invalid key")
	_, err = ResolveExecutable("pi", &ProcessIsolation{
		UID: 1, GID: 1, BaseEnvironment: map[string]string{"PATH": dir}, TestOnlyNoCredential: true,
	}, nil, map[string]string{privateEnvPrefix + "BAD": "x"})
	require.ErrorContains(t, err, "invalid key")

	t.Run("empty path entry means working directory", func(t *testing.T) {
		t.Chdir(dir)
		resolved, resolveErr := ResolveExecutable("pi", nil, map[string]string{
			"PATH": string(os.PathListSeparator) + filepath.Join(dir, "missing"),
		})
		require.NoError(t, resolveErr)
		require.Equal(t, executable, resolved)
	})

	t.Run("absolute resolution error refuses relative executable", func(t *testing.T) {
		original := ordinaryExecutableAbs
		ordinaryExecutableAbs = func(string) (string, error) { return "", os.ErrInvalid }
		t.Cleanup(func() { ordinaryExecutableAbs = original })
		_, resolveErr := ResolveExecutable("relative/pi", nil, map[string]string{})
		require.ErrorIs(t, resolveErr, os.ErrInvalid)
	})
}

func mustResolveExecutable(t *testing.T, name string, isolation *ProcessIsolation) string {
	t.Helper()
	path, err := ResolveExecutable(name, isolation, nil)
	require.NoError(t, err)

	return path
}
