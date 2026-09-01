//go:build !windows

package pi

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrdinaryEnvironmentAndExecutableResolutionEdges(t *testing.T) {
	t.Parallel()

	require.False(t, ordinaryEnvironmentKey(privateEnvPrefix+"SECRET"))
	require.False(t, ordinaryEnvironmentKey("BAD=NAME"))
	require.False(t, ordinaryEnvironmentKey("BAD\x00NAME"))
	require.True(t, ordinaryEnvironmentKey("lc_messages"))
	require.False(t, ordinaryEnvironmentKey("OPENAI_API_KEY"))

	environment := []string{"PATH=/first", "malformed", "PATH=/last"}
	require.Equal(t, "/last", environmentValue(environment, "PATH"))
	require.Empty(t, environmentValue(environment, "HOME"))

	dir := t.TempDir()
	executable := filepath.Join(dir, "pi")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o700))
	resolved, err := ResolveExecutable("pi", map[string]string{"PATH": "/missing"}, map[string]string{"PATH": dir})
	require.NoError(t, err)
	require.Equal(t, executable, resolved)

	_, err = ResolveExecutable("missing", map[string]string{"PATH": dir})
	require.ErrorContains(t, err, "resolve pi executable")
}

func TestOrdinaryUnixLookupEdges(t *testing.T) {
	_, err := lookPathInOrdinaryEnvironment("", nil)
	require.ErrorContains(t, err, "name is empty")

	dir := t.TempDir()
	executable := filepath.Join(dir, "pi")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o700))
	resolved, err := lookPathInOrdinaryEnvironment(executable, nil)
	require.NoError(t, err)
	require.Equal(t, executable, resolved)

	nonExecutable := filepath.Join(dir, "plain")
	require.NoError(t, os.WriteFile(nonExecutable, nil, 0o600))
	_, err = executableFile(nonExecutable)
	require.ErrorContains(t, err, "not executable")
	_, err = executableFile(dir)
	require.ErrorContains(t, err, "not executable")
	_, err = executableFile(filepath.Join(dir, "missing"))
	require.Error(t, err)

	originalAbs := ordinaryExecutableAbs
	t.Cleanup(func() { ordinaryExecutableAbs = originalAbs })
	ordinaryExecutableAbs = func(string) (string, error) { return "", errors.New("abs fault") }
	_, err = lookPathInOrdinaryEnvironment("relative/pi", nil)
	require.ErrorContains(t, err, "abs fault")
	ordinaryExecutableAbs = func(string) (string, error) { return executable, nil }
	resolved, err = lookPathInOrdinaryEnvironment("relative/pi", nil)
	require.NoError(t, err)
	require.Equal(t, executable, resolved)
	resolved, err = lookPathInOrdinaryEnvironment("pi", []string{"PATH=:"})
	require.NoError(t, err)
	require.Equal(t, executable, resolved)
}

func TestOrdinaryProcessDetachedFailureEdges(t *testing.T) {
	script := writeOrdinaryScript(t, "exit 0")
	_, err := StartOrdinaryProcess(t.Context(), LaunchSpec{
		ExecutablePath:  script,
		AgentDir:        t.TempDir(),
		BaseEnvironment: map[string]string{"PATH": os.Getenv("PATH")},
		Cwd:             filepath.Join(t.TempDir(), "missing"),
	})
	require.ErrorContains(t, err, "start pi process")

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	process := &Process{
		stdin:               &stubWriteCloser{},
		stdout:              &stubReadCloser{},
		stderr:              &tailBuffer{limit: 8},
		shutdownStepTimeout: time.Hour,
		exited:              make(chan struct{}),
	}
	require.ErrorIs(t, process.Shutdown(cancelled), context.Canceled)
	require.False(t, process.waitStep(cancelled))
	require.NoError(t, (*Process)(nil).Kill())

	running := startOrdinaryScript(t, "while :; do sleep 1; done", time.Second)
	require.NoError(t, running.Close())
}

func TestOrdinaryProcessPipeAndKillFaults(t *testing.T) {
	originalPipe := ordinaryProcessPipe
	originalKill := ordinaryProcessKill
	t.Cleanup(func() {
		ordinaryProcessPipe = originalPipe
		ordinaryProcessKill = originalKill
	})

	ordinaryProcessPipe = func() (*os.File, *os.File, error) {
		return nil, nil, errors.New("pipe fault")
	}
	_, err := StartOrdinaryProcess(t.Context(), LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorContains(t, err, "create native stdin")

	calls := 0
	ordinaryProcessPipe = func() (*os.File, *os.File, error) {
		calls++
		if calls == 2 {
			return nil, nil, errors.New("pipe fault")
		}

		return os.Pipe()
	}
	_, err = StartOrdinaryProcess(t.Context(), LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorContains(t, err, "create native stdout")

	ordinaryProcessKill = func(*os.Process) error { return errors.New("kill fault") }
	process := &Process{
		cmd:                 &exec.Cmd{Process: &os.Process{}},
		stdin:               &stubWriteCloser{},
		stdout:              &stubReadCloser{},
		stderr:              &tailBuffer{limit: 8},
		shutdownStepTimeout: time.Nanosecond,
		exited:              make(chan struct{}),
	}
	require.ErrorContains(t, process.Shutdown(t.Context()), "kill fault")
}

type stubWriteCloser struct{}

func (*stubWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (*stubWriteCloser) Close() error                { return nil }

type stubReadCloser struct{}

func (*stubReadCloser) Read([]byte) (int, error) { return 0, errors.New("read fault") }
func (*stubReadCloser) Close() error             { return nil }
