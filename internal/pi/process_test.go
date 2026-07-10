package pi

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLaunchSpecArgs(t *testing.T) {
	t.Parallel()

	t.Run("hydrated launch with both extensions", func(t *testing.T) {
		t.Parallel()

		spec := LaunchSpec{
			AgentDir:       "/agent",
			SessionDir:     "/sessions",
			SessionPath:    "/sessions/restored.jsonl",
			ExtensionPaths: []string{"/agent/acp-bridge.ts", "/agent/acp-mcp.ts"},
		}

		require.Equal(t, []string{
			"--mode", "rpc", "--no-extensions",
			"-e", "/agent/acp-bridge.ts",
			"-e", "/agent/acp-mcp.ts",
			"--no-skills", "--no-prompt-templates", "--no-themes",
			"--session-dir", "/sessions",
			"--no-approve",
			"--session", "/sessions/restored.jsonl",
		}, spec.Args())
	})

	t.Run("minimal launch omits session selection", func(t *testing.T) {
		t.Parallel()

		spec := LaunchSpec{
			AgentDir:       "/agent",
			SessionDir:     "/sessions",
			ExtensionPaths: []string{"/agent/acp-bridge.ts"},
		}

		args := spec.Args()
		require.NotContains(t, args, "--session")
		require.NotContains(t, args, "--session-id")
		require.Equal(t, 1, countOccurrences(args, "-e"))
	})

	t.Run("exact session id selection", func(t *testing.T) {
		t.Parallel()

		spec := LaunchSpec{
			AgentDir:   "/agent",
			SessionDir: "/sessions",
			SessionID:  "0199a940-2323-7abc-8000-000000000000",
		}

		args := spec.Args()
		require.NotContains(t, args, "--session")
		require.Equal(t, "--session-id", args[len(args)-2])
		require.Equal(t, "0199a940-2323-7abc-8000-000000000000", args[len(args)-1])
	})
}

func countOccurrences(values []string, want string) int {
	count := 0

	for _, value := range values {
		if value == want {
			count++
		}
	}

	return count
}

func TestLaunchSpecEnviron(t *testing.T) {
	t.Setenv("PATH", "/usr/bin")
	t.Setenv("HOME", "/home/user")
	t.Setenv("OPENAI_API_KEY", "ambient-secret")
	t.Setenv("ANTHROPIC_API_KEY", "ambient-secret")
	t.Setenv("TMPDIR", "")
	require.NoError(t, os.Unsetenv("TMPDIR"))

	spec := LaunchSpec{
		AgentDir: "/agent",
		Env: map[string]string{
			"ANTHROPIC_API_KEY": "explicit-key",
			"PI_OFFLINE":        "0", // managed keys always win
		},
	}

	environ := spec.Environ()
	env := make(map[string]string, len(environ))

	for _, entry := range environ {
		key, value, found := strings.Cut(entry, "=")
		require.True(t, found)
		env[key] = value
	}

	require.Equal(t, "/usr/bin", env["PATH"])
	require.Equal(t, "/home/user", env["HOME"])
	require.Equal(t, "explicit-key", env["ANTHROPIC_API_KEY"])
	require.Equal(t, "1", env["PI_OFFLINE"])
	require.Equal(t, "/agent", env["PI_CODING_AGENT_DIR"])
	require.NotContains(t, env, "OPENAI_API_KEY")
	require.NotContains(t, env, "TMPDIR")
	require.IsIncreasing(t, environ)
}

func writeScript(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "fake-pi")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700))

	return path
}

func TestStartProcessValidation(t *testing.T) {
	t.Parallel()

	_, err := StartProcess(t.Context(), LaunchSpec{})
	require.ErrorContains(t, err, "executable path is required")

	_, err = StartProcess(t.Context(), LaunchSpec{ExecutablePath: filepath.Join(t.TempDir(), "missing")})
	require.ErrorContains(t, err, "start pi process")
}

func TestProcessStdinEOFExit(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `cat >/dev/null; echo done; exit 0`)

	process, err := StartProcess(t.Context(), LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	require.ErrorContains(t, process.WaitErr(), "still running")

	_, err = process.Stdin().Write([]byte("swallowed by cat\n"))
	require.NoError(t, err)

	require.NoError(t, process.CloseStdin())
	require.NoError(t, process.CloseStdin()) // idempotent

	select {
	case <-process.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit on stdin EOF")
	}

	require.NoError(t, process.WaitErr())

	output := make([]byte, 16)
	n, _ := process.Stdout().Read(output)
	require.Equal(t, "done\n", string(output[:n]))
}

func TestProcessShutdownLadderStdinEOF(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `cat >/dev/null; exit 0`)

	process, err := StartProcess(t.Context(), LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))
	require.NoError(t, process.WaitErr())
}

func TestProcessShutdownLadderSigterm(t *testing.T) {
	t.Parallel()

	// Ignores stdin EOF, exits on TERM.
	script := writeScript(t, `trap 'exit 0' TERM; while :; do sleep 0.1; done`)

	process, err := StartProcess(t.Context(), LaunchSpec{
		ExecutablePath:      script,
		AgentDir:            t.TempDir(),
		ShutdownStepTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))
}

func TestProcessShutdownLadderSigkill(t *testing.T) {
	t.Parallel()

	// Ignores stdin EOF and TERM; only KILL works.
	script := writeScript(t, `trap '' TERM; while :; do sleep 0.1; done`)

	process, err := StartProcess(t.Context(), LaunchSpec{
		ExecutablePath:      script,
		AgentDir:            t.TempDir(),
		ShutdownStepTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))
	require.Error(t, process.WaitErr())
}

func TestProcessKill(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `while :; do sleep 0.1; done`)

	process, err := StartProcess(t.Context(), LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Kill())

	select {
	case <-process.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after kill")
	}
}

func TestProcessStderrTail(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `echo "boom: real cause" >&2; exit 3`)

	process, err := StartProcess(t.Context(), LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	<-process.Exited()
	require.ErrorContains(t, process.WaitErr(), "exit status 3")
	require.Contains(t, process.StderrTail(), "boom: real cause")
}

func TestProcessEnvironmentIsScrubbed(t *testing.T) {
	t.Setenv("ACP_GO_PI_TEST_AMBIENT", "leak")

	script := writeScript(t, `env; exit 0`)

	process, err := StartProcess(t.Context(), LaunchSpec{
		ExecutablePath: script,
		AgentDir:       t.TempDir(),
		Env:            map[string]string{"ACP_GO_PI_TEST_EXPLICIT": "ok"},
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = process.Close() })

	output := make([]byte, 0, 4<<10)
	buffer := make([]byte, 1<<10)

	for {
		n, readErr := process.Stdout().Read(buffer)
		output = append(output, buffer[:n]...)

		if readErr != nil {
			break
		}
	}

	<-process.Exited()

	environ := string(output)
	require.Contains(t, environ, "ACP_GO_PI_TEST_EXPLICIT=ok")
	require.Contains(t, environ, "PI_OFFLINE=1")
	require.Contains(t, environ, "PI_CODING_AGENT_DIR=")
	require.NotContains(t, environ, "ACP_GO_PI_TEST_AMBIENT")
}

func TestStartProcessCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := StartProcess(ctx, LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorIs(t, err, context.Canceled)
}

func TestTailBufferBounds(t *testing.T) {
	t.Parallel()

	buffer := &tailBuffer{limit: 8}

	n, err := buffer.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "23456789", buffer.String())

	_, err = buffer.Write([]byte("ab"))
	require.NoError(t, err)
	require.Equal(t, "456789ab", buffer.String())
}
