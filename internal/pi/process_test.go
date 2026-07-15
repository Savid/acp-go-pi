package pi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLaunchSpecArgs(t *testing.T) {
	t.Parallel()

	t.Run("hydrated launch with both extensions", func(t *testing.T) {
		t.Parallel()

		spec := LaunchSpec{
			AgentDir:            "/agent",
			SessionDir:          "/sessions",
			SessionPath:         "/sessions/restored.jsonl",
			ExtensionPaths:      []string{"/agent/extensions/seed.ts", "/agent/acp-bridge.ts", "/agent/acp-mcp.ts"},
			SkillPaths:          []string{"/agent/skills/review/SKILL.md"},
			PromptTemplatePaths: []string{"/agent/prompts/review.md"},
		}

		require.Equal(t, []string{
			"--mode", "rpc", "--no-extensions",
			"-e", "/agent/extensions/seed.ts",
			"-e", "/agent/acp-bridge.ts",
			"-e", "/agent/acp-mcp.ts",
			"--skill", "/agent/skills/review/SKILL.md",
			"--prompt-template", "/agent/prompts/review.md",
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
			"OPENAI_API_KEY":    "explicit-openai-key",
			"NODE_OPTIONS":      "--require=/tmp/hijack.js",
			"LD_PRELOAD":        "/tmp/hijack.so",
			"BAD-NAME":          "bad",
			"PATH":              "/tmp/hijack-bin",
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
	require.Equal(t, "explicit-openai-key", env["OPENAI_API_KEY"])
	require.Equal(t, "1", env["PI_OFFLINE"])
	require.Equal(t, "/agent", env["PI_CODING_AGENT_DIR"])
	require.NotContains(t, env, "NODE_OPTIONS")
	require.NotContains(t, env, "LD_PRELOAD")
	require.NotContains(t, env, "BAD-NAME")
	require.Equal(t, "/usr/bin", env["PATH"])
	require.NotContains(t, env, "TMPDIR")
	require.IsIncreasing(t, environ)
}

func TestSafeExplicitEnvKeyBoundary(t *testing.T) {
	require.False(t, safeExplicitEnvKey(""))
	require.True(t, safeExplicitEnvKey("A1"))
}

func writeScript(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "fake-pi")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700))

	return path
}

// startScriptProcess retries ETXTBSY: a concurrently forked child of a
// parallel test can inherit the just-written script's write descriptor across
// its own fork/exec window, making this exec transiently fail with
// "text file busy".
func startScriptProcess(t *testing.T, spec LaunchSpec) *Process {
	t.Helper()

	for attempt := 0; ; attempt++ {
		process, err := StartProcess(t.Context(), spec)
		if err == nil {
			return process
		}

		if attempt < 50 && errors.Is(err, syscall.ETXTBSY) {
			time.Sleep(10 * time.Millisecond)

			continue
		}

		require.NoError(t, err)
	}
}

func TestStartProcessValidation(t *testing.T) {
	t.Parallel()

	_, err := StartProcess(t.Context(), LaunchSpec{})
	require.ErrorContains(t, err, "executable path is required")

	_, err = StartProcess(t.Context(), LaunchSpec{ExecutablePath: filepath.Join(t.TempDir(), "missing")})
	require.ErrorContains(t, err, "start pi process")
}

func TestStartProcessPipeFailures(t *testing.T) {
	realPipe := processPipe
	t.Cleanup(func() { processPipe = realPipe })

	processPipe = func() (*os.File, *os.File, error) {
		return nil, nil, os.ErrPermission
	}
	_, err := StartProcess(t.Context(), LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorContains(t, err, "create stdin pipe")

	calls := 0
	processPipe = func() (*os.File, *os.File, error) {
		calls++
		if calls == 2 {
			return nil, nil, os.ErrPermission
		}

		return realPipe()
	}
	_, err = StartProcess(t.Context(), LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorContains(t, err, "create stdout pipe")
}

func TestProcessStdinEOFExit(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `cat >/dev/null; echo done; exit 0`)

	process := startScriptProcess(t, LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})

	t.Cleanup(func() { _ = process.Close() })

	require.ErrorContains(t, process.WaitErr(), "still running")

	_, err := process.Stdin().Write([]byte("swallowed by cat\n"))
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

	process := startScriptProcess(t, LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))
	require.NoError(t, process.WaitErr())
}

func TestProcessShutdownLadderSigterm(t *testing.T) {
	t.Parallel()

	// Ignores stdin EOF, exits on TERM.
	script := writeScript(t, `trap 'exit 0' TERM; while :; do sleep 0.1; done`)

	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath:      script,
		AgentDir:            t.TempDir(),
		ShutdownStepTimeout: 200 * time.Millisecond,
	})

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))
}

func TestProcessShutdownLadderSigkill(t *testing.T) {
	t.Parallel()

	// Ignores stdin EOF and TERM; only KILL works.
	script := writeScript(t, `trap '' TERM; while :; do sleep 0.1; done`)

	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath:      script,
		AgentDir:            t.TempDir(),
		ShutdownStepTimeout: 200 * time.Millisecond,
	})

	t.Cleanup(func() { _ = process.Close() })

	require.NoError(t, process.Shutdown(t.Context()))
	require.Error(t, process.WaitErr())
}

func TestProcessShutdownCanceled(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `trap '' TERM; while :; do sleep 0.1; done`)
	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath:      script,
		AgentDir:            t.TempDir(),
		ShutdownStepTimeout: time.Second,
	})
	t.Cleanup(func() { _ = process.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, process.Shutdown(ctx), context.Canceled)
	<-process.Exited()
}

func TestProcessKill(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `while :; do sleep 0.1; done`)

	process := startScriptProcess(t, LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})

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

	process := startScriptProcess(t, LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})

	t.Cleanup(func() { _ = process.Close() })

	<-process.Exited()
	require.ErrorContains(t, process.WaitErr(), "exit status 3")
	require.Contains(t, process.StderrTail(), "boom: real cause")
}

func TestProcessEnvironmentIsScrubbed(t *testing.T) {
	t.Setenv("TEST_AMBIENT_SECRET", "leak")

	script := writeScript(t, `env; exit 0`)

	process := startScriptProcess(t, LaunchSpec{
		ExecutablePath: script,
		AgentDir:       t.TempDir(),
		Env:            map[string]string{"TEST_EXPLICIT_VALUE": "ok"},
	})

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
	require.Contains(t, environ, "TEST_EXPLICIT_VALUE=ok")
	require.Contains(t, environ, "PI_OFFLINE=1")
	require.Contains(t, environ, "PI_CODING_AGENT_DIR=")
	require.NotContains(t, environ, "TEST_AMBIENT_SECRET")
}

func TestStartProcessCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := StartProcess(ctx, LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorIs(t, err, context.Canceled)
}

func TestStartProcessCancellationTerminatesContainedProcess(t *testing.T) {
	t.Parallel()

	script := writeScript(t, `trap '' TERM; while :; do sleep 0.1; done`)
	ctx, cancel := context.WithCancel(t.Context())
	process, err := StartProcess(ctx, LaunchSpec{ExecutablePath: script, AgentDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = process.Close() })

	cancel()

	select {
	case <-process.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("contained process did not exit after launch-context cancellation")
	}
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
