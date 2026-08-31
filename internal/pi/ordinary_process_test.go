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

	spec := LaunchSpec{
		AgentDir:            "/agent",
		SessionDir:          "/sessions",
		SessionPath:         "/sessions/restored.jsonl",
		ExtensionPaths:      []string{"/agent/extensions/seed.ts", "/agent/acp-bridge.ts"},
		SkillPaths:          []string{"/agent/skills/review/SKILL.md"},
		PromptTemplatePaths: []string{"/agent/prompts/review.md"},
	}
	require.Equal(t, []string{
		"--mode", "rpc", "--no-extensions",
		"-e", "/agent/extensions/seed.ts",
		"-e", "/agent/acp-bridge.ts",
		"--skill", "/agent/skills/review/SKILL.md",
		"--prompt-template", "/agent/prompts/review.md",
		"--no-skills", "--no-prompt-templates", "--no-themes",
		"--session-dir", "/sessions", "--no-approve",
		"--session", "/sessions/restored.jsonl",
	}, spec.Args())

	spec.SessionPath = ""
	spec.SessionID = "session-id"
	args := spec.Args()
	require.Equal(t, []string{"--session-id", "session-id"}, args[len(args)-2:])
}

func TestLaunchSpecEnviron(t *testing.T) {
	t.Parallel()

	separator := string(os.PathListSeparator)
	spec := LaunchSpec{
		AgentDir:        "/agent",
		BaseEnvironment: map[string]string{"PATH": "/usr/bin", "HOME": "/home/native"},
		ExtraPathDirs:   []string{"/session/bin"},
		Env: map[string]string{
			"OPENAI_API_KEY": "explicit",
			"NODE_OPTIONS":   "--require=/tmp/inject.js",
			"LD_PRELOAD":     "/tmp/inject.so",
			"PI_OFFLINE":     "0",
		},
	}

	environment := spec.Environ()
	require.IsIncreasing(t, environment)
	require.Contains(t, environment, "PATH=/session/bin"+separator+"/usr/bin")
	require.Contains(t, environment, "HOME=/home/native")
	require.Contains(t, environment, "OPENAI_API_KEY=explicit")
	require.Contains(t, environment, "PI_OFFLINE=1")
	require.Contains(t, environment, "PI_CODING_AGENT_DIR=/agent")
	require.NotContains(t, strings.Join(environment, "\n"), "NODE_OPTIONS")
	require.NotContains(t, strings.Join(environment, "\n"), "LD_PRELOAD")
}

func TestPrependPathDirsDropsUnusableEntries(t *testing.T) {
	t.Parallel()

	separator := string(os.PathListSeparator)
	require.Equal(t, "/usr/bin", prependPathDirs("/usr/bin", nil))
	require.Equal(t, "/usr/bin", prependPathDirs("/usr/bin", []string{"relative", ""}))
	require.Equal(t, "/opt/bin"+separator+"/usr/bin", prependPathDirs("/usr/bin", []string{"/opt/bin"}))
	require.Equal(t, "/usr/bin", prependPathDirs("/usr/bin", []string{"/a" + separator + "/b"}))
}

func TestSafeExplicitEnvKeyBoundary(t *testing.T) {
	t.Parallel()

	require.True(t, safeExplicitEnvKey("A1"))
	require.True(t, safeExplicitEnvKey("PATH"))
	for _, key := range []string{"", "BAD-NAME", "NODE_OPTIONS", "BASH_ENV", "ENV", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "ACP_GO_PI_INTERNAL_X"} {
		require.False(t, safeExplicitEnvKey(key), key)
	}
}

func writeOrdinaryScript(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "fake-pi")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700))

	return path
}

func startOrdinaryScript(t *testing.T, body string, step time.Duration) *Process {
	t.Helper()

	executable := writeOrdinaryScript(t, body)
	spec := LaunchSpec{
		ExecutablePath:      executable,
		AgentDir:            t.TempDir(),
		BaseEnvironment:     map[string]string{"PATH": os.Getenv("PATH")},
		ShutdownStepTimeout: step,
	}
	var process *Process
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		process, err = StartOrdinaryProcess(t.Context(), spec)
		if err == nil || !errors.Is(err, syscall.ETXTBSY) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = process.Close() })

	return process
}

func TestStartOrdinaryProcessValidation(t *testing.T) {
	t.Parallel()

	_, err := StartOrdinaryProcess(t.Context(), LaunchSpec{})
	require.ErrorContains(t, err, "executable path is required")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = StartOrdinaryProcess(ctx, LaunchSpec{ExecutablePath: "/bin/sh"})
	require.ErrorIs(t, err, context.Canceled)

	_, err = StartOrdinaryProcess(t.Context(), LaunchSpec{ExecutablePath: filepath.Join(t.TempDir(), "missing")})
	require.Error(t, err)
}

func TestOrdinaryProcessStdinEOFAndOutput(t *testing.T) {
	t.Parallel()

	process := startOrdinaryScript(t, `cat >/dev/null; echo done`, time.Second)
	require.ErrorContains(t, process.WaitErr(), "still running")
	_, err := process.Stdin().Write([]byte("input\n"))
	require.NoError(t, err)
	require.NoError(t, process.CloseStdin())
	require.NoError(t, process.CloseStdin())

	select {
	case <-process.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary process did not exit on stdin EOF")
	}
	require.NoError(t, process.WaitErr())
	data := make([]byte, 16)
	n, _ := process.Stdout().Read(data)
	require.Equal(t, "done\n", string(data[:n]))
}

func TestOrdinaryProcessShutdownAndKill(t *testing.T) {
	t.Parallel()

	graceful := startOrdinaryScript(t, `cat >/dev/null`, time.Second)
	require.NoError(t, graceful.Shutdown(t.Context()))
	require.NoError(t, graceful.WaitErr())

	forced := startOrdinaryScript(t, `trap '' TERM; while :; do sleep 0.1; done`, 20*time.Millisecond)
	require.NoError(t, forced.Shutdown(t.Context()))
	require.Error(t, forced.WaitErr())

	killed := startOrdinaryScript(t, `while :; do sleep 0.1; done`, time.Second)
	require.NoError(t, killed.Kill())
	select {
	case <-killed.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary process did not exit after kill")
	}
}

func TestOrdinaryProcessStderrAndEnvironment(t *testing.T) {
	t.Parallel()

	failed := startOrdinaryScript(t, `echo "boom: real cause" >&2; exit 3`, time.Second)
	<-failed.Exited()
	require.ErrorContains(t, failed.WaitErr(), "exit status 3")
	require.Contains(t, failed.StderrTail(), "boom: real cause")

	script := writeOrdinaryScript(t, `env`)
	process, err := StartOrdinaryProcess(t.Context(), LaunchSpec{
		ExecutablePath:  script,
		AgentDir:        t.TempDir(),
		BaseEnvironment: map[string]string{"PATH": os.Getenv("PATH")},
		Env:             map[string]string{"TEST_EXPLICIT_VALUE": "ok"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = process.Close() })
	var output strings.Builder
	buffer := make([]byte, 1024)
	for {
		n, readErr := process.Stdout().Read(buffer)
		output.Write(buffer[:n])
		if readErr != nil {
			break
		}
	}
	<-process.Exited()
	require.Contains(t, output.String(), "TEST_EXPLICIT_VALUE=ok")
	require.Contains(t, output.String(), "PI_OFFLINE=1")
}

func TestOrdinaryTailBufferBounds(t *testing.T) {
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

func TestOrdinaryProcessKillAfterExit(t *testing.T) {
	t.Parallel()

	process := startOrdinaryScript(t, `exit 0`, time.Second)
	<-process.Exited()
	require.NoError(t, process.Kill())
	require.NoError(t, process.WaitErr())
}
