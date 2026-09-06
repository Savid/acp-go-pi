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
		BaseEnvironment: map[string]string{"PATH": absTestPath("usr", "bin"), "HOME": "/home/native"},
		ExtraPathDirs:   []string{absTestPath("session", "bin")},
		Env: map[string]string{
			"OPENAI_API_KEY": "explicit",
			"https_proxy":    "",
			"PI_OFFLINE":     "0",
		},
	}

	environment := spec.Environ()
	require.IsIncreasing(t, environment)
	require.Contains(t, environment, "PATH="+absTestPath("session", "bin")+separator+absTestPath("usr", "bin"))
	require.Contains(t, environment, "HOME=/home/native")
	require.Contains(t, environment, "OPENAI_API_KEY=explicit")
	require.Contains(t, environment, "https_proxy=")
	require.Contains(t, environment, "PI_OFFLINE=1")
	require.Contains(t, environment, "PI_CODING_AGENT_DIR=/agent")
}

func TestPrependPathDirsDropsUnusableEntries(t *testing.T) {
	t.Parallel()

	separator := string(os.PathListSeparator)
	base := absTestPath("usr", "bin")
	extra := absTestPath("opt", "bin")
	require.Equal(t, base, prependPathDirs(base, nil))
	require.Equal(t, base, prependPathDirs(base, []string{"relative", ""}))
	require.Equal(t, extra+separator+base, prependPathDirs(base, []string{extra}))
	require.Equal(t, base, prependPathDirs(base, []string{absTestPath("a") + separator + absTestPath("b")}))
}

func startOrdinaryChild(t *testing.T, mode string, step time.Duration) *Process {
	t.Helper()

	process, err := StartOrdinaryProcess(t.Context(), LaunchSpec{
		ExecutablePath:      fakeOrdinaryExecutable(t),
		AgentDir:            t.TempDir(),
		BaseEnvironment:     map[string]string{"PATH": os.Getenv("PATH")},
		Env:                 map[string]string{ordinaryChildEnvKey: mode},
		ShutdownStepTimeout: step,
	})
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
	_, err = StartOrdinaryProcess(ctx, LaunchSpec{ExecutablePath: fakeOrdinaryExecutable(t)})
	require.ErrorIs(t, err, context.Canceled)

	_, err = StartOrdinaryProcess(t.Context(), LaunchSpec{ExecutablePath: filepath.Join(t.TempDir(), "missing")})
	require.Error(t, err)
}

func TestOrdinaryProcessStdinEOFAndOutput(t *testing.T) {
	t.Parallel()

	process := startOrdinaryChild(t, ordinaryChildDrainAndReport, 30*time.Second)
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
	require.Equal(t, ordinaryChildReply, strings.TrimSpace(string(data[:n])))
}

func TestOrdinaryProcessShutdownAndKill(t *testing.T) {
	t.Parallel()

	// The step timeout only has to outlast the child's own startup, and the
	// child is a whole Go binary rather than a shell: a second is not a
	// property of the shutdown being proved, and under race instrumentation it
	// is not always enough for one to reach its first read.
	graceful := startOrdinaryChild(t, ordinaryChildDrain, 30*time.Second)
	require.NoError(t, graceful.Shutdown(t.Context()))
	require.NoError(t, graceful.WaitErr())

	forced := startOrdinaryChild(t, ordinaryChildOutliveStdin, 20*time.Millisecond)
	require.NoError(t, forced.Shutdown(t.Context()))
	require.Error(t, forced.WaitErr())

	killed := startOrdinaryChild(t, ordinaryChildOutliveStdin, 30*time.Second)
	require.NoError(t, killed.Kill())
	select {
	case <-killed.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary process did not exit after kill")
	}
}

func TestOrdinaryProcessStderrAndEnvironment(t *testing.T) {
	t.Parallel()

	failed := startOrdinaryChild(t, ordinaryChildFail, 30*time.Second)
	<-failed.Exited()
	require.ErrorContains(t, failed.WaitErr(), "exit status 3")
	require.Contains(t, failed.StderrTail(), ordinaryChildStderr)

	process, err := StartOrdinaryProcess(t.Context(), LaunchSpec{
		ExecutablePath:  fakeOrdinaryExecutable(t),
		AgentDir:        t.TempDir(),
		BaseEnvironment: map[string]string{"PATH": os.Getenv("PATH")},
		Env:             map[string]string{"TEST_EXPLICIT_VALUE": "ok", ordinaryChildEnvKey: ordinaryChildReportEnv},
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

	process := startOrdinaryChild(t, ordinaryChildExit, 30*time.Second)
	<-process.Exited()
	require.NoError(t, process.Kill())
	require.NoError(t, process.WaitErr())
}
