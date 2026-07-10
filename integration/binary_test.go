//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

func TestPiACPAgentBinaryClosedInput(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := agentCommand(t, ctx,
		"-path", fakePiExecutable(t, fakeScenario{}),
		"-home", t.TempDir(),
	)
	cmd.Stdin = strings.NewReader("")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("run acp-go-pi: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	require.Empty(t, stdout.String(), "closed input must produce an empty ACP stdout stream")
}

func TestPiACPAgentBinaryVersionFlag(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := agentCommand(t, ctx, "-version")

	output, err := cmd.Output()
	require.NoError(t, err)
	require.NotEmpty(t, strings.TrimSpace(string(output)))
}

func TestPiACPAgentBinaryFakeConversation(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	agent := startAgentBinary(t, ctx,
		"-path", fakePiExecutable(t, fakeTurnScenario()),
		"-home", t.TempDir(),
	)

	client := &recordingClient{}
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)

	_, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "hello via binary"))
	require.NoError(t, err, "stderr: %s", agent.stderrString())
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, client.text(), fakeDefaultReply)

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}

// TestPiACPAgentBinaryOrphanReapOnCrash is the crash-during-pending-
// permission E2E: SIGKILL the wrapper while the fake harness blocks on a
// permission dialog and require the pi child to die with it
// (parent-death enforcement), leaving no orphan.
func TestPiACPAgentBinaryOrphanReapOnCrash(t *testing.T) {
	requireRunIntegration(t)

	if runtime.GOOS != "linux" && runtime.GOOS != "freebsd" {
		t.Skip("parent-death enforcement is verified via pdeathsig, which needs linux or freebsd")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	scenario := fakeTurnScenario()
	scenario.ToolName = "bash"
	scenario.ToolArgs = map[string]any{"command": "echo orphan-probe"}

	// The wrapper home path appears in the child's --session-dir argv, so it
	// doubles as a unique process-search marker.
	home := t.TempDir()

	agent := startAgentBinary(t, ctx,
		"-path", fakePiExecutable(t, scenario),
		"-home", home,
	)

	client := newBlockingPermissionClient()
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)

	_, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	go func() {
		_, _ = conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId, "trigger the tool"))
	}()

	select {
	case <-client.permissionRequested:
	case <-time.After(60 * time.Second):
		t.Fatalf("no permission request observed; stderr: %s", agent.stderrString())
	}

	require.NotEmpty(t, pgrepChildren(t, home), "expected a running pi child while the permission is pending")

	require.NoError(t, agent.cmd.Process.Kill())

	require.Eventually(t, func() bool {
		return len(pgrepChildren(t, home)) == 0
	}, 15*time.Second, 100*time.Millisecond, "pi child survived the wrapper crash")
}

func pgrepChildren(t *testing.T, marker string) []string {
	t.Helper()

	output, err := exec.Command("pgrep", "-f", marker).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil
		}
		t.Fatalf("pgrep: %v", err)
	}

	lines := strings.Fields(strings.TrimSpace(string(output)))
	pids := make([]string, 0, len(lines))
	pids = append(pids, lines...)

	return pids
}

func TestPiACPAgentBinaryLiveConversation(t *testing.T) {
	requireLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The auth bytes are copied into a private temp file for the -seed-file
	// host path, so the operator's pi home is never handed to the binary.
	authPath := filepath.Join(t.TempDir(), "auth.json")
	require.NoError(t, os.WriteFile(authPath, livePiAuth(t), 0o600))

	args := []string{
		"-path", livePiPath(t),
		"-home", t.TempDir(),
		"-seed-file", "auth.json=" + authPath,
	}
	if model := os.Getenv(envPiModel); model != "" {
		args = append(args, "-model", model)
	}

	agent := startAgentBinary(t, ctx, args...)

	client := &recordingClient{}
	conn := acp.NewClientSideConnection(client, agent.stdin, agent.stdout)

	_, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"Reply with exactly ACP_PI_BINARY_OK and no punctuation."))
	require.NoError(t, err, "stderr: %s", agent.stderrString())
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, client.text(), "ACP_PI_BINARY_OK")

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}
