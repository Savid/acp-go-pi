//go:build integration && (linux || darwin || freebsd || openbsd)

package integration

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

func TestAgentFakeCancelKillsBlockedNativeToolTree(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	pidFile := dir + "/blocked.pid"
	oldOutput := dir + "/old-output"

	scenario := fakeTurnScenario()
	scenario.PromptBehavior = fakeBehaviorBlockedTool
	scenario.BlockedToolPIDFile = pidFile
	scenario.BlockedToolOutput = oldOutput
	scenario.BlockedToolDelayMs = 2000

	client := &recordingClient{}
	conn := connectFakeAgentForTest(t, ctx, client, scenario)
	sessionID := newFakeSession(t, ctx, conn)

	promptDone := make(chan acp.PromptResponse, 1)
	promptErr := make(chan error, 1)
	go func() {
		resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "blocked-turn", "run blocked tool"))
		if err != nil {
			promptErr <- err

			return
		}

		promptDone <- resp
	}()

	var descendantPID int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(pidFile) // #nosec G304 -- private test path.
		if err != nil {
			return false
		}

		descendantPID, err = strconv.Atoi(strings.TrimSpace(string(data)))

		return err == nil && processAlive(descendantPID)
	}, 10*time.Second, 10*time.Millisecond, "blocked native descendant never started")

	cancelStarted := time.Now()
	require.NoError(t, conn.Cancel(ctx, piacp.CancelRequest(sessionID, "blocked-turn")))
	require.Less(t, time.Since(cancelStarted), 3*time.Second)

	select {
	case resp := <-promptDone:
		require.Equal(t, acp.StopReasonCancelled, resp.StopReason)
	case err := <-promptErr:
		t.Fatalf("cancelled prompt failed instead of stopping: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("blocked prompt did not return after cancellation escalation")
	}

	require.Eventually(t, func() bool { return !processAlive(descendantPID) },
		3*time.Second, 10*time.Millisecond, "blocked native descendant survived cancellation")
	require.Never(t, func() bool {
		_, err := os.Stat(oldOutput)

		return err == nil
	}, 2200*time.Millisecond, 20*time.Millisecond, "cancelled native descendant reached its delayed side effect")

	response, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "replacement-turn", "replacement"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Contains(t, client.text(), fakeDefaultReply)

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}
