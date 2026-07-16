//go:build integration && (linux || darwin || freebsd || openbsd)

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
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
	scenario.AbortAcksImmediately = true

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
	require.False(t, processAlive(descendantPID), "cancelled prompt returned before the native descendant was dead: %s", processDebug(descendantPID))

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

func TestAgentFakeTimeoutKillsAcknowledgedNativeToolTreeBeforeReturning(t *testing.T) {
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
	scenario.AbortAcksImmediately = true

	conn := connectFakeAgentForTest(
		t, ctx, &recordingClient{}, scenario, piacp.WithTurnTimeout(250*time.Millisecond),
	)
	sessionID := newFakeSession(t, ctx, conn)

	promptErr := make(chan error, 1)
	go func() {
		_, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "timeout-turn", "run blocked tool"))
		promptErr <- err
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

	err := <-promptErr
	requireTurnFailure(t, err, "timeout")
	require.False(t, processAlive(descendantPID), "timeout returned before the native descendant was dead")
	require.Never(t, func() bool {
		_, statErr := os.Stat(oldOutput)

		return statErr == nil
	}, 2200*time.Millisecond, 20*time.Millisecond, "timed-out native descendant reached its delayed side effect")

	response, err := conn.Prompt(ctx, piacp.TextPromptRequest(sessionID, "replacement-turn", "replacement"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
}

func processAlive(pid int) bool {
	if runtime.GOOS == "linux" {
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) // #nosec G304 -- pid came from the private test child.
		if err == nil {
			end := strings.LastIndexByte(string(stat), ')')
			if end >= 0 && len(stat) > end+2 && stat[end+2] == 'Z' {
				return false
			}
		}
	}

	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}

func processDebug(pid int) string {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) // #nosec G304 -- pid came from the private test child.
	if err != nil {
		return err.Error()
	}

	return string(stat)
}
