//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

const liveTurnTimeout = 3 * time.Minute

// livePiOptions builds the token-spending launch posture: the real pi
// binary against a hermetic temp scratch parent, with copied portable auth
// seeded into each session's agent directory.
func livePiOptions(t *testing.T) []piacp.Option {
	t.Helper()

	options := []piacp.Option{
		piacp.WithExecutablePath(livePiPath(t)),
		piacp.WithScratchDir(integrationScratchDir(t)),
		livePiAuthSeed(t),
	}

	if model := os.Getenv(envPiModel); model != "" {
		options = append(options, piacp.WithDefaultModel(model))
	}

	return options
}

func TestPiACPLivePromptTurn(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), liveTurnTimeout)
	defer cancel()

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client, livePiOptions(t)...)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t)))
	require.NoError(t, err)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Reply with exactly ACP_PI_LIVE_OK and no punctuation."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, client.text(), "ACP_PI_LIVE_OK")

	_, err = conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}

func TestPiACPLiveCancel(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), liveTurnTimeout)
	defer cancel()

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client, livePiOptions(t)...)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t)))
	require.NoError(t, err)

	promptDone := make(chan acp.PromptResponse, 1)
	promptFailed := make(chan error, 1)
	go func() {
		resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
			"test-turn",
			"Count from 1 to 500, one number per line, without stopping."))
		if err != nil {
			promptFailed <- err

			return
		}
		promptDone <- resp
	}()

	require.Eventually(t, func() bool { return client.text() != "" },
		liveTurnTimeout, 50*time.Millisecond, "no streamed output before cancel")

	require.NoError(t, conn.Cancel(ctx, piacp.CancelRequest(session.SessionId, "test-turn")))

	select {
	case resp := <-promptDone:
		require.Equal(t, acp.StopReasonCancelled, resp.StopReason)
	case err := <-promptFailed:
		t.Fatalf("cancelled live prompt failed: %v", err)
	case <-time.After(liveTurnTimeout):
		t.Fatal("live prompt did not return after cancel")
	}

	// The aborted turn must not poison the session.
	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Reply with exactly ACP_PI_AFTER_CANCEL and no punctuation."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestPiACPLivePermissionGate(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), liveTurnTimeout)
	defer cancel()

	client := &recordingClient{permissionChoice: permissionChoiceAllow}
	conn := connectAgentForTest(t, ctx, client, livePiOptions(t)...)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t)))
	require.NoError(t, err)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Use the bash tool to run exactly `echo ACP_PI_TOOL_OK` and then tell me its output."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.GreaterOrEqual(t, client.permissionCount(), 1,
		"the default ask mode must raise a permission request for the tool call")
	require.Contains(t, client.text(), "ACP_PI_TOOL_OK")
}

func TestPiACPLivePermissionDeny(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), liveTurnTimeout)
	defer cancel()

	client := &recordingClient{permissionChoice: permissionChoiceDeny}
	conn := connectAgentForTest(t, ctx, client, livePiOptions(t)...)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t)))
	require.NoError(t, err)

	// A denied tool call fails closed natively; the turn itself still
	// finishes with a normal stop.
	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Use the bash tool to run exactly `echo ACP_PI_DENIED` once. If the tool call fails, "+
			"reply with exactly ACP_PI_TOOL_BLOCKED and stop."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.GreaterOrEqual(t, client.permissionCount(), 1)
}

func TestPiACPLiveRawEventsOptIn(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), liveTurnTimeout)
	defer cancel()

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client, livePiOptions(t)...)

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(integrationWorkspaceDir(t),
		piacp.WithSessionRawEvents(true)))
	require.NoError(t, err)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Reply with exactly ACP_PI_RAW_OK and no punctuation."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Positive(t, client.rawEventCount(), "raw-event opt-in must forward native event lines")
}

func TestPiACPLiveForkSession(t *testing.T) {
	requireLiveTokens(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*liveTurnTimeout)
	defer cancel()

	client := &recordingClient{}
	conn := connectAgentForTest(t, ctx, client, livePiOptions(t)...)

	cwd := integrationWorkspaceDir(t)
	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd))
	require.NoError(t, err)

	// pi rejects cloning an empty session, so fork of a fresh session must
	// fail with a structured error rather than a native crash.
	_, err = piacp.CallForkSession(ctx, conn, piacp.ForkSessionRequest(session.SessionId, cwd))
	require.Error(t, err)

	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Remember the code word MANGOSTEEN. Reply with exactly ACP_PI_SEEDED."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	fork, err := piacp.CallForkSession(ctx, conn, piacp.ForkSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.NotEmpty(t, fork.SessionId)
	require.NotEqual(t, session.SessionId, fork.SessionId, "fork mints a new session id")

	resp, err = conn.Prompt(ctx, piacp.TextPromptRequest(fork.SessionId,
		"test-turn",
		"Reply with exactly the code word I asked you to remember, and nothing else."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, client.text(), "MANGOSTEEN", "the fork must carry the parent's history")

	// Both sessions stay independently usable.
	resp, err = conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"test-turn",
		"Reply with exactly ACP_PI_PARENT_OK."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}
