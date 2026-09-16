package piacp

import (
	"context"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestServePromptRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.NotNil(t, resp.Usage)
	require.Equal(t, 15, resp.Usage.TotalTokens)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return agentText(updates) == "Hello world" })

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
}

func TestCancelledVersionProbeDoesNotPoisonAgent(t *testing.T) {
	agent := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = agent.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := agent.ensureExecutable(ctx)
	require.Error(t, err)
	_, err = agent.ensureExecutable(t.Context())
	require.NoError(t, err, "a request-local cancelled probe permanently poisoned the agent")
}
