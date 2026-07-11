package piacp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAgentConcurrencyAndErrors(t *testing.T) {
	require.NoError(t, validateConcurrencyLimits(ConcurrencyLimits{}))
	require.Error(t, validateConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}))
	require.Error(t, validateConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: -1}))

	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2, MaxConcurrentClientCalls: 1}))
	require.Equal(t, 2, agent.maxActiveSessions())
	require.Equal(t, 1, agent.maxConcurrentClientCalls())
	release, err := agent.acquireClientCall(t.Context())
	require.NoError(t, err)
	_, err = agent.acquireClientCall(t.Context())
	requireInvalidRequest(t, err)
	release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	agent.clientCalls <- struct{}{}
	_, err = agent.acquireClientCall(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-agent.clientCalls

	invalid := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}))
	_, err = invalid.Initialize(t.Context(), defaultInitializeRequest())
	requireInvalidParams(t, err)
	require.Equal(t, defaultMaxActiveSessions, NewAgent().maxActiveSessions())
	require.Equal(t, defaultMaxConcurrentClientCalls, NewAgent().maxConcurrentClientCalls())
	require.Equal(t, 5*time.Second, NewAgent(WithTurnTimeout(5*time.Second)).turnTimeout())
}
