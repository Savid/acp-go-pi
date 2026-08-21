package piacp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// TestLifecycleIdentityNamesTheIncarnation pins the boundary-record identity:
// with a stream it names the incarnation, turn, and cycle; without one it
// names nothing rather than inventing identities.
func TestLifecycleIdentityNamesTheIncarnation(t *testing.T) {
	s, _ := lifecycleSession(t, false)
	streamID, turnID, cycleID := s.lifecycleIdentity()
	require.Empty(t, streamID)
	require.Empty(t, turnID)
	require.Empty(t, cycleID)

	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	streamID, turnID, cycleID = s.lifecycleIdentity()
	require.NotEmpty(t, streamID)
	require.NotEmpty(t, turnID)
	require.NotEmpty(t, cycleID)
}

func TestSettlementWaitIsBoundedAndCompletedBoundaryFencesLifecycle(t *testing.T) {
	session, _ := lifecycleSession(t, false)
	session.openSettlement()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	err := session.awaitSettlementContext(cancelled)
	require.ErrorContains(t, err, context.Canceled.Error())
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)

	want := errors.New("completed failed boundary")
	done := make(chan struct{})
	close(done)
	session.mu.Lock()
	session.turnFenceStarted = true
	session.turnFenceDone = done
	session.turnFenceErr = want
	session.mu.Unlock()

	_, err = session.settlePrompt(t.Context(), acp.PromptRequest{}, &promptTurnState{}, promptOutcome{}, &atomic.Bool{})
	require.ErrorIs(t, err, want)
	require.True(t, session.lc.fenced)
}
