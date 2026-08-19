package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
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
