package piacp

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublishSessionOpen pins the establishing snapshot: it is emitted exactly
// once, and a lifecycle stream that cannot open is fenced and recorded rather
// than continued from a first event that never landed.
func TestPublishSessionOpen(t *testing.T) {
	t.Run("publishes exactly once", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		s.publishSessionOpen(t.Context())
		events := len(client.notifications)
		require.NotZero(t, events)
		s.publishSessionOpen(t.Context())
		require.Len(t, client.notifications, events)
	})

	t.Run("stream failure fences the incarnation", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		client.updateErr = errors.New("delivery")
		s.publishSessionOpen(t.Context())
		require.True(t, s.lc.fenced)
	})
}
