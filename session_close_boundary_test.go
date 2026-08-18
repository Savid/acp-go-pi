package piacp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

// vacantStubProcess is a contained process that can enumerate its own tree,
// so a close boundary behind it proves whole-tree vacancy.
type vacantStubProcess struct {
	*stubProcess
	descendants int
	available   bool
}

func (p *vacantStubProcess) ProviderDescendantCount() (int, bool) { return p.descendants, p.available }

// TestSettleCloseBoundary pins the close-fenced settlement order: containment
// completes, owned entities terminalize, the resumable snapshot commits, and
// only a vacant boundary certifies quiescence.
func TestSettleCloseBoundary(t *testing.T) {
	t.Run("vacant boundary certifies quiescence", func(t *testing.T) {
		s, client := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))

		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.NoError(t, s.settleCloseBoundary(t.Context(), proc, nil))
		require.True(t, s.lc.vacancyProven)

		quiescent := false
		for _, notification := range client.notifications {
			envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
			event := anyMap(t, envelope["event"])
			if event["type"] == "quiescence_update" {
				require.Equal(t, closeBoundaryBarrier, event["barrier"])
				quiescent = true
			}
		}
		require.True(t, quiescent, "a vacant close certifies its barrier")

		entries, err := s.agent.sessionStore().Load(t.Context(), SessionKey{SessionID: "lifecycle", Subpath: SessionStoreLifecycleSubpath})
		require.NoError(t, err)
		require.Len(t, entries, 1, "the quiescence fact stands behind a durable snapshot commit")
	})

	t.Run("unenumerated boundary certifies nothing", func(t *testing.T) {
		s, client := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))

		require.NoError(t, s.settleCloseBoundary(t.Context(), newStubProcess(true), nil))
		for _, notification := range client.notifications {
			envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
			require.NotEqual(t, "quiescence_update", anyMap(t, envelope["event"])["type"])
		}
	})

	t.Run("incomplete containment settles nothing", func(t *testing.T) {
		s, _ := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.NoError(t, s.settleCloseBoundary(t.Context(), proc, pi.ErrProcessContainmentIncomplete))
		require.False(t, s.lc.vacancyProven)
	})

	t.Run("terminalization failure stops the order", func(t *testing.T) {
		s, client := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
		client.updateErr = errors.New("delivery")
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.ErrorContains(t, s.settleCloseBoundary(t.Context(), proc, nil), "delivery")
	})

	t.Run("commit failure stops the order", func(t *testing.T) {
		store := newFaultySessionStore()
		s, _ := lifecycleSession(t, true)
		s.agent.options.SessionStore = store
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		store.appendErr = errors.New("durability unavailable")
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.ErrorIs(t, s.settleCloseBoundary(t.Context(), proc, nil), errLifecycleBoundaryCommit)
		require.False(t, s.lc.vacancyProven, "no quiescence fact stands behind an uncommitted snapshot")
	})

	t.Run("mirror failure stops the order", func(t *testing.T) {
		s, _ := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		s.sessionFilePath = t.TempDir()
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.Error(t, s.settleCloseBoundary(t.Context(), proc, nil))
		require.False(t, s.lc.vacancyProven)
	})
}

// TestRecordGenerationLoss pins that a replaced native generation ends its
// incarnation in the durable record: the stream fences and the boundary names
// the loss instead of reconstructing events the generation never delivered.
func TestRecordGenerationLoss(t *testing.T) {
	s, _ := lifecycleSession(t, false)
	require.NoError(t, s.recordGenerationLoss(t.Context()), "no stream means no loss to record")

	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
	require.NoError(t, s.recordGenerationLoss(t.Context()))
	require.True(t, s.lc.fenced)
	require.Empty(t, s.lc.turnID)

	entries, err := s.agent.sessionStore().Load(t.Context(), SessionKey{SessionID: "lifecycle", Subpath: SessionStoreLifecycleSubpath})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	var record lifecycleBoundaryRecord
	require.NoError(t, json.Unmarshal(entries[0], &record))
	require.Equal(t, lifecycleBoundaryVersion, record.Version)
	require.Equal(t, nativeStateRetained, record.NativeState)
	require.NotEmpty(t, record.StreamID)
	require.NotEmpty(t, record.TurnID)
	require.NotZero(t, record.RecordedAtUnixMilli)
}
