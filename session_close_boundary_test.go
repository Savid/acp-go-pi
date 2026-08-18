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

// TestCloseSettlesOnAFencedOrNeverOpenedIncarnation pins the branch of the
// close-fenced order that runs when there is no live incarnation to speak on. A
// cancel or an incarnation loss already fenced the stream, and a session between
// prompt-contained turns never opened one at all: either way the emission rungs
// are skipped and the boundary emits nothing on the dead stream, because an
// event bearing a fenced streamId is exactly what a conforming reducer refuses
// as stale. The non-emission rungs still run unconditionally, so the durable
// commit lands and the close succeeds.
func TestCloseSettlesOnAFencedOrNeverOpenedIncarnation(t *testing.T) {
	t.Run("fenced by a cancel", func(t *testing.T) {
		s, client := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
		s.fenceLifecycleStream()

		emitted := len(client.notifications)
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.NoError(t, s.settleCloseBoundary(t.Context(), proc, nil))
		require.Len(t, client.notifications, emitted, "a fenced stream carries no close emission")

		requireLifecycleJournalLen(t, s, 1, "the durable commit lands whether or not the stream is live")
	})

	t.Run("never opened", func(t *testing.T) {
		s, client := lifecycleSession(t, true)

		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.NoError(t, s.settleCloseBoundary(t.Context(), proc, nil))
		require.Empty(t, client.notifications, "an incarnation that never opened has no stream to emit on")

		requireLifecycleJournalLen(t, s, 1, "a never-opened incarnation still commits its boundary")
	})

	t.Run("fenced concurrently with the boundary", func(t *testing.T) {
		s, client := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))

		emitted := len(client.notifications)
		captured, _, _ := s.lifecycleIdentity()

		// The fence lands after the boundary captured the generation it is about
		// and before it would have certified, which is the race the unconditional
		// rungs have to survive: the commit still names what was captured.
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		s.fenceLifecycleStream()
		require.NoError(t, s.settleCloseBoundary(t.Context(), proc, nil))
		require.Len(t, client.notifications, emitted)
		require.True(t, s.lc.vacancyProven, "the proof the boundary completed is not lost with the stream")

		entries := loadLifecycleJournal(t, s)
		require.Len(t, entries, 1)

		var record lifecycleBoundaryRecord

		require.NoError(t, json.Unmarshal(entries[0], &record))
		require.Equal(t, captured, record.StreamID)
	})

	t.Run("a capture failure still fails the close", func(t *testing.T) {
		s, _ := lifecycleSession(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		s.fenceLifecycleStream()
		s.sessionFilePath = t.TempDir()

		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.Error(t, s.settleCloseBoundary(t.Context(), proc, nil),
			"a fenced stream skips the emissions, never the durability")
		require.False(t, s.lc.vacancyProven)
	})
}

// TestCloseNeverRewritesALossTerminalizedFailure pins the durable branch's
// precision: the close terminalizes only what is still nonterminal in the store,
// so a turn the incarnation loss already recorded as `failed` stays `failed` and
// is never restated as `cancelled` by the boundary that came after it.
func TestCloseNeverRewritesALossTerminalizedFailure(t *testing.T) {
	s, _ := lifecycleSession(t, true)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))

	_, lostTurn, _ := s.lifecycleIdentity()
	require.NotEmpty(t, lostTurn)

	require.NoError(t, s.commitTurnBoundary(t.Context(), turnVerdict{
		outcome: lifecycle.OutcomeFailed,
		detail:  "the native event stream ended before pi settled",
	}))
	require.NoError(t, s.recordGenerationLoss(t.Context()))

	proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
	require.NoError(t, s.settleCloseBoundary(t.Context(), proc, nil))

	terminal := map[string]string{}

	for _, entry := range loadLifecycleJournal(t, s) {
		var record lifecycleBoundaryRecord

		require.NoError(t, json.Unmarshal(entry, &record))

		if record.TurnID != "" && record.Outcome != "" {
			terminal[record.TurnID] = record.Outcome
		}
	}

	require.Equal(t, map[string]string{lostTurn: string(lifecycle.OutcomeFailed)}, terminal)
}

func loadLifecycleJournal(t *testing.T, s *agentSession) []SessionStoreEntry {
	t.Helper()

	entries, err := s.agent.sessionStore().Load(t.Context(), SessionKey{
		SessionID: string(s.id), Subpath: SessionStoreLifecycleSubpath,
	})
	require.NoError(t, err)

	return entries
}

func requireLifecycleJournalLen(t *testing.T, s *agentSession, want int, msgAndArgs ...any) {
	t.Helper()
	require.Len(t, loadLifecycleJournal(t, s), want, msgAndArgs...)
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
