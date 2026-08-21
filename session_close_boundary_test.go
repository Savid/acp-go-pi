package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
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
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
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

func TestCloseBoundaryExactTerminalOwnershipEdges(t *testing.T) {
	want := errors.New("close terminal delivery")

	t.Run("quiescence delivery failure", func(t *testing.T) {
		session, client := lifecycleSession(t, true)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		client.updateErr = want
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.ErrorIs(t, session.settleCloseBoundary(t.Context(), proc, nil), want)
	})

	t.Run("captured blockers are sorted", func(t *testing.T) {
		session, _ := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
		session.lc.blockers = map[string]struct{}{"z": {}, "a": {}}
		terminal := session.prepareCloseLifecycleTerminal()
		require.Equal(t, []string{"a", "z"}, terminal.blockers)
	})

	t.Run("ownership mutation fences publication", func(t *testing.T) {
		session, _ := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		terminal := session.prepareCloseLifecycleTerminal()
		session.lc.generation++
		err := session.publishCloseLifecycleTerminal(t.Context(), terminal)
		require.ErrorContains(t, err, "ownership changed")
		require.True(t, session.lc.fenced)
	})

	t.Run("blocker cancellation delivery failure", func(t *testing.T) {
		session, client := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := session.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, session.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		terminal := session.prepareCloseLifecycleTerminal()
		client.updateErr = want
		require.ErrorIs(t, session.publishCloseLifecycleTerminal(t.Context(), terminal), want)
		require.Contains(t, session.lc.blockers, action.actionID)
	})

	t.Run("idle delivery fails after exact blockers are removed", func(t *testing.T) {
		session, base := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := session.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, session.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		terminal := session.prepareCloseLifecycleTerminal()
		session.agent.conn = &lifecycleIdleFailureClient{directAgentClient: base, want: want}
		require.ErrorIs(t, session.publishCloseLifecycleTerminal(t.Context(), terminal), want)
		require.Empty(t, session.lc.blockers)
		require.NotEmpty(t, session.lc.turnID)
	})

	require.False(t, sameLifecycleBlockers(map[string]struct{}{"a": {}}, nil))
	require.False(t, sameLifecycleBlockers(map[string]struct{}{"a": {}}, []string{"b"}))
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
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
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

// TestCloseCertifiesNothingAfterPersistenceIsFenced pins the third state of the
// close-fenced order, the one a delete-then-close reaches: both commits are
// reached and both silently no-op, because the delete already fenced this
// session's persistence. A quiescence fact asserts the store holds everything a
// later session/load or session/resume needs, so a boundary that wrote no rows
// may state nothing at all. The delete's fence ends the incarnation too, so the
// close settles in silence and still succeeds.
func TestCloseCertifiesNothingAfterPersistenceIsFenced(t *testing.T) {
	s, client := lifecycleSession(t, true)
	s.sessionFilePath = filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(s.sessionFilePath, []byte("{\"one\":1}\n"), 0o600))

	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

	// The delete path: persistence is fenced before the close runs its boundary.
	s.fencePersistence()

	emitted := len(client.notifications)
	proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
	require.NoError(t, s.settleCloseBoundary(t.Context(), proc, nil))

	require.Len(t, client.notifications, emitted,
		"a boundary whose commits can write nothing terminalizes nothing and certifies nothing")

	requireLifecycleJournalLen(t, s, 0, "no boundary record stands behind the close")

	mirrored, err := s.agent.sessionStore().Load(t.Context(), SessionKey{SessionID: string(s.id)})
	require.NoError(t, err)
	require.Empty(t, mirrored, "no native row is recreated after the tombstone")
}

// TestCloseNeverRewritesALossTerminalizedFailure pins the durable branch's
// precision: the close terminalizes only what is still nonterminal in the store,
// so a turn the incarnation loss already recorded as `failed` stays `failed` and
// is never restated as `cancelled` by the boundary that came after it.
func TestCloseNeverRewritesALossTerminalizedFailure(t *testing.T) {
	s, _ := lifecycleSession(t, true)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

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
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
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

// TestAgentCloseOwesTheSameDurableRungAsAWireClose pins the ladder's durable
// rung on the embedded shutdown path. Agent.Close applies the ladder identically
// to a wire session/close, and that includes the commit the boundary owes:
// embedded shutdown must not drop state a wire close would have committed just
// because the host tore the whole agent down instead of one session.
func TestAgentCloseOwesTheSameDurableRungAsAWireClose(t *testing.T) {
	newBoundary := func(t *testing.T) (*Agent, *InMemorySessionStore, *directAgentClient) {
		t.Helper()

		store := NewInMemorySessionStore()
		client := newDirectAgentClient()
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
		agent.conn = client
		agent.lifecycle = lifecycle.Negotiated{
			Versions:                []int{1},
			UpdatesOutsidePrompt:    true,
			ActivityKinds:           []lifecycle.ActivityKind{},
			AuthoritativeQuiescence: true,
			QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		}

		root := t.TempDir()
		sessionFile := filepath.Join(root, "session.jsonl")
		require.NoError(t, os.WriteFile(sessionFile, []byte("{\"row\":1}\n"), 0o600))

		session := &agentSession{
			agent:           agent,
			id:              "embedded",
			proc:            &vacantStubProcess{stubProcess: newStubProcess(true), available: true},
			sessionRoot:     root,
			sessionFilePath: sessionFile,
		}
		attachTestNativeBoundary(session)
		agent.sessions["embedded"] = session
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))

		return agent, store, client
	}

	requireDurableRungs := func(t *testing.T, store *InMemorySessionStore) {
		t.Helper()

		rows, err := store.Load(context.Background(), SessionKey{SessionID: "embedded"})
		require.NoError(t, err)
		require.Len(t, rows, 1, "the mirror commit ran")

		journal, err := store.Load(context.Background(),
			SessionKey{SessionID: "embedded", Subpath: SessionStoreLifecycleSubpath})
		require.NoError(t, err)
		require.Len(t, journal, 1, "the boundary record commit ran")
	}

	quiescenceEmitted := func(t *testing.T, client *directAgentClient) bool {
		t.Helper()

		for _, notification := range client.notifications {
			envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
			if anyMap(t, envelope["event"])["type"] == "quiescence_update" {
				return true
			}
		}

		return false
	}

	// Embedded shutdown detaches the connection, so no incarnation survives to
	// carry an event. The emission rungs are skipped on the fenced stream and
	// the durable rungs still run, with the chain of preconditions intact
	// across the skipped ones.
	t.Run("Agent.Close", func(t *testing.T) {
		agent, store, client := newBoundary(t)
		require.NoError(t, agent.Close())
		requireDurableRungs(t, store)
		require.False(t, quiescenceEmitted(t, client),
			"a fenced incarnation carries no quiescence fact")
	})

	t.Run("session/close", func(t *testing.T) {
		agent, store, client := newBoundary(t)
		t.Cleanup(func() { _ = agent.Close() })

		_, err := agent.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: "embedded"})
		require.NoError(t, err)
		requireDurableRungs(t, store)
		require.True(t, quiescenceEmitted(t, client), "a live incarnation certifies its barrier")
	})

	// A store that refuses the owed commit fails the embedded shutdown rather
	// than letting it drop the state silently.
	t.Run("Agent.Close fails closed on a refused commit", func(t *testing.T) {
		agent, _, _ := newBoundary(t)
		refused := errors.New("durability unavailable")
		faulty := newFaultySessionStore()
		faulty.appendErr = refused
		agent.options.SessionStore = faulty

		require.ErrorIs(t, agent.Close(), refused,
			"embedded shutdown reports the store's refusal rather than dropping the state")
	})
}
