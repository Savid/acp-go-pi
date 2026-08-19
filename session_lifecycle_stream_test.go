package piacp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

func TestLifecycleStreamFinalityActionsAndIncarnationLoss(t *testing.T) {
	s, client := lifecycleSession(t, true)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}))

	one, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	two, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.announceLifecycleAction(t.Context(), one, lifecycle.ActionPermission))
	require.NoError(t, s.announceLifecycleAction(t.Context(), two, lifecycle.ActionElicitation))
	require.NoError(t, s.lifecycleResolveAction(t.Context(), one.actionID, lifecycle.ActionAccepted))
	require.NoError(t, s.lifecycleResolveAction(t.Context(), "unknown", lifecycle.ActionFailed))
	require.NoError(t, s.lifecycleResolveAction(t.Context(), two.actionID, lifecycle.ActionDeclined))
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), "end_turn", lifecycle.OutcomeSuccess))
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), "end_turn", lifecycle.OutcomeSuccess), "terminal settlement is final")
	require.NoError(t, s.lifecycleCertifyBoundary(t.Context(), "proof"))

	// Losing an incarnation while active never fabricates a terminal event in a
	// replacement incarnation. The old sequence is fenced and the new one starts
	// at its own snapshot.
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "lost", ClientNonce: "n"}))
	before := len(client.notifications)
	s.fenceLifecycleStream()
	require.ErrorIs(t, s.emitLifecycleLocked(t.Context(), lifecycle.SnapshotEvent("impossible", lifecycle.QuiescenceFact{})), errLifecycleStreamFenced)
	require.Len(t, client.notifications, before)
	require.NoError(t, s.openLifecycleStream(t.Context(), 2))
	require.Equal(t, uint64(2), s.lc.generation)
	require.Empty(t, s.lc.turnID)
	// Closing the session is the stronger end: the emitter's own validator holds
	// it too, so a post-close envelope fails where it is minted and not only
	// where this struct's flag is consulted.
	s.closeLifecycleSession()
	require.True(t, s.lc.stream.State().Closed)
	require.NoError(t, s.openLifecycleStream(t.Context(), 3))
	require.Equal(t, uint64(2), s.lc.generation)
}

func TestLifecycleStreamFailureBranchesAndRequiredActionMembers(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	_, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, s.lifecycleResolveAction(t.Context(), "none", lifecycle.ActionFailed))
	require.NoError(t, s.lifecycleTerminalizeOwned(t.Context()))
	require.NoError(t, s.lifecycleCertifyBoundary(t.Context(), "none"))

	original := lifecycleRandRead
	lifecycleRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	t.Cleanup(func() { lifecycleRandRead = original })
	require.ErrorContains(t, s.openLifecycleStream(t.Context(), 1), "entropy")
	s.agent.lifecycle = lifecycle.Negotiated{}
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))

	lifecycleRandRead = original
	s.agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, ActivityKinds: []lifecycle.ActivityKind{}}
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	lifecycleRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	require.ErrorContains(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()), "entropy")
	lifecycleRandRead = original
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	action, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	meta := lifecycleActionMeta(action.streamID, action.actionID, action.owner)
	raw, err := json.Marshal(meta)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"actionId"`)
	require.Contains(t, string(raw), `"owner"`)
	require.Contains(t, string(raw), `"type"`)
	require.Contains(t, string(raw), `"id"`)

	client.updateErr = errors.New("delivery")
	require.ErrorContains(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission), "delivery")
	require.True(t, s.lc.fenced)
	s.agent.closed = true
	require.ErrorIs(t, s.deliverLifecycleNotification(t.Context(), map[string]any{}), errAgentClosed)
	s.agent.closed = false
	s.agent.conn = nil
	require.ErrorIs(t, s.deliverLifecycleNotification(t.Context(), map[string]any{}), errACPConnectionNotAttached)
}

// TestLifecycleStreamMintFailureBranches pins that an identity the stream
// cannot mint stops the emission that needed it before anything is published.
func TestLifecycleStreamMintFailureBranches(t *testing.T) {
	original := lifecycleRandRead
	t.Cleanup(func() { lifecycleRandRead = original })

	failOnCall := func(failing int) {
		calls := 0
		lifecycleRandRead = func(data []byte) (int, error) {
			calls++
			if calls == failing {
				return 0, errors.New("entropy")
			}

			return original(data)
		}
	}

	s, _ := lifecycleSession(t, false)
	failOnCall(2)
	require.ErrorContains(t, s.openLifecycleStream(t.Context(), 1), "entropy")
	require.Nil(t, s.lc.stream, "a failed cycle mint opens no stream")

	lifecycleRandRead = original
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))

	failOnCall(2)
	require.ErrorContains(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()), "entropy")

	lifecycleRandRead = original
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))

	failOnCall(1)
	_, _, err := s.prepareLifecycleAction()
	require.ErrorContains(t, err, "entropy")
}

// TestLifecycleStreamEmissionFailureBranches drives every ordered emitter
// against a failing delivery: each failure fences the incarnation instead of
// leaving a gap the host cannot see.
func TestLifecycleStreamEmissionFailureBranches(t *testing.T) {
	delivery := errors.New("delivery")

	t.Run("accept", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()), delivery)
		require.True(t, s.lc.fenced)
	})

	t.Run("settle terminalizes blockers", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled), delivery)
		require.Empty(t, s.lc.blockers)
	})

	t.Run("resolve", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionElicitation))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleResolveAction(t.Context(), action.actionID, lifecycle.ActionAccepted), delivery)
	})

	t.Run("terminalize owned", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleTerminalizeOwned(t.Context()), delivery)
	})
}

// TestLifecycleSettleCancelsUnresolvedBlockers pins that a cycle ending with
// actions still pending cancels them before its terminal idle, so terminal
// never precedes a later real event on one of them.
func TestLifecycleSettleCancelsUnresolvedBlockers(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	action, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))

	events := len(client.notifications)
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled))
	require.Empty(t, s.lc.blockers)
	require.Len(t, client.notifications, events+2, "the cancelled resolution precedes the terminal idle")
}

// TestLifecycleAnnounceAfterTurnEndIsANoOp pins that an action prepared while
// a turn was live announces nothing once that turn has ended: the prepared
// identity was never published, so there is nothing to announce.
func TestLifecycleAnnounceAfterTurnEndIsANoOp(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	action, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), "end_turn", lifecycle.OutcomeSuccess))

	events := len(client.notifications)
	require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
	require.Len(t, client.notifications, events)
}

// TestLifecycleTerminalizeOwned pins the close-boundary projection: an owned
// turn ends cancelled, and a session without one terminalizes nothing.
func TestLifecycleTerminalizeOwned(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	require.NoError(t, s.lifecycleTerminalizeOwned(t.Context()))
	require.Empty(t, s.lc.turnID)

	last := client.notifications[len(client.notifications)-1]
	envelope := anyMap(t, last.Meta[lifecycleMetaKey])
	require.Equal(t, "state_update", anyMap(t, envelope["event"])["type"])

	idle, idleClient := lifecycleSession(t, false)
	require.NoError(t, idle.openLifecycleStream(t.Context(), 1))
	events := len(idleClient.notifications)
	require.NoError(t, idle.lifecycleTerminalizeOwned(t.Context()))
	require.Len(t, idleClient.notifications, events, "a session owning no turn terminalizes nothing")
}

// TestLifecycleEmitRefusedByTheReducer pins that an event the reducer rejects
// is never delivered and fences the incarnation, claiming its sequence as a
// detectable gap rather than publishing an untruthful stream.
func TestLifecycleEmitRefusedByTheReducer(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))

	s.lc.stream = lifecycle.NewStream("unsnapshotted", s.lc.negotiated)
	events := len(client.notifications)
	require.Error(t, s.emitLifecycleLocked(t.Context(), lifecycle.RunningEvent("cycle", "turn")))
	require.True(t, s.lc.fenced)
	require.Len(t, client.notifications, events, "a refused event is never delivered")
}
