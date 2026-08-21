package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

type autonomousIdentityBarrierClient struct {
	*directAgentClient
	t           *testing.T
	session     *agentSession
	cycle       *agentCycle
	runningSeen bool
	failIdle    error
}

func (c *autonomousIdentityBarrierClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	envelope, carries := notification.Meta[lifecycleMetaKey].(map[string]any)
	if carries {
		event, _ := envelope["event"].(map[string]any)
		if event["state"] == string(lifecycle.ForegroundRunning) {
			c.runningSeen = true
			require.Equal(c.t, event["turnId"], c.session.lc.turnID)
			require.Equal(c.t, event["cycleId"], c.session.lc.cycleID)
			require.Equal(c.t, event["turnId"], c.cycle.turnID)
			require.Equal(c.t, event["cycleId"], c.cycle.cycleID)
		}
		if event["state"] == string(lifecycle.ForegroundIdle) && c.failIdle != nil {
			return c.failIdle
		}
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

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

func TestAutonomousCycleIdentityPrecedesRunningAndSurvivesTerminalFailure(t *testing.T) {
	session, base := lifecycleSession(t, false)
	require.NoError(t, session.openLifecycleStream(t.Context(), 7))
	cycle := &agentCycle{generation: 7, state: &promptTurnState{}}
	client := &autonomousIdentityBarrierClient{
		directAgentClient: base,
		t:                 t,
		session:           session,
		cycle:             cycle,
	}
	session.agent.conn = client

	require.NoError(t, session.lifecycleOpenAgentCycle(t.Context(), cycle))
	require.True(t, client.runningSeen)
	wantTurn, wantCycle := cycle.turnID, cycle.cycleID
	require.NotEmpty(t, wantTurn)
	require.NotEmpty(t, wantCycle)

	client.failIdle = errors.New("terminal idle refused")
	err := session.lifecycleSettleAgentCycle(t.Context(), cycle, turnVerdict{
		outcome: lifecycle.OutcomeSuccess, stopReason: lifecycle.StopReasonEndTurn,
	})
	require.ErrorIs(t, err, client.failIdle)
	require.Equal(t, wantTurn, session.lc.turnID)
	require.Equal(t, wantCycle, session.lc.cycleID)
	session.fenceLifecycleStream()
	require.Equal(t, wantTurn, session.lc.lostTurnID)
	require.Equal(t, wantCycle, session.lc.lostCycleID)
}

func TestLifecycleStreamFailureBranchesAndRequiredActionMembers(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	_, ok, err := s.prepareLifecycleAction()
	require.ErrorIs(t, err, errLifecycleStreamFenced)
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
	attachGeneration := func(t *testing.T, s *agentSession) (*stubProcess, *stubPiClient, *int) {
		t.Helper()
		process := newStubProcess(false)
		native := newStubPiClient()
		promptCalls := 0
		native.promptFunc = func(context.Context, string) error {
			promptCalls++

			return nil
		}
		s.proc = process
		s.client = native
		outbox := newTestSessionOutbox(1)
		bindTestRuntime(outbox, process, native, nil, nil, nil)
		s.outbox = outbox
		s.pumpGeneration = 1

		return process, native, &promptCalls
	}
	assertLaterPromptRefused := func(t *testing.T, s *agentSession, promptCalls *int) {
		t.Helper()
		request := TextPromptRequest(s.id, "later", "must not reach pi")
		request.Meta[lifecycleMetaKey] = decodeMeta(t, `{"version":1,"submission":{"submissionId":"later","clientNonce":"nonce"}}`)
		_, err := s.Prompt(t.Context(), request)
		require.Error(t, err)
		require.Zero(t, *promptCalls, "poisoned delivery failure reached PromptWithBoundary")
	}

	t.Run("accept", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()), delivery)
		require.True(t, s.lc.fenced)
	})

	t.Run("settle terminalizes blockers", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		process, _, promptCalls := attachGeneration(t, s)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled), delivery)
		require.Equal(t, 1, process.shutdownCalls)
		require.Equal(t, 1, process.closeCalls)
		assertLaterPromptRefused(t, s, promptCalls)
	})

	t.Run("resolve", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		process, _, promptCalls := attachGeneration(t, s)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionElicitation))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleResolveAction(t.Context(), action.actionID, lifecycle.ActionAccepted), delivery)
		require.Equal(t, 1, process.shutdownCalls)
		require.Equal(t, 1, process.closeCalls)
		assertLaterPromptRefused(t, s, promptCalls)
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

// TestLifecycleAnnounceAfterTurnEndIsRefused pins that an action prepared while
// a turn was live is refused once that turn has ended: the owner it names no
// longer holds the foreground, so the caller answers pi with a native cancel
// rather than announcing a blocker nobody can resolve.
func TestLifecycleAnnounceAfterTurnEndIsRefused(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	action, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), "end_turn", lifecycle.OutcomeSuccess))

	events := len(client.notifications)
	require.ErrorIs(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission), errLifecycleActionUnowned)
	require.Len(t, client.notifications, events)
}

// TestLifecycleAnnounceUnderALaterTurnIsRefused pins the owner fence across two
// turns: an action captured for turn A cannot block turn B's cycle or inherit
// its identity, so a delayed announcement is refused rather than attached.
func TestLifecycleAnnounceUnderALaterTurnIsRefused(t *testing.T) {
	s, client := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

	first, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), "end_turn", lifecycle.OutcomeSuccess))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{
		SubmissionID: "submission-b", ClientNonce: "nonce-b",
	}))

	events := len(client.notifications)
	require.ErrorIs(t, s.announceLifecycleAction(t.Context(), first, lifecycle.ActionPermission), errLifecycleActionUnowned)
	require.Len(t, client.notifications, events, "turn B publishes nothing for turn A's action")
	require.Empty(t, s.lc.blockers, "turn B's cycle is not blocked by turn A's action")
}

// TestLifecycleAnnounceOnALaterIncarnationIsRefused pins the other half of the
// owner fence: identity alone does not authorize an announcement, because a
// fresh incarnation mints its own identity space.
func TestLifecycleAnnounceOnALaterIncarnationIsRefused(t *testing.T) {
	s, _ := lifecycleSession(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))

	action, ok, err := s.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, ok)

	s.lc.generation = 2

	require.ErrorIs(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission), errLifecycleActionUnowned)
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

func TestLifecycleTurnWithoutOriginIsFenced(t *testing.T) {
	for _, settle := range []struct {
		name string
		run  func(*agentSession) error
	}{
		{
			name: "turn settlement",
			run: func(session *agentSession) error {
				return session.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonEndTurn, lifecycle.OutcomeSuccess)
			},
		},
		{name: "close terminalization", run: func(session *agentSession) error {
			return session.lifecycleTerminalizeOwned(t.Context())
		}},
	} {
		t.Run(settle.name, func(t *testing.T) {
			session, client := lifecycleSession(t, false)
			require.NoError(t, session.openLifecycleStream(t.Context(), 1))
			require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
			session.lc.origin = ""
			events := len(client.notifications)

			err := settle.run(session)
			require.ErrorIs(t, err, errLifecycleStreamFenced)
			require.True(t, session.lc.fenced)
			require.Empty(t, session.lc.turnID)
			require.Len(t, client.notifications, events)
		})
	}
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
