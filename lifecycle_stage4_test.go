package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

type lifecycleActionWireWriter struct {
	lines          chan []byte
	blockMethod    string
	requestWrite   chan struct{}
	releaseRequest chan struct{}
}

func (w *lifecycleActionWireWriter) Write(data []byte) (int, error) {
	w.lines <- append([]byte(nil), data...)

	var message lifecycleActionWireMessage
	if json.Unmarshal(data, &message) == nil && message.Method == w.blockMethod {
		close(w.requestWrite)
		<-w.releaseRequest
	}

	return len(data), nil
}

type lifecycleActionWireMessage struct {
	ID     *json.RawMessage `json:"id"`
	Method string           `json:"method"`
	Params json.RawMessage  `json:"params"`
}

func nextLifecycleActionWireMessage(t *testing.T, lines <-chan []byte) lifecycleActionWireMessage {
	t.Helper()

	select {
	case line := <-lines:
		var message lifecycleActionWireMessage
		require.NoError(t, json.Unmarshal(line, &message))

		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outbound ACP message")

		return lifecycleActionWireMessage{}
	}
}

func stage4Session(t *testing.T, authoritative bool) (*agentSession, *directAgentClient) {
	t.Helper()
	client := newDirectAgentClient()
	agent := NewAgent(testContainmentOption())
	agent.conn = client
	agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	if authoritative {
		agent.lifecycle.AuthoritativeQuiescence = true
		agent.lifecycle.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return &agentSession{agent: agent, id: "stage4"}, client
}

func TestLifecycleStreamFinalityActionsAndIncarnationLoss(t *testing.T) {
	s, client := stage4Session(t, true)
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
	s.closeLifecycleSession()
	require.NoError(t, s.openLifecycleStream(t.Context(), 3))
	require.Equal(t, uint64(2), s.lc.generation)
}

func TestLifecycleStreamFailureBranchesAndRequiredActionMembers(t *testing.T) {
	s, client := stage4Session(t, false)
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
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
	require.ErrorContains(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}), "entropy")
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

func TestLifecycleNegotiationPrecisionAndReservedRouting(t *testing.T) {
	agent := NewAgent(testContainmentOption())
	require.Equal(t, lifecycle.Negotiated{}, (*Agent)(nil).lifecycleNegotiated())

	answer, err := agent.negotiateLifecycle(map[string]any{lifecycleMetaKey: map[string]any{"versions": []any{json.Number("1")}}})
	require.NoError(t, err)
	require.Contains(t, answer, lifecycleMetaKey)

	// Values that float64 would round onto one are not equal to protocol 1.
	_, err = agent.negotiateLifecycle(map[string]any{lifecycleMetaKey: map[string]any{"versions": []any{json.Number("1.0000000000000000001")}}})
	require.Error(t, err)

	require.NoError(t, refuseLifecycleMeta(nil))
	require.Error(t, refuseLifecycleMeta(map[string]any{lifecycleMetaKey: map[string]any{}}))
	require.NoError(t, refuseLifecycleRawMeta(json.RawMessage(`{`)))
	require.NoError(t, refuseLifecycleRawMeta(json.RawMessage(`{"_meta":{}}`)))
	require.Error(t, refuseLifecycleRawMeta(json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))
	require.NoError(t, refuseLifecycleExtensionMeta("unknown/method", json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))
	require.Error(t, refuseLifecycleExtensionMeta(ForkSessionMethod, json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))

	_, err = agent.lifecyclePromptCorrelation(map[string]any{lifecycleMetaKey: map[string]any{"version": 1.0}})
	require.Error(t, err)
	require.Equal(t, acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}, lifecycleCarrier())
	require.Equal(t, map[string]any{lifecycleMetaKey: map[string]any{"x": true}}, lifecycleNotificationMeta(map[string]any{"x": true}))
}

func TestActionAnswerClassificationAndPlainRequest(t *testing.T) {
	errBoom := errors.New("boom")
	require.Equal(t, lifecycle.ActionFailed, permissionActionState(acp.RequestPermissionResponse{}, errBoom))
	require.Equal(t, lifecycle.ActionCancelled, permissionActionState(acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil))
	require.Equal(t, lifecycle.ActionCancelled, permissionActionState(acp.RequestPermissionResponse{}, nil))
	require.Equal(t, lifecycle.ActionAccepted, permissionActionState(acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow)}, nil))
	require.Equal(t, lifecycle.ActionDeclined, permissionActionState(acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected("deny_once")}, nil))
	require.Equal(t, lifecycle.ActionFailed, elicitationActionState(acp.UnstableCreateElicitationResponse{}, errBoom))
	require.Equal(t, lifecycle.ActionAccepted, elicitationActionState(acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{}}, nil))
	require.Equal(t, lifecycle.ActionDeclined, elicitationActionState(acp.UnstableCreateElicitationResponse{Decline: &acp.UnstableCreateElicitationDecline{}}, nil))
	require.Equal(t, lifecycle.ActionCancelled, elicitationActionState(acp.UnstableCreateElicitationResponse{}, nil))

	s := &agentSession{}
	value, err := announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
		func(_ context.Context, meta map[string]any) (string, error) {
			require.Nil(t, meta)

			return "plain", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.NoError(t, err)
	require.Equal(t, "plain", value)
}

// TestReservedLifecycleMetaRefusedOnEverySurface pins the family-literal rule:
// a surface that never carries the extension refuses the key rather than
// ignoring it, whatever else the request named.
func TestReservedLifecycleMetaRefusedOnEverySurface(t *testing.T) {
	reserved := map[string]any{lifecycleMetaKey: map[string]any{}}
	agent := NewAgent(testContainmentOption())

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{
		Meta: map[string]any{lifecycleMetaKey: map[string]any{"versions": []any{json.Number("1.0000000000000000001")}}},
	})
	require.Error(t, err, "a malformed offer fails initialize")

	_, err = agent.Authenticate(t.Context(), acp.AuthenticateRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.Logout(t.Context(), acp.LogoutRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`))
	require.Error(t, err)
	_, err = agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{Meta: reserved},
	})
	require.Error(t, err)
	_, err = agent.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = piOptionsFromMeta(reserved)
	require.Error(t, err)
	require.Error(t, (&agentSession{agent: agent}).cancelRouted(t.Context(), reserved))
}

// TestProvenLifecycleFactsFollowTheContainmentBoundary pins that the
// authoritative quiescence advertisement is resolved from the active
// containment configuration, never asserted where the boundary cannot prove
// whole-tree vacancy.
func TestProvenLifecycleFactsFollowTheContainmentBoundary(t *testing.T) {
	shared := NewAgent(testContainmentOption()).provenLifecycleFacts()
	require.True(t, shared.UpdatesOutsidePrompt)
	require.False(t, shared.AuthoritativeQuiescence)
	require.Empty(t, shared.QuiescenceSource)
	require.Empty(t, shared.ActivityKinds)

	previous := agentRuntimePlatform
	agentRuntimePlatform = linuxPlatform
	t.Cleanup(func() { agentRuntimePlatform = previous })

	authoritative := NewAgent(WithProcessIsolation(*policyForContainmentModeTest())).provenLifecycleFacts()
	require.True(t, authoritative.AuthoritativeQuiescence)
	require.Equal(t, lifecycle.ProofClassProcessContainment, authoritative.QuiescenceSource)
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

	s, _ := stage4Session(t, false)
	failOnCall(2)
	require.ErrorContains(t, s.openLifecycleStream(t.Context(), 1), "entropy")
	require.Nil(t, s.lc.stream, "a failed cycle mint opens no stream")

	lifecycleRandRead = original
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))

	failOnCall(2)
	require.ErrorContains(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}), "entropy")

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
		s, client := stage4Session(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}), delivery)
		require.True(t, s.lc.fenced)
	})

	t.Run("settle terminalizes blockers", func(t *testing.T) {
		s, client := stage4Session(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled), delivery)
		require.Empty(t, s.lc.blockers)
	})

	t.Run("resolve", func(t *testing.T) {
		s, client := stage4Session(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
		action, ok, err := s.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, s.announceLifecycleAction(t.Context(), action, lifecycle.ActionElicitation))
		client.updateErr = delivery
		require.ErrorIs(t, s.lifecycleResolveAction(t.Context(), action.actionID, lifecycle.ActionAccepted), delivery)
	})

	t.Run("terminalize owned", func(t *testing.T) {
		s, client := stage4Session(t, false)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
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
	s, client := stage4Session(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
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
	s, client := stage4Session(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
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
	s, client := stage4Session(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
	require.NoError(t, s.lifecycleTerminalizeOwned(t.Context()))
	require.Empty(t, s.lc.turnID)

	last := client.notifications[len(client.notifications)-1]
	envelope := anyMap(t, last.Meta[lifecycleMetaKey])
	require.Equal(t, "state_update", anyMap(t, envelope["event"])["type"])

	idle, idleClient := stage4Session(t, false)
	require.NoError(t, idle.openLifecycleStream(t.Context(), 1))
	events := len(idleClient.notifications)
	require.NoError(t, idle.lifecycleTerminalizeOwned(t.Context()))
	require.Len(t, idleClient.notifications, events, "a session owning no turn terminalizes nothing")
}

// TestLifecycleEmitRefusedByTheReducer pins that an event the reducer rejects
// is never delivered and fences the incarnation, claiming its sequence as a
// detectable gap rather than publishing an untruthful stream.
func TestLifecycleEmitRefusedByTheReducer(t *testing.T) {
	s, client := stage4Session(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))

	s.lc.stream = lifecycle.NewStream("unsnapshotted", s.lc.negotiated)
	events := len(client.notifications)
	require.Error(t, s.emitLifecycleLocked(t.Context(), lifecycle.RunningEvent("cycle", "turn")))
	require.True(t, s.lc.fenced)
	require.Len(t, client.notifications, events, "a refused event is never delivered")
}

// TestLifecycleIdentityNamesTheIncarnation pins the boundary-record identity:
// with a stream it names the incarnation, turn, and cycle; without one it
// names nothing rather than inventing identities.
func TestLifecycleIdentityNamesTheIncarnation(t *testing.T) {
	s, _ := stage4Session(t, false)
	streamID, turnID, cycleID := s.lifecycleIdentity()
	require.Empty(t, streamID)
	require.Empty(t, turnID)
	require.Empty(t, cycleID)

	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
	streamID, turnID, cycleID = s.lifecycleIdentity()
	require.NotEmpty(t, streamID)
	require.NotEmpty(t, turnID)
	require.NotEmpty(t, cycleID)
}

// TestAnnouncedActionRequestLifecycle pins the ordered action contract: the
// request carries the action identity on the wire, the announcement follows
// it, and the resolution lands exactly once with the answer's classification.
func TestAnnouncedActionRequestLifecycle(t *testing.T) {
	s, client := stage4Session(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))

	var sentMeta map[string]any
	value, err := announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
		func(ctx context.Context, meta map[string]any) (string, error) {
			sentMeta = meta
			acknowledgeActionRequestWrite(ctx, nil)

			return "answer", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.NoError(t, err)
	require.Equal(t, "answer", value)

	action := anyMap(t, anyMap(t, sentMeta[lifecycleMetaKey])["action"])
	actionID, _ := action["actionId"].(string)
	require.NotEmpty(t, actionID)
	require.Equal(t, s.lc.stream.ID(), anyMap(t, sentMeta[lifecycleMetaKey])["streamId"])

	var announced, resolved bool
	for _, notification := range client.notifications {
		envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
		event := anyMap(t, envelope["event"])
		if event["type"] != "action_update" {
			continue
		}

		update := anyMap(t, event["action"])
		if update["actionId"] != actionID {
			continue
		}

		switch update["state"] {
		case "pending":
			announced = true
			require.Equal(t, "permission", update["kind"])
		case "accepted":
			resolved = true
		}
	}
	require.True(t, announced, "the announced action names the request's identity")
	require.True(t, resolved, "the resolved action names the request's identity")

	sentErr := errors.New("send")
	notificationsBeforeFailure := len(client.notifications)
	_, err = announcedActionRequest(t.Context(), s, lifecycle.ActionElicitation,
		func(context.Context, map[string]any) (string, error) { return "", sentErr },
		func(_ string, err error) lifecycle.ActionState {
			require.ErrorIs(t, err, sentErr)

			return lifecycle.ActionFailed
		})
	require.ErrorIs(t, err, sentErr)
	require.Len(t, client.notifications, notificationsBeforeFailure,
		"a request that did not cross the write barrier announced an action")

	client.updateErr = errors.New("announce delivery")
	_, err = announcedActionRequest(t.Context(), s, lifecycle.ActionPermission,
		func(ctx context.Context, _ map[string]any) (string, error) {
			acknowledgeActionRequestWrite(ctx, nil)

			return "unpublished", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.ErrorContains(t, err, "announce delivery")

	fenced, _ := stage4Session(t, false)
	require.NoError(t, fenced.openLifecycleStream(t.Context(), 1))
	require.NoError(t, fenced.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
	original := lifecycleRandRead
	lifecycleRandRead = func([]byte) (int, error) { return 0, errors.New("entropy") }
	t.Cleanup(func() { lifecycleRandRead = original })
	_, err = announcedActionRequest(t.Context(), fenced, lifecycle.ActionPermission,
		func(context.Context, map[string]any) (string, error) { return "never sent", nil },
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.ErrorContains(t, err, "entropy")
}

// TestAnnouncedPermissionCrossesTheRequestWriteBarrier pins the transport
// boundary itself: the pending response is registered and the request write has
// completed before the lifecycle notification can announce its action id.
func TestAnnouncedPermissionCrossesTheRequestWriteBarrier(t *testing.T) {
	input, respond := io.Pipe()
	wire := &lifecycleActionWireWriter{
		lines:          make(chan []byte, 16),
		blockMethod:    acp.ClientMethodSessionRequestPermission,
		requestWrite:   make(chan struct{}),
		releaseRequest: make(chan struct{}),
	}
	agent := NewAgent(testContainmentOption())
	agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, ActivityKinds: []lifecycle.ActivityKind{}}
	connection := newLocalAgentConnection(agent, wire, input)
	agent.setConnection(connection)
	t.Cleanup(func() {
		select {
		case <-wire.releaseRequest:
		default:
			close(wire.releaseRequest)
		}
		require.NoError(t, respond.Close())

		select {
		case <-connection.Done():
		case <-time.After(time.Second):
			t.Error("ACP connection did not stop")
		}
	})

	session := &agentSession{agent: agent, id: "session"}
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}))

	for range 3 {
		nextLifecycleActionWireMessage(t, wire.lines)
	}
	session.lcMu.Lock()
	sequenceBeforeRequest := session.lc.stream.Sequence()
	session.lcMu.Unlock()

	type result struct {
		response acp.RequestPermissionResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := session.requestAnnouncedPermission(t.Context(), connection, acp.RequestPermissionRequest{
			SessionId: session.id,
		})
		done <- result{response: response, err: err}
	}()

	request := nextLifecycleActionWireMessage(t, wire.lines)
	require.Equal(t, acp.ClientMethodSessionRequestPermission, request.Method)
	require.NotNil(t, request.ID)
	<-wire.requestWrite

	session.lcMu.Lock()
	require.Equal(t, sequenceBeforeRequest, session.lc.stream.Sequence(),
		"action was claimed before the request write completed")
	session.lcMu.Unlock()

	// The SDK response registration precedes its transport write: route a
	// response while that write is still blocked, then let the write complete.
	encodedResult, err := json.Marshal(acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	})
	require.NoError(t, err)
	response, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      *request.ID,
		"result":  json.RawMessage(encodedResult),
	})
	require.NoError(t, err)
	_, err = respond.Write(append(response, '\n'))
	require.NoError(t, err)
	close(wire.releaseRequest)

	announcement := nextLifecycleActionWireMessage(t, wire.lines)
	require.Equal(t, acp.ClientMethodSessionUpdate, announcement.Method)
	var notification acp.SessionNotification
	require.NoError(t, json.Unmarshal(announcement.Params, &notification))
	envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
	event := anyMap(t, envelope["event"])
	require.Equal(t, "action_update", event["type"])
	require.Equal(t, "pending", anyMap(t, event["action"])["state"])

	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, permissionOptionAllow, result.response.Outcome.Selected.OptionId)
	case <-time.After(time.Second):
		t.Fatal("permission response was not routed to the registered request")
	}
}

// TestAnnouncedElicitationStampsActionMeta pins that the action correlation
// rides either elicitation variant's own _meta when a turn owns the request.
func TestAnnouncedElicitationStampsActionMeta(t *testing.T) {
	s, _ := stage4Session(t, false)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))

	dialog := newDialogStubClient()
	dialog.elicitationResponse = acp.UnstableCreateElicitationResponse{
		Accept: &acp.UnstableCreateElicitationAccept{Action: "accept", Content: map[string]any{elicitationFieldValue: "v"}},
	}
	_, accepted := s.createDialogElicitation(t.Context(), dialog, pi.UIRequest{ID: "dialog", Method: uiMethodInput})
	require.True(t, accepted)
	require.Len(t, dialog.elicitationRequests, 1)
	require.Contains(t, dialog.elicitationRequests[0].Form.Meta, lifecycleMetaKey)

	dialog.elicitationResponse = acp.UnstableCreateElicitationResponse{
		Decline: &acp.UnstableCreateElicitationDecline{Action: "decline"},
	}
	_, err := s.requestAnnouncedElicitation(t.Context(), dialog, acp.UnstableCreateElicitationRequest{
		Url: &acp.UnstableCreateElicitationUrl{
			ElicitationId: "url", Message: "open", Mode: elicitationModeURL, Url: "https://example.test",
		},
	}, elicitationScope{})
	require.NoError(t, err)
	require.Len(t, dialog.elicitationRequests, 2)
	require.Contains(t, dialog.elicitationRequests[1].Url.Meta, lifecycleMetaKey)
}

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
		s, client := stage4Session(t, true)
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

		entries, err := s.agent.sessionStore().Load(t.Context(), SessionKey{SessionID: "stage4", Subpath: SessionStoreLifecycleSubpath})
		require.NoError(t, err)
		require.Len(t, entries, 1, "the quiescence fact stands behind a durable snapshot commit")
	})

	t.Run("unenumerated boundary certifies nothing", func(t *testing.T) {
		s, client := stage4Session(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))

		require.NoError(t, s.settleCloseBoundary(t.Context(), newStubProcess(true), nil))
		for _, notification := range client.notifications {
			envelope := anyMap(t, notification.Meta[lifecycleMetaKey])
			require.NotEqual(t, "quiescence_update", anyMap(t, envelope["event"])["type"])
		}
	})

	t.Run("incomplete containment settles nothing", func(t *testing.T) {
		s, _ := stage4Session(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.NoError(t, s.settleCloseBoundary(t.Context(), proc, pi.ErrProcessContainmentIncomplete))
		require.False(t, s.lc.vacancyProven)
	})

	t.Run("terminalization failure stops the order", func(t *testing.T) {
		s, client := stage4Session(t, true)
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
		client.updateErr = errors.New("delivery")
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.ErrorContains(t, s.settleCloseBoundary(t.Context(), proc, nil), "delivery")
	})

	t.Run("commit failure stops the order", func(t *testing.T) {
		store := newFaultySessionStore()
		s, _ := stage4Session(t, true)
		s.agent.options.SessionStore = store
		require.NoError(t, s.openLifecycleStream(t.Context(), 1))
		store.appendErr = errors.New("durability unavailable")
		proc := &vacantStubProcess{stubProcess: newStubProcess(true), available: true}
		require.ErrorIs(t, s.settleCloseBoundary(t.Context(), proc, nil), errLifecycleBoundaryCommit)
		require.False(t, s.lc.vacancyProven, "no quiescence fact stands behind an uncommitted snapshot")
	})

	t.Run("mirror failure stops the order", func(t *testing.T) {
		s, _ := stage4Session(t, true)
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
	s, _ := stage4Session(t, false)
	require.NoError(t, s.recordGenerationLoss(t.Context()), "no stream means no loss to record")

	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), lifecycle.Submission{}))
	require.NoError(t, s.recordGenerationLoss(t.Context()))
	require.True(t, s.lc.fenced)
	require.Empty(t, s.lc.turnID)

	entries, err := s.agent.sessionStore().Load(t.Context(), SessionKey{SessionID: "stage4", Subpath: SessionStoreLifecycleSubpath})
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

// TestLifecycleBoundaryCommitFailure pins that a store the boundary record
// cannot append to fails the boundary instead of letting a terminal event
// stand behind nothing durable.
func TestLifecycleBoundaryCommitFailure(t *testing.T) {
	store := newFaultySessionStore()
	s, _ := stage4Session(t, false)
	s.agent.options.SessionStore = store
	store.appendErr = errors.New("durability unavailable")

	require.ErrorIs(t, s.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{StreamID: "stream"}), errLifecycleBoundaryCommit)

	s.id = ""
	require.NoError(t, s.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{}),
		"a session with no native identity has no key a boundary could record under")
}

// TestLifecycleBoundaryRestoreValidation pins the hard journal cut: malformed,
// unknown, unsupported, and semantically incoherent records fail closed rather
// than becoming an absent boundary or an unearned opening fact.
func TestLifecycleBoundaryRestoreValidation(t *testing.T) {
	t.Parallel()

	valid := json.RawMessage(`{"version":1,"streamId":"current","nativeRows":2,"nativeState":"committed","recordedAt":1}`)
	record, err := decodeLifecycleBoundaryRecord(valid)
	require.NoError(t, err)
	require.Equal(t, "current", record.StreamID)
	require.Equal(t, 2, record.NativeRows)

	invalid := map[string]json.RawMessage{
		"malformed":                 json.RawMessage(`not-json`),
		"trailing value":            json.RawMessage(string(valid) + ` {}`),
		"malformed trailing data":   json.RawMessage(string(valid) + ` {`),
		"unknown field":             json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1,"legacy":true}`),
		"missing required":          json.RawMessage(`{"version":1,"streamId":"","nativeState":"committed","recordedAt":1}`),
		"null optional":             json.RawMessage(`{"version":1,"streamId":"","turnId":null,"nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"unsupported version":       json.RawMessage(`{"version":2,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"negative rows":             json.RawMessage(`{"version":1,"streamId":"","nativeRows":-1,"nativeState":"committed","recordedAt":1}`),
		"nonpositive timestamp":     json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":0}`),
		"unsupported disposition":   json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"legacy","recordedAt":1}`),
		"uncommitted vacancy":       json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"retained","vacancyProven":true,"recordedAt":1}`),
		"identity without stream":   json.RawMessage(`{"version":1,"streamId":"","turnId":"turn","cycleId":"cycle","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"cycle without turn":        json.RawMessage(`{"version":1,"streamId":"stream","cycleId":"cycle","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"stop without outcome":      json.RawMessage(`{"version":1,"streamId":"","stopReason":"end_turn","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"unsupported outcome":       json.RawMessage(`{"version":1,"streamId":"","outcome":"unknown","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"failed with stop":          json.RawMessage(`{"version":1,"streamId":"","outcome":"failed","stopReason":"end_turn","nativeRows":0,"nativeState":"retained","recordedAt":1}`),
		"nonfailure without stop":   json.RawMessage(`{"version":1,"streamId":"","outcome":"success","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"nonfailure invalid stop":   json.RawMessage(`{"version":1,"streamId":"","outcome":"success","stopReason":"stop","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"wrong required field type": json.RawMessage(`{"version":1,"streamId":"","nativeRows":"zero","nativeState":"committed","recordedAt":1}`),
	}
	for name, entry := range invalid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, decodeErr := decodeLifecycleBoundaryRecord(entry)
			require.Error(t, decodeErr)
		})
	}
}

func TestLastLifecycleBoundaryValidatesTheCompleteJournal(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(testContainmentOption(), WithSessionStore(store))

	record, found, err := agent.lastLifecycleBoundary(t.Context(), "id")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, lifecycleBoundaryRecord{}, record)

	appendLifecycleBoundaryForRows(t, store, "id", 2)
	appendLifecycleBoundaryForRows(t, store, "id", 1)
	_, _, err = agent.lastLifecycleBoundary(t.Context(), "id")
	require.ErrorContains(t, err, "regressed")

	loadErr := errors.New("journal unavailable")
	agent.options.SessionStore = &lifecycleLoadFailingStore{SessionStore: store, err: loadErr}
	_, _, err = agent.lastLifecycleBoundary(t.Context(), "id")
	require.ErrorIs(t, err, loadErr)
}

func TestSessionRestoreMethodsRejectAnInvalidLifecycleJournal(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Agent) error{
		"load": func(agent *Agent) error {
			_, err := agent.LoadSession(t.Context(), LoadSessionRequest(validSessionUUID, "/cwd"))

			return err
		},
		"resume": func(agent *Agent) error {
			_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest(validSessionUUID, "/cwd"))

			return err
		},
		"fork": func(agent *Agent) error {
			_, err := agent.handleForkSession(t.Context(), forkRaw(t, ForkSessionRequest(validSessionUUID, "/cwd")))

			return err
		},
	}

	for name, invoke := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewInMemorySessionStore()
			require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, []SessionStoreEntry{
				json.RawMessage(`{"type":"session","id":"01234567-89ab-cdef-0123-456789abcdef","cwd":"/cwd"}`),
			}))
			require.NoError(t, store.Append(t.Context(), SessionKey{
				SessionID: validSessionUUID,
				Subpath:   SessionStoreLifecycleSubpath,
			}, []SessionStoreEntry{json.RawMessage(`{"version":1,"vacancyProven":true}`)}))

			agent := NewAgent(testContainmentOption(), WithSessionStore(store))
			require.ErrorContains(t, invoke(agent), "decode lifecycle journal")
		})
	}
}

// TestPublishSessionOpen pins the establishing snapshot: it is emitted exactly
// once, and a lifecycle stream that cannot open is fenced and recorded rather
// than continued from a first event that never landed.
func TestPublishSessionOpen(t *testing.T) {
	t.Run("publishes exactly once", func(t *testing.T) {
		s, client := stage4Session(t, false)
		s.publishSessionOpen(t.Context())
		events := len(client.notifications)
		require.NotZero(t, events)
		s.publishSessionOpen(t.Context())
		require.Len(t, client.notifications, events)
	})

	t.Run("stream failure fences the incarnation", func(t *testing.T) {
		s, client := stage4Session(t, false)
		client.updateErr = errors.New("delivery")
		s.publishSessionOpen(t.Context())
		require.True(t, s.lc.fenced)
	})
}
