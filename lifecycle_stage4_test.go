package piacp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

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
		func(meta map[string]any) (string, error) {
			require.Nil(t, meta)

			return "plain", nil
		},
		func(string, error) lifecycle.ActionState { return lifecycle.ActionAccepted })
	require.NoError(t, err)
	require.Equal(t, "plain", value)
}
