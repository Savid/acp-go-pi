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

// TestAnnouncedActionRequestLifecycle pins the ordered action contract: the
// request carries the action identity on the wire, the announcement follows
// it, and the resolution lands exactly once with the answer's classification.
func TestAnnouncedActionRequestLifecycle(t *testing.T) {
	s, client := lifecycleSession(t, false)
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

	fenced, _ := lifecycleSession(t, false)
	require.NoError(t, fenced.openLifecycleStream(t.Context(), 1))
	require.NoError(t, fenced.lifecycleAcceptTurn(t.Context(), testSubmission()))
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
	s, _ := lifecycleSession(t, false)
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
