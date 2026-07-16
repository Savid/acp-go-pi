package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type orderedPermissionClient struct {
	*dialogStubClient
	updatesBeforePermission []acp.SessionUpdate
}

func (c *orderedPermissionClient) RequestPermission(
	ctx context.Context,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.updatesBeforePermission = append(c.updatesBeforePermission, c.updates...)

	return c.dialogStubClient.RequestPermission(ctx, request)
}

func TestToolKindForNameMapping(t *testing.T) {
	for name, kind := range map[string]acp.ToolKind{
		"read": acp.ToolKindRead, "edit": acp.ToolKindEdit, "write": acp.ToolKindEdit,
		"bash": acp.ToolKindExecute, "grep": acp.ToolKindSearch, "find": acp.ToolKindSearch,
		"glob": acp.ToolKindSearch, "ls": acp.ToolKindSearch, "fetch": acp.ToolKindFetch,
		"web_fetch": acp.ToolKindFetch, "other": acp.ToolKindOther,
	} {
		require.Equal(t, kind, toolKindForName(name))
	}
}

func TestRegisterDialogCancelledTurn(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	session.turnCancelled = true
	dialogCtx, finishDialog := session.registerDialog(t.Context(), "cancelled")
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)
	finishDialog()
	require.Empty(t, session.pendingDialogs)
}

func TestRequestPermissionAnswerFailsClosed(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(t.Context(), pi.UIRequest{ID: "none"}, pi.PermissionPrompt{}),
	)

	permissionClient := newDialogStubClient()
	permissionClient.updateErr = errors.New("publish failed")
	agent.setConnection(permissionClient)
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "publish-error"},
			pi.PermissionPrompt{ToolCallID: "native-call-publish-error", ToolName: "bash"},
		),
	)
	require.Empty(t, permissionClient.permissionRequests)
	require.Empty(t, session.permissionTools)

	permissionClient.updateErr = nil
	permissionClient.permissionErr = errors.New("permission failed")
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "error"},
			pi.PermissionPrompt{
				ToolCallID: "native-call-error",
				ToolName:   "surface_tool",
				Input:      json.RawMessage(`{"value":true}`),
			},
		),
	)
}

func TestRequestPermissionUsesExactNativeToolCallID(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	permissionClient := newDialogStubClient()
	permissionClient.permissionResponse = acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	}
	agent.setConnection(permissionClient)
	session := &agentSession{agent: agent, id: "session"}

	answer := session.requestPermissionAnswer(
		t.Context(),
		pi.UIRequest{ID: "permission-1"},
		pi.PermissionPrompt{
			ToolCallID: "native-call-42",
			ToolName:   "bash",
			Input:      json.RawMessage(`{"command":"echo hi"}`),
		},
	)

	require.Equal(t, string(permissionOptionAllow), answer)
	require.Len(t, permissionClient.permissionRequests, 1)
	request := permissionClient.permissionRequests[0]
	require.Equal(t, acp.SessionId("session"), request.SessionId)
	require.Equal(t, acp.ToolCallId("native-call-42"), request.ToolCall.ToolCallId)
	require.NotEqual(t, acp.ToolCallId("bash"), request.ToolCall.ToolCallId)
	require.Equal(t, "bash", *request.ToolCall.Title)
	rawInput, ok := request.ToolCall.RawInput.(json.RawMessage)
	require.True(t, ok)
	require.JSONEq(t, `{"command":"echo hi"}`, string(rawInput))
}

func TestPermissionPublishesPendingCallBeforeRequestAndNativeStartUpdatesIt(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := &orderedPermissionClient{dialogStubClient: newDialogStubClient()}
	client.permissionResponse = acp.RequestPermissionResponse{
		Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow),
	}
	agent.setConnection(client)
	native := newStubPiClient()
	session := &agentSession{agent: agent, id: "session", client: native}

	session.handleUIDialog(t.Context(), pi.UIRequest{
		ID:     "permission-1",
		Method: uiMethodSelect,
		Title: pi.PermissionTitleMarker +
			`{"toolCallId":"native-call-42","toolName":"bash","input":{"command":"echo hi"}}`,
	})

	require.Len(t, client.updatesBeforePermission, 1)
	pending := client.updatesBeforePermission[0].ToolCall
	require.NotNil(t, pending)
	require.Equal(t, acp.ToolCallId("native-call-42"), pending.ToolCallId)
	require.Equal(t, acp.ToolCallStatusPending, pending.Status)
	require.Len(t, client.permissionRequests, 1)
	require.Equal(t, pending.ToolCallId, client.permissionRequests[0].ToolCall.ToolCallId)

	_, err := session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: "native-call-42",
		ToolName:   "bash",
		Args:       json.RawMessage(`{"command":"echo hi"}`),
	}, &promptTurnState{})
	require.NoError(t, err)

	starts := 0
	progressUpdates := 0
	for _, update := range client.updates {
		if update.ToolCall != nil {
			starts++
		}
		if update.ToolCallUpdate != nil && update.ToolCallUpdate.Status != nil &&
			*update.ToolCallUpdate.Status == acp.ToolCallStatusInProgress {
			progressUpdates++
			require.Equal(t, pending.ToolCallId, update.ToolCallUpdate.ToolCallId)
		}
	}
	require.Equal(t, 1, starts, "native start must not duplicate the pending tool call")
	require.Equal(t, 1, progressUpdates)
	require.Empty(t, session.permissionTools)
}

func TestMalformedPermissionMarkerFailsClosed(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	permissionClient := newDialogStubClient()
	agent.setConnection(permissionClient)
	native := newStubPiClient()
	session := &agentSession{agent: agent, id: "session", client: native}

	requests := []pi.UIRequest{
		{ID: "missing", Method: uiMethodSelect, Title: pi.PermissionTitleMarker + `{"toolName":"bash"}`},
		{ID: "malformed", Method: uiMethodSelect, Title: pi.PermissionTitleMarker + `{"toolCallId":7,"toolName":"bash"}`},
		{
			ID:     "wrong-method",
			Method: uiMethodConfirm,
			Title:  pi.PermissionTitleMarker + `{"toolCallId":"native-call-42","toolName":"bash"}`,
		},
	}
	for _, request := range requests {
		session.handleUIDialog(t.Context(), request)
	}

	require.Empty(t, permissionClient.permissionRequests, "malformed markers must not reach ACP permissions")
	require.Empty(t, permissionClient.elicitationRequests, "malformed markers must not be reclassified as elicitation")
	require.Len(t, native.responses, len(requests))
	for index, request := range requests {
		require.Equal(t, pi.UICancelResponse(request.ID), native.responses[index])
	}
}

func TestRespondUIDialogFailures(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	session.respondUIDialog(t.Context(), pi.UICancelResponse("no-client"))
	native := newStubPiClient()
	native.respondErr = errors.New("respond failed")
	session.client = native
	session.respondUIDialog(t.Context(), pi.UICancelResponse("error"))
}
