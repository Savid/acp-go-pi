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
	permissionClient.permissionErr = errors.New("permission failed")
	agent.setConnection(permissionClient)
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "error"},
			pi.PermissionPrompt{ToolName: "surface_tool", Input: json.RawMessage(`{"value":true}`)},
		),
	)
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
