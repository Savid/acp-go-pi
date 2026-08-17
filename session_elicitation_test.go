package piacp

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestElicitationMapping(t *testing.T) {
	tests := []struct {
		request pi.UIRequest
		content map[string]any
		accept  bool
	}{
		{request: pi.UIRequest{ID: "1", Method: uiMethodSelect, Options: []string{"a", "b"}}, content: map[string]any{elicitationFieldChoice: "b"}, accept: true},
		{request: pi.UIRequest{ID: "2", Method: uiMethodSelect, Options: []string{"a"}}, content: map[string]any{elicitationFieldChoice: "missing"}},
		{request: pi.UIRequest{ID: "3", Method: uiMethodConfirm}, content: map[string]any{elicitationFieldConfirmed: true}, accept: true},
		{request: pi.UIRequest{ID: "4", Method: uiMethodConfirm}, content: map[string]any{elicitationFieldConfirmed: "yes"}},
		{request: pi.UIRequest{ID: "5", Method: uiMethodInput}, content: map[string]any{elicitationFieldValue: "value"}, accept: true},
		{request: pi.UIRequest{ID: "6", Method: uiMethodEditor}, content: map[string]any{elicitationFieldValue: "value"}, accept: true},
		{request: pi.UIRequest{ID: "7", Method: uiMethodInput}, content: map[string]any{elicitationFieldValue: true}},
		{request: pi.UIRequest{ID: "8", Method: "unknown"}, content: map[string]any{}},
	}
	for _, test := range tests {
		_, accepted := dialogAnswer(test.request, test.content)
		require.Equal(t, test.accept, accepted)
	}
	require.True(t, containsOption([]string{"a", "b"}, "b"))
	require.False(t, containsOption([]string{"a"}, "b"))

	require.Equal(t, "pi needs more input.", elicitationMessage(pi.UIRequest{}))
	require.Equal(t, " Title ", elicitationMessage(pi.UIRequest{Title: " Title "}))
	require.Equal(t, " Message ", elicitationMessage(pi.UIRequest{Message: " Message "}))
	require.Equal(t, "Title\n\nMessage", elicitationMessage(pi.UIRequest{Title: "Title", Message: "Message"}))

	for _, request := range []pi.UIRequest{
		{Method: uiMethodSelect, Options: []string{"a", "b"}},
		{Method: uiMethodConfirm},
		{Method: uiMethodInput, Placeholder: "hint", Prefill: "default"},
		{Method: uiMethodEditor},
	} {
		schema := elicitationSchemaForDialog(request)
		require.NotEmpty(t, schema.Required)
		require.NotEmpty(t, schema.Properties)
	}
}

func TestCreateDialogElicitationFailsClosed(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	elicitClient := newDialogStubClient()
	elicitClient.elicitationErr = errors.New("elicitation failed")
	_, accepted := session.createDialogElicitation(
		t.Context(),
		elicitClient,
		pi.UIRequest{ID: "error", Method: uiMethodInput},
	)
	require.False(t, accepted)

	elicitClient.elicitationErr = nil
	_, accepted = session.createDialogElicitation(
		t.Context(),
		elicitClient,
		pi.UIRequest{ID: "cancel", Method: uiMethodInput},
	)
	require.False(t, accepted)
}
