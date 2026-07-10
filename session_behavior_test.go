package piacp

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestPromptContentMapping(t *testing.T) {
	mime := "image/png"
	textResource := acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{
		Uri: "file:///a<&\"'", Text: "body <&>",
	}}
	imageResource := acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
		Uri: "file:///image", Blob: "aW1hZ2U=", MimeType: &mime,
	}}
	userAnnotations := &acp.Annotations{Audience: []acp.Role{acp.RoleUser}}
	ignored := acp.TextBlock("ignored")
	ignored.Text.Annotations = userAnnotations

	mapped, err := promptToPi([]acp.ContentBlock{
		acp.TextBlock("hello"), ignored,
		acp.ImageBlock("aW1hZ2U=", "image/png"),
		acp.ResourceLinkBlock("link", " https://example.test "),
		acp.ResourceBlock(textResource),
		acp.ResourceBlock(imageResource),
	})
	require.NoError(t, err)
	require.Contains(t, mapped.Message, "hello")
	require.NotContains(t, mapped.Message, "ignored")
	require.Contains(t, mapped.Message, "https://example.test")
	require.Contains(t, mapped.Message, "&lt;")
	require.Len(t, mapped.Images, 2)

	invalid := [][]acp.ContentBlock{
		nil,
		{{}},
		{acp.TextBlock("  ")},
		{acp.AudioBlock("data", "audio/wav")},
		{acp.ImageBlock("", "image/png")},
		{acp.ResourceBlock(acp.EmbeddedResourceResource{})},
		{acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{Uri: "x", Blob: "data"}})},
	}
	for _, blocks := range invalid {
		_, invalidErr := promptToPi(blocks)
		requireInvalidParams(t, invalidErr)
	}

	text, contextText, image, err := resourceToPi(textResource)
	require.NoError(t, err)
	require.NotEmpty(t, text)
	require.NotEmpty(t, contextText)
	require.Nil(t, image)
	require.Equal(t, "&amp;&lt;&gt;&quot;&apos;", xmlEscape("&<>\"'"))
}

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

func TestModelAndCommandMapping(t *testing.T) {
	models := []pi.Model{
		{Provider: "p", ID: "one", Name: "One", ContextWindow: 100, MaxTokens: 10, Reasoning: true, Input: []string{"image", "audio", "pdf", "video", "text"}},
		{Provider: "p", ID: "one"},
		{Provider: "", ID: "bad"},
		{Provider: "p", ID: "two"},
	}
	options := modelSelectOptions("p/missing", models)
	require.Len(t, options, 3)
	require.Equal(t, "One", options[0].Name)
	require.Equal(t, "p/two", modelDisplayName(&models[3]))
	require.Equal(t, []string{"reasoning", "image", "audio", "pdf", "video"}, modelCapabilities(&models[0]))
	require.Empty(t, modelCapabilities(&pi.Model{}))
	require.NotEmpty(t, piModelInfoMeta(&models[0]))

	for _, level := range pi.ThinkingLevels() {
		require.NotEmpty(t, thinkingLevelDisplayName(level))
	}
	require.Equal(t, "custom", thinkingLevelDisplayName("custom"))
	require.Len(t, thinkingLevelSelectOptions(), len(pi.ThinkingLevels()))

	commands := availableCommandsFromNative([]pi.SlashCommand{
		{Name: "good", Description: "ok"},
		{Name: ""}, {Name: "bad/name"}, {Name: "bad name"}, {Name: "bad\x00"}, {Name: "bad\u200e"},
		{Name: string([]byte{0xff})},
	})
	require.Len(t, commands, 1)
	require.True(t, availableCommandsEqual(commands, cloneAvailableCommands(commands)))
	require.False(t, availableCommandsEqual(commands, nil))
	other := cloneAvailableCommands(commands)
	other[0].Description = "different"
	require.False(t, availableCommandsEqual(commands, other))
	require.Nil(t, cloneAvailableCommands(nil))

	unstructured := acp.UnstructuredCommandInput{Hint: "hint"}
	input := acp.AvailableCommandInput{Unstructured: &unstructured}
	withInput := []acp.AvailableCommand{{Name: "input", Input: &input}}
	clone := cloneAvailableCommands(withInput)
	clone[0].Input.Unstructured.Hint = "changed"
	require.Equal(t, "hint", withInput[0].Input.Unstructured.Hint)
}

func TestReplayMappingsAndStoreMetadata(t *testing.T) {
	rows := []SessionStoreEntry{
		json.RawMessage(`bad`),
		json.RawMessage(`{"type":"session","cwd":"/cwd"}`),
		json.RawMessage(`{"type":"session_info","name":" Named "}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hello"},{"type":"image","data":"aW1n","mimeType":"image/png"}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[{"type":"thinking","thinking":"thought"},{"type":"text","text":"answer"},{"type":"toolCall","id":"call","name":"bash","arguments":{"command":"true"}}]`)}),
		messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call", IsError: true, Content: json.RawMessage(`[{"type":"text","text":"failed"},{"type":"image","data":"aW1n","mimeType":"image/png"}]`)}),
	}
	updates := replayUpdates(rows)
	require.Len(t, updates, 6)
	require.Equal(t, "Named", storeSessionTitle("fallback", rows))
	require.Equal(t, "/cwd", storeSessionCwd(rows))
	require.True(t, storeSessionHasContent(rows))

	require.Equal(t, "hello", storeSessionTitle("fallback", []SessionStoreEntry{rows[0], rows[1], rows[3]}))
	require.Equal(t, "fallback", storeSessionTitle("fallback", nil))
	require.Empty(t, storeSessionCwd(nil))
	require.False(t, storeSessionHasContent(nil))
	require.Nil(t, messageReplayUpdates(pi.AgentMessage{Role: "unknown", Content: json.RawMessage(`[]`)}))
	require.Nil(t, messageReplayUpdates(pi.AgentMessage{Role: messageRoleToolResult, Content: json.RawMessage(`[]`)}))
	require.Nil(t, messageReplayUpdates(pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`bad`)}))

	content := toolCallContent([]pi.ContentBlock{{Type: contentBlockTypeText}, {Type: contentBlockTypeImage}, {Type: "other"}})
	require.Empty(t, content)
	_, ok := decodeStoreRow(json.RawMessage(`bad`))
	require.False(t, ok)
}

func messageRow(t *testing.T, message pi.AgentMessage) SessionStoreEntry {
	t.Helper()
	data, err := json.Marshal(message)
	require.NoError(t, err)
	row, err := json.Marshal(storeRow{Type: storeRowTypeMessage, Message: data})
	require.NoError(t, err)

	return row
}

func TestSessionTitlesStopReasonsAndToolKinds(t *testing.T) {
	for name, kind := range map[string]acp.ToolKind{
		"read": acp.ToolKindRead, "edit": acp.ToolKindEdit, "write": acp.ToolKindEdit,
		"bash": acp.ToolKindExecute, "grep": acp.ToolKindSearch, "find": acp.ToolKindSearch,
		"glob": acp.ToolKindSearch, "ls": acp.ToolKindSearch, "fetch": acp.ToolKindFetch,
		"web_fetch": acp.ToolKindFetch, "other": acp.ToolKindOther,
	} {
		require.Equal(t, kind, toolKindForName(name))
	}

	session := &agentSession{agent: &Agent{log: slog.New(slog.DiscardHandler)}}
	for native, want := range map[string]acp.StopReason{
		stopReasonStop:      acp.StopReasonEndTurn,
		stopReasonLength:    acp.StopReasonMaxTokens,
		stopReasonMaxTokens: acp.StopReasonMaxTokens,
		stopReasonAborted:   acp.StopReasonCancelled,
		"unknown":           acp.StopReasonEndTurn,
	} {
		require.Equal(t, want, acpStopReason(session, &promptTurnState{stopReason: native}))
	}

	require.Equal(t, "short title", normalizeLiveSessionTitle("  short   title  "))
	require.Equal(t, "", normalizeLiveSessionTitle(" \n\t "))
	long := strings.Repeat("x", 300)
	require.LessOrEqual(t, len(normalizeLiveSessionTitle(long)), liveSessionTitleMaxRunes)
	prompt := []acp.ContentBlock{acp.TextBlock(" first "), acp.TextBlock("second")}
	require.Equal(t, "first", liveSessionTitleFromPrompt(prompt))
	require.Empty(t, liveSessionTitleFromPrompt(nil))

	err := providerTurnFailure(&promptTurnState{stopReason: stopReasonError, errorMessage: "message"})
	requirePiTurnFailure(t, err, failureCauseProvider)
	require.NoError(t, providerTurnFailure(&promptTurnState{}))
	require.False(t, emptyCloneError(errors.New("other")))
	require.True(t, emptyCloneError(&pi.CommandError{Message: "Entry abcd not found"}))
}
