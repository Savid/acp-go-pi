package piacp

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestSessionRequestBuildersAndCloning(t *testing.T) {
	meta := map[string]any{"foreign": map[string]any{"list": []any{"value"}}}
	request := NewSessionRequest("/cwd",
		WithSessionAdditionalDirectories("/one", "/two"),
		WithSessionMeta(meta),
		WithSessionPiOptions(NewPiOptions(WithPiModel("fake/model"))),
		WithSessionRawEvents(true),
	)
	require.Equal(t, []string{"/one", "/two"}, request.AdditionalDirectories)
	require.NotNil(t, request.McpServers)
	require.Equal(t, "fake/model", anyMap(t, anyMap(t, request.Meta[piMetaKey])[metaOptionsKey])[metaModelKey])

	anyMap(t, meta["foreign"])["changed"] = true
	require.NotContains(t, anyMap(t, request.Meta["foreign"]), "changed")

	load := LoadSessionRequest("id", "/cwd", WithSessionMeta(map[string]any{"x": true}))
	resume := ResumeSessionRequest("id", "/cwd")
	require.Equal(t, acp.SessionId("id"), load.SessionId)
	require.Equal(t, acp.SessionId("id"), resume.SessionId)
	require.NotNil(t, load.McpServers)
	require.Nil(t, resume.AdditionalDirectories)

	output := NewSessionRequest("/cwd", WithSessionOutputSchema(map[string]any{"type": "object"}))
	require.NotEmpty(t, output.Meta)

	prompt := PromptRequest("id", "turn-1", acp.TextBlock("text"))
	require.Len(t, prompt.Prompt, 1)
	require.Len(t, TextPromptRequest("id", "test-turn", "text").Prompt, 1)
	require.NotNil(t, PromptRequest("id", "turn-2").Prompt)
	require.Equal(t, turnRouteMeta("turn-cancel"), CancelRequest("id", "turn-cancel").Meta)

	config := SetConfigOptionRequest("id", "custom", "value")
	require.NotNil(t, config.ValueId)
	model := SetModelRequest("id", "fake/model")
	require.Equal(t, configModel, model.ValueId.ConfigId)
	require.Equal(t, acp.SessionId("id"), DeleteSessionRequest("id").SessionId)

	list := ListSessionsRequest(
		WithListSessionsCwd("/cwd"),
		WithListSessionsCursor("cursor"),
		WithListSessionsMeta(map[string]any{"x": []string{"y"}}),
	)
	require.Equal(t, "/cwd", *list.Cwd)
	require.Equal(t, "cursor", *list.Cursor)
	require.Equal(t, []string{"y"}, list.Meta["x"])
}

func TestMCPBuilderConversionsAndClones(t *testing.T) {
	stdio := StdioMCPServer("stdio", "cmd", []string{"arg"}, map[string]string{"A": "B"})
	http := HTTPMCPServer("http", "https://example.test", map[string]string{"X": "Y"})
	sse := acp.McpServer{Sse: &acp.McpServerSseInline{
		Type: "sse", Name: "sse", Url: "https://example.test/sse",
		Headers: []acp.HttpHeader{{Name: "H", Value: "V", Meta: map[string]any{"m": true}}},
		Meta:    map[string]any{"nested": map[string]any{"x": true}},
	}}
	acpServer := acp.McpServer{Acp: &acp.McpServerAcpInline{Type: "acp", Name: "acp", Id: "id", Meta: map[string]any{"x": true}}}
	servers := []acp.McpServer{stdio, http, sse, acpServer, {}}

	cloned := cloneMCPServers(servers)
	require.Equal(t, servers, cloned)
	servers[0].Stdio.Args[0] = "changed"
	require.Equal(t, "arg", cloned[0].Stdio.Args[0])
	require.Nil(t, cloneMCPServers(nil))
	require.Nil(t, cloneMCPServerStdio(nil))
	require.Nil(t, cloneHTTPHeaders(nil))
	require.Nil(t, cloneEnvVariables(nil))

	unstable := unstableMCPServersFromStable(cloned)
	require.Len(t, unstable, len(cloned))
	require.NotNil(t, unstable[0].Stdio)
	require.NotNil(t, unstable[1].Http)
	require.NotNil(t, unstable[2].Sse)
	require.NotNil(t, unstable[3].Acp)
	require.Equal(t, acp.UnstableMcpServer{}, unstable[4])
	require.Nil(t, unstableMCPServersFromStable(nil))
}

func TestMapAndOptionCloneHelpers(t *testing.T) {
	base := map[string]any{"nested": map[string]any{"base": true}, "keep": "yes"}
	overlay := map[string]any{"nested": map[string]any{"overlay": true}, "slice": []any{map[string]any{"x": true}}, "strings": []string{"a"}}
	merged := mergeAnyMap(base, overlay)
	require.Equal(t, true, anyMap(t, merged["nested"])["base"])
	require.Equal(t, true, anyMap(t, merged["nested"])["overlay"])
	require.Equal(t, map[string]any{}, mergeAnyMap(nil, nil))

	meta := map[string]any{"wrong": "value", "right": map[string]any{"a": true}}
	require.Empty(t, ensureMetaMap(meta, "wrong"))
	right := ensureMetaMap(meta, "right")
	right["b"] = true
	require.Contains(t, anyMap(t, meta["right"]), "b")

	require.Nil(t, cloneAnyMap(nil))
	require.Equal(t, 7, cloneAny(7))
	options := NewPiOptions(WithPiOutputSchema(map[string]any{"x": []string{"y"}}))
	cloned := clonePiOptions(options)
	cloned.OutputSchema["changed"] = true
	require.NotContains(t, options.OutputSchema, "changed")
}

func TestForkCallRejectsMalformedResponse(t *testing.T) {
	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()
	t.Cleanup(func() {
		_ = clientToAgentReader.Close()
		_ = clientToAgentWriter.Close()
		_ = agentToClientReader.Close()
		_ = agentToClientWriter.Close()
	})

	_ = acp.NewConnection(
		func(context.Context, string, json.RawMessage) (any, *acp.RequestError) {
			return "not a fork response", nil
		},
		agentToClientWriter,
		clientToAgentReader,
	)
	conn := acp.NewClientSideConnection(&conformanceClient{}, clientToAgentWriter, agentToClientReader)

	_, err := CallForkSession(t.Context(), conn, forkParams(t))
	require.Error(t, err)
}
