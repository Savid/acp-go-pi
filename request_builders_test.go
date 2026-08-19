package piacp

import (
	"context"
	"encoding/json"
	"io"
	"strings"
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
	emptyOutput := NewSessionRequest("/cwd", WithSessionOutputSchema(map[string]any{}))
	require.Equal(t, map[string]any{}, anyMap(t, anyMap(t, emptyOutput.Meta[piMetaKey])[metaOptionsKey])[metaOutputSchemaKey])

	prompt := PromptRequest("id", "turn-1", acp.TextBlock("text"))
	require.Len(t, prompt.Prompt, 1)
	require.Len(t, TextPromptRequest("id", "test-turn", "text").Prompt, 1)
	require.NotNil(t, PromptRequest("id", "turn-2").Prompt)
	require.Equal(t, turnRouteMeta("turn-cancel"), CancelRequest("id", "turn-cancel").Meta)
	boundaryNonce := strings.Repeat("n", routeTurnNonceMaxBytes)
	require.Equal(t, turnRouteMeta(boundaryNonce), PromptRequest("id", boundaryNonce).Meta)
	require.Equal(t, turnRouteMeta(boundaryNonce), CancelRequest("id", boundaryNonce).Meta)
	for _, invalidNonce := range []string{"", " \t", strings.Repeat("n", routeTurnNonceMaxBytes+1)} {
		require.Nil(t, PromptRequest("id", invalidNonce).Meta)
		require.Nil(t, CancelRequest("id", invalidNonce).Meta)
	}

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

// TestMetaBuildersRejectEveryReservedLiteral pins the family-literal guard on
// the two builders that take a host-supplied _meta map. Each `acp-go.dev/*`
// namespace is the family's: the wrapper stamps the route envelope and the
// lifecycle correlation, the host writes handoff and media-envelope values only
// where this contract says so, and none of them may arrive through a caller map
// that the builder would otherwise merge or let overwrite. Everything the host
// actually owns still rides.
func TestMetaBuildersRejectEveryReservedLiteral(t *testing.T) {
	t.Parallel()

	require.Len(t, reservedMetaLiterals, 4, "the reserved set is closed at four")

	caller := map[string]any{"host": map[string]any{"trace": "keep-me"}}
	for _, literal := range reservedMetaLiterals {
		caller[literal] = map[string]any{"forged": true}
	}

	t.Run("WithSessionMeta", func(t *testing.T) {
		t.Parallel()

		for name, meta := range map[string]map[string]any{
			"session/new":    NewSessionRequest("/cwd", WithSessionMeta(caller)).Meta,
			"session/load":   LoadSessionRequest("id", "/cwd", WithSessionMeta(caller)).Meta,
			"session/resume": ResumeSessionRequest("id", "/cwd", WithSessionMeta(caller)).Meta,
			"session/fork":   ForkSessionRequest("id", "/cwd", WithSessionMeta(caller)).Meta,
		} {
			for _, literal := range reservedMetaLiterals {
				require.NotContains(t, meta, literal, name)
			}

			require.Equal(t, map[string]any{"trace": "keep-me"}, meta["host"], name)
		}
	})

	t.Run("WithListSessionsMeta", func(t *testing.T) {
		t.Parallel()

		meta := ListSessionsRequest(WithListSessionsMeta(caller)).Meta
		for _, literal := range reservedMetaLiterals {
			require.NotContains(t, meta, literal)
		}

		require.Equal(t, map[string]any{"trace": "keep-me"}, meta["host"])
	})

	t.Run("the caller's own map is never mutated", func(t *testing.T) {
		t.Parallel()

		supplied := map[string]any{lifecycleMetaKey: map[string]any{"v": 1}}
		_ = NewSessionRequest("/cwd", WithSessionMeta(supplied))
		require.Contains(t, supplied, lifecycleMetaKey)
	})

	t.Run("a nil map carries nothing", func(t *testing.T) {
		t.Parallel()

		require.Empty(t, ListSessionsRequest(WithListSessionsMeta(nil)).Meta)
	})
}
