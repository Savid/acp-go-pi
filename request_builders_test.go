package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestSessionRequestBuilders(t *testing.T) {
	t.Parallel()

	request := NewSessionRequest("/w", WithSessionAdditionalDirectories("/a"), WithSessionMeta(map[string]any{"host": 1}), WithSessionRawEvents(true), WithSessionPiOptions(NewPiOptions(WithPiModel("p/m"))))
	require.Equal(t, "/w", request.Cwd)
	require.Equal(t, []acp.McpServer{}, request.McpServers)
	require.Equal(t, []string{"/a"}, request.AdditionalDirectories)
	require.Equal(t, 1, request.Meta["host"])
	require.Equal(t, map[string]any{"options": map[string]any{"model": "p/m"}, "rawEvent": map[string]any{"enabled": true}}, request.Meta["pi"])

	load := LoadSessionRequest("id", "/w")
	require.Equal(t, acp.SessionId("id"), load.SessionId)
	require.Equal(t, []acp.McpServer{}, load.McpServers)

	resume := ResumeSessionRequest("id", "/w", WithSessionOutputSchema(map[string]any{"type": "object"}))
	require.Equal(t, []acp.McpServer{}, resume.McpServers)
	require.NotNil(t, resume.Meta["pi"])

	require.Equal(t, acp.SessionId("id"), DeleteSessionRequest("id").SessionId)
	require.Equal(t, acp.SessionId("id"), CancelRequest("id").SessionId)
	require.Len(t, TextPromptRequest("id", "hi").Prompt, 1)
	require.NotNil(t, PromptRequest("id").Prompt)
	require.Equal(t, configModel, SetModelRequest("id", "p/m").ValueId.ConfigId)

	list := ListSessionsRequest(WithListSessionsCwd("/w"), WithListSessionsCursor("c"), WithListSessionsMeta(map[string]any{"k": "v"}))
	require.Equal(t, "/w", *list.Cwd)
	require.Equal(t, "c", *list.Cursor)
	require.Equal(t, "v", list.Meta["k"])
}

func TestBuildersRejectReservedMeta(t *testing.T) {
	t.Parallel()

	for _, literal := range wire.ReservedLiterals {
		require.Panics(t, func() { WithSessionMeta(map[string]any{literal: 1}) })
		require.Panics(t, func() { WithListSessionsMeta(map[string]any{literal: 1}) })
	}
}
