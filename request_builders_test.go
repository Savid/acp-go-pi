package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

func TestSessionRequestBuilders(t *testing.T) {
	t.Parallel()

	request := wire.NewSessionRequest("/w", WithSessionRawEvents(true), WithSessionPiOptions(NewPiOptions(WithPiModel("p/m"))))
	require.Equal(t, map[string]any{"options": map[string]any{"model": "p/m"}, "rawEvent": map[string]any{"enabled": true}}, request.Meta["pi"])

	require.Equal(t, configModel, SetModelRequest("id", "p/m").ValueId.ConfigId)
	require.Equal(t, acp.SessionConfigValueId("p/m"), SetModelRequest("id", "p/m").ValueId.Value)
}

func TestMetadataClonesTypedEnvironment(t *testing.T) {
	t.Parallel()

	env := map[string]string{"PI_EXAMPLE": "original"}
	meta := map[string]any{vendor: map[string]any{"options": map[string]any{"env": env}}}
	option := wire.WithSessionMeta(meta)
	first := wire.NewSessionRequest(t.TempDir(), option)
	env["PI_EXAMPLE"] = "caller changed"
	second := wire.NewSessionRequest(t.TempDir(), option)

	for _, request := range []map[string]any{first.Meta, second.Meta} {
		parsed, err := parseSessionMeta(request)
		require.Nil(t, err)
		require.Equal(t, "original", parsed.options.Env["PI_EXAMPLE"], "a caller mutation reached the session environment")
	}
}
