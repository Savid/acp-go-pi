package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

func TestModelSelectOptions(t *testing.T) {
	t.Parallel()

	models := []pi.Model{
		{Provider: "a", ID: "one", Name: "One", ContextWindow: 10},
		{Provider: "a", ID: "one"},
		{Provider: "", ID: "bad"},
	}

	values := modelSelectOptions("z/current", models, []string{"a/one", "h/listed"})
	require.Len(t, values, 3)
	require.Equal(t, acp.SessionConfigValueId("a/one"), values[0].Value)
	require.Equal(t, "One", values[0].Name)
	require.Equal(t, acp.SessionConfigValueId("h/listed"), values[1].Value)
	require.Equal(t, acp.SessionConfigValueId("z/current"), values[2].Value)
	require.Nil(t, values[2].Meta)

	levels := thinkingLevelSelectOptions()
	require.Len(t, levels, 7)
	require.Equal(t, "Extra High", levels[5].Name)
}

func TestConfigOptionsOmitUnknown(t *testing.T) {
	t.Parallel()

	s := &session{agent: NewAgent(testOptions(t)...)}
	require.Empty(t, s.configOptions())

	s.model = "a/b"
	s.thinkingLevel = "low"
	options := s.configOptions()
	require.Len(t, options, 2)
	require.Equal(t, "select", options[0].Select.Type)
}

func TestNewSessionReportsEffectiveThinkingLevel(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ model, requested, effective string }{
		{"fake/text-only", "", "off"},
		{"fake/text-only", "high", "off"},
		{"fake/vision", "high", "high"},
	} {
		t.Run(tc.model+"/"+tc.requested, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.initialize()
			session := h.newSession(WithSessionPiOptions(NewPiOptions(
				WithPiModel(tc.model), WithPiThinkingLevel(tc.requested),
			)))
			require.Equal(t, acp.SessionConfigValueId(tc.effective), session.ConfigOptions[1].Select.CurrentValue)
		})
	}
}

func TestReasoningSelectionSurvivesRuntimeRelaunch(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir(), WithSessionPiOptions(NewPiOptions(WithPiThinkingLevel("high")))))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	_, err = s.setConfigOption(t.Context(), configThoughtLevel, "low")
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	selected := s.thinkingLevel
	s.mu.Unlock()
	require.Equal(t, "low", selected)
	s.stopRuntime(t.Context(), rt)
	_, err = s.ensureRuntime(t.Context())
	require.NoError(t, err)
	s.mu.Lock()
	selected = s.thinkingLevel
	s.mu.Unlock()
	require.Equal(t, "low", selected, "successful reasoning change reverted on relaunch")
}
