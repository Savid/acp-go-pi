package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

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
