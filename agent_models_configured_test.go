package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestConfiguredModelsRefuseMalformedIDs(t *testing.T) {
	for name, ids := range map[string][]string{
		"empty":             {""},
		"surrounding space": {"omp/qwen "},
		"unqualified":       {"qwen"},
		"duplicate":         {"omp/qwen", "omp/qwen"},
	} {
		t.Run(name, func(t *testing.T) {
			agent := NewAgent(WithConfiguredModels(ids))
			t.Cleanup(func() { _ = agent.Close() })

			var reqErr *acp.RequestError
			require.ErrorAs(t, agent.optionErr, &reqErr)
			require.Equal(t, optionFieldConfiguredModels, optionFailureField(reqErr))
		})
	}

	accepted := NewAgent(WithConfiguredModels([]string{"omp/qwen", "omp/opencode-go/deepseek"}))
	t.Cleanup(func() { _ = accepted.Close() })
	require.NoError(t, accepted.optionErr)
}

// TestHostListedModelsFollowTheNativeRows pins the host-listed entry: it
// follows the native rows as the id alone, a native row of the same value
// stands with its facts, and the current model still closes the menu.
func TestHostListedModelsFollowTheNativeRows(t *testing.T) {
	models := []pi.Model{
		{Provider: "p", ID: "one", Name: "One", ContextWindow: 100, MaxTokens: 10},
	}

	values := modelSelectOptions("p/current", models, []string{"p/one", "omp/qwen", "omp/deepseek"})
	require.Len(t, values, 4)
	require.Equal(t, "One", values[0].Name, "the native row stands and the host entry adds nothing")
	require.NotEmpty(t, values[0].Meta)
	require.Equal(t, acp.SessionConfigValueId("omp/qwen"), values[1].Value)
	require.Equal(t, "omp/qwen", values[1].Name)
	require.Empty(t, values[1].Meta, "a host-listed row carries no invented facts")
	require.Equal(t, acp.SessionConfigValueId("omp/deepseek"), values[2].Value)
	require.Equal(t, acp.SessionConfigValueId("p/current"), values[3].Value)

	// A host-listed current model is published once.
	require.Len(t, modelSelectOptions("omp/qwen", nil, []string{"omp/qwen"}), 1)
}
