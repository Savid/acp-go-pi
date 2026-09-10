package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestWithAmbientEnvironmentReplacesTheAdapterEnvironment(t *testing.T) {
	original := captureAmbientEnvironment
	t.Cleanup(func() { captureAmbientEnvironment = original })

	captureAmbientEnvironment = func() []string {
		t.Error("the adapter environment was read despite a supplied ambient block")

		return []string{"PATH=/adapter/bin"}
	}

	agent := NewAgent(WithAmbientEnvironment(map[string]string{
		"HOME":           "/host/home",
		"PATH":           "/host/bin",
		"OPENAI_API_KEY": "must-not-cross",
	}))
	t.Cleanup(func() { _ = agent.Close() })
	require.NoError(t, agent.optionErr)
	require.Equal(t, map[string]string{"HOME": "/host/home", "PATH": "/host/bin"}, agent.ordinaryEnvironment)

	empty := NewAgent(WithAmbientEnvironment(map[string]string{}))
	t.Cleanup(func() { _ = empty.Close() })
	require.Empty(t, empty.ordinaryEnvironment)
}

func TestWithAmbientEnvironmentRefusesMalformedEntries(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"empty key":  {"": "x"},
		"equals key": {"A=B": "x"},
		"nul key":    {"A\x00B": "x"},
		"nul value":  {"A": "x\x00y"},
	} {
		t.Run(name, func(t *testing.T) {
			agent := NewAgent(WithAmbientEnvironment(env))
			t.Cleanup(func() { _ = agent.Close() })

			var reqErr *acp.RequestError
			require.ErrorAs(t, agent.optionErr, &reqErr)
			require.Equal(t, optionFieldAmbientEnvironment, optionFailureField(reqErr))
		})
	}
}
