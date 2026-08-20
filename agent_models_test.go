package piacp

import (
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestModelMetadataMapping(t *testing.T) {
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
	require.NotEmpty(t, piModelInfoMeta(&models[0]))

	// The per-model _meta.pi payload carries exactly identity and size
	// metadata: no capabilities array and no modality fields, even for a
	// vision-capable reasoning model.
	encodedMeta, err := json.Marshal(piModelInfoMeta(&models[0]))
	require.NoError(t, err)
	require.JSONEq(t,
		`{"pi":{"modelId":"p/one","contextWindow":100,"maxOutputTokens":10}}`,
		string(encodedMeta))

	encodedOptions, err := json.Marshal(modelSelectOptions("p/one", models))
	require.NoError(t, err)
	require.NotContains(t, string(encodedOptions), `"capabilities"`)
	require.NotContains(t, string(encodedOptions), `"input"`)

	for _, level := range pi.ThinkingLevels() {
		require.NotEmpty(t, thinkingLevelDisplayName(level))
	}
	require.Equal(t, "custom", thinkingLevelDisplayName("custom"))
	require.Len(t, thinkingLevelSelectOptions(), len(pi.ThinkingLevels()))
}

func TestConfigSelectionFailureBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{})
	requireUnsupportedField(t, err, acpFieldValue)

	_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest("missing", "p/model"))
	requireInvalidParams(t, err)

	poisoned := &agentSession{agent: agent, id: "poisoned", poisonCause: "broken"}
	agent.sessions[poisoned.id] = poisoned
	_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(poisoned.id, "p/model"))
	require.Error(t, err)

	busy := &agentSession{agent: agent, id: "busy", turn: make(chan struct{}, 1)}
	busy.turn <- struct{}{}
	agent.sessions[busy.id] = busy
	_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(busy.id, "p/model"))
	requireInvalidRequest(t, err)

	late := &agentSession{agent: agent, id: "late", turn: make(chan struct{}, 1)}
	agent.sessions[late.id] = late
	lateCtx := &poisonOnAdmissionContext{session: late}
	_, err = agent.SetSessionConfigOption(lateCtx, SetModelRequest(late.id, "p/model"))
	require.Error(t, err)

	updateClient := newDialogStubClient()
	updateClient.updateErr = errors.New("update failed")
	agent.setConnection(updateClient)
	native := newStubPiClient()
	native.model = pi.Model{ID: "selected", ContextWindow: 100}
	session := &agentSession{
		agent:  agent,
		id:     "selection",
		client: native,
		turn:   make(chan struct{}, 1),
	}
	agent.sessions[session.id] = session
	_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, "p/model"))
	require.ErrorContains(t, err, "update failed")

	native.setModelErr = errors.New("set model failed")
	require.ErrorContains(t, session.applyModelSelection(t.Context(), "p/model"), "set model failed")

	native.thinkingErr = errors.New("set thinking failed")
	_, err = agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, pi.ThinkingLevelHigh))
	require.ErrorContains(
		t,
		err,
		"set thinking failed",
	)
	requireUnsupportedField(t, session.applyThinkingLevelSelection(t.Context(), ""), acpFieldValue)
}

func TestConfigThinkingLevelPassesThroughToNative(t *testing.T) {
	const level = "registry-unknown"

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	native := newStubPiClient()
	var sent string
	native.thinkingFunc = func(value string) { sent = value }
	session := &agentSession{
		agent: agent, id: "selection", client: native, turn: make(chan struct{}, 1),
	}
	agent.sessions[session.id] = session

	response, err := agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, level))
	require.NoError(t, err)
	require.Equal(t, level, sent)
	require.Len(t, response.ConfigOptions, 1)
	require.NotNil(t, response.ConfigOptions[0].Select)
	require.Equal(t, acp.SessionConfigValueId(level), response.ConfigOptions[0].Select.CurrentValue)
	require.Len(t, *response.ConfigOptions[0].Select.Options.Ungrouped, len(pi.ThinkingLevels()))

	unstable := sessionUnstableConfigOptions(session)
	require.Len(t, unstable, 1)
	require.NotNil(t, unstable[0].Select)
	require.Equal(t, acp.SessionConfigValueId(level), unstable[0].Select.CurrentValue)
}
