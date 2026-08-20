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

// TestConfigThinkingLevelPassesThroughToNative pins both halves of the live-set
// door: the value the host names travels to pi unchanged, and what the session
// advertises afterwards is the level pi reports running — the retained one when
// pi acknowledged the request without adopting it.
func TestConfigThinkingLevelPassesThroughToNative(t *testing.T) {
	const unknownLevel = "registry-unknown"

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	native := newStubPiClient()
	native.state = pi.SessionState{ThinkingLevel: pi.ThinkingLevelLow}
	var sent string
	native.thinkingFunc = func(value string) { sent = value }
	session := &agentSession{
		agent: agent, id: "selection", client: native, turn: make(chan struct{}, 1),
	}
	agent.sessions[session.id] = session

	response, err := agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, unknownLevel))
	require.NoError(t, err)
	require.Equal(t, unknownLevel, sent, "the value still travels to pi unchanged")
	require.Len(t, response.ConfigOptions, 1)
	require.NotNil(t, response.ConfigOptions[0].Select)
	require.Equal(t, acp.SessionConfigValueId(pi.ThinkingLevelLow), response.ConfigOptions[0].Select.CurrentValue,
		"an acknowledged-but-unapplied value leaves the retained level advertised, not the echo")
	require.Len(t, *response.ConfigOptions[0].Select.Options.Ungrouped, len(pi.ThinkingLevels()))

	unstable := sessionUnstableConfigOptions(session)
	require.Len(t, unstable, 1)
	require.NotNil(t, unstable[0].Select)
	require.Equal(t, acp.SessionConfigValueId(pi.ThinkingLevelLow), unstable[0].Select.CurrentValue)

	// Whitespace is a level pi does not know, not an absent one: it is not
	// refused as empty, it travels, and the advert reports what pi kept.
	response, err = agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, " high "))
	require.NoError(t, err)
	require.Equal(t, " high ", sent)
	require.Equal(t, acp.SessionConfigValueId(pi.ThinkingLevelLow), response.ConfigOptions[0].Select.CurrentValue)

	response, err = agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, pi.ThinkingLevelMax))
	require.NoError(t, err)
	require.Equal(t, pi.ThinkingLevelMax, sent)
	require.Equal(t, acp.SessionConfigValueId(pi.ThinkingLevelMax), response.ConfigOptions[0].Select.CurrentValue,
		"a value pi adopts is advertised because pi reports it, not because the host asked for it")
}

// TestConfigThinkingLevelReadBackFailure pins that a set whose effective level
// cannot be read back fails rather than advertising a level the adapter cannot
// vouch for.
func TestConfigThinkingLevelReadBackFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	native := newStubPiClient()
	native.state = pi.SessionState{ThinkingLevel: pi.ThinkingLevelLow}
	native.stateErr = errors.New("read back failed")
	session := &agentSession{
		agent: agent, id: "selection", client: native, turn: make(chan struct{}, 1),
		thinkingLevel: pi.ThinkingLevelLow,
	}
	agent.sessions[session.id] = session

	_, err := agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, pi.ThinkingLevelMax))
	require.ErrorContains(t, err, "read back failed")

	session.mu.Lock()
	advertised := session.thinkingLevel
	session.mu.Unlock()
	require.Equal(t, pi.ThinkingLevelLow, advertised, "an unreadable set leaves the last proven level in place")
}
