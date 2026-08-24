package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type configurationMutatingClient struct {
	*stubPiClient
	outbox *sessionOutbox
}

func (c *configurationMutatingClient) GetStateWithBoundary(
	ctx context.Context,
	boundary pi.CallBoundary,
) (pi.SessionState, error) {
	state, err := c.stubPiClient.GetStateWithBoundary(ctx, boundary)
	c.outbox.mu.Lock()
	c.outbox.state = outboxIdle
	c.outbox.mu.Unlock()

	return state, err
}

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
		outbox: newTestSessionOutbox(1),
	}
	agent.sessions[session.id] = session
	_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, "p/model"))
	require.ErrorContains(t, err, "update failed")

	native.setModelErr = errors.New("set model failed")
	require.ErrorContains(t, session.applyModelSelection(t.Context(), nil, native, "p/model"), "set model failed")

	native.thinkingErr = errors.New("set thinking failed")
	_, err = agent.SetSessionConfigOption(t.Context(),
		SetConfigOptionRequest(session.id, configThoughtLevel, pi.ThinkingLevelHigh))
	require.ErrorContains(
		t,
		err,
		"set thinking failed",
	)
	requireUnsupportedField(t, session.applyThinkingLevelSelection(t.Context(), nil, native, ""), acpFieldValue)
}

func TestConfigurationOwnershipFailureEdges(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	missingOutbox, missingClient, missingRelease, err := session.beginConfiguration(t.Context())
	require.ErrorIs(t, err, pi.ErrTransportClosed)
	require.Nil(t, missingOutbox)
	require.Nil(t, missingClient)
	require.Nil(t, missingRelease)

	outbox := newTestSessionOutbox(1)
	outbox.state = outboxPromptPending
	session.outbox = outbox
	busyOutbox, busyClient, busyRelease, err := session.beginConfiguration(t.Context())
	requireInvalidRequest(t, err)
	require.Nil(t, busyOutbox)
	require.Nil(t, busyClient)
	require.Nil(t, busyRelease)
	session.id = "busy-configuration"
	session.turn = make(chan struct{}, 1)
	session.agent.sessions[session.id] = session
	_, err = session.agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, "p/model"))
	requireInvalidRequest(t, err)

	require.Nil(t, session.configurationWriteBoundary(t.Context(), nil).BeforeDispatch)
	require.NoError(t, outbox.dispatchMu.lock(t.Context()))
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	boundary := session.configurationWriteBoundary(cancelled, outbox)
	_, err = boundary.BeforeDispatch()
	require.ErrorIs(t, err, context.Canceled)
	outbox.dispatchMu.Unlock()

	outbox.state = outboxConfiguring
	session.outbox = newTestSessionOutbox(2)
	boundary = session.configurationWriteBoundary(t.Context(), outbox)
	_, err = boundary.BeforeDispatch()
	require.ErrorIs(t, err, pi.ErrTransportClosed)

	session.outbox = outbox
	outbox.state = outboxIdle
	require.ErrorIs(t, session.configurationResultAllowed(t.Context(), outbox), pi.ErrTransportClosed)
	session.closing = true
	require.Error(t, session.configurationResultAllowed(t.Context(), outbox))

	session.closing = false
	outbox.state = outboxConfiguring
	mutating := &configurationMutatingClient{stubPiClient: newStubPiClient(), outbox: outbox}
	mutating.state = pi.SessionState{ThinkingLevel: pi.ThinkingLevelLow}
	require.ErrorIs(t,
		session.applyThinkingLevelSelection(t.Context(), outbox, mutating, pi.ThinkingLevelLow),
		pi.ErrTransportClosed,
	)
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
		agent: agent, id: "selection", client: native, turn: make(chan struct{}, 1), outbox: newTestSessionOutbox(1),
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

func TestConfigurationAdmissionOrdersAutonomousWork(t *testing.T) {
	newSession := func(t *testing.T) (*Agent, *agentSession, *stubPiClient, *sessionOutbox) {
		t.Helper()

		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		agent.setConnection(newDirectAgentClient())
		client := newStubPiClient()
		client.model = pi.Model{ID: "selected", ContextWindow: 100}
		outbox := newTestSessionOutbox(1)
		session := &agentSession{
			agent:  agent,
			id:     "configuration",
			client: client,
			turn:   make(chan struct{}, 1),
			outbox: outbox,
		}
		agent.sessions[session.id] = session

		return agent, session, client, outbox
	}

	t.Run("configuration wins", func(t *testing.T) {
		agent, session, client, outbox := newSession(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		client.setModelFunc = func(string, string) {
			close(entered)
			<-release
		}

		configured := make(chan error, 1)
		go func() {
			_, err := agent.SetSessionConfigOption(context.Background(), SetModelRequest(session.id, "p/model"))
			configured <- err
		}()

		<-entered
		queueAdmission := outbox.admit(pi.QueueUpdateEvent{Steering: []string{"queued"}})
		startAdmission := outbox.admit(pi.AgentStartEvent{})
		require.Equal(t, outboxQueued, queueAdmission.disposition)
		require.Equal(t, outboxQueued, startAdmission.disposition)

		outbox.mu.Lock()
		require.Equal(t, outboxConfiguring, outbox.state)
		require.Len(t, outbox.queued, 2)
		require.Nil(t, outbox.cycle)
		outbox.mu.Unlock()

		close(release)
		require.NoError(t, <-configured)

		session.drainOutbox(t.Context(), outbox)
		outbox.mu.Lock()
		require.Equal(t, outboxAgentCycle, outbox.state)
		require.NotNil(t, outbox.cycle)
		require.Empty(t, outbox.queued)
		outbox.mu.Unlock()
	})

	t.Run("agent start wins", func(t *testing.T) {
		agent, session, client, outbox := newSession(t)
		called := false
		client.setModelFunc = func(string, string) { called = true }

		session.routeNativeEvent(t.Context(), outbox, pi.AgentStartEvent{})
		_, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, "p/model"))
		requireInvalidRequest(t, err)
		require.False(t, called)

		outbox.mu.Lock()
		require.Equal(t, outboxAgentCycle, outbox.state)
		require.NotNil(t, outbox.cycle)
		outbox.mu.Unlock()
	})

	t.Run("queue update wins", func(t *testing.T) {
		agent, session, client, outbox := newSession(t)
		called := false
		client.setModelFunc = func(string, string) { called = true }

		session.routeNativeEvent(t.Context(), outbox, pi.QueueUpdateEvent{FollowUp: []string{"queued"}})
		_, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(session.id, "p/model"))
		requireInvalidRequest(t, err)
		require.False(t, called)

		outbox.mu.Lock()
		require.Equal(t, 1, outbox.nativeQueueDepth)
		outbox.mu.Unlock()
	})
}

func TestBlockedConfigurationOverflowAndCloseContainOneGeneration(t *testing.T) {
	originalWaitContext := sessionCloseTurnWaitContext
	closeJoiningHolder := make(chan struct{}, 1)
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		waitCtx, cancel := context.WithCancel(ctx)
		closeJoiningHolder <- struct{}{}

		return waitCtx, cancel
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWaitContext })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.setConnection(newDirectAgentClient())
	client := newStubPiClient()
	client.model = pi.Model{ID: "selected", ContextWindow: 100}
	responseBlocked := make(chan struct{})
	releaseResponse := make(chan struct{})
	client.setModelFunc = func(string, string) {
		close(responseBlocked)
		<-releaseResponse
	}

	process := newStubProcess(false)
	shutdownEntered := make(chan struct{})
	releaseShutdown := make(chan struct{})
	process.shutdownFunc = func(context.Context) error {
		close(shutdownEntered)
		<-releaseShutdown

		return nil
	}
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, client, nil, nil, nil)
	session := &agentSession{
		agent:       agent,
		id:          "configuration",
		client:      client,
		proc:        process,
		turn:        make(chan struct{}, 1),
		outbox:      outbox,
		sessionRoot: t.TempDir(),
	}
	agent.sessions[session.id] = session

	configured := make(chan error, 1)
	go func() {
		_, err := agent.SetSessionConfigOption(context.Background(), SetModelRequest(session.id, "p/model"))
		configured <- err
	}()
	<-responseBlocked

	for range outboxQueueCapacity + 2 {
		session.routeNativeEvent(t.Context(), outbox, pi.AgentStartEvent{})
	}
	<-shutdownEntered

	closed := make(chan error, 1)
	go func() { closed <- session.Close(context.Background()) }()
	close(releaseShutdown)
	<-closeJoiningHolder

	select {
	case err := <-closed:
		t.Fatalf("close passed a configuration holder whose response was blocked: %v", err)
	default:
	}

	close(releaseResponse)
	require.Error(t, <-configured)
	require.NoError(t, <-closed)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
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
		thinkingLevel: pi.ThinkingLevelLow, outbox: newTestSessionOutbox(1),
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
