package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type stubProcess struct {
	exited   chan struct{}
	waitErr  error
	stderr   string
	shutdown error
	kill     error
	close    error
}

func newStubProcess(exited bool) *stubProcess {
	process := &stubProcess{exited: make(chan struct{})}
	if exited {
		close(process.exited)
	}

	return process
}

func (*stubProcess) CloseStdin() error                { return nil }
func (p *stubProcess) Exited() <-chan struct{}        { return p.exited }
func (p *stubProcess) WaitErr() error                 { return p.waitErr }
func (p *stubProcess) StderrTail() string             { return p.stderr }
func (p *stubProcess) Shutdown(context.Context) error { return p.shutdown }
func (p *stubProcess) Kill() error                    { return p.kill }
func (p *stubProcess) Close() error                   { return p.close }

type directAgentClient struct {
	done      chan struct{}
	notifyErr error
	updateErr error
	notified  []map[string]any
	updates   []acp.SessionUpdate
}

type appendControlledStore struct {
	SessionStore
	mu       sync.Mutex
	failures int
	calls    int
	err      error
}

type errorSessionStore struct {
	SessionStore
	loadErr   error
	listErr   error
	deleteErr error
}

func (s *errorSessionStore) Load(context.Context, SessionKey) ([]SessionStoreEntry, error) {
	return nil, s.loadErr
}

func (s *errorSessionStore) ListSessions(context.Context) ([]SessionSummary, error) {
	return nil, s.listErr
}

func (s *errorSessionStore) Delete(context.Context, SessionKey) error {
	return s.deleteErr
}

func (s *appendControlledStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	s.mu.Lock()
	s.calls++
	if s.failures > 0 {
		s.failures--
		err := s.err
		s.mu.Unlock()

		return err
	}
	s.mu.Unlock()

	return s.SessionStore.Append(ctx, key, entries)
}

type stubPiClient struct {
	events       chan pi.Event
	uiRequests   chan pi.UIRequest
	done         chan struct{}
	err          error
	abortErr     error
	promptErr    error
	respondErr   error
	stats        pi.SessionStats
	statsErr     error
	model        pi.Model
	setModelErr  error
	thinkingErr  error
	startErr     error
	cloneCancel  bool
	cloneErr     error
	autoRetryErr error
	state        pi.SessionState
	stateErr     error
	models       []pi.Model
	modelsErr    error
	commands     []pi.SlashCommand
	commandsErr  error
}

func newStubPiClient() *stubPiClient {
	return &stubPiClient{events: make(chan pi.Event), uiRequests: make(chan pi.UIRequest), done: make(chan struct{})}
}

func (c *stubPiClient) Start(context.Context) error                             { return c.startErr }
func (c *stubPiClient) Events() <-chan pi.Event                                 { return c.events }
func (c *stubPiClient) UIRequests() <-chan pi.UIRequest                         { return c.uiRequests }
func (c *stubPiClient) Done() <-chan struct{}                                   { return c.done }
func (c *stubPiClient) Err() error                                              { return c.err }
func (c *stubPiClient) RespondUI(pi.UIResponse) error                           { return c.respondErr }
func (c *stubPiClient) Prompt(context.Context, string, []pi.ImageContent) error { return c.promptErr }
func (c *stubPiClient) Abort(context.Context) error                             { return c.abortErr }
func (c *stubPiClient) Clone(context.Context) (bool, error)                     { return c.cloneCancel, c.cloneErr }
func (c *stubPiClient) GetState(context.Context) (pi.SessionState, error) {
	return c.state, c.stateErr
}
func (c *stubPiClient) GetAvailableModels(context.Context) ([]pi.Model, error) {
	return c.models, c.modelsErr
}
func (c *stubPiClient) SetModel(context.Context, string, string) (pi.Model, error) {
	return c.model, c.setModelErr
}
func (c *stubPiClient) SetThinkingLevel(context.Context, string) error { return c.thinkingErr }
func (c *stubPiClient) SetAutoRetry(context.Context, bool) error       { return c.autoRetryErr }
func (c *stubPiClient) GetSessionStats(context.Context) (pi.SessionStats, error) {
	return c.stats, c.statsErr
}
func (c *stubPiClient) GetCommands(context.Context) ([]pi.SlashCommand, error) {
	return c.commands, c.commandsErr
}

func newDirectAgentClient() *directAgentClient {
	return &directAgentClient{done: make(chan struct{})}
}

func (c *directAgentClient) Done() <-chan struct{} { return c.done }
func (*directAgentClient) CreateElicitation(
	context.Context,
	acp.UnstableCreateElicitationRequest,
	elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return acp.UnstableCreateElicitationResponse{}, nil
}
func (*directAgentClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}
func (c *directAgentClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.updates = append(c.updates, notification.Update)

	return c.updateErr
}
func (c *directAgentClient) NotifyExtension(_ context.Context, _ string, value any) error {
	if payload, ok := value.(map[string]any); ok {
		c.notified = append(c.notified, payload)
	}

	return c.notifyErr
}

func TestAgentDirectSurfaceAndVersionChecks(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.Authenticate(t.Context(), acp.AuthenticateRequest{MethodId: "native"})
	requireInvalidParams(t, err)
	_, err = agent.Logout(t.Context(), acp.LogoutRequest{})
	require.NoError(t, err)
	_, err = agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{})
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)

	agent.lookPath = func(string) (string, error) { return "/fake/pi", nil }
	agent.probeVersion = func(context.Context, string) (string, error) { return pi.DefaultMinimumVersion, nil }
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.NoError(t, agent.ensureVersion(t.Context()))

	missing := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	missing.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	require.Error(t, missing.ensureVersion(t.Context()))
	probe := NewAgent(WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	probe.probeVersion = func(context.Context, string) (string, error) { return "", errors.New("probe") }
	require.Error(t, probe.ensureVersion(t.Context()))
	old := NewAgent(WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	old.probeVersion = func(context.Context, string) (string, error) { return "0.1.0", nil }
	require.Error(t, old.ensureVersion(t.Context()))

	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorIs(t, err, errAgentClosed)
	require.NoError(t, agent.Close())
}

func TestAgentConcurrencyAndErrors(t *testing.T) {
	require.NoError(t, validateConcurrencyLimits(ConcurrencyLimits{}))
	require.Error(t, validateConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}))
	require.Error(t, validateConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: -1}))

	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2, MaxConcurrentClientCalls: 1}))
	require.Equal(t, 2, agent.maxActiveSessions())
	require.Equal(t, 1, agent.maxConcurrentClientCalls())
	release, err := agent.acquireClientCall(t.Context())
	require.NoError(t, err)
	_, err = agent.acquireClientCall(t.Context())
	requireInvalidRequest(t, err)
	release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	agent.clientCalls <- struct{}{}
	_, err = agent.acquireClientCall(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-agent.clientCalls

	invalid := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}))
	_, err = invalid.Initialize(t.Context(), defaultInitializeRequest())
	requireInvalidParams(t, err)
	require.Equal(t, defaultMaxActiveSessions, NewAgent().maxActiveSessions())
	require.Equal(t, defaultMaxConcurrentClientCalls, NewAgent().maxConcurrentClientCalls())
	require.Equal(t, 5*time.Second, NewAgent(WithTurnTimeout(5*time.Second)).turnTimeout())
}

func requireInvalidRequest(t *testing.T, err error) {
	t.Helper()
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32600, requestError.Code)
}

func TestAgentConnectionAndErrorMapping(t *testing.T) {
	_, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{}, elicitationScope{})
	require.Error(t, err)
	params := acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{
		Message: "message", Mode: elicitationModeForm,
		RequestedSchema: acp.UnstableElicitationSchema{Type: acp.UnstableElicitationSchemaTypeObject},
		Meta:            map[string]any{"x": true},
	}}
	raw, err := scopedElicitationParams(params, elicitationScope{SessionID: "session", ToolCallID: "tool"})
	require.NoError(t, err)
	require.Contains(t, string(raw), "toolCallId")

	require.Nil(t, requestError(nil))
	requestErr := acp.NewInvalidParams(nil)
	require.Same(t, requestErr, requestError(requestErr))
	require.Equal(t, -32800, requestError(context.Canceled).Code)
	require.Equal(t, -32603, requestError(errors.New("failure")).Code)
	require.Same(t, requestErr, lifecycleMetaError(requestErr))
	var lifecycleRequestError *acp.RequestError
	require.ErrorAs(t, lifecycleMetaError(errors.New("bad meta")), &lifecycleRequestError)
	require.Equal(t, -32602, lifecycleRequestError.Code)
}

func TestNativeFailureClassification(t *testing.T) {
	previousGrace := processExitClassifyGrace
	processExitClassifyGrace = time.Millisecond
	t.Cleanup(func() { processExitClassifyGrace = previousGrace })

	session := &agentSession{}
	require.NoError(t, session.nativeTurnFailure(nil))
	requirePiTurnFailure(t, session.nativeTurnFailure(&pi.CommandError{Message: "provider"}), failureCauseProvider)
	requirePiTurnFailure(t, session.nativeTurnFailure(io.EOF), failureCauseTransport)

	process := newStubProcess(true)
	process.waitErr = errors.New("exit 2")
	process.stderr = " stderr "
	session.proc = process
	data := requirePiTurnFailure(t, session.nativeTurnFailure(io.EOF), failureCauseProcessExit)
	require.Contains(t, data[jsonFieldMessage], "stderr")

	process = newStubProcess(false)
	session.proc = process
	_, exited := session.processExitMessage()
	require.False(t, exited)

	requirePiTurnFailure(t, providerTurnFailure(&promptTurnState{stopReason: stopReasonError}), failureCauseProvider)
	spawn := spawnFailureError(errors.New("start"), nil)
	require.Error(t, spawn)
	process = newStubProcess(true)
	process.waitErr = errors.New("exit")
	process.stderr = "tail"
	spawn = spawnFailureError(errors.New("start"), process)
	require.Contains(t, spawn.Error(), "Internal error")
}

func TestRawEventSixCases(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	first := &agentSession{agent: agent, id: "first", rawMessages: rawMessageConfig{All: true}}
	second := &agentSession{agent: agent, id: "second", rawMessages: rawMessageConfig{All: true}}

	first.emitRawPiEvent(t.Context(), []byte(`{"type":"one"}`))
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"two"}`))
	second.emitRawPiEvent(t.Context(), []byte(`{"type":"one"}`))
	require.Len(t, client.notified, 3)
	require.EqualValues(t, 1, client.notified[0][rawEventFieldSequence])
	require.EqualValues(t, 2, client.notified[1][rawEventFieldSequence])
	require.EqualValues(t, 1, client.notified[2][rawEventFieldSequence])

	first.emitRawPiEvent(t.Context(), []byte(`{"value":"`+strings.Repeat("x", rawEventMaxBytes)+`"}`))
	require.Equal(t, rawEventReasonOversize, anyMap(t, client.notified[3][rawEventFieldEvent])[rawEventFieldReason])
	first.emitRawPiEvent(t.Context(), []byte(`not-json`))
	require.Equal(t, rawEventReasonUnserializable, anyMap(t, client.notified[4][rawEventFieldEvent])[rawEventFieldReason])

	client.notifyErr = errors.New("emit")
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"still-success"}`))
	disabled := &agentSession{agent: agent, id: "disabled"}
	disabled.emitRawPiEvent(t.Context(), []byte(`{"type":"off"}`))
	first.emitRawPiEvent(t.Context(), nil)
	require.Len(t, client.notified, 6)

	agent.conn = nil
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"no-client"}`))
	agent.conn = client
	agent.closed = true
	first.emitRawPiEvent(t.Context(), []byte(`{"type":"closed"}`))
}

func TestSessionFilesystemHelpers(t *testing.T) {
	agent := NewAgent(WithHome(t.TempDir()))
	session := &agentSession{agent: agent, id: "01234567-89ab-cdef-0123-456789abcdef"}
	dirs, err := agent.createSessionDirs()
	require.NoError(t, err)
	session.sessionRoot = dirs.Root
	require.True(t, filepath.IsAbs(session.sessionRoot))
	entries := []SessionStoreEntry{json.RawMessage(`{"type":"session"}`), json.RawMessage(`{"type":"message"}`)}
	path, err := writeHydratedSessionFile(dirs, string(session.id), entries)
	require.NoError(t, err)
	session.sessionFilePath = path
	require.True(t, session.sessionFileExists())
	session.sessionFilePath = filepath.Join(t.TempDir(), "missing")
	require.False(t, session.sessionFileExists())
	session.sessionFilePath = ""
	require.False(t, session.sessionFileExists())
	require.NoError(t, session.removeSessionRoot())
	require.NoError(t, session.removeSessionRoot())

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	_, err = NewAgent(WithHome(file)).createSessionDirs()
	require.Error(t, err)
}

func TestSessionUpdateEmissionAndPoisoning(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	session := &agentSession{
		agent:             agent,
		id:                "id",
		availableCommands: []pi.SlashCommand{{Name: "one", Description: "first"}},
	}
	require.NoError(t, session.emitUpdates(t.Context(), nil))
	require.NoError(t, session.emitOptionalUpdates(t.Context(), nil))
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Len(t, client.updates, 1)
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Len(t, client.updates, 1)
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), true))
	require.Len(t, client.updates, 2)

	session.availableCommands = nil
	require.NoError(t, session.emitAvailableCommandsUpdate(t.Context(), false))
	require.Len(t, client.updates, 3)
	require.NoError(t, session.emitClearAvailableCommandsUpdate(t.Context()))
	require.Len(t, client.updates, 3)
	session.advertisedCommands = []acp.AvailableCommand{{Name: "one"}}
	require.NoError(t, session.emitClearAvailableCommandsUpdate(t.Context()))
	require.Len(t, client.updates, 4)
	require.Len(t, emptyAvailableCommandsUpdate(), 1)

	client.updateErr = errors.New("update")
	require.Error(t, session.emitUpdates(t.Context(), []acp.SessionUpdate{{}}))
	require.Error(t, session.emitOptionalUpdates(t.Context(), []acp.SessionUpdate{{}}))
	client.updateErr = nil
	agent.conn = nil
	require.ErrorIs(t, session.emitUpdates(t.Context(), []acp.SessionUpdate{{}}), errACPConnectionNotAttached)
	require.NoError(t, session.emitOptionalUpdates(t.Context(), []acp.SessionUpdate{{}}))
	agent.conn = client
	agent.closed = true
	require.ErrorIs(t, session.emitUpdates(t.Context(), []acp.SessionUpdate{{}}), errAgentClosed)
	require.NoError(t, session.emitOptionalUpdates(t.Context(), []acp.SessionUpdate{{}}))
	agent.closed = false

	cancelled := false
	session.cancel = func() { cancelled = true }
	session.advertisedCommands = []acp.AvailableCommand{{Name: "one"}}
	require.Error(t, session.poison(t.Context(), "broken"))
	require.True(t, cancelled)
	require.Error(t, session.poisonedError())
	require.Error(t, session.poison(t.Context(), "other"))
	require.Error(t, poisonedSessionError("broken"))
	nilAgent := &agentSession{}
	require.Error(t, nilAgent.poison(t.Context(), "broken"))
}

func TestSessionInfoAndAgentBookkeeping(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	client := newDirectAgentClient()
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "id", cwd: "/cwd", additionalDirectories: []string{"/extra"}, turn: make(chan struct{}, 1)}
	session.fingerprint = sessionStartFingerprint(sessionStart{})
	require.NoError(t, agent.storeStartedSession(t.Context(), session))
	require.Same(t, session, agent.activeSessionForStart("id", sessionStart{Cwd: ""}))
	require.Nil(t, agent.activeSessionForStart("missing", sessionStart{}))
	require.Equal(t, "", session.currentProvider())
	session.model = "provider/model"
	require.Equal(t, "provider", session.currentProvider())

	info := session.sessionInfo("id")
	require.Equal(t, "/cwd", info.Cwd)
	require.Equal(t, "id", *info.Title)
	require.NoError(t, session.emitLiveSessionInfoUpdate(t.Context(), []acp.ContentBlock{acp.TextBlock(" title ")}))
	info = session.sessionInfo("id")
	require.Equal(t, "title", *info.Title)
	require.NotNil(t, info.UpdatedAt)

	other := &agentSession{agent: agent, id: "other", turn: make(chan struct{}, 1)}
	err := agent.storeStartedSession(t.Context(), other)
	requireInvalidRequest(t, err)
	require.NoError(t, agent.storeStartedSession(t.Context(), session))
	agent.removeSession(t.Context(), "missing", nil)
	agent.removeSession(t.Context(), "id", session)
	require.Nil(t, agent.sessions["id"])

	agent.closed = true
	closedSession := &agentSession{agent: agent, id: "closed", turn: make(chan struct{}, 1)}
	require.ErrorIs(t, agent.storeStartedSession(t.Context(), closedSession), errAgentClosed)
}

func TestSessionFingerprintAndMCPNames(t *testing.T) {
	servers := []acp.McpServer{
		HTTPMCPServer("http", "http://example.test", nil),
		{Sse: &acp.McpServerSseInline{Name: "sse"}},
		{Acp: &acp.McpServerAcpInline{Name: "acp"}},
		StdioMCPServer("stdio", "cmd", nil, nil),
		{},
	}
	for index, want := range []string{"http", "sse", "acp", "stdio", ""} {
		require.Equal(t, want, mcpServerName(servers[index]))
	}
	left := sessionStart{Cwd: "/cwd", McpServers: servers}
	right := sessionStart{Cwd: "/cwd", McpServers: []acp.McpServer{servers[4], servers[3], servers[2], servers[1], servers[0]}}
	require.Equal(t, sessionStartFingerprint(left), sessionStartFingerprint(right))
}

func TestSessionTurnLifecycleBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	_, err = session.acquireTurn(t.Context())
	requireInvalidRequest(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = session.acquireTurn(ctx)
	require.ErrorIs(t, err, context.Canceled)
	release()

	require.NoError(t, session.ensureProcessAlive(t.Context()))
	noProcess := &agentSession{}
	require.Error(t, noProcess.ensureProcessAlive(t.Context()))

	require.NoError(t, session.Cancel(t.Context()))
	client.abortErr = errors.New("abort")
	require.Error(t, session.Cancel(t.Context()))
	noClient := &agentSession{}
	require.NoError(t, noClient.Cancel(t.Context()))

	cancelled := 0
	session.cancel = func() { cancelled++ }
	dialogCtx, cancelDialog := context.WithCancel(context.Background())
	session.pendingDialogs = map[string]*dialogCancel{"dialog": {cancel: cancelDialog}}
	session.cancelPendingInteractions()
	require.True(t, session.wasTurnCancelled())
	require.Empty(t, session.pendingDialogs)
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)

	process := newStubProcess(false)
	process.shutdown = errors.New("shutdown")
	process.close = errors.New("close")
	closing := &agentSession{agent: agent, proc: process, cancel: func() { cancelled++ }, closeTurnWait: time.Millisecond, sessionRoot: t.TempDir()}
	require.Error(t, closing.Close(t.Context()))
	require.Equal(t, 1, cancelled)
	require.Equal(t, defaultSessionCloseTurnWait, (&agentSession{}).closeTurnTimeout())
}

func TestTurnEventAndUsageBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false), contextWindowSize: 123}
	state := &promptTurnState{}

	settled, err := session.handleTurnEvent(t.Context(), pi.AgentSettledEvent{}, state)
	require.NoError(t, err)
	require.True(t, settled)
	_, err = session.handleTurnEvent(t.Context(), pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant, Model: "m", Provider: "p"}}, state)
	require.NoError(t, err)
	require.Equal(t, "m", state.model)

	for _, delta := range []pi.AssistantMessageEvent{
		{Type: assistantEventTextDelta, Delta: "text"},
		{Type: assistantEventTextDelta},
		{Type: assistantEventThinkingDelta, Delta: "thought"},
		{Type: assistantEventThinkingDelta},
		{Type: "unknown", Delta: "ignored"},
	} {
		_, err = session.handleTurnEvent(t.Context(), pi.MessageUpdateEvent{AssistantMessageEvent: delta}, state)
		require.NoError(t, err)
	}

	usage := &pi.Usage{Input: 1, Output: 2, CacheRead: 3, CacheWrite: 4, Cost: &pi.UsageCost{Total: 0.5}}
	_, err = session.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role: messageRoleAssistant, Model: "model", Provider: "provider", StopReason: stopReasonStop,
		ErrorMessage: "error", Usage: usage,
	}}, state)
	require.NoError(t, err)
	require.Equal(t, 10, state.usage.TotalTokens)
	mergeTurnUsage(state.usage, nil)
	mergeTurnUsage(state.usage, &pi.Usage{Input: 2})
	require.Equal(t, 12, state.usage.TotalTokens)

	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionStartEvent{ToolCallID: "call", ToolName: "bash", Args: json.RawMessage(`{"x":true}`)}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionUpdateEvent{ToolCallID: "call"}, state)
	require.NoError(t, err)
	result := &pi.ToolResult{Content: []pi.ContentBlock{{Type: contentBlockTypeText, Text: "output"}}}
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionUpdateEvent{ToolCallID: "call", PartialResult: result}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{ToolCallID: "call", IsError: true, Result: result}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.ToolExecutionEndEvent{ToolCallID: "call"}, state)
	require.NoError(t, err)
	_, err = session.handleTurnEvent(t.Context(), pi.AgentStartEvent{}, state)
	require.NoError(t, err)

	require.EqualValues(t, 123, session.currentModelContextWindow())
	client.statsErr = errors.New("stats")
	require.Nil(t, session.settledSessionStats(t.Context()))
	client.statsErr = nil
	tokens := int64(7)
	client.stats = pi.SessionStats{ContextUsage: &pi.ContextUsage{Tokens: &tokens, ContextWindow: 456}}
	require.NotNil(t, session.settledSessionStats(t.Context()))
	session.emitTurnUsageUpdate(t.Context(), &promptTurnState{}, nil)
	session.emitTurnUsageUpdate(t.Context(), state, &client.stats)
	require.NotEmpty(t, connection.updates)
}

func TestTurnTerminationBranches(t *testing.T) {
	previousGrace := processExitClassifyGrace
	processExitClassifyGrace = time.Millisecond
	t.Cleanup(func() { processExitClassifyGrace = previousGrace })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	client := newStubPiClient()
	session := &agentSession{agent: agent, client: client, proc: newStubProcess(false)}
	var timedOut atomic.Bool
	messageID := "message"

	response, err := session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTransport)
	require.Empty(t, response.StopReason)
	client.err = errors.New("native stream")
	_, err = session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTransport)

	timedOut.Store(true)
	_, err = session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTimeout)
	_, err = session.contextEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseTimeout)
	timedOut.Store(false)
	response, err = session.contextEndedTurn(&messageID, &timedOut)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	session.turnCancelled = true
	response, err = session.transportEndedTurn(&messageID, &timedOut)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	response, err = session.contextEndedTurn(&messageID, &timedOut)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	session.turnCancelled = false
	session.proc = newStubProcess(true)
	_, err = session.transportEndedTurn(&messageID, &timedOut)
	requirePiTurnFailure(t, err, failureCauseProcessExit)

	original := errors.New("emit")
	require.ErrorIs(t, session.abortAfterEmitError(t.Context(), original), original)
	session.client = nil
	require.ErrorIs(t, session.abortAfterEmitError(t.Context(), original), original)
}

func TestNativeSessionSetupBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	newSession := func(client *stubPiClient) *agentSession {
		return &agentSession{agent: agent, client: client, proc: newStubProcess(false)}
	}
	baseClient := func() *stubPiClient {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id", SessionFile: "/session", ThinkingLevel: "off"}

		return client
	}

	client := baseClient()
	session := newSession(client)
	require.NoError(t, agent.setUpNativeSession(t.Context(), session, sessionStart{}, pi.ModelRef{}, false))
	require.Equal(t, acp.SessionId("id"), session.id)

	client = baseClient()
	client.cloneErr = &pi.CommandError{Message: "Entry null not found"}
	requireInvalidParams(t, agent.setUpNativeSession(t.Context(), newSession(client), sessionStart{ForkSession: true}, pi.ModelRef{}, false))
	client = baseClient()
	client.cloneErr = errors.New("clone")
	require.Error(t, agent.setUpNativeSession(t.Context(), newSession(client), sessionStart{ForkSession: true}, pi.ModelRef{}, false))
	client = baseClient()
	client.cloneCancel = true
	require.Error(t, agent.setUpNativeSession(t.Context(), newSession(client), sessionStart{ForkSession: true}, pi.ModelRef{}, false))

	for _, configure := range []func(*stubPiClient){
		func(c *stubPiClient) { c.autoRetryErr = errors.New("retry") },
		func(c *stubPiClient) { c.stateErr = errors.New("state") },
		func(c *stubPiClient) { c.thinkingErr = errors.New("thinking") },
		func(c *stubPiClient) { c.modelsErr = errors.New("models") },
		func(c *stubPiClient) { c.commandsErr = errors.New("commands") },
	} {
		client = baseClient()
		configure(client)
		start := sessionStart{}
		if client.thinkingErr != nil {
			start.MetaOptions.ThinkingLevel = "high"
		}
		require.Error(t, agent.setUpNativeSession(t.Context(), newSession(client), start, pi.ModelRef{}, false))
	}

	client = baseClient()
	client.state.SessionID = "different"
	require.Error(t, agent.setUpNativeSession(t.Context(), newSession(client), sessionStart{ResumeID: "expected", HydrateEntries: []SessionStoreEntry{json.RawMessage(`{}`)}}, pi.ModelRef{}, false))

	client = baseClient()
	client.setModelErr = &pi.CommandError{Message: "missing model"}
	requireInvalidParams(t, agent.setUpNativeSession(t.Context(), newSession(client), sessionStart{}, pi.ModelRef{Provider: "p", ID: "m"}, true))
	client = baseClient()
	client.setModelErr = errors.New("set model")
	require.Error(t, agent.setUpNativeSession(t.Context(), newSession(client), sessionStart{}, pi.ModelRef{Provider: "p", ID: "m"}, true))

	client = baseClient()
	client.state.Model = &pi.Model{Provider: "p", ID: "state", ContextWindow: 10}
	client.model = pi.Model{ID: "selected", ContextWindow: 20}
	client.models = []pi.Model{{Provider: "p", ID: "selected"}}
	client.commands = []pi.SlashCommand{{Name: "command"}}
	session = newSession(client)
	require.NoError(t, agent.setUpNativeSession(t.Context(), session, sessionStart{MetaOptions: PiOptions{ThinkingLevel: "high"}}, pi.ModelRef{Provider: "p", ID: "requested"}, true))
	require.Equal(t, "p/selected", session.model)
	require.EqualValues(t, 20, session.contextWindowSize)
	require.Len(t, session.availableModels, 1)
	require.Len(t, session.availableCommands, 1)

	require.Empty(t, stateModelRef(pi.SessionState{}))
	require.Empty(t, stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "", ID: "id"}}))
	require.Empty(t, stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "unknown", ID: "unknown"}}))
	require.Equal(t, "p/m", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "p", ID: "m"}}))

	ref, has, err := agent.resolveInitialModel(PiOptions{})
	require.NoError(t, err)
	require.False(t, has)
	require.Empty(t, ref)
	defaultAgent := NewAgent(WithDefaultModel("p/default"))
	ref, has, err = defaultAgent.resolveInitialModel(PiOptions{})
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, "p/default", ref.String())
	_, _, err = agent.resolveInitialModel(PiOptions{Model: "invalid"})
	requireInvalidParams(t, err)
}

func TestCurrentUsageAndListPaginationHelpers(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStoreLoadTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client}

	client.statsErr = errors.New("stats")
	session.emitCurrentUsageUpdate(t.Context())
	client.statsErr = nil
	client.stats = pi.SessionStats{}
	session.emitCurrentUsageUpdate(t.Context())
	client.stats.ContextUsage = &pi.ContextUsage{}
	session.emitCurrentUsageUpdate(t.Context())
	tokens := int64(3)
	client.stats.ContextUsage = &pi.ContextUsage{Tokens: &tokens, ContextWindow: 100}
	session.emitCurrentUsageUpdate(t.Context())
	require.EqualValues(t, 100, session.contextWindowSize)
	require.NotEmpty(t, connection.updates)
	require.Equal(t, time.Second, agent.sessionStoreLoadTimeout())
	require.Equal(t, defaultSessionStoreLoadTimeout, NewAgent().sessionStoreLoadTimeout())

	infos := make([]acp.SessionInfo, 51)
	for index := range infos {
		infos[index] = acp.SessionInfo{SessionId: acp.SessionId(fmt.Sprintf("id-%02d", index))}
	}
	page, cursor, err := paginateSessionInfos(infos, nil)
	require.NoError(t, err)
	require.Len(t, page, listSessionsPageSize)
	require.NotNil(t, cursor)
	decoded, err := decodeListCursor(cursor)
	require.NoError(t, err)
	require.Equal(t, listSessionsPageSize, decoded)
	page, cursor, err = paginateSessionInfos(infos, cursor)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Nil(t, cursor)
	past := encodeListCursor(len(infos) + 1)
	_, _, err = paginateSessionInfos(infos, &past)
	requireInvalidParams(t, err)
	decoded, err = decodeListCursor(acp.Ptr(""))
	require.NoError(t, err)
	require.Zero(t, decoded)
	_, err = decodeListCursor(acp.Ptr("%%%"))
	require.Error(t, err)
}

func TestStartSessionFailureBranches(t *testing.T) {
	baseAgent := func() *Agent {
		agent := NewAgent(WithExecutablePath("/fake/pi"), WithHome(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
		agent.probeVersion = func(context.Context, string) (string, error) { return pi.DefaultMinimumVersion, nil }

		return agent
	}

	agent := baseAgent()
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd", ResumeID: "id"})
	require.ErrorIs(t, err, errUnknownStoredSession)

	agent = baseAgent()
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, errors.New("spawn")
	}
	_, err = agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	agent = baseAgent()
	client := newStubPiClient()
	client.startErr = errors.New("client start")
	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}
	_, err = agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	agent = baseAgent()
	agent.options.SeedFiles = map[string]string{"../bad": "value"}
	_, err = agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	requireInvalidParams(t, err)

	agent = baseAgent()
	client = newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: filepath.Join(t.TempDir(), "native.jsonl")}
	process = newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}
	session, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd", MetaOptions: PiOptions{Permission: pi.PermissionModeAllow, Env: map[string]string{"KEY": "VALUE"}}})
	require.NoError(t, err)
	require.Equal(t, pi.PermissionModeAllow, session.permissionMode)
	require.NoError(t, session.Close(t.Context()))
}

func TestMaterializeFaultBranches(t *testing.T) {
	originalMkdirAll := materializeMkdirAll
	originalMkdirTemp := materializeMkdirTemp
	originalRemoveAll := materializeRemoveAll
	originalWriteFile := materializeWriteFile
	t.Cleanup(func() {
		materializeMkdirAll = originalMkdirAll
		materializeMkdirTemp = originalMkdirTemp
		materializeRemoveAll = originalRemoveAll
		materializeWriteFile = originalWriteFile
	})

	agent := NewAgent(WithHome(t.TempDir()))
	materializeMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
	_, err := agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirAll = originalMkdirAll
	materializeMkdirTemp = func(string, string) (string, error) { return "", errors.New("temp") }
	_, err = agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirTemp = originalMkdirTemp

	calls := 0
	materializeMkdirAll = func(path string, mode os.FileMode) error {
		calls++
		if calls == 2 {
			return errors.New("child")
		}

		return originalMkdirAll(path, mode)
	}
	_, err = agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirAll = originalMkdirAll

	dirs, err := agent.createSessionDirs()
	require.NoError(t, err)
	materializeWriteFile = func(string, []byte, os.FileMode) error { return errors.New("write") }
	_, err = writeHydratedSessionFile(dirs, "id", []SessionStoreEntry{nil, json.RawMessage(`{}`)})
	require.Error(t, err)
	materializeWriteFile = originalWriteFile

	session := &agentSession{sessionRoot: "root"}
	materializeRemoveAll = func(string) error { return errors.New("remove") }
	require.Error(t, session.removeSessionRoot())
}

func TestMirrorCommitAndRetryBranches(t *testing.T) {
	base := NewInMemorySessionStore()
	controlled := &appendControlledStore{SessionStore: base, failures: 1, err: errors.New("temporary")}
	agent := NewAgent(WithSessionStore(controlled), WithLogger(slog.New(slog.DiscardHandler)))
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(" \n{\"one\":1}\n\n{\"two\":2}\n"), 0o600))
	session := &agentSession{agent: agent, id: "id", sessionFilePath: path}
	require.NoError(t, session.commitMirror(t.Context()))
	require.Equal(t, 2, session.mirroredRows)
	require.GreaterOrEqual(t, controlled.calls, 2)
	require.NoError(t, session.commitMirror(t.Context()))

	session.sessionFilePath = filepath.Join(t.TempDir(), "missing")
	require.NoError(t, session.commitMirror(t.Context()))
	dir := t.TempDir()
	session.sessionFilePath = dir
	require.Error(t, session.commitMirror(t.Context()))
	session.sessionFilePath = ""
	require.NoError(t, session.commitMirror(t.Context()))

	controlled = &appendControlledStore{SessionStore: base, failures: 10, err: errors.New("persistent")}
	agent.options.SessionStore = controlled
	session.sessionFilePath = path
	session.mirroredRows = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, session.commitMirror(ctx))

	previousTimeout := sessionMirrorAppendTimeout
	sessionMirrorAppendTimeout = time.Nanosecond
	t.Cleanup(func() { sessionMirrorAppendTimeout = previousTimeout })
	require.Error(t, appendMirrorEntries(t.Context(), controlled, SessionKey{SessionID: "id"}, []SessionStoreEntry{json.RawMessage(`{}`)}))
	require.Len(t, splitJSONLRows([]byte("\n a \n\n b\n")), 2)
}

func TestAgentSessionLifecycleErrorBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.NewSession(t.Context(), acp.NewSessionRequest{Cwd: "/cwd", Meta: map[string]any{piMetaKey: "bad"}})
	requireInvalidParams(t, err)
	_, err = agent.NewSession(t.Context(), acp.NewSessionRequest{Cwd: "relative"})
	requireInvalidParams(t, err)
	agent.closed = true
	_, err = agent.NewSession(t.Context(), NewSessionRequest("/cwd"))
	require.ErrorIs(t, err, errAgentClosed)
	agent.closed = false

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "missing"})
	requireInvalidParams(t, err)
	err = agent.Cancel(t.Context(), acp.CancelNotification{SessionId: "missing"})
	requireInvalidParams(t, err)

	storeErr := errors.New("store")
	errorStore := &errorSessionStore{SessionStore: NewInMemorySessionStore(), loadErr: storeErr, listErr: storeErr, deleteErr: storeErr}
	agent.options.SessionStore = errorStore
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("id", "/cwd"))
	require.Error(t, err)
	_, err = agent.ListSessions(t.Context(), ListSessionsRequest())
	require.Error(t, err)
	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest("id"))
	require.Error(t, err)

	agent.options.SessionStore = NewInMemorySessionStore()
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("id", "relative"))
	requireInvalidParams(t, err)
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("id", "/cwd", WithSessionMeta(map[string]any{piMetaKey: "bad"})))
	requireInvalidParams(t, err)
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("id", "/cwd"))
	requireInvalidParams(t, err)
	agent.deleted["deleted"] = struct{}{}
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("deleted", "/cwd"))
	requireInvalidParams(t, err)

	_, err = agent.ListSessions(t.Context(), ListSessionsRequest(WithListSessionsCwd("relative")))
	requireInvalidParams(t, err)
}

func TestRestoreActiveAndCleanupBranches(t *testing.T) {
	store := NewInMemorySessionStore()
	id := acp.SessionId("01234567-89ab-cdef-0123-456789abcdef")
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"01234567-89ab-cdef-0123-456789abcdef","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"history"}]`)}),
	}
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: string(id)}, entries))
	agent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	start := sessionStart{Cwd: "/cwd", ResumeID: string(id)}
	active := &agentSession{agent: agent, id: id, cwd: "/cwd", fingerprint: sessionStartFingerprint(start), turn: make(chan struct{}, 1)}
	agent.sessions[id] = active

	session, loaded, started, err := agent.restoreSession(t.Context(), id, start, nil)
	require.NoError(t, err)
	require.Same(t, active, session)
	require.Equal(t, entries, loaded)
	require.False(t, started)

	connection := newDirectAgentClient()
	connection.updateErr = errors.New("replay")
	agent.setConnection(connection)
	_, err = agent.LoadSession(t.Context(), LoadSessionRequest(id, "/cwd"))
	require.Error(t, err)
	require.Contains(t, agent.sessions, id)

	process := newStubProcess(false)
	process.shutdown = errors.New("shutdown")
	active.proc = process
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: id})
	require.Error(t, err)
	require.NotContains(t, agent.sessions, id)

	cleanup := &agentSession{agent: agent, id: id, proc: process, turn: make(chan struct{}, 1)}
	agent.sessions[id] = cleanup
	agent.options.SessionStore = store
	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
	require.Error(t, err)
}

func TestListStoredSessionFiltering(t *testing.T) {
	store := NewInMemorySessionStore()
	valid := "01234567-89ab-cdef-0123-456789abcdef"
	other := "11234567-89ab-cdef-0123-456789abcdef"
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: valid}, []SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/one"}`)}))
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: other}, []SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/two"}`)}))
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "invalid"}, []SessionStoreEntry{json.RawMessage(`{}`)}))
	agent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	agent.deleted[acp.SessionId(other)] = struct{}{}
	cwd := "/one"
	infos, err := agent.listStoreSessions(t.Context(), acp.ListSessionsRequest{Cwd: &cwd})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Equal(t, acp.SessionId(valid), infos[0].SessionId)

	active := &agentSession{agent: agent, id: acp.SessionId(valid), cwd: "/one"}
	agent.sessions[acp.SessionId(valid)] = active
	response, err := agent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, response.Sessions, 1)
}

func TestPromptAndFinishTurnErrorBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	client.stats = pi.SessionStats{SessionID: "id"}
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	session.poisonCause = "poisoned"
	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "hello"))
	require.Error(t, err)
	session.poisonCause = ""
	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	_, err = session.Prompt(t.Context(), TextPromptRequest("id", "hello"))
	requireInvalidRequest(t, err)
	release()
	_, err = session.Prompt(t.Context(), PromptRequest("id"))
	requireInvalidParams(t, err)
	session.proc = nil
	_, err = session.Prompt(t.Context(), TextPromptRequest("id", "hello"))
	requirePiTurnFailure(t, err, failureCauseTransport)

	finish := func(state *promptTurnState, timedOut bool) (acp.PromptResponse, error) {
		var timeout atomic.Bool
		timeout.Store(timedOut)

		return session.finishTurn(t.Context(), t.Context(), TextPromptRequest("id", "title"), state, &timeout)
	}
	session.proc = newStubProcess(false)
	client.stats = pi.SessionStats{SessionID: "other"}
	_, err = finish(&promptTurnState{}, false)
	require.Error(t, err)
	session.poisonCause = ""
	client.stats = pi.SessionStats{SessionID: "id"}
	connection.updateErr = errors.New("update")
	_, err = finish(&promptTurnState{}, false)
	require.Error(t, err)
	connection.updateErr = nil
	session.turnCancelled = true
	response, err := finish(&promptTurnState{}, false)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	session.turnCancelled = false
	_, err = finish(&promptTurnState{}, true)
	requirePiTurnFailure(t, err, failureCauseTimeout)
	_, err = finish(&promptTurnState{stopReason: stopReasonError, errorMessage: "provider"}, false)
	requirePiTurnFailure(t, err, failureCauseProvider)
	response, err = finish(&promptTurnState{stopReason: stopReasonStop}, false)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
}

func TestPumpDeliveryBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}
	event := pi.AgentStartEvent{}
	session.dispatchEvent(t.Context(), event)

	sink := newTurnSink()
	session.turnSink = sink
	close(sink.done)
	session.dispatchEvent(t.Context(), event)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink = newTurnSink()
	session.turnSink = sink
	session.dispatchEvent(ctx, event)

	session.turnSink = nil
	session.dispatchUIRequest(t.Context(), pi.UIRequest{Method: "notify"})
	session.turnSink = newTurnSink()
	session.dispatchUIRequest(t.Context(), pi.UIRequest{Method: "notify"})

	client := newStubPiClient()
	close(client.events)
	close(client.uiRequests)
	done := make(chan struct{})
	session.pump(t.Context(), client, done)
	select {
	case <-done:
	default:
		t.Fatal("pump did not finish")
	}
}

func TestReplayInvalidMetadataRows(t *testing.T) {
	rows := []SessionStoreEntry{
		json.RawMessage(`{"type":"message","message":"bad"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`[]`)}),
		json.RawMessage(`{"type":"message","message":{"role":"user","content":{}}}`),
	}
	require.Empty(t, replayUpdates(rows))
	require.Equal(t, "fallback", storeSessionTitle("fallback", rows))
}

func TestRelaunchProcessBranches(t *testing.T) {
	base := func(client *stubPiClient) (*agentSession, *Agent) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		old := newStubProcess(true)
		session := &agentSession{agent: agent, id: "id", proc: old, client: newStubPiClient(), sessionFilePath: filepath.Join(t.TempDir(), "missing"), mirroredRows: 5}
		relaunched := newStubProcess(false)
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return relaunched, client, nil
		}

		return session, agent
	}

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/fresh"}
	session, _ := base(client)
	require.NoError(t, session.ensureProcessAlive(t.Context()))
	require.Equal(t, "/fresh", session.sessionFilePath)
	require.Zero(t, session.mirroredRows)

	client = newStubPiClient()
	session, agent := base(client)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, errors.New("relaunch")
	}
	require.Error(t, session.ensureProcessAlive(t.Context()))

	client = newStubPiClient()
	client.startErr = errors.New("start")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.autoRetryErr = errors.New("retry")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.stateErr = errors.New("state")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.state.SessionID = "other"
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))

	client = newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/hydrated"}
	session, _ = base(client)
	file := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(file, []byte(`{}`), 0o600))
	session.sessionFilePath = file
	require.NoError(t, session.ensureProcessAlive(t.Context()))
	require.Equal(t, "/hydrated", session.sessionFilePath)
}

func TestRemainingStoreReplayAndUpdateBranches(t *testing.T) {
	store := &InMemorySessionStore{
		entries: map[SessionKey][]SessionStoreEntry{
			{SessionID: "b"}: {json.RawMessage(`{}`)},
			{SessionID: "a"}: {json.RawMessage(`{}`)},
		},
		updatedAt: map[SessionKey]int64{},
		tombstone: map[SessionKey]struct{}{},
	}
	summaries, err := store.ListSessions(t.Context())
	require.NoError(t, err)
	require.Equal(t, "a", summaries[0].SessionID)

	rows := []SessionStoreEntry{
		json.RawMessage(`{"type":"message","message":{"role":"assistant","content":[]}}`),
		json.RawMessage(`{"type":"message","message":{"role":"user","content":{}}}`),
		json.RawMessage(`{"type":"message","message":{"role":"tool","content":[]}}`),
	}
	require.Equal(t, "fallback", storeSessionTitle("fallback", rows))

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newDirectAgentClient()
	agent.setConnection(client)
	session := &agentSession{agent: agent, id: "id", advertisedCommands: []acp.AvailableCommand{{Name: "one"}}}
	client.updateErr = errors.New("clear")
	require.Error(t, session.emitClearAvailableCommandsUpdate(t.Context()))
	require.Error(t, session.poison(t.Context(), "broken"))
	require.Equal(t, "text", liveSessionTitleFromPrompt([]acp.ContentBlock{{}, acp.TextBlock("text")}))
}

func TestStartRealPiProcessRejectsEmptySpec(t *testing.T) {
	_, _, err := startRealPiProcess(t.Context(), pi.LaunchSpec{})
	require.Error(t, err)
}

func TestServeContextAndConnectionBranches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, Serve(ctx, strings.NewReader(""), io.Discard), context.Canceled)

	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })

	newServeAgent = func(opts ...Option) *Agent {
		agent := NewAgent(append(opts, WithLogger(slog.New(slog.DiscardHandler)))...)
		agent.sessions["serve"] = &agentSession{
			agent: agent,
			id:    "serve",
			proc:  newFailingCloseProcess(),
			turn:  make(chan struct{}, sessionTurnCapacity),
		}

		return agent
	}

	require.NoError(t, Serve(context.Background(), strings.NewReader(""), io.Discard))
}

func TestAgentCloseJoinsSessionCloseError(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.sessions["id"] = &agentSession{
		agent: agent,
		id:    "id",
		proc:  newFailingCloseProcess(),
		turn:  make(chan struct{}, sessionTurnCapacity),
	}
	require.Error(t, agent.Close())
}
