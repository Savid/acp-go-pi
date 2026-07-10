package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	surfaceParentID = acp.SessionId("11111111-1111-4111-8111-111111111111")
	surfaceChildID  = "22222222-2222-4222-8222-222222222222"
)

type surfaceAgentClient struct {
	*directAgentClient

	elicitationResponse acp.UnstableCreateElicitationResponse
	elicitationErr      error
	permissionResponse  acp.RequestPermissionResponse
	permissionErr       error
}

func newSurfaceAgentClient() *surfaceAgentClient {
	return &surfaceAgentClient{directAgentClient: newDirectAgentClient()}
}

func (c *surfaceAgentClient) CreateElicitation(
	context.Context,
	acp.UnstableCreateElicitationRequest,
	elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.elicitationResponse, c.elicitationErr
}

func (c *surfaceAgentClient) RequestPermission(
	context.Context,
	acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	return c.permissionResponse, c.permissionErr
}

type surfaceSessionStore struct {
	*InMemorySessionStore

	appendErr error
	loadErr   error
}

func newSurfaceSessionStore() *surfaceSessionStore {
	return &surfaceSessionStore{InMemorySessionStore: NewInMemorySessionStore()}
}

func (s *surfaceSessionStore) Append(
	ctx context.Context,
	key SessionKey,
	entries []SessionStoreEntry,
) error {
	if s.appendErr != nil {
		return s.appendErr
	}

	return s.InMemorySessionStore.Append(ctx, key, entries)
}

func (s *surfaceSessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}

	return s.InMemorySessionStore.Load(ctx, key)
}

type poisonOnDoneContext struct {
	session *agentSession
	once    sync.Once
}

func (*poisonOnDoneContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (*poisonOnDoneContext) Err() error { return nil }

func (*poisonOnDoneContext) Value(any) any { return nil }

func (c *poisonOnDoneContext) Done() <-chan struct{} {
	c.once.Do(func() {
		c.session.mu.Lock()
		c.session.poisonCause = "late poison"
		c.session.mu.Unlock()
	})

	return nil
}

func surfaceForkRaw(t *testing.T, params acp.UnstableForkSessionRequest) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(params)
	require.NoError(t, err)

	return raw
}

func surfaceForkParams(t *testing.T) acp.UnstableForkSessionRequest {
	t.Helper()

	return ForkSessionRequest(surfaceParentID, t.TempDir())
}

func surfaceStoreRows(t *testing.T, store *surfaceSessionStore, entries ...SessionStoreEntry) {
	t.Helper()

	require.NoError(t, store.InMemorySessionStore.Append(
		t.Context(),
		SessionKey{SessionID: string(surfaceParentID)},
		entries,
	))
}

func TestForkExtensionValidationAndConversionBranches(t *testing.T) {
	converted := stableMCPServers(unstableMCPServersFromStable([]acp.McpServer{
		HTTPMCPServer("http", "https://example.test/mcp", map[string]string{"X": "Y"}),
		{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://example.test/sse"}},
		{Acp: &acp.McpServerAcpInline{Name: "acp", Id: "server"}},
		StdioMCPServer("stdio", "tool", []string{"serve"}, map[string]string{"A": "B"}),
		{},
	}))
	require.Len(t, converted, 5)
	require.NotNil(t, converted[0].Http)
	require.NotNil(t, converted[1].Sse)
	require.NotNil(t, converted[2].Acp)
	require.NotNil(t, converted[3].Stdio)
	require.Equal(t, acp.McpServer{}, converted[4])

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.handleForkSession(t.Context(), json.RawMessage(`{`))
	requireInvalidParams(t, err)

	_, err = agent.handleForkSession(t.Context(), json.RawMessage(`{}`))
	requireInvalidParams(t, err)

	params := surfaceForkParams(t)
	params.Meta = map[string]any{piMetaKey: "invalid"}
	_, err = agent.handleForkSession(t.Context(), surfaceForkRaw(t, params))
	requireInvalidParams(t, err)

	params = surfaceForkParams(t)
	params.Cwd = "relative"
	_, err = agent.handleForkSession(t.Context(), surfaceForkRaw(t, params))
	requireInvalidParams(t, err)

	params = surfaceForkParams(t)
	agent.deleted[params.SessionId] = struct{}{}
	_, err = agent.handleForkSession(t.Context(), surfaceForkRaw(t, params))
	requireInvalidParams(t, err)

	loadStore := newSurfaceSessionStore()
	loadStore.loadErr = errors.New("load failed")
	loadAgent := NewAgent(
		WithSessionStore(loadStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = loadAgent.handleForkSession(t.Context(), surfaceForkRaw(t, surfaceForkParams(t)))
	require.ErrorContains(t, err, "load failed")

	unknownAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err = unknownAgent.handleForkSession(t.Context(), surfaceForkRaw(t, surfaceForkParams(t)))
	requireInvalidParams(t, err)

	headerStore := newSurfaceSessionStore()
	surfaceStoreRows(t, headerStore, json.RawMessage(`{"type":"session"}`))
	headerAgent := NewAgent(
		WithSessionStore(headerStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = headerAgent.handleForkSession(t.Context(), surfaceForkRaw(t, surfaceForkParams(t)))
	requireInvalidParams(t, err)

	contentStore := newSurfaceSessionStore()
	surfaceStoreRows(t, contentStore,
		json.RawMessage(`{"type":"session"}`),
		json.RawMessage(`{"type":"message"}`),
	)
	startAgent := NewAgent(
		WithExecutablePath(filepath.Join(t.TempDir(), "missing-pi")),
		WithSessionStore(contentStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = startAgent.handleForkSession(t.Context(), surfaceForkRaw(t, surfaceForkParams(t)))
	require.Error(t, err)

	commitStore := newSurfaceSessionStore()
	commitStore.appendErr = errors.New("append failed")
	commitAgent := NewAgent(
		WithSessionStore(commitStore),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	sessionFile := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(
		sessionFile,
		[]byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"),
		0o600,
	))
	commitAgent.sessions[surfaceParentID] = &agentSession{
		agent:           commitAgent,
		id:              surfaceParentID,
		sessionFilePath: sessionFile,
	}
	_, err = commitAgent.handleForkSession(t.Context(), surfaceForkRaw(t, surfaceForkParams(t)))
	require.ErrorIs(t, err, errSessionMirrorAppend)
}

func TestForkExtensionStoreLimitAfterNativeClone(t *testing.T) {
	store := newSurfaceSessionStore()
	surfaceStoreRows(t, store,
		json.RawMessage(`{"type":"session"}`),
		json.RawMessage(`{"type":"message"}`),
	)

	agent := NewAgent(
		WithExecutablePath("/fake/pi"),
		WithHome(t.TempDir()),
		WithSessionStore(store),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent.probeVersion = func(context.Context, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	agent.sessions["occupied"] = &agentSession{agent: agent, id: "occupied"}

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: surfaceChildID, ThinkingLevel: pi.ThinkingLevelMedium}
	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}

	_, err := agent.handleForkSession(t.Context(), surfaceForkRaw(t, surfaceForkParams(t)))
	requireInvalidRequest(t, err)

	close(client.events)
	close(client.uiRequests)
}

func TestConfigSelectionFailureBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{})
	requireInvalidParams(t, err)

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
	lateCtx := &poisonOnDoneContext{session: late}
	_, err = agent.SetSessionConfigOption(lateCtx, SetModelRequest(late.id, "p/model"))
	require.Error(t, err)

	updateClient := newSurfaceAgentClient()
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
	require.ErrorContains(
		t,
		session.applyThinkingLevelSelection(t.Context(), pi.ThinkingLevelHigh),
		"set thinking failed",
	)
}

func TestForkCallRejectsMalformedResponse(t *testing.T) {
	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()
	t.Cleanup(func() {
		_ = clientToAgentReader.Close()
		_ = clientToAgentWriter.Close()
		_ = agentToClientReader.Close()
		_ = agentToClientWriter.Close()
	})

	_ = acp.NewConnection(
		func(context.Context, string, json.RawMessage) (any, *acp.RequestError) {
			return "not a fork response", nil
		},
		agentToClientWriter,
		clientToAgentReader,
	)
	conn := acp.NewClientSideConnection(&conformanceClient{}, clientToAgentWriter, agentToClientReader)

	_, err := CallForkSession(t.Context(), conn, surfaceForkParams(t))
	require.Error(t, err)
}

func TestDialogFailureBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "session"}

	session.turnCancelled = true
	dialogCtx, finishDialog := session.registerDialog(t.Context(), "cancelled")
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)
	finishDialog()
	require.Empty(t, session.pendingDialogs)

	elicitClient := newSurfaceAgentClient()
	elicitClient.elicitationErr = errors.New("elicitation failed")
	_, accepted := session.createDialogElicitation(
		t.Context(),
		elicitClient,
		pi.UIRequest{ID: "error", Method: uiMethodInput},
	)
	require.False(t, accepted)

	elicitClient.elicitationErr = nil
	_, accepted = session.createDialogElicitation(
		t.Context(),
		elicitClient,
		pi.UIRequest{ID: "cancel", Method: uiMethodInput},
	)
	require.False(t, accepted)

	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(t.Context(), pi.UIRequest{ID: "none"}, pi.PermissionPrompt{}),
	)

	permissionClient := newSurfaceAgentClient()
	permissionClient.permissionErr = errors.New("permission failed")
	agent.setConnection(permissionClient)
	require.Equal(
		t,
		string(permissionOptionDeny),
		session.requestPermissionAnswer(
			t.Context(),
			pi.UIRequest{ID: "error"},
			pi.PermissionPrompt{ToolName: "surface_tool", Input: json.RawMessage(`{"value":true}`)},
		),
	)

	session.respondUIDialog(t.Context(), pi.UICancelResponse("no-client"))
	native := newStubPiClient()
	native.respondErr = errors.New("respond failed")
	session.client = native
	session.respondUIDialog(t.Context(), pi.UICancelResponse("error"))
}
