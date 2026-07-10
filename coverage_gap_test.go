package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

const coverageValidUUID = "01234567-89ab-cdef-0123-456789abcdef"

func coverageStubAgent(t *testing.T, client *stubPiClient, opts ...Option) *Agent {
	t.Helper()

	base := make([]Option, 0, 3+len(opts))
	base = append(base,
		WithExecutablePath("/fake/pi"),
		WithHome(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent := NewAgent(append(base, opts...)...)
	agent.probeVersion = func(context.Context, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}

	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}

	return agent
}

func coverageBadProcess() *stubProcess {
	process := newStubProcess(false)
	process.shutdown = errors.New("shutdown")
	process.close = errors.New("close")

	return process
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
			proc:  coverageBadProcess(),
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
		proc:  coverageBadProcess(),
		turn:  make(chan struct{}, sessionTurnCapacity),
	}
	require.Error(t, agent.Close())
}

func TestStoreStartedSessionCloseErrorBranches(t *testing.T) {
	closedAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	closedAgent.closed = true
	rejected := &agentSession{agent: closedAgent, id: "rejected", proc: coverageBadProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	require.ErrorIs(t, closedAgent.storeStartedSession(t.Context(), rejected), errAgentClosed)

	fullAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	fullAgent.sessions["filler"] = &agentSession{agent: fullAgent, id: "filler", turn: make(chan struct{}, sessionTurnCapacity)}
	backpressured := &agentSession{agent: fullAgent, id: "backpressured", proc: coverageBadProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	requireInvalidRequest(t, fullAgent.storeStartedSession(t.Context(), backpressured))

	replaceAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	replaceAgent.sessions["shared"] = &agentSession{agent: replaceAgent, id: "shared", proc: coverageBadProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	replacement := &agentSession{agent: replaceAgent, id: "shared", turn: make(chan struct{}, sessionTurnCapacity)}
	require.NoError(t, replaceAgent.storeStartedSession(t.Context(), replacement))
}

func TestRemoveSessionCloseError(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "id", proc: coverageBadProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	agent.removeSession(t.Context(), "unmapped", session)
}

func TestNewSessionBackpressure(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "fresh"}
	agent := coverageStubAgent(t, client, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	agent.sessions["filler"] = &agentSession{agent: agent, id: "filler", turn: make(chan struct{}, sessionTurnCapacity)}

	_, err := agent.NewSession(t.Context(), NewSessionRequest("/cwd"))
	requireInvalidRequest(t, err)
}

func TestRestoreSessionAdditionalBranches(t *testing.T) {
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-id","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"}]`)}),
	}

	newStore := func() SessionStore {
		store := NewInMemorySessionStore()
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "resume-id"}, entries))

		return store
	}

	closedAgent := NewAgent(WithSessionStore(newStore()), WithLogger(slog.New(slog.DiscardHandler)))
	closedAgent.closed = true
	_, err := closedAgent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	require.ErrorIs(t, err, errAgentClosed)

	spawnAgent := coverageStubAgent(t, nil, WithSessionStore(newStore()))
	spawnAgent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, errors.New("spawn")
	}
	_, err = spawnAgent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	require.Error(t, err)

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "resume-id"}
	backpressureAgent := coverageStubAgent(t, client, WithSessionStore(newStore()), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	backpressureAgent.sessions["filler"] = &agentSession{agent: backpressureAgent, id: "filler", turn: make(chan struct{}, sessionTurnCapacity)}
	_, err = backpressureAgent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	requireInvalidRequest(t, err)
}

func TestLoadSessionRemovesStartedSessionOnReplayFailure(t *testing.T) {
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-load","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"}]`)}),
	}
	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "resume-load"}, entries))

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "resume-load"}
	agent := coverageStubAgent(t, client, WithSessionStore(store))
	connection := newDirectAgentClient()
	connection.updateErr = errors.New("replay")
	agent.setConnection(connection)

	_, err := agent.LoadSession(t.Context(), LoadSessionRequest("resume-load", "/cwd"))
	require.Error(t, err)
	require.NotContains(t, agent.sessions, acp.SessionId("resume-load"))
}

func TestListSessionsFilterAndDedupBranches(t *testing.T) {
	filterAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	filterAgent.sessions["active"] = &agentSession{agent: filterAgent, id: "active", cwd: "/other", turn: make(chan struct{}, sessionTurnCapacity)}
	cwd := "/one"
	resp, err := filterAgent.ListSessions(t.Context(), acp.ListSessionsRequest{Cwd: &cwd})
	require.NoError(t, err)
	require.Empty(t, resp.Sessions)

	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: coverageValidUUID}, []SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/one"}`)}))
	storeAgent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	resp, err = storeAgent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, resp.Sessions, 1)
}

func TestListStoreSessionsLoadErrorAndCwdFilter(t *testing.T) {
	loadStore := newSurfaceSessionStore()
	require.NoError(t, loadStore.InMemorySessionStore.Append(
		t.Context(),
		SessionKey{SessionID: coverageValidUUID},
		[]SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/one"}`)},
	))
	loadStore.loadErr = errors.New("load failed")
	loadAgent := NewAgent(WithSessionStore(loadStore), WithLogger(slog.New(slog.DiscardHandler)))
	_, err := loadAgent.listStoreSessions(t.Context(), acp.ListSessionsRequest{})
	require.Error(t, err)

	cwdStore := NewInMemorySessionStore()
	require.NoError(t, cwdStore.Append(
		t.Context(),
		SessionKey{SessionID: coverageValidUUID},
		[]SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/other"}`)},
	))
	cwdAgent := NewAgent(WithSessionStore(cwdStore), WithLogger(slog.New(slog.DiscardHandler)))
	requested := "/one"
	infos, err := cwdAgent.listStoreSessions(t.Context(), acp.ListSessionsRequest{Cwd: &requested})
	require.NoError(t, err)
	require.Empty(t, infos)
}

func TestValidateMCPServersRejectionBranches(t *testing.T) {
	require.Error(t, validateMCPServers([]acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "acp"}}}))
	require.Error(t, validateMCPServers([]acp.McpServer{{}}))
}

func TestStartSessionEarlyFailureBranches(t *testing.T) {
	missingExec := NewAgent(WithHome(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
	missingExec.versionChecked = true
	missingExec.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	_, err := missingExec.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	badModel := coverageStubAgent(t, nil)
	_, err = badModel.startSession(t.Context(), sessionStart{Cwd: "/cwd", MetaOptions: PiOptions{Model: "invalid"}})
	requireInvalidParams(t, err)

	dirFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(dirFile, []byte("x"), 0o600))
	badHome := NewAgent(WithExecutablePath("/fake/pi"), WithHome(dirFile), WithLogger(slog.New(slog.DiscardHandler)))
	badHome.versionChecked = true
	_, err = badHome.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestStartSessionHydrateWriteFailure(t *testing.T) {
	original := materializeWriteFile
	t.Cleanup(func() { materializeWriteFile = original })
	materializeWriteFile = func(string, []byte, os.FileMode) error { return errors.New("write") }

	agent := coverageStubAgent(t, nil)
	_, err := agent.startSession(t.Context(), sessionStart{
		Cwd:            "/cwd",
		ResumeID:       "resume-id",
		HydrateEntries: []SessionStoreEntry{json.RawMessage(`{}`)},
	})
	require.Error(t, err)
}

func TestStartSessionExtensionAndConfigFailures(t *testing.T) {
	original := materializeMkdirAll
	t.Cleanup(func() { materializeMkdirAll = original })

	mkdirAllPlacingDir := func(child string) func(string, os.FileMode) error {
		return func(path string, mode os.FileMode) error {
			if mkErr := original(path, mode); mkErr != nil {
				return mkErr
			}

			if filepath.Base(path) == "agent" {
				return original(filepath.Join(path, child), 0o700)
			}

			return nil
		}
	}

	bridgeAsDir := coverageStubAgent(t, nil)
	materializeMkdirAll = mkdirAllPlacingDir(pi.BridgeExtensionFileName)
	_, err := bridgeAsDir.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	configClient := newStubPiClient()
	configClient.state = pi.SessionState{SessionID: "id"}
	configAsDir := coverageStubAgent(t, configClient)
	materializeMkdirAll = mkdirAllPlacingDir(pi.MCPConfigFileName)
	_, err = configAsDir.startSession(t.Context(), sessionStart{
		Cwd:        "/cwd",
		McpServers: []acp.McpServer{StdioMCPServer("stdio", "/bin/true", nil, nil)},
	})
	require.Error(t, err)
}

func TestStartSessionSeedWriteFailure(t *testing.T) {
	agent := coverageStubAgent(t, nil)
	agent.options.SeedFiles = map[string]string{"collide": "file", "collide/child": "blocked"}
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestStartSessionManagedModelAndSetupFailure(t *testing.T) {
	successClient := newStubPiClient()
	successClient.state = pi.SessionState{SessionID: "id"}
	successClient.model = pi.Model{ID: "m", ContextWindow: 5}
	managed := coverageStubAgent(t, successClient)
	session, err := managed.startSession(t.Context(), sessionStart{Cwd: "/cwd", MetaOptions: PiOptions{Model: "p/m"}})
	require.NoError(t, err)
	require.Equal(t, "p/m", session.model)
	require.NoError(t, session.Close(t.Context()))

	setupClient := newStubPiClient()
	setupClient.autoRetryErr = errors.New("retry")
	setupFail := coverageStubAgent(t, setupClient)
	_, err = setupFail.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestSetUpNativeSessionForkCommitMirrorFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: t.TempDir(), ThinkingLevel: "off"}
	session := &agentSession{agent: agent, client: client, proc: newStubProcess(false)}
	err := agent.setUpNativeSession(t.Context(), session, sessionStart{ForkSession: true}, pi.ModelRef{}, false)
	require.Error(t, err)
}

func TestSessionCloseTurnWaitFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	session := &agentSession{
		agent:         agent,
		id:            "id",
		proc:          process,
		turn:          make(chan struct{}, sessionTurnCapacity),
		closeTurnWait: time.Millisecond,
	}

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	t.Cleanup(release)

	require.Error(t, session.Close(t.Context()))
}

func TestPromptSecondPoisonCheck(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}
	lateCtx := &poisonOnDoneContext{session: session}

	_, err := session.Prompt(lateCtx, TextPromptRequest("id", "hi"))
	require.Error(t, err)
}

func TestPromptClientPromptFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	client.promptErr = errors.New("prompt")
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "hi"))
	requirePiTurnFailure(t, err, failureCauseTransport)
}

func TestPromptHandleTurnEventEmitFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	connection.updateErr = errors.New("emit")
	agent.setConnection(connection)
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}
	session.startPump(client)
	t.Cleanup(session.stopPump)

	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for session.activeTurnSink() == nil {
			if time.Now().After(deadline) {
				return
			}

			time.Sleep(time.Millisecond)
		}

		select {
		case client.events <- pi.ToolExecutionStartEvent{ToolCallID: "call", ToolName: "bash"}:
		case <-time.After(2 * time.Second):
		}
	}()

	_, err := session.Prompt(t.Context(), TextPromptRequest("id", "hi"))
	require.Error(t, err)
}

func TestFinishTurnCommitMirrorFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	client := newStubPiClient()
	client.stats = pi.SessionStats{SessionID: "id"}
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false), sessionFilePath: t.TempDir()}

	var timedOut atomic.Bool
	_, err := session.finishTurn(t.Context(), t.Context(), TextPromptRequest("id", "title"), &promptTurnState{}, &timedOut)
	require.Error(t, err)
}

func TestListSessionsSortsByUpdatedTime(t *testing.T) {
	store := &InMemorySessionStore{
		entries: map[SessionKey][]SessionStoreEntry{
			{SessionID: "older"}: {json.RawMessage(`{}`)},
			{SessionID: "newer"}: {json.RawMessage(`{}`)},
		},
		updatedAt: map[SessionKey]int64{
			{SessionID: "older"}: 1,
			{SessionID: "newer"}: 2,
		},
		tombstone: map[SessionKey]struct{}{},
	}

	summaries, err := store.ListSessions(t.Context())
	require.NoError(t, err)
	require.Equal(t, "newer", summaries[0].SessionID)
}
