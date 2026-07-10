package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestStoreStartedSessionCloseErrorBranches(t *testing.T) {
	closedAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	closedAgent.closed = true
	rejected := &agentSession{agent: closedAgent, id: "rejected", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	require.ErrorIs(t, closedAgent.storeStartedSession(t.Context(), rejected), errAgentClosed)

	fullAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	fullAgent.sessions["filler"] = &agentSession{agent: fullAgent, id: "filler", turn: make(chan struct{}, sessionTurnCapacity)}
	backpressured := &agentSession{agent: fullAgent, id: "backpressured", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	requireInvalidRequest(t, fullAgent.storeStartedSession(t.Context(), backpressured))

	replaceAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	replaceAgent.sessions["shared"] = &agentSession{agent: replaceAgent, id: "shared", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	replacement := &agentSession{agent: replaceAgent, id: "shared", turn: make(chan struct{}, sessionTurnCapacity)}
	require.NoError(t, replaceAgent.storeStartedSession(t.Context(), replacement))
}

func TestRemoveSessionCloseError(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "id", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)}
	agent.removeSession(t.Context(), "unmapped", session)
}

func TestNewSessionBackpressure(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "fresh"}
	agent := newStubClientAgent(t, client, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
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

	spawnAgent := newStubClientAgent(t, nil, WithSessionStore(newStore()))
	spawnAgent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, errors.New("spawn")
	}
	_, err = spawnAgent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	require.Error(t, err)

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "resume-id"}
	backpressureAgent := newStubClientAgent(t, client, WithSessionStore(newStore()), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
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
	agent := newStubClientAgent(t, client, WithSessionStore(store))
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
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, []SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/one"}`)}))
	storeAgent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	resp, err = storeAgent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, resp.Sessions, 1)
}

func TestListStoreSessionsLoadErrorAndCwdFilter(t *testing.T) {
	loadStore := newFaultySessionStore()
	require.NoError(t, loadStore.InMemorySessionStore.Append(
		t.Context(),
		SessionKey{SessionID: validSessionUUID},
		[]SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/one"}`)},
	))
	loadStore.loadErr = errors.New("load failed")
	loadAgent := NewAgent(WithSessionStore(loadStore), WithLogger(slog.New(slog.DiscardHandler)))
	_, err := loadAgent.listStoreSessions(t.Context(), acp.ListSessionsRequest{})
	require.Error(t, err)

	cwdStore := NewInMemorySessionStore()
	require.NoError(t, cwdStore.Append(
		t.Context(),
		SessionKey{SessionID: validSessionUUID},
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

	badModel := newStubClientAgent(t, nil)
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

	agent := newStubClientAgent(t, nil)
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

	bridgeAsDir := newStubClientAgent(t, nil)
	materializeMkdirAll = mkdirAllPlacingDir(pi.BridgeExtensionFileName)
	_, err := bridgeAsDir.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	configClient := newStubPiClient()
	configClient.state = pi.SessionState{SessionID: "id"}
	configAsDir := newStubClientAgent(t, configClient)
	materializeMkdirAll = mkdirAllPlacingDir(pi.MCPConfigFileName)
	_, err = configAsDir.startSession(t.Context(), sessionStart{
		Cwd:        "/cwd",
		McpServers: []acp.McpServer{StdioMCPServer("stdio", "/bin/true", nil, nil)},
	})
	require.Error(t, err)
}

func TestStartSessionSeedWriteFailure(t *testing.T) {
	agent := newStubClientAgent(t, nil)
	agent.options.SeedFiles = map[string]string{"collide": "file", "collide/child": "blocked"}
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestStartSessionManagedModelAndSetupFailure(t *testing.T) {
	successClient := newStubPiClient()
	successClient.state = pi.SessionState{SessionID: "id"}
	successClient.model = pi.Model{ID: "m", ContextWindow: 5}
	managed := newStubClientAgent(t, successClient)
	session, err := managed.startSession(t.Context(), sessionStart{Cwd: "/cwd", MetaOptions: PiOptions{Model: "p/m"}})
	require.NoError(t, err)
	require.Equal(t, "p/m", session.model)
	require.NoError(t, session.Close(t.Context()))

	setupClient := newStubPiClient()
	setupClient.autoRetryErr = errors.New("retry")
	setupFail := newStubClientAgent(t, setupClient)
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
