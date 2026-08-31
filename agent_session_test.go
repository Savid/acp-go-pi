package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/savid/acp-go-pi/internal/pi"
)

type commandCatalogFailClient struct {
	*directAgentClient
	want error
}

func (c *commandCatalogFailClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if notification.Update.AvailableCommandsUpdate != nil {
		return c.want
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

func TestStoreStartedSessionCloseErrorBranches(t *testing.T) {
	closedAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	closedAgent.closed = true
	rejected := attachTestNativeBoundary(&agentSession{agent: closedAgent, id: "rejected", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)})
	require.ErrorIs(t, closedAgent.storeStartedSession(t.Context(), rejected), errAgentClosed)

	fullAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	fullAgent.sessions["filler"] = &agentSession{agent: fullAgent, id: "filler", turn: make(chan struct{}, sessionTurnCapacity)}
	backpressured := attachTestNativeBoundary(&agentSession{agent: fullAgent, id: "backpressured", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)})
	requireInvalidRequest(t, fullAgent.storeStartedSession(t.Context(), backpressured))

	replaceAgent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	replaceAgent.sessions["shared"] = attachTestNativeBoundary(&agentSession{agent: replaceAgent, id: "shared", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)})
	replacement := &agentSession{agent: replaceAgent, id: "shared", turn: make(chan struct{}, sessionTurnCapacity)}
	require.NoError(t, replaceAgent.storeStartedSession(t.Context(), replacement))
}

func TestRemoveSessionCloseError(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := attachTestNativeBoundary(&agentSession{agent: agent, id: "id", proc: newFailingCloseProcess(), turn: make(chan struct{}, sessionTurnCapacity)})
	require.Error(t, agent.removeSession(t.Context(), "unmapped", session))
}

func TestContainmentIncompleteInstallAndRemovalOwnersSurviveUntilAgentClose(t *testing.T) {
	newIncomplete := func(agent *Agent, id acp.SessionId) (*agentSession, *stubProcess) {
		process := newStubProcess(false)
		process.close = ErrContainmentIncomplete

		return attachTestNativeBoundary(&agentSession{
			agent:       agent,
			id:          id,
			proc:        process,
			turn:        make(chan struct{}, sessionTurnCapacity),
			sessionRoot: t.TempDir(),
		}), process
	}
	requireRetained := func(t *testing.T, agent *Agent, session *agentSession) {
		t.Helper()
		agent.mu.Lock()
		_, retained := agent.retainedSessions[session]
		agent.mu.Unlock()
		require.True(t, retained, "the exact incomplete session lost addressable ownership")
	}

	t.Run("rejected install", func(t *testing.T) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		session, process := newIncomplete(agent, "rejected")
		agent.deleted[session.id] = struct{}{}

		err := agent.storeStartedSession(t.Context(), session)
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		requireRetained(t, agent, session)
		require.Equal(t, 1, process.closeCalls)

		require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
		requireRetained(t, agent, session)
		require.Equal(t, 1, process.closeCalls, "Agent.Close reran rather than joined the immutable close")
	})

	t.Run("replaced install", func(t *testing.T) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		previous, previousProcess := newIncomplete(agent, "shared")
		replacementProcess := newStubProcess(false)
		replacement := &agentSession{
			agent: agent, id: "shared", proc: replacementProcess, turn: make(chan struct{}, sessionTurnCapacity),
			sessionRoot: t.TempDir(),
		}
		attachTestNativeBoundary(replacement)
		agent.sessions[previous.id] = previous

		require.ErrorIs(t, agent.storeStartedSession(t.Context(), replacement), ErrContainmentIncomplete)
		requireRetained(t, agent, previous)
		require.Same(t, replacement, agent.sessions["shared"])

		require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
		requireRetained(t, agent, previous)
		require.Equal(t, 1, previousProcess.closeCalls)
		require.Equal(t, 1, replacementProcess.closeCalls)
	})

	for _, path := range []string{"replay cleanup", "removal cleanup"} {
		t.Run(path, func(t *testing.T) {
			agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
			session, process := newIncomplete(agent, acp.SessionId(path))
			agent.sessions[session.id] = session

			require.ErrorIs(t, agent.removeSession(t.Context(), session.id, session), ErrContainmentIncomplete)
			requireRetained(t, agent, session)
			require.NotContains(t, agent.sessions, session.id)

			require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
			requireRetained(t, agent, session)
			require.Equal(t, 1, process.closeCalls)
		})
	}
}

// TestSameSessionIDKeepsOneLinearizedOwner proves the close-versus-same-id
// race: a load, resume, or fork that stores a replacement under a live id takes
// ownership of that id, the superseded session is closed exactly once, and its
// closer can never evict the replacement from any removal site.
func TestSameSessionIDKeepsOneLinearizedOwner(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	superseded := &agentSession{
		agent: agent, id: "id", proc: newStubProcess(false), sessionRoot: t.TempDir(),
	}
	attachTestNativeBoundary(superseded)

	agent.mu.Lock()
	agent.sessions[superseded.id] = superseded
	agent.mu.Unlock()

	replacement := &agentSession{
		agent: agent, id: "id", proc: newStubProcess(false), sessionRoot: t.TempDir(),
	}
	attachTestNativeBoundary(replacement)
	require.NoError(t, agent.storeStartedSession(t.Context(), replacement))

	supersededProcess, ok := superseded.proc.(*stubProcess)
	require.True(t, ok)
	require.Equal(t, 1, supersededProcess.closeCalls)

	agent.mu.Lock()
	require.Same(t, replacement, agent.sessions["id"])
	agent.mu.Unlock()

	require.False(t, agent.detachSession("id", superseded))
	require.NoError(t, agent.removeSession(t.Context(), "id", superseded))

	agent.mu.Lock()
	require.Same(t, replacement, agent.sessions["id"], "a superseded closer evicted the replacement")
	agent.mu.Unlock()

	require.Equal(t, 1, supersededProcess.closeCalls, "the superseded session was torn down twice")

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "id"})
	require.NoError(t, err)

	agent.mu.Lock()
	require.NotContains(t, agent.sessions, acp.SessionId("id"))
	agent.mu.Unlock()

	replacementProcess, ok := replacement.proc.(*stubProcess)
	require.True(t, ok)
	require.Equal(t, 1, replacementProcess.closeCalls)
	require.Empty(t, replacement.turn, "close left the replacement's turn admission unbalanced")
}

func TestFailedCloseRetainsOneImmutableResult(t *testing.T) {
	refused := errors.New("durability unavailable")
	// The commit exhausts its own retries before the close reports the refusal.
	store := &appendControlledStore{SessionStore: NewInMemorySessionStore(), failures: len(mirrorAppendDelays), err: refused}
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	agent.conn = newDirectAgentClient()

	session := &agentSession{
		agent:       agent,
		id:          "retained",
		proc:        newStubProcess(false),
		sessionRoot: t.TempDir(),
		turn:        make(chan struct{}, sessionTurnCapacity),
	}
	attachTestNativeBoundary(session)
	agent.sessions[session.id] = session

	t.Cleanup(func() { _ = agent.Close() })

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, refused)

	resolved, err := agent.session(session.id)
	require.NoError(t, err, "a failed close lost the id carrying its immutable result")
	require.Same(t, session, resolved)

	_, err = agent.Prompt(t.Context(), acp.PromptRequest{SessionId: session.id})
	requireInvalidParams(t, err)
	require.Equal(t, len(mirrorAppendDelays), store.calls, "a prompt slipped in behind the close and drove the session")

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	require.ErrorIs(t, err, refused)
	require.Equal(t, len(mirrorAppendDelays), store.calls, "a second close re-ran an immutable teardown")

	journal, err := store.Load(t.Context(), SessionKey{SessionID: string(session.id), Subpath: SessionStoreLifecycleSubpath})
	require.NoError(t, err)
	require.Empty(t, journal)

	agent.mu.Lock()
	require.Contains(t, agent.sessions, session.id)
	agent.mu.Unlock()
}

func TestNewSessionBackpressure(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "fresh"}
	agent := newStubClientAgent(t, client, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	agent.sessions["filler"] = &agentSession{agent: agent, id: "filler", turn: make(chan struct{}, sessionTurnCapacity)}

	_, err := agent.NewSession(t.Context(), NewSessionRequest("/cwd"))
	requireInvalidRequest(t, err)
}

func TestNewSessionPreservesStartupCancellation(t *testing.T) {
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			client := newStubPiClient()
			client.autoRetryErr = fmt.Errorf("configure native retry: %w", sentinel)
			agent := newStubClientAgent(t, client)
			t.Cleanup(func() { require.NoError(t, agent.Close()) })

			_, err := agent.NewSession(t.Context(), NewSessionRequest("/cwd"))
			require.ErrorIs(t, err, sentinel)
			require.ErrorContains(t, err, "configure native retry")
			var requestErr *acp.RequestError
			require.ErrorAs(t, err, &requestErr)
		})
	}
}

func TestRestoreSessionAdditionalBranches(t *testing.T) {
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-id","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"}]`)}),
	}

	newStore := func() SessionStore {
		store := NewInMemorySessionStore()
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "resume-id"}, entries))
		appendLifecycleBoundaryForRows(t, store, "resume-id", len(entries))

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

func TestResumeSessionPublishesTerminalNativeIdentityWithoutHistory(t *testing.T) {
	messageID := "018f47ad-839d-7f70-b7f7-c01d6d97b675"
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-id","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{
			Role: messageRoleAssistant, ACPMessageID: messageID,
			Content: json.RawMessage(`[{"type":"text","text":"answer"}]`),
		}),
	}
	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "resume-id"}, entries))
	appendLifecycleBoundaryForRows(t, store, "resume-id", len(entries))

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "resume-id"}
	agent := newStubClientAgent(t, client, WithSessionStore(store))
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	connection := newDirectAgentClient()
	agent.setConnection(connection)

	_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	require.NoError(t, err)
	require.Len(t, connection.notifications, 2)
	require.NotNil(t, connection.notifications[0].Update.SessionInfoUpdate)
	require.Equal(t, messageID,
		anyMap(t, connection.notifications[0].Meta[piMetaKey])[jsonFieldMessageID])

	// The establishing snapshot is an answer rather than a silence: a session
	// whose harness discovered no commands says so explicitly.
	require.NotNil(t, connection.notifications[1].Update.AvailableCommandsUpdate)
	require.Empty(t, connection.notifications[1].Update.AvailableCommandsUpdate.AvailableCommands)

	connection.updateErr = errors.New("identity")
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	require.ErrorContains(t, err, "identity")

	failingClient := newStubPiClient()
	failingClient.state = pi.SessionState{SessionID: "resume-id"}
	failingAgent := newStubClientAgent(t, failingClient, WithSessionStore(store))
	failingConnection := newDirectAgentClient()
	failingConnection.updateErr = errors.New("identity")
	failingAgent.setConnection(failingConnection)

	_, err = failingAgent.ResumeSession(t.Context(), ResumeSessionRequest("resume-id", "/cwd"))
	require.ErrorContains(t, err, "identity")
	require.NotContains(t, failingAgent.sessions, acp.SessionId("resume-id"))
}

func TestEstablishingMethodsFailClosedWhenOpeningCatalogIsRejected(t *testing.T) {
	want := errors.New("opening catalog rejected")
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-open","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"}]`)}),
	}
	newStore := func() SessionStore {
		store := NewInMemorySessionStore()
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "resume-open"}, entries))
		appendLifecycleBoundaryForRows(t, store, "resume-open", len(entries))

		return store
	}

	for _, method := range []string{"new", "resume", "load"} {
		t.Run(method, func(t *testing.T) {
			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: "resume-open"}
			opts := []Option{}
			if method != "new" {
				opts = append(opts, WithSessionStore(newStore()))
			}
			agent := newStubClientAgent(t, client, opts...)
			agent.setConnection(&commandCatalogFailClient{directAgentClient: newDirectAgentClient(), want: want})

			var err error
			switch method {
			case "new":
				_, err = agent.NewSession(t.Context(), NewSessionRequest("/cwd"))
			case "resume":
				_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("resume-open", "/cwd"))
			case "load":
				_, err = agent.LoadSession(t.Context(), LoadSessionRequest("resume-open", "/cwd"))
			}
			require.ErrorIs(t, err, want)
		})
	}
}

func TestLoadSessionRemovesStartedSessionOnReplayFailure(t *testing.T) {
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-load","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"}]`)}),
	}
	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "resume-load"}, entries))
	appendLifecycleBoundaryForRows(t, store, "resume-load", len(entries))

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "resume-load"}
	agent := newStubClientAgent(t, client, WithSessionStore(store))
	process := newStubProcess(false)
	process.close = ErrContainmentIncomplete
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}
	connection := newDirectAgentClient()
	connection.updateErr = errors.New("replay")
	agent.setConnection(connection)

	_, err := agent.LoadSession(t.Context(), LoadSessionRequest("resume-load", "/cwd"))
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.NotContains(t, agent.sessions, acp.SessionId("resume-load"))
	agent.mu.Lock()
	require.Len(t, agent.retainedSessions, 1)
	var retained *agentSession
	for session := range agent.retainedSessions {
		retained = session
	}
	agent.mu.Unlock()
	require.Same(t, process, retained.proc)
	require.Equal(t, 1, process.closeCalls)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
	agent.mu.Lock()
	_, stillRetained := agent.retainedSessions[retained]
	agent.mu.Unlock()
	require.True(t, stillRetained, "memoized Agent.Close erased replay quarantine")
	require.Equal(t, 1, process.closeCalls, "Agent.Close did not join the replay cleanup's immutable result")
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
	missingExec := NewAgent(testContainmentOption(), WithScratchDir(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
	missingExec.versionChecked = true
	missingExec.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	_, err := missingExec.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	badModel := newStubClientAgent(t, nil)
	_, err = badModel.startSession(t.Context(), sessionStart{Cwd: "/cwd", MetaOptions: PiOptions{Model: "invalid"}})
	requireInvalidParams(t, err)

	dirFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(dirFile, []byte("x"), 0o600))
	badScratch := NewAgent(WithExecutablePath("/fake/pi"), WithScratchDir(dirFile), WithLogger(slog.New(slog.DiscardHandler)))
	badScratch.versionChecked = true
	_, err = badScratch.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestSessionConstructionPanicAndOwnershipHelperEdges(t *testing.T) {
	originalMkdirTemp := materializeMkdirTemp
	t.Cleanup(func() { materializeMkdirTemp = originalMkdirTemp })
	materializeMkdirTemp = func(string, string) (string, error) { panic("constructor panic") }
	agent := newStubClientAgent(t, nil)
	agent.versionChecked = true
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.ErrorContains(t, err, "construction callback panicked")

	bare := &Agent{retainedSessions: nil}
	session := &agentSession{id: "retained"}
	bare.retainIncompleteSession(session, errors.New("retain"))
	require.Contains(t, bare.retainedSessions, session)

	bare = &Agent{constructions: make(map[*nativeConstruction]struct{})}
	owner := &nativeConstruction{done: make(chan struct{}), nativeBoundary: newNativeBoundaryTracker()}
	bare.constructions[owner] = struct{}{}
	closed, transferred := bare.transferConstructionToSession(owner, session)
	require.False(t, closed)
	require.True(t, transferred)
	require.Contains(t, bare.retainedSessions, session)
}

func TestRestoreActiveSessionReleasesGateOnStoreFailure(t *testing.T) {
	store := newFaultySessionStore()
	store.loadErr = errors.New("load active prefix")
	agent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent, id: "active", outbox: newTestSessionOutbox(1)}
	_, err := agent.restoreActiveSession(t.Context(), session.id, session)
	require.ErrorIs(t, err, store.loadErr)
	release, err := session.beginRestore(t.Context())
	require.NoError(t, err, "failed load retained the restore gate")
	release()
}

func TestStartSessionRejectsUnsafeGlobalEnvironment(t *testing.T) {
	for _, key := range []string{"NODE_OPTIONS", "BASH_ENV", "ENV", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "BAD-NAME", pi.EnvExtraPathDirs, strings.ToLower(pi.EnvExtraPathDirs)} {
		t.Run(key, func(t *testing.T) {
			client := newStubPiClient()
			agent := newStubClientAgent(t, client, WithEnv(map[string]string{key: "unsafe"}))
			starts := 0
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				starts++

				return newStubProcess(false), client, nil
			}

			_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
			requireUnsupportedOption(t, err, optionFieldEnv+"."+key)
			require.Zero(t, starts)
		})
	}
}

// A static base PATH is the one thing the agent-scoped environment owns that
// the session-scoped one does not, so it must reach the launch instead of
// failing session start.
func TestStartSessionAcceptsAgentScopedBasePath(t *testing.T) {
	for _, key := range []string{"PATH", "Path"} {
		t.Run(key, func(t *testing.T) {
			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: "id"}
			agent := newStubClientAgent(t, client, WithEnv(map[string]string{key: "/base/bin"}))

			var launched pi.LaunchSpec

			agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
				launched = spec

				return newStubProcess(false), client, nil
			}

			session, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })
			require.Equal(t, "/base/bin", launched.Env[key])
		})
	}
}

func TestStartSessionRejectsReservedSessionEnvironment(t *testing.T) {
	for _, key := range []string{pi.EnvExtraPathDirs, strings.ToLower(pi.EnvExtraPathDirs)} {
		t.Run(key, func(t *testing.T) {
			client := newStubPiClient()
			agent := newStubClientAgent(t, client)
			starts := 0
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				starts++

				return newStubProcess(false), client, nil
			}

			_, err := agent.NewSession(t.Context(), NewSessionRequest("/cwd",
				WithSessionPiOptions(NewPiOptions(WithPiEnv(map[string]string{key: "/attacker/bin"}))),
			))
			requireInvalidParams(t, err)
			require.Zero(t, starts)
		})
	}
}

// The session-scoped list is the only PATH prefix authority: it reaches the
// launch verbatim, and the agent-scoped WithEnv PATH is the static base behind
// it rather than a second ordered list merged into it.
func TestStartSessionCarriesOnlySessionExtraPathDirs(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id"}
	agent := newStubClientAgent(t, client, WithEnv(map[string]string{"PATH": "/base/bin"}))

	var launched pi.LaunchSpec
	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		launched = spec

		return newStubProcess(false), client, nil
	}

	session, err := agent.startSession(t.Context(), sessionStart{
		Cwd:         "/cwd",
		MetaOptions: PiOptions{ExtraPathDirs: []string{"/session/bin", "/session/bin"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

	require.Equal(t, []string{"/session/bin", "/session/bin"}, launched.ExtraPathDirs)
	require.Equal(t,
		strings.Join(launched.ExtraPathDirs, string(os.PathListSeparator)),
		launched.Env[pi.EnvExtraPathDirs],
	)
	require.Equal(t, "/base/bin", launched.Env["PATH"])
	require.Contains(t,
		launchedEnvironValue(launched, "PATH"),
		"/session/bin"+string(os.PathListSeparator)+"/session/bin"+string(os.PathListSeparator)+"/base/bin",
	)
}

func launchedEnvironValue(spec pi.LaunchSpec, key string) string {
	for _, entry := range spec.Environ() {
		if name, value, ok := strings.Cut(entry, "="); ok && name == key {
			return value
		}
	}

	return ""
}

func TestStartSessionLoadsExplicitSeedResourcesAndProviderEnv(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id"}
	agent := newStubClientAgent(t, client,
		WithEnv(map[string]string{"OPENAI_API_KEY": "explicit-key"}),
		WithSeedFiles(map[string]string{
			"extensions/command.ts":  "extension",
			"skills/review/SKILL.md": "skill",
			"prompts/review.md":      "prompt",
		}),
	)

	var launched pi.LaunchSpec
	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		launched = spec

		return newStubProcess(false), client, nil
	}

	session, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

	residence := session.residence.Root()
	require.Equal(t, "explicit-key", launched.Env["OPENAI_API_KEY"])
	require.Len(t, launched.ExtensionPaths, 3)
	require.Contains(t, filepath.ToSlash(launched.ExtensionPaths[0]), "/extensions/command.ts")
	require.Equal(t, filepath.Join(residence, pi.BridgeExtensionFileName), launched.ExtensionPaths[1])
	require.Equal(t, filepath.Join(residence, pi.PathExtensionFileName), launched.ExtensionPaths[2])
	require.Equal(t, launched.AgentDir, filepath.Dir(filepath.Dir(residence)))
	require.Equal(t, []string{filepath.Join(launched.AgentDir, "skills", "review", "SKILL.md")}, launched.SkillPaths)
	require.Equal(t, []string{filepath.Join(launched.AgentDir, "prompts", "review.md")}, launched.PromptTemplatePaths)
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

func TestStartSessionRefusesAnUnusableSessionResidence(t *testing.T) {
	original := materializeMkdirAll
	t.Cleanup(func() { materializeMkdirAll = original })

	materializeMkdirAll = func(path string, mode os.FileMode) error {
		if mkErr := original(path, mode); mkErr != nil {
			return mkErr
		}
		if filepath.Base(path) != "agent" {
			return nil
		}

		return os.WriteFile(filepath.Join(path, ".acp-session"), nil, 0o600)
	}

	blocked := newStubClientAgent(t, nil)
	_, err := blocked.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.ErrorContains(t, err, "create session residence root")

	blockedWithMCP := newStubClientAgent(t, nil)
	_, err = blockedWithMCP.startSession(t.Context(), sessionStart{
		Cwd:        "/cwd",
		McpServers: []acp.McpServer{StdioMCPServer("stdio", "/bin/true", nil, nil)},
	})
	require.ErrorContains(t, err, "create session residence root")
}

func TestStartSessionSeedWriteFailure(t *testing.T) {
	agent := newStubClientAgent(t, nil)
	agent.options.SeedFiles = map[string]string{"collide": "file", "collide/child": "blocked"}
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestStartSessionExplicitResourcesFailure(t *testing.T) {
	agent := newStubClientAgent(t, nil)
	wantErr := errors.New("explicit resources")
	previous := agentDirExplicitResources
	agentDirExplicitResources = func(pi.AgentDir) (pi.ExplicitResources, error) {
		return pi.ExplicitResources{}, wantErr
	}
	t.Cleanup(func() { agentDirExplicitResources = previous })

	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.ErrorIs(t, err, wantErr)
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

// TestNewSessionPassesThinkingLevelThroughToNative pins both halves of the
// establishment door: the level named in `_meta.pi.options` travels to pi
// unchanged, and the session advertises the level pi reports afterwards — the
// one it retained when it acknowledged the request without adopting it.
func TestNewSessionPassesThinkingLevelThroughToNative(t *testing.T) {
	for _, test := range []struct {
		name      string
		requested string
		effective string
	}{
		{name: "unapplied value leaves the retained level advertised", requested: "registry-unknown", effective: pi.ThinkingLevelOff},
		{name: "whitespace is a value pi does not know, not an empty one", requested: " high ", effective: pi.ThinkingLevelOff},
		{name: "an adopted value is advertised because pi reports it", requested: pi.ThinkingLevelMax, effective: pi.ThinkingLevelMax},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: "id", SessionFile: "/session", ThinkingLevel: pi.ThinkingLevelOff}
			var sent string
			client.thinkingFunc = func(value string) { sent = value }
			agent := newStubClientAgent(t, client)

			response, err := agent.NewSession(t.Context(), NewSessionRequest("/cwd",
				WithSessionPiOptions(NewPiOptions(WithPiThinkingLevel(test.requested)))))
			require.NoError(t, err)
			require.Equal(t, test.requested, sent, "the value still travels to pi unchanged")
			require.Len(t, response.ConfigOptions, 1)
			require.NotNil(t, response.ConfigOptions[0].Select)
			require.Equal(t, acp.SessionConfigValueId(test.effective), response.ConfigOptions[0].Select.CurrentValue)
			require.Len(t, *response.ConfigOptions[0].Select.Options.Ungrouped, len(pi.ThinkingLevels()))

			_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: response.SessionId})
			require.NoError(t, err)
		})
	}
}

func TestSetUpNativeSessionForkCommitMirrorFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: t.TempDir(), ThinkingLevel: "off"}
	session := &agentSession{agent: agent, client: client, proc: newStubProcess(false)}
	err := agent.setUpNativeSession(t.Context(), session, sessionStart{ForkSession: true}, pi.ModelRef{}, false)
	require.Error(t, err)
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
	require.NoError(t, agent.removeSession(t.Context(), "missing", nil))
	require.NoError(t, agent.removeSession(t.Context(), "id", session))
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

	// A set whose effective level cannot be read back fails the start rather
	// than establishing a session that advertises a level pi never confirmed.
	client = baseClient()
	client.stateErr = errors.New("read back")
	client.stateErrAfter = 1
	require.Error(t, agent.setUpNativeSession(t.Context(), newSession(client),
		sessionStart{MetaOptions: PiOptions{ThinkingLevel: "high"}}, pi.ModelRef{}, false))

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
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
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
		agent := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithScratchDir(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
		agent.probeVersion = func(context.Context, string, string, string) (string, error) {
			return pi.DefaultMinimumVersion, nil
		}

		return agent
	}

	originalMkdirTemp := materializeMkdirTemp
	t.Cleanup(func() {
		materializeMkdirTemp = originalMkdirTemp
	})
	agent := baseAgent()
	materializeMkdirTemp = func(string, string) (string, error) { return "", errors.New("session root") }
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
	materializeMkdirTemp = originalMkdirTemp

	agent = baseAgent()
	_, err = agent.startSession(t.Context(), sessionStart{Cwd: "/cwd", ResumeID: "id"})
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

	agent = NewAgent(
		testContainmentOption(), WithExecutablePath("/fake/pi"),
		WithScratchDir(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)),
	)
	_, err = agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)

	agent = baseAgent()
	client = newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: filepath.Join(t.TempDir(), "native.jsonl")}
	process = newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}
	session, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd", MetaOptions: PiOptions{Permission: pi.PermissionModeAllow, Env: map[string]string{"KEY": "VALUE"}, AutoRetry: true}})
	require.NoError(t, err)
	require.Equal(t, pi.PermissionModeAllow, session.permissionMode)
	require.True(t, session.autoRetry)
	require.Equal(t, []bool{true}, client.autoRetrySet)
	require.NoError(t, session.Close(t.Context()))
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
	appendLifecycleBoundaryForRows(t, store, string(id), len(entries))
	agent := NewAgent(WithSessionStore(store), WithLogger(slog.New(slog.DiscardHandler)))
	start := sessionStart{Cwd: "/cwd", ResumeID: string(id)}
	active := &agentSession{agent: agent, id: id, cwd: "/cwd", fingerprint: sessionStartFingerprint(start), turn: make(chan struct{}, 1)}
	agent.sessions[id] = active

	restored, err := agent.restoreSession(t.Context(), id, start, nil)
	require.NoError(t, err)
	require.Same(t, active, restored.session)
	require.Equal(t, entries, restored.entries)
	require.False(t, restored.started)

	restored.finish()

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
	require.Contains(t, agent.sessions, id, "a failed close orphaned the session it did not finish")

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

// launchBarrier holds every launch of a proof until all of them have arrived,
// with a deadline. A regression that fails a session before it spawns must
// surface at that session's own assertion, not as a package-wide test timeout
// on the launches still waiting for it.
type launchBarrier struct {
	mu        sync.Mutex
	parties   int
	remaining int
	ready     chan struct{}
}

func newLaunchBarrier(parties int) *launchBarrier {
	return &launchBarrier{parties: parties, remaining: parties, ready: make(chan struct{})}
}

func (b *launchBarrier) arrive(timeout time.Duration) error {
	b.mu.Lock()
	b.remaining--

	if b.remaining == 0 {
		close(b.ready)
	}

	b.mu.Unlock()

	select {
	case <-b.ready:
		return nil
	case <-time.After(timeout):
		b.mu.Lock()
		arrived := b.parties - b.remaining
		b.mu.Unlock()

		return fmt.Errorf("launch barrier timed out with %d of %d launches arrived", arrived, b.parties)
	}
}

// nativeSettingsWriter mimics pi's own persistence: pi rewrites
// defaultProvider, defaultModel, and defaultThinkingLevel in its agent
// directory whenever a session changes model or thinking level, and every
// session of a configured home shares that one file.
func nativeSettingsWriter(t *testing.T, home string) (func(provider string, id string), func(level string)) {
	t.Helper()

	var mu sync.Mutex

	persist := func(values map[string]string) {
		mu.Lock()
		defer mu.Unlock()

		settings := map[string]string{}

		data, err := os.ReadFile(filepath.Join(home, pi.SettingsFileName)) // #nosec G304 -- the path is this test's own temp dir.
		if err == nil {
			require.NoError(t, json.Unmarshal(data, &settings))
		}

		for key, value := range values {
			settings[key] = value
		}

		encoded, err := json.Marshal(settings)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(home, pi.SettingsFileName), encoded, 0o600))
	}

	setModel := func(provider string, id string) {
		persist(map[string]string{"defaultProvider": provider, "defaultModel": id})
	}
	setThinkingLevel := func(level string) {
		persist(map[string]string{"defaultThinkingLevel": level})
	}

	return setModel, setThinkingLevel
}

// TestSessionStartsOnOperatorDefaultsNotAnotherSessionsSelection is the
// durable-home isolation rule. A configured home is one agent directory shared
// by every session; pi reads settings.json there once at process start to pick
// its model and thinking level, and writes its own selection back into that
// same file whenever a session changes either. Without reconciliation the next
// session to launch — precisely the one that asked for nothing and so has
// nothing to override with — would start on the previous session's choice.
func TestSessionStartsOnOperatorDefaultsNotAnotherSessionsSelection(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	agent := newStubClientAgent(t, nil, WithHome(home))

	selecting := newStubPiClient()
	selecting.state = pi.SessionState{SessionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
	selecting.model = pi.Model{Provider: "openai", ID: "gpt-4o"}
	selecting.setModelFunc, selecting.thinkingFunc = nativeSettingsWriter(t, home)

	inheriting := newStubPiClient()
	inheriting.state = pi.SessionState{SessionID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}

	clients := map[string]*stubPiClient{"/selecting": selecting, "/inheriting": inheriting}
	launched := make(map[string]string, len(clients))

	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		client, ok := clients[spec.Cwd]
		if !ok {
			return nil, nil, fmt.Errorf("unexpected launch cwd %q", spec.Cwd)
		}

		settings, readErr := os.ReadFile(filepath.Join(spec.AgentDir, pi.SettingsFileName)) // #nosec G304 -- the path is this test's own temp dir.
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return nil, nil, readErr
		}

		launched[spec.Cwd] = string(settings)

		return newStubProcess(false), client, nil
	}

	first, err := agent.NewSession(t.Context(), NewSessionRequest("/selecting",
		WithSessionPiOptions(NewPiOptions(WithPiModel("openai/gpt-4o"), WithPiThinkingLevel(pi.ThinkingLevelHigh))),
	))
	require.NoError(t, err)

	// The hazard: pi has now recorded this session's selection in the one file
	// every later launch reads.
	persisted, err := os.ReadFile(filepath.Join(home, pi.SettingsFileName)) // #nosec G304 -- the path is this test's own temp dir.
	require.NoError(t, err)
	require.Contains(t, string(persisted), "gpt-4o")
	require.Contains(t, string(persisted), pi.ThinkingLevelHigh)

	second, err := agent.NewSession(t.Context(), NewSessionRequest("/inheriting"))
	require.NoError(t, err)

	require.NotContains(t, launched["/inheriting"], "gpt-4o",
		"a session that asked for no model launched against another session's model")
	require.NotContains(t, launched["/inheriting"], pi.ThinkingLevelHigh,
		"a session that asked for no thinking level launched against another session's level")

	for _, id := range []acp.SessionId{first.SessionId, second.SessionId} {
		session, sessionErr := agent.session(id)
		require.NoError(t, sessionErr)
		t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })
	}
}

// TestConcurrentSessionsUnderOneHomeKeepTheirOwnModel proves the same rule
// under overlap. The barrier makes it exact: every session finishes authoring
// the shared home before any of them spawns, so anything one launch wrote
// there is guaranteed visible to the others, and the session that requested no
// model is the one with nothing of its own to override an inherited value.
func TestConcurrentSessionsUnderOneHomeKeepTheirOwnModel(t *testing.T) {
	type sessionCase struct {
		cwd       string
		model     string
		sessionID string
		client    *stubPiClient
	}

	cases := []*sessionCase{
		{cwd: "/first", model: "openai/gpt-4o", sessionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
		{cwd: "/second", model: "anthropic/claude-sonnet-4-5", sessionID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
		{cwd: "/third", sessionID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc"},
	}

	byCwd := make(map[string]*sessionCase, len(cases))

	home := filepath.Join(t.TempDir(), "home")
	agent := newStubClientAgent(t, nil, WithHome(home))
	setModel, setThinkingLevel := nativeSettingsWriter(t, home)

	for _, test := range cases {
		test.client = newStubPiClient()
		test.client.state = pi.SessionState{SessionID: test.sessionID}
		test.client.setModelFunc, test.client.thinkingFunc = setModel, setThinkingLevel

		if test.model != "" {
			provider, id, _ := strings.Cut(test.model, "/")
			test.client.model = pi.Model{Provider: provider, ID: id}
		}

		byCwd[test.cwd] = test
	}

	var observed sync.Map

	// Two phases: every launch finishes authoring the shared home before any of
	// them reads it, and every read finishes before any launch returns into the
	// post-spawn commands that make pi rewrite the same file.
	authored := newLaunchBarrier(len(cases))
	inspected := newLaunchBarrier(len(cases))

	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		test, ok := byCwd[spec.Cwd]
		if !ok {
			return nil, nil, fmt.Errorf("unexpected launch cwd %q", spec.Cwd)
		}

		if err := authored.arrive(30 * time.Second); err != nil {
			return nil, nil, err
		}

		settings, readErr := os.ReadFile(filepath.Join(spec.AgentDir, pi.SettingsFileName)) // #nosec G304 -- the path is this test's own temp dir.
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return nil, nil, readErr
		}

		observed.Store(test.cwd, string(settings))

		if err := inspected.arrive(30 * time.Second); err != nil {
			return nil, nil, err
		}

		return newStubProcess(false), test.client, nil
	}

	started := make(chan acp.SessionId, len(cases))
	errs := make(chan error, len(cases))

	var running sync.WaitGroup

	running.Add(len(cases))

	for _, test := range cases {
		go func() {
			defer running.Done()

			options := []PiOption{}
			if test.model != "" {
				options = append(options, WithPiModel(test.model))
			}

			response, err := agent.NewSession(t.Context(), NewSessionRequest(test.cwd,
				WithSessionPiOptions(NewPiOptions(options...)),
			))
			if err != nil {
				errs <- err

				return
			}

			started <- response.SessionId
		}()
	}

	running.Wait()
	close(errs)
	close(started)

	for err := range errs {
		require.NoError(t, err)
	}

	require.Len(t, started, len(cases))

	for id := range started {
		session, err := agent.session(id)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })
	}

	for _, test := range cases {
		settings, ok := observed.Load(test.cwd)
		require.True(t, ok, "session %s never launched", test.cwd)

		for _, other := range cases {
			if other.model == "" {
				continue
			}

			require.NotContains(t, settings, other.model[strings.Index(other.model, "/")+1:],
				"session %s launched against a shared settings.json naming a session model", test.cwd)
		}

		session, err := agent.session(acp.SessionId(test.sessionID))
		require.NoError(t, err)

		if test.model == "" {
			continue
		}

		require.Equal(t, test.model, session.currentModel(),
			"session %s did not keep its own model", test.cwd)
	}
}

// newPartialDeleteAgent stages a session whose first teardown fails, so a
// delete tombstones and hides the id and then leaves its cleanup unfinished.
func newPartialDeleteAgent(t *testing.T, refused error) (*Agent, *stubProcess, acp.SessionId) {
	t.Helper()

	store := NewInMemorySessionStore()
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	agent.conn = newDirectAgentClient()

	id := acp.SessionId(validSessionUUID)
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: string(id)},
		[]SessionStoreEntry{json.RawMessage(`{"type":"session","cwd":"/cwd"}`)}))

	process := newStubProcess(false)
	process.closeFunc = func() error {
		if process.closeCalls == 1 {
			return refused
		}

		return nil
	}

	agent.sessions[id] = attachTestNativeBoundary(&agentSession{
		agent:       agent,
		id:          id,
		proc:        process,
		sessionRoot: t.TempDir(),
		turn:        make(chan struct{}, sessionTurnCapacity),
	})

	return agent, process, id
}

func TestFailedDeleteTeardownKeepsOneImmutableResult(t *testing.T) {
	refused := errors.New("close the contained tree")
	agent, process, id := newPartialDeleteAgent(t, refused)

	t.Cleanup(func() { _ = agent.Close() })

	_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
	require.ErrorIs(t, err, refused)
	require.Equal(t, 1, process.closeCalls)

	_, err = agent.LoadSession(t.Context(), LoadSessionRequest(id, "/cwd"))
	requireInvalidParams(t, err)

	listed, err := agent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, listed.Sessions, "a tombstoned id stayed visible after its teardown failed")

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
	require.ErrorIs(t, err, refused)
	require.Equal(t, 1, process.closeCalls)

	agent.mu.Lock()
	require.Contains(t, agent.sessions, id)
	agent.mu.Unlock()
}

func TestAgentCloseJoinsAPartiallyDeletedSessionResult(t *testing.T) {
	refused := errors.New("close the contained tree")
	agent, process, id := newPartialDeleteAgent(t, refused)

	_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
	require.ErrorIs(t, err, refused)

	agent.mu.Lock()
	require.Contains(t, agent.sessions, id)
	agent.mu.Unlock()

	require.ErrorIs(t, agent.Close(), refused)
	require.Equal(t, 1, process.closeCalls)
}

type settlementDeleteStore struct {
	SessionStore
	session *agentSession
	deleted chan bool
}

func (s *settlementDeleteStore) Delete(ctx context.Context, key SessionKey) error {
	s.session.commitMu.Lock()
	fenced := s.session.persistFenced
	s.session.commitMu.Unlock()
	s.deleted <- fenced

	return s.SessionStore.Delete(ctx, key)
}

// TestDeleteTombstonesBeforeItFencesOrWaitsForSettlement pins the fixed delete
// order. The tombstone is the first rung: it reaches the store
// while a settlement is still running and while the session's persistence is
// still unfenced, because store tombstone finality — not a wait — is what stops
// a commit in flight from recreating the row. Only then is the incarnation
// fenced, and the settlement's own failure is reported after the tombstone is
// durable and the id already hidden.
func TestDeleteTombstonesBeforeItFencesOrWaitsForSettlement(t *testing.T) {
	base := NewInMemorySessionStore()
	store := &settlementDeleteStore{SessionStore: base, deleted: make(chan bool, 1)}
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	session := &agentSession{
		agent:       agent,
		id:          "id",
		proc:        newStubProcess(false),
		sessionRoot: t.TempDir(),
	}
	attachTestNativeBoundary(session)
	agent.sessions["id"] = session
	session.openSettlement()
	store.session = session

	settleErr := errors.New("settlement commit failed")
	done := make(chan error, 1)

	go func() {
		_, err := agent.UnstableDeleteSession(context.Background(), acp.UnstableDeleteSessionRequest{SessionId: "id"})
		done <- err
	}()

	// The tombstone lands with the settlement still open and the session still
	// unfenced: nothing about the delete waits on the turn it is deleting.
	require.False(t, <-store.deleted, "the tombstone is written before the fence, not after it")

	session.mu.Lock()
	settlement := session.settlement
	session.mu.Unlock()
	<-settlement.waiting

	require.True(t, agent.isDeleted("id"), "the id is hidden from the moment the tombstone lands")

	session.commitMu.Lock()
	fenced := session.persistFenced
	session.commitMu.Unlock()
	require.True(t, fenced, "the durable tombstone ends the incarnation with it")

	session.completeSettlement(settleErr)

	require.ErrorIs(t, <-done, settleErr, "partial cleanup is reported after the tombstone is durable")
	require.Contains(t, agent.sessions, acp.SessionId("id"),
		"the hidden id retains the exact immutable close owner")
}

// TestDeleteNeverWedgesBehindALivePrompt pins the reason the tombstone comes
// first. The close-and-cancel rung is what ends a live turn, and it runs after
// the tombstone: a delete issued during a live prompt cancels that prompt rather
// than waiting for it. With TurnTimeout defaulting to zero, a delete that waited
// first would have nothing inside the wrapper to bound it.
func TestDeleteNeverWedgesBehindALivePrompt(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "wedge-id"}
	agent := newStubClientAgent(t, client)
	agent.setConnection(newDirectAgentClient())

	response, err := agent.NewSession(t.Context(), NewSessionRequest("/cwd"))
	require.NoError(t, err)

	session, err := agent.session(response.SessionId)
	require.NoError(t, err)

	promptDone := make(chan struct{})

	go func() {
		defer close(promptDone)

		_, _ = agent.Prompt(context.Background(), acp.PromptRequest{
			SessionId: response.SessionId,
			Prompt:    []acp.ContentBlock{acp.TextBlock("hi")},
			Meta:      map[string]any{routeMetaKey: map[string]any{routeFieldVer: 1, routeFieldTurn: "n1"}},
		})
	}()

	require.Eventually(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()

		return session.settlement != nil
	}, 5*time.Second, time.Millisecond, "the turn armed its settlement latch")

	deleted := make(chan error, 1)

	go func() {
		_, delErr := agent.UnstableDeleteSession(context.Background(),
			acp.UnstableDeleteSessionRequest{SessionId: response.SessionId})
		deleted <- delErr
	}()

	select {
	case <-deleted:
	case <-time.After(30 * time.Second):
		t.Fatal("delete wedged behind the live prompt it never cancelled")
	}

	<-promptDone

	require.True(t, agent.isDeleted(response.SessionId))
	require.NotContains(t, agent.sessions, response.SessionId)
}

// refusingDeleteStore refuses the tombstone write and serves every other
// operation from the store it wraps.
type refusingDeleteStore struct {
	SessionStore
	deleteErr error
}

func (s *refusingDeleteStore) Delete(ctx context.Context, key SessionKey) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}

	return s.SessionStore.Delete(ctx, key)
}

// TestRefusedDeleteLeavesTheSessionTheHostsAndUnfenced pins the delete that did
// not tombstone: it changes nothing. The session stays in the active map, stays
// listed, stays promptable, and — the part that is not merely cosmetic — keeps
// its persistence unfenced, so every later commit it makes is durable rather
// than silently dropped behind a success the wrapper reported anyway. A caller
// that cancels its own delete request is the same case: the tombstone write is
// caller-context-bound, so it is refused, and refusing changes nothing.
func TestRefusedDeleteLeavesTheSessionTheHostsAndUnfenced(t *testing.T) {
	for _, test := range []struct {
		name    string
		request func(*Agent, *refusingDeleteStore) error
	}{
		{
			name: "the store refuses the tombstone",
			request: func(agent *Agent, store *refusingDeleteStore) error {
				store.deleteErr = errors.New("store refused the tombstone")

				_, err := agent.UnstableDeleteSession(context.Background(),
					acp.UnstableDeleteSessionRequest{SessionId: "id"})

				return err
			},
		},
		{
			name: "the caller cancels its own delete",
			request: func(agent *Agent, _ *refusingDeleteStore) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := agent.UnstableDeleteSession(ctx,
					acp.UnstableDeleteSessionRequest{SessionId: "id"})

				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := NewInMemorySessionStore()
			store := &refusingDeleteStore{SessionStore: base}
			agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))

			root := t.TempDir()
			sessionFile := filepath.Join(root, "session.jsonl")
			require.NoError(t, os.WriteFile(sessionFile, []byte("{\"row\":1}\n"), 0o600))

			session := &agentSession{
				agent:           agent,
				id:              "id",
				proc:            newStubProcess(false),
				sessionRoot:     root,
				sessionFilePath: sessionFile,
			}
			agent.sessions["id"] = session

			require.Error(t, test.request(agent, store), "a refused tombstone fails the delete")

			// The session is still the host's.
			require.Contains(t, agent.sessions, acp.SessionId("id"))
			require.False(t, agent.isDeleted("id"))

			resolved, sessErr := agent.session("id")
			require.NoError(t, sessErr)
			require.Same(t, session, resolved)

			listed, listErr := agent.ListSessions(context.Background(), acp.ListSessionsRequest{})
			require.NoError(t, listErr)
			require.Condition(t, func() bool {
				for _, info := range listed.Sessions {
					if info.SessionId == "id" {
						return true
					}
				}

				return false
			}, "the refused delete leaves the id visible to list")

			// And its persistence still works: a later commit is durable.
			session.commitMu.Lock()
			fenced := session.persistFenced
			session.commitMu.Unlock()
			require.False(t, fenced, "a delete that did not tombstone fences nothing")

			require.NoError(t, session.commitMirror(context.Background()))
			require.NoError(t, session.commitLifecycleBoundary(context.Background(), lifecycleBoundaryRecord{
				StreamID:    "s",
				NativeRows:  1,
				NativeState: nativeStateCommitted,
			}))

			rows, loadErr := base.Load(context.Background(), SessionKey{SessionID: "id"})
			require.NoError(t, loadErr)
			require.NotEmpty(t, rows, "the live session's native rows are still committed")

			journal, journalErr := base.Load(context.Background(),
				SessionKey{SessionID: "id", Subpath: SessionStoreLifecycleSubpath})
			require.NoError(t, journalErr)
			require.NotEmpty(t, journal, "the live session's boundary records are still committed")
		})
	}
}

// gatedLoadStore blocks the first lifecycle-subpath Load after the underlying
// read returns, which parks a restore inside its own preparation window.
type gatedLoadStore struct {
	SessionStore
	gate    chan struct{}
	release chan struct{}
	armed   bool
	mu      sync.Mutex
}

func (s *gatedLoadStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	entries, err := s.SessionStore.Load(ctx, key)

	s.mu.Lock()
	armed := s.armed && key.Subpath == SessionStoreLifecycleSubpath
	if armed {
		s.armed = false
	}
	s.mu.Unlock()

	if armed {
		close(s.gate)
		<-s.release
	}

	return entries, err
}

// TestResumeRacingDeleteResurrectsNothing pins the install re-check. A resume
// that passed its entry check and prepared a whole replacement still loses to a
// delete that completed while it was preparing: the tombstone is re-read under
// the same lock that installs, the prepared replacement is torn down, the resume
// answers unknown-session, and nothing — the active map, session/list,
// session-scoped resolution, or a durable row — reports the id as alive again.
func TestResumeRacingDeleteResurrectsNothing(t *testing.T) {
	entries := []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"resume-id","cwd":"/cwd"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleUser, Content: json.RawMessage(`[{"type":"text","text":"hi"}]`)}),
	}
	base := NewInMemorySessionStore()
	require.NoError(t, base.Append(t.Context(), SessionKey{SessionID: "resume-id"}, entries))
	appendLifecycleBoundaryForRows(t, base, "resume-id", len(entries))

	store := &gatedLoadStore{
		SessionStore: base,
		gate:         make(chan struct{}),
		release:      make(chan struct{}),
		armed:        true,
	}

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "resume-id"}
	agent := newStubClientAgent(t, client, WithSessionStore(store))
	agent.setConnection(newDirectAgentClient())

	resumed := make(chan error, 1)

	go func() {
		_, err := agent.ResumeSession(context.Background(), ResumeSessionRequest("resume-id", "/cwd"))
		resumed <- err
	}()

	<-store.gate

	_, delErr := agent.UnstableDeleteSession(context.Background(),
		acp.UnstableDeleteSessionRequest{SessionId: "resume-id"})
	require.NoError(t, delErr, "the delete completes while the resume is mid-flight")
	require.True(t, agent.isDeleted("resume-id"))

	close(store.release)

	requireUnknownSession(t, <-resumed)

	agent.mu.Lock()
	installed := agent.sessions["resume-id"]
	agent.mu.Unlock()
	require.Nil(t, installed, "no live session is installed under a tombstoned id")

	_, sessErr := agent.session("resume-id")
	requireUnknownSession(t, sessErr)

	listed, listErr := agent.ListSessions(context.Background(), acp.ListSessionsRequest{})
	require.NoError(t, listErr)

	for _, info := range listed.Sessions {
		require.NotEqual(t, acp.SessionId("resume-id"), info.SessionId, "a deleted id is invisible to list")
	}

	rows, loadErr := base.Load(context.Background(), SessionKey{SessionID: "resume-id"})
	require.NoError(t, loadErr)
	require.Empty(t, rows, "no durable row survives under the deleted key")
}

// TestInstallUnderATombstoneTearsDownThePreparedReplacement pins the
// install-lock verdict directly, without the timing the race above needs: a
// fully prepared session offered for an id a delete already tombstoned is closed
// rather than installed, and the deletion marker is not cleared by the attempt.
func TestInstallUnderATombstoneTearsDownThePreparedReplacement(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))

	agent.mu.Lock()
	agent.deleted["gone"] = struct{}{}
	agent.mu.Unlock()

	proc := newStubProcess(false)
	prepared := &agentSession{agent: agent, id: "gone", proc: proc, sessionRoot: t.TempDir()}
	attachTestNativeBoundary(prepared)

	requireUnknownSession(t, agent.storeStartedSession(t.Context(), prepared))

	agent.mu.Lock()
	_, installed := agent.sessions["gone"]
	_, stillDeleted := agent.deleted["gone"]
	agent.mu.Unlock()

	require.False(t, installed, "the prepared replacement is never installed")
	require.True(t, stillDeleted, "installing never clears the deletion marker")
	require.Positive(t, proc.closeCalls, "the prepared replacement is torn down")

	// A teardown that itself fails is logged and still refuses the install:
	// the verdict about the id is the tombstone's, not the teardown's.
	failing := &agentSession{
		agent: agent, id: "gone", proc: newFailingCloseProcess(), sessionRoot: t.TempDir(),
		turn: make(chan struct{}, sessionTurnCapacity),
	}
	attachTestNativeBoundary(failing)
	requireUnknownSession(t, agent.storeStartedSession(t.Context(), failing))
}

// TestListHidesATombstonedIDStillHeldInTheActiveMap pins the active half of the
// hiding rule. The store half filters tombstoned keys on its own; the active
// half must answer on the same terms, so an id something still holds in the map
// after a delete is invisible to session/list rather than listed as live.
func TestListHidesATombstonedIDStillHeldInTheActiveMap(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	t.Cleanup(func() { _ = agent.Close() })

	held := &agentSession{agent: agent, id: "held", cwd: "/cwd"}
	live := &agentSession{agent: agent, id: "live", cwd: "/cwd"}

	agent.mu.Lock()
	agent.sessions["held"] = held
	agent.sessions["live"] = live
	agent.deleted["held"] = struct{}{}
	agent.mu.Unlock()

	listed, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.NoError(t, err)

	ids := make([]acp.SessionId, 0, len(listed.Sessions))
	for _, info := range listed.Sessions {
		ids = append(ids, info.SessionId)
	}

	require.Equal(t, []acp.SessionId{"live"}, ids)
}

// TestDeleteBoundaryIsObserved pins the observer span every other boundary
// handler opens. session/delete is a boundary like close: a host tracing its
// lifecycle must see the delete it issued, with its own outcome, rather than a
// hole between the last update and the id disappearing.
func TestDeleteBoundaryIsObserved(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))),
	)

	_, err := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: "missing"})
	require.NoError(t, err, "deleting an unknown session silently succeeds")

	methods := make([]string, 0, len(recorder.Ended()))

	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if attr.Key == "acp.method" {
				methods = append(methods, attr.Value.AsString())
			}
		}
	}

	require.Contains(t, methods, "session/delete")
}
