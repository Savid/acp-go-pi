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

// TestSameSessionIDKeepsOneLinearizedOwner proves the close-versus-same-id
// race: a load, resume, or fork that stores a replacement under a live id takes
// ownership of that id, the superseded session is closed exactly once, and its
// closer can never evict the replacement from any removal site.
func TestSameSessionIDKeepsOneLinearizedOwner(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	superseded := &agentSession{
		agent: agent, id: "id", proc: newStubProcess(false), sessionRoot: t.TempDir(),
	}

	agent.mu.Lock()
	agent.sessions[superseded.id] = superseded
	agent.mu.Unlock()

	replacement := &agentSession{
		agent: agent, id: "id", proc: newStubProcess(false), sessionRoot: t.TempDir(),
	}
	require.NoError(t, agent.storeStartedSession(t.Context(), replacement))

	supersededProcess, ok := superseded.proc.(*stubProcess)
	require.True(t, ok)
	require.Equal(t, 1, supersededProcess.closeCalls)

	agent.mu.Lock()
	require.Same(t, replacement, agent.sessions["id"])
	agent.mu.Unlock()

	require.False(t, agent.detachSession("id", superseded))
	agent.removeSession(t.Context(), "id", superseded)

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

// TestStartSessionRefusesAnUnusableSessionResidence proves a session that
// cannot get a private residence inside its agent directory never launches: the
// residence root is blocked by a file, so no session can fall back to writing
// its MCP servers and headers at a path another session also addresses.
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
		agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
			return pi.DefaultMinimumVersion, nil
		}

		return agent
	}

	originalMkdirTemp := materializeMkdirTemp
	originalHandoff := agentSessionHandoffNativeTree
	t.Cleanup(func() {
		materializeMkdirTemp = originalMkdirTemp
		agentSessionHandoffNativeTree = originalHandoff
	})
	agent := baseAgent()
	materializeMkdirTemp = func(string, string) (string, error) { return "", errors.New("session root") }
	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
	materializeMkdirTemp = originalMkdirTemp

	agent = baseAgent()
	agentSessionHandoffNativeTree = func(string, *ProcessIsolation) error { return errors.New("handoff") }
	_, err = agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.ErrorContains(t, err, "handoff")
	agentSessionHandoffNativeTree = originalHandoff

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
		testProcessIsolationOption(), WithExecutablePath("/fake/pi"),
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
