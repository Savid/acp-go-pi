package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	require.Len(t, connection.notifications, 1)
	require.NotNil(t, connection.notifications[0].Update.SessionInfoUpdate)
	require.Equal(t, messageID,
		anyMap(t, connection.notifications[0].Meta[piMetaKey])[jsonFieldMessageID])

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
	for _, key := range []string{"NODE_OPTIONS", "BASH_ENV", "ENV", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "BAD-NAME"} {
		t.Run(key, func(t *testing.T) {
			client := newStubPiClient()
			agent := newStubClientAgent(t, client, WithEnv(map[string]string{key: "unsafe"}))
			starts := 0
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				starts++

				return newStubProcess(false), client, nil
			}

			_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
			requireInvalidParams(t, err)
			require.Zero(t, starts)
		})
	}
}

func TestStartSessionOrdersExtraPathDirs(t *testing.T) {
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id"}
	agent := newStubClientAgent(t, client, WithExtraPathDirs("/agent-wide/bin"))

	var launched pi.LaunchSpec
	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		launched = spec

		return newStubProcess(false), client, nil
	}

	session, err := agent.startSession(t.Context(), sessionStart{
		Cwd:         "/cwd",
		MetaOptions: PiOptions{ExtraPathDirs: []string{"/session/bin"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

	require.Equal(t, []string{"/session/bin", "/agent-wide/bin"}, launched.ExtraPathDirs)
	require.NotContains(t, launched.Env, "PATH")
}

func TestStartSessionRejectsUnusableGlobalExtraPathDir(t *testing.T) {
	for _, dir := range []string{"relative/bin", "", "/opt/bin" + string(os.PathListSeparator) + "/srv/bin"} {
		t.Run(dir, func(t *testing.T) {
			client := newStubPiClient()
			agent := newStubClientAgent(t, client, WithExtraPathDirs(dir))
			starts := 0
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				starts++

				return newStubProcess(false), client, nil
			}

			_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
			requireInvalidParams(t, err)
			require.Zero(t, starts)
		})
	}
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

	require.Equal(t, "explicit-key", launched.Env["OPENAI_API_KEY"])
	require.Len(t, launched.ExtensionPaths, 2)
	require.Contains(t, filepath.ToSlash(launched.ExtensionPaths[0]), "/extensions/command.ts")
	require.Equal(t, filepath.Join(launched.AgentDir, pi.BridgeExtensionFileName), launched.ExtensionPaths[1])
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
