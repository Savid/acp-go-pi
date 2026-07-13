package piacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/observer"
	"github.com/savid/acp-go-pi/internal/pi"
)

// errUnknownStoredSession reports that a session id resolves to no stored
// rows; lifecycle methods map it to the uniform unknown-session error.
var errUnknownStoredSession = errors.New("session not found in store")

const modelFieldUnknown = "unknown"

// NewSession creates and starts a pi RPC session.
func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/new")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	metaOptions, err := piOptionsFromMeta(params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, lifecycleMetaError(err)
	}

	additionalDirectories := sessionAdditionalDirectories(params.AdditionalDirectories)
	if validationErr := validateSessionStartPaths(params.Cwd, additionalDirectories); validationErr != nil {
		return acp.NewSessionResponse{}, validationErr
	}

	if openErr := a.ensureOpen(); openErr != nil {
		return acp.NewSessionResponse{}, openErr
	}

	session, err := a.startSession(ctx, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: additionalDirectories,
		McpServers:            params.McpServers,
		MetaOptions:           metaOptions,
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	})
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	if err := a.storeStartedSession(ctx, session); err != nil {
		return acp.NewSessionResponse{}, err
	}

	resp = acp.NewSessionResponse{
		SessionId:     session.id,
		ConfigOptions: sessionConfigOptions(session),
	}

	return resp, nil
}

// ResumeSession restores a pi session without replaying previous updates.
func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/resume")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	session, _, _, err := a.restoreSession(ctx, params.SessionId, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: sessionAdditionalDirectories(params.AdditionalDirectories),
		McpServers:            params.McpServers,
		ResumeID:              string(params.SessionId),
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	}, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	session.emitCurrentUsageUpdate(ctx)

	resp = acp.ResumeSessionResponse{
		ConfigOptions: sessionConfigOptions(session),
	}

	return resp, nil
}

// LoadSession restores a pi session and replays saved history as session
// updates.
func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (resp acp.LoadSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/load")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	session, entries, started, err := a.restoreSession(ctx, params.SessionId, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: sessionAdditionalDirectories(params.AdditionalDirectories),
		McpServers:            params.McpServers,
		ResumeID:              string(params.SessionId),
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	}, params.Meta)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	if replayErr := session.replayStoredSession(ctx, entries); replayErr != nil {
		if started {
			a.removeSession(ctx, params.SessionId, session)
		}

		return acp.LoadSessionResponse{}, replayErr
	}

	session.emitCurrentUsageUpdate(ctx)

	resp = acp.LoadSessionResponse{
		ConfigOptions: sessionConfigOptions(session),
	}

	return resp, nil
}

// restoreSession runs the shared load/resume path: meta and path validation,
// deleted/tombstone checks, active-session reuse, then a store-backed
// hydrate.
func (a *Agent) restoreSession(
	ctx context.Context,
	sessionID acp.SessionId,
	start sessionStart,
	meta map[string]any,
) (*agentSession, []SessionStoreEntry, bool, error) {
	metaOptions, err := piOptionsFromMeta(meta)
	if err != nil {
		return nil, nil, false, lifecycleMetaError(err)
	}

	start.MetaOptions = metaOptions

	if validationErr := validateSessionStartPaths(start.Cwd, start.AdditionalDirectories); validationErr != nil {
		return nil, nil, false, validationErr
	}

	if a.isDeleted(sessionID) {
		return nil, nil, false, unknownSessionError()
	}

	entries, err := a.loadStoreEntries(ctx, a.sessionStore(), SessionKey{SessionID: string(sessionID)})
	if err != nil {
		return nil, nil, false, err
	}

	if session := a.activeSessionForStart(sessionID, start); session != nil {
		return session, entries, false, nil
	}

	if len(entries) == 0 {
		return nil, nil, false, unknownSessionError()
	}

	if openErr := a.ensureOpen(); openErr != nil {
		return nil, nil, false, openErr
	}

	start.HydrateEntries = entries

	session, err := a.startSession(ctx, start)
	if err != nil {
		return nil, nil, false, err
	}

	if storeErr := a.storeStartedSession(ctx, session); storeErr != nil {
		return nil, nil, false, storeErr
	}

	return session, entries, true, nil
}

// ListSessions lists active sessions and stored pi sessions.
func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (resp acp.ListSessionsResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/list")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	if validationErr := validateOptionalAbsolutePath(jsonFieldCwd, params.Cwd); validationErr != nil {
		return acp.ListSessionsResponse{}, validationErr
	}

	a.mu.Lock()

	activeSessions := make(map[acp.SessionId]*agentSession, len(a.sessions))
	for id, session := range a.sessions {
		if params.Cwd != nil && *params.Cwd != session.cwd {
			continue
		}

		activeSessions[id] = session
	}
	a.mu.Unlock()

	active := make([]acp.SessionInfo, 0, len(activeSessions))
	for id, session := range activeSessions {
		active = append(active, session.sessionInfo(id))
	}

	storeSessions, err := a.listStoreSessions(ctx, params)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	sessions := make([]acp.SessionInfo, 0, len(active)+len(storeSessions))
	seen := make(map[acp.SessionId]struct{}, len(active)+len(storeSessions))

	for _, session := range active {
		sessions = append(sessions, session)
		seen[session.SessionId] = struct{}{}
	}

	for _, session := range storeSessions {
		if _, ok := seen[session.SessionId]; ok {
			continue
		}

		sessions = append(sessions, session)
		seen[session.SessionId] = struct{}{}
	}

	paged, nextCursor, err := paginateSessionInfos(sessions, params.Cursor)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	return acp.ListSessionsResponse{Sessions: paged, NextCursor: nextCursor}, nil
}

// Prompt sends a user prompt to pi and streams ACP session updates until the
// run settles.
func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, err error) {
	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	ctx, finish := a.observe.StartPrompt(ctx, params.Meta, session.currentModel())
	defer func() { finish(promptResultForObserver(resp, err, session)) }()

	// A native turn failure (process death, transport, provider, timeout)
	// leaves the session addressable and retriable: it stays in the map, so a
	// follow-up session/prompt relaunches the pi process lazily rather than
	// returning the unknown-session error.
	resp, err = session.Prompt(ctx, params)

	return resp, err
}

// Cancel interrupts an active pi turn for the session.
func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) (err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/cancel")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	session, err := a.session(params.SessionId)
	if err != nil {
		return err
	}

	err = session.Cancel(ctx)

	return err
}

// CloseSession closes a pi session process and removes it from the active
// map.
func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (resp acp.CloseSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/close")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	_ = session.Cancel(ctx)
	closeErr := session.Close(ctx)

	a.mu.Lock()
	_, existed := a.sessions[params.SessionId]
	delete(a.sessions, params.SessionId)
	a.mu.Unlock()

	if existed {
		a.observe.AddActiveSession(ctx, -1)
	}

	if closeErr != nil {
		return acp.CloseSessionResponse{}, closeErr
	}

	return acp.CloseSessionResponse{}, nil
}

// UnstableDeleteSession implements ACP session/delete: durable tombstone
// first, then close and clean up the active session and its native state.
func (a *Agent) UnstableDeleteSession(
	ctx context.Context,
	params acp.UnstableDeleteSessionRequest,
) (acp.UnstableDeleteSessionResponse, error) {
	if err := a.sessionStore().Delete(ctx, SessionKey{SessionID: string(params.SessionId)}); err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	a.mu.Lock()
	session := a.sessions[params.SessionId]
	delete(a.sessions, params.SessionId)
	a.deleted[params.SessionId] = struct{}{}
	a.mu.Unlock()

	var cleanupErr error

	if session != nil {
		_ = session.Cancel(ctx)
		if err := session.Close(ctx); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}

		a.observe.AddActiveSession(ctx, -1)
	}

	if cleanupErr != nil {
		return acp.UnstableDeleteSessionResponse{}, cleanupErr
	}

	return acp.UnstableDeleteSessionResponse{}, nil
}

func (a *Agent) session(sessionID acp.SessionId) (*agentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	session := a.sessions[sessionID]
	if session == nil {
		return nil, unknownSessionError()
	}

	return session, nil
}

func (s *agentSession) currentModel() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.model
}

func (s *agentSession) currentProvider() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	provider, _, found := strings.Cut(s.model, "/")
	if !found {
		return ""
	}

	return provider
}

func promptResultForObserver(resp acp.PromptResponse, err error, session *agentSession) observer.PromptResult {
	result := observer.PromptResult{
		Err:        err,
		Model:      session.currentModel(),
		Provider:   session.currentProvider(),
		StopReason: string(resp.StopReason),
	}
	if resp.Usage == nil {
		return result
	}

	result.InputTokens = resp.Usage.InputTokens
	result.OutputTokens = resp.Usage.OutputTokens
	result.TotalTokens = resp.Usage.TotalTokens

	if resp.Usage.CachedReadTokens != nil {
		result.CachedReadTokens = *resp.Usage.CachedReadTokens
	}

	if resp.Usage.CachedWriteTokens != nil {
		result.CachedWriteTokens = *resp.Usage.CachedWriteTokens
	}

	return result
}

func (a *Agent) removeSession(ctx context.Context, sessionID acp.SessionId, session *agentSession) {
	a.mu.Lock()
	removed := false

	if a.sessions[sessionID] == session {
		delete(a.sessions, sessionID)

		removed = true
	}

	a.mu.Unlock()

	if removed {
		a.observe.AddActiveSession(context.Background(), -1)
	}

	if session != nil {
		if err := session.Close(ctx); err != nil {
			a.log.DebugContext(ctx, "close removed pi session failed", slog.String(jsonFieldError, err.Error()))
		}
	}
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errAgentClosed
	}

	return nil
}

func (a *Agent) storeStartedSession(ctx context.Context, session *agentSession) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		if err := session.Close(ctx); err != nil {
			a.log.DebugContext(ctx, "close rejected pi session failed", slog.String(jsonFieldError, err.Error()))
		}

		return errAgentClosed
	}

	previous := a.sessions[session.id]
	if previous == nil && len(a.sessions) >= a.maxActiveSessions() {
		a.mu.Unlock()

		if err := session.Close(ctx); err != nil {
			a.log.DebugContext(ctx, "close backpressured pi session failed", slog.String(jsonFieldError, err.Error()))
		}

		return backpressureError("active_sessions")
	}

	a.sessions[session.id] = session
	a.mu.Unlock()

	if previous != nil {
		if err := previous.Close(ctx); err != nil {
			a.log.WarnContext(ctx, "close replaced pi session failed", slog.String(jsonFieldError, err.Error()))
		}

		return nil
	}

	a.observe.AddActiveSession(ctx, 1)

	return nil
}

func (a *Agent) connection() agentClient {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

func (a *Agent) clientSupportsFormElicitation() bool {
	a.mu.Lock()
	caps := a.clientCapabilities.Elicitation
	a.mu.Unlock()

	if caps == nil {
		return false
	}

	// A present but empty elicitation object is equivalent to form support.
	return caps.Form != nil || caps.Url == nil
}

func (a *Agent) isDeleted(sessionID acp.SessionId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, ok := a.deleted[sessionID]

	return ok
}

func (a *Agent) activeSessionForStart(id acp.SessionId, start sessionStart) *agentSession {
	fingerprint := sessionStartFingerprint(start)

	a.mu.Lock()
	defer a.mu.Unlock()

	session := a.sessions[id]
	if session == nil || session.fingerprint != fingerprint {
		return nil
	}

	return session
}

func sessionStartFingerprint(start sessionStart) string {
	servers := slices.Clone(start.McpServers)
	slices.SortFunc(servers, func(left, right acp.McpServer) int {
		return strings.Compare(mcpServerName(left), mcpServerName(right))
	})

	data := struct {
		Cwd                   string           `json:"cwd"`
		AdditionalDirectories []string         `json:"additionalDirectories,omitempty"`
		McpServers            []acp.McpServer  `json:"mcpServers,omitempty"`
		MetaOptions           PiOptions        `json:"metaOptions,omitzero"`
		RawMessages           rawMessageConfig `json:"rawMessages,omitzero"`
	}{
		Cwd:                   start.Cwd,
		AdditionalDirectories: slices.Clone(start.AdditionalDirectories),
		McpServers:            servers,
		MetaOptions:           start.MetaOptions,
		RawMessages:           start.RawMessages,
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		return fmt.Sprintf("marshal-error:%T:%v", data, err)
	}

	return string(encoded)
}

func mcpServerName(server acp.McpServer) string {
	switch {
	case server.Http != nil:
		return server.Http.Name
	case server.Sse != nil:
		return server.Sse.Name
	case server.Acp != nil:
		return server.Acp.Name
	case server.Stdio != nil:
		return server.Stdio.Name
	default:
		return ""
	}
}

// unknownSessionError is returned by every session-scoped method when the
// session id cannot be resolved (unknown, not in the store, or tombstoned).
// All such cases share one invalid-params shape.
func unknownSessionError() *acp.RequestError {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: "unknown session",
		jsonFieldField: acpFieldSessionID,
	})
}

// startSession launches one pi RPC process for a session: isolated per-session
// directories, wrapper-owned extensions, MCP config, seeded agent dir, then
// the native setup sequence (auto-retry off, thinking level, model, catalog,
// state, commands).
func (a *Agent) startSession(ctx context.Context, start sessionStart) (session *agentSession, err error) {
	// pi has no native config or auth root, so a configured Home is an
	// unsupported option; every session-establishing method fails here.
	if a.options.Home != "" {
		return nil, unsupportedField(optionFieldHome)
	}

	if versionErr := a.ensureVersion(ctx); versionErr != nil {
		return nil, versionErr
	}

	if mcpErr := validateMCPServers(start.McpServers); mcpErr != nil {
		return nil, mcpErr
	}

	executable, err := a.resolveExecutablePath()
	if err != nil {
		return nil, err
	}

	modelRef, hasModel, err := a.resolveInitialModel(start.MetaOptions)
	if err != nil {
		return nil, err
	}

	dirs, err := a.createSessionDirs()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = materializeRemoveAll(dirs.Root)
		}
	}()

	hydratedPath := ""

	if start.ResumeID != "" {
		if len(start.HydrateEntries) == 0 {
			return nil, errUnknownStoredSession
		}

		hydratedPath, err = writeHydratedSessionFile(dirs, start.ResumeID, start.HydrateEntries)
		if err != nil {
			return nil, err
		}
	}

	includeMCP := len(start.McpServers) > 0

	extensionPaths, err := pi.WriteExtensions(dirs.AgentDir, includeMCP)
	if err != nil {
		return nil, err
	}

	permission := start.MetaOptions.Permission
	if permission == "" {
		permission = pi.PermissionModeAsk
	}

	env := cloneStringMap(a.options.Env)
	if env == nil {
		env = make(map[string]string, len(start.MetaOptions.Env)+3)
	}

	for key, value := range start.MetaOptions.Env {
		env[key] = value
	}

	env[pi.EnvPermissionMode] = permission
	env[envAgentVersion] = a.options.AgentVersion

	if includeMCP {
		configPath, mcpErr := pi.WriteMCPConfig(dirs.AgentDir, mcpConfigForServers(start.McpServers))
		if mcpErr != nil {
			return nil, mcpErr
		}

		env[pi.EnvMCPConfig] = configPath
	}

	env = a.observe.InjectTraceEnv(ctx, env)

	managedSettings := map[string]any(nil)
	if hasModel {
		managedSettings = map[string]any{
			"defaultProvider": modelRef.Provider,
			"defaultModel":    modelRef.ID,
		}
	}

	agentDir := pi.AgentDir{
		Root:            dirs.AgentDir,
		ManagedSettings: managedSettings,
		SeedFiles:       a.options.SeedFiles,
	}
	if writeErr := agentDir.Write(); writeErr != nil {
		var seedErr *pi.SeedFileError
		if errors.As(writeErr, &seedErr) {
			return nil, unsupportedField("seedFiles." + seedErr.Name)
		}

		return nil, writeErr
	}

	spec := pi.LaunchSpec{
		ExecutablePath: executable,
		AgentDir:       dirs.AgentDir,
		SessionDir:     dirs.SessionDir,
		SessionPath:    hydratedPath,
		ExtensionPaths: extensionPaths,
		Env:            env,
		Cwd:            start.Cwd,
	}

	// The pi child must outlive the lifecycle request that spawns it: its
	// launch context is detached so the request-scoped cancel cannot kill the
	// session's long-lived process. Teardown is owned by the shutdown ladder.
	startCtx, finishStart := a.observe.StartPiProcess(context.WithoutCancel(ctx), "start")
	proc, client, err := a.startPiProcess(startCtx, spec)

	finishStart(err)

	if err != nil {
		return nil, err
	}

	session = &agentSession{
		agent:                 a,
		cwd:                   start.Cwd,
		additionalDirectories: slices.Clone(start.AdditionalDirectories),
		fingerprint:           sessionStartFingerprint(start),
		launch:                spec,
		sessionRoot:           dirs.Root,
		permissionMode:        permission,
		autoRetry:             start.MetaOptions.AutoRetry,
		proc:                  proc,
		client:                client,
		turn:                  make(chan struct{}, sessionTurnCapacity),
		rawMessages:           start.RawMessages,
	}

	// The cleanup defer must hold its own reference: failure paths return a
	// nil session, which resets the named return before the defer runs.
	created := session
	started := true

	defer func() {
		if started && err != nil {
			created.stopPump()

			_ = proc.Kill()
			_ = proc.Close()
		}
	}()

	err = client.Start(context.WithoutCancel(ctx))
	if err != nil {
		return nil, err
	}

	session.startPump(client)

	err = a.setUpNativeSession(ctx, session, start, modelRef, hasModel)
	if err != nil {
		return nil, err
	}

	started = false

	return session, nil
}

// setUpNativeSession drives the post-spawn command sequence. Any transport
// failure here means pi exited during startup (for example an MCP server
// connect failure), so errors are recovered into a structured session-start
// failure naming the real stderr cause.
func (a *Agent) setUpNativeSession(
	ctx context.Context,
	session *agentSession,
	start sessionStart,
	modelRef pi.ModelRef,
	hasModel bool,
) error {
	client := session.client
	proc := session.proc

	if start.ForkSession {
		cancelled, err := client.Clone(ctx)
		if err != nil {
			if emptyCloneError(err) {
				return emptyForkSessionError()
			}

			return spawnFailureError(err, proc)
		}

		if cancelled {
			return spawnFailureError(errors.New("pi cancelled the clone"), proc)
		}
	}

	// Auto-retry is off unless the session opted in via _meta.pi.options, so
	// native failures surface once, immediately, with the real cause by
	// default; opted-in sessions let pi absorb transient provider errors.
	if err := client.SetAutoRetry(ctx, start.MetaOptions.AutoRetry); err != nil {
		return spawnFailureError(err, proc)
	}

	state, err := client.GetState(ctx)
	if err != nil {
		return spawnFailureError(err, proc)
	}

	if start.ResumeID != "" && !start.ForkSession && state.SessionID != start.ResumeID {
		return spawnFailureError(fmt.Errorf("native session id drift: expected %s, got %s", start.ResumeID, state.SessionID), proc)
	}

	session.id = acp.SessionId(state.SessionID)

	session.mu.Lock()
	session.sessionFilePath = state.SessionFile
	session.thinkingLevel = state.ThinkingLevel

	if start.ResumeID != "" && !start.ForkSession {
		session.mirroredRows = len(start.HydrateEntries)
	}

	if selected := stateModelRef(state); selected != "" {
		session.model = selected
		if state.Model != nil {
			session.contextWindowSize = state.Model.ContextWindow
		}
	}
	session.mu.Unlock()

	if level := start.MetaOptions.ThinkingLevel; level != "" {
		if levelErr := client.SetThinkingLevel(ctx, level); levelErr != nil {
			return spawnFailureError(levelErr, proc)
		}

		session.mu.Lock()
		session.thinkingLevel = level
		session.mu.Unlock()
	}

	if hasModel {
		selected, setErr := client.SetModel(ctx, modelRef.Provider, modelRef.ID)
		if setErr != nil {
			var commandErr *pi.CommandError
			if errors.As(setErr, &commandErr) {
				return acp.NewInvalidParams(map[string]any{
					jsonFieldError: commandErr.Message,
					jsonFieldField: metaOptionPath(metaModelKey),
				})
			}

			return spawnFailureError(setErr, proc)
		}

		session.mu.Lock()
		session.model = modelRef.Provider + "/" + selected.ID
		session.contextWindowSize = selected.ContextWindow
		session.mu.Unlock()
	}

	models, err := client.GetAvailableModels(ctx)
	if err != nil {
		return spawnFailureError(err, proc)
	}

	commands, err := client.GetCommands(ctx)
	if err != nil {
		return spawnFailureError(err, proc)
	}

	session.mu.Lock()
	session.availableModels = models
	session.availableCommands = commands
	session.mu.Unlock()

	if start.ForkSession {
		if err := session.commitMirror(ctx); err != nil {
			return err
		}
	}

	return nil
}

// stateModelRef normalizes pi's reported model. With no usable credentials pi
// reports a model whose id, name, and provider are all "unknown"; that is
// treated as absent.
func stateModelRef(state pi.SessionState) string {
	if state.Model == nil {
		return ""
	}

	if state.Model.Provider == "" || state.Model.ID == "" {
		return ""
	}

	if state.Model.Provider == modelFieldUnknown && state.Model.ID == modelFieldUnknown {
		return ""
	}

	return state.Model.Provider + "/" + state.Model.ID
}

func (a *Agent) resolveInitialModel(options PiOptions) (pi.ModelRef, bool, error) {
	model := firstNonEmptyString(options.Model, a.options.DefaultModel)
	if model == "" {
		return pi.ModelRef{}, false, nil
	}

	ref, err := pi.ParseModelRef(model)
	if err != nil {
		return pi.ModelRef{}, false, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	return ref, true, nil
}

// emitCurrentUsageUpdate reports the restored session's context usage when pi
// knows it; absence emits nothing rather than a fabricated value.
func (s *agentSession) emitCurrentUsageUpdate(ctx context.Context) {
	stats, err := s.currentClient().GetSessionStats(ctx)
	if err != nil {
		s.agent.log.DebugContext(ctx, "get pi session stats failed", slog.String(jsonFieldError, err.Error()))

		return
	}

	if stats.ContextUsage == nil || stats.ContextUsage.ContextWindow <= 0 {
		return
	}

	used := 0
	if stats.ContextUsage.Tokens != nil {
		used = int(*stats.ContextUsage.Tokens)
	}

	s.mu.Lock()
	s.contextWindowSize = stats.ContextUsage.ContextWindow
	s.mu.Unlock()

	_ = s.emitOptionalUpdates(ctx, []acp.SessionUpdate{{
		UsageUpdate: &acp.SessionUsageUpdate{
			Size: int(stats.ContextUsage.ContextWindow),
			Used: used,
		},
	}})
}

func validateMCPServers(servers []acp.McpServer) error {
	seen := make(map[string]struct{}, len(servers))

	for index, server := range servers {
		var name string

		switch {
		case server.Stdio != nil:
			name = server.Stdio.Name
		case server.Http != nil:
			name = server.Http.Name
		case server.Sse != nil:
			return acp.NewInvalidParams(map[string]any{
				jsonFieldError:  validationUnsupported,
				jsonFieldField:  fmt.Sprintf("mcpServers[%d]", index),
				jsonFieldServer: server.Sse.Name,
			})
		case server.Acp != nil:
			return acp.NewInvalidParams(map[string]any{
				jsonFieldError:  validationUnsupported,
				jsonFieldField:  fmt.Sprintf("mcpServers[%d]", index),
				jsonFieldServer: server.Acp.Name,
			})
		default:
			return acp.NewInvalidParams(map[string]any{
				jsonFieldError: "no_transport",
				jsonFieldField: fmt.Sprintf("mcpServers[%d]", index),
			})
		}

		if strings.TrimSpace(name) == "" {
			return acp.NewInvalidParams(map[string]any{mcpServerNameField(index): validationRequired})
		}

		if _, exists := seen[name]; exists {
			return acp.NewInvalidParams(map[string]any{mcpServerNameField(index): validationDuplicate})
		}

		seen[name] = struct{}{}
	}

	return nil
}

func mcpServerNameField(index int) string {
	return fmt.Sprintf("mcpServers[%d].name", index)
}

// mcpConfigForServers converts validated ACP MCP declarations into the
// per-session config the MCP extension consumes. Names are forwarded
// verbatim.
func mcpConfigForServers(servers []acp.McpServer) pi.MCPConfig {
	config := pi.MCPConfig{Servers: make([]pi.MCPServer, 0, len(servers))}

	for _, server := range servers {
		switch {
		case server.Stdio != nil:
			env := make(map[string]string, len(server.Stdio.Env))
			for _, variable := range server.Stdio.Env {
				env[variable.Name] = variable.Value
			}

			config.Servers = append(config.Servers, pi.MCPServer{
				Name:    server.Stdio.Name,
				Type:    "stdio",
				Command: server.Stdio.Command,
				Args:    append([]string(nil), server.Stdio.Args...),
				Env:     env,
			})
		case server.Http != nil:
			headers := make(map[string]string, len(server.Http.Headers))
			for _, header := range server.Http.Headers {
				headers[header.Name] = header.Value
			}

			config.Servers = append(config.Servers, pi.MCPServer{
				Name:    server.Http.Name,
				Type:    "http",
				URL:     server.Http.Url,
				Headers: headers,
			})
		}
	}

	return config
}

func (a *Agent) listStoreSessions(ctx context.Context, params acp.ListSessionsRequest) ([]acp.SessionInfo, error) {
	listCtx, cancel := context.WithTimeout(ctx, a.sessionStoreLoadTimeout())
	defer cancel()

	listCtx, finishList := a.observe.StartSessionStore(listCtx, "list")
	summaries, err := a.sessionStore().ListSessions(listCtx)
	finishList(err)

	if err != nil {
		return nil, fmt.Errorf("list session store: %w", err)
	}

	infos := make([]acp.SessionInfo, 0, len(summaries))

	for _, summary := range summaries {
		if !validUUIDShape(summary.SessionID) {
			continue
		}

		if a.isDeleted(acp.SessionId(summary.SessionID)) {
			continue
		}

		entries, err := a.loadStoreEntries(ctx, a.sessionStore(), SessionKey{SessionID: summary.SessionID})
		if err != nil {
			return nil, err
		}

		cwd := firstNonEmptyString(summary.Cwd, storeSessionCwd(entries))
		if params.Cwd != nil && strings.TrimSpace(*params.Cwd) != "" && cwd != "" && cwd != *params.Cwd {
			continue
		}

		title := firstNonEmptyString(summary.Title, storeSessionTitle(summary.SessionID, entries))
		updatedAt := time.UnixMilli(summary.UpdatedAtUnixMilli).UTC().Format(time.RFC3339)

		infos = append(infos, acp.SessionInfo{
			SessionId: acp.SessionId(summary.SessionID),
			Cwd:       cwd,
			Title:     &title,
			UpdatedAt: &updatedAt,
			Meta:      cloneAnyMap(summary.Meta),
		})
	}

	return infos, nil
}

func paginateSessionInfos(sessions []acp.SessionInfo, cursor *string) ([]acp.SessionInfo, *string, error) {
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, nil, acp.NewInvalidParams(map[string]any{"cursor": "invalid cursor"})
	}

	if offset > len(sessions) {
		return nil, nil, acp.NewInvalidParams(map[string]any{"cursor": "cursor is past end"})
	}

	end := offset + listSessionsPageSize
	if end >= len(sessions) {
		return sessions[offset:], nil, nil
	}

	next := encodeListCursor(end)

	return sessions[offset:end], &next, nil
}

func decodeListCursor(cursor *string) (int, error) {
	if cursor == nil || *cursor == "" {
		return 0, nil
	}

	data, err := base64.RawURLEncoding.DecodeString(*cursor)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(data))
	if err != nil || offset < 0 {
		return 0, strconv.ErrSyntax
	}

	return offset, nil
}

func encodeListCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}
