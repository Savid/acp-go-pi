package piacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

var agentDirExplicitResources = func(dir pi.AgentDir) (pi.ExplicitResources, error) {
	return dir.ExplicitResources()
}

var agentSessionHandoffNativeTree = handoffGeneratedNativeTree

const modelFieldUnknown = "unknown"

// NewSession creates and starts a pi RPC session.
func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/new")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	metaOptions, err := piOptionsFromMeta(params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	additionalDirectories := sessionAdditionalDirectories(params.AdditionalDirectories)
	if validationErr := validateSessionStartPaths(params.Cwd, additionalDirectories); validationErr != nil {
		return acp.NewSessionResponse{}, validationErr
	}

	session, err := a.startAndStoreSession(ctx, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: additionalDirectories,
		McpServers:            params.McpServers,
		MetaOptions:           metaOptions,
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	})
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	resp = acp.NewSessionResponse{
		SessionId:     session.id,
		ConfigOptions: sessionConfigOptions(session),
	}

	a.publishSessionOpenInline(ctx, session)

	return resp, nil
}

// ResumeSession restores a pi session without replaying previous updates.
func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/resume")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	session, entries, started, err := a.restoreSession(ctx, params.SessionId, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: sessionAdditionalDirectories(params.AdditionalDirectories),
		McpServers:            params.McpServers,
		ResumeID:              string(params.SessionId),
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	}, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	if emitErr := session.emitNativeMessageIdentity(ctx, terminalAssistantMessageID(entries)); emitErr != nil {
		if started {
			a.removeSession(ctx, params.SessionId, session)
		}

		return acp.ResumeSessionResponse{}, emitErr
	}

	session.emitCurrentUsageUpdate(ctx)

	resp = acp.ResumeSessionResponse{
		ConfigOptions: sessionConfigOptions(session),
	}

	a.publishSessionOpenInline(ctx, session)

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

	a.publishSessionOpenInline(ctx, session)

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
		return nil, nil, false, err
	}

	start.MetaOptions = metaOptions

	if validationErr := validateSessionStartPaths(start.Cwd, start.AdditionalDirectories); validationErr != nil {
		return nil, nil, false, validationErr
	}

	if a.isDeleted(sessionID) {
		return nil, nil, false, unknownSessionError()
	}

	entries, boundary, err := a.loadCurrentStoreEntries(ctx, string(sessionID))
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
	start.PriorBoundary = boundary

	session, err := a.startAndStoreSession(ctx, start)
	if err != nil {
		return nil, nil, false, err
	}

	return session, entries, true, nil
}

// ListSessions lists active sessions and stored pi sessions.
func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (resp acp.ListSessionsResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/list")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.ListSessionsResponse{}, refusal
	}

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

	_, err = parseInboundTurnRoute(params.Meta)
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

	err = session.cancelRouted(ctx, params.Meta)

	return err
}

// deleteSessionSettlement serializes a delete after the settlement that could
// still be writing, then fences every later durable write. A tombstone written
// while a commit was in flight would be recreated by that commit; fencing after
// the settlement completes is what makes the delete final.
func (a *Agent) deleteSessionSettlement(session *agentSession) error {
	if session == nil {
		return nil
	}

	settleErr := session.awaitSettlement()
	session.fencePersistence()

	return settleErr
}

// CloseSession closes a pi session process and removes it from the active
// map.
func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (resp acp.CloseSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/close")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.CloseSessionResponse{}, refusal
	}

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	// Detaching before teardown closes the window where a prompt could still
	// resolve this id and relaunch the process being closed.
	if a.detachSession(params.SessionId, session) {
		a.observe.AddActiveSession(ctx, -1)
	}

	_ = session.cancelForClose(ctx)

	closeErr := session.Close(ctx)
	if closeErr != nil {
		return acp.CloseSessionResponse{}, closeErr
	}

	return acp.CloseSessionResponse{}, nil
}

// UnstableDeleteSession implements ACP session/delete. The delete serializes
// after the addressed session's full settlement and fences its persistence
// before the tombstone lands, so no write that was still in flight can recreate
// the row the tombstone removed. The tombstone is durable before the native
// state and the active session are torn down, and the session is hidden from
// list, load, and resume from that moment on.
func (a *Agent) UnstableDeleteSession(
	ctx context.Context,
	params acp.UnstableDeleteSessionRequest,
) (acp.UnstableDeleteSessionResponse, error) {
	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.UnstableDeleteSessionResponse{}, refusal
	}

	a.mu.Lock()
	session := a.sessions[params.SessionId]
	a.mu.Unlock()

	var cleanupErr error

	if settleErr := a.deleteSessionSettlement(session); settleErr != nil {
		cleanupErr = errors.Join(cleanupErr, settleErr)
	}

	if err := a.sessionStore().Delete(ctx, SessionKey{SessionID: string(params.SessionId)}); err != nil {
		return acp.UnstableDeleteSessionResponse{}, errors.Join(cleanupErr, err)
	}

	a.mu.Lock()
	if a.sessions[params.SessionId] == session && session != nil {
		delete(a.sessions, params.SessionId)
	}

	a.deleted[params.SessionId] = struct{}{}
	a.mu.Unlock()

	if session != nil {
		_ = session.cancelForClose(ctx)
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

// detachSession removes an id from the active map only while it still resolves
// to this exact session. A replacement stored under the same id belongs to a
// different lifecycle, and a closer of the superseded session must never evict
// it.
func (a *Agent) detachSession(sessionID acp.SessionId, session *agentSession) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sessions[sessionID] != session {
		return false
	}

	delete(a.sessions, sessionID)

	return true
}

func (a *Agent) removeSession(ctx context.Context, sessionID acp.SessionId, session *agentSession) {
	if a.detachSession(sessionID, session) {
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

func (a *Agent) startAndStoreSession(ctx context.Context, start sessionStart) (*agentSession, error) {
	if err := a.beginNativeConstruction(); err != nil {
		return nil, err
	}
	defer a.endNativeConstruction()

	session, err := a.startSession(ctx, start)
	if err != nil {
		return nil, err
	}

	if err := a.storeStartedSession(ctx, session); err != nil {
		return nil, err
	}

	return session, nil
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

	return caps.Form != nil
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
	defer func() { a.recordNativeContainment(err) }()

	// Every session-establishing method fails here on agent configuration a
	// session cannot start under.
	if configErr := a.sessionStartConfigurationError(); configErr != nil {
		return nil, configErr
	}

	readinessStarted := time.Now()
	versionErr := a.ensureVersion(ctx)
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery, RuntimeStartupReadiness, readinessStarted, versionErr)

	if versionErr != nil {
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

	scratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	var (
		dirs          sessionDirs
		nativeRelease func()
		browserShim   *pi.BrowserShim
		residence     *pi.SessionResidence
	)

	keepScratch := false
	defer func() {
		if !keepScratch {
			err = finalizeSessionRuntimeResources(
				err, nativeRelease, dirs.SessionRoot, scratchRelease, browserShim, residence,
			)
		}
	}()

	dirs, browserShim, err = a.createSessionRuntime()
	if err != nil {
		return nil, err
	}

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

	configurationStarted := time.Now()

	var mcpConfig *pi.MCPConfig

	includeMCP := len(start.McpServers) > 0
	if includeMCP {
		config := mcpConfigForServers(start.McpServers)
		mcpConfig = &config
	}

	residence, residenceFiles, err := pi.CreateSessionResidence(dirs.AgentDir, mcpConfig)
	if err != nil {
		return nil, err
	}

	extensionPaths := residenceFiles.ExtensionPaths

	permission := start.MetaOptions.Permission
	if permission == "" {
		permission = pi.PermissionModeAsk
	}

	extraPathDirs := slices.Clone(start.MetaOptions.ExtraPathDirs)

	managedEnv := map[string]string{
		pi.EnvPermissionMode: permission,
		envAgentVersion:      a.options.AgentVersion,
	}
	if len(extraPathDirs) > 0 {
		managedEnv[pi.EnvExtraPathDirs] = strings.Join(extraPathDirs, string(os.PathListSeparator))
	}

	if includeMCP {
		managedEnv[pi.EnvMCPConfig] = residenceFiles.MCPConfigPath
	}

	env := pi.ComposeEnvironment(
		a.options.Env,
		start.MetaOptions.Env,
		managedEnv,
		a.observe.InjectTraceEnv(ctx, nil),
	)

	// The requested model reaches this session's pi child on the per-session
	// set_model command below. It is never written to settings.json: a durable
	// home puts that file at one path shared by every concurrent session, so a
	// model recorded there would decide some other session's startup model.
	// What that file may hold at launch is the operator baseline, restored
	// below.
	agentDir := pi.AgentDir{
		Root:      dirs.AgentDir,
		SeedFiles: a.options.SeedFiles,
	}
	if writeErr := agentDir.Write(); writeErr != nil {
		observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupConfiguration, configurationStarted, writeErr)

		var seedErr *pi.SeedFileError
		if errors.As(writeErr, &seedErr) {
			return nil, unsupportedField("seedFiles." + seedErr.Name)
		}

		return nil, writeErr
	}

	// Reconciled after the seed write, so an operator-seeded settings.json is
	// the baseline this and every later launch starts from.
	if reconcileErr := a.reconcileHomeStartupDefaults(dirs.AgentDir); reconcileErr != nil {
		observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupConfiguration, configurationStarted, reconcileErr)

		return nil, reconcileErr
	}

	seededResources, err := agentDirExplicitResources(agentDir)
	if err != nil {
		return nil, err
	}

	if handoffErr := agentSessionHandoffNativeTree(dirs.Root, a.nativeOwnershipIsolation()); handoffErr != nil {
		return nil, handoffErr
	}

	// Load operator-seeded extensions before wrapper-owned extensions so the
	// wrapper's reserved question tool and correlation hooks cannot be
	// replaced by a seed with the same registration name.
	extensionPaths = append(seededResources.Extensions, extensionPaths...)

	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupConfiguration, configurationStarted, nil)

	spec := pi.LaunchSpec{
		ExecutablePath:      executable,
		AgentDir:            dirs.AgentDir,
		SessionDir:          dirs.SessionDir,
		SessionPath:         hydratedPath,
		ExtensionPaths:      extensionPaths,
		SkillPaths:          seededResources.Skills,
		PromptTemplatePaths: seededResources.PromptTemplates,
		Env:                 env,
		ExtraPathDirs:       extraPathDirs,
		Cwd:                 start.Cwd,
		BrowserShim:         browserShim,
	}

	spec.Containment, err = a.containmentSpecForRoot(scratchParent(a.options.ScratchDir), dirs.Root, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	// The pi child must outlive the lifecycle request that spawns it: its
	// launch context is detached so the request-scoped cancel cannot kill the
	// session's long-lived process. Teardown is owned by the shutdown ladder.
	startCtx, finishStart := a.observe.StartPiProcess(context.WithoutCancel(ctx), "start")

	nativeRelease, err = acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	spawnStarted := time.Now()
	proc, client, processRoot, err := a.startTrackedPiProcess(startCtx, spec)
	observeRuntimeStartupStage(startCtx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSpawn, spawnStarted, err)

	finishStart(err)

	if err != nil {
		return nil, a.nativeStartFailure(ctx, failureCauseProcessExit, err, nil)
	}

	session = &agentSession{
		agent:                 a,
		cwd:                   start.Cwd,
		additionalDirectories: slices.Clone(start.AdditionalDirectories),
		fingerprint:           sessionStartFingerprint(start),
		launch:                spec,
		sessionRoot:           dirs.SessionRoot,
		browserShim:           browserShim,
		residence:             residence,
		permissionMode:        permission,
		autoRetry:             start.MetaOptions.AutoRetry,
		mcpRefreshPending:     includeMCP,
		proc:                  proc,
		client:                client,
		turn:                  make(chan struct{}, sessionTurnCapacity),
		rawMessages:           start.RawMessages,
		nativeRootRelease:     nativeRelease,
		scratchRootRelease:    scratchRelease,
		providerProcessRoot:   processRoot,
	}

	// The cleanup defer must hold its own reference: failure paths return a
	// nil session, which resets the named return before the defer runs.
	created := session
	started := true

	defer func() {
		if started && err != nil {
			created.stopPump()

			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), sessionShutdownTimeout)
			shutdownErr := proc.Shutdown(shutdownCtx)

			cancelShutdown()

			closeErr := proc.Close()
			cleanupErr := errors.Join(shutdownErr, closeErr)
			created.retireProviderProcess(context.Background(), closeErr)

			err = errors.Join(err, cleanupErr)
		}
	}()

	readinessStarted = time.Now()
	err = client.Start(context.WithoutCancel(ctx))
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupReadiness, readinessStarted, err)

	if err != nil {
		return nil, err
	}

	session.startPump(client)

	sessionStarted := time.Now()
	err = a.setUpNativeSession(ctx, session, start, modelRef, hasModel)
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSession, sessionStarted, err)

	if err != nil {
		return nil, err
	}

	started = false
	keepScratch = true

	processRoot.observe(ctx, proc)

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

			return a.nativeStartFailure(ctx, failureCauseTransport, err, proc)
		}

		if cancelled {
			return a.nativeStartFailure(ctx, failureCauseTransport, errors.New("pi cancelled the clone"), proc)
		}
	}

	// Auto-retry is off unless the session opted in via _meta.pi.options, so
	// native failures surface once, immediately, with the real cause by
	// default; opted-in sessions let pi absorb transient provider errors.
	if err := client.SetAutoRetry(ctx, start.MetaOptions.AutoRetry); err != nil {
		return a.nativeStartFailure(ctx, failureCauseTransport, err, proc)
	}

	state, err := client.GetState(ctx)
	if err != nil {
		return a.nativeStartFailure(ctx, failureCauseTransport, err, proc)
	}

	if start.ResumeID != "" && !start.ForkSession && state.SessionID != start.ResumeID {
		return a.nativeStartFailure(ctx, failureCauseTransport, fmt.Errorf("native session id drift: expected %s, got %s", start.ResumeID, state.SessionID), proc)
	}

	session.id = acp.SessionId(state.SessionID)

	session.lcMu.Lock()
	session.lc.vacancyProven = start.PriorBoundary.VacancyProven
	session.lcMu.Unlock()

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
			return a.nativeStartFailure(ctx, failureCauseTransport, levelErr, proc)
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
				a.log.ErrorContext(ctx, "pi rejected the configured model", slog.String("message", commandErr.Message))

				return unsupportedField(metaOptionPath(metaModelKey))
			}

			return a.nativeStartFailure(ctx, failureCauseTransport, setErr, proc)
		}

		session.mu.Lock()
		session.model = modelRef.Provider + "/" + selected.ID
		session.contextWindowSize = selected.ContextWindow
		session.mu.Unlock()
	}

	models, err := client.GetAvailableModels(ctx)
	if err != nil {
		return a.nativeStartFailure(ctx, failureCauseTransport, err, proc)
	}

	commands, err := client.GetCommands(ctx)
	if err != nil {
		return a.nativeStartFailure(ctx, failureCauseTransport, err, proc)
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
		return pi.ModelRef{}, false, unsupportedField(optionFieldDefaultModel)
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
		return nil, nil, acp.NewInvalidParams(map[string]any{jsonFieldCursor: "invalid cursor"})
	}

	if offset > len(sessions) {
		return nil, nil, acp.NewInvalidParams(map[string]any{jsonFieldCursor: "cursor is past end"})
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
