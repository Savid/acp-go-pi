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
	"sync"
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

	if openErr := a.publishSessionOpenInline(ctx, session); openErr != nil {
		return acp.NewSessionResponse{}, openErr
	}

	return resp, nil
}

// ResumeSession restores a pi session without replaying previous updates.
func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/resume")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	restored, err := a.restoreSession(ctx, params.SessionId, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: sessionAdditionalDirectories(params.AdditionalDirectories),
		McpServers:            params.McpServers,
		ResumeID:              string(params.SessionId),
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	}, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	defer restored.finish()

	session := restored.session

	if emitErr := session.emitNativeMessageIdentity(ctx, terminalAssistantMessageID(restored.entries)); emitErr != nil {
		var cleanupErr error
		if restored.started {
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.ResumeSessionResponse{}, errors.Join(emitErr, cleanupErr)
	}

	session.emitCurrentUsageUpdate(ctx)

	resp = acp.ResumeSessionResponse{
		ConfigOptions: sessionConfigOptions(session),
	}

	if openErr := a.publishSessionOpenInline(ctx, session); openErr != nil {
		return acp.ResumeSessionResponse{}, openErr
	}

	return resp, nil
}

// LoadSession restores a pi session and replays saved history as session
// updates.
func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (resp acp.LoadSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/load")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	restored, err := a.restoreSession(ctx, params.SessionId, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: sessionAdditionalDirectories(params.AdditionalDirectories),
		McpServers:            params.McpServers,
		ResumeID:              string(params.SessionId),
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	}, params.Meta)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	defer restored.finish()

	session := restored.session

	if replayErr := session.replayStoredSession(ctx, restored.entries); replayErr != nil {
		var cleanupErr error
		if restored.started {
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.LoadSessionResponse{}, errors.Join(replayErr, cleanupErr)
	}

	session.emitCurrentUsageUpdate(ctx)

	resp = acp.LoadSessionResponse{
		ConfigOptions: sessionConfigOptions(session),
	}

	if openErr := a.publishSessionOpenInline(ctx, session); openErr != nil {
		return acp.LoadSessionResponse{}, openErr
	}

	return resp, nil
}

// restoredSession is what the shared load/resume path resolved: the session to
// answer with, the stored prefix to replay, whether this call started it, and
// the gate an active session holds until its replay is done.
type restoredSession struct {
	session *agentSession
	entries []SessionStoreEntry
	started bool
	// release ends the active-session restore gate, if any, and the session's
	// carrier transition. The carrier remains serialized until replay and the
	// opening response boundary have both completed.
	release func()
}

// finish ends whatever the restore held. It is safe to call on a zero value, so
// every exit of a restoring method can defer it unconditionally.
func (r restoredSession) finish() {
	if r.release != nil {
		r.release()
	}
}

// restoreSession runs the shared load/resume path: meta and path validation,
// deleted/tombstone checks, active-session reuse, then a store-backed
// hydrate.
//
// A live session is gated before its prefix is read. The gate excludes an
// in-flight prompt and every new agent-origin cycle in one transition and holds
// until the caller's replay is done, because a transcript that keeps being
// written while it is being read would answer the restore with a snapshot the
// session has already moved past.
func (a *Agent) restoreSession(
	ctx context.Context,
	sessionID acp.SessionId,
	start sessionStart,
	meta map[string]any,
) (restoredSession, error) {
	releaseTransition, err := a.acquireSessionCarrier(ctx, sessionID)
	if err != nil {
		// Cancellation may refuse a queued carrier transition, but an already
		// published poison or close fence still owns the active native
		// generation. Join that exact containment before returning so a
		// cancelled restore cannot outlive its native owner.
		if session := a.activeSession(sessionID); session != nil {
			if fenceErr := session.admissionFenceError(ctx); fenceErr != nil {
				return restoredSession{}, fenceErr
			}
		}

		return restoredSession{}, err
	}

	carrierReleased := false
	carrierTransferred := false
	releaseCarrier := func() {
		if !carrierReleased {
			carrierReleased = true

			releaseTransition()
		}
	}

	defer func() {
		if !carrierTransferred {
			releaseCarrier()
		}
	}()

	metaOptions, configurationPresence, err := piOptionsFromMetaWithConfigurationPresence(meta)
	if err != nil {
		return restoredSession{}, err
	}

	start.MetaOptions = metaOptions

	if validationErr := validateSessionStartPaths(start.Cwd, start.AdditionalDirectories); validationErr != nil {
		return restoredSession{}, validationErr
	}

	if a.isDeleted(sessionID) {
		return restoredSession{}, unknownSessionError()
	}

	if activeConfiguration, ok := a.activeSessionConfiguration(sessionID); ok {
		start.MetaOptions, err = resolveSessionConfiguration(
			start.MetaOptions,
			configurationPresence,
			activeConfiguration,
		)
		if err != nil {
			return restoredSession{}, err
		}
	}

	if session := a.activeSessionForStart(sessionID, start); session != nil {
		restored, restoreErr := a.restoreActiveSession(ctx, sessionID, session)
		if restoreErr != nil {
			return restoredSession{}, restoreErr
		}

		activeRelease := restored.release
		restored.release = func() {
			if activeRelease != nil {
				activeRelease()
			}

			releaseCarrier()
		}
		carrierTransferred = true

		return restored, nil
	}

	// A live session with a different fingerprint is an explicit carrier
	// rotation. The hard cut is containment first: the old native generation
	// remains the only addressable owner until Close proves it contained, then
	// it is detached before any successor process is constructed. A failed
	// close leaves that exact predecessor installed with its immutable result.
	if previous := a.activeSession(sessionID); previous != nil {
		if closeErr := previous.Close(ctx); !nativeContainmentComplete(closeErr) {
			a.retainIncompleteSession(previous, closeErr)

			return restoredSession{}, closeErr
		}

		if a.detachSession(sessionID, previous) {
			a.observe.AddActiveSession(ctx, -1)
		}
	}

	entries, boundary, err := a.loadCurrentStoreEntries(ctx, string(sessionID))
	if err != nil {
		return restoredSession{}, err
	}

	if len(entries) == 0 {
		return restoredSession{}, unknownSessionError()
	}

	start.MetaOptions = mergeSessionConfiguration(
		metaOptions,
		configurationPresence,
		boundary.Configuration,
	)

	if openErr := a.ensureOpen(); openErr != nil {
		return restoredSession{}, openErr
	}

	start.HydrateEntries = entries
	start.PriorBoundary = boundary

	session, err := a.startAndStoreSession(ctx, start)
	if err != nil {
		return restoredSession{}, err
	}

	carrierTransferred = true

	return restoredSession{
		session: session,
		entries: entries,
		started: true,
		release: releaseCarrier,
	}, nil
}

type sessionCarrierTransition struct {
	token chan struct{}
	users int
}

// acquireSessionCarrier serializes restore and rotation decisions for one
// addressable id without coupling unrelated sessions. The returned release is
// transferred to restoredSession so replay and opening publication remain in
// the same ordered transition.
func (a *Agent) acquireSessionCarrier(ctx context.Context, id acp.SessionId) (func(), error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	a.mu.Lock()
	if a.sessionCarriers == nil {
		a.sessionCarriers = make(map[acp.SessionId]*sessionCarrierTransition)
	}

	transition := a.sessionCarriers[id]
	if transition == nil {
		transition = &sessionCarrierTransition{token: make(chan struct{}, 1)}
		transition.token <- struct{}{}

		a.sessionCarriers[id] = transition
	}

	transition.users++
	a.mu.Unlock()

	select {
	case <-transition.token:
		if err := context.Cause(ctx); err != nil {
			transition.token <- struct{}{}

			a.releaseSessionCarrierUser(id, transition)

			return nil, err
		}
	case <-ctx.Done():
		a.releaseSessionCarrierUser(id, transition)

		return nil, context.Cause(ctx)
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			transition.token <- struct{}{}

			a.releaseSessionCarrierUser(id, transition)
		})
	}, nil
}

func (a *Agent) releaseSessionCarrierUser(id acp.SessionId, transition *sessionCarrierTransition) {
	a.mu.Lock()
	defer a.mu.Unlock()

	transition.users--
	if transition.users == 0 && a.sessionCarriers[id] == transition {
		delete(a.sessionCarriers, id)
	}
}

// restoreActiveSession answers a restore from a session that is already live.
// The gate is taken before the store is read and released by the caller after
// replay, so the entries handed back are exactly the prefix the gate froze.
func (a *Agent) restoreActiveSession(
	ctx context.Context,
	sessionID acp.SessionId,
	session *agentSession,
) (restoredSession, error) {
	release, err := session.beginRestore(ctx)
	if err != nil {
		return restoredSession{}, err
	}

	entries, _, err := a.loadCurrentStoreEntries(ctx, string(sessionID))
	if err != nil {
		release()

		return restoredSession{}, err
	}

	return restoredSession{session: session, entries: entries, release: release}, nil
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
		if params.Cwd != nil && *params.Cwd != "" && *params.Cwd != session.cwd {
			continue
		}

		// A tombstoned id is hidden from list even while something still holds
		// it in the active map: the store half already filters, and the active
		// half answers on the same terms.
		if _, deleted := a.deleted[id]; deleted {
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

	closeErr := session.Close(ctx)
	if closeErr != nil {
		return acp.CloseSessionResponse{}, closeErr
	}

	if a.detachSession(params.SessionId, session) {
		a.observe.AddActiveSession(ctx, -1)
	}

	return acp.CloseSessionResponse{}, nil
}

// UnstableDeleteSession implements ACP session/delete in a fixed order: the
// durable tombstone first, then close-and-cancel of any active session with the
// same id, then the native state and store entries, and the id is hidden from
// list, load, and resume from the moment the tombstone lands.
//
// The tombstone is written before anything is torn down and before the session's
// persistence is fenced. It does not need to wait for a settlement that is still
// committing, because the store itself refuses an Append or a Replace over a
// tombstone that write did not create: a commit racing the delete either landed
// before it or lands nowhere. Waiting instead would wedge the delete behind a
// live prompt nothing has cancelled yet — the cancel is the rung *after* the
// tombstone — and with no turn deadline configured, nothing inside the wrapper
// bounds that wait.
//
// A delete that did not tombstone changes nothing: the session stays the host's,
// stays listed, stays promptable, and keeps its persistence unfenced. Fencing a
// session the host still owns would silently drop every later commit it makes
// while reporting success on each one.
//
// A delete that tombstoned and then failed teardown keeps that wire posture —
// the tombstone is durable and the id stays hidden — while retaining the exact
// session owner and its one immutable close result.
func (a *Agent) UnstableDeleteSession(
	ctx context.Context,
	params acp.UnstableDeleteSessionRequest,
) (resp acp.UnstableDeleteSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, params.Meta, "session/delete")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.UnstableDeleteSessionResponse{}, refusal
	}

	a.mu.Lock()
	session := a.sessions[params.SessionId]
	a.mu.Unlock()

	if deleteErr := a.sessionStore().Delete(ctx, SessionKey{SessionID: string(params.SessionId)}); deleteErr != nil {
		return acp.UnstableDeleteSessionResponse{}, deleteErr
	}

	a.mu.Lock()

	_, alreadyDeleted := a.deleted[params.SessionId]
	if current := a.sessions[params.SessionId]; current != nil {
		session = current
	}

	a.deleted[params.SessionId] = struct{}{}
	a.mu.Unlock()

	if session != nil && !alreadyDeleted {
		a.observe.AddActiveSession(ctx, -1)
	}

	if session == nil {
		return acp.UnstableDeleteSessionResponse{}, nil
	}

	// The tombstone is durable, so the incarnation ends with it: the close
	// boundary that follows terminalizes nothing, certifies nothing, and commits
	// nothing behind a session the host was told is gone.
	session.fencePersistence()

	if cleanupErr := session.Close(ctx); cleanupErr != nil {
		return acp.UnstableDeleteSessionResponse{}, cleanupErr
	}

	a.mu.Lock()
	if a.sessions[params.SessionId] == session {
		delete(a.sessions, params.SessionId)
	}
	a.mu.Unlock()

	return acp.UnstableDeleteSessionResponse{}, nil
}

// session resolves an addressed id to its live session. The tombstone is
// consulted in the same critical section as the map, so a deleted id is
// wire-indistinguishable from one that never existed on every session-scoped
// method — whatever an install racing the delete managed to leave behind.
func (a *Agent) session(sessionID acp.SessionId) (*agentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, deleted := a.deleted[sessionID]; deleted {
		return nil, unknownSessionError()
	}

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

func (a *Agent) removeSession(ctx context.Context, sessionID acp.SessionId, session *agentSession) error {
	if a.detachSession(sessionID, session) {
		a.observe.AddActiveSession(context.Background(), -1)
	}

	if session == nil {
		return nil
	}

	err := session.Close(ctx)
	a.retainIncompleteSession(session, err)

	return err
}

func (a *Agent) retainIncompleteSession(session *agentSession, err error) {
	if session == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err == nil {
		delete(a.retainedSessions, session)

		return
	}

	if a.retainedSessions == nil {
		a.retainedSessions = make(map[*agentSession]struct{})
	}

	a.retainedSessions[session] = struct{}{}
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errAgentClosed
	}

	return nil
}

// storeStartedSession installs a freshly prepared session under its id. The
// deletion tombstone is re-checked here, under the same lock that installs, and
// never cleared as a side effect of installing: a delete that completed while
// this session was being prepared wins however far the preparation got, so the
// prepared replacement is torn down and the id answers unknown-session. A load
// or resume that passed its entry check is not licensed to resurrect an id the
// host was told is gone.
func (a *Agent) storeStartedSession(ctx context.Context, session *agentSession) error {
	a.sessionInstallMu.Lock()
	defer a.sessionInstallMu.Unlock()

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		closeErr := session.Close(ctx)
		a.retainIncompleteSession(session, closeErr)

		return errors.Join(errAgentClosed, closeErr)
	}

	if _, deleted := a.deleted[session.id]; deleted {
		a.mu.Unlock()

		closeErr := session.Close(ctx)
		a.retainIncompleteSession(session, closeErr)

		return errors.Join(unknownSessionError(), closeErr)
	}

	previous := a.sessions[session.id]
	if previous == session {
		delete(a.retainedSessions, session)
		a.mu.Unlock()

		return nil
	}

	if previous == nil && len(a.sessions) >= a.maxActiveSessions() {
		a.mu.Unlock()

		closeErr := session.Close(ctx)
		a.retainIncompleteSession(session, closeErr)

		return errors.Join(backpressureError("active_sessions"), closeErr)
	}

	if previous == nil {
		a.sessions[session.id] = session
		delete(a.retainedSessions, session)
		a.mu.Unlock()

		a.observe.AddActiveSession(ctx, 1)

		return nil
	}

	a.mu.Unlock()

	closeErr := previous.Close(ctx)
	a.retainIncompleteSession(previous, closeErr)

	if !nativeContainmentComplete(closeErr) {
		replacementCloseErr := session.Close(context.WithoutCancel(ctx))
		a.retainIncompleteSession(session, replacementCloseErr)

		return errors.Join(closeErr, replacementCloseErr)
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		closeErr := session.Close(context.WithoutCancel(ctx))
		a.retainIncompleteSession(session, closeErr)

		return errors.Join(errAgentClosed, closeErr)
	}

	if _, deleted := a.deleted[session.id]; deleted {
		a.mu.Unlock()

		closeErr := session.Close(context.WithoutCancel(ctx))
		a.retainIncompleteSession(session, closeErr)

		return errors.Join(unknownSessionError(), closeErr)
	}

	// CloseSession or delete may have detached the contained predecessor while
	// its immutable close was being joined. No other installer can publish here
	// because sessionInstallMu spans the whole decision.
	if current := a.sessions[session.id]; current != nil && current != previous {
		a.mu.Unlock()

		closeErr := session.Close(context.WithoutCancel(ctx))
		a.retainIncompleteSession(session, closeErr)

		return errors.Join(errors.New("session carrier changed during installation"), closeErr)
	}

	a.sessions[session.id] = session
	delete(a.retainedSessions, session)
	a.mu.Unlock()

	return nil
}

func (a *Agent) startAndStoreSession(ctx context.Context, start sessionStart) (*agentSession, error) {
	session, err := a.startSession(ctx, start)
	if err != nil {
		return nil, err
	}

	if err := a.storeStartedSession(ctx, session); err != nil {
		return nil, err
	}

	return session, nil
}

func (a *Agent) updateNativeConstruction(
	construction *nativeConstruction,
	update func(*nativeConstruction),
) {
	if construction == nil {
		return
	}

	a.mu.Lock()
	// Ownership publication continues after a close waiter memoizes the
	// construction as incomplete. The verdict is immutable; newly acquired
	// handles are not. Dropping one here would orphan the exact resource the
	// late constructor is obligated to quarantine.
	update(construction)
	a.mu.Unlock()
}

func (a *Agent) nativeConstructionError(construction *nativeConstruction) error {
	if construction == nil {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return construction.err
}

// transferConstructionToSession moves every exact cleanup handle from the
// construction fence to a retained session in one Agent critical section.
// The retained entry closes the publication gap before store installation.
func (a *Agent) transferConstructionToSession(
	construction *nativeConstruction,
	session *agentSession,
) (bool, bool) {
	a.mu.Lock()
	if construction.immutable {
		closed := a.closed
		a.mu.Unlock()

		return closed, false
	}

	if session != nil {
		if a.retainedSessions == nil {
			a.retainedSessions = make(map[*agentSession]struct{})
		}

		a.retainedSessions[session] = struct{}{}
	}

	closed := a.closed
	if construction.err == nil {
		delete(a.constructions, construction)
	}

	close(construction.done)
	a.mu.Unlock()

	return closed, true
}

// transferConstructionToRelaunch moves the exact post-spawn owner into the
// relaunch attempt before any observer callback can run.
func (a *Agent) transferConstructionToRelaunch(
	construction *nativeConstruction,
	attempt *sessionRelaunchAttempt,
) bool {
	a.mu.Lock()
	if construction.immutable {
		a.mu.Unlock()

		return false
	}

	attempt.mu.Lock()
	attempt.proc = construction.proc
	attempt.client = construction.client
	attempt.generationRoot = construction.generationRoot
	attempt.generationPrepared = construction.generationPrepared
	attempt.nativeBoundary = construction.nativeBoundary

	if construction.err == nil {
		delete(a.constructions, construction)
	}

	close(construction.done)
	attempt.mu.Unlock()
	a.mu.Unlock()

	return true
}

func (a *Agent) cleanupNativeConstruction(
	ctx context.Context,
	construction *nativeConstruction,
	cause error,
) error {
	if construction == nil {
		return nil
	}

	construction.cleanupMu.Lock()
	defer construction.cleanupMu.Unlock()

	if construction.cleanupDone {
		return construction.cleanupErr
	}

	construction.cleanupErr = a.cleanupNativeConstructionOwned(ctx, construction, cause)
	construction.cleanupDone = !errors.Is(construction.cleanupErr, ErrNativeTreeBusy)

	return construction.cleanupErr
}

func (a *Agent) cleanupNativeConstructionOwned(
	ctx context.Context,
	construction *nativeConstruction,
	cause error,
) error {
	a.mu.Lock()
	immutable := construction.immutable
	proc := construction.proc
	session := construction.session
	sessionRoot := construction.sessionRoot
	generationPrepared := construction.generationPrepared
	browserShim := construction.browserShim
	residence := construction.residence
	a.mu.Unlock()

	if !immutable && !nativeContainmentComplete(cause) {
		return cause
	}

	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, sessionShutdownTimeout)
	defer cancelCleanup()

	var containmentErr error
	if session != nil {
		containmentErr = session.stopPumpBounded(cleanupCtx)
	}

	if proc != nil {
		shutdownErr := construction.nativeBoundary.run(cleanupCtx, "construction shutdown", func() error {
			return proc.Shutdown(cleanupCtx)
		})

		var killErr error
		if immutable {
			// A constructor that returned after Agent.Close timed out is a late
			// native result, not an ordinary graceful startup failure. Its exact
			// quarantine always executes every containment rung synchronously.
			killErr = construction.nativeBoundary.run(cleanupCtx, "construction kill", proc.Kill)
		}

		closeErr := construction.nativeBoundary.run(cleanupCtx, "construction close", proc.Close)

		containmentErr = terminalNativeClose(errors.Join(containmentErr, shutdownErr, killErr), closeErr)
	}

	if !nativeContainmentComplete(containmentErr) {
		return containmentErr
	}

	resourceErr := runNativeBoundaryStep(cleanupCtx, "construction resource cleanup", func() error {
		return finalizeSessionNativeResources(
			a, containmentErr, construction.generationRoot, generationPrepared,
			sessionRoot, browserShim, residence,
		)
	})

	if immutable {
		// Retain the waiter's immutable incomplete verdict even after the late
		// resource has been successfully contained. The cleanup is evidence that
		// nothing was orphaned, not permission to rewrite Agent.Close's answer.
		return errors.Join(cause, resourceErr)
	}

	return resourceErr
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

	if _, deleted := a.deleted[id]; deleted {
		return nil
	}

	session := a.sessions[id]
	if session == nil || session.fingerprint != fingerprint {
		return nil
	}

	return session
}

func (a *Agent) activeSession(id acp.SessionId) *agentSession {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, deleted := a.deleted[id]; deleted {
		return nil
	}

	return a.sessions[id]
}

func (a *Agent) activeSessionConfiguration(id acp.SessionId) (sessionConfigurationRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, deleted := a.deleted[id]; deleted {
		return sessionConfigurationRecord{}, false
	}

	session := a.sessions[id]
	if session == nil {
		return sessionConfigurationRecord{}, false
	}

	return sessionConfiguration(PiOptions{
		Env:           session.configuration.Env,
		ExtraPathDirs: session.configuration.ExtraPathDirs,
	}), true
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
	construction, err := a.beginNativeConstruction()
	if err != nil {
		return nil, err
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(
				errors.New("pi session construction callback panicked"),
				a.nativeConstructionError(construction),
			)
		}

		if err != nil {
			ownerErr := a.nativeConstructionError(construction)
			cause := errors.Join(err, ownerErr)
			cleanupErr := a.cleanupNativeConstruction(context.WithoutCancel(ctx), construction, cause)
			err = errors.Join(cause, cleanupErr)
			a.finishNativeConstruction(construction, cleanupErr != nil, cleanupErr)

			return
		}

		closed, transferred := a.transferConstructionToSession(construction, session)
		if !transferred {
			ownerErr := a.nativeConstructionError(construction)
			cleanupErr := a.cleanupNativeConstruction(context.WithoutCancel(ctx), construction, ownerErr)
			err = errors.Join(ownerErr, cleanupErr)
			session = nil

			return
		}

		a.mu.Lock()
		closed = closed || a.closed
		a.mu.Unlock()

		if closed {
			closeErr := session.Close(context.WithoutCancel(ctx))
			a.retainIncompleteSession(session, closeErr)
			session = nil
			err = errors.Join(errAgentClosed, closeErr)
		}
	}()

	return a.startSessionConstruction(ctx, start, construction)
}

//nolint:gocyclo // Startup is one fail-closed ownership ladder whose ordered gates must remain visible.
func (a *Agent) startSessionConstruction(
	ctx context.Context,
	start sessionStart,
	construction *nativeConstruction,
) (session *agentSession, err error) {
	defer func() { a.recordNativeContainment(err) }()

	if retryErr := a.retryBusyNativeConstructions(ctx); retryErr != nil {
		return nil, retryErr
	}

	// Every session-establishing method fails here on agent configuration a
	// session cannot start under.
	if configErr := a.sessionStartConfigurationError(); configErr != nil {
		return nil, configErr
	}

	versionErr := a.ensureVersion(ctx)

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, closedErr
	}

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

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, closedErr
	}

	var (
		dirs        sessionDirs
		browserShim *pi.BrowserShim
		residence   *pi.SessionResidence
	)

	dirs, browserShim, err = a.createSessionRuntime()
	if err != nil {
		return nil, err
	}

	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.generationRoot = dirs.Root
		owner.sessionRoot = dirs.SessionRoot
		owner.browserShim = browserShim
	})

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

	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.residence = residence
	})

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
		var seedErr *pi.SeedFileError
		if errors.As(writeErr, &seedErr) {
			return nil, unsupportedField("seedFiles." + seedErr.Name)
		}

		return nil, writeErr
	}

	// Reconciled after the seed write, so an operator-seeded settings.json is
	// the baseline this and every later launch starts from.
	if reconcileErr := a.reconcileHomeStartupDefaults(dirs.AgentDir); reconcileErr != nil {
		return nil, reconcileErr
	}

	seededResources, err := agentDirExplicitResources(agentDir)
	if err != nil {
		return nil, err
	}

	if prepareErr := a.prepareNativeTree(ctx, dirs.Root); prepareErr != nil {
		return nil, prepareErr
	}

	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.generationPrepared = a.options.hostAuthoritySupplied
	})

	// Load operator-seeded extensions before wrapper-owned extensions so the
	// wrapper's reserved question tool and correlation hooks cannot be
	// replaced by a seed with the same registration name.
	extensionPaths = append(seededResources.Extensions, extensionPaths...)

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, closedErr
	}

	spec := pi.LaunchSpec{
		ExecutablePath:      executable,
		NativeRoot:          dirs.Root,
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
		BaseEnvironment:     a.nativeBaseEnvironment(),
	}

	// The pi child must outlive the lifecycle request that spawns it: its
	// launch context is detached so the request-scoped cancel cannot kill the
	// session's long-lived process. Teardown is owned by the shutdown ladder.
	startCtx, finishStart := a.observe.StartPiProcess(context.WithoutCancel(ctx), "start")

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, closedErr
	}

	// Final native-launch gate: no hook or ownership transfer occurs between
	// this check and startTrackedPiProcess.
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, closedErr
	}

	proc, client, err := a.startTrackedPiProcess(startCtx, spec)
	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.proc = proc
		owner.client = client

		if owner.err == nil {
			owner.err = err
		}
	})

	finishStart(err)

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, errors.Join(err, closedErr)
	}

	if err != nil {
		return nil, a.nativeStartFailure(ctx, failureCauseProcessExit, err, nil)
	}

	session = &agentSession{
		agent:                 a,
		cwd:                   start.Cwd,
		additionalDirectories: slices.Clone(start.AdditionalDirectories),
		fingerprint:           sessionStartFingerprint(start),
		configuration:         sessionConfiguration(start.MetaOptions),
		launch:                spec,
		sessionRoot:           dirs.SessionRoot,
		generationPrepared:    a.options.hostAuthoritySupplied,
		browserShim:           browserShim,
		residence:             residence,
		permissionMode:        permission,
		autoRetry:             start.MetaOptions.AutoRetry,
		mcpServers:            cloneMCPServers(start.McpServers),
		mcpRefreshPending:     includeMCP,
		proc:                  proc,
		client:                client,
		turn:                  make(chan struct{}, sessionTurnCapacity),
		rawMessages:           start.RawMessages,
		nativeBoundary:        construction.nativeBoundary,
	}
	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.session = session
	})

	generationCtx, generationCancel := context.WithCancel(context.Background())

	err = client.Start(generationCtx)
	if err != nil {
		generationCancel()

		return nil, err
	}

	if closedErr := a.ensureOpen(); closedErr != nil {
		generationCancel()

		return nil, closedErr
	}

	if _, pumpErr := session.startPumpContext(generationCtx, generationCancel, client, construction.nativeBoundary); pumpErr != nil {
		generationCancel()

		return nil, pumpErr
	}

	err = a.setUpNativeSession(ctx, session, start, modelRef, hasModel)

	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, errors.Join(err, closedErr)
	}

	if err != nil {
		return nil, err
	}

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

		// pi acknowledges a level it does not apply, so the acknowledgement
		// proves delivery, never adoption. What the session records — and
		// therefore advertises — is the level read back from pi, so a value pi
		// silently declined leaves the prior effective level on show instead of
		// an echo of the request.
		applied, levelErr := client.GetState(ctx)
		if levelErr != nil {
			return a.nativeStartFailure(ctx, failureCauseTransport, levelErr, proc)
		}

		session.mu.Lock()
		session.thinkingLevel = applied.ThinkingLevel
		session.mu.Unlock()
	}

	if hasModel {
		selected, setErr := client.SetModel(ctx, modelRef.Provider, modelRef.ID)
		if setErr != nil {
			var commandErr *pi.CommandError
			if errors.As(setErr, &commandErr) {
				a.log.ErrorContext(ctx, "pi rejected the configured model")

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
		session.mu.Lock()
		outbox := session.outbox
		session.mu.Unlock()

		if err := session.retireManagedGeneration(ctx, outbox); err != nil {
			return err
		}

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
		s.agent.log.DebugContext(ctx, "get pi session stats failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)

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
