package piacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// sessionInterruptTimeout bounds the native abort. The abort runs under a
// background-derived context so a cancelled caller context cannot abort it.
var sessionInterruptTimeout = 5 * time.Second

// sessionCancelAbortGrace is the time a user cancellation gives pi to emit
// its native terminal ladder and acknowledge abort. A native tool can block
// that acknowledgement indefinitely, so expiry escalates to the session's
// process-tree containment boundary.
var sessionCancelAbortGrace = 500 * time.Millisecond

// sessionShutdownTimeout bounds the process shutdown ladder on close.
var sessionShutdownTimeout = 10 * time.Second

// finalizeSessionRuntimeResources releases each admission only after its
// selected containment boundary completes. An incomplete native boundary
// retains its admission and private session root because descendants may
// still use them.
func finalizeSessionRuntimeResources(
	runtimeErr error,
	nativeRelease func(),
	sessionRoot string,
	scratchRelease func(),
	browserShim *pi.BrowserShim,
) error {
	if !pi.ProcessContainmentComplete(runtimeErr) {
		return runtimeErr
	}

	if nativeRelease != nil {
		nativeRelease()
	}

	// The shim is deleted here rather than at process exit for the same reason
	// the session root is: a surviving descendant would otherwise fall through
	// the removed no-ops to the real browser launcher on PATH.
	removeErr := browserShim.Remove()
	if sessionRoot != "" {
		removeErr = errors.Join(removeErr, materializeRemoveAll(sessionRoot))
	}

	if removeErr == nil && scratchRelease != nil {
		scratchRelease()
	}

	return errors.Join(runtimeErr, removeErr)
}

func (s *agentSession) acquireTurn(ctx context.Context) (func(), error) {
	turn := s.turnQueue()

	select {
	case turn <- struct{}{}:
		return func() { <-turn }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("session_prompt")
	}
}

func (s *agentSession) turnQueue() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.turn == nil {
		s.turn = make(chan struct{}, sessionTurnCapacity)
	}

	return s.turn
}

func (s *agentSession) currentClient() piClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.client
}

// ensureProcessAlive relaunches the pi process when it died on a previous
// turn. The session is never removed on a native failure, so a follow-up
// prompt lands here and brings the process back up on the same native session
// file rather than returning the unknown-session error.
func (s *agentSession) ensureProcessAlive(ctx context.Context) error {
	if containmentErr := s.nativeContainmentError(); containmentErr != nil {
		return containmentErr
	}

	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()

	if proc == nil {
		return errors.New("pi session has no process")
	}

	select {
	case <-proc.Exited():
	default:
		return nil
	}

	s.stopPump()

	closeErr := proc.Close()
	s.retireProviderProcess(context.WithoutCancel(ctx), closeErr)
	s.releaseNativeRootAfterCompletion(closeErr)

	if closeErr != nil {
		s.recordNativeContainment(closeErr)

		return closeErr
	}

	return s.relaunchProcess(ctx)
}

// refreshMCPTools rebuilds pi's fixed extension-tool registry on the first
// user turn. MCP lifecycle establishment can occur before operation authority
// is armed, in which case a server may deliberately expose only a local
// readiness surface. Restarting the otherwise-idle native process here makes
// discovery run under the real turn authority and prevents that provisional
// tool list from becoming the session's permanent registry.
func (s *agentSession) refreshMCPTools(ctx context.Context) error {
	if err := s.ensureProcessAlive(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	pending := s.mcpRefreshPending
	proc := s.proc
	s.mu.Unlock()

	if !pending {
		return nil
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	shutdownErr := proc.Shutdown(shutdownCtx)

	cancelShutdown()

	s.stopPump()

	closeErr := proc.Close()
	containmentErr := errors.Join(shutdownErr, closeErr)
	s.retireProviderProcess(context.WithoutCancel(ctx), closeErr)
	s.recordNativeContainment(containmentErr)
	s.releaseNativeRootAfterCompletion(containmentErr)

	if !pi.ProcessContainmentComplete(containmentErr) {
		return containmentErr
	}

	if err := s.relaunchProcess(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	s.mcpRefreshPending = false
	s.mu.Unlock()

	return nil
}

// relaunchProcess starts the same logical pi session after the previous
// process reaches its selected containment boundary. Its extension factories run again, so
// their fixed tool registry is rebuilt from the MCP server's current view.
func (s *agentSession) relaunchProcess(ctx context.Context) (err error) {
	if constructionErr := s.agent.beginNativeConstruction(); constructionErr != nil {
		return constructionErr
	}
	defer func() {
		s.recordNativeContainment(err)
		s.agent.endNativeConstruction()
	}()

	s.mu.Lock()
	lastSessionFile := s.sessionFilePath
	previousLaunch := s.launch
	s.mu.Unlock()

	spec, err := s.nextRuntimeLaunch(previousLaunch, lastSessionFile)
	if err != nil {
		return err
	}

	keepGeneration := false
	defer func() {
		if !keepGeneration && pi.ProcessContainmentComplete(err) {
			err = errors.Join(err, materializeRemoveAll(spec.Containment.GenerationRoot))
		}
	}()

	// Detached like the initial launch: the relaunched child must survive
	// past the prompt request that triggered it.
	startCtx, finishStart := s.agent.observe.StartPiProcess(context.WithoutCancel(ctx), "relaunch")

	nativeRelease, err := acquireNativeRoot(ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		finishStart(err)

		return err
	}

	s.mu.Lock()
	s.nativeRootRelease = nativeRelease
	s.mu.Unlock()

	defer func() {
		if err != nil {
			s.releaseNativeRootAfterCompletion(err)
		}
	}()

	spawnStarted := time.Now()
	relaunched, client, processRoot, err := s.agent.startTrackedPiProcess(startCtx, spec)
	observeRuntimeStartupStage(startCtx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSpawn, spawnStarted, err)

	finishStart(err)

	if err != nil {
		s.recordNativeContainment(err)

		return err
	}

	readinessStarted := time.Now()
	if startErr := client.Start(context.WithoutCancel(ctx)); startErr != nil {
		observeRuntimeStartupStage(ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupReadiness, readinessStarted, startErr)

		killErr := relaunched.Kill()
		closeErr := relaunched.Close()
		cleanupErr := errors.Join(killErr, closeErr)
		processRoot.retire(context.WithoutCancel(ctx), providerProcessTreeComplete(closeErr))
		s.recordNativeContainment(cleanupErr)

		return errors.Join(startErr, cleanupErr)
	}

	observeRuntimeStartupStage(ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupReadiness, readinessStarted, nil)

	s.mu.Lock()
	s.proc = relaunched
	s.client = client
	s.providerProcessRoot = processRoot
	s.mu.Unlock()
	processRoot.observe(ctx, relaunched)

	s.startPump(client)

	if retryErr := client.SetAutoRetry(ctx, s.autoRetry); retryErr != nil {
		return s.cleanupFailedRelaunch(relaunched, retryErr)
	}

	// pi computes a fresh session file path per process even for the same
	// --session-id, so the mirror source must be re-captured after every
	// relaunch or commitMirror would silently read a path that never exists.
	state, stateErr := client.GetState(ctx)
	if stateErr != nil {
		return s.cleanupFailedRelaunch(relaunched, stateErr)
	}

	if state.SessionID != string(s.id) {
		return s.cleanupFailedRelaunch(
			relaunched,
			fmt.Errorf("native session id drift on relaunch: expected %s, got %s", s.id, state.SessionID),
		)
	}

	s.mu.Lock()
	s.sessionFilePath = state.SessionFile
	s.launch = spec

	if spec.SessionID != "" {
		s.mirroredRows = 0
	}
	s.mu.Unlock()

	keepGeneration = true

	return nil
}

func (s *agentSession) nextRuntimeLaunch(previous pi.LaunchSpec, lastSessionFile string) (pi.LaunchSpec, error) {
	dirs, err := createSessionGeneration(s.sessionRoot)
	if err != nil {
		return pi.LaunchSpec{}, err
	}

	fail := func(cause error) (pi.LaunchSpec, error) {
		return pi.LaunchSpec{}, errors.Join(cause, materializeRemoveAll(dirs.Root))
	}

	if homeErr := s.agent.applyDurableHome(&dirs); homeErr != nil {
		return fail(homeErr)
	}

	// A durable home is the same directory across generations: there is nothing
	// to copy and no path to rebase, and copying it forward would strand the
	// credential store pi refreshes under its own lock.
	if dirs.AgentDir != previous.AgentDir {
		if copyErr := copyGenerationAgentDir(previous.AgentDir, dirs.AgentDir); copyErr != nil {
			return fail(fmt.Errorf("copy pi agent generation: %w", copyErr))
		}
	}

	spec := previous
	spec.Env = cloneStringMap(previous.Env)
	spec.AgentDir = dirs.AgentDir
	spec.SessionDir = dirs.SessionDir

	rebasePaths := func(paths []string) ([]string, error) {
		rebased := make([]string, len(paths))
		for index, path := range paths {
			rebased[index], err = rebaseGenerationPath(path, previous.AgentDir, dirs.AgentDir)
			if err != nil {
				return nil, err
			}
		}

		return rebased, nil
	}
	if spec.ExtensionPaths, err = rebasePaths(previous.ExtensionPaths); err != nil {
		return fail(err)
	}

	if spec.SkillPaths, err = rebasePaths(previous.SkillPaths); err != nil {
		return fail(err)
	}

	if spec.PromptTemplatePaths, err = rebasePaths(previous.PromptTemplatePaths); err != nil {
		return fail(err)
	}

	for key, value := range spec.Env {
		if rebased, rebaseErr := rebaseGenerationPath(value, previous.AgentDir, dirs.AgentDir); rebaseErr == nil {
			spec.Env[key] = rebased
		}
	}

	if lastSessionFile != "" {
		contents, readErr := materializeReadFile(lastSessionFile)
		if readErr == nil {
			spec.SessionPath = filepath.Join(dirs.SessionDir, filepath.Base(lastSessionFile))
			if writeErr := materializeWriteFile(spec.SessionPath, contents, 0o600); writeErr != nil {
				return fail(fmt.Errorf("hydrate relaunched pi session: %w", writeErr))
			}

			spec.SessionID = ""
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return fail(fmt.Errorf("read prior pi session: %w", readErr))
		}
	}

	if spec.SessionPath == "" {
		spec.SessionID = string(s.id)
	}

	spec.Containment, err = s.agent.containmentSpecForRoot(
		scratchParent(s.agent.options.ScratchDir),
		dirs.Root,
		RuntimeResourceSession,
	)
	if err != nil {
		return fail(err)
	}

	if previous.Containment.GenerationRoot != "" {
		if err := materializeRemoveAll(previous.Containment.GenerationRoot); err != nil {
			return fail(fmt.Errorf("remove prior runtime generation: %w", err))
		}
	}

	return spec, nil
}

func (s *agentSession) cleanupFailedRelaunch(proc piProcess, cause error) error {
	s.stopPump()

	killErr := proc.Kill()
	closeErr := proc.Close()
	cleanupErr := errors.Join(killErr, closeErr)
	s.retireProviderProcess(context.Background(), closeErr)
	s.recordNativeContainment(cleanupErr)

	return errors.Join(cause, cleanupErr)
}

// Cancel cancels the active pi turn. Pending dialogs are resolved cancelled
// first, then native abort is attempted and the complete per-session process
// tree is closed and reaches its selected containment boundary before either
// Cancel or Prompt settles.
func (s *agentSession) Cancel(ctx context.Context) (err error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.cancelNativeLocked(ctx, true)
}

// cancelForClose contains active work for session close/delete without
// adopting a fresh native generation. Close and delete are teardown, not an
// explicit session/cancel request, and retain their documented no-mirror
// behavior.
func (s *agentSession) cancelForClose(ctx context.Context) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.cancelNativeLocked(ctx, false)
}

// cancelRouted validates the active turn and keeps its native abort fenced
// from turn completion and admission of the next turn.
func (s *agentSession) cancelRouted(ctx context.Context, meta map[string]any) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	activeNonce := s.turnNonce
	active := s.cancel != nil && activeNonce != ""
	s.mu.Unlock()

	if active {
		route, err := parseInboundTurnRoute(meta)
		if err != nil {
			return err
		}

		if route.turnNonce != activeNonce {
			return routeInvalid("stale route turnNonce")
		}
	}

	return s.cancelNativeLocked(ctx, true)
}

func (s *agentSession) cancelNativeLocked(ctx context.Context, commitSettled bool) (err error) {
	s.cancelPendingInteractions()

	s.mu.Lock()
	turnCancel := s.cancel
	client := s.client

	if turnCancel != nil && commitSettled {
		s.turnCommitOnCancel = true
	}
	s.mu.Unlock()

	if s.agent != nil {
		var finish func(error)

		ctx, finish = s.agent.observe.StartPiProcess(ctx, "abort")
		defer func() { finish(err) }()
	}

	if turnCancel != nil {
		return s.fenceActiveTurnLocked(context.WithoutCancel(ctx))
	}

	if client == nil {
		return nil
	}

	interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), sessionInterruptTimeout)
	defer cancelInterrupt()

	return client.Abort(interruptCtx)
}

// fenceTimedOutTurn contains the exact active turn before its timeout can
// settle. The native abort is advisory; only closing and proving the complete
// per-session process tree authorizes the turn context to be released.
func (s *agentSession) fenceTimedOutTurn(ctx context.Context, timedOut *atomic.Bool) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	active := s.cancel != nil && !s.turnSettling
	s.mu.Unlock()

	if !active {
		return nil
	}

	timedOut.Store(true)

	return s.fenceActiveTurnLocked(ctx)
}

// fenceTurnAfterContext makes parent-context cancellation use the same native
// containment boundary as session/cancel and timeout.
func (s *agentSession) fenceTurnAfterContext(ctx context.Context) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.fenceActiveTurnLocked(ctx)
}

// fenceTurnAfterFailure prevents a native command or ACP update failure from
// returning while the failed turn can still own native tool descendants.
func (s *agentSession) fenceTurnAfterFailure(ctx context.Context) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.fenceActiveTurnLocked(ctx)
}

// fenceActiveTurnLocked is the one idempotent containment fence for the live
// turn. cancelMu is held by every caller, so a coincident cancel, timeout, or
// failure observes and returns the same selected-boundary result.
func (s *agentSession) fenceActiveTurnLocked(ctx context.Context) (err error) {
	s.mu.Lock()
	if s.cancel == nil || s.turnSettling {
		s.mu.Unlock()

		return nil
	}

	if s.turnFenceStarted {
		err = s.turnFenceErr
		s.mu.Unlock()

		return err
	}

	s.turnFenceStarted = true
	if s.turnFenceDone == nil {
		s.turnFenceDone = make(chan struct{})
	}

	done := s.turnFenceDone
	turnCancel := s.cancel
	client := s.client
	proc := s.proc
	s.mu.Unlock()

	var abortErr error

	if client != nil {
		interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), sessionCancelAbortGrace)
		abortErr = client.Abort(interruptCtx)

		cancelInterrupt()
	}

	// Stop transport delivery before closing its pipes. Prompt settlement may
	// observe the closed turn sink, but awaitTurnFence keeps it behind this
	// selected containment boundary.
	s.stopPump()
	err = s.terminateCancelledTurn(context.WithoutCancel(ctx), proc, turnCancel, abortErr)

	// Pi's abort response follows its native terminal ladder. The pump records
	// agent_settled before forwarding it to the prompt goroutine, so even when
	// stopPump wins that delivery race we can still publish the complete
	// durable aborted generation. Never adopt a forced-kill partial turn: both
	// successful containment and the native settle marker are required.
	s.mu.Lock()
	commitCancelled := err == nil && s.turnCommitOnCancel && s.turnNativeSettled
	s.mu.Unlock()

	if commitCancelled {
		err = s.commitMirror(context.WithoutCancel(ctx))
	}

	s.mu.Lock()
	s.turnFenceErr = err

	close(done)
	s.mu.Unlock()

	return err
}

// claimTurnSettlement linearizes a native AgentSettled event against cancel
// and timeout. Once claimed, the turn has no active native work to contain;
// a fence that won the race is already complete because it holds cancelMu for
// its entire selected containment boundary.
func (s *agentSession) claimTurnSettlement() error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.turnFenceStarted {
		return s.turnFenceErr
	}

	s.turnSettling = true

	return nil
}

// awaitTurnFence blocks every prompt terminal path behind a containment fence
// that has started. A normal, natively settled turn has no fence and returns
// immediately.
func (s *agentSession) awaitTurnFence() error {
	s.mu.Lock()
	started := s.turnFenceStarted
	done := s.turnFenceDone
	s.mu.Unlock()

	if !started || done == nil {
		return nil
	}

	<-done

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnFenceErr
}

// terminateCancelledTurn is the hard containment boundary after the advisory
// native abort. Killing and closing the contained process completes the
// selected boundary before the turn context is released; a later prompt can then
// relaunch the same logical session without racing the cancelled command tree.
func (s *agentSession) terminateCancelledTurn(
	ctx context.Context,
	proc piProcess,
	turnCancel context.CancelFunc,
	abortErr error,
) error {
	if proc == nil {
		turnCancel()

		containmentErr := errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("active pi turn has no contained process root"))
		s.recordNativeContainment(containmentErr)

		return errors.Join(abortErr, containmentErr)
	}

	killErr := proc.Kill()
	closeErr := proc.Close()
	containmentErr := errors.Join(killErr, closeErr)

	s.retireProviderProcess(ctx, containmentErr)
	s.recordNativeContainment(containmentErr)
	s.releaseNativeRootAfterCompletion(containmentErr)
	turnCancel()

	if containmentErr != nil {
		return errors.Join(abortErr, containmentErr)
	}

	return nil
}

// cancelPendingInteractions marks the turn cancelled and resolves any pending
// extension UI dialogs as cancelled. Callers invoke this before the native
// abort so outstanding client requests are answered cancelled first.
func (s *agentSession) cancelPendingInteractions() {
	s.mu.Lock()
	if s.cancel != nil || len(s.pendingDialogs) > 0 {
		s.turnCancelled = true
	}

	dialogCancels := make([]context.CancelFunc, 0, len(s.pendingDialogs))
	for id, entry := range s.pendingDialogs {
		dialogCancels = append(dialogCancels, entry.cancel)

		delete(s.pendingDialogs, id)
	}
	s.mu.Unlock()

	for _, cancel := range dialogCancels {
		cancel()
	}
}

func (s *agentSession) wasTurnCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnCancelled
}

// Close shuts the pi process down and releases the session's resources.
func (s *agentSession) Close(ctx context.Context) (err error) {
	if s.agent != nil {
		var finish func(error)

		ctx, finish = s.agent.observe.StartPiProcess(ctx, "close")
		defer func() { finish(err) }()
	}

	s.cancelPendingInteractions()

	// Pending logins are terminalized after pending dialogs are resolved and
	// before the native interrupt, so a flow is never abandoned to a process
	// already being torn down.
	if s.agent != nil && s.agent.providerAuth != nil {
		s.agent.providerAuth.closeSession(ctx, s.id)
	}

	s.mu.Lock()
	cancel := s.cancel
	proc := s.proc
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if proc != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)

		if shutdownErr := proc.Shutdown(shutdownCtx); shutdownErr != nil {
			err = errors.Join(err, shutdownErr)
		}

		cancelShutdown()
	}

	s.stopPump()

	if proc != nil {
		closeErr := proc.Close()

		err = errors.Join(err, closeErr)
		s.retireProviderProcess(context.WithoutCancel(ctx), closeErr)
		s.recordNativeContainment(closeErr)
	}

	err = errors.Join(err, s.nativeContainmentError())

	waitCtx, stopWaiting := context.WithTimeout(context.WithoutCancel(ctx), s.closeTurnTimeout())
	defer stopWaiting()

	if releaseTurn, waitErr := s.acquireTurn(waitCtx); waitErr != nil {
		err = errors.Join(err, waitErr)
	} else {
		releaseTurn()
	}

	err = finalizeSessionRuntimeResources(
		err, s.nativeRootRelease, s.sessionRoot, s.scratchRootRelease, s.browserShim,
	)

	if s.agent != nil {
		s.agent.observe.RecordPiProcessExit(ctx, "closed", err)
	}

	return err
}

func (s *agentSession) retireProviderProcess(ctx context.Context, err error) {
	complete := providerProcessTreeComplete(err)

	s.mu.Lock()

	root := s.providerProcessRoot
	if complete {
		s.providerProcessRoot = nil
	}
	s.mu.Unlock()

	if root != nil {
		root.retire(ctx, complete)
	}
}

func (s *agentSession) releaseNativeRootAfterCompletion(err error) {
	if !pi.ProcessContainmentComplete(err) {
		return
	}

	s.mu.Lock()
	release := s.nativeRootRelease
	s.nativeRootRelease = nil
	s.mu.Unlock()

	if release != nil {
		release()
	}
}

func (s *agentSession) observeProviderProcess(ctx context.Context) {
	s.mu.Lock()
	root := s.providerProcessRoot
	process := s.proc
	s.mu.Unlock()

	if root != nil {
		root.observe(ctx, process)
	}
}

func (s *agentSession) recordNativeContainment(err error) {
	if pi.ProcessContainmentComplete(err) {
		return
	}

	s.mu.Lock()
	s.nativeContainmentErr = errors.Join(s.nativeContainmentErr, err)
	s.mu.Unlock()

	if s.agent != nil {
		s.agent.recordNativeContainment(err)
	}
}

func (s *agentSession) nativeContainmentError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nativeContainmentErr
}

func (s *agentSession) closeTurnTimeout() time.Duration {
	if s.closeTurnWait > 0 {
		return s.closeTurnWait
	}

	return defaultSessionCloseTurnWait
}
