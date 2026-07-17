package piacp

import (
	"context"
	"errors"
	"fmt"
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

// finalizeSessionRuntimeResources releases each admission only after the
// corresponding resource is proven gone. An unproven native tree retains its
// admission and the private session root because that tree may still use it.
func finalizeSessionRuntimeResources(
	runtimeErr error,
	nativeRelease func(),
	sessionRoot string,
	scratchRelease func(),
) error {
	if !pi.ProcessTreeQuiescent(runtimeErr) {
		return runtimeErr
	}

	if nativeRelease != nil {
		nativeRelease()
	}

	var removeErr error
	if sessionRoot != "" {
		removeErr = materializeRemoveAll(sessionRoot)
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
	if quiescenceErr := s.nativeQuiescenceError(); quiescenceErr != nil {
		return quiescenceErr
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

	if closeErr != nil {
		s.recordNativeQuiescence(closeErr)

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
	quiescenceErr := errors.Join(shutdownErr, closeErr)
	s.retireProviderProcess(context.WithoutCancel(ctx), closeErr)
	s.recordNativeQuiescence(quiescenceErr)

	if !pi.ProcessTreeQuiescent(quiescenceErr) {
		return quiescenceErr
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
// process has been proven quiescent. Its extension factories run again, so
// their fixed tool registry is rebuilt from the MCP server's current view.
func (s *agentSession) relaunchProcess(ctx context.Context) error {
	s.mu.Lock()
	lastSessionFile := s.sessionFilePath
	s.mu.Unlock()

	spec := s.launch
	if s.sessionFileExists() {
		spec.SessionPath = lastSessionFile
		spec.SessionID = ""
	} else {
		spec.SessionPath = ""
		spec.SessionID = string(s.id)
	}

	// Detached like the initial launch: the relaunched child must survive
	// past the prompt request that triggered it.
	startCtx, finishStart := s.agent.observe.StartPiProcess(context.WithoutCancel(ctx), "relaunch")
	spawnStarted := time.Now()
	relaunched, client, processRoot, err := s.agent.startTrackedPiProcess(startCtx, spec)
	observeRuntimeStartupStage(startCtx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSpawn, spawnStarted, err)

	finishStart(err)

	if err != nil {
		s.recordNativeQuiescence(err)

		return err
	}

	readinessStarted := time.Now()
	if startErr := client.Start(context.WithoutCancel(ctx)); startErr != nil {
		observeRuntimeStartupStage(ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupReadiness, readinessStarted, startErr)

		killErr := relaunched.Kill()
		closeErr := relaunched.Close()
		cleanupErr := errors.Join(killErr, closeErr)
		processRoot.retire(context.WithoutCancel(ctx), providerProcessTreeProven(closeErr))
		s.recordNativeQuiescence(cleanupErr)

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

	if spec.SessionID != "" {
		s.mirroredRows = 0
	}
	s.mu.Unlock()

	return nil
}

func (s *agentSession) cleanupFailedRelaunch(proc piProcess, cause error) error {
	s.stopPump()

	killErr := proc.Kill()
	closeErr := proc.Close()
	cleanupErr := errors.Join(killErr, closeErr)
	s.retireProviderProcess(context.Background(), closeErr)
	s.recordNativeQuiescence(cleanupErr)

	return errors.Join(cause, cleanupErr)
}

// Cancel cancels the active pi turn. Pending dialogs are resolved cancelled
// first, then native abort is attempted and the complete per-session process
// tree is closed and proved quiescent before either Cancel or Prompt settles.
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
// failure observes and returns the same proof result.
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
	// proof boundary.
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
// its entire process-tree proof.
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
// native abort. Killing and closing the contained process proves descendants
// quiescent before the turn context is released; a later prompt can then
// relaunch the same logical session without racing the cancelled command tree.
func (s *agentSession) terminateCancelledTurn(
	ctx context.Context,
	proc piProcess,
	turnCancel context.CancelFunc,
	abortErr error,
) error {
	if proc == nil {
		turnCancel()

		return errors.Join(abortErr, pi.ErrProcessTreeNotQuiescent, errors.New("active pi turn has no contained process root"))
	}

	killErr := proc.Kill()
	closeErr := proc.Close()
	quiescenceErr := errors.Join(killErr, closeErr)

	s.retireProviderProcess(ctx, quiescenceErr)
	s.recordNativeQuiescence(quiescenceErr)
	turnCancel()

	if quiescenceErr != nil {
		return errors.Join(abortErr, quiescenceErr)
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
		s.recordNativeQuiescence(closeErr)

		quiescenceErr := s.nativeQuiescenceError()
		err = errors.Join(err, quiescenceErr)
	}

	waitCtx, stopWaiting := context.WithTimeout(context.WithoutCancel(ctx), s.closeTurnTimeout())
	defer stopWaiting()

	if releaseTurn, waitErr := s.acquireTurn(waitCtx); waitErr != nil {
		err = errors.Join(err, waitErr)
	} else {
		releaseTurn()
	}

	err = finalizeSessionRuntimeResources(
		err, s.nativeRootRelease, s.sessionRoot, s.scratchRootRelease,
	)

	if s.agent != nil {
		s.agent.observe.RecordPiProcessExit(ctx, "closed", err)
	}

	return err
}

func (s *agentSession) retireProviderProcess(ctx context.Context, err error) {
	proven := providerProcessTreeProven(err)

	s.mu.Lock()

	root := s.providerProcessRoot
	if proven {
		s.providerProcessRoot = nil
	}
	s.mu.Unlock()

	if root != nil {
		root.retire(ctx, proven)
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

func (s *agentSession) recordNativeQuiescence(err error) {
	if pi.ProcessTreeQuiescent(err) {
		return
	}

	s.mu.Lock()
	s.nativeQuiescenceErr = errors.Join(s.nativeQuiescenceErr, err)
	s.mu.Unlock()
}

func (s *agentSession) nativeQuiescenceError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nativeQuiescenceErr
}

func (s *agentSession) closeTurnTimeout() time.Duration {
	if s.closeTurnWait > 0 {
		return s.closeTurnWait
	}

	return defaultSessionCloseTurnWait
}
