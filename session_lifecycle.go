package piacp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// sessionInterruptTimeout bounds the native abort. The abort runs under a
// background-derived context so a cancelled caller context cannot abort it.
var sessionInterruptTimeout = 5 * time.Second

// cancelDrainTimeout bounds how long a cancelled or timed-out turn waits for
// pi's terminal events after abort before the turn context is cut.
var cancelDrainTimeout = 5 * time.Second

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
// first, then the native abort runs on a bounded background context; the
// prompt loop keeps draining until pi settles the aborted turn so the mirror
// commit still lands, with a bounded backstop that cuts the turn context if
// the settle never arrives.
func (s *agentSession) Cancel(ctx context.Context) (err error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.cancelNative(ctx)
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

	return s.cancelNative(ctx)
}

func (s *agentSession) cancelNative(ctx context.Context) (err error) {
	s.cancelPendingInteractions()

	s.mu.Lock()
	turnCancel := s.cancel
	client := s.client
	s.mu.Unlock()

	if client == nil {
		return nil
	}

	if s.agent != nil {
		var finish func(error)

		ctx, finish = s.agent.observe.StartPiProcess(ctx, "abort")
		defer func() { finish(err) }()
	}

	interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), sessionInterruptTimeout)
	defer cancelInterrupt()

	err = client.Abort(interruptCtx)

	if turnCancel != nil {
		time.AfterFunc(cancelDrainTimeout, turnCancel)
	}

	return err
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
