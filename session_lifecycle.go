package piacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

// sessionCloseTurnWait bounds how long close blocks for an in-flight turn to
// release the session's single turn admission. Teardown removes the roots that
// turn is still running out of, so close waits rather than racing it.
var sessionCloseTurnWait = 5 * time.Second

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
	residence *pi.SessionResidence,
) error {
	if !pi.ProcessContainmentComplete(runtimeErr) {
		return runtimeErr
	}

	if nativeRelease != nil {
		nativeRelease()
	}

	removeErr := errors.Join(browserShim.Remove(), residence.Remove())
	if sessionRoot != "" {
		removeErr = errors.Join(removeErr, materializeRemoveAll(sessionRoot))
	}

	if removeErr == nil && scratchRelease != nil {
		scratchRelease()
	}

	return errors.Join(runtimeErr, removeErr)
}

// acquireTurn admits one prompt turn. Admission is deliberately fail-fast: pi
// serializes turns within a session, so a second concurrent prompt is refused
// rather than queued. A cancelled caller context always loses the admission,
// and a session whose close has begun never admits another turn.
func (s *agentSession) acquireTurn(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()

	if s.closing {
		s.mu.Unlock()

		return nil, unknownSessionError()
	}

	turn := s.turnQueueLocked()
	s.mu.Unlock()

	select {
	case turn <- struct{}{}:
		return func() { <-turn }, nil
	default:
		return nil, backpressureError("session_prompt")
	}
}

// awaitTurnIdle is close's blocking counterpart to acquireTurn: it waits for
// the in-flight turn to release the session's single admission, or for the
// bound the caller armed to elapse. Only teardown may block on a turn.
func (s *agentSession) awaitTurnIdle(ctx context.Context) (func(), error) {
	s.mu.Lock()
	turn := s.turnQueueLocked()
	s.mu.Unlock()

	select {
	case turn <- struct{}{}:
		return func() { <-turn }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *agentSession) turnQueueLocked() chan struct{} {
	if s.turn == nil {
		s.turn = make(chan struct{}, sessionTurnCapacity)
	}

	return s.turn
}

// fenceAdmission refuses every later turn admission on this session. A close
// ladder fences the session it is tearing down instead of unmapping the id,
// which keeps a prompt from slipping in behind the close while leaving the id
// addressable for the retry a failed boundary still owes.
func (s *agentSession) fenceAdmission() {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
}

// beginClose fences admission and claims the teardown ladder. The first caller
// of an attempt owns it and publishes that attempt's result; a caller arriving
// while it runs waits for the same result instead of tearing the same resources
// down beside it.
//
// The fence is permanent and the claim is not. A teardown that failed leaves
// the session still owning the native scope and the durable rows it did not
// finish, so the claim is released with the failure and the next Close runs the
// ladder again; replaying the recorded failure would leave that scope owned by
// nobody.
func (s *agentSession) beginClose() (*sessionCloseAttempt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closing = true

	if s.closeAttempt != nil {
		return s.closeAttempt, false
	}

	s.closeAttempt = &sessionCloseAttempt{done: make(chan struct{})}

	return s.closeAttempt, true
}

func (s *agentSession) finishClose(attempt *sessionCloseAttempt, err error) {
	s.mu.Lock()
	if err != nil {
		s.closeAttempt = nil
	}
	s.mu.Unlock()

	attempt.err = err

	close(attempt.done)
}

func (s *agentSession) awaitClose(attempt *sessionCloseAttempt) error {
	<-attempt.done

	return attempt.err
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

	if s.closing {
		s.mu.Unlock()

		return unknownSessionError()
	}

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

	if s.closing {
		s.mu.Unlock()

		return unknownSessionError()
	}

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

	// The generation that produced the old stream is gone, so the stream ends
	// with it: its undelivered events are lost and the loss is recorded rather
	// than inferred from an absent router. The next generation opens a fresh
	// incarnation with its own identity, sequence space, and snapshot.
	if fenceErr := s.recordGenerationLoss(ctx); fenceErr != nil {
		s.agent.endNativeConstruction()

		return fenceErr
	}

	defer func() {
		s.recordNativeContainment(err)
		s.agent.endNativeConstruction()
	}()

	s.mu.Lock()

	if s.closing {
		s.mu.Unlock()

		return unknownSessionError()
	}

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

	if adoptErr := s.adoptNativeRoot(nativeRelease); adoptErr != nil {
		finishStart(adoptErr)

		return adoptErr
	}

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

	if s.closing {
		s.mu.Unlock()

		return s.containUnadoptedRelaunch(ctx, relaunched, processRoot)
	}

	s.proc = relaunched
	s.client = client
	s.providerProcessRoot = processRoot
	s.mu.Unlock()
	processRoot.observe(ctx, relaunched)

	generation := s.startPump(client)

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

	commands, commandsErr := client.GetCommands(ctx)
	if commandsErr != nil {
		return s.cleanupFailedRelaunch(relaunched, commandsErr)
	}

	s.mu.Lock()
	s.sessionFilePath = state.SessionFile
	s.launch = spec
	s.availableCommands = commands

	if spec.SessionID != "" {
		s.mirroredRows = 0
	}
	s.mu.Unlock()

	// A relaunch re-runs the extension factories, so the catalog pi advertises
	// now is the one this generation actually has. Re-emitting it — including
	// the explicit empty snapshot — is what keeps a host from holding a
	// catalog the live process no longer serves.
	if emitErr := s.emitAvailableCommandsUpdate(ctx, true); emitErr != nil {
		return s.cleanupFailedRelaunch(relaunched, emitErr)
	}

	if streamErr := s.openLifecycleStream(ctx, generation); streamErr != nil {
		return s.cleanupFailedRelaunch(relaunched, streamErr)
	}

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

	if agentDirErr := s.agent.applyGenerationAgentDir(&dirs); agentDirErr != nil {
		return fail(agentDirErr)
	}

	// A relaunch reads settings.json exactly like a first launch, so a session
	// whose own model came from pi's defaults would otherwise adopt whatever
	// another session selected while this one was running.
	if reconcileErr := s.agent.reconcileHomeStartupDefaults(dirs.AgentDir); reconcileErr != nil {
		return fail(reconcileErr)
	}

	// A durable home keeps one agent directory across every generation, and the
	// session's residence inside it is private and already carries this
	// session's extensions and MCP config; only a generation-private agent
	// directory has to be carried forward.
	if dirs.AgentDir != previous.AgentDir {
		if copyErr := copyGenerationAgentDir(previous.AgentDir, dirs.AgentDir); copyErr != nil {
			return fail(fmt.Errorf("copy pi agent generation: %w", copyErr))
		}
	}

	spec := previous
	spec.Env = cloneStringMap(previous.Env)
	spec.ExtraPathDirs = slices.Clone(previous.ExtraPathDirs)
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

// adoptNativeRoot installs a freshly acquired native-root admission, or
// refuses and returns it when close has already claimed the session, so a
// closing session never takes an admission its teardown has stopped tracking.
func (s *agentSession) adoptNativeRoot(release func()) error {
	s.mu.Lock()

	if s.closing {
		s.mu.Unlock()
		release()

		return unknownSessionError()
	}

	s.nativeRootRelease = release
	s.mu.Unlock()

	return nil
}

// containUnadoptedRelaunch contains a relaunched process that close claimed the
// session out from under. The session never publishes it, so this is its only
// owner and it must reach its containment boundary here rather than survive as
// a pi process nothing tracks.
func (s *agentSession) containUnadoptedRelaunch(
	ctx context.Context,
	relaunched piProcess,
	processRoot *providerProcessRoot,
) error {
	killErr := relaunched.Kill()
	closeErr := relaunched.Close()
	cleanupErr := errors.Join(killErr, closeErr)

	processRoot.retire(context.WithoutCancel(ctx), providerProcessTreeComplete(closeErr))
	s.recordNativeContainment(cleanupErr)

	return errors.Join(unknownSessionError(), cleanupErr)
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

// cancelRouted authenticates the cancel against the session's current turn and
// keeps its native abort fenced from turn completion and admission of the next
// turn. Authorization is unconditional: the route nonce is the anti-stale
// admission and is validated first, so a cancel never reports two rejections,
// and an envelope that is missing, malformed, or names anything other than the
// current turn fails closed before native interrupt. A session with no current
// turn has no nonce to authorize against, so it authorizes nothing and the
// cancel fails closed there too — an unvalidated cancel never reaches a native
// interrupt or a pending dialog.
func (s *agentSession) cancelRouted(ctx context.Context, meta map[string]any) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	route, err := parseInboundTurnRoute(meta)
	if err != nil {
		return err
	}

	s.mu.Lock()
	activeNonce := s.turnNonce
	active := s.cancel != nil && activeNonce != ""
	s.mu.Unlock()

	if !active || route.turnNonce != activeNonce {
		return routeInvalid()
	}

	// The reserved family literal then fails the cancel closed before native
	// interrupt: being a notification it carries no response frame, so the
	// rejection is wire-silent and the cancel is never applied.
	if refusal := refuseLifecycleMeta(meta); refusal != nil {
		return refusal
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
	// observe the ended generation, but joinTurnBoundary keeps it behind this
	// selected containment boundary.
	s.stopPump()
	err = s.terminateCancelledTurn(context.WithoutCancel(ctx), proc, turnCancel, abortErr)

	// The durable commit is not made here. Pi's abort response follows its
	// native terminal ladder and the pump records agent_settled before
	// forwarding it, so the prompt's one settlement point can still adopt the
	// complete durable aborted generation after this boundary completes —
	// which is also what keeps a cancelled cycle's terminal idle ordered
	// after the commit it stands behind.
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

// settleBoundaryInteractions runs the settle phase of the shutdown ladder:
// pending permission and elicitation requests are resolved, and then every
// pending provider-auth flow is cancelled — disarmed, terminalized as
// cancelled/session_closed, and its native login dismissed.
//
// Step 4 has a fixed position rather than a floating obligation: it runs after
// the pending interactions are resolved and *before* the native interrupt, so no
// flow is ever abandoned to a process already being torn down. Close, delete,
// and Agent.Close all reach it through here, and it is idempotent — a boundary
// that already ran it finds nothing pending and nothing nonterminal left.
func (s *agentSession) settleBoundaryInteractions(ctx context.Context) {
	s.cancelPendingInteractions()

	if s.agent != nil && s.agent.providerAuth != nil {
		s.agent.providerAuth.closeSession(ctx, s)
	}
}

func (s *agentSession) wasTurnCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnCancelled
}

// Close shuts the pi process down and releases the session's resources. One
// teardown runs at a time and a caller arriving while it runs reports its
// result; a teardown that succeeded is final, and a teardown that failed is
// retried by the next Close, because every rung of the ladder is idempotent and
// the session still owns whatever the failed attempt left behind.
func (s *agentSession) Close(ctx context.Context) (err error) {
	attempt, owner := s.beginClose()
	if !owner {
		return s.awaitClose(attempt)
	}

	defer func() { s.finishClose(attempt, err) }()

	if s.agent != nil {
		var finish func(error)

		ctx, finish = s.agent.observe.StartPiProcess(ctx, "close")
		defer func() { finish(err) }()
	}

	// Ladder steps 2-4. A boundary that already ran them before its native
	// interrupt finds nothing left to resolve; a direct Close is where they run
	// for the first time.
	s.settleBoundaryInteractions(ctx)

	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	// An in-flight turn owns the native generation, up to and including a
	// relaunch that installs a fresh process, so which process this close must
	// contain is only knowable once that turn's settlement has finished. Close
	// waits for the whole settlement result — the durable commit, the terminal
	// idle, and any quiescence fact — rather than racing the roots that
	// settlement is still writing through. The settlement's own bounded context
	// is what makes the wait terminate.
	err = errors.Join(err, s.awaitSettlement())

	waitCtx, stopWaiting := context.WithTimeout(context.WithoutCancel(ctx), sessionCloseTurnWait)
	defer stopWaiting()

	if releaseTurn, waitErr := s.awaitTurnIdle(waitCtx); waitErr != nil {
		err = errors.Join(err, waitErr)
	} else {
		releaseTurn()
	}

	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()

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

	err = errors.Join(err, s.settleCloseBoundary(context.WithoutCancel(ctx), proc, err))
	s.closeLifecycleSession()

	// Every admission is taken out of the session before it is finalized, so a
	// coincident turn fence releasing the same native root cannot release it
	// twice, and an incomplete containment boundary retains it by leaving it
	// unreleased.
	s.mu.Lock()
	nativeRelease := s.nativeRootRelease
	s.nativeRootRelease = nil
	scratchRelease := s.scratchRootRelease
	s.scratchRootRelease = nil
	sessionRoot := s.sessionRoot
	browserShim := s.browserShim
	residence := s.residence
	s.mu.Unlock()

	err = finalizeSessionRuntimeResources(err, nativeRelease, sessionRoot, scratchRelease, browserShim, residence)

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
