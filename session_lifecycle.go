package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// sessionCloseTurnWait bounds how long close joins an in-flight operation after
// containing its exact native generation. Expiry is itself containment-
// incomplete: no close boundary is committed and no resource is released while
// the holder can still touch the session.
var sessionCloseTurnWait = 5 * time.Second

var sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, sessionCloseTurnWait)
}

var sessionRelaunchWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, sessionCloseTurnWait)
}

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
//
// Work the agent began on its own occupies the same single foreground, and a
// native queue pi has not drained is work already accepted ahead of this
// prompt. Both refuse admission rather than interleaving, because pi serializes
// them against a client turn exactly as it serializes two client turns.
func (s *agentSession) acquireTurn(ctx context.Context) (func(), error) {
	if err := s.admissionFenceError(ctx); err != nil {
		return nil, err
	}

	ctxErr := ctx.Err()

	s.mu.Lock()

	if s.closing {
		s.mu.Unlock()

		return nil, unknownSessionError()
	}

	if s.poisonCause != "" {
		s.mu.Unlock()

		return nil, s.admissionFenceError(ctx)
	}

	if ctxErr != nil {
		s.mu.Unlock()

		return nil, ctxErr
	}

	turn := s.turnQueueLocked()
	s.mu.Unlock()

	if s.agentWorkPending() {
		return nil, backpressureError(limitSessionPrompt)
	}

	select {
	case turn <- struct{}{}:
		return func() { <-turn }, nil
	default:
		return nil, backpressureError(limitSessionPrompt)
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

func (s *agentSession) registerContainmentOutboxLocked(outbox *sessionOutbox) {
	for _, registered := range s.containmentOutboxes {
		if registered == outbox {
			return
		}
	}

	s.containmentOutboxes = append(s.containmentOutboxes, outbox)
}

func (s *agentSession) beginClose() (*sessionCloseAttempt, bool) {
	s.mu.Lock()
	if s.closeAttempt != nil {
		attempt := s.closeAttempt
		s.mu.Unlock()

		return attempt, false
	}

	outbox := s.outbox
	if outbox == nil {
		attempt := &sessionCloseAttempt{done: make(chan struct{}), relaunch: s.relaunchAttempt}
		s.closing = true
		s.closeAttempt = attempt
		s.mu.Unlock()

		return attempt, true
	}

	// TryLock is non-blocking, so it is safe under s.mu and turns the close
	// election plus generation capture into one critical section. A writer that
	// already owns dispatch wins its exact frame; every later writer observes the
	// close fence before it can claim the gate.
	dispatchClaimed := outbox.dispatchMu.TryLock()
	outbox.mu.Lock()
	containment, owner := outbox.claimContainmentLocked(containmentOwnerClose)
	outbox.claimForCloseLocked()

	attempt := &sessionCloseAttempt{
		done:             make(chan struct{}),
		outbox:           outbox,
		containment:      containment,
		containmentOwner: owner,
		relaunch:         s.relaunchAttempt,
	}
	s.closing = true
	s.closeAttempt = attempt
	s.registerContainmentOutboxLocked(outbox)

	outbox.mu.Unlock()
	s.mu.Unlock()

	if dispatchClaimed {
		outbox.dispatchMu.Unlock()
	}

	return attempt, true
}

func (s *agentSession) beginRelaunch() (*sessionRelaunchAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return nil, unknownSessionError()
	}

	if s.relaunchAttempt != nil {
		return nil, errors.New("pi process relaunch already in progress")
	}

	attempt := &sessionRelaunchAttempt{done: make(chan struct{})}
	s.relaunchAttempt = attempt

	return attempt, nil
}

func (s *agentSession) finishRelaunch(attempt *sessionRelaunchAttempt, err error) {
	s.mu.Lock()
	if s.relaunchAttempt == attempt {
		s.relaunchAttempt = nil
	}
	s.mu.Unlock()

	attempt.finishOnce.Do(func() {
		attempt.mu.Lock()
		if !pi.ProcessContainmentComplete(err) {
			attempt.err = err
		}

		close(attempt.done)
		attempt.mu.Unlock()
	})
}

func awaitRelaunch(ctx context.Context, attempt *sessionRelaunchAttempt) error {
	if attempt == nil {
		return nil
	}

	waitCtx, cancelWait := sessionRelaunchWaitContext(ctx)
	defer cancelWait()

	select {
	case <-attempt.done:
	case <-waitCtx.Done():
		attempt.finishOnce.Do(func() {
			attempt.mu.Lock()
			attempt.err = fmt.Errorf("%w: join native relaunch owner: %v",
				pi.ErrProcessContainmentIncomplete, waitCtx.Err())
			close(attempt.done)
			attempt.mu.Unlock()
		})
	}

	attempt.mu.Lock()
	defer attempt.mu.Unlock()

	return attempt.err
}

func (s *agentSession) finishClose(attempt *sessionCloseAttempt, err error) {
	if !attempt.beginFinalSettlement() && attempt.settlement.Load() == closeSettlementQuarantined {
		return
	}

	attempt.finishOnce.Do(func() {
		attempt.err = err
		attempt.settlement.Store(closeSettlementFinished)
		close(attempt.done)
	})
}

func (a *sessionCloseAttempt) beginFinalSettlement() bool {
	state := a.settlement.Load()
	if state == closeSettlementFinalizing || state == closeSettlementFinished {
		return true
	}

	return a.settlement.CompareAndSwap(closeSettlementOpen, closeSettlementFinalizing)
}

func (a *sessionCloseAttempt) result() error {
	<-a.done

	return a.err
}

func (s *agentSession) quarantineCloseAttempt(err error) bool {
	s.mu.Lock()
	attempt := s.closeAttempt
	s.mu.Unlock()

	if attempt == nil || !attempt.settlement.CompareAndSwap(closeSettlementOpen, closeSettlementQuarantined) {
		return false
	}

	attempt.finishOnce.Do(func() {
		attempt.err = err
		close(attempt.done)
	})

	return true
}

func (s *agentSession) awaitClose(attempt *sessionCloseAttempt) error {
	select {
	case <-attempt.done:
		return attempt.err
	default:
	}

	waitCtx, cancelWait := sessionCloseTurnWaitContext(context.Background())
	defer cancelWait()

	select {
	case <-attempt.done:
	case <-waitCtx.Done():
		if !s.quarantineCloseAttempt(fmt.Errorf("%w: join session close attempt: %v",
			pi.ErrProcessContainmentIncomplete, waitCtx.Err())) {
			// The owner crossed the final-settlement CAS first. It has no live
			// producer or native owner left, so join its publication rather than
			// racing an unsynchronized read of the result.
			<-attempt.done
		}
	}

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
	outbox := s.outbox

	boundary := s.nativeBoundary
	if outbox != nil && outbox.nativeBoundary != nil {
		boundary = outbox.nativeBoundary
		s.nativeBoundary = boundary
	}
	s.mu.Unlock()

	if boundary == nil {
		return errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("pi session has no native boundary owner"))
	}

	if proc == nil {
		return errors.New("pi session has no process")
	}

	ended := false

	if outbox != nil {
		outbox.mu.Lock()
		ended = outbox.ended
		outbox.mu.Unlock()
	}

	select {
	case <-proc.Exited():
	default:
		if !ended {
			return nil
		}
	}

	s.stopPump()

	closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	closeErr := boundary.run(closeCtx, "close", proc.Close)
	retireErr := s.retireProviderProcess(closeCtx, closeErr)

	cancelClose()

	containmentErr := errors.Join(closeErr, retireErr)
	s.releaseNativeRootAfterCompletion(containmentErr)

	if containmentErr != nil {
		s.recordNativeContainment(containmentErr)

		return containmentErr
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

	boundary := s.nativeBoundary
	if s.outbox != nil && s.outbox.nativeBoundary != nil {
		boundary = s.outbox.nativeBoundary
		s.nativeBoundary = boundary
	}
	s.mu.Unlock()

	if !pending {
		return nil
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	shutdownErr := boundary.run(shutdownCtx, "shutdown", func() error {
		return proc.Shutdown(shutdownCtx)
	})

	s.stopPump()

	closeErr := boundary.run(shutdownCtx, "close", proc.Close)

	containmentErr := errors.Join(shutdownErr, closeErr)
	containmentErr = errors.Join(containmentErr, s.retireProviderProcess(shutdownCtx, containmentErr))

	cancelShutdown()
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
//
//nolint:gocyclo // Relaunch keeps every ownership transfer and refusal gate in one ordered ladder.
func (s *agentSession) relaunchProcess(ctx context.Context) (err error) {
	attempt, err := s.beginRelaunch()
	if err != nil {
		return err
	}
	defer func() { s.finishRelaunch(attempt, err) }()

	construction, constructionErr := s.agent.beginNativeConstruction()
	if constructionErr != nil {
		return constructionErr
	}

	constructionOwned := true
	defer func() {
		if !constructionOwned {
			return
		}

		cleanupErr := s.agent.cleanupNativeConstruction(context.WithoutCancel(ctx), construction, err)
		err = errors.Join(err, cleanupErr)
		s.agent.finishNativeConstruction(construction, cleanupErr != nil, cleanupErr)
	}()

	// The generation that produced the old stream is gone, so the stream ends
	// with it: its undelivered events are lost and the loss is recorded rather
	// than inferred from an absent router. The next generation opens a fresh
	// incarnation with its own identity, sequence space, and snapshot.
	if fenceErr := s.recordGenerationLoss(ctx); fenceErr != nil {
		return fenceErr
	}

	defer func() {
		s.recordNativeContainment(err)
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

	s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.generationRoot = spec.Containment.GenerationRoot
		owner.sessionRoot = spec.Containment.GenerationRoot
	})

	keepGeneration := false
	defer func() {
		if !keepGeneration && pi.ProcessContainmentComplete(err) {
			removeErr := materializeRemoveAll(spec.Containment.GenerationRoot)
			if removeErr != nil {
				attempt.mu.Lock()
				attempt.err = errors.Join(attempt.err, removeErr)
				attempt.mu.Unlock()
			}

			err = errors.Join(err, removeErr)
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

	s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.nativeRelease = nativeRelease
	})

	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()

	if closing {
		nativeRelease()
		s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
			owner.nativeRelease = nil
		})
		finishStart(unknownSessionError())

		return unknownSessionError()
	}

	if closedErr := s.agent.ensureOpen(); closedErr != nil {
		nativeRelease()
		s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
			owner.nativeRelease = nil
		})
		finishStart(closedErr)

		return closedErr
	}

	// From the first successful spawn onward, any panic is converted into an
	// immutable relaunch result after the exact replacement has been contained.
	// This defer is registered after the resource/root defers so it runs first
	// and gives them the containment result they must obey.
	defer func() {
		if recover() == nil {
			return
		}

		panicErr := errors.Join(
			errors.New("pi process relaunch callback panicked"),
			s.agent.nativeConstructionError(construction),
		)
		containmentErr := s.containHoistedRelaunch(context.WithoutCancel(ctx), attempt)
		err = errors.Join(panicErr, containmentErr)
		s.recordNativeContainment(containmentErr)
	}()

	spawnStarted := time.Now()

	if contextErr := ctx.Err(); contextErr != nil {
		finishStart(contextErr)

		return contextErr
	}

	relaunched, client, processRoot, err := s.agent.startTrackedPiProcess(startCtx, spec)
	s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.proc = relaunched
		owner.client = client

		owner.processRoot = processRoot
		if owner.err == nil {
			owner.err = err
		}
	})

	if err == nil {
		if !s.agent.transferConstructionToRelaunch(construction, attempt) {
			return s.agent.nativeConstructionError(construction)
		}

		constructionOwned = false
	}

	observationErr := observeRuntimeStartupStage(
		startCtx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSpawn, spawnStarted, err,
	)

	finishStart(err)

	if observationErr != nil {
		containmentErr := s.containHoistedRelaunch(context.WithoutCancel(ctx), attempt)

		return errors.Join(err, observationErr, containmentErr)
	}

	if err != nil {
		s.recordNativeContainment(err)

		return err
	}

	if closedErr := s.agent.ensureOpen(); closedErr != nil {
		containmentErr := s.containHoistedRelaunch(context.WithoutCancel(ctx), attempt)

		return errors.Join(closedErr, containmentErr)
	}

	readinessStarted := time.Now()

	generationCtx, generationCancel := context.WithCancel(context.Background())
	if startErr := client.Start(generationCtx); startErr != nil {
		generationCancel()

		observationErr := observeRuntimeStartupStage(
			ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupReadiness, readinessStarted, startErr,
		)

		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
		killErr := attempt.nativeBoundary.run(cleanupCtx, "kill", relaunched.Kill)

		closeErr := attempt.nativeBoundary.run(cleanupCtx, "close", relaunched.Close)

		cleanupErr := errors.Join(killErr, closeErr)
		cleanupErr = errors.Join(cleanupErr, s.retireProviderRoot(cleanupCtx, processRoot, cleanupErr))

		cancelCleanup()
		s.recordNativeContainment(cleanupErr)

		return errors.Join(startErr, observationErr, cleanupErr)
	}

	if observationErr := observeRuntimeStartupStage(
		ctx, s.agent.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupReadiness, readinessStarted, nil,
	); observationErr != nil {
		generationCancel()

		containmentErr := s.containHoistedRelaunch(context.WithoutCancel(ctx), attempt)

		return errors.Join(observationErr, containmentErr)
	}

	generation, outbox, published := s.publishRuntimeGeneration(
		generationCtx, generationCancel, relaunched, client, processRoot, nativeRelease, attempt.nativeBoundary,
	)
	if !published {
		return s.containUnadoptedRelaunch(ctx, attempt)
	}

	s.mu.Lock()
	publishedAttempt := s.relaunchAttempt == attempt
	s.mu.Unlock()

	if publishedAttempt {
		attempt.mu.Lock()
		attempt.outbox = outbox
		attempt.nativeRelease = nil
		attempt.mu.Unlock()
	}

	if retryErr := client.SetAutoRetry(ctx, s.autoRetry); retryErr != nil {
		return s.cleanupFailedRelaunch(ctx, outbox, retryErr)
	}

	// pi computes a fresh session file path per process even for the same
	// --session-id, so the mirror source must be re-captured after every
	// relaunch or commitMirror would silently read a path that never exists.
	state, stateErr := client.GetState(ctx)
	if stateErr != nil {
		return s.cleanupFailedRelaunch(ctx, outbox, stateErr)
	}

	if state.SessionID != string(s.id) {
		return s.cleanupFailedRelaunch(
			ctx,
			outbox,
			fmt.Errorf("native session id drift on relaunch: expected %s, got %s", s.id, state.SessionID),
		)
	}

	commands, commandsErr := client.GetCommands(ctx)
	if commandsErr != nil {
		return s.cleanupFailedRelaunch(ctx, outbox, commandsErr)
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
		return s.cleanupFailedRelaunch(ctx, outbox, emitErr)
	}

	if streamErr := s.openLifecycleStream(ctx, generation); streamErr != nil {
		return s.cleanupFailedRelaunch(ctx, outbox, streamErr)
	}

	if establishmentErr := outbox.acceptEstablishment(); establishmentErr != nil {
		return s.cleanupFailedRelaunch(ctx, outbox, establishmentErr)
	}

	s.drainOutbox(ctx, outbox)
	outbox.wake()

	keepGeneration = true

	return nil
}

func (s *agentSession) containHoistedRelaunch(ctx context.Context, attempt *sessionRelaunchAttempt) error {
	attempt.mu.Lock()
	outbox := attempt.outbox
	proc := attempt.proc
	root := attempt.processRoot
	nativeRelease := attempt.nativeRelease
	attempt.mu.Unlock()

	if outbox != nil {
		s.containGeneration(ctx, outbox, "the native process relaunch callback panicked")
		containmentErr, _ := outbox.awaitContainment()

		return containmentErr
	}

	if proc == nil {
		return nil
	}

	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, sessionShutdownTimeout)
	defer cancelCleanup()

	killErr := attempt.nativeBoundary.run(cleanupCtx, "kill", proc.Kill)

	closeErr := attempt.nativeBoundary.run(cleanupCtx, "close", proc.Close)

	containmentErr := errors.Join(killErr, closeErr)

	containmentErr = errors.Join(containmentErr, s.retireProviderRoot(cleanupCtx, root, containmentErr))
	if pi.ProcessContainmentComplete(containmentErr) && nativeRelease != nil {
		nativeRelease()
		attempt.mu.Lock()
		attempt.nativeRelease = nil
		attempt.mu.Unlock()
	}

	return containmentErr
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

// containUnadoptedRelaunch contains a relaunched process that close claimed the
// session out from under. The session never publishes it, so this is its only
// owner and it must reach its containment boundary here rather than survive as
// a pi process nothing tracks.
func (s *agentSession) containUnadoptedRelaunch(
	ctx context.Context,
	attempt *sessionRelaunchAttempt,
) error {
	attempt.mu.Lock()
	relaunched := attempt.proc
	processRoot := attempt.processRoot
	nativeRelease := attempt.nativeRelease
	attempt.mu.Unlock()

	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancelCleanup()

	killErr := attempt.nativeBoundary.run(cleanupCtx, "kill", relaunched.Kill)

	closeErr := attempt.nativeBoundary.run(cleanupCtx, "close", relaunched.Close)

	cleanupErr := errors.Join(killErr, closeErr)

	cleanupErr = errors.Join(cleanupErr, s.retireProviderRoot(cleanupCtx, processRoot, cleanupErr))
	if pi.ProcessContainmentComplete(cleanupErr) && nativeRelease != nil {
		nativeRelease()
		attempt.mu.Lock()
		attempt.nativeRelease = nil
		attempt.mu.Unlock()
	}

	s.recordNativeContainment(cleanupErr)

	return errors.Join(unknownSessionError(), cleanupErr)
}

func (s *agentSession) cleanupFailedRelaunch(ctx context.Context, outbox *sessionOutbox, cause error) error {
	s.containGeneration(ctx, outbox, "native process relaunch setup failed")

	outbox.mu.Lock()
	containment := outbox.containment
	closeOwns := containment != nil && containment.owner == containmentOwnerClose
	outbox.mu.Unlock()

	if closeOwns {
		return cause
	}

	containmentErr, _ := outbox.awaitContainment()

	return errors.Join(cause, containmentErr)
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
// adopting a fresh native generation. This rung does not commit by itself:
// session close commits its durable boundary after containment, while delete's
// prior tombstone fences persistence before it reaches the same ladder.
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
	s.mu.Lock()
	turnCancel := s.cancel
	client := s.client
	outbox := s.outbox

	boundary := s.nativeBoundary
	if outbox != nil && outbox.nativeBoundary != nil {
		boundary = outbox.nativeBoundary
	}

	if turnCancel != nil && commitSettled {
		s.turnCommitOnCancel = true
	}
	s.mu.Unlock()

	if boundary == nil {
		return errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("pi session has no native boundary owner"))
	}

	joinCtx, cancelJoin := sessionCloseTurnWaitContext(context.WithoutCancel(ctx))
	interactionErr := s.settleBoundaryInteractions(joinCtx, outbox)

	cancelJoin()

	if interactionErr != nil {
		// A response writer that ignored cancellation must not veto mandatory
		// containment. The exact interaction tracker stays live; process close
		// interrupts its native stdin and the selected generation fence is memoized
		// for every retry.
		if turnCancel != nil {
			containmentErr := s.fenceActiveTurnLocked(context.WithoutCancel(ctx))

			err = errors.Join(interactionErr, containmentErr)
			if outbox != nil {
				s.quarantineLifecycleGeneration(outbox.generation, err)
			}

			s.recordNativeContainment(err)

			return err
		}

		containmentErr := s.containGenerationSync(
			context.WithoutCancel(ctx), outbox, "a native interaction response did not finish before cancellation",
		)
		err = errors.Join(interactionErr, containmentErr)
		s.quarantineLifecycleGeneration(outbox.generation, err)
		s.recordNativeContainment(err)

		return err
	}

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

	return boundary.run(interruptCtx, "abort", func() error {
		return client.Abort(interruptCtx)
	})
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
	outbox := s.outbox
	s.mu.Unlock()

	containment, owner := s.claimTurnContainment(outbox)

	switch {
	case containment == nil:
		err = errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("active pi turn has no native generation"))
		s.recordNativeContainment(err)

		turnCancel()
	case owner:
		containCtx, cancelContain := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
		err = runNativeBoundaryStep(containCtx, "turn containment ladder", func() error {
			return s.stopTurnGeneration(containCtx, outbox)
		})

		cancelContain()
		outbox.finishContainment(containment, err)

		if err != nil {
			s.recordNativeContainment(err)
		}

		turnCancel()
	default:
		err, _ = outbox.awaitContainment()

		turnCancel()
	}

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

func (s *agentSession) claimTurnContainment(outbox *sessionOutbox) (*generationContainment, bool) {
	if outbox == nil {
		return nil, false
	}

	outbox.mu.Lock()
	containment, owner := outbox.claimContainmentLocked(containmentOwnerTurn)
	outbox.mu.Unlock()

	return containment, owner
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

func (s *agentSession) stopTurnGeneration(ctx context.Context, outbox *sessionOutbox) error {
	if outbox == nil || outbox.proc == nil {
		return errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("active pi turn has no contained process root"))
	}

	var abortErr error

	if outbox.client != nil {
		interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), sessionCancelAbortGrace)
		abortErr = outbox.nativeBoundary.run(interruptCtx, "abort", func() error {
			return outbox.client.Abort(interruptCtx)
		})

		cancelInterrupt()
	}

	if outbox.pumpCancel != nil {
		outbox.pumpCancel()
	}

	if outbox.pumpDone != nil {
		select {
		case <-outbox.pumpDone:
		case <-ctx.Done():
			return fmt.Errorf("%w: wait for native generation pump: %v", pi.ErrProcessContainmentIncomplete, ctx.Err())
		}
	}

	killErr := outbox.nativeBoundary.run(ctx, "kill", outbox.proc.Kill)

	closeErr := outbox.nativeBoundary.run(ctx, "close", outbox.proc.Close)

	containmentErr := errors.Join(killErr, closeErr)
	if !pi.ProcessContainmentComplete(abortErr) {
		containmentErr = errors.Join(containmentErr, abortErr)
	}

	if outbox.processRoot != nil {
		complete := providerProcessTreeComplete(containmentErr)
		retireErr := runNativeBoundaryStep(ctx, "retirement", func() error {
			outbox.processRoot.retire(context.WithoutCancel(ctx), complete)

			return nil
		})
		containmentErr = errors.Join(containmentErr, retireErr)

		if complete && retireErr == nil {
			s.mu.Lock()
			if s.providerProcessRoot == outbox.processRoot {
				s.providerProcessRoot = nil
			}
			s.mu.Unlock()
		}
	}

	s.releaseNativeRootAfterCompletion(containmentErr)

	return containmentErr
}

// cancelPendingInteractions marks the turn cancelled and cancels every
// registered host request. It is retained as the narrow test surface for the
// registration map; production boundaries use settleBoundaryInteractions so
// queued dialogs and handler joins are part of the same fence.
func (s *agentSession) cancelPendingInteractions() {
	s.mu.Lock()
	if s.cancel != nil || len(s.pendingDialogs) > 0 {
		s.turnCancelled = true
	}

	dialogCancels := make([]context.CancelCauseFunc, 0, len(s.pendingDialogs))
	for id, entry := range s.pendingDialogs {
		dialogCancels = append(dialogCancels, entry.cancel)

		delete(s.pendingDialogs, id)
	}
	s.mu.Unlock()

	for _, cancel := range dialogCancels {
		cancel(errSessionInteractionClosed)
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
func (s *agentSession) settleBoundaryInteractions(ctx context.Context, outbox *sessionOutbox) error {
	s.mu.Lock()

	var (
		delivery     *turnDelivery
		interactions *generationProducers
	)

	sealInteractions := false

	if outbox != nil {
		outbox.mu.Lock()
		if !outbox.interactionsClosed {
			outbox.interactionsClosed = true
			sealInteractions = true
		}

		delivery = outbox.turn
		interactions = outbox.interactions
		outbox.mu.Unlock()
	}

	if s.cancel != nil || len(s.pendingDialogs) > 0 || delivery != nil {
		s.turnCancelled = true
	}

	dialogCancels := make([]context.CancelCauseFunc, 0, len(s.pendingDialogs))
	for id, entry := range s.pendingDialogs {
		dialogCancels = append(dialogCancels, entry.cancel)

		delete(s.pendingDialogs, id)
	}

	s.mu.Unlock()

	if sealInteractions && interactions != nil {
		interactions.releaseRoot()
	}

	for _, cancel := range dialogCancels {
		cancel(errSessionInteractionClosed)
	}

	if delivery != nil {
		delivery.abandonQueuedDialogs(context.WithoutCancel(ctx), s)
	}

	if s.agent != nil && s.agent.providerAuth != nil {
		s.agent.providerAuth.closeSession(ctx, s)
	}

	if interactions != nil {
		return interactions.wait(ctx)
	}

	return nil
}

func (s *agentSession) wasTurnCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnCancelled
}

func (s *agentSession) Close(ctx context.Context) error {
	attempt, owner := s.beginClose()
	if owner {
		_ = s.closeOwned(ctx, attempt)
	}

	return s.awaitClose(attempt)
}

func (s *agentSession) closeOwned(ctx context.Context, attempt *sessionCloseAttempt) (err error) {
	containmentFinished := false

	defer func() {
		if recover() != nil {
			panicErr := generationContainmentPanicError("host close ladder")
			err = errors.Join(err, panicErr)
			s.recordNativeContainment(panicErr)

			if attempt.containmentOwner && !containmentFinished {
				attempt.outbox.finishContainment(attempt.containment, panicErr)

				containmentFinished = true
			}

			if s.agent != nil {
				s.agent.log.ErrorContext(context.Background(), "pi session close panicked",
					slog.String(acpFieldSessionID, string(s.id)),
				)
			}
		}

		s.finishClose(attempt, err)
	}()

	closeCtx := context.WithoutCancel(ctx)

	if s.agent != nil {
		var finish func(error)

		closeCtx, finish = s.agent.observe.StartPiProcess(closeCtx, "close")
		defer func() { finish(err) }()
	}

	joinCtx, cancelJoin := sessionCloseTurnWaitContext(closeCtx)
	// Give every dialog/action response writer its ordinary pre-interrupt join.
	// A hostile native write may ignore the context, so this bounded attempt
	// cannot veto the Close ladder; the same sealed tracker is joined again
	// after native containment has made the write interruptible.
	_ = s.settleBoundaryInteractions(joinCtx, attempt.outbox)

	cancelJoin()

	// Cancel the broader turn after every admitted host handler has either
	// observed cancellation or exhausted the bounded pre-interrupt join.
	s.mu.Lock()
	turnCancel := s.cancel
	s.mu.Unlock()

	if turnCancel != nil {
		turnCancel()
	}

	containCtx, cancelContain := context.WithTimeout(closeCtx, sessionSettleTimeout)

	switch {
	case attempt.containment == nil:
		err = errors.Join(err, runNativeBoundaryStep(containCtx, "host close ladder", func() error {
			return s.stopHostGenerationWithoutOutbox(containCtx)
		}))
	case attempt.containmentOwner:
		containmentErr := runNativeBoundaryStep(containCtx, "host close ladder", func() error {
			return s.stopNativeGeneration(containCtx, attempt.outbox)
		})
		attempt.outbox.finishContainment(attempt.containment, containmentErr)

		containmentFinished = true
		err = errors.Join(err, containmentErr)
	default:
		containmentErr, _ := attempt.outbox.awaitContainment()
		containmentFinished = true
		err = errors.Join(err, containmentErr)
	}

	cancelContain()

	err = errors.Join(err, awaitRelaunch(closeCtx, attempt.relaunch))

	if !pi.ProcessContainmentComplete(err) {
		if attempt.outbox != nil {
			s.quarantineLifecycleGeneration(attempt.outbox.generation, err)
		}

		s.recordNativeContainment(err)

		// Native containment did not complete. From this point the exact owner is
		// immutable quarantine: no lifecycle lock, notification, terminal state,
		// additional join, or resource release is permitted while native work may
		// still be live.
		return err
	}

	err = errors.Join(err, s.awaitPoisonContainment())
	if !pi.ProcessContainmentComplete(err) {
		if attempt.outbox != nil {
			s.quarantineLifecycleGeneration(attempt.outbox.generation, err)
		}

		s.recordNativeContainment(err)

		return err
	}

	joinCtx, stopJoining := sessionCloseTurnWaitContext(closeCtx)
	defer stopJoining()

	if attempt.outbox == nil {
		err = errors.Join(err, s.stopPumpBounded(joinCtx))
	} else {
		if attempt.outbox.pumpDone != nil {
			err = errors.Join(err, attempt.outbox.producers.wait(joinCtx))
			if !pi.ProcessContainmentComplete(err) {
				s.recordNativeContainment(err)

				return err
			}
		} else {
			err = errors.Join(err, attempt.outbox.producers.waitChildren(joinCtx))
		}

		err = errors.Join(err, attempt.outbox.interactions.wait(joinCtx))
	}

	err = errors.Join(err, s.awaitSettlementContext(joinCtx))
	if releaseTurn, waitErr := s.awaitTurnIdle(joinCtx); waitErr != nil {
		err = errors.Join(err, pi.ErrProcessContainmentIncomplete,
			fmt.Errorf("join session operation holder: %w", waitErr))
	} else {
		releaseTurn()
	}

	proc := s.process()
	if attempt.outbox != nil {
		proc = attempt.outbox.proc
	}

	err = errors.Join(err, s.nativeContainmentError())
	if !pi.ProcessContainmentComplete(err) {
		// A bounded holder/action/join timeout is retained ownership, not a
		// lifecycle terminal. Do not acquire lcMu, publish terminal state, close
		// the emitter, or release any resource while that owner may still touch
		// the session.
		if attempt.outbox != nil {
			s.quarantineLifecycleGeneration(attempt.outbox.generation, err)
		}

		s.recordNativeContainment(err)

		return err
	}

	if !attempt.beginFinalSettlement() {
		// A bounded reentrant/concurrent join already memoized the attempt as
		// incomplete. Its owner may finish local unwinding, but it must not publish
		// or release anything after that quarantine result became immutable.
		return attempt.result()
	}

	err = errors.Join(err, s.settleCloseBoundary(closeCtx, proc, err))
	s.closeLifecycleSession()

	if err != nil {
		if !pi.ProcessContainmentComplete(err) {
			s.recordNativeContainment(err)
		}

		return err
	}

	// Every admission is taken out of the session before it is finalized, so a
	// coincident turn fence releasing the same native root cannot release it
	// twice, and an incomplete containment boundary retains it by leaving it
	// unreleased.
	s.mu.Lock()
	nativeRelease := s.nativeRootRelease
	scratchRelease := s.scratchRootRelease
	sessionRoot := s.sessionRoot
	browserShim := s.browserShim
	residence := s.residence
	s.mu.Unlock()

	err = finalizeSessionRuntimeResources(err, nativeRelease, sessionRoot, scratchRelease, browserShim, residence)
	if err == nil {
		s.mu.Lock()
		if s.nativeRootRelease != nil {
			s.nativeRootRelease = nil
		}

		if s.scratchRootRelease != nil {
			s.scratchRootRelease = nil
		}
		s.mu.Unlock()
	}

	if s.agent != nil {
		s.agent.observe.RecordPiProcessExit(closeCtx, "closed", err)
	}

	return err
}

func (s *agentSession) stopHostGenerationWithoutOutbox(ctx context.Context) error {
	s.mu.Lock()
	proc := s.proc
	root := s.providerProcessRoot

	boundary := s.nativeBoundary
	s.mu.Unlock()

	if proc == nil {
		return nil
	}

	if boundary == nil {
		return errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("pi session has no native boundary owner"))
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	shutdownErr := boundary.run(shutdownCtx, "shutdown", func() error {
		return proc.Shutdown(shutdownCtx)
	})

	cancelShutdown()

	s.stopPump()

	closeErr := boundary.run(ctx, "close", proc.Close)

	containmentErr := errors.Join(shutdownErr, closeErr)

	if root != nil {
		complete := providerProcessTreeComplete(containmentErr)
		retireErr := runNativeBoundaryStep(ctx, "retirement", func() error {
			root.retire(context.WithoutCancel(ctx), complete)

			return nil
		})

		containmentErr = errors.Join(containmentErr, retireErr)
		if complete && retireErr == nil {
			s.mu.Lock()
			if s.providerProcessRoot == root {
				s.providerProcessRoot = nil
			}
			s.mu.Unlock()
		}
	}

	return containmentErr
}

func (s *agentSession) retireProviderProcess(ctx context.Context, containmentErr error) error {
	s.mu.Lock()
	root := s.providerProcessRoot
	s.mu.Unlock()

	return s.retireProviderRoot(ctx, root, containmentErr)
}

func (s *agentSession) retireProviderRoot(
	ctx context.Context,
	root *providerProcessRoot,
	containmentErr error,
) error {
	if root == nil {
		return nil
	}

	complete := providerProcessTreeComplete(containmentErr)

	retireErr := runNativeBoundaryStep(ctx, "retirement", func() error {
		root.retire(context.WithoutCancel(ctx), complete)

		return nil
	})
	if complete && retireErr == nil {
		s.mu.Lock()
		if s.providerProcessRoot == root {
			s.providerProcessRoot = nil
		}
		s.mu.Unlock()
	}

	return retireErr
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

func (s *agentSession) observeProviderProcessBounded(ctx context.Context, outbox *sessionOutbox) error {
	if outbox == nil || outbox.processRoot == nil {
		return nil
	}

	observeCtx, cancelObserve := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancelObserve()

	err := runNativeBoundaryStep(observeCtx, "provider process observation", func() error {
		outbox.processRoot.observe(observeCtx, outbox.proc)

		return nil
	})
	if err == nil {
		return nil
	}

	containmentErr := s.containGenerationSync(
		context.WithoutCancel(ctx), outbox, "the provider process observer failed",
	)
	s.recordNativeContainment(errors.Join(err, containmentErr))

	return errors.Join(err, containmentErr)
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
