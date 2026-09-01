package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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

// finalizeSessionNativeResources releases each admission only after its
// native boundary completes. An incomplete native boundary
// retains its admission and private session root because descendants may
// still use them.
func finalizeSessionNativeResources(
	agent *Agent,
	runtimeErr error,
	generationRoot string,
	generationPrepared bool,
	sessionRoot string,
	browserShim *pi.BrowserShim,
	residence *pi.SessionResidence,
) error {
	if !nativeContainmentComplete(runtimeErr) {
		return runtimeErr
	}

	if agent != nil && generationRoot != "" {
		var disposeErr error

		if generationPrepared {
			disposeErr = agent.disposeNativeTree(context.Background(), generationRoot)
		} else {
			disposeErr = agent.removeNativeTree(generationRoot)
		}

		if disposeErr != nil {
			return errors.Join(runtimeErr, disposeErr)
		}
	}

	removeErr := errors.Join(browserShim.Remove(), residence.Remove())
	if sessionRoot != "" {
		removeErr = errors.Join(removeErr, materializeRemoveAll(sessionRoot))
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
		select {
		case <-attempt.done:
			if errors.Is(attempt.err, ErrNativeTreeBusy) && nativeContainmentComplete(attempt.err) {
				retry := &sessionCloseAttempt{
					done:        make(chan struct{}),
					outbox:      attempt.outbox,
					containment: attempt.containment,
					relaunch:    attempt.relaunch,
				}
				s.closeAttempt = retry
				s.mu.Unlock()

				return retry, true
			}
		default:
		}
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
		if !nativeContainmentComplete(err) {
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
				ErrContainmentIncomplete, waitCtx.Err())
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
			ErrContainmentIncomplete, waitCtx.Err())) {
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
		return errors.Join(ErrContainmentIncomplete, errors.New("pi session has no native boundary owner"))
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

	cancelClose()

	containmentErr := closeErr
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

	containmentErr := terminalNativeClose(shutdownErr, closeErr)

	cancelShutdown()
	s.recordNativeContainment(containmentErr)

	if !nativeContainmentComplete(containmentErr) {
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
// process reaches its native boundary. Its extension factories run again, so
// their fixed tool registry is rebuilt from the MCP server's current view.
//
//nolint:gocyclo // Relaunch is one ordered ownership transfer with shared rollback.
func (s *agentSession) relaunchProcess(ctx context.Context) (err error) {
	attempt, err := s.beginRelaunch()
	if err != nil {
		return err
	}
	defer func() { s.finishRelaunch(attempt, err) }()

	if releaseErr := s.releaseRetainedGeneration(context.WithoutCancel(ctx)); releaseErr != nil {
		return releaseErr
	}

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

	previousLaunch := s.launch
	s.mu.Unlock()

	replacement, err := s.nextRuntimeLaunch(ctx, previousLaunch)
	spec := replacement.spec

	if spec.NativeRoot != "" {
		s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
			owner.generationRoot = spec.NativeRoot
			owner.generationPrepared = replacement.prepared
			owner.browserShim = replacement.browserShim
			owner.residence = replacement.ownedResidence
		})
	}

	if err != nil {
		return err
	}

	keepGeneration := false
	defer func() {
		if !keepGeneration && !constructionOwned && nativeContainmentComplete(err) {
			stillPrepared, removeErr := s.cleanupGenerationRoot(
				context.WithoutCancel(ctx), spec.NativeRoot, replacement.prepared,
			)
			if removeErr != nil {
				s.retainGeneration(spec.NativeRoot, stillPrepared, removeErr)

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

	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()

	if closing {
		finishStart(unknownSessionError())

		return unknownSessionError()
	}

	if closedErr := s.agent.ensureOpen(); closedErr != nil {
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

	if contextErr := ctx.Err(); contextErr != nil {
		finishStart(contextErr)

		return contextErr
	}

	relaunched, client, err := s.agent.startTrackedPiProcess(startCtx, spec)
	s.agent.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.proc = relaunched
		owner.client = client

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

	finishStart(err)

	if err != nil {
		s.recordNativeContainment(err)

		return err
	}

	if closedErr := s.agent.ensureOpen(); closedErr != nil {
		containmentErr := s.containHoistedRelaunch(context.WithoutCancel(ctx), attempt)

		return errors.Join(closedErr, containmentErr)
	}

	generationCtx, generationCancel := context.WithCancel(context.Background())
	if startErr := client.Start(generationCtx); startErr != nil {
		generationCancel()

		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
		killErr := attempt.nativeBoundary.run(cleanupCtx, "kill", relaunched.Kill)

		closeErr := attempt.nativeBoundary.run(cleanupCtx, "close", relaunched.Close)

		cleanupErr := terminalNativeClose(killErr, closeErr)

		cancelCleanup()
		s.recordNativeContainment(cleanupErr)

		return errors.Join(startErr, cleanupErr)
	}

	generation, outbox, published := s.publishRuntimeGeneration(
		generationCtx, generationCancel, relaunched, client, attempt.nativeBoundary,
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
		attempt.mu.Unlock()
	}

	s.mu.Lock()
	autoRetry := s.autoRetry
	thinkingLevel := s.thinkingLevel
	model := s.model
	s.mu.Unlock()

	if retryErr := client.SetAutoRetry(ctx, autoRetry); retryErr != nil {
		return s.cleanupFailedRelaunch(ctx, outbox, retryErr)
	}

	if thinkingLevel != "" {
		if thinkingErr := client.SetThinkingLevel(ctx, thinkingLevel); thinkingErr != nil {
			return s.cleanupFailedRelaunch(ctx, outbox, thinkingErr)
		}
	}

	if provider, modelID, ok := strings.Cut(model, "/"); ok && provider != "" && modelID != "" {
		if _, modelErr := client.SetModel(ctx, provider, modelID); modelErr != nil {
			return s.cleanupFailedRelaunch(ctx, outbox, modelErr)
		}
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
	s.generationPrepared = replacement.prepared

	s.browserShim = replacement.browserShim
	if replacement.residence != nil {
		s.residence = replacement.residence
	}

	s.availableCommands = commands

	s.thinkingLevel = state.ThinkingLevel
	if selected := stateModelRef(state); selected != "" {
		s.model = selected
		if state.Model != nil {
			s.contextWindowSize = state.Model.ContextWindow
		}
	}

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

	containmentErr := terminalNativeClose(killErr, closeErr)

	return containmentErr
}

type runtimeLaunchMaterialization struct {
	spec           pi.LaunchSpec
	browserShim    *pi.BrowserShim
	residence      *pi.SessionResidence
	ownedResidence *pi.SessionResidence
	prepared       bool
}

func (s *agentSession) nextRuntimeLaunch(
	ctx context.Context,
	previous pi.LaunchSpec,
) (runtimeLaunchMaterialization, error) {
	s.mu.Lock()
	previousPrepared := s.generationPrepared
	s.mu.Unlock()

	if previous.NativeRoot != "" && previousPrepared {
		if err := s.agent.reclaimNativeTree(context.WithoutCancel(ctx), previous.NativeRoot); err != nil {
			return runtimeLaunchMaterialization{}, err
		}

		s.mu.Lock()
		if s.launch.NativeRoot == previous.NativeRoot {
			s.generationPrepared = false
		}
		s.mu.Unlock()
	}

	if previous.NativeRoot != "" {
		s.mu.Lock()
		commitPending := s.managedCommitPending
		s.mu.Unlock()

		if commitPending {
			if err := s.commitMirror(ctx); err != nil {
				return runtimeLaunchMaterialization{}, err
			}
		}

		if err := s.agent.removeNativeTree(previous.NativeRoot); err != nil {
			return runtimeLaunchMaterialization{}, fmt.Errorf("remove prior runtime generation: %w", err)
		}

		s.mu.Lock()
		if s.launch.NativeRoot == previous.NativeRoot {
			s.launch.NativeRoot = ""
			s.sessionFilePath = ""
		}
		s.mu.Unlock()
	}

	if err := s.agent.nativeAdmissionError(); err != nil {
		return runtimeLaunchMaterialization{}, err
	}

	entries, err := s.agent.loadStoreEntries(ctx, s.agent.sessionStore(), SessionKey{SessionID: string(s.id)})
	if err != nil {
		return runtimeLaunchMaterialization{}, err
	}

	dirs, err := createSessionGeneration(s.sessionRoot)
	if err != nil {
		return runtimeLaunchMaterialization{}, err
	}

	result := runtimeLaunchMaterialization{}
	fail := func(cause error) (runtimeLaunchMaterialization, error) {
		return result, errors.Join(cause, materializeRemoveAll(dirs.Root))
	}

	if agentDirErr := s.agent.applyGenerationAgentDir(&dirs); agentDirErr != nil {
		return fail(agentDirErr)
	}

	result.spec = previous
	result.spec.Env = cloneStringMap(previous.Env)

	if result.spec.Env == nil {
		result.spec.Env = make(map[string]string)
	}

	result.spec.ExtraPathDirs = slices.Clone(previous.ExtraPathDirs)
	result.spec.AgentDir = dirs.AgentDir
	result.spec.SessionDir = dirs.SessionDir
	result.spec.NativeRoot = dirs.Root
	result.spec.SessionPath = ""
	result.spec.SessionID = string(s.id)
	result.spec.BaseEnvironment = s.agent.nativeBaseEnvironment()

	result.browserShim = s.agent.newSessionBrowserShim(dirs.Root)
	result.spec.BrowserShim = result.browserShim

	if reconcileErr := s.agent.reconcileHomeStartupDefaults(dirs.AgentDir); reconcileErr != nil {
		return fail(reconcileErr)
	}

	if s.agent.options.Home == "" {
		var mcpConfig *pi.MCPConfig

		if len(s.mcpServers) > 0 {
			config := mcpConfigForServers(s.mcpServers)
			mcpConfig = &config
		}

		residence, files, residenceErr := pi.CreateSessionResidence(dirs.AgentDir, mcpConfig)
		if residenceErr != nil {
			return fail(residenceErr)
		}

		result.residence = residence
		result.ownedResidence = residence

		agentDir := pi.AgentDir{Root: dirs.AgentDir, SeedFiles: s.agent.options.SeedFiles}
		if writeErr := agentDir.Write(); writeErr != nil {
			var seedErr *pi.SeedFileError
			if errors.As(writeErr, &seedErr) {
				return fail(unsupportedField("seedFiles." + seedErr.Name))
			}

			return fail(writeErr)
		}

		seeded, resourceErr := agentDirExplicitResources(agentDir)
		if resourceErr != nil {
			return fail(resourceErr)
		}

		result.spec.ExtensionPaths = append(slices.Clone(seeded.Extensions), files.ExtensionPaths...)
		result.spec.SkillPaths = seeded.Skills
		result.spec.PromptTemplatePaths = seeded.PromptTemplates

		if files.MCPConfigPath == "" {
			delete(result.spec.Env, pi.EnvMCPConfig)
		} else {
			result.spec.Env[pi.EnvMCPConfig] = files.MCPConfigPath
		}
	} else {
		s.mu.Lock()
		result.residence = s.residence
		s.mu.Unlock()

		result.spec.ExtensionPaths = slices.Clone(previous.ExtensionPaths)
		result.spec.SkillPaths = slices.Clone(previous.SkillPaths)
		result.spec.PromptTemplatePaths = slices.Clone(previous.PromptTemplatePaths)
	}

	if len(entries) > 0 {
		result.spec.SessionPath, err = writeHydratedSessionFile(dirs, string(s.id), entries)
		if err != nil {
			return fail(err)
		}

		result.spec.SessionID = ""
	}

	if err := s.agent.prepareNativeTree(context.Background(), dirs.Root); err != nil {
		return result, err
	}

	result.prepared = s.agent.options.hostAuthoritySupplied

	return result, nil
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
	attempt.mu.Unlock()

	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancelCleanup()

	killErr := attempt.nativeBoundary.run(cleanupCtx, "kill", relaunched.Kill)

	closeErr := attempt.nativeBoundary.run(cleanupCtx, "close", relaunched.Close)

	cleanupErr := terminalNativeClose(killErr, closeErr)

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
// tree is closed and reaches its native boundary before either
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
		return errors.Join(ErrContainmentIncomplete, errors.New("pi session has no native boundary owner"))
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
		err = errors.Join(ErrContainmentIncomplete, errors.New("active pi turn has no native generation"))
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
// and timeout. Managed generations then reach their terminal process boundary
// and return their tree before the settlement reads the native transcript.
func (s *agentSession) claimTurnSettlement(ctx context.Context) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	if s.turnFenceStarted {
		err := s.turnFenceErr
		s.mu.Unlock()

		return err
	}

	s.turnSettling = true
	outbox := s.outbox
	s.mu.Unlock()

	return s.retireManagedGeneration(ctx, outbox)
}

func (s *agentSession) retireManagedGeneration(ctx context.Context, outbox *sessionOutbox) error {
	if s.agent == nil || !s.agent.options.hostAuthoritySupplied {
		return nil
	}

	s.mu.Lock()
	s.managedCommitPending = true
	s.mu.Unlock()

	if outbox == nil {
		return errors.Join(ErrContainmentIncomplete, errors.New("managed pi generation owner is missing"))
	}

	outbox.mu.Lock()
	containment, owner := outbox.claimContainmentLocked(containmentOwnerTurn)
	outbox.mu.Unlock()

	var containmentErr error

	if owner {
		containCtx, cancelContain := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
		containmentErr = runNativeBoundaryStep(containCtx, "settled generation retirement", func() error {
			return s.stopSettledManagedGeneration(containCtx, outbox)
		})

		cancelContain()
		outbox.finishContainment(containment, containmentErr)
	} else {
		containmentErr, _ = outbox.awaitContainment()
	}

	if !nativeContainmentComplete(containmentErr) {
		s.recordNativeContainment(containmentErr)

		return containmentErr
	}

	return s.reclaimManagedGeneration(context.WithoutCancel(ctx), outbox)
}

func (s *agentSession) stopSettledManagedGeneration(ctx context.Context, outbox *sessionOutbox) error {
	if outbox.proc == nil || outbox.nativeBoundary == nil {
		return errors.Join(ErrContainmentIncomplete, errors.New("managed pi generation has no process boundary"))
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	shutdownErr := outbox.nativeBoundary.run(shutdownCtx, "settled shutdown", func() error {
		return outbox.proc.Shutdown(shutdownCtx)
	})

	cancelShutdown()

	if outbox.pumpCancel != nil {
		outbox.pumpCancel()
	}

	closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	closeErr := outbox.nativeBoundary.run(closeCtx, "settled close", outbox.proc.Close)

	cancelClose()

	var pumpErr error

	if outbox.pumpDone != nil {
		select {
		case <-outbox.pumpDone:
		case <-ctx.Done():
			pumpErr = fmt.Errorf("%w: wait for settled native generation pump: %v", ErrContainmentIncomplete, ctx.Err())
		}
	}

	if closeErr == nil {
		shutdownErr = nil
	}

	return errors.Join(terminalNativeClose(shutdownErr, closeErr), pumpErr)
}

func (s *agentSession) reclaimManagedGeneration(ctx context.Context, outbox *sessionOutbox) error {
	s.mu.Lock()
	root := s.launch.NativeRoot
	prepared := s.generationPrepared
	current := outbox == nil || s.outbox == outbox
	s.mu.Unlock()

	if !current {
		return errors.Join(ErrContainmentIncomplete, errors.New("managed pi generation changed before reclaim"))
	}

	if root == "" || !prepared {
		return nil
	}

	if err := s.agent.reclaimNativeTree(ctx, root); err != nil {
		if errors.Is(err, ErrNativeTreeBusy) {
			s.agent.markNativeTreeBusy(root)
		}

		return err
	}

	s.agent.clearNativeTreeBusy(root)

	s.mu.Lock()
	if s.launch.NativeRoot != root || (outbox != nil && s.outbox != outbox) {
		s.mu.Unlock()

		return errors.Join(ErrContainmentIncomplete, errors.New("managed pi generation changed during reclaim"))
	}

	s.generationPrepared = false
	s.mu.Unlock()

	return nil
}

func (s *agentSession) retainGeneration(root string, prepared bool, cleanupErr error) {
	if root == "" {
		return
	}

	s.mu.Lock()
	if s.retainedRoot == "" {
		s.retainedRoot = root

		s.retainedPrepared = prepared
		if !nativeContainmentComplete(cleanupErr) && !errors.Is(cleanupErr, ErrNativeTreeBusy) {
			s.retainedErr = cleanupErr
		}
	}
	s.mu.Unlock()
}

func (s *agentSession) cleanupGenerationRoot(ctx context.Context, root string, prepared bool) (bool, error) {
	if root == "" {
		return false, nil
	}

	if prepared {
		if err := s.agent.reclaimNativeTree(ctx, root); err != nil {
			return true, err
		}
	}

	return false, s.agent.removeNativeTree(root)
}

func (s *agentSession) releaseRetainedGeneration(ctx context.Context) error {
	s.mu.Lock()
	root := s.retainedRoot
	prepared := s.retainedPrepared
	retainedErr := s.retainedErr
	s.mu.Unlock()

	if root == "" {
		return nil
	}

	if retainedErr != nil {
		return retainedErr
	}

	if prepared {
		if err := s.agent.reclaimNativeTree(ctx, root); err != nil {
			if errors.Is(err, ErrNativeTreeBusy) {
				s.agent.markNativeTreeBusy(root)
			}

			return err
		}

		s.agent.clearNativeTreeBusy(root)

		s.mu.Lock()
		if s.retainedRoot == root {
			s.retainedPrepared = false
		}
		s.mu.Unlock()
	}

	if err := s.agent.removeNativeTree(root); err != nil {
		return err
	}

	s.mu.Lock()
	if s.retainedRoot == root {
		s.retainedRoot = ""
		s.retainedPrepared = false
		s.retainedErr = nil
	}
	s.mu.Unlock()

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
		return errors.Join(ErrContainmentIncomplete, errors.New("active pi turn has no contained process root"))
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

	controlCtx, cancelControl := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	killErr := outbox.nativeBoundary.run(controlCtx, "kill", outbox.proc.Kill)
	closeErr := outbox.nativeBoundary.run(controlCtx, "close", outbox.proc.Close)

	cancelControl()

	var pumpErr error

	if outbox.pumpDone != nil {
		select {
		case <-outbox.pumpDone:
		case <-ctx.Done():
			pumpErr = fmt.Errorf("%w: wait for native generation pump: %v", ErrContainmentIncomplete, ctx.Err())
		}
	}

	containmentErr := errors.Join(terminalNativeClose(killErr, closeErr), pumpErr)
	if !nativeContainmentComplete(abortErr) && !nativeContainmentComplete(closeErr) {
		containmentErr = errors.Join(containmentErr, abortErr)
	}

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

	if !nativeContainmentComplete(err) {
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
	if !nativeContainmentComplete(err) {
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
			if !nativeContainmentComplete(err) {
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
		err = errors.Join(err, ErrContainmentIncomplete,
			fmt.Errorf("join session operation holder: %w", waitErr))
	} else {
		releaseTurn()
	}

	proc := s.process()
	if attempt.outbox != nil {
		proc = attempt.outbox.proc
	}

	err = errors.Join(err, s.nativeContainmentError())
	if !nativeContainmentComplete(err) {
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

	if reclaimErr := s.reclaimManagedGeneration(closeCtx, attempt.outbox); reclaimErr != nil {
		err = errors.Join(err, reclaimErr)
		if !errors.Is(reclaimErr, ErrNativeTreeBusy) {
			s.recordNativeContainment(reclaimErr)
		}

		return err
	}

	if releaseErr := s.releaseRetainedGeneration(closeCtx); releaseErr != nil {
		err = errors.Join(err, releaseErr)
		if !errors.Is(releaseErr, ErrNativeTreeBusy) {
			s.recordNativeContainment(releaseErr)
		}

		return err
	}

	err = errors.Join(err, s.settleCloseBoundary(closeCtx, proc, err))
	s.closeLifecycleSession()

	if err != nil {
		if !nativeContainmentComplete(err) {
			s.recordNativeContainment(err)
		}

		return err
	}

	// Every admission is taken out of the session before it is finalized, so a
	// coincident turn fence releasing the same native root cannot release it
	// twice, and an incomplete containment boundary retains it by leaving it
	// unreleased.
	s.mu.Lock()
	sessionRoot := s.sessionRoot
	generationRoot := s.launch.NativeRoot
	generationPrepared := s.generationPrepared
	browserShim := s.browserShim
	residence := s.residence
	s.mu.Unlock()

	err = finalizeSessionNativeResources(s.agent, err, generationRoot, generationPrepared, sessionRoot, browserShim, residence)

	if s.agent != nil {
		s.agent.observe.RecordPiProcessExit(closeCtx, "closed", err)
	}

	return err
}

func (s *agentSession) stopHostGenerationWithoutOutbox(ctx context.Context) error {
	s.mu.Lock()
	proc := s.proc

	boundary := s.nativeBoundary
	s.mu.Unlock()

	if proc == nil {
		return nil
	}

	if boundary == nil {
		return errors.Join(ErrContainmentIncomplete, errors.New("pi session has no native boundary owner"))
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	shutdownErr := boundary.run(shutdownCtx, "shutdown", func() error {
		return proc.Shutdown(shutdownCtx)
	})

	cancelShutdown()

	s.stopPump()

	closeErr := boundary.run(ctx, "close", proc.Close)

	return terminalNativeClose(shutdownErr, closeErr)
}

func (s *agentSession) recordNativeContainment(err error) {
	if nativeContainmentComplete(err) {
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
