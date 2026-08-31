package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

type signalErrContext struct {
	context.Context //nolint:containedctx // The test wrapper mutates ownership when Err is inspected.
	entered         chan struct{}
	once            sync.Once
	mutate          func()
}

type relaunchCatalogEndingClient struct {
	*directAgentClient
	session *agentSession
}

type appendCallbackStore struct {
	SessionStore
	afterAppend func()
}

func (s *appendCallbackStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	if err := s.SessionStore.Append(ctx, key, entries); err != nil {
		return err
	}
	if s.afterAppend != nil {
		s.afterAppend()
	}

	return nil
}

func (c *relaunchCatalogEndingClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if notification.Update.AvailableCommandsUpdate != nil {
		c.session.mu.Lock()
		outbox := c.session.outbox
		c.session.mu.Unlock()
		outbox.end()
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

func (c *signalErrContext) Err() error {
	c.once.Do(func() {
		if c.mutate != nil {
			c.mutate()
		}
		close(c.entered)
	})

	return nil
}

// TestCloseBoundWithARealLeaseCommitsNothing proves a timed-out operation join
// is containment-incomplete. Native containment may finish, but the holder can
// still touch session state, so close publishes no terminal boundary and keeps
// every cleanup/resource owner addressable.
func TestCloseBoundWithARealLeaseCommitsNothing(t *testing.T) {
	originalWaitContext := sessionCloseTurnWaitContext
	waiting := make(chan context.CancelFunc, 1)
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		waitCtx, cancel := context.WithCancel(ctx)
		waiting <- cancel

		return waitCtx, cancel
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWaitContext })

	session, client := lifecycleSession(t, true)
	process := newStubProcess(false)
	root := t.TempDir()
	session.proc = process
	attachTestNativeBoundary(session)
	session.sessionRoot = root
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
	wantTurn := session.lc.turnID
	baseline := len(client.notifications)

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	t.Cleanup(release)

	done := make(chan error, 1)
	go func() { done <- session.Close(context.Background()) }()
	<-waiting // the pre-native interaction join completed without waiting
	cancelWait := <-waiting

	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
	require.DirExists(t, root)
	require.False(t, session.lc.closed)
	require.Equal(t, wantTurn, session.lc.turnID)
	require.Len(t, client.notifications, baseline)

	cancelWait()
	closeErr := <-done
	require.ErrorIs(t, closeErr, ErrContainmentIncomplete)
	require.DirExists(t, root)

	entries, loadErr := session.agent.sessionStore().Load(t.Context(), SessionKey{
		SessionID: string(session.id),
		Subpath:   SessionStoreLifecycleSubpath,
	})
	require.NoError(t, loadErr)
	require.Empty(t, entries, "an unjoined holder gained a durable close boundary")
}

func TestCloseWinningDispatchFenceWritesNoPrompt(t *testing.T) {
	client := newStubPiClient()
	writes := 0
	client.promptFunc = func(context.Context, string) error {
		writes++

		return nil
	}
	process := newStubProcess(false)
	session := &agentSession{agent: NewAgent(), id: "dispatch-fence", proc: process, client: client}
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, client, nil, nil, nil)
	delivery := newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	session.outbox = outbox
	session.turnEvents = delivery

	_, owner := session.beginClose()
	require.True(t, owner)
	err := client.PromptWithBoundary(t.Context(), "must-not-write", nil, pi.CallBoundary{
		BeforeDispatch: func() (func(), error) { return session.beginPromptDispatch(t.Context(), outbox, delivery) },
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown session")
	require.NotContains(t, err.Error(), limitSessionPrompt)
	require.Zero(t, writes)
}

func TestTimedOutShutdownRemainsTrackedAndAllowsMandatoryClose(t *testing.T) {
	tracker := &nativeBoundaryTracker{}
	entered := make(chan struct{})
	release := make(chan struct{})
	closeCalls := 0
	ctx, cancel := context.WithCancel(context.Background())

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- tracker.run(ctx, "shutdown", func() error {
			close(entered)
			<-release

			return nil
		})
	}()
	<-entered
	cancel()
	require.ErrorIs(t, <-shutdownDone, ErrContainmentIncomplete)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- tracker.run(t.Context(), "close", func() error {
			closeCalls++

			return nil
		})
	}()
	require.NoError(t, <-closeDone)
	require.Equal(t, 1, closeCalls)
	require.ErrorIs(t, tracker.retainedIncomplete(), ErrContainmentIncomplete,
		"later mandatory containment erased the timed-out phase owner")
	close(release)
}

func TestNativeBoundaryCancellationPrecheckAndTimeoutPrecedence(t *testing.T) {
	t.Run("already cancelled never starts", func(t *testing.T) {
		tracker := newNativeBoundaryTracker()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		started := false

		err := tracker.run(ctx, "shutdown", func() error {
			started = true

			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		require.False(t, started)
	})

	t.Run("deadline beats simultaneous late success", func(t *testing.T) {
		tracker := newNativeBoundaryTracker()
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		release := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- tracker.run(ctx, "shutdown", func() error {
				close(entered)
				<-release

				return nil
			})
		}()
		<-entered
		cancel()
		close(release)

		err := <-result
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		require.ErrorIs(t, tracker.retainedIncomplete(), ErrContainmentIncomplete)
	})
}

func TestExactGenerationSharesOneNativeBoundaryTracker(t *testing.T) {
	tracker := newNativeBoundaryTracker()
	agent := NewAgent()
	construction := &nativeConstruction{done: make(chan struct{}), nativeBoundary: tracker}
	agent.constructions[construction] = struct{}{}
	attempt := &sessionRelaunchAttempt{done: make(chan struct{})}

	require.True(t, agent.transferConstructionToRelaunch(construction, attempt))
	require.Same(t, tracker, attempt.nativeBoundary)

	session := &agentSession{agent: agent, nativeBoundary: attempt.nativeBoundary}
	outbox := newSessionOutbox(7, tracker)
	require.NoError(t, outbox.bindRuntime(nil, nil, nil, nil, session.nativeBoundary))
	require.Same(t, tracker, session.nativeBoundary)
	require.Same(t, tracker, outbox.nativeBoundary)
}

func TestMissingNativeBoundaryFailsClosedWithoutNativeMutation(t *testing.T) {
	outbox := newSessionOutbox(1, nil)
	require.ErrorIs(t, outbox.bindRuntime(nil, nil, nil, nil, nil), ErrContainmentIncomplete)

	process := newStubProcess(false)
	session := &agentSession{agent: NewAgent(), proc: process}
	require.ErrorIs(t, session.Close(t.Context()), ErrContainmentIncomplete)
	require.Zero(t, process.shutdownCalls)
	require.Zero(t, process.killCalls)
	require.Zero(t, process.closeCalls)
}

func TestGenerationProducerRootClosesAdmissionBeforeFinalWait(t *testing.T) {
	producers := newGenerationProducers()
	releaseChild, admitted := producers.acquire(1)
	require.True(t, admitted)
	producers.releaseRoot()

	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	require.ErrorIs(t, producers.wait(waitCtx), ErrContainmentIncomplete)

	releaseChild()
	require.NoError(t, producers.wait(t.Context()))
	_, admitted = producers.acquire(1)
	require.False(t, admitted, "a producer was admitted after the final wait became safe")
}

func TestLifecycleAdmissionAndMissingOwnerEdges(t *testing.T) {
	t.Run("close wins during turn admission", func(t *testing.T) {
		session := &agentSession{agent: NewAgent()}
		ctx := &signalErrContext{Context: t.Context(), entered: make(chan struct{}), mutate: func() {
			session.closing = true
		}}
		_, err := session.acquireTurn(ctx)
		requireInvalidParams(t, err)
	})

	closing := &agentSession{closing: true}
	_, err := closing.beginRelaunch()
	requireInvalidParams(t, err)
	require.ErrorIs(t, (&agentSession{}).refreshMCPTools(t.Context()), ErrContainmentIncomplete)
	require.Nil(t, func() *generationContainment {
		containment, owner := (&agentSession{}).claimTurnContainment(nil)
		require.False(t, owner)

		return containment
	}())
	require.ErrorIs(t, (&agentSession{}).stopTurnGeneration(t.Context(), nil), ErrContainmentIncomplete)
	require.NoError(t, (&agentSession{nativeBoundary: newNativeBoundaryTracker()}).cancelNativeLocked(t.Context(), false))
}

func TestRepeatedInteractionSettlementJoinsOneMemoizedTracker(t *testing.T) {
	session := &agentSession{agent: NewAgent()}
	outbox := newTestSessionOutbox(1)
	session.outbox = outbox
	release, admitted := outbox.interactions.acquire(1)
	require.True(t, admitted)
	wantDone := outbox.interactions.done

	bounded, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, session.settleBoundaryInteractions(bounded, outbox), ErrContainmentIncomplete)
	require.Equal(t, wantDone, outbox.interactions.done)
	require.ErrorIs(t, session.settleBoundaryInteractions(bounded, outbox), ErrContainmentIncomplete)
	require.Equal(t, wantDone, outbox.interactions.done)
	_, admitted = session.admitDialogHandler(outbox)
	require.False(t, admitted, "retry reopened interaction admission")

	release()
	require.NoError(t, session.settleBoundaryInteractions(t.Context(), outbox))
}

func TestCancelRetainsTimedOutInteractionOwnerAfterNativeContainment(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return bounded, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	for _, activeTurn := range []bool{true, false} {
		t.Run(fmt.Sprintf("active=%t", activeTurn), func(t *testing.T) {
			process := newStubProcess(false)
			native := newStubPiClient()
			outbox := newTestSessionOutbox(1)
			bindTestRuntime(outbox, process, native, nil, nil, nil)
			session := &agentSession{
				agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
				proc:  process, client: native, outbox: outbox, nativeBoundary: outbox.nativeBoundary,
			}
			if activeTurn {
				session.cancel = func() {}
				session.turnFenceDone = make(chan struct{})
			}
			release, admitted := outbox.interactions.acquire(1)
			require.True(t, admitted)

			err := session.cancelNativeLocked(t.Context(), false)
			require.ErrorIs(t, err, ErrContainmentIncomplete)
			require.Equal(t, 1, process.closeCalls)
			require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)

			release()
			require.NoError(t, outbox.interactions.wait(t.Context()))
		})
	}
}

func TestTurnContainmentAndCloseFailureEdges(t *testing.T) {
	t.Run("active turn without generation", func(t *testing.T) {
		cancelled := false
		session := &agentSession{cancel: func() { cancelled = true }, turnFenceDone: make(chan struct{})}
		err := session.fenceActiveTurnLocked(t.Context())
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		require.True(t, cancelled)
	})

	t.Run("pump join bound", func(t *testing.T) {
		process := newStubProcess(false)
		outbox := newTestSessionOutbox(1)
		bindTestRuntime(outbox, process, newStubPiClient(), nil, make(chan struct{}), nil)
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, (&agentSession{}).stopTurnGeneration(cancelled, outbox), ErrContainmentIncomplete)
		require.Zero(t, process.killCalls)
	})

	t.Run("close panic finishes native boundary", func(t *testing.T) {
		process := newStubProcess(false)
		outbox := newTestSessionOutbox(1)
		bindTestRuntime(outbox, process, newStubPiClient(), nil, nil, nil)
		session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler))), outbox: outbox, cancel: func() { panic("turn cancel") }}
		attempt, owner := session.beginClose()
		require.True(t, owner)
		require.True(t, attempt.containmentOwner)
		require.ErrorIs(t, session.closeOwned(t.Context(), attempt), ErrContainmentIncomplete)
		containmentErr, ok := outbox.awaitContainment()
		require.True(t, ok)
		require.ErrorIs(t, containmentErr, ErrContainmentIncomplete)
	})

	t.Run("prior poison containment remains immutable", func(t *testing.T) {
		process := newStubProcess(false)
		current := newTestSessionOutbox(2)
		bindTestRuntime(current, process, newStubPiClient(), nil, nil, nil)
		old := newTestSessionOutbox(1)
		old.mu.Lock()
		containment, _ := old.claimContainmentLocked(containmentOwnerTurn)
		old.mu.Unlock()
		old.finishContainment(containment, ErrContainmentIncomplete)
		session := &agentSession{agent: NewAgent(), outbox: current, containmentOutboxes: []*sessionOutbox{old}}
		session.lc.generation = current.generation
		err := session.Close(t.Context())
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		require.ErrorIs(t, session.lifecycleGenerationQuarantine(current.generation), ErrContainmentIncomplete)
	})

	t.Run("producer join remains incomplete after native close", func(t *testing.T) {
		originalWait := sessionCloseTurnWaitContext
		sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
			bounded, cancel := context.WithCancel(ctx)
			cancel()

			return bounded, func() {}
		}
		t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

		process := newStubProcess(false)
		outbox := newTestSessionOutbox(1)
		pumpDone := make(chan struct{})
		close(pumpDone)
		bindTestRuntime(outbox, process, newStubPiClient(), nil, pumpDone, nil)
		release, admitted := outbox.producers.acquire(1)
		require.True(t, admitted)
		outbox.producers.releaseRoot()
		session := &agentSession{agent: NewAgent(), outbox: outbox}
		err := session.Close(t.Context())
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		require.Equal(t, 1, process.closeCalls)
		release()
	})
}

func TestQuarantinedSessionCloseCannotPublishOrReleaseAfterContainment(t *testing.T) {
	session, client := lifecycleSession(t, false)
	process := newStubProcess(false)
	native := newStubPiClient()
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, native, nil, nil, nil)
	session.proc = process
	session.client = native
	session.outbox = outbox
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	baseline := len(client.notifications)

	attempt, owner := session.beginClose()
	require.True(t, owner)
	want := errors.Join(ErrContainmentIncomplete, errors.New("waiter quarantine"))
	require.True(t, session.quarantineCloseAttempt(want))
	require.ErrorIs(t, session.closeOwned(t.Context(), attempt), ErrContainmentIncomplete)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
	require.False(t, session.lc.closed)
	require.Len(t, client.notifications, baseline)
	require.ErrorIs(t, session.awaitClose(attempt), want)
}

func TestCloseWaiterJoinsOwnerThatAlreadyLinearizedFinalSettlement(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	entered := make(chan struct{})
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()
		close(entered)

		return bounded, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	session := &agentSession{}
	attempt, owner := session.beginClose()
	require.True(t, owner)
	require.True(t, attempt.beginFinalSettlement())
	done := make(chan error, 1)
	go func() { done <- session.awaitClose(attempt) }()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("waiter read the result before final publication: %v", err)
	default:
	}

	want := errors.New("final result")
	session.finishClose(attempt, want)
	require.ErrorIs(t, <-done, want)
}

func TestIncompleteActionJoinDoesNotTouchLifecycle(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return bounded, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	session, _ := lifecycleSession(t, false)
	process := newStubProcess(false)
	native := newStubPiClient()
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, native, nil, nil, nil)
	session.proc = process
	session.client = native
	session.outbox = outbox
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
	wantTurn, wantCycle := session.lc.turnID, session.lc.cycleID
	releaseAction, admitted := outbox.interactions.acquire(1)
	require.True(t, admitted)

	err := session.Close(t.Context())
	releaseAction()
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.False(t, session.lc.closed)
	require.Equal(t, wantTurn, session.lc.turnID)
	require.Equal(t, wantCycle, session.lc.cycleID)
}

// TestSessionCloseIsTerminalAndIdempotent proves a teardown that completes has
// exactly one owner: the session admits no prompt, MCP refresh, or relaunch
// from the moment the first caller claims the ladder, and a second caller
// reports the first caller's result without running a rung of its own. The
// owner's boundary completes here, so the claim it recorded is never released
// and the second caller is a no-op whether it arrives while the ladder runs or
// after it finished.
func TestSessionCloseIsTerminalAndIdempotent(t *testing.T) {
	spawns := 0
	agent := newStubClientAgent(t, newStubPiClient())
	realStart := agent.startPiProcess
	agent.startPiProcess = func(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return realStart(ctx, spec)
	}

	tearingDown := make(chan struct{})
	release := make(chan struct{})
	process := newStubProcess(false)
	process.shutdownFunc = func(context.Context) error {
		close(tearingDown)
		<-release

		return nil
	}

	session := &agentSession{agent: agent, id: "id", proc: process, sessionRoot: t.TempDir()}
	prepareRelaunchFixture(t, session)

	first := make(chan error, 1)
	go func() { first <- session.Close(context.Background()) }()

	<-tearingDown

	admissionEntered := make(chan struct{})
	admission := make(chan error, 1)
	go func() {
		close(admissionEntered)
		_, err := session.acquireTurn(context.Background())
		admission <- err
	}()
	<-admissionEntered
	select {
	case err := <-admission:
		t.Fatalf("admission returned before the close owner completed: %v", err)
	default:
	}

	requireInvalidParams(t, session.refreshMCPTools(t.Context()))
	requireInvalidParams(t, session.ensureProcessAlive(t.Context()))
	requireInvalidParams(t, session.relaunchProcess(t.Context()))

	second := make(chan error, 1)
	go func() { second <- session.Close(context.Background()) }()

	close(release)

	require.NoError(t, <-first)
	require.NoError(t, <-second, "the second caller reported a result the owner never produced")
	requireInvalidParams(t, <-admission)
	require.Equal(t, 1, process.shutdownCalls, "a second caller ran the shutdown ladder beside the owner")
	require.Equal(t, 1, process.closeCalls, "a second caller ran the containment boundary beside the owner")
	require.Zero(t, spawns, "a closing session spawned a pi process")
}

func TestHostCloseNativePanicsPublishIncompleteContainment(t *testing.T) {
	const secret = "host-close-native-panic-secret-sentinel"

	for _, stage := range []string{"shutdown", "close"} {
		t.Run(stage, func(t *testing.T) {
			logs := &strings.Builder{}
			process := newStubProcess(false)
			switch stage {
			case "shutdown":
				process.shutdownFunc = func(context.Context) error { panic(secret) }
			case "close":
				process.closeFunc = func() error { panic(secret) }
			}

			session := &agentSession{
				agent:       NewAgent(WithLogger(slog.New(slog.NewTextHandler(logs, nil)))),
				id:          "id",
				proc:        process,
				sessionRoot: t.TempDir(),
			}

			err := session.Close(t.Context())
			require.ErrorIs(t, err, ErrContainmentIncomplete)
			require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)
			require.NotContains(t, logs.String(), secret)

			replayed, owner := session.beginClose()
			require.False(t, owner)
			require.ErrorIs(t, session.awaitClose(replayed), ErrContainmentIncomplete)
		})
	}
}

func TestCloseClaimPublishesOneImmutableResult(t *testing.T) {
	session := &agentSession{id: "id"}

	attempt, owner := session.beginClose()
	require.True(t, owner, "the first caller did not own the teardown it opened")

	shared, owner := session.beginClose()
	require.False(t, owner, "a caller arriving mid-teardown opened a second ladder")
	require.Same(t, attempt, shared)

	session.mu.Lock()
	require.True(t, session.closing, "the claim left admission unfenced")
	session.mu.Unlock()

	refused := errors.New("boundary refused")

	reported := make(chan error, 1)
	go func() { reported <- session.awaitClose(shared) }()

	session.finishClose(attempt, refused)
	require.ErrorIs(t, <-reported, refused, "the waiting caller did not report the owner's result")

	replayed, owner := session.beginClose()
	require.False(t, owner, "a failed immutable teardown opened a second ladder")
	require.Same(t, attempt, replayed)
	require.ErrorIs(t, session.awaitClose(replayed), refused)

	session.finishClose(attempt, nil)
	require.ErrorIs(t, session.awaitClose(replayed), refused, "a second publication changed the teardown result")
}

// TestClosedSessionOwnsNoSurvivingProcess proves the post-close relaunch door
// is shut: the closed process reached its containment boundary exactly once and
// no path spawns a replacement the session no longer tracks.
func TestClosedSessionOwnsNoSurvivingProcess(t *testing.T) {
	spawns := 0
	agent := newStubClientAgent(t, newStubPiClient())
	realStart := agent.startPiProcess
	agent.startPiProcess = func(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return realStart(ctx, spec)
	}

	process := newStubProcess(true)
	session := &agentSession{agent: agent, id: "id", proc: process, sessionRoot: t.TempDir()}
	prepareRelaunchFixture(t, session)

	require.NoError(t, session.Close(t.Context()))
	require.Equal(t, 1, process.closeCalls)

	_, err := session.acquireTurn(t.Context())
	requireInvalidParams(t, err)
	requireInvalidParams(t, session.ensureProcessAlive(t.Context()))
	requireInvalidParams(t, session.refreshMCPTools(t.Context()))
	requireInvalidParams(t, session.relaunchProcess(t.Context()))
	require.Zero(t, spawns)
	require.Equal(t, 1, process.closeCalls)
}

// TestRefreshMCPToolsRefusesACloseClaimedMidCheck proves the refresh guard sits
// inside the critical section that mutates the session rather than in front of
// it: a close claimed after the liveness check still stops the refresh from
// shutting the native process down under teardown.
func TestRefreshMCPToolsRefusesACloseClaimedMidCheck(t *testing.T) {
	process := newStubProcess(false)
	session := &agentSession{
		agent:             NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
		id:                "id",
		proc:              process,
		mcpRefreshPending: true,
	}
	attachTestNativeBoundary(session)
	process.onExited = func() {
		session.mu.Lock()
		session.closing = true
		session.mu.Unlock()
	}

	requireInvalidParams(t, session.refreshMCPTools(t.Context()))
	require.Zero(t, process.shutdownCalls)
	require.True(t, session.mcpRefreshPending)
}

// TestRelaunchRefusesToAdoptAfterCloseBegins proves the two mid-relaunch
// windows are closed inside the critical sections that publish them: a native
// root admitted after close begins is handed straight back, and a process
// spawned before close claimed the session is contained here rather than
// surviving as a pi process nothing owns.
func TestRelaunchRefusesToAdoptAfterCloseBegins(t *testing.T) {
	var session *agentSession
	agent := newStubClientAgent(t, newStubPiClient())
	relaunched := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		session.mu.Lock()
		session.closing = true
		session.mu.Unlock()

		return relaunched, newStubPiClient(), nil
	}
	session = &agentSession{agent: agent, id: "id"}
	prepareRelaunchFixture(t, session)
	requireInvalidParams(t, session.relaunchProcess(t.Context()))
	require.Equal(t, 1, relaunched.killCalls)
	require.Equal(t, 1, relaunched.closeCalls)
	session.mu.Lock()
	defer session.mu.Unlock()
	require.Nil(t, session.proc)
}
func TestCloseAndRelaunchElectOneExactGeneration(t *testing.T) {
	t.Run("close wins before atomic publication", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		process := newStubProcess(false)
		client := newStubPiClient()
		started := make(chan struct{})
		releaseStart := make(chan struct{})
		client.startFunc = func(context.Context) error {
			close(started)
			<-releaseStart

			return nil
		}
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return process, client, nil
		}

		session := &agentSession{agent: agent, id: "id"}
		prepareRelaunchFixture(t, session)

		relaunchDone := make(chan error, 1)
		go func() { relaunchDone <- session.relaunchProcess(context.Background()) }()
		<-started

		attempt, owner := session.beginClose()
		require.True(t, owner)
		require.Nil(t, attempt.outbox)
		require.NotNil(t, attempt.relaunch)
		closeDone := make(chan error, 1)
		go func() { closeDone <- session.closeOwned(context.Background(), attempt) }()

		close(releaseStart)
		requireInvalidParams(t, <-relaunchDone)
		require.NoError(t, <-closeDone)
		require.Equal(t, 1, process.killCalls)
		require.Equal(t, 1, process.closeCalls)
		session.mu.Lock()
		require.Nil(t, session.proc)
		require.Nil(t, session.outbox)
		session.mu.Unlock()
	})

	t.Run("relaunch publishes the whole generation before close", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		process := newStubProcess(false)
		client := newStubPiClient()
		published := make(chan struct{})
		releaseConfig := make(chan struct{})
		client.state = pi.SessionState{SessionID: "id"}
		client.autoRetryFunc = func(context.Context, bool) error {
			close(published)
			<-releaseConfig

			return pi.ErrTransportClosed
		}
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return process, client, nil
		}

		session := &agentSession{agent: agent, id: "id"}
		prepareRelaunchFixture(t, session)

		relaunchDone := make(chan error, 1)
		go func() { relaunchDone <- session.relaunchProcess(context.Background()) }()
		<-published

		attempt, owner := session.beginClose()
		require.True(t, owner)
		require.NotNil(t, attempt.outbox)
		require.Same(t, process, attempt.outbox.proc)
		require.Same(t, client, attempt.outbox.client)
		closeDone := make(chan error, 1)
		go func() { closeDone <- session.closeOwned(context.Background(), attempt) }()

		close(releaseConfig)
		require.ErrorIs(t, <-relaunchDone, pi.ErrTransportClosed)
		require.NoError(t, <-closeDone)
		require.Equal(t, 1, process.shutdownCalls)
		require.Equal(t, 1, process.closeCalls)
	})
}

func TestAwaitRelaunchTimeoutMemoizesWithoutBoundaryOrCleanup(t *testing.T) {
	originalWaitContext := sessionRelaunchWaitContext
	waiting := make(chan context.CancelFunc, 1)
	sessionRelaunchWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		waitCtx, cancel := context.WithCancel(ctx)
		waiting <- cancel

		return waitCtx, cancel
	}
	t.Cleanup(func() { sessionRelaunchWaitContext = originalWaitContext })

	process := newStubProcess(false)
	root := t.TempDir()
	attempt := &sessionRelaunchAttempt{
		done:           make(chan struct{}),
		proc:           process,
		generationRoot: root,
	}
	session := &agentSession{agent: NewAgent(testContainmentOption()), id: "id", relaunchAttempt: attempt}

	closed := make(chan error, 1)
	go func() { closed <- session.Close(context.Background()) }()
	cancelWait := <-waiting
	cancelWait()
	closeErr := <-closed
	require.ErrorIs(t, closeErr, ErrContainmentIncomplete)
	require.Zero(t, process.killCalls)
	require.Zero(t, process.shutdownCalls)
	require.Zero(t, process.closeCalls)
	require.DirExists(t, root)

	session.finishRelaunch(attempt, nil)
	require.ErrorIs(t, session.Close(t.Context()), ErrContainmentIncomplete)
}

func TestSessionRetainsIncompleteNativeContainment(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent}
	session.recordNativeContainment(errors.New("ordinary native error"))
	require.NoError(t, session.nativeContainmentError())
	require.NoError(t, agent.nativeContainmentError())

	session.recordNativeContainment(ErrContainmentIncomplete)
	require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)
	require.ErrorIs(t, agent.nativeContainmentError(), ErrContainmentIncomplete)
}

func TestSessionTurnLifecycleBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}
	attachTestNativeBoundary(session)

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	_, err = session.acquireTurn(t.Context())
	requireInvalidRequest(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = session.acquireTurn(ctx)
	require.ErrorIs(t, err, context.Canceled)
	release()

	require.NoError(t, session.ensureProcessAlive(t.Context()))
	noProcess := &agentSession{}
	require.Error(t, noProcess.ensureProcessAlive(t.Context()))

	require.NoError(t, session.Cancel(t.Context()))
	client.abortErr = errors.New("abort")
	require.Error(t, session.Cancel(t.Context()))
	noClient := &agentSession{}
	require.ErrorIs(t, noClient.Cancel(t.Context()), ErrContainmentIncomplete)

	cancelled := 0
	session.cancel = func() { cancelled++ }
	dialogCtx, cancelDialog := context.WithCancelCause(context.Background())
	session.pendingDialogs = map[string]*dialogCancel{"dialog": {cancel: cancelDialog}}
	session.cancelPendingInteractions()
	require.True(t, session.wasTurnCancelled())
	require.Empty(t, session.pendingDialogs)
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)

	process := newStubProcess(false)
	process.shutdown = errors.New("shutdown")
	process.close = errors.New("close")
	closing := &agentSession{agent: agent, proc: process, cancel: func() { cancelled++ }, sessionRoot: t.TempDir()}
	attachTestNativeBoundary(closing)
	require.Error(t, closing.Close(t.Context()))
	require.Equal(t, 1, cancelled)
}

func TestSessionCancelEscalatesUnacknowledgedAbort(t *testing.T) {
	originalGrace := sessionCancelAbortGrace
	sessionCancelAbortGrace = 10 * time.Millisecond
	t.Cleanup(func() { sessionCancelAbortGrace = originalGrace })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	abortEntered := make(chan struct{})
	releaseAbort := make(chan struct{})
	abortReturned := make(chan struct{})
	client.abortFunc = func(context.Context) error {
		close(abortEntered)
		<-releaseAbort
		close(abortReturned)

		return nil
	}

	process := newStubProcess(false)
	turnCtx, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:  agent,
		client: client,
		proc:   process,
		cancel: turnCancel,
	}
	bindTestOutbox(session)

	require.ErrorIs(t, session.Cancel(t.Context()), ErrContainmentIncomplete)
	<-abortEntered
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.Equal(t, 1, process.killCalls, "a timed-out abort must escalate to kill")
	require.Equal(t, 1, process.closeCalls, "a timed-out abort must still reach close")
	require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)
	close(releaseAbort)
	<-abortReturned
}

func TestSessionCancelContainsAcknowledgedAbortBeforeReturning(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	process := newStubProcess(false)
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	process.closeFunc = func() error {
		close(closeStarted)
		<-releaseClose

		return nil
	}

	turnCtx, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:         agent,
		client:        client,
		proc:          process,
		cancel:        turnCancel,
		turnFenceDone: make(chan struct{}),
	}
	bindTestOutbox(session)

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- session.Cancel(t.Context()) }()

	<-closeStarted
	require.NoError(t, turnCtx.Err(),
		"the turn context remains live until the selected native boundary completes")
	select {
	case err := <-cancelDone:
		t.Fatalf("cancel settled before the native boundary: %v", err)
	default:
	}

	close(releaseClose)
	require.NoError(t, <-cancelDone)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
}

// TestUnvalidatedCancelNeverTouchesNativeState pins cancel determinism at the
// native boundary: the version-1 route nonce authorizes the cancel before any
// native side effect, so a cancel that authorizes nothing — a missing
// envelope, a stale nonce, or a session with no current turn to authorize
// against — reaches neither the native interrupt nor a pending dialog.
func TestUnvalidatedCancelNeverTouchesNativeState(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))

	for _, test := range []struct {
		name       string
		activeTurn bool
		meta       map[string]any
	}{
		{name: "missing envelope on the active turn", activeTurn: true},
		{name: "stale nonce on the active turn", activeTurn: true, meta: turnRouteMeta("stale-turn")},
		{name: "current nonce with no active turn", meta: turnRouteMeta("active-turn")},
		{name: "missing envelope with no active turn"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var aborts atomic.Int64

			client := newStubPiClient()
			client.abortFunc = func(context.Context) error {
				aborts.Add(1)

				return nil
			}

			process := newStubProcess(false)
			turnCtx, turnCancel := context.WithCancel(t.Context())
			t.Cleanup(turnCancel)
			dialogCtx, cancelDialog := context.WithCancelCause(t.Context())
			t.Cleanup(func() { cancelDialog(context.Canceled) })

			session := &agentSession{
				agent:          agent,
				id:             "id",
				client:         client,
				proc:           process,
				pendingDialogs: map[string]*dialogCancel{"dialog": {cancel: cancelDialog}},
			}

			if test.activeTurn {
				session.cancel = turnCancel
				session.turnNonce = "active-turn"
			}

			require.Error(t, session.cancelRouted(t.Context(), test.meta))
			require.Zero(t, aborts.Load(), "an unvalidated cancel must not reach the native interrupt")
			require.Zero(t, process.killCalls, "an unvalidated cancel must not contain the native process")
			require.Zero(t, process.closeCalls)
			require.Len(t, session.pendingDialogs, 1, "an unvalidated cancel must not resolve a pending dialog")
			require.NoError(t, dialogCtx.Err())
			require.NoError(t, turnCtx.Err())
			require.False(t, session.wasTurnCancelled())
		})
	}
}

// TestValidatedCancelContainsTheActiveTurn is the other half of the rule: the
// exact active turn's nonce authorizes the cancel, and that cancel does reach
// the native interrupt, the containment boundary, and every pending dialog.
func TestValidatedCancelContainsTheActiveTurn(t *testing.T) {
	var aborts atomic.Int64

	client := newStubPiClient()
	client.abortFunc = func(context.Context) error {
		aborts.Add(1)

		return nil
	}

	process := newStubProcess(false)
	turnCtx, turnCancel := context.WithCancel(t.Context())
	t.Cleanup(turnCancel)
	dialogCtx, cancelDialog := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancelDialog(context.Canceled) })

	session := &agentSession{
		agent:          NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
		id:             "id",
		client:         client,
		proc:           process,
		cancel:         turnCancel,
		turnNonce:      "active-turn",
		pendingDialogs: map[string]*dialogCancel{"dialog": {cancel: cancelDialog}},
	}
	bindTestOutbox(session)

	require.NoError(t, session.cancelRouted(t.Context(), turnRouteMeta("active-turn")))
	require.Equal(t, int64(1), aborts.Load())
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
	require.Empty(t, session.pendingDialogs)
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.True(t, session.wasTurnCancelled())
}

// TestSessionCancelRecordsPumpObservedSettlementForTheCommitAfterContainment
// pins the response-barrier race and the one commit point together: the outbox
// records agent_settled at receipt even when cancellation prevents delivery, the
// containment boundary commits nothing itself, and the settlement that follows
// adopts the complete durable aborted generation after that boundary.
func TestSessionCancelRecordsPumpObservedSettlementForTheCommitAfterContainment(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))

	process := newStubProcess(false)
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	process.closeFunc = func() error {
		close(closeStarted)
		<-releaseClose

		return nil
	}

	turnCtx, turnCancel := context.WithCancel(t.Context())
	delivery := newTurnDelivery()
	outbox := newTestSessionOutbox(1)
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	require.NoError(t, outbox.activate(delivery))
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          newStubPiClient(),
		proc:            process,
		cancel:          turnCancel,
		outbox:          outbox,
		turnEvents:      delivery,
		turnFenceDone:   make(chan struct{}),
		sessionFilePath: path,
	}
	bindTestRuntime(outbox, process, session.client, nil, nil, nil)

	// Reproduce the response-barrier race: the outbox has accepted
	// agent_settled, but cancellation prevents delivery to the prompt loop.
	routeCtx, stopRouting := context.WithCancel(t.Context())
	stopRouting()
	session.routeNativeEvent(routeCtx, outbox, pi.AgentSettledEvent{})
	require.True(t, session.turnNativeSettled, "settlement is recorded at outbox receipt")

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- session.Cancel(t.Context()) }()

	<-closeStarted
	entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Empty(t, entries, "the mirror must remain behind process-tree containment")

	close(releaseClose)
	require.NoError(t, <-cancelDone)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)

	entries, err = store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Empty(t, entries, "the containment boundary is not a commit point")

	var timedOut atomic.Bool
	response, err := session.settlePrompt(
		t.Context(), TextPromptRequest("id", "turn", "done"), &promptTurnState{}, promptOutcome{transportEnded: true}, &timedOut,
	)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	entries, err = store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Len(t, entries, 2, "the durable cancelled generation lands after the boundary that proved it")
}

func TestSessionCancelWithoutNativeSettlementPreservesMirror(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
	_, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          newStubPiClient(),
		proc:            newStubProcess(false),
		cancel:          turnCancel,
		turnFenceDone:   make(chan struct{}),
		sessionFilePath: path,
	}
	bindTestOutbox(session)

	require.NoError(t, session.Cancel(t.Context()))
	entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Empty(t, entries, "a forced pre-settle cancellation must not adopt partial native bytes")
}

func TestSessionCloseAndTimeoutDoNotCommitSettledCancellation(t *testing.T) {
	for _, test := range []struct {
		name  string
		fence func(*agentSession) error
	}{
		{
			name: "close",
			fence: func(session *agentSession) error {
				return session.cancelForClose(t.Context())
			},
		},
		{
			name: "timeout",
			fence: func(session *agentSession) error {
				var timedOut atomic.Bool

				return session.fenceTimedOutTurn(t.Context(), &timedOut)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			agent := NewAgent(
				WithLogger(slog.New(slog.DiscardHandler)),
				WithSessionStore(store),
			)
			path := filepath.Join(t.TempDir(), "session.jsonl")
			require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
			_, turnCancel := context.WithCancel(t.Context())
			session := &agentSession{
				agent:             agent,
				id:                "id",
				client:            newStubPiClient(),
				proc:              newStubProcess(false),
				cancel:            turnCancel,
				turnNativeSettled: true,
				turnFenceDone:     make(chan struct{}),
				sessionFilePath:   path,
			}
			bindTestOutbox(session)

			require.NoError(t, test.fence(session))
			entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
			require.NoError(t, err)
			require.Empty(t, entries, "non-user cancellation must preserve the prior mirror")
		})
	}
}

// TestSettledCancelMirrorFailureFailsTheSettlement pins that a durability
// failure on a cancelled cycle fails the prompt rather than reporting a
// cancelled success, and that no terminal idle stands behind a store that does
// not hold the prefix.
func TestSettledCancelMirrorFailureFailsTheSettlement(t *testing.T) {
	store := newFaultySessionStore()
	store.appendErr = errors.New("durability unavailable")
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
	_, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:             agent,
		id:                "id",
		client:            newStubPiClient(),
		proc:              newStubProcess(false),
		cancel:            turnCancel,
		turnNativeSettled: true,
		turnFenceDone:     make(chan struct{}),
		sessionFilePath:   path,
	}
	bindTestOutbox(session)

	require.NoError(t, session.Cancel(t.Context()))

	var timedOut atomic.Bool
	_, err := session.settlePrompt(
		t.Context(), TextPromptRequest("id", "turn", "done"), &promptTurnState{}, promptOutcome{transportEnded: true}, &timedOut,
	)
	require.ErrorIs(t, err, errSessionMirrorAppend)
	require.ErrorContains(t, err, "durability unavailable")
}

func TestPromptTimeoutContainsProcessTreeBeforeReturning(t *testing.T) {
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithTurnTimeout(20*time.Millisecond),
	)
	client := newStubPiClient()
	process := newStubProcess(false)
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	process.closeFunc = func() error {
		close(closeStarted)
		<-releaseClose

		return nil
	}
	session := &agentSession{agent: agent, id: "id", client: client, proc: process}
	startTestPump(session, client)

	promptDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(t.Context(), TextPromptRequest("id", "timeout-turn", "hang"))
		promptDone <- err
	}()

	<-closeStarted
	select {
	case err := <-promptDone:
		t.Fatalf("timeout settled before the native boundary: %v", err)
	default:
	}

	close(releaseClose)
	err := <-promptDone
	requirePiTurnFailure(t, err, failureCauseTimeout)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
}

func TestTurnFenceInactiveIdempotentAndErrorBranches(t *testing.T) {
	var timedOut atomic.Bool
	inactive := &agentSession{}
	require.NoError(t, inactive.fenceTimedOutTurn(t.Context(), &timedOut))
	require.False(t, timedOut.Load())
	require.NoError(t, inactive.fenceTurnAfterContext(t.Context()))
	require.NoError(t, inactive.fenceTurnAfterFailure(t.Context()))

	turnCtx, turnCancel := context.WithCancel(t.Context())
	process := newStubProcess(false)
	process.close = ErrContainmentIncomplete
	active := &agentSession{
		client:        newStubPiClient(),
		proc:          process,
		cancel:        turnCancel,
		turnFenceDone: make(chan struct{}),
	}
	bindTestOutbox(active)
	err := active.fenceTurnAfterContext(t.Context())
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.ErrorIs(t, active.fenceTurnAfterFailure(t.Context()), ErrContainmentIncomplete)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)

	settling := &agentSession{cancel: func() {}, turnSettling: true}
	require.NoError(t, settling.fenceTurnAfterContext(t.Context()))
}

func TestRelaunchProcessBranches(t *testing.T) {
	base := func(client *stubPiClient) (*agentSession, *Agent) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		old := newStubProcess(true)
		session := &agentSession{agent: agent, id: "id", proc: old, client: newStubPiClient(), sessionFilePath: filepath.Join(t.TempDir(), "missing"), mirroredRows: 5}
		prepareRelaunchFixture(t, session)
		relaunched := newStubProcess(false)
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return relaunched, client, nil
		}

		return session, agent
	}

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/fresh"}
	session, _ := base(client)
	require.NoError(t, session.ensureProcessAlive(t.Context()))
	require.Equal(t, "/fresh", session.sessionFilePath)
	require.Zero(t, session.mirroredRows)

	client = newStubPiClient()
	session, agent := base(client)
	starts := 0
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		starts++

		return nil, nil, ErrContainmentIncomplete
	}
	require.Error(t, session.ensureProcessAlive(t.Context()))
	require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)
	require.ErrorIs(t, session.ensureProcessAlive(t.Context()), ErrContainmentIncomplete)
	require.Equal(t, 1, starts, "incomplete failed relaunch admitted another native root")

	client = newStubPiClient()
	session, _ = base(client)
	oldProcess, ok := session.proc.(*stubProcess)
	require.True(t, ok)
	oldProcess.close = ErrContainmentIncomplete
	require.ErrorIs(t, session.ensureProcessAlive(t.Context()), ErrContainmentIncomplete)

	client = newStubPiClient()
	client.startErr = errors.New("start")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.autoRetryErr = errors.New("retry")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.stateErr = errors.New("state")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.state.SessionID = "other"
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))

	client = newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/hydrated"}
	session, _ = base(client)
	file := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(file, []byte(`{}`), 0o600))
	session.sessionFilePath = file
	require.NoError(t, session.ensureProcessAlive(t.Context()))
	require.Equal(t, "/hydrated", session.sessionFilePath)
}

// TestSessionResidenceSurvivesRelaunch proves a relaunched generation still
// launches from this session's own residence: a generation-private agent
// directory carries the residence forward and rebases every path onto the new
// generation, a durable home keeps addressing the residence the session already
// owns, and the published files stay byte-identical and read-only either way.
func TestSessionResidenceSurvivesRelaunch(t *testing.T) {
	for _, test := range []struct {
		name       string
		durable    bool
		sameConfig bool
	}{
		{name: "generation private agent directory"},
		{name: "durable home", durable: true, sameConfig: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: "id"}

			options := []Option(nil)
			if test.durable {
				options = append(options, WithHome(filepath.Join(t.TempDir(), "home")))
			}

			agent := newStubClientAgent(t, client, options...)
			processes := []piProcess{newStubProcess(true), newStubProcess(false)}

			var launched pi.LaunchSpec

			agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
				launched = spec
				process := processes[0]
				processes = processes[1:]

				return process, client, nil
			}

			session, err := agent.startSession(t.Context(), sessionStart{
				Cwd:        t.TempDir(),
				McpServers: []acp.McpServer{StdioMCPServer("stdio", "/bin/true", nil, nil)},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

			started := launched
			require.Equal(t, session.residence.Root(), filepath.Dir(started.Env[pi.EnvMCPConfig]))

			config, err := os.ReadFile(started.Env[pi.EnvMCPConfig]) // #nosec G304 -- test temp dir.
			require.NoError(t, err)

			require.NoError(t, session.ensureProcessAlive(t.Context()))

			relaunched := launched
			require.NotEmpty(t, relaunched.Env[pi.EnvMCPConfig])
			require.Equal(t, test.sameConfig, started.Env[pi.EnvMCPConfig] == relaunched.Env[pi.EnvMCPConfig])
			require.Equal(t, filepath.Dir(relaunched.Env[pi.EnvMCPConfig]), filepath.Dir(relaunched.ExtensionPaths[0]))

			relaunchedConfig, err := os.ReadFile(relaunched.Env[pi.EnvMCPConfig]) // #nosec G304 -- test temp dir.
			require.NoError(t, err)
			require.Equal(t, config, relaunchedConfig)

			info, err := os.Stat(relaunched.Env[pi.EnvMCPConfig])
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o400), info.Mode().Perm())

			for _, path := range relaunched.ExtensionPaths {
				require.FileExists(t, path)
			}
		})
	}
}

func TestRefreshMCPToolsRebuildsRegistryOnFirstTurn(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	old := newStubProcess(false)
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/refreshed"}
	relaunched := newStubProcess(false)

	sessionFile := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(sessionFile, []byte(`{}`), 0o600))

	var launched pi.LaunchSpec
	starts := 0
	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		starts++
		launched = spec

		return relaunched, client, nil
	}

	session := &agentSession{
		agent:             agent,
		id:                "id",
		launch:            pi.LaunchSpec{SessionDir: "/sessions"},
		sessionFilePath:   sessionFile,
		mcpRefreshPending: true,
		proc:              old,
		client:            newStubPiClient(),
	}
	prepareRelaunchFixture(t, session)
	t.Cleanup(session.stopPump)

	require.NoError(t, session.refreshMCPTools(t.Context()))
	require.Equal(t, 1, starts)
	require.NotEqual(t, sessionFile, launched.SessionPath)
	require.FileExists(t, launched.SessionPath)
	require.Contains(t, launched.SessionPath, launched.NativeRoot)
	require.Empty(t, launched.SessionID)
	require.Same(t, relaunched, session.proc)
	require.Same(t, client, session.client)
	require.Equal(t, "/refreshed", session.sessionFilePath)
	require.False(t, session.mcpRefreshPending)

	require.NoError(t, session.refreshMCPTools(t.Context()))
	require.Equal(t, 1, starts, "the fixed registry is refreshed only once")
}

func TestRefreshMCPToolsKeepsRetryPendingAfterFailedRelaunch(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, errors.New("relaunch")
	}
	session := &agentSession{
		agent:             agent,
		id:                "id",
		mcpRefreshPending: true,
		proc:              newStubProcess(false),
		client:            newStubPiClient(),
	}
	prepareRelaunchFixture(t, session)

	require.ErrorContains(t, session.refreshMCPTools(t.Context()), "relaunch")
	require.True(t, session.mcpRefreshPending)

	incomplete := newStubProcess(false)
	incomplete.close = ErrContainmentIncomplete
	session.proc = incomplete
	session.nativeContainmentErr = nil
	require.ErrorIs(t, session.refreshMCPTools(t.Context()), ErrContainmentIncomplete)
	require.True(t, session.mcpRefreshPending)
}

func prepareRelaunchFixture(t *testing.T, session *agentSession) {
	t.Helper()
	attachTestNativeBoundary(session)
	parent := t.TempDir()
	sessionRoot, err := os.MkdirTemp(parent, "acp-go-pi-session-*")
	require.NoError(t, err)
	dirs, err := createSessionGeneration(sessionRoot)
	require.NoError(t, err)
	require.NoError(t, session.agent.applyGenerationAgentDir(&dirs))
	session.sessionRoot = sessionRoot
	session.launch.AgentDir = dirs.AgentDir
	session.launch.SessionDir = dirs.SessionDir
	session.launch.NativeRoot = dirs.Root
}

func TestNextRuntimeLaunchFailureBranches(t *testing.T) {
	wantErr := errors.New("injected relaunch materialization failure")

	fixture := func(t *testing.T) (*agentSession, pi.LaunchSpec, string) {
		t.Helper()
		parent := t.TempDir()
		sessionRoot, err := os.MkdirTemp(parent, "acp-go-pi-session-")
		require.NoError(t, err)
		oldRoot, err := os.MkdirTemp(sessionRoot, "acp-go-pi-runtime-")
		require.NoError(t, err)
		oldAgent := filepath.Join(oldRoot, "agent")
		require.NoError(t, os.Mkdir(oldAgent, 0o700))
		agent := NewAgent(testContainmentOption(), WithScratchDir(parent), WithLogger(slog.New(slog.DiscardHandler)))
		session := &agentSession{agent: agent, id: "id", sessionRoot: sessionRoot}
		previous := pi.LaunchSpec{
			AgentDir:   oldAgent,
			NativeRoot: oldRoot,
		}

		return session, previous, oldRoot
	}

	t.Run("generation", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		materializeMkdirTemp = func(string, string) (string, error) { return "", wantErr }
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("startup defaults", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		home := filepath.Join(t.TempDir(), "home")
		require.NoError(t, os.MkdirAll(home, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(home, pi.SettingsFileName), []byte(`{"defaultModel":`), 0o600))
		session.agent.options.Home = home
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorContains(t, err, "decode pi settings")
	})

	// A relaunch reads settings.json exactly like a first launch, so the home
	// is reconciled again before the new generation spawns.
	t.Run("reconciles the home", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		home := filepath.Join(t.TempDir(), "home")
		require.NoError(t, os.MkdirAll(home, 0o700))
		session.agent.options.Home = home
		previous.AgentDir = home

		_, err := session.nextRuntimeLaunch(previous, "")
		require.NoError(t, err)

		settings := filepath.Join(home, pi.SettingsFileName)
		require.NoError(t, os.WriteFile(settings, []byte(`{"defaultModel":"gpt-4o"}`), 0o600))

		spec, err := session.nextRuntimeLaunch(previous, "")
		require.NoError(t, err)
		require.Equal(t, home, spec.AgentDir)
		contents, err := os.ReadFile(settings) // #nosec G304 -- the path is this test's own temp dir.
		require.NoError(t, err)
		require.NotContains(t, string(contents), "gpt-4o")
	})

	t.Run("copy agent", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		previous.AgentDir = filepath.Join(t.TempDir(), "missing")
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorContains(t, err, "copy pi agent generation")
	})

	for _, test := range []struct {
		name  string
		apply func(*pi.LaunchSpec, string)
	}{
		{name: "extension", apply: func(spec *pi.LaunchSpec, outside string) { spec.ExtensionPaths = []string{outside} }},
		{name: "skill", apply: func(spec *pi.LaunchSpec, outside string) { spec.SkillPaths = []string{outside} }},
		{name: "prompt template", apply: func(spec *pi.LaunchSpec, outside string) { spec.PromptTemplatePaths = []string{outside} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreMaterializeSeams(t)
			session, previous, _ := fixture(t)
			test.apply(&previous, filepath.Join(t.TempDir(), "outside"))
			_, err := session.nextRuntimeLaunch(previous, "")
			require.ErrorContains(t, err, "outside")
		})
	}

	t.Run("hydrate write", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		last := filepath.Join(t.TempDir(), "session.jsonl")
		require.NoError(t, os.WriteFile(last, []byte("{}\n"), 0o600))
		materializeWriteFile = func(string, []byte, os.FileMode) error { return wantErr }
		_, err := session.nextRuntimeLaunch(previous, last)
		require.ErrorContains(t, err, "hydrate relaunched pi session")
	})

	t.Run("hydrate read", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		materializeReadFile = func(string) ([]byte, error) { return nil, wantErr }
		_, err := session.nextRuntimeLaunch(previous, "/prior/session.jsonl")
		require.ErrorContains(t, err, "read prior pi session")
	})

	t.Run("remove previous", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, oldRoot := fixture(t)
		materializeRemoveAll = func(path string) error {
			if path == oldRoot {
				return wantErr
			}

			return os.RemoveAll(path)
		}
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorContains(t, err, "remove prior runtime generation")
	})

	t.Run("rebases environment", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		previous.Env = map[string]string{"RESOURCE": filepath.Join(previous.AgentDir, "resource"), "OTHER": "/outside"}
		spec, err := session.nextRuntimeLaunch(previous, "")
		require.NoError(t, err)
		require.Equal(t, filepath.Join(spec.AgentDir, "resource"), spec.Env["RESOURCE"])
		require.Equal(t, "/outside", spec.Env["OTHER"])
		require.Equal(t, string(session.id), spec.SessionID)
	})
}

func TestRelaunchAdmissionFailureBranches(t *testing.T) {
	t.Run("agent closed", func(t *testing.T) {
		agent := NewAgent(testContainmentOption())
		agent.mu.Lock()
		agent.closed = true
		agent.mu.Unlock()

		session := &agentSession{agent: agent}
		require.ErrorIs(t, session.relaunchProcess(t.Context()), errAgentClosed)
	})

	t.Run("next launch", func(t *testing.T) {
		session := &agentSession{agent: NewAgent(testContainmentOption()), sessionRoot: filepath.Join(t.TempDir(), "missing")}
		require.Error(t, session.relaunchProcess(t.Context()))
	})
}

func TestRelaunchExactOwnershipGateMatrix(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*agentSession, *Agent, *stubPiClient)
	}{
		{
			name: "immutable construction refuses transfer",
			setup: func(_ *agentSession, agent *Agent, _ *stubPiClient) {
				agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
					agent.mu.Lock()
					for owner := range agent.constructions {
						owner.immutable = true
						owner.err = errors.Join(ErrContainmentIncomplete, errors.New("relaunch waiter"))
					}
					agent.mu.Unlock()

					return newStubProcess(false), newStubPiClient(), nil
				}
			},
		},
		{
			name: "session closes during client start",
			setup: func(session *agentSession, _ *Agent, client *stubPiClient) {
				client.startFunc = func(context.Context) error {
					session.mu.Lock()
					session.closing = true
					session.mu.Unlock()

					return nil
				}
			},
		},
		{
			name: "establishment gate ends during catalog publication",
			setup: func(session *agentSession, agent *Agent, _ *stubPiClient) {
				agent.setConnection(&relaunchCatalogEndingClient{directAgentClient: newDirectAgentClient(), session: session})
			},
		},
		{
			name: "spawn callback panics",
			setup: func(_ *agentSession, agent *Agent, _ *stubPiClient) {
				agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
					panic("spawn callback")
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: "id"}
			session := &agentSession{agent: agent, id: "id", proc: newStubProcess(true), client: newStubPiClient()}
			prepareRelaunchFixture(t, session)
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				return newStubProcess(false), client, nil
			}
			test.setup(session, agent, client)
			require.Error(t, session.relaunchProcess(t.Context()))
		})
	}

	duplicate := &agentSession{relaunchAttempt: &sessionRelaunchAttempt{}}
	_, err := duplicate.beginRelaunch()
	require.ErrorContains(t, err, "already in progress")

	require.NoError(t, (&agentSession{}).containHoistedRelaunch(t.Context(), &sessionRelaunchAttempt{}))
	process := newStubProcess(false)
	attempt := &sessionRelaunchAttempt{proc: process, nativeBoundary: newNativeBoundaryTracker()}
	require.NoError(t, (&agentSession{}).containHoistedRelaunch(t.Context(), attempt))
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)

	containedProcess := newStubProcess(false)
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, containedProcess, newStubPiClient(), nil, nil, nil)
	attempt = &sessionRelaunchAttempt{outbox: outbox}
	session := &agentSession{agent: NewAgent()}
	require.NoError(t, session.containHoistedRelaunch(t.Context(), attempt))
	require.Equal(t, 1, containedProcess.closeCalls)
}
func TestRelaunchPublicationFailureBranches(t *testing.T) {
	base := func(client *stubPiClient) (*agentSession, *Agent) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		session := &agentSession{agent: agent, id: "id", proc: newStubProcess(true), client: newStubPiClient()}
		prepareRelaunchFixture(t, session)
		relaunched := newStubProcess(false)
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return relaunched, client, nil
		}

		return session, agent
	}

	t.Run("catalog fetch", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id"}
		client.commandsErr = errors.New("commands")
		session, _ := base(client)
		require.ErrorContains(t, session.relaunchProcess(t.Context()), "commands")
	})

	t.Run("catalog publication", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id"}
		session, agent := base(client)
		connection := newDirectAgentClient()
		connection.updateErr = errors.New("catalog delivery")
		agent.setConnection(connection)
		require.ErrorContains(t, session.relaunchProcess(t.Context()), "catalog delivery")
	})

	t.Run("lifecycle stream", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id"}
		session, agent := base(client)
		agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
		agent.setConnection(&lifecycleFailingClient{directAgentClient: newDirectAgentClient(), err: errors.New("stream delivery")})
		require.ErrorContains(t, session.relaunchProcess(t.Context()), "stream delivery")
	})
}

// TestRelaunchRecordsGenerationLossBeforeLaunch pins the ordering at the head
// of a relaunch: the lost generation's boundary is recorded before the
// replacement launches, and a store that cannot record it stops the relaunch
// before any native root is admitted.
func TestRelaunchRecordsGenerationLossBeforeLaunch(t *testing.T) {
	store := newFaultySessionStore()
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	agent.setConnection(newDirectAgentClient())
	session := &agentSession{agent: agent, id: "id", proc: newStubProcess(true), client: newStubPiClient()}
	prepareRelaunchFixture(t, session)
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))

	spawns := 0
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return newStubProcess(false), newStubPiClient(), nil
	}
	store.appendErr = errors.New("durability unavailable")

	require.ErrorIs(t, session.relaunchProcess(t.Context()), errLifecycleBoundaryCommit)
	require.True(t, session.lc.fenced, "the lost generation's stream ends even when its record cannot commit")
	require.Zero(t, spawns, "an unrecorded generation loss admits no replacement")
}

func TestRelaunchRechecksCloseAfterRecordingGenerationLoss(t *testing.T) {
	store := &appendCallbackStore{SessionStore: NewInMemorySessionStore()}
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	agent.lifecycle = lifecycle.Negotiated{
		Version:              1,
		UpdatesOutsidePrompt: true,
		ActivityKinds:        []lifecycle.ActivityKind{},
	}
	agent.setConnection(newDirectAgentClient())
	session := &agentSession{agent: agent, id: "id", proc: newStubProcess(true), client: newStubPiClient()}
	prepareRelaunchFixture(t, session)
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))

	store.afterAppend = func() {
		store.afterAppend = nil
		session.mu.Lock()
		session.closing = true
		session.mu.Unlock()
	}
	spawns := 0
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return newStubProcess(false), newStubPiClient(), nil
	}

	requireInvalidParams(t, session.relaunchProcess(t.Context()))
	require.Zero(t, spawns, "close won after the durable loss record but before native launch")
}

func TestCloseRetainsIncompleteSettlementOwner(t *testing.T) {
	store := newFaultySessionStore()
	session, _ := lifecycleSession(t, false)
	session.agent.options.SessionStore = store
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))

	process := newStubProcess(false)
	native := newStubPiClient()
	session.proc = process
	session.client = native
	outbox := bindTestOutbox(session)
	require.Same(t, outbox.nativeBoundary, session.nativeBoundary)
	store.appendErr = ErrContainmentIncomplete

	err := session.Close(t.Context())
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
}

type lifecycleOwnershipMutatingClient struct {
	*directAgentClient
	mutate func()
}

type lifecycleRunningFailureClient struct {
	*directAgentClient
	want error
}

type lifecycleIdleFailureClient struct {
	*directAgentClient
	want error
}

func (c *lifecycleIdleFailureClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	if envelope, ok := notification.Meta[lifecycleMetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok &&
			event["state"] == string(lifecycle.ForegroundIdle) {
			return c.want
		}
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

func (c *lifecycleRunningFailureClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	if envelope, ok := notification.Meta[lifecycleMetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok &&
			event["state"] == string(lifecycle.ForegroundRunning) {
			return c.want
		}
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

func (c *lifecycleOwnershipMutatingClient) SessionUpdate(
	ctx context.Context,
	notification acp.SessionNotification,
) error {
	if c.mutate != nil {
		c.mutate()
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

func TestLifecycleAgentCycleOwnershipFailureMatrix(t *testing.T) {
	negotiated, _ := lifecycleSession(t, false)
	require.ErrorIs(t, negotiated.lifecycleOpenAgentCycle(t.Context(), &agentCycle{generation: 1}), errLifecycleStreamFenced)

	absent, _ := lifecycleSession(t, false)
	absent.agent.lifecycle = lifecycle.Negotiated{}
	absent.lc.negotiated = lifecycle.Negotiated{}
	require.NoError(t, absent.lifecycleOpenAgentCycle(t.Context(), &agentCycle{generation: 1}))
	require.NoError(t, absent.lifecycleSettleAgentCycle(t.Context(), &agentCycle{}, turnVerdict{}))
	require.Empty(t, absent.lifecycleStreamID())

	session, _ := lifecycleSession(t, false)
	require.NoError(t, session.openLifecycleStream(t.Context(), 7))
	require.ErrorIs(t, session.lifecycleOpenAgentCycle(t.Context(), nil), errLifecycleStreamFenced)
	require.ErrorIs(t, session.lifecycleOpenAgentCycle(t.Context(), &agentCycle{generation: 8}), errLifecycleStreamFenced)
	require.ErrorIs(t, session.lifecycleOpenAgentCycle(t.Context(), &agentCycle{}), errLifecycleStreamFenced)
	session.lc.generation = 0
	require.ErrorIs(t, session.lifecycleOpenAgentCycle(t.Context(), &agentCycle{}), errLifecycleStreamFenced)
	session.lc.generation = 7

	cycle := &agentCycle{generation: 7, state: &promptTurnState{}}
	require.NoError(t, session.lifecycleOpenAgentCycle(t.Context(), cycle))
	require.ErrorIs(t, session.lifecycleOpenAgentCycle(t.Context(), &agentCycle{generation: 7}), errLifecycleStreamFenced)
	require.ErrorIs(t,
		session.lifecycleSettleAgentCycle(t.Context(), &agentCycle{generation: 7}, turnVerdict{}),
		errLifecycleStreamFenced,
	)
	session.lc.fenced = true
	require.ErrorIs(t, session.lifecycleSettleAgentCycle(t.Context(), cycle, turnVerdict{}), errLifecycleStreamFenced)

	missingStream, _ := lifecycleSession(t, false)
	require.ErrorIs(t,
		missingStream.lifecycleSettleAgentCycle(t.Context(), &agentCycle{}, turnVerdict{}),
		errLifecycleStreamFenced,
	)
}

func TestLifecycleAgentCycleMintFailures(t *testing.T) {
	original := lifecycleRandRead
	t.Cleanup(func() { lifecycleRandRead = original })

	for _, failingCall := range []int{1, 2} {
		session, _ := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), uint64(failingCall)))
		calls := 0
		lifecycleRandRead = func(data []byte) (int, error) {
			calls++
			if calls == failingCall {
				return 0, errors.New("cycle entropy")
			}

			return original(data)
		}

		err := session.lifecycleOpenAgentCycle(t.Context(), &agentCycle{generation: uint64(failingCall)})
		require.ErrorContains(t, err, "cycle entropy")
		lifecycleRandRead = original
	}
}

func TestLifecycleActionAndDeliveryOwnershipEdges(t *testing.T) {
	session, _ := lifecycleSession(t, false)
	require.NoError(t, session.openLifecycleStream(t.Context(), 3))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))

	wrongOutbox := newTestSessionOutbox(4)
	_, announceable, err := session.prepareLifecycleActionFor(wrongOutbox)
	require.ErrorIs(t, err, errLifecycleActionUnowned)
	require.False(t, announceable)

	action, announceable, err := session.prepareLifecycleAction()
	require.NoError(t, err)
	require.True(t, announceable)
	session.revokeLifecycleAction(pendingAction{generation: 99, actionID: action.actionID})
	require.False(t, session.lc.fenced)

	stream := session.lc.stream
	session.lc.stream = nil
	require.NoError(t, session.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
	session.lc.stream = stream

	want := errors.New("immutable lifecycle quarantine")
	session.quarantineLifecycleGeneration(3, nil)
	session.quarantineLifecycleGeneration(99, want)
	require.NoError(t, session.lifecycleGenerationQuarantine(3))
	session.quarantineLifecycleGeneration(3, want)
	require.ErrorIs(t, session.lifecycleGenerationQuarantine(3), ErrContainmentIncomplete)
	require.ErrorIs(t, session.lifecycleGenerationQuarantine(99), errLifecycleStreamFenced)
	session.quarantineLifecycleGeneration(3, errors.New("replacement"))
	require.ErrorIs(t, session.emitLifecycleLocked(t.Context(), lifecycle.QuiescenceEvent(lifecycle.QuiescenceFact{})), want)
}

func TestLifecycleDeliveryMutationFencesExactGeneration(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*agentSession)
		want   error
	}{
		{
			name: "generation changed",
			mutate: func(session *agentSession) {
				session.lcMu.Lock()
				session.lc.generation++
				session.lcMu.Unlock()
			},
			want: errLifecycleStreamFenced,
		},
		{
			name: "generation quarantined",
			mutate: func(session *agentSession) {
				session.quarantineLifecycleGeneration(session.lc.generation, errors.New("delivery quarantined"))
			},
			want: ErrContainmentIncomplete,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			session, base := lifecycleSession(t, true)
			require.NoError(t, session.openLifecycleStream(t.Context(), 5))
			session.agent.conn = &lifecycleOwnershipMutatingClient{
				directAgentClient: base,
				mutate:            func() { testCase.mutate(session) },
			}

			err := func() error {
				session.lcMu.Lock()
				defer session.lcMu.Unlock()

				return session.emitLifecycleLocked(t.Context(), lifecycle.QuiescenceEvent(lifecycle.QuiescenceFact{
					Quiescent: true,
					Source:    session.lc.negotiated.QuiescenceSource,
					Watermark: session.lc.stream.State().ReducedThrough,
					Barrier:   "ownership-edge",
				}))
			}()
			require.ErrorIs(t, err, testCase.want)
			require.True(t, session.lc.fenced)
		})
	}
}

func TestLifecycleTerminalDeliveryFailureEdges(t *testing.T) {
	want := errors.New("terminal host delivery")

	t.Run("agent cycle blocker", func(t *testing.T) {
		session, client := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		cycle := &agentCycle{generation: 1, state: &promptTurnState{}}
		require.NoError(t, session.lifecycleOpenAgentCycle(t.Context(), cycle))
		action, ok, err := session.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, session.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		client.updateErr = want
		require.ErrorIs(t, session.lifecycleSettleAgentCycle(t.Context(), cycle, turnVerdict{}), want)
	})

	t.Run("turn idle", func(t *testing.T) {
		session, client := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
		client.updateErr = want
		require.ErrorIs(t,
			session.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonEndTurn, lifecycle.OutcomeSuccess),
			want,
		)
	})

	t.Run("resume after final action", func(t *testing.T) {
		session, client := lifecycleSession(t, false)
		require.NoError(t, session.openLifecycleStream(t.Context(), 1))
		require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
		action, ok, err := session.prepareLifecycleAction()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, session.announceLifecycleAction(t.Context(), action, lifecycle.ActionPermission))
		session.agent.conn = &lifecycleRunningFailureClient{directAgentClient: client, want: want}
		require.ErrorIs(t, session.lifecycleResolveAction(t.Context(), action.actionID, lifecycle.ActionAccepted), want)
	})
}
