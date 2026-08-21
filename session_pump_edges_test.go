package piacp

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type outboxMutatingClient struct {
	*directAgentClient
	outbox *sessionOutbox
}

func (c *outboxMutatingClient) NotifyExtension(ctx context.Context, method string, value any) error {
	c.outbox.mu.Lock()
	c.outbox.state = outboxReserved
	c.outbox.mu.Unlock()

	return c.directAgentClient.NotifyExtension(ctx, method, value)
}

type postStepCancelledContext struct {
	context.Context //nolint:containedctx // The test wrapper controls cancellation between boundary steps.
	cancelled       atomic.Bool
	never           chan struct{}
}

type secondWaitCancelledContext struct {
	context.Context //nolint:containedctx // The test wrapper controls the second exact ownership wait.
	calls           atomic.Int32
	open            chan struct{}
	done            chan struct{}
}

func (c *secondWaitCancelledContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 1 {
		return c.open
	}

	return c.done
}

func (*secondWaitCancelledContext) Err() error { return context.Canceled }

func (c *postStepCancelledContext) Done() <-chan struct{} { return c.never }
func (c *postStepCancelledContext) Err() error {
	if c.cancelled.Load() {
		return context.Canceled
	}

	return nil
}

func TestNativeDialogAndDispatchOwnershipEdges(t *testing.T) {
	var missing *nativeDialog
	missing.complete()
	missing.answer(t.Context(), &agentSession{}, pi.UICancelResponse("missing"))
	var delivery *turnDelivery
	delivery.abandonQueuedDialogs(t.Context(), &agentSession{})

	var missingGate *dispatchGate
	require.ErrorIs(t, missingGate.lock(t.Context()), pi.ErrTransportClosed)
	require.False(t, missingGate.TryLock())
	gate := newDispatchGate()
	require.NoError(t, gate.lock(t.Context()))
	require.False(t, gate.TryLock())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, gate.lock(cancelled), context.Canceled)
	gate.Unlock()
}

func TestGenerationProducerAndObservationClosedEdges(t *testing.T) {
	var missing *generationProducers
	_, admitted := missing.acquire(1)
	require.False(t, admitted)
	require.NoError(t, missing.wait(t.Context()))
	require.NoError(t, missing.waitChildren(t.Context()))
	missing.releaseRoot()

	producers := newGenerationProducers()
	_, admitted = producers.acquire(0)
	require.False(t, admitted)
	producers.releaseRoot()
	_, admitted = producers.acquire(1)
	require.False(t, admitted)

	var outbox *sessionOutbox
	require.False(t, outbox.claimProviderObservation())
}

func TestStopPumpJoinsProducerRootAfterPumpExit(t *testing.T) {
	outbox := newTestSessionOutbox(1)
	pumpDone := make(chan struct{})
	close(pumpDone)
	session := &agentSession{outbox: outbox, pumpDone: pumpDone}
	closed := make(chan struct{})
	close(closed)
	ctx := &secondWaitCancelledContext{Context: t.Context(), open: make(chan struct{}), done: closed}

	err := session.stopPumpBounded(ctx)
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
	outbox.producers.releaseRoot()
	require.NoError(t, outbox.producers.wait(t.Context()))
}

func TestOutboxReservationAndEstablishmentFailureEdges(t *testing.T) {
	var missing *sessionOutbox
	require.NoError(t, missing.waitEstablished(t.Context()))
	require.ErrorIs(t, missing.reserveClaimed(newTurnDelivery()), pi.ErrTransportClosed)
	delivery := newTurnDelivery()
	require.ErrorIs(t, missing.activate(delivery), pi.ErrTransportClosed)
	_, open := <-delivery.events
	require.False(t, open)
	require.Nil(t, missing.takePreAcceptance(delivery))
	require.False(t, missing.beginConfiguration())
	missing.finishConfiguration()
	require.True(t, missing.beginRestore())
	missing.finishRestore()
	deferred, overflow := missing.deferStartupUI(pi.UIRequest{})
	require.False(t, deferred)
	require.False(t, overflow)
	require.ErrorIs(t, missing.acceptEstablishment(), pi.ErrProcessContainmentIncomplete)

	outbox := newSessionOutbox(1, newNativeBoundaryTracker())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, outbox.waitEstablished(cancelled), context.Canceled)
	require.Error(t, outbox.reserveClaimed(newTurnDelivery()))
	require.Error(t, outbox.activate(newTurnDelivery()))
	require.Nil(t, outbox.takePreAcceptance(newTurnDelivery()))
	outbox.end()
	require.False(t, outbox.beginConfiguration())

	admission := newTurnDelivery()
	outbox = newTestSessionOutbox(1)
	outbox.state = outboxPromptPending
	outbox.admission = admission
	outbox.releasePromptAdmission(admission)
	require.Equal(t, outboxIdle, outbox.state)
	require.False(t, outbox.beginCycleSettlement(&agentCycle{}))
}

func TestOutboxRetentionAndStartupBounds(t *testing.T) {
	outbox := newTestSessionOutbox(1)
	outbox.state = outboxState(255)
	require.Equal(t, outboxViolation, outbox.classifyLocked(pi.TurnStartEvent{}).disposition)

	outbox = newTestSessionOutbox(1)
	outbox.state = outboxReserved
	outbox.queued = []pi.Event{pi.TurnStartEvent{}}
	_, _, ok := outbox.popRetained()
	require.False(t, ok)

	var missing *sessionOutbox
	deferred, overflow := missing.deferStartupUI(pi.UIRequest{})
	require.False(t, deferred)
	require.False(t, overflow)
	outbox = newSessionOutbox(1, newNativeBoundaryTracker())
	outbox.startup = make([]startupRecord, outboxQueueCapacity)
	deferred, overflow = outbox.deferStartupUI(pi.UIRequest{ID: "overflow"})
	require.False(t, deferred)
	require.True(t, overflow)

	admission := outbox.admit(pi.AgentStartEvent{})
	require.Equal(t, outboxOverflow, admission.disposition)
	outbox = newTestSessionOutbox(1)
	outbox.state = outboxPromptPending
	outbox.preAcceptance = make([]pi.Event, outboxQueueCapacity)
	require.Equal(t, outboxOverflow, outbox.admit(pi.QueueUpdateEvent{}).disposition)

	outbox = newTestSessionOutbox(2)
	outbox.queued = []pi.Event{pi.TurnStartEvent{}}
	require.Equal(t, outboxQueued, outbox.admit(pi.TurnStartEvent{}).disposition)
}

func TestContainmentMemoizationRejectsForeignOrMissingOwners(t *testing.T) {
	var missing *sessionOutbox
	missing.finishContainment(nil, nil)
	_, ok := missing.awaitContainment()
	require.False(t, ok)

	outbox := newTestSessionOutbox(1)
	owned, _ := outbox.claimContainmentLocked(containmentOwnerClose)
	foreign := &generationContainment{done: make(chan struct{})}
	outbox.finishContainment(foreign, errors.New("foreign"))
	select {
	case <-foreign.done:
		t.Fatal("foreign containment was published")
	default:
	}
	outbox.finishContainment(nil, nil)
	outbox.finishContainment(owned, nil)
	require.NoError(t, func() error {
		err, _ := outbox.awaitContainment()

		return err
	}())
}

func TestPumpPublicationFailsClosedWithoutExactBoundary(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	cancelled := false
	_, err := session.startPumpContext(t.Context(), func() { cancelled = true }, newStubPiClient(), nil)
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
	require.True(t, cancelled)

	boundary := newNativeBoundaryTracker()
	session.nativeBoundary = boundary
	session.closing = true
	cancelled = false
	_, err = session.startPumpContext(t.Context(), func() { cancelled = true }, newStubPiClient(), boundary)
	require.Error(t, err)
	require.True(t, cancelled)

	session.closing = false
	cancelled = false
	_, err = session.startPumpContext(t.Context(), func() { cancelled = true }, newStubPiClient(), newNativeBoundaryTracker())
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
	require.True(t, cancelled)

	cancelled = false
	_, outbox, published := session.publishRuntimeGeneration(t.Context(), func() { cancelled = true }, nil, nil, nil, nil, nil)
	require.False(t, published)
	require.Nil(t, outbox)
	require.True(t, cancelled)
}

func TestPumpDrainsClaimedBoundaryOnGenerationCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := newStubPiClient()
	buffered := make(chan pi.ResponseBoundary, 1)
	buffered <- pi.ResponseBoundary{}
	close(buffered)
	resolveBufferedResponseBoundaries(ctx, buffered)
	resolveBufferedResponseBoundaries(ctx, nil)
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	outbox := newTestSessionOutbox(1)
	done := make(chan struct{})
	cancel()
	session.pump(ctx, client, outbox, done)
	select {
	case <-done:
	default:
		t.Fatal("cancelled pump did not finish")
	}
}

func TestExistingIncompleteContainmentIsRetainedByFirstPoison(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	outbox := newTestSessionOutbox(1)
	containment, _ := outbox.claimContainmentLocked(containmentOwnerClose)
	session.containGeneration(t.Context(), outbox, "first poison")
	want := errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("owner incomplete"))
	outbox.finishContainment(containment, want)
	require.NoError(t, outbox.producers.waitChildren(t.Context()))
	require.ErrorIs(t, session.nativeContainmentError(), pi.ErrProcessContainmentIncomplete)
}

func TestContainmentAndNativeBoundaryFailureEdges(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	session.containGeneration(t.Context(), nil, "missing")
	require.ErrorIs(t, session.containGenerationSync(t.Context(), nil, "missing"), pi.ErrProcessContainmentIncomplete)

	outbox := newTestSessionOutbox(1)
	outbox.producers.releaseRoot()
	require.ErrorIs(t, session.containGenerationSync(t.Context(), outbox, "closed admission"), pi.ErrProcessContainmentIncomplete)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, runNativeBoundaryStep(cancelled, "cancelled", func() error { return nil }), context.Canceled)

	post := &postStepCancelledContext{Context: t.Context(), never: make(chan struct{})}
	require.ErrorIs(t, runNativeBoundaryStep(post, "late", func() error {
		post.cancelled.Store(true)

		return nil
	}), context.Canceled)

	entered := make(chan struct{})
	release := make(chan struct{})
	bounded, cancelBounded := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- runNativeBoundaryStep(bounded, "blocked", func() error {
			close(entered)
			<-release

			return nil
		})
	}()
	<-entered
	cancelBounded()
	require.ErrorIs(t, <-done, context.Canceled)
	close(release)

	var tracker *nativeBoundaryTracker
	require.ErrorIs(t, tracker.run(t.Context(), "missing", func() error { return nil }), pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, tracker.retainedIncomplete(), pi.ErrProcessContainmentIncomplete)
	require.Contains(t, generationContainmentPanicError("panic").Error(), "panic")
}

func TestStopNativeGenerationBoundsPumpJoin(t *testing.T) {
	process := newStubProcess(false)
	native := newStubPiClient()
	outbox := newTestSessionOutbox(1)
	pumpDone := make(chan struct{})
	bindTestRuntime(outbox, process, native, nil, pumpDone, nil)
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler))), outbox: outbox}
	ctx, cancel := context.WithCancel(t.Context())
	process.shutdownFunc = func(context.Context) error {
		cancel()

		return nil
	}
	err := session.stopNativeGeneration(ctx, outbox)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, process.closeCalls)
}

func TestNativeBoundaryPriorAndRetainedIncompleteEdges(t *testing.T) {
	tracker := newNativeBoundaryTracker()
	completed := &nativeBoundaryCall{stage: "abort", done: make(chan struct{})}
	close(completed.done)
	require.NoError(t, tracker.awaitPrior(t.Context(), "close", completed))

	blocked := &nativeBoundaryCall{stage: "abort", done: make(chan struct{})}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, tracker.awaitPrior(cancelled, "close", blocked), pi.ErrProcessContainmentIncomplete)
	first := tracker.retainedIncomplete()
	tracker.retainIncomplete(errors.New("later"))
	require.Equal(t, first, tracker.retainedIncomplete())

	fresh := newNativeBoundaryTracker()
	want := errors.New("first")
	fresh.retainIncomplete(want)
	require.Equal(t, want, fresh.retainedIncomplete())
}

func TestNativeBoundaryRunSerializesPriorOwnerAndLateCancellation(t *testing.T) {
	t.Run("prior owner completes", func(t *testing.T) {
		tracker := newNativeBoundaryTracker()
		prior := &nativeBoundaryCall{stage: "abort", done: make(chan struct{})}
		tracker.active = prior
		observed := &observedDoneContext{Context: t.Context(), entered: make(chan struct{})}
		done := make(chan error, 1)
		go func() { done <- tracker.run(observed, "close", func() error { return nil }) }()
		<-observed.entered
		tracker.mu.Lock()
		tracker.active = nil
		close(prior.done)
		tracker.mu.Unlock()
		require.NoError(t, <-done)
	})

	t.Run("prior owner misses bound", func(t *testing.T) {
		tracker := newNativeBoundaryTracker()
		tracker.active = &nativeBoundaryCall{stage: "abort", done: make(chan struct{})}
		parent, cancel := context.WithCancel(t.Context())
		observed := &observedDoneContext{Context: parent, entered: make(chan struct{})}
		done := make(chan error, 1)
		go func() { done <- tracker.run(observed, "close", func() error { return nil }) }()
		<-observed.entered
		cancel()
		require.ErrorIs(t, <-done, pi.ErrProcessContainmentIncomplete)
	})

	t.Run("completed call observes late cancellation", func(t *testing.T) {
		tracker := newNativeBoundaryTracker()
		ctx := &postStepCancelledContext{Context: t.Context(), never: make(chan struct{})}
		err := tracker.run(ctx, "shutdown", func() error {
			ctx.cancelled.Store(true)

			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestDialogRoutingClosedAndBoundedBranches(t *testing.T) {
	native := newStubPiClient()
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler))), client: native}
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, newStubProcess(false), native, nil, nil, nil)
	session.outbox = outbox

	outbox.interactionsClosed = true
	session.routeUIRequestEstablished(t.Context(), outbox, pi.UIRequest{
		ID: "deny", Method: uiMethodSelect, Title: pi.PermissionTitleMarker + `{}`,
	})
	require.Equal(t, pi.UIValueResponse("deny", pi.PermissionOptionDeny), native.responses[0])
	outbox.interactionsClosed = false
	session.routeUIRequestEstablished(t.Context(), outbox, pi.UIRequest{Method: "notify"})

	delivery := newTurnDelivery()
	outbox.turn = delivery
	outbox.state = outboxForeground
	delivery.uiRequests <- &nativeDialog{}
	close(delivery.done)
	session.routeUIRequestEstablished(t.Context(), outbox, pi.UIRequest{ID: "closed", Method: uiMethodInput})

	outbox = newTestSessionOutbox(2)
	bindTestRuntime(outbox, newStubProcess(false), native, nil, nil, nil)
	delivery = newTurnDelivery()
	outbox.turn = delivery
	outbox.state = outboxForeground
	delivery.uiRequests <- &nativeDialog{}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	session.routeUIRequestEstablished(cancelled, outbox, pi.UIRequest{ID: "cancelled", Method: uiMethodInput})

	mutatingHost := &outboxMutatingClient{directAgentClient: newDirectAgentClient(), outbox: outbox}
	session.agent.conn = mutatingHost
	session.rawMessages = rawMessageConfig{All: true}
	delivery = newTurnDelivery()
	outbox.turn = delivery
	outbox.state = outboxForeground
	session.routeUIRequestEstablished(t.Context(), outbox, uiRequest(t, "stale-after-raw", uiMethodInput, "question"))
	require.Contains(t, native.responses, pi.UICancelResponse("stale-after-raw"))
}

func TestStartupUIOverflowContainsExactGeneration(t *testing.T) {
	process := newStubProcess(false)
	native := newStubPiClient()
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	outbox := newSessionOutbox(1, newNativeBoundaryTracker())
	bindTestRuntime(outbox, process, native, nil, nil, nil)
	outbox.startup = make([]startupRecord, outboxQueueCapacity)
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{ID: "overflow", Method: uiMethodInput})
	err, ok := outbox.awaitContainment()
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
}

func TestHandlerAdmissionFailureEdges(t *testing.T) {
	session := &agentSession{}
	_, admitted := session.admitDialogHandler(nil)
	require.False(t, admitted)
	_, _, admitted = session.admitActionHandlers(nil)
	require.False(t, admitted)

	outbox := newTestSessionOutbox(1)
	outbox.producers.releaseRoot()
	_, admitted = session.admitDialogHandler(outbox)
	require.False(t, admitted)
	_, _, admitted = session.admitActionHandlers(outbox)
	require.False(t, admitted)

	outbox = newTestSessionOutbox(2)
	outbox.interactions.releaseRoot()
	_, admitted = session.admitDialogHandler(outbox)
	require.False(t, admitted)

	outbox = newTestSessionOutbox(3)
	outbox.interactionsClosed = true
	_, _, admitted = session.admitActionHandlers(outbox)
	require.False(t, admitted)
}

func TestStopPumpBoundedJoinsExactProducerRoot(t *testing.T) {
	t.Run("pump misses bound", func(t *testing.T) {
		session := &agentSession{pumpDone: make(chan struct{})}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, session.stopPumpBounded(ctx), pi.ErrProcessContainmentIncomplete)
	})

	t.Run("producer misses bound after pump", func(t *testing.T) {
		outbox := newTestSessionOutbox(1)
		done := make(chan struct{})
		close(done)
		session := &agentSession{pumpDone: done, outbox: outbox}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, session.stopPumpBounded(ctx), pi.ErrProcessContainmentIncomplete)
	})
}

func TestPromptForegroundAdmissionFailureEdges(t *testing.T) {
	delivery := newTurnDelivery()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, (&agentSession{}).claimPromptForeground(cancelled, delivery), context.Canceled)

	tests := []struct {
		name  string
		setup func(*agentSession, *sessionOutbox)
	}{
		{name: "closing", setup: func(s *agentSession, _ *sessionOutbox) { s.closing = true }},
		{name: "poisoned", setup: func(s *agentSession, _ *sessionOutbox) { s.poisonCause = "poison" }},
		{name: "already admitted", setup: func(s *agentSession, _ *sessionOutbox) { s.promptAdmission = newTurnDelivery() }},
		{name: "outbox refused", setup: func(_ *agentSession, o *sessionOutbox) { o.state = outboxAgentCycle }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outbox := newTestSessionOutbox(1)
			session := &agentSession{agent: NewAgent(), outbox: outbox}
			test.setup(session, outbox)
			require.Error(t, session.claimPromptForeground(t.Context(), delivery))
		})
	}

	old := newSessionOutbox(1, newNativeBoundaryTracker())
	next := newTestSessionOutbox(2)
	session := &agentSession{agent: NewAgent(), outbox: old}
	observed := &observedDoneContext{Context: t.Context(), entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- session.claimPromptForeground(observed, delivery) }()
	<-observed.entered
	session.mu.Lock()
	session.outbox = next
	session.mu.Unlock()
	old.mu.Lock()
	old.established = true
	old.finishEstablishmentLocked()
	old.mu.Unlock()
	require.NoError(t, <-done)
	require.Same(t, delivery, session.promptAdmission)
}

func TestReserveClaimedPromptFailureEdges(t *testing.T) {
	delivery := newTurnDelivery()
	tests := []struct {
		name  string
		setup func(*agentSession)
	}{
		{name: "poisoned", setup: func(s *agentSession) { s.poisonCause = "poison" }},
		{name: "closing", setup: func(s *agentSession) { s.closing = true }},
		{name: "wrong claim", setup: func(s *agentSession) { s.promptAdmission = newTurnDelivery() }},
		{name: "missing outbox", setup: func(s *agentSession) { s.promptAdmission = delivery }},
		{name: "outbox refuses", setup: func(s *agentSession) {
			s.promptAdmission = delivery
			s.outbox = newTestSessionOutbox(1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &agentSession{agent: NewAgent()}
			test.setup(session)
			_, _, err := session.reserveClaimedPromptForeground(delivery)
			require.Error(t, err)
		})
	}
}

func TestRefreshAndReservePromptFailureClassification(t *testing.T) {
	delivery := newTurnDelivery()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := (&agentSession{}).refreshAndReservePrompt(cancelled, delivery)
	require.ErrorIs(t, err, context.Canceled)

	t.Run("refresh error without fence", func(t *testing.T) {
		session := &agentSession{agent: NewAgent(), nativeBoundary: newNativeBoundaryTracker()}
		_, _, err := session.refreshAndReservePrompt(t.Context(), delivery)
		require.ErrorContains(t, err, "no process")
	})

	t.Run("refresh error with fence", func(t *testing.T) {
		session := &agentSession{agent: NewAgent(), nativeBoundary: newNativeBoundaryTracker(), closing: true}
		_, _, err := session.refreshAndReservePrompt(t.Context(), delivery)
		require.Error(t, err)
	})

	t.Run("reservation backpressure", func(t *testing.T) {
		process := newStubProcess(false)
		outbox := newTestSessionOutbox(1)
		session := &agentSession{
			agent: NewAgent(), proc: process, client: newStubPiClient(), nativeBoundary: outbox.nativeBoundary,
			outbox: outbox, promptAdmission: newTurnDelivery(),
		}
		_, _, err := session.refreshAndReservePrompt(t.Context(), delivery)
		require.Error(t, err)
		require.NotErrorIs(t, err, pi.ErrTransportClosed)
	})

	t.Run("reservation fence", func(t *testing.T) {
		process := newStubProcess(false)
		outbox := newTestSessionOutbox(1)
		session := &agentSession{
			agent: NewAgent(), proc: process, client: newStubPiClient(), nativeBoundary: outbox.nativeBoundary,
			outbox: outbox, promptAdmission: delivery, poisonCause: "poison",
		}
		_, _, err := session.refreshAndReservePrompt(t.Context(), delivery)
		require.ErrorContains(t, err, "poison")
	})

	t.Run("transport without router", func(t *testing.T) {
		process := newStubProcess(false)
		session := &agentSession{
			agent: NewAgent(), proc: process, client: newStubPiClient(), nativeBoundary: newNativeBoundaryTracker(),
			promptAdmission: delivery,
		}
		_, _, err := session.refreshAndReservePrompt(t.Context(), delivery)
		require.ErrorIs(t, err, pi.ErrTransportClosed)
	})
}

func TestContainmentPanicPublishesImmutableFailure(t *testing.T) {
	process := newStubProcess(false)
	native := newStubPiClient()
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, native, nil, nil, nil)
	session := &agentSession{agent: &Agent{}}
	session.containGeneration(t.Context(), outbox, "panic after containment")
	err, ok := outbox.awaitContainment()
	require.True(t, ok)
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
}

func TestPromptAcceptanceReturnsExistingGenerationQuarantine(t *testing.T) {
	session, _ := lifecycleSession(t, false)
	outbox := newTestSessionOutbox(1)
	delivery := newTurnDelivery()
	session.outbox = outbox
	session.turnEvents = delivery
	session.lc.generation = outbox.generation
	session.lc.quarantineErr = errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("retained"))

	err := session.acceptPromptResponse(t.Context(), outbox, delivery, testSubmission())
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
}

func TestPromptDispatchRefusesEveryStaleOwner(t *testing.T) {
	session := &agentSession{}
	delivery := newTurnDelivery()
	_, err := session.beginPromptDispatch(t.Context(), nil, delivery)
	require.ErrorIs(t, err, pi.ErrTransportClosed)

	outbox := newTestSessionOutbox(1)
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	session.outbox = outbox
	session.turnEvents = delivery

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, outbox.dispatchMu.lock(t.Context()))
	_, err = session.beginPromptDispatch(cancelled, outbox, delivery)
	outbox.dispatchMu.Unlock()
	require.ErrorIs(t, err, context.Canceled)

	session.closing = true
	_, err = session.beginPromptDispatch(t.Context(), outbox, delivery)
	require.Error(t, err)
	session.closing = false
	session.poisonCause = "poisoned"
	_, err = session.beginPromptDispatch(t.Context(), outbox, delivery)
	require.Error(t, err)
	session.poisonCause = ""
	session.turnEvents = newTurnDelivery()
	_, err = session.beginPromptDispatch(t.Context(), outbox, delivery)
	require.ErrorIs(t, err, pi.ErrTransportClosed)
	session.turnEvents = delivery
	outbox.state = outboxForeground
	_, err = session.beginPromptDispatch(t.Context(), outbox, delivery)
	require.Error(t, err)

	require.ErrorIs(t, session.acceptPromptResponse(t.Context(), newTestSessionOutbox(2), delivery, testSubmission()), pi.ErrTransportClosed)
}
