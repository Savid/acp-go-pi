package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type opaqueNativeEvent struct{ kind string }

func (e opaqueNativeEvent) Kind() string           { return e.kind }
func (opaqueNativeEvent) RawJSON() json.RawMessage { return json.RawMessage(`{"type":"opaque"}`) }

func TestQueuedPermissionAbandonDeniesExactEmitterAndNotSuccessor(t *testing.T) {
	oldClient := newStubPiClient()
	successorClient := newStubPiClient()
	session := &agentSession{agent: NewAgent(), id: "dialog-owner", client: oldClient}
	oldOutbox := newTestSessionOutbox(1)
	bindTestRuntime(oldOutbox, newStubProcess(false), oldClient, nil, nil, nil)
	delivery := newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(oldOutbox, delivery))
	require.NoError(t, oldOutbox.activate(delivery))
	session.outbox = oldOutbox
	session.turnEvents = delivery

	request := uiRequest(t, "permission-dialog", uiMethodSelect, pi.PermissionTitleMarker+`{"toolCallId":"call"}`)
	session.routeUIRequest(t.Context(), oldOutbox, request)

	successor := newTestSessionOutbox(2)
	bindTestRuntime(successor, newStubProcess(false), successorClient, nil, nil, nil)
	session.mu.Lock()
	session.client = successorClient
	session.outbox = successor
	session.registerContainmentOutboxLocked(oldOutbox)
	session.mu.Unlock()

	delivery.abandonQueuedDialogs(t.Context(), session)
	delivery.end()
	require.NoError(t, oldOutbox.producers.waitChildren(t.Context()))

	oldClient.mu.Lock()
	require.Equal(t, []pi.UIResponse{pi.UIValueResponse("permission-dialog", pi.PermissionOptionDeny)}, oldClient.responses)
	oldClient.mu.Unlock()
	successorClient.mu.Lock()
	require.Empty(t, successorClient.responses)
	successorClient.mu.Unlock()
}

func TestUnroutedUnknownNativeKindIsNotLogged(t *testing.T) {
	const secret = "unknown-native-kind-secret-sentinel"
	logs := &strings.Builder{}
	agent := NewAgent(WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
	session := &agentSession{agent: agent, id: "id"}
	outbox := newTestSessionOutbox(1)
	outbox.end()

	session.routeNativeEvent(t.Context(), outbox, opaqueNativeEvent{kind: secret})
	require.NotContains(t, logs.String(), secret)
}

func TestDialogTokenPrecedesEnqueueAndAbandonmentReleasesIt(t *testing.T) {
	native := newStubPiClient()
	session := &agentSession{agent: NewAgent(), id: "dialog-token", client: native}
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, newStubProcess(false), native, nil, nil, nil)
	delivery := newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	require.NoError(t, outbox.activate(delivery))
	session.outbox = outbox

	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{ID: "queued", Method: uiMethodInput})
	close(delivery.done)
	joinCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, outbox.producers.waitChildren(joinCtx))

	dialog := <-delivery.uiRequests
	require.False(t, dialog.claim(), "an abandoned queued dialog regained a consumer")
	require.NoError(t, outbox.producers.waitChildren(joinCtx))
}

// TestOutboxRoutingBranches walks the dispositions one generation produces: a
// dispatched foreground that has stopped reading, a delivery whose context has
// ended, a dialog with no foreground to block, a dialog a foreground answers,
// and the pump's own end.
func TestOutboxRoutingBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}
	event := pi.TurnStartEvent{}
	outbox := newTestSessionOutbox(1)
	session.outbox = outbox

	// A generation that has ended drops the record without touching anything.
	ended := newTestSessionOutbox(0)
	ended.end()
	session.routeNativeEvent(t.Context(), ended, event)
	require.Empty(t, connection.notified)

	delivery := newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	session.turnEvents = delivery
	require.NoError(t, outbox.activate(delivery))
	close(delivery.done)
	session.routeNativeEvent(t.Context(), outbox, event)
	session.turnEvents = nil
	outbox.release(delivery)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	delivery = newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	session.turnEvents = delivery
	require.NoError(t, outbox.activate(delivery))
	session.routeNativeEvent(ctx, outbox, event)

	session.finishPromptForeground(delivery)
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{Method: "notify"})
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{ID: "stale", Method: uiMethodSelect})

	delivery = newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	session.turnEvents = delivery
	require.NoError(t, outbox.activate(delivery))

	dispatched := make(chan struct{})
	go func() {
		session.routeUIRequest(t.Context(), outbox, pi.UIRequest{Method: "notify"})
		close(dispatched)
	}()
	<-delivery.uiRequests
	<-dispatched

	client := newStubPiClient()
	close(client.events)
	close(client.uiRequests)
	done := make(chan struct{})
	session.pump(t.Context(), client, outbox, done)
	select {
	case <-done:
	default:
		t.Fatal("pump did not finish")
	}
}

// TestOutboxRoutesAnEventWithNoForegroundCycle pins that an event arriving
// between accepted prompts is routed rather than discarded: it still reaches the
// raw-event stream, which is what makes the generation's stream gap-detectable.
func TestOutboxRoutesAnEventWithNoForegroundCycle(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)

	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}

	message, err := pi.DecodeMessage([]byte(`{"type":"queue_update","steering":[],"followUp":[]}`))
	require.NoError(t, err)
	require.NotNil(t, message.Event)

	session.routeNativeEvent(t.Context(), newTestSessionOutbox(7), message.Event)

	require.Len(t, connection.notified, 1)
}

// TestOutboxRecordsSettlementOnlyForItsOwnGeneration pins the generation
// binding: a settlement observed on a delivery the session no longer holds
// fences nothing.
func TestOutboxRecordsSettlementOnlyForItsOwnGeneration(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler))), id: "id"}

	stale := newTurnDelivery()
	session.recordNativeSettlement(stale)
	require.False(t, session.turnNativeSettled)

	session.turnEvents = stale
	session.recordNativeSettlement(stale)
	require.True(t, session.turnNativeSettled)

	session.recordNativeSettlement(nil)
}

// TestEndedGenerationRefusesWithoutEndingSuccessorDelivery pins that process
// recovery, not a dead router, owns the delivery's eventual generation.
func TestEndedGenerationRefusesWithoutEndingSuccessorDelivery(t *testing.T) {
	outbox := newTestSessionOutbox(3)
	outbox.end()

	delivery := newTurnDelivery()
	require.ErrorIs(t, reserveOutboxPrompt(outbox, delivery), pi.ErrTransportClosed)

	select {
	case <-delivery.events:
		t.Fatal("the ended generation closed a successor delivery")
	default:
	}
	require.Nil(t, outbox.foreground())
}

func TestPumpPanicIsContainedAndEndsTurn(t *testing.T) {
	const secret = "pump-panic-secret-sentinel"

	var logs bytes.Buffer
	agent := NewAgent(WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	delivery := newTurnDelivery()
	outbox := newTestSessionOutbox(1)
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	require.NoError(t, outbox.activate(delivery))
	session := &agentSession{agent: agent, id: "id", turnEvents: delivery}
	client := newStubPiClient()
	client.eventsFunc = func() <-chan pi.Event {
		panic(secret)
	}
	done := make(chan struct{})

	session.pump(t.Context(), client, outbox, done)

	select {
	case <-done:
	default:
		t.Fatal("panicking pump did not finish")
	}
	require.NoError(t, outbox.producers.waitChildren(t.Context()))
	if _, ok := <-delivery.events; ok {
		t.Fatal("panicking pump left the turn event stream open")
	}
	if !strings.Contains(logs.String(), "session event pump") || strings.Contains(logs.String(), secret) {
		t.Fatalf("panic log = %q", logs.String())
	}
}

func TestStructuralClientFailureContainsCapturedGenerationWithoutTerminal(t *testing.T) {
	session, connection := lifecycleSession(t, false)
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))
	require.NoError(t, session.lifecycleAcceptTurn(t.Context(), testSubmission()))
	baseline := len(connection.notifications)

	failedProcess := newStubProcess(false)
	failedClient := newStubPiClient()
	failedClient.err = pi.ErrJSONLStructural
	close(failedClient.events)
	close(failedClient.uiRequests)
	failedOutbox := newTestSessionOutbox(1)
	bindTestRuntime(failedOutbox, failedProcess, failedClient, nil, nil, nil)

	successorProcess := newStubProcess(false)
	session.proc = successorProcess
	session.client = newStubPiClient()
	session.outbox = newTestSessionOutbox(2)
	session.pumpGeneration = 2

	done := make(chan struct{})
	session.pump(t.Context(), failedClient, failedOutbox, done)
	<-done
	containmentErr, contained := failedOutbox.awaitContainment()
	require.True(t, contained)
	require.NoError(t, containmentErr)
	require.Equal(t, 1, failedProcess.shutdownCalls)
	require.Equal(t, 1, failedProcess.closeCalls)
	require.Zero(t, successorProcess.shutdownCalls)
	require.Zero(t, successorProcess.closeCalls)
	require.Len(t, connection.notifications, baseline, "structural transport loss fabricated a lifecycle terminal")
}

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
	require.ErrorIs(t, err, ErrContainmentIncomplete)
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
	require.ErrorIs(t, missing.acceptEstablishment(), ErrContainmentIncomplete)

	// The gate names who owns the generation. A generation an owner already
	// claimed is retired; a gate this generation already released is the
	// single-release invariant, and only that one is fail-closed.
	for name, claim := range map[string]func(*sessionOutbox){
		"closing": func(o *sessionOutbox) { o.closing = true },
		"fenced":  func(o *sessionOutbox) { o.fenced = true },
		"ended":   func(o *sessionOutbox) { o.ended = true },
	} {
		retired := newSessionOutbox(1, newNativeBoundaryTracker())
		claim(retired)
		require.ErrorIsf(t, retired.acceptEstablishment(), errGenerationRetired, "claim %s", name)
		require.False(t, retired.openingAccepted, "a retired generation releases no gate")
	}

	released := newSessionOutbox(1, newNativeBoundaryTracker())
	require.NoError(t, released.acceptEstablishment())
	require.True(t, released.openingAccepted)
	require.ErrorIs(t, released.acceptEstablishment(), pi.ErrTransportClosed)
	require.ErrorIs(t, newTestSessionOutbox(1).acceptEstablishment(), pi.ErrTransportClosed)

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
	outbox.preAcceptance = []pi.Event{pi.QueueUpdateEvent{}}
	outbox.releasePromptAdmission(newTurnDelivery())
	require.Equal(t, outboxPromptPending, outbox.state, "a foreign delivery releases nothing")
	require.Len(t, outbox.preAcceptance, 1, "a foreign delivery keeps the pending admission's frames")
	outbox.releasePromptAdmission(admission)
	require.Equal(t, outboxIdle, outbox.state)
	require.Nil(t, outbox.preAcceptance, "an abandoned admission drops the frames it held")
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
	require.ErrorIs(t, err, ErrContainmentIncomplete)
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
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.True(t, cancelled)

	cancelled = false
	_, outbox, published := session.publishRuntimeGeneration(t.Context(), func() { cancelled = true }, nil, nil, nil)
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
	want := errors.Join(ErrContainmentIncomplete, errors.New("owner incomplete"))
	outbox.finishContainment(containment, want)
	require.NoError(t, outbox.producers.waitChildren(t.Context()))
	require.ErrorIs(t, session.nativeContainmentError(), ErrContainmentIncomplete)
}

func TestContainmentAndNativeBoundaryFailureEdges(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	session.containGeneration(t.Context(), nil, "missing")
	require.ErrorIs(t, session.containGenerationSync(t.Context(), nil, "missing"), ErrContainmentIncomplete)

	outbox := newTestSessionOutbox(1)
	outbox.producers.releaseRoot()
	require.ErrorIs(t, session.containGenerationSync(t.Context(), outbox, "closed admission"), ErrContainmentIncomplete)

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
	require.ErrorIs(t, tracker.run(t.Context(), "missing", func() error { return nil }), ErrContainmentIncomplete)
	require.ErrorIs(t, tracker.retainedIncomplete(), ErrContainmentIncomplete)
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
	require.ErrorIs(t, tracker.awaitPrior(cancelled, "close", blocked), ErrContainmentIncomplete)
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
		require.ErrorIs(t, <-done, ErrContainmentIncomplete)
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
		require.ErrorIs(t, session.stopPumpBounded(ctx), ErrContainmentIncomplete)
	})

	t.Run("producer misses bound after pump", func(t *testing.T) {
		outbox := newTestSessionOutbox(1)
		done := make(chan struct{})
		close(done)
		session := &agentSession{pumpDone: done, outbox: outbox}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, session.stopPumpBounded(ctx), ErrContainmentIncomplete)
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
	require.ErrorIs(t, err, ErrContainmentIncomplete)
}

func TestPromptAcceptanceReturnsExistingGenerationQuarantine(t *testing.T) {
	session, _ := lifecycleSession(t, false)
	outbox := newTestSessionOutbox(1)
	delivery := newTurnDelivery()
	session.outbox = outbox
	session.turnEvents = delivery
	session.lc.generation = outbox.generation
	session.lc.quarantineErr = errors.Join(ErrContainmentIncomplete, errors.New("retained"))

	err := session.acceptPromptResponse(t.Context(), outbox, delivery, testSubmission())
	require.ErrorIs(t, err, ErrContainmentIncomplete)
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
