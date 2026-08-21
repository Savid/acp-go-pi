package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
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
