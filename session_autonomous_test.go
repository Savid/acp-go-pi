package piacp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

// recordingClient is directAgentClient with the locking a settlement running
// beside the pump requires, plus a hook that fails a chosen notification so a
// delivery failure can be tested without breaking every other emission.
type recordingClient struct {
	mu            sync.Mutex
	notifications []acp.SessionNotification
	notified      []map[string]any
	trace         []string
	lifecycleSeen int
	// lifecycleFailAfter fails every lifecycle-bearing notification past the
	// given count. Zero means none fail.
	lifecycleFailAfter int
	failOrdinary       bool
	failureErr         error
	done               chan struct{}
}

type restoreMutationContext struct {
	context.Context //nolint:containedctx // The test wrapper mutates ownership at the second admission check.
	mutate          func()
}

func (c restoreMutationContext) Err() error {
	if c.mutate != nil {
		c.mutate()
	}

	return nil
}

func TestAutonomousSettlementAndRestoreOwnershipEdges(t *testing.T) {
	session := &agentSession{agent: NewAgent(WithLogger(slog.New(slog.DiscardHandler)))}
	cycle := &agentCycle{state: &promptTurnState{}}

	closedProducers := newTestSessionOutbox(1)
	closedProducers.producers.releaseRoot()
	session.settleAgentCycle(t.Context(), closedProducers, cycle)

	wrongState := newTestSessionOutbox(2)
	session.settleAgentCycle(t.Context(), wrongState, cycle)
	require.NoError(t, wrongState.producers.waitChildren(t.Context()))

	limitVerdict := agentCycleVerdict(&agentCycle{state: &promptTurnState{stopReason: stopReasonLength}})
	require.Equal(t, lifecycle.OutcomeLimit, limitVerdict.outcome)
	require.Equal(t, string(acp.StopReasonMaxTokens), limitVerdict.stopReason)
	cancelledVerdict := agentCycleVerdict(&agentCycle{state: &promptTurnState{stopReason: stopReasonAborted}})
	require.Equal(t, lifecycle.OutcomeCancelled, cancelledVerdict.outcome)
	require.Equal(t, string(acp.StopReasonCancelled), cancelledVerdict.stopReason)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := session.beginRestore(cancelled)
	require.ErrorIs(t, err, context.Canceled)

	session.closing = true
	_, err = session.beginRestore(t.Context())
	requireInvalidParams(t, err)
	session.closing = false
	session.poisonCause = "poisoned"
	_, err = session.beginRestore(t.Context())
	require.Error(t, err)
	session.poisonCause = ""
	session.turn = make(chan struct{}, 1)
	session.turn <- struct{}{}
	_, err = session.beginRestore(t.Context())
	requireInvalidRequest(t, err)

	for _, testCase := range []struct {
		name   string
		mutate func(*agentSession)
	}{
		{name: "close wins after admission check", mutate: func(s *agentSession) { s.closing = true }},
		{name: "poison wins after admission check", mutate: func(s *agentSession) { s.poisonCause = "late" }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			late := &agentSession{agent: NewAgent(), outbox: newTestSessionOutbox(3)}
			ctx := restoreMutationContext{Context: t.Context(), mutate: func() {
				late.mu.Lock()
				testCase.mutate(late)
				late.mu.Unlock()
			}}
			_, err := late.beginRestore(ctx)
			require.Error(t, err)
		})
	}
}

func newRecordingClient() *recordingClient {
	return &recordingClient{done: make(chan struct{})}
}

func (c *recordingClient) Done() <-chan struct{} { return c.done }

func (*recordingClient) CreateElicitation(
	ctx context.Context,
	_ acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	acknowledgeActionRequestWrite(ctx, nil)

	return acp.UnstableCreateElicitationResponse{}, nil
}

func (*recordingClient) RequestPermission(
	ctx context.Context,
	_ acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	acknowledgeActionRequestWrite(ctx, nil)

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *recordingClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, carries := notification.Meta[lifecycleMetaKey]; carries {
		c.lifecycleSeen++

		if c.lifecycleFailAfter > 0 && c.lifecycleSeen > c.lifecycleFailAfter {
			if c.failureErr != nil {
				return c.failureErr
			}

			return errors.New("lifecycle notification refused")
		}

		envelope, _ := notification.Meta[lifecycleMetaKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		eventType, _ := event["type"].(string)
		c.trace = append(c.trace, "lifecycle:"+eventType)
	} else if c.failOrdinary {
		if c.failureErr != nil {
			return c.failureErr
		}

		return errors.New("ordinary notification refused")
	} else {
		c.trace = append(c.trace, "typed")
	}

	c.notifications = append(c.notifications, notification)

	return nil
}

func (c *recordingClient) NotifyExtension(_ context.Context, _ string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	decoded := map[string]any{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.notified = append(c.notified, decoded)
	c.trace = append(c.trace, "raw")

	return nil
}

func (c *recordingClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.notifications)
}

func (c *recordingClient) snapshot() []acp.SessionNotification {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.SessionNotification(nil), c.notifications...)
}

// lifecycleEvents decodes the lifecycle envelopes the client accepted, in
// arrival order.
func (c *recordingClient) lifecycleEvents(t *testing.T) []map[string]any {
	t.Helper()

	events := make([]map[string]any, 0)

	for _, notification := range c.snapshot() {
		envelope, carries := notification.Meta[lifecycleMetaKey]
		if !carries {
			continue
		}

		events = append(events, anyMap(t, anyMap(t, envelope)["event"]))
	}

	return events
}

// markingStore records how many notifications had been delivered at the moment
// of each durable append, which is how a test states an ordering between the
// store and the wire rather than inspecting timing.
type markingStore struct {
	SessionStore

	mu    sync.Mutex
	marks []int
	mark  func() int
	// failSubpath fails every append against one store subpath, which is how a
	// boundary commit is failed without failing the native mirror beside it.
	failSubpath  string
	panicMain    bool
	panicSubpath string
	panicValue   string
	failureErr   error
	blockSubpath string
	blockAppend  bool
	blockEntered chan struct{}
	blockRelease chan struct{}
	blockOnce    sync.Once
}

func (s *markingStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	s.mu.Lock()
	s.marks = append(s.marks, s.mark())
	failSubpath := s.failSubpath
	panicMain := s.panicMain
	panicSubpath := s.panicSubpath
	panicValue := s.panicValue
	failureErr := s.failureErr
	blockSubpath := s.blockSubpath
	blockAppend := s.blockAppend
	blockEntered := s.blockEntered
	blockRelease := s.blockRelease
	s.mu.Unlock()
	if blockAppend && key.Subpath == blockSubpath {
		s.blockOnce.Do(func() {
			close(blockEntered)
			<-blockRelease
		})
	}

	if (panicMain && key.Subpath == SessionStoreMainSubpath) ||
		(panicSubpath != "" && key.Subpath == panicSubpath) {
		panic(panicValue)
	}

	if failSubpath != "" && key.Subpath == failSubpath {
		if failureErr != nil {
			return failureErr
		}

		return errors.New("append refused")
	}

	return s.SessionStore.Append(ctx, key, entries)
}

func (s *markingStore) lastMark() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.marks) == 0 {
		return -1
	}

	return s.marks[len(s.marks)-1]
}

// agentCycleFixture is a session wired for the between-prompt path: a live
// lifecycle incarnation on generation one, a native session file to mirror, and
// a store that records the wire position of every durable append. Its records
// are routed on the test goroutine, so every ordering it pins is the router's
// rather than the scheduler's.
type agentCycleFixture struct {
	session *agentSession
	client  *recordingClient
	store   *markingStore
	outbox  *sessionOutbox
	logs    *strings.Builder
	pi      *stubPiClient
	process *stubProcess
}

func newAgentCycleFixture(t *testing.T) *agentCycleFixture {
	t.Helper()

	fixture := newAgentCycleSession(t)
	fixture.outbox = newTestSessionOutbox(1)
	bindTestRuntime(fixture.outbox, fixture.process, fixture.pi, nil, nil, nil)
	fixture.session.outbox = fixture.outbox
	fixture.session.pumpGeneration = 1

	require.NoError(t, fixture.session.openLifecycleStream(t.Context(), 1))

	return fixture
}

// newAgentCycleSession builds the session without choosing how its records
// arrive, so a live pump and a directly routed fixture share one wiring.
func newAgentCycleSession(t *testing.T) *agentCycleFixture {
	t.Helper()

	client := newRecordingClient()
	store := &markingStore{SessionStore: NewInMemorySessionStore(), mark: client.count}
	logs := &strings.Builder{}

	agent := NewAgent(
		testContainmentOption(),
		WithSessionStore(store),
		WithLogger(slog.New(slog.NewTextHandler(logs, nil))),
	)
	agent.conn = client
	agent.lifecycle = lifecycle.Negotiated{
		Version:              1,
		UpdatesOutsidePrompt: true,
		ActivityKinds:        []lifecycle.ActivityKind{},
	}

	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))

	piClient := newStubPiClient()
	process := newStubProcess(false)

	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          piClient,
		proc:            process,
		sessionFilePath: path,
	}

	return &agentCycleFixture{session: session, client: client, store: store, logs: logs, pi: piClient, process: process}
}

func (f *agentCycleFixture) route(t *testing.T, events ...pi.Event) {
	t.Helper()

	for _, event := range events {
		f.session.routeNativeEvent(t.Context(), f.outbox, event)
	}
}

// quiesce waits for every settlement and containment the routed records
// started. Both run beside the pump on purpose, so a test that asserts their
// result joins them explicitly.
func (f *agentCycleFixture) quiesce() {
	// The containment owner closes its attempt only after lifecycle fencing and
	// poison reporting have completed.
	_, _ = f.outbox.awaitContainment()

	if err := f.outbox.producers.waitChildren(context.Background()); err != nil {
		panic(err)
	}
}

func (f *agentCycleFixture) settle(t *testing.T) {
	t.Helper()

	f.route(t, pi.AgentSettledEvent{})
	f.quiesce()
}

func (f *agentCycleFixture) boundaries(t *testing.T) []lifecycleBoundaryRecord {
	t.Helper()

	entries, err := f.store.Load(t.Context(), SessionKey{SessionID: "id", Subpath: SessionStoreLifecycleSubpath})
	require.NoError(t, err)

	records := make([]lifecycleBoundaryRecord, 0, len(entries))

	for _, entry := range entries {
		record, decodeErr := decodeLifecycleBoundaryRecord(entry)
		require.NoError(t, decodeErr)
		records = append(records, record)
	}

	return records
}

func (f *agentCycleFixture) lifecycleFenced() bool {
	f.session.lcMu.Lock()
	defer f.session.lcMu.Unlock()

	return f.session.lc.fenced
}

func (f *agentCycleFixture) trackedTools() []string {
	f.session.toolMu.Lock()
	defer f.session.toolMu.Unlock()

	names := make([]string, 0, len(f.session.turnTools))
	for id := range f.session.turnTools {
		names = append(names, id)
	}

	return names
}

func assistantWork() []pi.Event {
	return []pi.Event{
		pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant, Model: "m", Provider: "p"}},
		pi.MessageUpdateEvent{AssistantMessageEvent: pi.AssistantMessageEvent{
			Type: assistantEventTextDelta, Delta: "between prompts",
		}},
		pi.ToolExecutionStartEvent{ToolCallID: "tool-1", ToolName: toolNameRead},
		pi.ToolExecutionEndEvent{ToolCallID: "tool-1", Result: &pi.ToolResult{
			Content: []pi.ContentBlock{{Type: pi.ContentBlockTypeText, Text: "read"}},
		}},
		pi.MessageEndEvent{Message: pi.AgentMessage{
			Role:         messageRoleAssistant,
			ACPMessageID: "message-1",
			StopReason:   stopReasonStop,
			Usage:        &pi.Usage{Input: 11, Output: 7},
		}},
	}
}

// TestAgentOriginCycleOpensStreamsAndSettles pins the whole between-prompt path:
// agent_start is the only opener, ordinary output maps under it with no borrowed
// route, and the settle marker commits the durable boundary before the terminal
// idle the host reduces.
func TestAgentOriginCycleOpensStreamsAndSettles(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	fixture.settle(t)

	events := fixture.client.lifecycleEvents(t)
	require.Len(t, events, 3, "an opening snapshot, one running transition, one terminal idle")

	for _, event := range events {
		require.NotEqual(t, string(lifecycle.EventPromptAccepted), event["type"],
			"an agent-origin cycle states no acceptance")
		require.NotContains(t, event, "submissionId")
		require.NotContains(t, event, "runId")
	}

	running := events[1]
	require.Equal(t, string(lifecycle.EventStateUpdate), running["type"])
	require.Equal(t, string(lifecycle.ForegroundRunning), running["state"])
	require.Equal(t, string(lifecycle.CauseActivity), running["cause"])
	require.NotEmpty(t, running["turnId"])

	idle := events[2]
	require.Equal(t, string(lifecycle.ForegroundIdle), idle["state"])
	require.Equal(t, string(lifecycle.CauseActivity), idle["cause"])
	require.Equal(t, string(lifecycle.OutcomeSuccess), idle["outcome"])
	require.Equal(t, lifecycle.StopReasonEndTurn, idle["stopReason"])
	require.Equal(t, running["turnId"], idle["turnId"])
	require.Equal(t, running["cycleId"], idle["cycleId"])

	rows, err := fixture.store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Len(t, rows, 2, "the cycle's native rows are mirrored")

	records := fixture.boundaries(t)
	require.Len(t, records, 1)
	require.Equal(t, nativeStateCommitted, records[0].NativeState)
	require.Equal(t, string(lifecycle.OutcomeSuccess), records[0].Outcome)
	require.Equal(t, running["turnId"], records[0].TurnID)

	idlePosition := -1

	for index, notification := range fixture.client.snapshot() {
		if _, carries := notification.Meta[lifecycleMetaKey]; carries {
			idlePosition = index
		}
	}

	require.Positive(t, idlePosition)
	require.GreaterOrEqual(t, idlePosition, fixture.store.lastMark(),
		"the terminal idle was not on the wire when its boundary was committed")
}

// TestAgentOriginCycleProjectsOrdinaryUpdatesWithoutARoute pins that
// between-prompt output reaches the host as ordinary ACP updates and never
// borrows a prompt's route envelope.
func TestAgentOriginCycleProjectsOrdinaryUpdatesWithoutARoute(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.session.turnNonce = "a-prompt-that-is-not-running"

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	fixture.settle(t)

	var chunks, tools int

	for _, notification := range fixture.client.snapshot() {
		require.NotContains(t, notification.Meta, routeMetaKey,
			"an agent-origin update names no turn route")

		switch {
		case notification.Update.AgentMessageChunk != nil:
			chunks++
		case notification.Update.ToolCall != nil, notification.Update.ToolCallUpdate != nil:
			tools++
		}
	}

	require.Equal(t, 1, chunks)
	require.Equal(t, 2, tools, "the tool call starts and reaches its terminal status")
}

// TestAgentOriginCycleEmitsNoActivityUpdate pins the negotiated answer: this
// configuration proves no activity kind, so a cycle it opens is foreground and
// never an activity entity.
func TestAgentOriginCycleEmitsNoActivityUpdate(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	fixture.settle(t)

	for _, event := range fixture.client.lifecycleEvents(t) {
		require.NotEqual(t, string(lifecycle.EventActivityUpdate), event["type"])
	}
}

// TestExtensionErrorDrainsToTheNativeSettleMarker pins the terminal authority: a
// cycle whose extension surface threw is failed but not ended, because only pi's
// own marker says the work stopped. The whole ladder that follows still runs,
// and exactly one terminal idle lands — a failed one, after the marker.
func TestExtensionErrorDrainsToTheNativeSettleMarker(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, pi.ExtensionErrorEvent{ExtensionPath: "/private/ext.ts", Error: "boom"})

	require.NotNil(t, fixture.outbox.currentCycle(), "the failed cycle is still the open one")

	for _, event := range fixture.client.lifecycleEvents(t) {
		require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"],
			"no terminal state precedes pi's own settle marker")
	}

	// The cycle keeps streaming after the failure, exactly as pi keeps running.
	fixture.route(t, assistantWork()...)

	for _, event := range fixture.client.lifecycleEvents(t) {
		require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"])
	}

	fixture.settle(t)

	events := fixture.client.lifecycleEvents(t)

	var idles int

	for _, event := range events {
		if event["state"] == string(lifecycle.ForegroundIdle) {
			idles++
		}
	}

	require.Equal(t, 1, idles, "exactly one terminal idle, and only after the marker")

	idle := events[len(events)-1]
	require.Equal(t, string(lifecycle.ForegroundIdle), idle["state"])
	require.Equal(t, string(lifecycle.CauseActivity), idle["cause"])
	require.Equal(t, string(lifecycle.OutcomeFailed), idle["outcome"])
	require.NotContains(t, idle, "stopReason", "no ACP v1 stop reason names a failure")

	records := fixture.boundaries(t)
	require.Len(t, records, 1)
	require.Equal(t, string(lifecycle.OutcomeFailed), records[0].Outcome)
	require.Equal(t, nativeStateCommitted, records[0].NativeState)
	require.NotContains(t, records[0].Detail, "ext.ts")
	require.Nil(t, fixture.outbox.currentCycle())
	require.NoError(t, fixture.session.poisonedError(), "a failed cycle is contained, not a poisoned session")
	require.NotContains(t, fixture.logs.String(), "ext.ts")
}

// TestExtensionErrorInsideAForegroundTurnFailsItClosed pins the same rule for a
// client turn, with the native path and thrown text withheld. A prompt's own
// failure path ends its incarnation, which is the containment an agent-origin
// cycle reaches through pi's marker instead.
func TestExtensionErrorInsideAForegroundTurnFailsItClosed(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	settled, err := fixture.session.handleTurnEvent(t.Context(), pi.ExtensionErrorEvent{
		ExtensionPath: "/private/pi/ext/permission-bridge.ts",
		Error:         "TypeError: undefined",
	}, &promptTurnState{})

	require.False(t, settled)
	require.Error(t, err)
	require.Contains(t, err.Error(), failureCauseExtension)
	require.Contains(t, err.Error(), extensionFailureMessage)
	require.NotContains(t, err.Error(), "permission-bridge")
	require.NotContains(t, err.Error(), "TypeError")
}

// TestAgentStartAndPromptReserveAreOneTransition pins the admission barrier.
// Classifying the opener and owning the cycle happen under one lock, so the
// instant the router has seen agent_start a prompt can no longer reserve — there
// is no interval in which the lifecycle stream is being opened and the
// foreground is still free.
func TestAgentStartAndPromptReserveAreOneTransition(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	admitted := fixture.outbox.admit(pi.AgentStartEvent{})
	require.Equal(t, outboxOpenCycle, admitted.disposition)
	require.NotNil(t, admitted.cycle)

	// Ownership is already established, before a single lifecycle event exists.
	require.Same(t, admitted.cycle, fixture.outbox.currentCycle())
	require.Empty(t, fixture.client.lifecycleEvents(t)[1:], "nothing has been stated yet")

	racing := newTurnDelivery()
	requireInvalidRequest(t, reserveOutboxPrompt(fixture.outbox, racing))
	require.True(t, fixture.session.agentWorkPending())

	fixture.session.openAgentCycle(t.Context(), fixture.outbox, admitted.cycle)

	running := fixture.client.lifecycleEvents(t)[1]
	require.Equal(t, string(lifecycle.CauseActivity), running["cause"])
	require.Equal(t, running["turnId"], admitted.cycle.turnID)
}

// TestPromptReservationFailsClosedOnPreAckCycleWork pins the causal side of
// prompt acceptance. The command response is the first record that can make
// later work belong to the prompt; a record that says the agent loop is already
// running is ambiguous before it and contains the generation instead of being
// replayed with the prompt's route, action authority, or lifecycle identity.
func TestPromptReservationFailsClosedOnPreAckCycleWork(t *testing.T) {
	preAck := map[string]pi.Event{
		"agent opener":      pi.AgentStartEvent{},
		"message":           pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant}},
		"tool":              pi.ToolExecutionStartEvent{ToolCallID: "pre-ack", ToolName: toolNameRead},
		"turn bracket":      pi.TurnStartEvent{},
		"agent bracket":     pi.AgentEndEvent{},
		"settlement":        pi.AgentSettledEvent{},
		"extension failure": pi.ExtensionErrorEvent{ExtensionPath: "/private/ext.ts", Error: "secret"},
	}

	for name, event := range preAck {
		t.Run(name, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			fixture.session.rawMessages = rawMessageConfig{All: true}
			fixture.session.turnNonce = "not-yet-accepted"

			delivery := newTurnDelivery()
			require.NoError(t, fixture.session.reservePromptForeground(delivery))

			fixture.route(t, event)
			fixture.quiesce()

			require.Error(t, fixture.session.poisonedError())
			require.True(t, fixture.lifecycleFenced())
			require.Nil(t, fixture.outbox.currentCycle())
			require.Positive(t, fixture.process.shutdownCalls)
			require.Positive(t, fixture.process.closeCalls)

			_, open := <-delivery.events
			require.False(t, open, "the reserved prompt receives no pre-ack work")

			for _, notification := range fixture.client.snapshot() {
				require.NotContains(t, notification.Meta, routeMetaKey)
			}
			for _, emitted := range fixture.client.lifecycleEvents(t) {
				require.Equal(t, string(lifecycle.EventSnapshot), emitted["type"],
					"pre-ack work receives no lifecycle identity")
			}
		})
	}
}

// TestPromptReservationHoldsPiOwnPreAcceptanceWork pins the other side. pi
// decides whether the transcript must be compacted before it answers the prompt
// command, so an auto-compaction pass, its summarization retries, the queue
// report, and whatever an extension journals from the compaction hooks all
// arrive while the reservation stands. None of it says the agent loop is
// running, so it is held for the acceptance boundary rather than ending the
// incarnation the prompt is about to use.
func TestPromptReservationHoldsPiOwnPreAcceptanceWork(t *testing.T) {
	preAck := []pi.Event{
		pi.QueueUpdateEvent{},
		pi.CompactionStartEvent{Reason: "threshold"},
		pi.UnknownEvent{EventType: "summarization_retry_scheduled"},
		pi.UnknownEvent{EventType: "summarization_retry_attempt_start"},
		pi.UnknownEvent{EventType: "summarization_retry_finished"},
		pi.CompactionEndEvent{Reason: "threshold"},
		pi.UnknownEvent{EventType: "entry_appended"},
		pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleCustom}},
		pi.MessageEndEvent{Message: pi.AgentMessage{Role: messageRoleCustom}},
		pi.UnknownEvent{EventType: "thinking_level_changed"},
	}

	fixture := newAgentCycleFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}
	fixture.session.turnNonce = "not-yet-accepted"

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))

	fixture.route(t, preAck...)
	fixture.quiesce()

	require.NoError(t, fixture.session.poisonedError())
	require.False(t, fixture.lifecycleFenced())
	require.Zero(t, fixture.process.shutdownCalls)
	require.Nil(t, fixture.outbox.currentCycle(), "pi's own pre-prompt work opens no cycle")

	fixture.outbox.mu.Lock()
	held := len(fixture.outbox.preAcceptance)
	fixture.outbox.mu.Unlock()
	require.Len(t, preAck, held, "every pre-acceptance record waits for the acceptance boundary")

	require.Empty(t, fixture.client.notified, "a held frame reaches no raw subscriber")

	for _, emitted := range fixture.client.lifecycleEvents(t) {
		require.Equal(t, string(lifecycle.EventSnapshot), emitted["type"],
			"pi's own pre-prompt work states no lifecycle identity")
	}
}

// TestAgentCycleMappingFailureStopsNativeGeneration pins the first settlement
// rung: output that cannot be projected is already an incomplete host view, so
// the exact process stops immediately and no idle claims the cycle succeeded.
func TestAgentCycleMappingFailureStopsNativeGeneration(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.route(t, pi.AgentStartEvent{})

	fixture.client.mu.Lock()
	fixture.client.failOrdinary = true
	fixture.client.mu.Unlock()
	fixture.route(t, pi.MessageUpdateEvent{AssistantMessageEvent: pi.AssistantMessageEvent{
		Type: assistantEventTextDelta, Delta: "cannot deliver",
	}})
	fixture.quiesce()

	require.Error(t, fixture.session.poisonedError())
	require.True(t, fixture.lifecycleFenced())
	require.Positive(t, fixture.process.shutdownCalls)
	require.Positive(t, fixture.process.closeCalls)
	for _, event := range fixture.client.lifecycleEvents(t) {
		require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"])
	}
}

func TestAgentCycleFailuresDoNotLogOpaqueErrors(t *testing.T) {
	const secret = "agent-cycle-opaque-error-secret-sentinel"

	for name, fail := range map[string]func(*agentCycleFixture, error){
		"open": func(f *agentCycleFixture, err error) {
			f.client.mu.Lock()
			f.client.lifecycleFailAfter = f.client.lifecycleSeen
			f.client.failureErr = err
			f.client.mu.Unlock()
			f.route(t, pi.AgentStartEvent{})
		},
		"projection": func(f *agentCycleFixture, err error) {
			f.route(t, pi.AgentStartEvent{})
			f.client.mu.Lock()
			f.client.failOrdinary = true
			f.client.failureErr = err
			f.client.mu.Unlock()
			f.route(t, pi.MessageUpdateEvent{AssistantMessageEvent: pi.AssistantMessageEvent{
				Type: assistantEventTextDelta, Delta: "cannot deliver",
			}})
		},
		"settlement": func(f *agentCycleFixture, err error) {
			f.store.mu.Lock()
			f.store.failSubpath = SessionStoreLifecycleSubpath
			f.store.failureErr = err
			f.store.mu.Unlock()
			f.route(t, pi.AgentStartEvent{}, pi.AgentSettledEvent{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			fail(fixture, errors.New(secret))
			fixture.quiesce()

			require.Error(t, fixture.session.poisonedError())
			require.True(t, fixture.lifecycleFenced())
			require.NotContains(t, fixture.logs.String(), secret)
		})
	}
}

// TestPromptTeardownFinishesBeforeItReleasesTheRouter pins the teardown order. A
// release wakes the pump into a drain that can open an agent-origin cycle
// immediately, so this turn's route, delivery, and tool tracker are all finished
// first; a tool that cycle starts is not erased by the turn that just ended.
func TestPromptTeardownFinishesBeforeItReleasesTheRouter(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	require.NoError(t, fixture.outbox.activate(delivery))

	fixture.session.mu.Lock()
	fixture.session.turnNonce = "live-turn"
	fixture.session.mu.Unlock()

	require.NoError(t, fixture.session.publishNativeToolStart(t.Context(), pi.ToolExecutionStartEvent{
		ToolCallID: "tool-prompt", ToolName: toolNameRead,
	}))

	read := make(chan pi.Event, 1)

	go func() { read <- <-delivery.events }()

	fixture.route(t, pi.AgentSettledEvent{})
	require.IsType(t, pi.AgentSettledEvent{}, <-read)

	// The agent begins its own work in the same breath the prompt is ending.
	fixture.route(t, pi.AgentStartEvent{}, pi.ToolExecutionStartEvent{
		ToolCallID: "tool-agent", ToolName: toolNameRead,
	})

	// The activation already woke the router once. Clearing that token is what
	// makes the wake below the release's own.
	select {
	case <-fixture.outbox.drain:
	default:
	}

	drained := make(chan struct{})

	go func() {
		defer close(drained)

		<-fixture.outbox.drain
		fixture.session.drainOutbox(context.WithoutCancel(t.Context()), fixture.outbox)
	}()

	fixture.session.finishPromptForeground(delivery)
	<-drained

	require.Equal(t, []string{"tool-agent"}, fixture.trackedTools(),
		"the drain the release woke found a reset tracker and kept its own tool")

	fixture.session.mu.Lock()
	nonce := fixture.session.turnNonce
	events := fixture.session.turnEvents
	fixture.session.mu.Unlock()

	require.Empty(t, nonce, "the route is cleared before the router is released")
	require.Nil(t, events)
}

// TestForegroundSettlementRetainsLaterRecordsWithoutDeadlock pins the settling
// state: the sentinel reaches the prompt, every record behind it is retained in
// arrival order rather than sent to a delivery nobody reads, and the drain that
// follows the prompt's release opens the cycle those records belong to.
func TestForegroundSettlementRetainsLaterRecordsWithoutDeadlock(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	require.NoError(t, fixture.outbox.activate(delivery))

	read := make(chan pi.Event, 1)

	go func() { read <- <-delivery.events }()

	fixture.route(t, pi.AgentSettledEvent{})
	require.IsType(t, pi.AgentSettledEvent{}, <-read)

	// Every record behind the sentinel is retained: the send would otherwise
	// block on a prompt loop that has already stopped reading.
	fixture.route(t, pi.AgentStartEvent{}, pi.MessageUpdateEvent{AssistantMessageEvent: pi.AssistantMessageEvent{
		Type: assistantEventTextDelta, Delta: "late",
	}})

	require.True(t, fixture.outbox.agentBusy())
	require.Nil(t, fixture.outbox.currentCycle(), "a settling foreground opens no cycle")

	// A prompt cannot claim the foreground while records are retained, so the
	// first late record can never be attached to the next turn.
	require.Error(t, reserveOutboxPrompt(fixture.outbox, newTurnDelivery()))

	fixture.session.finishPromptForeground(delivery)
	fixture.session.drainOutbox(t.Context(), fixture.outbox)

	require.NotNil(t, fixture.outbox.currentCycle(), "the retained opener opens its own cycle")

	events := fixture.client.lifecycleEvents(t)
	running := events[len(events)-1]
	require.Equal(t, string(lifecycle.CauseActivity), running["cause"])
}

// TestPromptAdmissionIsRefusedWhileTheAgentOwnsWork pins that agent-origin work
// and a native queue pi has not drained both hold back a client turn.
func TestPromptAdmissionIsRefusedWhileTheAgentOwnsWork(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	release, err := fixture.session.acquireTurn(t.Context())
	require.NoError(t, err)
	release()

	fixture.route(t, pi.AgentStartEvent{})

	_, err = fixture.session.acquireTurn(t.Context())
	requireInvalidRequest(t, err)
	require.Contains(t, err.Error(), "backpressure")

	fixture.settle(t)

	release, err = fixture.session.acquireTurn(t.Context())
	require.NoError(t, err)
	release()

	fixture.route(t, pi.QueueUpdateEvent{Steering: []string{"one", "two"}})

	_, err = fixture.session.acquireTurn(t.Context())
	requireInvalidRequest(t, err)
}

// TestNativeQueueIsConsumedWithoutExposingItsEntries pins the queue's secrecy:
// its size is structural state this session acts on, and its entries are user
// text that reaches no notification and no log.
func TestNativeQueueIsConsumedWithoutExposingItsEntries(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	const secret = "deploy-the-production-database"

	fixture.route(t, pi.QueueUpdateEvent{Steering: []string{secret}, FollowUp: []string{secret + "-again"}})

	require.False(t, fixture.outbox.nativeQueueDrained())
	require.True(t, fixture.session.agentWorkPending())
	require.NoError(t, fixture.session.poisonedError(), "a queue report bears no work and fences nothing")

	for _, notification := range fixture.client.snapshot() {
		encoded, err := json.Marshal(notification)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), secret)
	}

	require.Empty(t, fixture.client.notified, "raw events are opt-in and stay off by default")
	require.NotContains(t, fixture.logs.String(), secret)

	fixture.route(t, pi.QueueUpdateEvent{})
	require.True(t, fixture.outbox.nativeQueueDrained())
	require.False(t, fixture.session.agentWorkPending())
}

// TestNativeQueueAdmissionIsAtomicWithPromptAndRestore drives both sides of the
// outbox lock with explicit barriers. Whichever transition wins is observed as
// a whole: a prior non-empty queue refuses admission, while an admitted prompt
// or restore owns the foreground before the later report lands. Once non-empty,
// admission remains refused until an ordered zero-depth report is admitted.
func TestNativeQueueAdmissionIsAtomicWithPromptAndRestore(t *testing.T) {
	type admission struct {
		name    string
		acquire func(*sessionOutbox) (func(), bool)
	}

	admissions := []admission{
		{
			name: "prompt reservation",
			acquire: func(outbox *sessionOutbox) (func(), bool) {
				delivery := newTurnDelivery()
				if err := reserveOutboxPrompt(outbox, delivery); err != nil {
					return nil, false
				}

				return func() { outbox.release(delivery) }, true
			},
		},
		{
			name: "restore",
			acquire: func(outbox *sessionOutbox) (func(), bool) {
				if !outbox.beginRestore() {
					return nil, false
				}

				return outbox.finishRestore, true
			},
		},
	}

	for _, admission := range admissions {
		t.Run(admission.name+"/queue wins", func(t *testing.T) {
			outbox := newTestSessionOutbox(1)
			queueGate := make(chan struct{})
			queueDone := make(chan struct{})
			admitGate := make(chan struct{})
			admitDone := make(chan bool, 1)

			go func() {
				<-queueGate
				outbox.admit(pi.QueueUpdateEvent{Steering: []string{"secret"}})
				close(queueDone)
			}()
			go func() {
				<-admitGate
				_, ok := admission.acquire(outbox)
				admitDone <- ok
			}()

			close(queueGate)
			<-queueDone
			close(admitGate)
			require.False(t, <-admitDone)
			require.False(t, outbox.nativeQueueDrained())

			_, admitted := admission.acquire(outbox)
			require.False(t, admitted, "non-empty depth remains an admission fence")
			outbox.admit(pi.QueueUpdateEvent{FollowUp: []string{"still secret"}})
			_, admitted = admission.acquire(outbox)
			require.False(t, admitted, "another non-empty report does not clear the fence")

			outbox.admit(pi.QueueUpdateEvent{})
			release, admitted := admission.acquire(outbox)
			require.True(t, admitted, "only the ordered zero-depth report clears the fence")
			release()
		})

		t.Run(admission.name+"/admission wins", func(t *testing.T) {
			outbox := newTestSessionOutbox(1)
			admitGate := make(chan struct{})
			admitDone := make(chan struct {
				release func()
				ok      bool
			}, 1)
			queueGate := make(chan struct{})
			queueDone := make(chan struct{})

			go func() {
				<-admitGate
				release, ok := admission.acquire(outbox)
				admitDone <- struct {
					release func()
					ok      bool
				}{release: release, ok: ok}
			}()
			go func() {
				<-queueGate
				outbox.admit(pi.QueueUpdateEvent{Steering: []string{"secret"}})
				close(queueDone)
			}()

			close(admitGate)
			winner := <-admitDone
			require.True(t, winner.ok)
			close(queueGate)
			<-queueDone
			winner.release()
			for {
				_, _, ok := outbox.popRetained()
				if !ok {
					break
				}
			}

			_, admitted := admission.acquire(outbox)
			require.False(t, admitted, "the later non-empty report fences the next admission")
			outbox.admit(pi.QueueUpdateEvent{})
			release, admitted := admission.acquire(outbox)
			require.True(t, admitted)
			release()
		})
	}
}

// TestSettleWithANonEmptyQueueFencesTheGeneration pins that a settle marker
// contradicted by a queue pi has not drained is never believed.
func TestSettleWithANonEmptyQueueFencesTheGeneration(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, pi.QueueUpdateEvent{Steering: []string{"still pending"}})
	fixture.route(t, pi.AgentSettledEvent{})
	fixture.quiesce()

	require.Error(t, fixture.session.poisonedError())
	require.Empty(t, fixture.boundaries(t), "a contradicted marker settles nothing")

	for _, event := range fixture.client.lifecycleEvents(t) {
		require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"])
	}
}

// TestOrphanCycleWorkFailsClosed pins the invariant across every record that
// says pi's agent loop is running: work with no cycle behind it is never given
// an invented opener and never attached to a later prompt.
func TestOrphanCycleWorkFailsClosed(t *testing.T) {
	orphans := map[string]pi.Event{
		"assistant start": pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant}},
		"assistant delta": pi.MessageUpdateEvent{},
		"assistant end":   pi.MessageEndEvent{Message: pi.AgentMessage{Role: messageRoleAssistant}},
		"tool start":      pi.ToolExecutionStartEvent{ToolCallID: "t", ToolName: toolNameRead},
		"tool update":     pi.ToolExecutionUpdateEvent{ToolCallID: "t"},
		"tool end":        pi.ToolExecutionEndEvent{ToolCallID: "t"},
		"turn start":      pi.TurnStartEvent{},
		"turn end":        pi.TurnEndEvent{},
		"agent end":       pi.AgentEndEvent{},
		"settle marker":   pi.AgentSettledEvent{},
		"extension error": pi.ExtensionErrorEvent{ExtensionPath: "/private/ext.ts", Error: "boom"},
	}

	for name, event := range orphans {
		t.Run(name, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			fixture.route(t, event)
			fixture.quiesce()

			require.Error(t, fixture.session.poisonedError(),
				"a fenced session admits no later turn")
			require.Nil(t, fixture.outbox.currentCycle(), "no cycle is invented")
			require.True(t, fixture.lifecycleFenced())
			requireFencedReservationRefused(t, fixture.outbox)

			for _, emitted := range fixture.client.lifecycleEvents(t) {
				require.Equal(t, string(lifecycle.EventSnapshot), emitted["type"],
					"a fenced generation states nothing beyond the snapshot it opened with")
			}
		})
	}
}

// TestBetweenRunRecordsOpenNothingAndKeepTheSession pins the vocabulary pi emits
// with no run in flight: the queue report, the compaction pair it runs around a
// turn, the level echo a configuration change produces, the journal entry and
// bare custom message an extension writes, and a type this package does not
// model. None of them bears work, so each is recorded, none invents a cycle, and
// the session still admits the next prompt.
func TestBetweenRunRecordsOpenNothingAndKeepTheSession(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	between := []pi.Event{
		pi.QueueUpdateEvent{},
		pi.CompactionStartEvent{Reason: "threshold"},
		pi.CompactionEndEvent{Reason: "threshold"},
		pi.AutoRetryStartEvent{Attempt: 1},
		pi.AutoRetryEndEvent{Success: true, Attempt: 1},
		pi.UnknownEvent{EventType: "thinking_level_changed"},
		pi.UnknownEvent{EventType: "entry_appended"},
		pi.UnknownEvent{EventType: "session_info_changed"},
		pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleCustom}},
		pi.MessageEndEvent{Message: pi.AgentMessage{Role: messageRoleCustom}},
	}

	fixture.route(t, between...)
	fixture.quiesce()

	require.NoError(t, fixture.session.poisonedError())
	require.False(t, fixture.lifecycleFenced())
	require.Zero(t, fixture.process.shutdownCalls)
	require.Nil(t, fixture.outbox.currentCycle(), "a record that bears no work opens no cycle")

	for _, emitted := range fixture.client.lifecycleEvents(t) {
		require.Equal(t, string(lifecycle.EventSnapshot), emitted["type"],
			"a record that opens nothing states no lifecycle identity")
	}

	require.NoError(t, reserveOutboxPrompt(fixture.outbox, newTurnDelivery()),
		"the session still admits the next prompt")
}

// TestConfigurationDrainKeepsTheSession pins the same rule across a host
// configuration command. pi emits thinking_level_changed before the response to
// the command that caused it, so the echo is retained for the whole write and
// replayed into an idle router, which must record it rather than treat it as
// work nothing owns.
func TestConfigurationDrainKeepsTheSession(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	require.True(t, fixture.outbox.beginConfiguration())

	admitted := fixture.outbox.admit(pi.UnknownEvent{EventType: "thinking_level_changed"})
	require.Equal(t, outboxQueued, admitted.disposition)

	fixture.outbox.finishConfiguration()
	fixture.session.drainOutbox(t.Context(), fixture.outbox)
	fixture.quiesce()

	require.NoError(t, fixture.session.poisonedError())
	require.False(t, fixture.lifecycleFenced())
	require.Zero(t, fixture.process.shutdownCalls)
}

// TestASecondOpenerJoinsTheRunningAgentCycle pins pi's own bracketing. One
// settled scope runs the agent loop as often as compaction, a retry, or a queued
// message demands, and each continuation states its own opener; the cycle the
// first opener began is the one the settle marker ends.
func TestASecondOpenerJoinsTheRunningAgentCycle(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	cycle := fixture.outbox.currentCycle()
	require.NotNil(t, cycle)

	fixture.route(t, pi.AgentEndEvent{}, pi.AgentStartEvent{})
	require.Same(t, cycle, fixture.outbox.currentCycle(), "the continuation joined the open cycle")

	fixture.settle(t)

	require.NoError(t, fixture.session.poisonedError())
	require.Len(t, fixture.boundaries(t), 1, "one scope committed exactly one boundary")

	events := fixture.client.lifecycleEvents(t)
	require.Equal(t, string(lifecycle.ForegroundIdle), events[len(events)-1]["state"])
}

// TestUnownedRecordFenceKeepsTheNativePathSecret pins that the extension
// surface's own paths and thrown text never leave the adapter.
func TestUnownedRecordFenceKeepsTheNativePathSecret(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.ExtensionErrorEvent{
		ExtensionPath: "/private/pi/ext/permission-bridge.ts",
		Event:         "tool_execute",
		Error:         "TypeError: cannot read property of undefined",
	})
	fixture.quiesce()

	failure := fixture.session.poisonedError()
	require.Error(t, failure)
	require.NotContains(t, failure.Error(), "permission-bridge")
	require.NotContains(t, failure.Error(), "TypeError")
	require.NotContains(t, fixture.logs.String(), "permission-bridge")
	require.NotContains(t, fixture.logs.String(), "TypeError")
}

// TestSettlementFailureContainsWithoutStatingSuccess pins the durability rule for
// the between-prompt path. A cycle whose native rows, boundary record, or
// terminal event did not land states no success, mints no idle, and hands the
// foreground to nobody: the session has just proved it cannot describe what
// happened, so its incarnation ends here.
func TestSettlementFailureContainsWithoutStatingSuccess(t *testing.T) {
	for name, step := range map[string]struct {
		breakIt func(*agentCycleFixture)
		// durable reports whether the failure happened after the boundary
		// record was already committed. Durability precedes the terminal event
		// on purpose, so a boundary that landed still states what pi did; what
		// must never appear is a success on the wire.
		durable bool
	}{
		"mirror": {breakIt: func(f *agentCycleFixture) {
			// A directory is readable as a path and unreadable as a file, which
			// is the mirror source failing without failing anything else.
			f.session.sessionFilePath = filepath.Dir(f.session.sessionFilePath)
		}},
		"boundary": {breakIt: func(f *agentCycleFixture) {
			f.store.mu.Lock()
			f.store.failSubpath = SessionStoreLifecycleSubpath
			f.store.mu.Unlock()
		}},
		"terminal idle": {durable: true, breakIt: func(f *agentCycleFixture) {
			f.client.mu.Lock()
			f.client.lifecycleFailAfter = 2
			f.client.mu.Unlock()
		}},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			fixture.session.sessionFilePath = filepath.Join(t.TempDir(), "session.jsonl")
			require.NoError(t, os.WriteFile(fixture.session.sessionFilePath, []byte("{\"type\":\"session\"}\n"), 0o600))
			step.breakIt(fixture)

			fixture.route(t, pi.AgentStartEvent{})
			fixture.route(t, assistantWork()...)
			fixture.settle(t)

			require.Error(t, fixture.session.poisonedError(), "the session is contained")
			require.True(t, fixture.lifecycleFenced(), "the incarnation ends with no terminal event")
			require.Equal(t, uint64(1), fixture.session.lc.generation,
				"containment names the generation it was raised on")
			requireFencedReservationRefused(t, fixture.outbox)
			require.Positive(t, fixture.process.shutdownCalls)
			require.Positive(t, fixture.process.closeCalls)

			for _, event := range fixture.client.lifecycleEvents(t) {
				require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"],
					"a cycle that could not be described states no terminal event")
			}

			if !step.durable {
				require.Empty(t, fixture.boundaries(t),
					"a cycle whose prefix was never made durable commits nothing")

				return
			}

			// The boundary landed before the wire failed, so the store keeps the
			// truthful record of what pi did. The client is simply never told a
			// terminal state, and the poisoned session refuses the next turn.
			records := fixture.boundaries(t)
			require.Len(t, records, 1)
			require.Equal(t, nativeStateCommitted, records[0].NativeState)
		})
	}
}

// TestNegotiatedFencedLifecycleContainsAgentOpener pins that only a genuinely
// unnegotiated lifecycle stream may be absent. Once negotiated, a fenced or
// generation-mismatched owner cannot silently open invisible autonomous work.
func TestNegotiatedFencedLifecycleContainsAgentOpener(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.session.fenceLifecycleGeneration(fixture.outbox.generation)
	before := len(fixture.client.lifecycleEvents(t))

	fixture.route(t, pi.AgentStartEvent{})
	fixture.quiesce()

	require.Error(t, fixture.session.poisonedError())
	require.True(t, fixture.lifecycleFenced())
	require.Nil(t, fixture.outbox.currentCycle())
	require.Positive(t, fixture.process.shutdownCalls)
	require.Positive(t, fixture.process.closeCalls)
	require.Len(t, fixture.client.lifecycleEvents(t), before,
		"the fenced stream emitted no invisible running or idle transition")
	require.Empty(t, fixture.boundaries(t))
	require.False(t, fixture.session.agentWorkPending(), "containment leaves no settling wedge")

	_, promptErr := fixture.session.acquireTurn(t.Context())
	require.Error(t, promptErr)
	_, restoreErr := fixture.session.beginRestore(t.Context())
	require.Error(t, restoreErr)
}

func TestNegotiatedFencedLifecycleContainsAgentSettlement(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.route(t, pi.AgentStartEvent{})
	fixture.session.fenceLifecycleGeneration(fixture.outbox.generation)

	fixture.route(t, pi.AgentSettledEvent{})
	fixture.quiesce()

	require.Error(t, fixture.session.poisonedError())
	require.True(t, fixture.lifecycleFenced())
	require.Positive(t, fixture.process.shutdownCalls)
	require.Positive(t, fixture.process.closeCalls)
	for _, event := range fixture.client.lifecycleEvents(t) {
		require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"])
	}
}

// TestAgentCycleSettlementPanicContainsWithoutIdle pins the panic branch of the
// autonomous settlement goroutine. A panicking mirror or boundary append must
// release its persistence lock, fence and poison the session, stop the exact
// process, and publish neither a false terminal idle nor permanent backpressure.
func TestAgentCycleSettlementPanicContainsWithoutIdle(t *testing.T) {
	const secret = "agent-cycle-panic-secret-sentinel"

	for name, inject := range map[string]func(*markingStore){
		"mirror": func(store *markingStore) {
			store.panicMain = true
		},
		"boundary": func(store *markingStore) {
			store.panicSubpath = SessionStoreLifecycleSubpath
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			logs := &strings.Builder{}
			fixture.session.agent.log = slog.New(slog.NewTextHandler(logs, nil))
			fixture.store.mu.Lock()
			fixture.store.panicValue = secret
			inject(fixture.store)
			fixture.store.mu.Unlock()

			fixture.route(t, pi.AgentStartEvent{})
			fixture.route(t, assistantWork()...)
			fixture.route(t, pi.AgentSettledEvent{})
			fixture.quiesce()

			require.Error(t, fixture.session.poisonedError())
			require.True(t, fixture.lifecycleFenced())
			require.Positive(t, fixture.process.shutdownCalls)
			require.Positive(t, fixture.process.closeCalls)
			require.False(t, fixture.session.agentWorkPending(), "the settling state was fenced, not wedged")

			for _, event := range fixture.client.lifecycleEvents(t) {
				require.NotEqual(t, string(lifecycle.ForegroundIdle), event["state"],
					"a panicking settlement never states terminal success")
			}

			require.True(t, fixture.session.commitMu.TryLock(), "the panicking settlement retained the persistence lock")
			fixture.session.commitMu.Unlock()
			require.NotContains(t, logs.String(), secret)
		})
	}
}

// TestContainmentNamesTheGenerationItWasRaisedOn pins the other half of exact
// containment: a failure observed on a replaced generation never retires the
// stream its successor has already opened.
func TestContainmentNamesTheGenerationItWasRaisedOn(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	stale := fixture.outbox
	oldProcess := fixture.process
	successorProcess := newStubProcess(false)
	successor := newTestSessionOutbox(2)
	bindTestRuntime(successor, successorProcess, fixture.pi, nil, nil, nil)

	// The successor incarnation opens while the old router is still holding a
	// record it is about to fail on.
	fixture.session.mu.Lock()
	fixture.session.outbox = successor
	fixture.session.proc = successorProcess
	fixture.session.pumpGeneration = 2
	fixture.session.mu.Unlock()
	require.NoError(t, fixture.session.openLifecycleStream(t.Context(), 2))

	fixture.session.containGeneration(t.Context(), stale, "a stale generation failed")
	fixture.quiesce()

	require.False(t, fixture.lifecycleFenced(), "the live incarnation is not retired by an older one")
	require.Equal(t, uint64(2), fixture.session.lc.generation)
	require.Error(t, fixture.session.poisonedError(), "the session that owned both is still poisoned")
	require.Positive(t, oldProcess.shutdownCalls)
	require.Positive(t, oldProcess.closeCalls)
	require.Zero(t, successorProcess.shutdownCalls, "stale containment did not stop the successor")
	require.Zero(t, successorProcess.closeCalls)
}

func TestPoisonedHostAdmissionsJoinExactGenerationContainment(t *testing.T) {
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(NewInMemorySessionStore()),
	)
	client := newStubPiClient()
	process := newStubProcess(false)
	shutdownStarted := make(chan struct{})
	releaseShutdown := make(chan struct{})
	nativeClosed := make(chan struct{})
	process.shutdownFunc = func(context.Context) error {
		close(shutdownStarted)
		<-releaseShutdown

		return nil
	}
	process.closeFunc = func() error {
		close(nativeClosed)

		return nil
	}

	sessionFile := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(sessionFile, []byte("{\"type\":\"session\"}\n"), 0o600))
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          client,
		proc:            process,
		sessionFilePath: sessionFile,
		sessionRoot:     t.TempDir(),
	}
	outbox := newTestSessionOutbox(1)
	bindTestRuntime(outbox, process, client, nil, nil, nil)
	session.outbox = outbox
	session.pumpGeneration = 1

	session.containGeneration(t.Context(), outbox, "the generation violated its routing invariant")
	<-shutdownStarted
	outbox.mu.Lock()
	containment := outbox.containment
	require.Equal(t, containmentOwnerPump, containment.owner)
	outbox.mu.Unlock()

	promptResult := make(chan error, 1)
	promptEntered := make(chan struct{})
	go func() {
		close(promptEntered)
		_, err := session.Prompt(context.Background(), TextPromptRequest("id", "blocked-prompt", "prompt"))
		promptResult <- err
	}()

	restoreResult := make(chan error, 1)
	restoreEntered := make(chan struct{})
	go func() {
		close(restoreEntered)
		_, err := session.beginRestore(context.Background())
		restoreResult <- err
	}()

	closeResult := make(chan error, 1)
	closeEntered := make(chan struct{})
	go func() {
		close(closeEntered)
		closeResult <- session.Close(context.Background())
	}()

	<-promptEntered
	<-restoreEntered
	<-closeEntered
	<-containment.waiting

	for name, result := range map[string]<-chan error{
		"prompt":  promptResult,
		"restore": restoreResult,
		"close":   closeResult,
	} {
		select {
		case err := <-result:
			t.Fatalf("%s returned before exact-generation shutdown completed: %v", name, err)
		default:
		}
	}

	close(releaseShutdown)
	<-nativeClosed

	require.Error(t, <-promptResult)
	require.Error(t, <-restoreResult)
	require.NoError(t, <-closeResult)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
}

func TestCancelledHostDoorsStillJoinObservableContainment(t *testing.T) {
	for _, cancellation := range []string{"before entry", "while waiting"} {
		t.Run(cancellation, func(t *testing.T) {
			agent := NewAgent(
				WithLogger(slog.New(slog.DiscardHandler)),
				WithSessionStore(NewInMemorySessionStore()),
			)
			client := newStubPiClient()
			client.model = pi.Model{ID: "selected", ContextWindow: 100}
			process := newStubProcess(false)
			shutdownEntered := make(chan struct{})
			releaseNative := make(chan struct{})
			nativeClosed := make(chan struct{})
			process.shutdownFunc = func(context.Context) error {
				close(shutdownEntered)
				<-releaseNative

				return nil
			}
			process.closeFunc = func() error {
				close(nativeClosed)

				return nil
			}

			start := sessionStart{Cwd: testCwd, ResumeID: "id"}
			session := &agentSession{
				agent:       agent,
				id:          "id",
				cwd:         start.Cwd,
				fingerprint: sessionStartFingerprint(start),
				client:      client,
				proc:        process,
				turn:        make(chan struct{}, 1),
				sessionRoot: t.TempDir(),
			}
			outbox := newTestSessionOutbox(1)
			bindTestRuntime(outbox, process, client, nil, nil, nil)
			session.outbox = outbox
			session.pumpGeneration = 1
			agent.sessions[session.id] = session

			session.containGeneration(t.Context(), outbox, "the generation violated its routing invariant")
			<-shutdownEntered
			outbox.mu.Lock()
			containment := outbox.containment
			outbox.mu.Unlock()

			type hostDoor struct {
				name string
				call func(context.Context) error
			}
			doors := []hostDoor{
				{name: "prompt", call: func(ctx context.Context) error {
					_, err := agent.Prompt(ctx, TextPromptRequest(session.id, "cancelled-prompt", "prompt"))

					return err
				}},
				{name: "resume", call: func(ctx context.Context) error {
					_, err := agent.ResumeSession(ctx, ResumeSessionRequest(session.id, start.Cwd))

					return err
				}},
				{name: "configuration", call: func(ctx context.Context) error {
					_, err := agent.SetSessionConfigOption(ctx, SetModelRequest(session.id, "p/model"))

					return err
				}},
				{name: "load", call: func(ctx context.Context) error {
					_, err := agent.LoadSession(ctx, LoadSessionRequest(session.id, start.Cwd))

					return err
				}},
				{name: "close", call: func(ctx context.Context) error {
					_, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.id})

					return err
				}},
			}

			results := make(map[string]<-chan error, len(doors))
			entered := make([]<-chan struct{}, 0, len(doors))
			cancels := make([]context.CancelFunc, 0, len(doors))
			for _, door := range doors {
				ctx, cancel := context.WithCancel(context.Background())
				if cancellation == "before entry" {
					cancel()
				}
				cancels = append(cancels, cancel)
				started := make(chan struct{})
				result := make(chan error, 1)
				results[door.name] = result
				entered = append(entered, started)
				go func() {
					close(started)
					result <- door.call(ctx)
				}()
			}

			for _, started := range entered {
				<-started
			}
			if cancellation == "while waiting" {
				for _, cancel := range cancels {
					cancel()
				}
			}
			<-containment.waiting

			for name, result := range results {
				select {
				case err := <-result:
					t.Fatalf("%s returned before exact native containment completed: %v", name, err)
				default:
				}
			}

			close(releaseNative)
			<-nativeClosed
			for name, result := range results {
				err := <-result
				if name == "close" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.Canceled, name)
				}
			}
			require.Equal(t, 1, process.shutdownCalls)
			require.Equal(t, 1, process.closeCalls)
		})
	}
}

func uiRequest(t *testing.T, id, method, title string) pi.UIRequest {
	t.Helper()

	line, err := json.Marshal(map[string]any{
		jsonFieldType: "extension_ui_request", "id": id, jsonFieldMethod: method, "title": title,
		"options": []string{"a", "b"},
	})
	require.NoError(t, err)

	message, err := pi.DecodeMessage(line)
	require.NoError(t, err)
	require.Equal(t, pi.MessageKindUIRequest, message.Kind)

	return message.UIRequest
}

// TestAgentCycleCancelsOrdinaryDialogsAndKeepsAuthOnItsBroker pins that opening
// a cycle adds no background answering surface: an ordinary dialog with no
// foreground to block is still actively cancelled, while a provider-auth marker
// stays on its own broker and off the raw-event stream, because a login's
// presentation and its answers are credential material.
func TestAgentCycleCancelsOrdinaryDialogsAndKeepsAuthOnItsBroker(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.session.mu.Lock()
	fixture.session.rawMessages = rawMessageConfig{All: true}
	fixture.session.mu.Unlock()

	fixture.session.agent.providerAuth = &providerAuth{
		agent:     fixture.session.agent,
		exchanges: make(map[string]*authExchange),
	}

	fixture.route(t, pi.AgentStartEvent{})
	require.NotNil(t, fixture.outbox.currentCycle())

	ordinary := uiRequest(t, "dialog-1", uiMethodSelect, "pick one")
	fixture.session.routeUIRequest(t.Context(), fixture.outbox, ordinary)

	auth := uiRequest(t, "dialog-2", uiMethodSelect, pi.AuthTitleMarker+"opaque-login-payload")
	fixture.session.routeUIRequest(t.Context(), fixture.outbox, auth)
	require.NoError(t, fixture.outbox.producers.waitChildren(t.Context()))

	fixture.pi.mu.Lock()
	responses := append([]pi.UIResponse(nil), fixture.pi.responses...)
	fixture.pi.mu.Unlock()

	require.Len(t, responses, 2, "both dialogs are answered rather than left holding pi's extension")
	require.Equal(t, pi.UICancelResponse("dialog-1"), responses[0])
	require.Equal(t, pi.UICancelResponse("dialog-2"), responses[1])

	fixture.client.mu.Lock()
	notified := append([]map[string]any(nil), fixture.client.notified...)
	fixture.client.mu.Unlock()

	require.Len(t, notified, 1, "the auth dialog never reaches the raw-event stream")
	require.Contains(t, string(mustJSON(t, notified[0])), "dialog-1")
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)

	return encoded
}

// TestGenerationEndDuringACycleSettlementIsNotALoss pins that a cycle whose
// boundary is already running is not fenced out from under itself, so the
// terminal state the session is in the middle of stating still lands.
func TestGenerationEndDuringACycleSettlementIsNotALoss(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	release := make(chan struct{})
	fixture.store.mu.Lock()
	fixture.store.mark = func() int {
		<-release

		return fixture.client.count()
	}
	fixture.store.mu.Unlock()

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	fixture.route(t, pi.AgentSettledEvent{})

	// The transport dies while the cycle's own boundary is mid-commit.
	fixture.session.endGeneration(fixture.outbox)
	require.False(t, fixture.lifecycleFenced(), "a settling cycle is not a generation loss")

	close(release)
	fixture.quiesce()

	events := fixture.client.lifecycleEvents(t)
	idle := events[len(events)-1]
	require.Equal(t, string(lifecycle.ForegroundIdle), idle["state"])
	require.Equal(t, string(lifecycle.OutcomeSuccess), idle["outcome"])
}

// TestEOFThenRelaunchRecordsTheLostCycleIdentity pins the production order: the
// transport ends first and the relaunch path writes the loss down afterwards, so
// the fence must keep the identity of the cycle that died. It also pins that a
// record the replaced incarnation never replayed is lost with it and never
// mutates the generation that follows.
func TestEOFThenRelaunchRecordsTheLostCycleIdentity(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)

	running := fixture.client.lifecycleEvents(t)[1]
	lostTurn, lostCycle := running["turnId"], running["cycleId"]
	require.NotEmpty(t, lostTurn)

	before := len(fixture.client.lifecycleEvents(t))

	// EOF first: the pump's own end fences the incarnation.
	stale := fixture.outbox
	fixture.session.endGeneration(stale)
	fixture.quiesce()

	require.True(t, fixture.lifecycleFenced())
	require.Len(t, fixture.client.lifecycleEvents(t), before, "an end is not an idle")

	// Relaunch second: the loss the fence retired is what the boundary names.
	require.NoError(t, fixture.session.recordGenerationLoss(t.Context()))

	records := fixture.boundaries(t)
	require.Len(t, records, 1)
	require.Equal(t, lostTurn, records[0].TurnID, "the boundary names the cycle that was lost")
	require.Equal(t, lostCycle, records[0].CycleID)
	require.Equal(t, nativeStateRetained, records[0].NativeState)
	require.Empty(t, records[0].Outcome, "a loss states no outcome")

	next := newTestSessionOutbox(2)
	fixture.session.outbox = next
	fixture.session.pumpGeneration = 2
	require.NoError(t, fixture.session.openLifecycleStream(t.Context(), 2))

	after := len(fixture.client.lifecycleEvents(t))

	// The replaced generation's own router refuses the frames it still held.
	fixture.session.routeNativeEvent(t.Context(), stale, pi.MessageUpdateEvent{})
	fixture.session.drainOutbox(t.Context(), stale)
	fixture.quiesce()

	require.Len(t, fixture.client.lifecycleEvents(t), after)
	require.Nil(t, next.currentCycle(), "the new incarnation opens no cycle for an old record")
	require.False(t, fixture.lifecycleFenced(), "the successor is untouched by the record it never owned")
}

// TestOutboxOverflowContainsRatherThanDroppingARecord pins the bound: reaching
// it is a containment failure, never a licence to lose a record or to reorder
// the ones behind it. The record that overflowed moves nothing — not the queue,
// not the raw stream.
func TestOutboxOverflowContainsRatherThanDroppingARecord(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	require.NoError(t, fixture.outbox.activate(delivery))
	close(delivery.done)

	fixture.route(t, pi.AgentSettledEvent{})

	for range outboxQueueCapacity {
		fixture.route(t, pi.TurnStartEvent{})
	}

	fixture.outbox.mu.Lock()
	retained := len(fixture.outbox.queued)
	fixture.outbox.mu.Unlock()
	require.Equal(t, outboxQueueCapacity, retained)

	rawBefore := len(fixture.client.notified)

	fixture.route(t, pi.TurnStartEvent{})
	fixture.quiesce()

	require.Error(t, fixture.session.poisonedError())
	require.Len(t, fixture.client.notified, rawBefore, "the overflowed record reached no subscriber")
	requireFencedReservationRefused(t, fixture.outbox)
}

// requireFencedReservationRefused pins that a fenced generation cannot mutate
// a delivery that recovery may attach to its successor.
func requireFencedReservationRefused(t *testing.T, outbox *sessionOutbox) {
	t.Helper()

	delivery := newTurnDelivery()
	require.ErrorIs(t, reserveOutboxPrompt(outbox, delivery), pi.ErrTransportClosed)

	select {
	case <-delivery.events:
		t.Fatal("the fenced generation closed a successor delivery")
	default:
	}
}

// livePumpFixture runs the real pump over a stub transport, which is the only
// way to state that containment never stops the sole reader of that transport.
type livePumpFixture struct {
	*agentCycleFixture
}

func newLivePumpFixture(t *testing.T) *livePumpFixture {
	t.Helper()

	fixture := newAgentCycleSession(t)
	generation := startTestPump(fixture.session, fixture.pi)
	require.Equal(t, uint64(1), generation)

	fixture.outbox = fixture.session.outboxRouter()
	require.NoError(t, fixture.session.openLifecycleStream(t.Context(), generation))

	t.Cleanup(fixture.session.stopPump)

	return &livePumpFixture{agentCycleFixture: fixture}
}

// send hands one record to the live pump and fails the test if the pump has
// stopped reading, which is the property a containment must never break.
func (f *livePumpFixture) send(t *testing.T, events ...pi.Event) {
	t.Helper()

	for _, event := range events {
		select {
		case f.pi.events <- event:
		case <-time.After(2 * time.Second):
			t.Fatal("the pump stopped reading the native transport")
		}
	}
}

// realClientPumpFixture runs the production JSONL client and the session pump
// over pipes. It is the causal-order harness: stdout records cross the same
// synchronous reader boundary used by a real pi process.
type realClientPumpFixture struct {
	*agentCycleFixture
	native      *pi.Client
	commands    *bufio.Scanner
	stdout      *io.PipeWriter
	stdinWriter *io.PipeWriter
	cancel      context.CancelFunc
}

func newRealClientPumpFixture(t *testing.T) *realClientPumpFixture {
	t.Helper()

	fixture := newAgentCycleSession(t)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	native := pi.NewClient(stdinWriter, stdoutReader)
	clientCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, native.Start(clientCtx))

	fixture.session.client = native
	generation := startTestPump(fixture.session, native)
	fixture.outbox = fixture.session.outboxRouter()
	require.NoError(t, fixture.session.openLifecycleStream(t.Context(), generation))

	commands := bufio.NewScanner(stdinReader)
	commands.Buffer(make([]byte, 0, 64<<10), 1<<20)

	result := &realClientPumpFixture{
		agentCycleFixture: fixture,
		native:            native,
		commands:          commands,
		stdout:            stdoutWriter,
		stdinWriter:       stdinWriter,
		cancel:            cancel,
	}

	t.Cleanup(func() {
		cancel()
		_ = stdoutWriter.Close()
		fixture.session.stopPump()
		_ = stdinWriter.Close()
		_ = native.Stop()
		_ = stdinReader.Close()
	})

	return result
}

func (f *realClientPumpFixture) nextCommand(t *testing.T) map[string]any {
	t.Helper()
	require.True(t, f.commands.Scan(), "expected a native command")

	command := map[string]any{}
	require.NoError(t, json.Unmarshal(f.commands.Bytes(), &command))

	return command
}

func (f *realClientPumpFixture) emit(t *testing.T, records ...string) <-chan error {
	t.Helper()
	done := make(chan error, 1)

	go func() {
		done <- func() error {
			for _, record := range records {
				if _, err := io.WriteString(f.stdout, record+"\n"); err != nil {
					return err
				}
			}

			return nil
		}()
	}()

	return done
}

// TestRealClientPromptResponseWatermark pins both sides of the response record:
// post-response foreground records route normally with the prompt nonce, and a
// later autonomous cycle routes without it. No scheduler can place either side
// differently because the acceptance hook runs on the JSONL reader itself.
func TestRealClientPromptResponseWatermark(t *testing.T) {
	fixture := newRealClientPumpFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	fixture.session.mu.Lock()
	fixture.session.turnNonce = "accepted-route"
	fixture.session.mu.Unlock()

	promptDone := make(chan error, 1)
	go func() {
		promptDone <- fixture.native.PromptWithBoundary(t.Context(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return fixture.session.beginPromptDispatch(t.Context(), fixture.outbox, delivery)
			},
			Accepted: func(acceptCtx context.Context) error {
				return fixture.session.acceptPromptResponse(acceptCtx, fixture.outbox, delivery, testSubmission())
			},
		})
	}()

	command := fixture.nextCommand(t)
	require.Equal(t, "prompt", command["type"])
	id, ok := command["id"].(string)
	require.True(t, ok)

	ack := fixture.emit(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
	require.NoError(t, <-promptDone)
	require.NoError(t, <-ack)

	post := fixture.emit(t,
		`{"type":"message_start","message":{"role":"assistant"}}`,
		`{"type":"message_update","message":{"role":"assistant"},"assistantMessageEvent":{"type":"text_delta","delta":"prompt output"}}`,
		`{"type":"agent_settled"}`,
	)
	for range 3 {
		<-delivery.events
	}
	require.NoError(t, <-post)
	require.NoError(t, fixture.session.lifecycleSettleTurn(t.Context(),
		lifecycle.StopReasonEndTurn, lifecycle.OutcomeSuccess))

	fixture.session.finishPromptForeground(delivery)

	autonomous := fixture.emit(t,
		`{"type":"agent_start"}`,
		`{"type":"message_update","message":{"role":"assistant"},"assistantMessageEvent":{"type":"text_delta","delta":"autonomous output"}}`,
		`{"type":"agent_settled"}`,
	)
	require.NoError(t, <-autonomous)
	require.Eventually(t, func() bool {
		events := fixture.client.lifecycleEvents(t)

		return len(events) >= 3 && events[len(events)-1]["state"] == string(lifecycle.ForegroundIdle)
	}, 2*time.Second, time.Millisecond, "the post-prompt autonomous cycle settled")
	fixture.quiesce()

	routed := 0
	unrouted := 0
	fixture.client.mu.Lock()
	raw := append([]map[string]any(nil), fixture.client.notified...)
	fixture.client.mu.Unlock()
	for _, notification := range raw {
		if _, carries := notification["_meta"]; carries {
			routed++
		} else {
			unrouted++
		}
	}
	require.Equal(t, 3, routed, "every post-response foreground frame carries the prompt route")
	require.Equal(t, 3, unrouted, "the later autonomous cycle carries no prompt route")

	events := fixture.client.lifecycleEvents(t)
	require.Equal(t, string(lifecycle.CauseActivity), events[len(events)-2]["cause"])
	require.Equal(t, string(lifecycle.ForegroundRunning), events[len(events)-2]["state"])
	require.Equal(t, string(lifecycle.ForegroundIdle), events[len(events)-1]["state"])
}

// TestRealClientAcceptancePrecedesRawAndTypedWork proves the session-level
// ordering through the production JSONL reader and pump. A single stdout write
// contains the successful response and later work, so only the response
// watermark can put prompt_accepted ahead of both projections.
func TestRealClientAcceptancePrecedesRawAndTypedWork(t *testing.T) {
	fixture := newRealClientPumpFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	fixture.session.mu.Lock()
	fixture.session.turnNonce = "ordered-route"
	fixture.session.mu.Unlock()

	typedDone := make(chan error, 1)
	go func() {
		state := &promptTurnState{}
		for range 2 {
			event := <-delivery.events
			_, err := fixture.session.handleTurnEvent(t.Context(), event, state)
			if err != nil {
				typedDone <- err

				return
			}
		}
		typedDone <- nil
	}()

	promptDone := make(chan error, 1)
	go func() {
		promptDone <- fixture.native.PromptWithBoundary(t.Context(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return fixture.session.beginPromptDispatch(t.Context(), fixture.outbox, delivery)
			},
			Accepted: func(acceptCtx context.Context) error {
				return fixture.session.acceptPromptResponse(acceptCtx, fixture.outbox, delivery, testSubmission())
			},
		})
	}()

	command := fixture.nextCommand(t)
	id, ok := command["id"].(string)
	require.True(t, ok)

	written := fixture.emit(t,
		`{"id":"`+id+`","type":"response","command":"prompt","success":true}`,
		`{"type":"message_start","message":{"role":"assistant"}}`,
		`{"type":"message_update","message":{"role":"assistant"},"assistantMessageEvent":{"type":"text_delta","delta":"work"}}`,
	)
	require.NoError(t, <-promptDone)
	require.NoError(t, <-typedDone)
	require.NoError(t, <-written)

	fixture.client.mu.Lock()
	trace := append([]string(nil), fixture.client.trace...)
	fixture.client.mu.Unlock()

	accepted := slices.Index(trace, "lifecycle:prompt_accepted")
	raw := slices.Index(trace, "raw")
	typed := slices.Index(trace, "typed")
	require.NotEqual(t, -1, accepted)
	require.NotEqual(t, -1, raw)
	require.NotEqual(t, -1, typed)
	require.Less(t, accepted, raw)
	require.Less(t, accepted, typed)
}

func TestCloseAfterDecodedSuccessPreservesClaimedAcceptance(t *testing.T) {
	fixture := newRealClientPumpFixture(t)
	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	fixture.session.mu.Lock()
	fixture.session.turnNonce = "claimed-response"
	fixture.session.mu.Unlock()

	acceptanceClaimed := make(chan struct{})
	releaseAcceptance := make(chan struct{})
	promptDone := make(chan error, 1)
	go func() {
		promptDone <- fixture.native.PromptWithBoundary(context.Background(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return fixture.session.beginPromptDispatch(context.Background(), fixture.outbox, delivery)
			},
			Accepted: func(ctx context.Context) error {
				close(acceptanceClaimed)
				<-releaseAcceptance

				return fixture.session.acceptPromptResponse(ctx, fixture.outbox, delivery, testSubmission())
			},
		})
	}()

	command := fixture.nextCommand(t)
	id, ok := command["id"].(string)
	require.True(t, ok)
	written := fixture.emit(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`)
	<-acceptanceClaimed

	shutdownEntered := make(chan struct{})
	fixture.process.shutdownFunc = func(context.Context) error {
		close(shutdownEntered)

		return nil
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.session.Close(context.Background()) }()
	abort := fixture.nextCommand(t)
	require.Equal(t, "abort", abort["type"])
	abortID, ok := abort["id"].(string)
	require.True(t, ok)
	select {
	case err := <-closeDone:
		t.Fatalf("close crossed the claimed acceptance boundary early: %v", err)
	default:
	}
	close(releaseAcceptance)
	require.NoError(t, <-fixture.emit(t,
		`{"id":"`+abortID+`","type":"response","command":"abort","success":true}`,
	))
	<-shutdownEntered

	require.NoError(t, <-promptDone)
	require.NoError(t, <-written)
	require.NoError(t, <-closeDone)
	events := fixture.client.lifecycleEvents(t)
	accepted := 0
	for _, event := range events {
		if event["type"] == string(lifecycle.EventPromptAccepted) {
			accepted++
		}
	}
	require.Equal(t, 1, accepted, "close erased or duplicated the response claim")
}

func TestQueueUpdateBeforeSuccessDefersEveryFrameUntilAcceptance(t *testing.T) {
	fixture := newRealClientPumpFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	fixture.session.mu.Lock()
	fixture.session.turnNonce = "queue-route"
	fixture.session.mu.Unlock()

	promptDone := make(chan error, 1)
	go func() {
		promptDone <- fixture.native.PromptWithBoundary(t.Context(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return fixture.session.beginPromptDispatch(t.Context(), fixture.outbox, delivery)
			},
			Accepted: func(acceptCtx context.Context) error {
				return fixture.session.acceptPromptResponse(acceptCtx, fixture.outbox, delivery, testSubmission())
			},
		})
	}()

	command := fixture.nextCommand(t)
	id, ok := command["id"].(string)
	require.True(t, ok)
	fixture.client.mu.Lock()
	baseline := append([]string(nil), fixture.client.trace...)
	fixture.client.mu.Unlock()

	require.NoError(t, <-fixture.emit(t, `{"type":"queue_update","steering":[],"followUp":[]}`))
	fixture.client.mu.Lock()
	require.Equal(t, baseline, fixture.client.trace, "a structural pre-acceptance report escaped as raw or typed output")
	fixture.client.mu.Unlock()

	require.NoError(t, <-fixture.emit(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`))
	require.NoError(t, <-promptDone)

	fixture.client.mu.Lock()
	trace := append([]string(nil), fixture.client.trace...)
	fixture.client.mu.Unlock()
	accepted := slices.Index(trace, "lifecycle:prompt_accepted")
	raw := slices.Index(trace, "raw")
	require.NotEqual(t, -1, accepted)
	require.NotEqual(t, -1, raw)
	require.Less(t, accepted, raw)
}

// TestCompactionBeforeSuccessStillAcceptsThePrompt runs pi's real pre-prompt
// order through the production JSONL reader: auto-compaction is checked before
// the prompt command is answered, so its whole pass — the start marker, the
// summarization retries the compaction request makes, the extension journal
// entry, and the end marker — crosses the reader ahead of the response. The
// prompt is accepted, every held frame is released as raw diagnostics behind the
// acceptance, and the turn runs on the same incarnation.
func TestCompactionBeforeSuccessStillAcceptsThePrompt(t *testing.T) {
	fixture := newRealClientPumpFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	fixture.session.mu.Lock()
	fixture.session.turnNonce = "compacted-route"
	fixture.session.mu.Unlock()

	promptDone := make(chan error, 1)
	go func() {
		promptDone <- fixture.native.PromptWithBoundary(t.Context(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return fixture.session.beginPromptDispatch(t.Context(), fixture.outbox, delivery)
			},
			Accepted: func(acceptCtx context.Context) error {
				return fixture.session.acceptPromptResponse(acceptCtx, fixture.outbox, delivery, testSubmission())
			},
		})
	}()

	command := fixture.nextCommand(t)
	id, ok := command["id"].(string)
	require.True(t, ok)

	fixture.client.mu.Lock()
	baseline := append([]string(nil), fixture.client.trace...)
	fixture.client.mu.Unlock()

	compaction := []string{
		`{"type":"compaction_start","reason":"threshold"}`,
		`{"type":"summarization_retry_scheduled","attempt":1,"maxAttempts":3,"delayMs":1000,"errorMessage":"stream closed"}`,
		`{"type":"summarization_retry_attempt_start","source":"compaction","reason":"threshold"}`,
		`{"type":"summarization_retry_finished"}`,
		`{"type":"entry_appended","entry":{"type":"custom","customType":"note"}}`,
		`{"type":"compaction_end","reason":"threshold","result":{"summary":"s","tokensBefore":100,"estimatedTokensAfter":10},"aborted":false,"willRetry":false}`,
	}
	require.NoError(t, <-fixture.emit(t, compaction...))

	fixture.client.mu.Lock()
	require.Equal(t, baseline, fixture.client.trace, "a pre-acceptance compaction escaped as raw or typed output")
	fixture.client.mu.Unlock()
	require.NoError(t, fixture.session.poisonedError(), "pi's own pre-prompt compaction is not an invariant failure")

	require.NoError(t, <-fixture.emit(t, `{"id":"`+id+`","type":"response","command":"prompt","success":true}`))
	require.NoError(t, <-promptDone)

	fixture.client.mu.Lock()
	trace := append([]string(nil), fixture.client.trace...)
	fixture.client.mu.Unlock()
	accepted := slices.Index(trace, "lifecycle:prompt_accepted")
	require.NotEqual(t, -1, accepted)
	require.Equal(t, slices.Repeat([]string{"raw"}, len(compaction)), trace[len(trace)-len(compaction):],
		"every held frame is released behind the acceptance")
	require.Less(t, accepted, len(trace)-len(compaction))

	work := fixture.emit(t,
		`{"type":"message_update","message":{"role":"assistant"},"assistantMessageEvent":{"type":"text_delta","delta":"after compaction"}}`,
		`{"type":"agent_settled"}`,
	)
	for range 2 {
		<-delivery.events
	}
	require.NoError(t, <-work)
}

// TestRealClientPreResponseWorkContainsBeforeAcknowledgement proves the other
// side through the same client+pump ordering. The pre-response opener is never
// attributed, the response cannot resurrect the prompt, and containment stops
// and reaps the captured fake process while later admission fails synchronously.
func TestRealClientPreResponseWorkContainsBeforeAcknowledgement(t *testing.T) {
	fixture := newRealClientPumpFixture(t)
	fixture.session.rawMessages = rawMessageConfig{All: true}
	var reaped sync.Once
	fixture.process.shutdownFunc = func(context.Context) error {
		reaped.Do(func() { close(fixture.process.exited) })

		return nil
	}

	delivery := newTurnDelivery()
	require.NoError(t, fixture.session.reservePromptForeground(delivery))
	fixture.session.mu.Lock()
	fixture.session.turnNonce = "must-not-escape"
	fixture.session.mu.Unlock()

	promptDone := make(chan error, 1)
	go func() {
		promptDone <- fixture.native.PromptWithBoundary(t.Context(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return fixture.session.beginPromptDispatch(t.Context(), fixture.outbox, delivery)
			},
			Accepted: func(acceptCtx context.Context) error {
				return fixture.session.acceptPromptResponse(acceptCtx, fixture.outbox, delivery, testSubmission())
			},
		})
	}()

	command := fixture.nextCommand(t)
	id, ok := command["id"].(string)
	require.True(t, ok)

	written := fixture.emit(t,
		`{"type":"agent_start"}`,
		`{"id":"`+id+`","type":"response","command":"prompt","success":true}`,
	)
	require.Error(t, <-promptDone)
	require.NoError(t, <-written)
	abort := fixture.nextCommand(t)
	require.Equal(t, "abort", abort["type"])
	abortID, ok := abort["id"].(string)
	require.True(t, ok)
	require.NoError(t, <-fixture.emit(t,
		`{"id":"`+abortID+`","type":"response","command":"abort","success":true}`,
	))
	fixture.quiesce()

	require.Error(t, fixture.session.poisonedError())
	require.True(t, fixture.lifecycleFenced())
	require.Positive(t, fixture.process.shutdownCalls)
	require.Positive(t, fixture.process.closeCalls)
	select {
	case <-fixture.process.Exited():
	default:
		t.Fatal("the contained fake process was not reaped")
	}

	_, reserveErr := func() (*turnDelivery, error) {
		next := newTurnDelivery()

		return next, fixture.session.reservePromptForeground(next)
	}()
	require.Error(t, reserveErr, "poison refuses later prompt admission synchronously")
	_, restoreErr := fixture.session.beginRestore(t.Context())
	require.Error(t, restoreErr, "poisoned native work refuses later restore admission")

	for _, emitted := range fixture.client.lifecycleEvents(t) {
		require.Equal(t, string(lifecycle.EventSnapshot), emitted["type"])
	}
	fixture.client.mu.Lock()
	require.Empty(t, fixture.client.notified, "the refused pre-response frame reached no routed raw stream")
	fixture.client.mu.Unlock()
}

// TestContainmentStopsTheExactNativeGeneration pins the fail-closed transport
// boundary. Containment runs beside the sole reader, stops the captured process,
// and cancels the pump so it unblocks; the fenced transport accepts no record.
func TestContainmentStopsTheExactNativeGeneration(t *testing.T) {
	for name, breaker := range map[string]func(*testing.T, *livePumpFixture){
		"orphan work": func(t *testing.T, f *livePumpFixture) {
			t.Helper()
			f.send(t, pi.MessageEndEvent{Message: pi.AgentMessage{Role: messageRoleAssistant}})
		},
		"extension violation": func(t *testing.T, f *livePumpFixture) {
			t.Helper()
			f.send(t, pi.ExtensionErrorEvent{ExtensionPath: "/private/ext.ts", Error: "boom"})
		},
		"overflow": func(t *testing.T, f *livePumpFixture) {
			t.Helper()

			delivery := newTurnDelivery()
			require.NoError(t, f.session.reservePromptForeground(delivery))
			require.NoError(t, f.outbox.activate(delivery))
			close(delivery.done)

			f.send(t, pi.AgentSettledEvent{})

			for range outboxQueueCapacity + 1 {
				f.send(t, pi.TurnStartEvent{})
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newLivePumpFixture(t)

			breaker(t, fixture)
			require.Eventually(t, func() bool {
				return fixture.session.poisonedError() != nil
			}, 2*time.Second, time.Millisecond, "containment published its admission fence")
			fixture.quiesce()
			require.Error(t, fixture.session.poisonedError())
			require.True(t, fixture.lifecycleFenced())
			requireFencedReservationRefused(t, fixture.outbox)
			require.Positive(t, fixture.process.shutdownCalls)
			require.Positive(t, fixture.process.closeCalls)

			select {
			case <-fixture.session.pumpDone:
			case <-time.After(2 * time.Second):
				t.Fatal("the contained generation's pump did not unblock")
			}

			select {
			case fixture.pi.events <- pi.TurnStartEvent{}:
				t.Fatal("the contained transport still accepted a record")
			default:
			}
		})
	}
}

func TestBlockedPromptNativeWriteCannotBlockCloseFence(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	writeEntered := make(chan struct{})
	releaseWrite := make(chan struct{})
	client.promptWriteFunc = func(context.Context, string) error {
		close(writeEntered)
		<-releaseWrite

		return pi.ErrTransportClosed
	}

	process := newStubProcess(false)
	shutdownEntered := make(chan struct{})
	releaseShutdown := make(chan struct{})
	process.shutdownFunc = func(context.Context) error {
		close(shutdownEntered)
		<-releaseShutdown

		return nil
	}

	session := &agentSession{
		agent:       agent,
		id:          "blocked-write",
		proc:        process,
		client:      client,
		turn:        make(chan struct{}, 1),
		sessionRoot: t.TempDir(),
	}
	outbox := bindTestOutbox(session)
	delivery := newTurnDelivery()
	require.NoError(t, reserveOutboxPrompt(outbox, delivery))
	session.mu.Lock()
	session.turnEvents = delivery
	session.mu.Unlock()

	promptDone := make(chan error, 1)
	go func() {
		promptDone <- client.PromptWithBoundary(context.Background(), "hi", nil, pi.CallBoundary{
			BeforeDispatch: func() (func(), error) {
				return session.beginPromptDispatch(context.Background(), outbox, delivery)
			},
			Accepted: func(ctx context.Context) error {
				return session.acceptPromptResponse(ctx, outbox, delivery, testSubmission())
			},
		})
	}()
	<-writeEntered

	closeDone := make(chan error, 1)
	go func() { closeDone <- session.Close(context.Background()) }()
	<-shutdownEntered
	close(releaseShutdown)
	require.NoError(t, <-closeDone)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)

	close(releaseWrite)
	require.Error(t, <-promptDone)
	require.NoError(t, session.awaitPoisonContainment())
	require.NoError(t, outbox.producers.waitChildren(t.Context()))
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
}

// TestCloseDrainsShutdownRecordsAndRefusesANewCycle pins the close claim. A
// shutdown emits records of its own, and they are drained on purpose rather than
// judged as orphans; an agent_start racing the same teardown opens nothing,
// because close is the last thing this router does.
func TestCloseDrainsShutdownRecordsAndRefusesANewCycle(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.session.pumpDone = make(chan struct{})
	fixture.session.pumpCancel = func() {}

	close(fixture.session.pumpDone)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	require.NotNil(t, fixture.outbox.currentCycle())

	fixture.process.shutdownFunc = func(context.Context) error {
		// Exactly what a native shutdown emits, plus the opener that races it.
		fixture.route(t, pi.TurnEndEvent{}, pi.AgentEndEvent{}, pi.AgentSettledEvent{}, pi.AgentStartEvent{})

		return nil
	}

	require.NoError(t, fixture.session.Close(t.Context()))
	fixture.quiesce()

	require.NoError(t, fixture.session.poisonedError(), "shutdown records are drained, not judged")
	require.Nil(t, fixture.outbox.currentCycle(), "the racing opener opened nothing")

	events := fixture.client.lifecycleEvents(t)
	idle := events[len(events)-1]
	require.Equal(t, string(lifecycle.ForegroundIdle), idle["state"])
	require.Equal(t, string(lifecycle.CauseActivity), idle["cause"], "the cycle ends for the reason it opened")
	require.Equal(t, string(lifecycle.OutcomeCancelled), idle["outcome"])
	require.Equal(t, lifecycle.StopReasonCancelled, idle["stopReason"])

	var idles int

	for _, event := range events {
		if event["state"] == string(lifecycle.ForegroundIdle) {
			idles++
		}
	}

	require.Equal(t, 1, idles, "the claimed cycle is terminalized exactly once")

	records := fixture.boundaries(t)
	require.Len(t, records, 1, "the close boundary is the cycle's only durable record")
	require.False(t, records[0].VacancyProven)
}

// TestCloseCommitRungFailureOrdering pins the two distinct durable rungs. A
// mirror refusal precedes terminalization; a resumable-boundary refusal follows
// terminalization and fences the stream without certifying quiescence.
func TestCloseCommitRungFailureOrdering(t *testing.T) {
	for _, stage := range []string{"mirror", "boundary"} {
		t.Run(stage, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			fixture.session.pumpDone = make(chan struct{})
			fixture.session.pumpCancel = func() {}
			close(fixture.session.pumpDone)

			fixture.route(t, pi.AgentStartEvent{})
			before := fixture.client.lifecycleEvents(t)
			require.Equal(t, string(lifecycle.ForegroundRunning), before[len(before)-1]["state"])

			refused := errors.New("durability refusal with provider sentinel")
			switch stage {
			case "mirror":
				fixture.session.sessionFilePath = t.TempDir()
			case "boundary":
				fixture.store.failSubpath = SessionStoreLifecycleSubpath
				fixture.store.failureErr = refused
			}

			err := fixture.session.Close(t.Context())
			require.Error(t, err)
			require.Equal(t, 1, fixture.process.shutdownCalls)
			require.Equal(t, 1, fixture.process.closeCalls)

			after := fixture.client.lifecycleEvents(t)
			if stage == "mirror" {
				require.Equal(t, before, after, "mirror failure emitted a terminal transition")
			} else {
				require.Len(t, after, len(before)+1)
				require.Equal(t, string(lifecycle.ForegroundIdle), after[len(after)-1]["state"])
				require.Equal(t, string(lifecycle.OutcomeCancelled), after[len(after)-1]["outcome"])
			}

			fixture.session.lcMu.Lock()
			require.True(t, fixture.session.lc.fenced)
			require.True(t, fixture.session.lc.closed)
			if stage == "mirror" {
				require.NotEmpty(t, fixture.session.lc.lostTurnID)
			} else {
				require.Empty(t, fixture.session.lc.lostTurnID,
					"the terminal rung completed before the boundary refusal")
			}
			fixture.session.lcMu.Unlock()
		})
	}
}

func TestCloseCommitRungBlockingUsesCausalBarriers(t *testing.T) {
	for _, stage := range []struct {
		name           string
		subpath        string
		terminalBefore bool
	}{
		{name: "mirror", subpath: SessionStoreMainSubpath},
		{name: "boundary", subpath: SessionStoreLifecycleSubpath, terminalBefore: true},
	} {
		t.Run(stage.name, func(t *testing.T) {
			fixture := newAgentCycleFixture(t)
			fixture.session.pumpDone = make(chan struct{})
			fixture.session.pumpCancel = func() {}
			close(fixture.session.pumpDone)
			fixture.route(t, pi.AgentStartEvent{})
			before := fixture.client.lifecycleEvents(t)
			fixture.store.blockSubpath = stage.subpath
			fixture.store.blockAppend = true
			fixture.store.blockEntered = make(chan struct{})
			fixture.store.blockRelease = make(chan struct{})

			closed := make(chan error, 1)
			go func() { closed <- fixture.session.Close(context.Background()) }()
			<-fixture.store.blockEntered

			during := fixture.client.lifecycleEvents(t)
			if stage.terminalBefore {
				require.Len(t, during, len(before)+1)
				require.Equal(t, string(lifecycle.ForegroundIdle), during[len(during)-1]["state"])
			} else {
				require.Equal(t, before, during, "mirror-blocked close emitted a terminal transition")
			}

			close(fixture.store.blockRelease)
			require.NoError(t, <-closed)
		})
	}
}

func TestBeginCloseFencesVacantAgentStartAtTheInitialTransition(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.session.pumpDone = make(chan struct{})
	fixture.session.pumpCancel = func() {}
	close(fixture.session.pumpDone)

	attempt, owner := fixture.session.beginClose()
	require.True(t, owner)
	require.True(t, attempt.containmentOwner)

	admitted := fixture.outbox.admit(pi.AgentStartEvent{})
	require.Equal(t, outboxShutdown, admitted.disposition)
	require.Nil(t, admitted.cycle)
	require.Nil(t, fixture.outbox.currentCycle())

	require.NoError(t, fixture.session.closeOwned(t.Context(), attempt))
	require.Nil(t, fixture.outbox.currentCycle())
}

// TestCloseDuringAnAgentCycleSettlementWaitsForItsBoundary pins that teardown
// never races a settlement that is already writing: the cycle's own boundary
// lands, the close boundary follows it, and the completing settlement never
// returns the router to a state that would admit a new cycle.
func TestCloseDuringAnAgentCycleSettlementWaitsForItsBoundary(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	fixture.session.pumpDone = make(chan struct{})
	fixture.session.pumpCancel = func() {}

	close(fixture.session.pumpDone)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	fixture.route(t, pi.AgentSettledEvent{})

	require.NoError(t, fixture.session.Close(t.Context()))

	records := fixture.boundaries(t)
	require.Len(t, records, 2, "the cycle's boundary precedes the close boundary")
	require.Equal(t, string(lifecycle.OutcomeSuccess), records[0].Outcome)
	require.Empty(t, records[1].Outcome, "a close boundary records no cycle outcome of its own")

	fixture.route(t, pi.AgentStartEvent{})
	require.Nil(t, fixture.outbox.currentCycle(), "a closed router opens nothing after its settlement")
}

// TestAgentCycleSettlementSurvivesAColdRestore pins the durable half: the
// boundary an agent-origin cycle wrote validates on a later cold read, so a
// restored session resumes from a boundary it can name.
func TestAgentCycleSettlementSurvivesAColdRestore(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	fixture.route(t, pi.AgentStartEvent{})
	fixture.route(t, assistantWork()...)
	fixture.settle(t)

	record, present, err := fixture.session.agent.lastLifecycleBoundary(t.Context(), "id")
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, nativeStateCommitted, record.NativeState)
	require.Equal(t, 2, record.NativeRows)
	require.NotEmpty(t, record.CycleID)
	require.False(t, record.VacancyProven, "a cycle boundary proves no vacancy")
}

// TestActiveRestoreGatesTheSingleForeground pins the load/resume gate. A live
// session answers a restore from the prefix the gate froze, so the gate excludes
// an in-flight prompt and every new agent-origin cycle in one transition and
// holds until the replay is done.
func TestActiveRestoreGatesTheSingleForeground(t *testing.T) {
	fixture := newAgentCycleFixture(t)
	agent := fixture.session.agent

	start := sessionStart{Cwd: t.TempDir(), ResumeID: "id"}
	fixture.session.fingerprint = sessionStartFingerprint(sessionStart{Cwd: start.Cwd})

	agent.mu.Lock()
	if agent.sessions == nil {
		agent.sessions = map[acp.SessionId]*agentSession{}
	}

	agent.sessions["id"] = fixture.session
	agent.mu.Unlock()

	require.NoError(t, fixture.store.Append(t.Context(), SessionKey{SessionID: "id"}, []SessionStoreEntry{
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant}),
	}))
	appendLifecycleBoundaryForRows(t, fixture.store, "id", 1)

	restored, err := agent.restoreSession(t.Context(), "id", start, nil)
	require.NoError(t, err)
	require.Same(t, fixture.session, restored.session)

	// The gate is still held: no prompt is admitted and no cycle may open.
	_, promptErr := fixture.session.acquireTurn(t.Context())
	requireInvalidRequest(t, promptErr)

	fixture.route(t, pi.AgentStartEvent{})
	require.Nil(t, fixture.outbox.currentCycle(), "the gate excludes new agent-origin admission")
	require.NoError(t, fixture.session.poisonedError(), "the opener is held, not refused")

	restored.finish()

	// Released, the record the gate held opens the cycle it always named.
	fixture.session.drainOutbox(t.Context(), fixture.outbox)
	require.NotNil(t, fixture.outbox.currentCycle())

	// And a restore arriving under that cycle is refused rather than answered
	// from a transcript the session has already moved past.
	refused, err := agent.restoreSession(t.Context(), "id", start, nil)
	require.Nil(t, refused.session)
	requireInvalidRequest(t, err)
	require.Contains(t, err.Error(), "session_restore")
}

// TestActiveRestoreIsRefusedDuringAPrompt pins the submission half of the same
// gate: a turn a client submitted holds the session's one foreground exactly as
// agent-origin work does.
func TestActiveRestoreIsRefusedDuringAPrompt(t *testing.T) {
	fixture := newAgentCycleFixture(t)

	release, err := fixture.session.acquireTurn(t.Context())
	require.NoError(t, err)

	_, restoreErr := fixture.session.beginRestore(t.Context())
	requireInvalidRequest(t, restoreErr)
	require.Contains(t, restoreErr.Error(), "session_restore")

	release()

	restoreRelease, err := fixture.session.beginRestore(t.Context())
	require.NoError(t, err)

	// And the gate is exclusive against a second restore.
	_, secondErr := fixture.session.beginRestore(t.Context())
	requireInvalidRequest(t, secondErr)

	restoreRelease()
}

// TestFullPumpDelayedAgentCycleFollowsAPrompt is the whole path end to end over
// the real pump: a client prompt settles and returns, and the work pi began on
// its own immediately afterwards is projected as its own cycle — route-less
// output, its native transcript and boundary record durable before the terminal
// activity idle the host reduces.
func TestFullPumpDelayedAgentCycleFollowsAPrompt(t *testing.T) {
	fixture := newLivePumpFixture(t)

	fed := make(chan struct{})

	fixture.pi.promptFunc = func(context.Context, string) error { return nil }
	fixture.pi.afterAccepted = func() {
		go func() {
			defer close(fed)

			fixture.pi.events <- pi.MessageStartEvent{Message: pi.AgentMessage{
				Role: messageRoleAssistant, Model: "m", Provider: "p",
			}}
			fixture.pi.events <- pi.MessageUpdateEvent{AssistantMessageEvent: pi.AssistantMessageEvent{
				Type: assistantEventTextDelta, Delta: "answering the prompt",
			}}
			fixture.pi.events <- pi.MessageEndEvent{Message: pi.AgentMessage{
				Role: messageRoleAssistant, ACPMessageID: "message-prompt", StopReason: stopReasonStop,
			}}
			fixture.pi.events <- pi.AgentSettledEvent{}

			// The agent begins its own cycle in the same breath. Every record
			// is retained behind the settling foreground and replayed by the
			// drain the prompt's release wakes.
			fixture.pi.events <- pi.AgentStartEvent{}
			fixture.pi.events <- pi.MessageStartEvent{Message: pi.AgentMessage{Role: messageRoleAssistant}}
			fixture.pi.events <- pi.MessageUpdateEvent{AssistantMessageEvent: pi.AssistantMessageEvent{
				Type: assistantEventTextDelta, Delta: "work nobody asked for",
			}}
			fixture.pi.events <- pi.MessageEndEvent{Message: pi.AgentMessage{
				Role: messageRoleAssistant, ACPMessageID: "message-agent", StopReason: stopReasonStop,
			}}
			fixture.pi.events <- pi.AgentSettledEvent{}
		}()
	}

	request := TextPromptRequest("id", "turn-nonce", "hi")
	request.Meta[lifecycleMetaKey] = map[string]any{
		"version":    1,
		"submission": map[string]any{"submissionId": "submission-1", "clientNonce": "client-nonce-1"},
	}

	response, err := fixture.session.Prompt(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

	<-fed

	require.Eventually(t, func() bool {
		events := fixture.client.lifecycleEvents(t)
		if len(events) == 0 {
			return false
		}

		last := events[len(events)-1]

		return last["state"] == string(lifecycle.ForegroundIdle) &&
			last["cause"] == string(lifecycle.CauseActivity)
	}, 5*time.Second, time.Millisecond, "the agent-origin cycle reached its terminal idle")

	fixture.quiesce()

	events := fixture.client.lifecycleEvents(t)
	causes := make([]string, 0, len(events))

	for _, event := range events {
		if event["type"] != string(lifecycle.EventStateUpdate) {
			continue
		}

		state, stateOK := event["state"].(string)
		cause, causeOK := event["cause"].(string)
		require.True(t, stateOK && causeOK, "a state update names both a state and a cause")

		causes = append(causes, state+":"+cause)
	}

	require.Equal(t, []string{
		string(lifecycle.ForegroundRunning) + ":" + string(lifecycle.CauseSubmission),
		string(lifecycle.ForegroundIdle) + ":" + string(lifecycle.CauseSubmission),
		string(lifecycle.ForegroundRunning) + ":" + string(lifecycle.CauseActivity),
		string(lifecycle.ForegroundIdle) + ":" + string(lifecycle.CauseActivity),
	}, causes)

	// The prompt's own output carried its route; the agent-origin cycle's did
	// not, because there is no submission to name.
	var routed, unrouted int

	for _, notification := range fixture.client.snapshot() {
		chunk := notification.Update.AgentMessageChunk
		if chunk == nil || chunk.Content.Text == nil {
			continue
		}

		if _, carries := notification.Meta[routeMetaKey]; carries {
			require.Equal(t, "answering the prompt", chunk.Content.Text.Text)

			routed++

			continue
		}

		require.Equal(t, "work nobody asked for", chunk.Content.Text.Text)

		unrouted++
	}

	require.Equal(t, 1, routed)
	require.Equal(t, 1, unrouted)

	records := fixture.boundaries(t)
	require.Len(t, records, 2, "the prompt's boundary, then the cycle's own")
	require.Equal(t, string(lifecycle.OutcomeSuccess), records[1].Outcome)
	require.Equal(t, nativeStateCommitted, records[1].NativeState)

	idlePosition := -1

	for index, notification := range fixture.client.snapshot() {
		if _, carries := notification.Meta[lifecycleMetaKey]; carries {
			idlePosition = index
		}
	}

	require.GreaterOrEqual(t, idlePosition, fixture.store.lastMark(),
		"the activity idle was not on the wire when its boundary was committed")
}
