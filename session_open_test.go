package piacp

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

type cancelAfterOpeningUpdateClient struct {
	*directAgentClient
	cancel context.CancelFunc
}

func (c *cancelAfterOpeningUpdateClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	c.cancel()

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

// TestPublishSessionOpen pins the establishing snapshot: it is emitted exactly
// once, and a lifecycle stream that cannot open is fenced and recorded rather
// than continued from a first event that never landed.
func TestPublishSessionOpen(t *testing.T) {
	t.Run("publishes exactly once", func(t *testing.T) {
		s, client := lifecycleSession(t, false)
		bindTestEstablishingOutbox(s, 1, nil, nil)
		require.NoError(t, s.publishSessionOpen(t.Context()))
		events := len(client.notifications)
		require.NotZero(t, events)
		require.NoError(t, s.publishSessionOpen(t.Context()))
		require.Len(t, client.notifications, events)
	})

	t.Run("establishment failure emits no incarnation", func(t *testing.T) {
		const secret = "initial-catalog-lifecycle-secret-sentinel"
		logs := &strings.Builder{}
		s, client := lifecycleSession(t, false)
		bindTestEstablishingOutbox(s, 1, newStubProcess(false), newStubPiClient())
		s.agent.log = slog.New(slog.NewTextHandler(logs, nil))
		client.updateErr = errors.New(secret)
		require.Error(t, s.publishSessionOpen(t.Context()))
		require.Nil(t, s.lc.stream)
		process, ok := s.proc.(*stubProcess)
		require.True(t, ok)
		require.Equal(t, 1, process.shutdownCalls)
		require.Equal(t, 1, process.closeCalls)
		require.NotContains(t, logs.String(), secret)
	})
}

func TestSessionOpenDeliveryFailureSynchronouslyPoisonsAndContainsGeneration(t *testing.T) {
	session, client := lifecycleSession(t, false)
	process := newStubProcess(false)
	native := newStubPiClient()
	bindTestEstablishingOutbox(session, 3, process, native)
	client.updateErr = errors.New("required catalog refused")

	err := session.publishSessionOpen(t.Context())
	require.Error(t, err)
	require.Error(t, session.poisonedError())
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
	configOutbox, configClient, configDone, configErr := session.beginConfiguration(t.Context())
	require.Error(t, configErr)
	require.Nil(t, configOutbox)
	require.Nil(t, configClient)
	require.Nil(t, configDone)
	_, promptErr := session.acquireTurn(t.Context())
	require.Error(t, promptErr)
	_, restoreErr := session.beginRestore(t.Context())
	require.Error(t, restoreErr)
}

func TestGenerationDoneContextTracksExactGenerationCancellation(t *testing.T) {
	done := make(chan struct{})
	ctx := generationDoneContext{done: done}

	require.NoError(t, ctx.Err())
	require.Equal(t, (<-chan struct{})(done), ctx.Done())
	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline)
	require.Nil(t, ctx.Value("key"))

	close(done)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestStartupRecordsWaitForSessionIdentityAndOpeningSnapshot(t *testing.T) {
	client := newRecordingClient()
	agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	agent.conn = client
	agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
	process := newStubProcess(false)
	native := newStubPiClient()
	native.respondFunc = func(pi.UIResponse) {
		client.mu.Lock()
		client.trace = append(client.trace, "ui")
		client.mu.Unlock()
	}
	session := &agentSession{
		agent:       agent,
		rawMessages: rawMessageConfig{All: true},
	}
	outbox := bindTestEstablishingOutbox(session, 1, process, native)

	message, err := pi.DecodeMessage([]byte(`{"type":"agent_start"}`))
	require.NoError(t, err)
	session.routeNativeEvent(t.Context(), outbox, message.Event)
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{ID: "startup-dialog", Method: uiMethodInput})
	client.mu.Lock()
	require.Empty(t, client.trace)
	client.mu.Unlock()
	require.Empty(t, session.id)

	session.id = "established"
	require.NoError(t, session.publishSessionOpen(t.Context()))
	session.drainOutbox(t.Context(), outbox)

	client.mu.Lock()
	trace := append([]string(nil), client.trace...)
	client.mu.Unlock()
	require.Equal(t, []string{
		"typed",
		"lifecycle:" + string(lifecycle.EventSnapshot),
		"raw",
		"lifecycle:" + string(lifecycle.EventStateUpdate),
		"ui",
	}, trace)
	native.mu.Lock()
	require.Equal(t, []pi.UIResponse{pi.UICancelResponse("startup-dialog")}, native.responses)
	native.mu.Unlock()
}

func TestFailedStartupContainsBufferedRecordsWithoutEmission(t *testing.T) {
	client := newRecordingClient()
	client.failOrdinary = true
	agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	agent.conn = client
	agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
	process := newStubProcess(false)
	native := newStubPiClient()
	session := &agentSession{
		agent:       agent,
		id:          "failed-establishment",
		rawMessages: rawMessageConfig{All: true},
	}
	outbox := bindTestEstablishingOutbox(session, 1, process, native)
	session.routeNativeEvent(t.Context(), outbox, pi.AgentStartEvent{})
	session.routeUIRequest(t.Context(), outbox, pi.UIRequest{ID: "never-visible", Method: uiMethodInput})

	require.Error(t, session.publishSessionOpen(context.Background()))
	client.mu.Lock()
	require.Empty(t, client.trace)
	require.Empty(t, client.notified)
	client.mu.Unlock()
	require.Nil(t, session.lc.stream)
	native.mu.Lock()
	require.Empty(t, native.responses)
	native.mu.Unlock()
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
}

func TestPostResponseHookAdmissionFailsForEveryClosedOwner(t *testing.T) {
	session := &agentSession{}
	_, _, admitted := session.admitPostResponseHook()
	require.False(t, admitted)
	session.outbox = newTestSessionOutbox(1)
	session.closing = true
	_, _, admitted = session.admitPostResponseHook()
	require.False(t, admitted)
	session.closing = false
	session.outbox.interactionsClosed = true
	_, _, admitted = session.admitPostResponseHook()
	require.False(t, admitted)
	session.outbox.interactionsClosed = false
	session.outbox.producers.releaseRoot()
	_, _, admitted = session.admitPostResponseHook()
	require.False(t, admitted)

	session.outbox = newTestSessionOutbox(2)
	hookCtx, release, admitted := session.admitPostResponseHook()
	require.True(t, admitted)
	require.NotNil(t, hookCtx)
	release()
}

func TestPublishSessionOpenCancellationAndLifecycleFailureBoundaries(t *testing.T) {
	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, (&agentSession{}).publishSessionOpen(ctx), context.Canceled)
	})

	t.Run("cancellation after catalog has no continuation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		host := &cancelAfterOpeningUpdateClient{directAgentClient: newDirectAgentClient(), cancel: cancel}
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		agent.conn = host
		session := &agentSession{agent: agent, id: "cancelled-open"}
		bindTestEstablishingOutbox(session, 1, newStubProcess(false), newStubPiClient())
		require.ErrorIs(t, session.publishSessionOpen(ctx), context.Canceled)
		require.Nil(t, session.lc.stream)
	})

	t.Run("lifecycle snapshot failure contains", func(t *testing.T) {
		host := &lifecycleFailingClient{directAgentClient: newDirectAgentClient(), err: errors.New("snapshot refused")}
		agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
		agent.conn = host
		agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
		process := newStubProcess(false)
		session := &agentSession{agent: agent, id: "snapshot-failure"}
		bindTestEstablishingOutbox(session, 1, process, newStubPiClient())
		require.Error(t, session.publishSessionOpen(t.Context()))
		require.Equal(t, 1, process.shutdownCalls)
		require.Equal(t, 1, process.closeCalls)
	})

	// A host may close a session at any point, including while the snapshot
	// deferred behind its own establishing response is still landing. That
	// sequence is legal, so the opening stops without poisoning the session or
	// raising a containment the close ladder already owns.
	t.Run("close claimed mid-open retires the opening", func(t *testing.T) {
		logs := &strings.Builder{}
		session, _ := lifecycleSession(t, false)
		process := newStubProcess(false)
		outbox := bindTestEstablishingOutbox(session, 1, process, newStubPiClient())
		session.agent.log = slog.New(slog.NewTextHandler(logs, nil))

		outbox.mu.Lock()
		outbox.claimForCloseLocked()
		outbox.mu.Unlock()

		require.ErrorIs(t, session.publishSessionOpen(t.Context()), errGenerationRetired)
		require.NoError(t, session.poisonedError(), "a closed session is not a poisoned one")
		require.Zero(t, process.shutdownCalls, "the close ladder owns this generation's containment")
		require.Zero(t, process.closeCalls)
		require.Empty(t, logs.String(), "a legal close is not an invariant violation")
	})

	t.Run("already released establishment gate contains", func(t *testing.T) {
		session, _ := lifecycleSession(t, false)
		process := newStubProcess(false)
		native := newStubPiClient()
		outbox := newTestSessionOutbox(1)
		bindTestRuntime(outbox, process, native, nil, nil, nil)
		session.proc, session.client, session.outbox = process, native, outbox
		session.pumpGeneration = 1
		require.ErrorIs(t, session.publishSessionOpen(t.Context()), pi.ErrTransportClosed)
		require.Error(t, session.poisonedError())
		require.Equal(t, 1, process.shutdownCalls)
		require.Equal(t, 1, process.closeCalls)
	})
}
