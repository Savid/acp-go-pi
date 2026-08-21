package piacp

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

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
	require.ErrorIs(t, session.lifecycleGenerationQuarantine(3), pi.ErrProcessContainmentIncomplete)
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
			want: pi.ErrProcessContainmentIncomplete,
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
