package piacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

// reduceAll replays one session's recorded notifications through the core
// lifecycle reducer and returns the state they produce.
func reduceAll(t *testing.T, sessionID acp.SessionId, updates []acp.SessionNotification) lifecycle.State {
	t.Helper()

	reducer := lifecycle.NewReducer(lifecycle.Options{Negotiated: lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}})

	for _, update := range updates {
		if update.SessionId != sessionID {
			continue
		}

		params, err := json.Marshal(update)
		require.NoError(t, err)

		if err := reducer.ReduceSessionUpdate(params); err != nil {
			require.ErrorIs(t, err, lifecycle.ErrNoEnvelope, "reducer refused a notification")
		}
	}

	return reducer.State()
}

func TestLifecyclePromptCycle(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithSessionStore(nil))
	h.initialize(withLifecycle())
	session := h.newSession()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 1 })

	first := h.rec.snapshot()
	require.NotNil(t, first[0].Update.AvailableCommandsUpdate, "commands precede the snapshot")
	require.Equal(t, []string{"lifecycle_snapshot"}, eventTypes(lifecycleEvents(first)))

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	events := lifecycleEvents(h.rec.snapshot())
	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running", "state_update:idle"}, eventTypes(events))
	require.Equal(t, "sub-1", events[1]["submissionId"])
	require.Equal(t, "non-1", events[1]["clientNonce"])
	require.Equal(t, "end_turn", events[3]["stopReason"])
	require.Equal(t, "success", events[3]["outcome"])

	state := reduceAll(t, session.SessionId, h.rec.snapshot())
	require.True(t, state.Settled())
	require.Len(t, state.Turns, 1)
}

func TestLifecycleBlockingPermission(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "TOOL", promptMeta(1))
	require.NoError(t, err)

	types := eventTypes(lifecycleEvents(h.rec.snapshot()))
	require.Equal(t, []string{
		"lifecycle_snapshot", "prompt_accepted", "state_update:running",
		"action_update:pending", "state_update:requires_action",
		"action_update:accepted", "state_update:running", "state_update:idle",
	}, types)

	require.Len(t, h.rec.permissions, 1)
	correlation, ok := h.rec.permissions[0].Meta[wire.LifecycleKey].(map[string]any)
	require.True(t, ok)
	action, _ := correlation["action"].(map[string]any)
	require.NotEmpty(t, action["actionId"])

	state := reduceAll(t, session.SessionId, h.rec.snapshot())
	require.True(t, state.Settled())
	require.Len(t, state.Actions, 1)
}

func TestLifecycleAgentOriginTurn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "AGENTWORK", promptMeta(1))
	require.NoError(t, err)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 6 })

	events := lifecycleEvents(h.rec.snapshot())
	require.Equal(t, []string{
		"lifecycle_snapshot", "prompt_accepted", "state_update:running", "state_update:idle",
		"state_update:running", "state_update:idle",
	}, eventTypes(events))
	require.Equal(t, "activity", events[4]["cause"])
	require.Equal(t, "activity", events[5]["cause"])

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return agentText(updates) == "Hello worldHello world" })

	state := reduceAll(t, session.SessionId, h.rec.snapshot())
	require.True(t, state.Settled())
	require.Len(t, state.Turns, 2)
}

func TestLifecycleCancelledTurn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(session.SessionId, "SLOW", promptMeta(1))
		done <- resp
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return len(lifecycleEvents(updates)) >= 3
	})

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)

	events := lifecycleEvents(h.rec.snapshot())
	last := events[len(events)-1]
	require.Equal(t, "idle", last["state"])
	require.Equal(t, "cancelled", last["outcome"])
	require.Equal(t, "cancelled", last["stopReason"])
}

func TestLifecycleProcessDeathFencesAndRelaunchReopens(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "DIE", promptMeta(1))
	data := requestErrorData(t, err)
	require.Equal(t, "pi_turn_failed", data["error"])
	require.Equal(t, "process_exit", data["cause"])
	require.Contains(t, data["message"], "fatal: dead")

	events := lifecycleEvents(h.rec.snapshot())
	last := events[len(events)-1]
	require.Equal(t, "idle", last["state"])
	require.Equal(t, "failed", last["outcome"])
	require.NotContains(t, last, "stopReason")

	firstStream, _ := h.rec.snapshot()[len(h.rec.snapshot())-1].Meta[wire.LifecycleKey].(map[string]any)

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	updates := h.rec.snapshot()
	types := eventTypes(lifecycleEvents(updates))
	require.Equal(t, "lifecycle_snapshot", types[len(types)-4])

	lastStream, _ := updates[len(updates)-1].Meta[wire.LifecycleKey].(map[string]any)
	require.NotEqual(t, firstStream["streamId"], lastStream["streamId"])
}

func TestLifecycleCloseEmitsNothingAfterFence(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)

	before := len(lifecycleEvents(h.rec.snapshot()))

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.Len(t, lifecycleEvents(h.rec.snapshot()), before)
}

func TestLifecycleDormantWithoutNegotiation(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "TOOL", nil)
	require.NoError(t, err)
	require.Empty(t, lifecycleEvents(h.rec.snapshot()))
	require.Len(t, h.rec.permissions, 1)
	require.NotContains(t, h.rec.permissions[0].Meta, wire.LifecycleKey)
}

// An incarnation ends with its native generation, including a clean exit that
// follows a turn the session already settled.
func TestLifecycleCleanExitAfterSettledTurnFences(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "QUIT", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	// The native process is gone; the next prompt relaunches it.
	settled := h.rec.snapshot()
	firstStream, _ := settled[len(settled)-1].Meta[wire.LifecycleKey].(map[string]any)

	// A prompt that arrives before the adapter has observed the exit fails on
	// the lost transport; the one after it runs on the replacement process.
	for attempt := range 3 {
		resp, err = h.prompt(session.SessionId, "HELLO", promptMeta(2+attempt))
		if err == nil {
			break
		}
	}

	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	updates := h.rec.snapshot()
	types := eventTypes(lifecycleEvents(updates))
	require.Equal(t, "lifecycle_snapshot", types[len(types)-4], "the replacement process opens its own incarnation: %v", types)

	for _, update := range updates[len(settled):] {
		envelope, ok := update.Meta[wire.LifecycleKey].(map[string]any)
		if !ok {
			continue
		}

		require.NotEqual(t, firstStream["streamId"], envelope["streamId"],
			"an incarnation carried work after the native process behind it exited")
	}
}

// Close terminalizes an open agent-origin cycle before it fences the stream.
func TestLifecycleCloseTerminalizesAgentCycle(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "AGENTHANG", promptMeta(1))
	require.NoError(t, err)

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		types := eventTypes(lifecycleEvents(updates))

		return len(types) >= 5 && types[4] == "state_update:running"
	})

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	events := lifecycleEvents(h.rec.snapshot())
	last := events[len(events)-1]
	require.Equal(t, "idle", last["state"])
	require.Equal(t, "cancelled", last["outcome"])
	require.Equal(t, "activity", last["cause"])
}

// A relaunch republishes the command catalog for the new incarnation.
func TestCommandsReEmittedAfterRelaunch(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "QUIT", nil)
	require.NoError(t, err)

	before := commandCatalogs(h.rec.snapshot())

	for range 3 {
		if _, err = h.prompt(session.SessionId, "HELLO", nil); err == nil {
			break
		}
	}

	require.NoError(t, err)

	require.Greater(t, commandCatalogs(h.rec.snapshot()), before, "the relaunched incarnation republishes its catalog")
}

func commandCatalogs(updates []acp.SessionNotification) int {
	count := 0

	for _, update := range updates {
		if update.Update.AvailableCommandsUpdate != nil {
			count++
		}
	}

	return count
}

func TestCloseBackgroundCycleRequiresCommit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "failed"}[fail], func(t *testing.T) {
			store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store))
			h.initialize(withLifecycle())
			created := h.newSession()
			_, err := h.prompt(created.SessionId, "AGENTHANG", promptMeta(1))
			require.NoError(t, err)
			h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
				events := lifecycleEvents(updates)

				return len(events) >= 5 && events[4]["state"] == "running"
			})
			before := len(lifecycleEvents(h.rec.snapshot()))
			store.fail.Store(fail)
			_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
			store.fail.Store(false)
			if fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			terminal := 0
			for _, event := range lifecycleEvents(h.rec.snapshot())[before:] {
				if event["state"] == "idle" {
					terminal++
					require.Equal(t, "cancelled", event["outcome"])
				}
			}
			if fail {
				require.Zero(t, terminal)
			} else {
				require.Equal(t, 1, terminal)
			}
		})
	}
}
