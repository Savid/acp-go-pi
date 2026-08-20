package piacp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

// TestLifecycleBoundaryCommitFailure pins that a store the boundary record
// cannot append to fails the boundary instead of letting a terminal event
// stand behind nothing durable.
func TestLifecycleBoundaryCommitFailure(t *testing.T) {
	store := newFaultySessionStore()
	s, _ := lifecycleSession(t, false)
	s.agent.options.SessionStore = store
	store.appendErr = errors.New("durability unavailable")

	require.ErrorIs(t, s.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{StreamID: "stream"}), errLifecycleBoundaryCommit)

	s.id = ""
	require.NoError(t, s.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{}),
		"a session with no native identity has no key a boundary could record under")
}

// TestLifecycleBoundaryRestoreValidation pins the hard journal cut: malformed,
// unknown, unsupported, and semantically incoherent records fail closed rather
// than becoming an absent boundary or an unearned opening fact.
func TestLifecycleBoundaryRestoreValidation(t *testing.T) {
	t.Parallel()

	valid := json.RawMessage(`{"version":1,"streamId":"current","nativeRows":2,"nativeState":"committed","recordedAt":1}`)
	record, err := decodeLifecycleBoundaryRecord(valid)
	require.NoError(t, err)
	require.Equal(t, "current", record.StreamID)
	require.Equal(t, 2, record.NativeRows)

	invalid := map[string]json.RawMessage{
		"malformed":                 json.RawMessage(`not-json`),
		"trailing value":            json.RawMessage(string(valid) + ` {}`),
		"malformed trailing data":   json.RawMessage(string(valid) + ` {`),
		"unknown field":             json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1,"legacy":true}`),
		"missing required":          json.RawMessage(`{"version":1,"streamId":"","nativeState":"committed","recordedAt":1}`),
		"null optional":             json.RawMessage(`{"version":1,"streamId":"","turnId":null,"nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"unsupported version":       json.RawMessage(`{"version":2,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"negative rows":             json.RawMessage(`{"version":1,"streamId":"","nativeRows":-1,"nativeState":"committed","recordedAt":1}`),
		"nonpositive timestamp":     json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":0}`),
		"unsupported disposition":   json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"legacy","recordedAt":1}`),
		"uncommitted vacancy":       json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"retained","vacancyProven":true,"recordedAt":1}`),
		"identity without stream":   json.RawMessage(`{"version":1,"streamId":"","turnId":"turn","cycleId":"cycle","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"stop without outcome":      json.RawMessage(`{"version":1,"streamId":"","stopReason":"end_turn","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"unsupported outcome":       json.RawMessage(`{"version":1,"streamId":"","outcome":"unknown","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"failed with stop":          json.RawMessage(`{"version":1,"streamId":"","outcome":"failed","stopReason":"end_turn","nativeRows":0,"nativeState":"retained","recordedAt":1}`),
		"nonfailure without stop":   json.RawMessage(`{"version":1,"streamId":"","outcome":"success","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"nonfailure invalid stop":   json.RawMessage(`{"version":1,"streamId":"","outcome":"success","stopReason":"stop","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"wrong required field type": json.RawMessage(`{"version":1,"streamId":"","nativeRows":"zero","nativeState":"committed","recordedAt":1}`),
	}
	for name, entry := range invalid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, decodeErr := decodeLifecycleBoundaryRecord(entry)
			require.Error(t, decodeErr)
		})
	}
}

// TestCloseBoundaryOnAnIdleForegroundReadsBack pins the journal reader against
// the shape its own writer produces. A settled turn leaves the foreground idle:
// the terminal transition clears the turn and the cycle outlives it, so the
// close boundary records a cycle with no turn. That is the only shape an idle
// foreground may carry — a turnId there is a malformed envelope, present only
// while a turn is open — so the journal a resume stands behind reads it back
// instead of refusing it. A reader that refused it would strand every session
// on its first rotation.
func TestCloseBoundaryOnAnIdleForegroundReadsBack(t *testing.T) {
	s, _ := lifecycleSession(t, true)
	require.NoError(t, s.openLifecycleStream(t.Context(), 1))
	require.NoError(t, s.lifecycleAcceptTurn(t.Context(), testSubmission()))
	require.NoError(t, s.lifecycleSettleTurn(t.Context(), lifecycle.StopReasonEndTurn, lifecycle.OutcomeSuccess))

	streamID, turnID, cycleID := s.lifecycleIdentity()
	require.NotEmpty(t, streamID)
	require.Empty(t, turnID, "the terminal idle clears the turn")
	require.NotEmpty(t, cycleID, "the cycle outlives the turn it carried")

	require.NoError(t, s.commitCloseBoundary(t.Context(), true))

	record, found, err := s.agent.lastLifecycleBoundary(t.Context(), string(s.id))
	require.NoError(t, err, "the close boundary the writer just recorded is readable")
	require.True(t, found)
	require.Equal(t, streamID, record.StreamID)
	require.Equal(t, cycleID, record.CycleID)
	require.Empty(t, record.TurnID)
}

func TestLastLifecycleBoundaryValidatesTheCompleteJournal(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(testContainmentOption(), WithSessionStore(store))

	record, found, err := agent.lastLifecycleBoundary(t.Context(), "id")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, lifecycleBoundaryRecord{}, record)

	appendLifecycleBoundaryForRows(t, store, "id", 2)
	appendLifecycleBoundaryForRows(t, store, "id", 1)
	_, _, err = agent.lastLifecycleBoundary(t.Context(), "id")
	require.ErrorContains(t, err, "regressed")

	loadErr := errors.New("journal unavailable")
	agent.options.SessionStore = &lifecycleLoadFailingStore{SessionStore: store, err: loadErr}
	_, _, err = agent.lastLifecycleBoundary(t.Context(), "id")
	require.ErrorIs(t, err, loadErr)
}

func TestSessionRestoreMethodsRejectAnInvalidLifecycleJournal(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Agent) error{
		"load": func(agent *Agent) error {
			_, err := agent.LoadSession(t.Context(), LoadSessionRequest(validSessionUUID, "/cwd"))

			return err
		},
		"resume": func(agent *Agent) error {
			_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest(validSessionUUID, "/cwd"))

			return err
		},
		"fork": func(agent *Agent) error {
			_, err := agent.handleForkSession(t.Context(), forkRaw(t, ForkSessionRequest(validSessionUUID, "/cwd")))

			return err
		},
	}

	for name, invoke := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewInMemorySessionStore()
			require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, []SessionStoreEntry{
				json.RawMessage(`{"type":"session","id":"01234567-89ab-cdef-0123-456789abcdef","cwd":"/cwd"}`),
			}))
			require.NoError(t, store.Append(t.Context(), SessionKey{
				SessionID: validSessionUUID,
				Subpath:   SessionStoreLifecycleSubpath,
			}, []SessionStoreEntry{json.RawMessage(`{"version":1,"vacancyProven":true}`)}))

			agent := NewAgent(testContainmentOption(), WithSessionStore(store))
			require.ErrorContains(t, invoke(agent), "decode lifecycle journal")
		})
	}
}
