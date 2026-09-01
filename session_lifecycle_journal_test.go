package piacp

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

func TestLifecycleBoundaryDecoderEdges(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		``,
		`[]`,
		`{"`,
		`{"allowed":`,
		`{"allowed":1`,
		`{} x`,
	} {
		_, err := decodeLifecycleBoundaryObject([]byte(raw), "edge", []string{"allowed"})
		require.Error(t, err, raw)
	}

	for _, raw := range []string{
		``,
		`[]`,
		`{"`,
		`{"outer":{"inner":1}`,
		`{"array":[1, {"nested": true}]`,
		`{"array":[`,
		`{"array":[1`,
		`{"array":[{"nested":}`,
		`{} {}`,
		`{} x`,
	} {
		require.Error(t, validateLifecycleBoundaryDynamicObject([]byte(raw), "edge"), raw)
	}
	require.NoError(t, validateLifecycleBoundaryDynamicObject(
		[]byte(`{"array":[1,{"nested":[true,null]}]}`), "edge",
	))

	decoder := json.NewDecoder(bytes.NewBufferString(`{"unterminated":`))
	_, err := decoder.Token()
	require.NoError(t, err)
	require.Error(t, walkLifecycleBoundaryDynamicObject(decoder, "edge"))

	for _, entry := range []SessionStoreEntry{
		json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		json.RawMessage(`{"version":1,"configuration":{"env":{},"extraPathDirs":["relative"]},"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
	} {
		_, err = decodeLifecycleBoundaryRecord(entry)
		require.Error(t, err)
	}

	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(t.Context(), SessionKey{
		SessionID: "edge", Subpath: SessionStoreLifecycleSubpath,
	}, []SessionStoreEntry{json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1}`)}))
	_, found, err := NewAgent(WithSessionStore(store)).lastLifecycleBoundary(t.Context(), "edge")
	require.False(t, found)
	require.Error(t, err)

	session, _ := lifecycleSession(t, false)
	session.fencePersistence()
	require.True(t, session.persistFenced)
	require.NoError(t, session.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{}))
}

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

	valid := json.RawMessage(`{"version":1,"configuration":{"env":null,"extraPathDirs":null},"streamId":"current","nativeRows":2,"nativeState":"committed","recordedAt":1}`)
	record, err := decodeLifecycleBoundaryRecord(valid)
	require.NoError(t, err)
	require.Equal(t, "current", record.StreamID)
	require.Equal(t, 2, record.NativeRows)

	invalid := map[string]json.RawMessage{
		"malformed":                 json.RawMessage(`not-json`),
		"trailing value":            json.RawMessage(string(valid) + ` {}`),
		"malformed trailing data":   json.RawMessage(string(valid) + ` {`),
		"unknown field":             json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1,"extra":true}`),
		"missing required":          json.RawMessage(`{"version":1,"streamId":"","nativeState":"committed","recordedAt":1}`),
		"null optional":             json.RawMessage(`{"version":1,"streamId":"","turnId":null,"nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"unsupported version":       json.RawMessage(`{"version":2,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":1}`),
		"negative rows":             json.RawMessage(`{"version":1,"streamId":"","nativeRows":-1,"nativeState":"committed","recordedAt":1}`),
		"nonpositive timestamp":     json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"committed","recordedAt":0}`),
		"unsupported native state":  json.RawMessage(`{"version":1,"streamId":"","nativeRows":0,"nativeState":"invented","recordedAt":1}`),
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
			encoded := string(entry)
			if strings.HasPrefix(encoded, "{") && !strings.Contains(encoded, `"configuration"`) {
				entry = json.RawMessage(`{"configuration":{"env":null,"extraPathDirs":null},` + strings.TrimPrefix(encoded, "{"))
			}

			_, decodeErr := decodeLifecycleBoundaryRecord(entry)
			require.Error(t, decodeErr)
		})
	}
}

// TestLastLifecycleBoundaryRejectsAmbiguousSchemaFields exercises the
// production journal reader rather than a test-only decoder. The closed
// boundary/configuration objects reject aliases and duplicates, while the
// dynamic environment rejects exact duplicates recursively without folding
// case. Ambiguous durable facts make the whole restore ineligible.
func TestLastLifecycleBoundaryRejectsAmbiguousSchemaFields(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		boundary json.RawMessage
		want     string
	}{
		"top-level duplicate": {
			boundary: json.RawMessage(`{"version":1,"version":1,"configuration":{"env":null,"extraPathDirs":null},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
			want:     "duplicate",
		},
		"configuration duplicate": {
			boundary: json.RawMessage(`{"version":1,"configuration":{"env":null,"env":{},"extraPathDirs":null},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
			want:     "duplicate",
		},
		"environment duplicate": {
			boundary: json.RawMessage(`{"version":1,"configuration":{"env":{"TOKEN":"one","TOKEN":"two"},"extraPathDirs":[]},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
			want:     "duplicate",
		},
		"nested environment duplicate": {
			boundary: json.RawMessage(`{"version":1,"configuration":{"env":{"nested":{"token":"one","token":"two"}},"extraPathDirs":[]},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
			want:     "duplicate",
		},
		"top-level case alias": {
			boundary: json.RawMessage(`{"Version":1,"configuration":{"env":null,"extraPathDirs":null},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
			want:     "unknown",
		},
		"configuration case alias": {
			boundary: json.RawMessage(`{"version":1,"configuration":{"Env":null,"extraPathDirs":null},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
			want:     "unknown",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := NewInMemorySessionStore()
			require.NoError(t, store.Append(t.Context(), SessionKey{
				SessionID: "session",
				Subpath:   SessionStoreLifecycleSubpath,
			}, []SessionStoreEntry{test.boundary}))

			agent := NewAgent(WithSessionStore(store))
			_, found, err := agent.lastLifecycleBoundary(t.Context(), "session")
			require.False(t, found)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestLastLifecycleBoundaryAcceptsCaseDistinctEnvironmentKeys(t *testing.T) {
	t.Parallel()

	boundary := json.RawMessage(`{"version":1,"configuration":{"env":{"Token":"one","TOKEN":"two"},"extraPathDirs":[]},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`)
	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(t.Context(), SessionKey{
		SessionID: "session",
		Subpath:   SessionStoreLifecycleSubpath,
	}, []SessionStoreEntry{boundary}))

	agent := NewAgent(WithSessionStore(store))
	record, found, err := agent.lastLifecycleBoundary(t.Context(), "session")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, map[string]string{"Token": "one", "TOKEN": "two"}, record.Configuration.Env)
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

	require.NoError(t, s.commitCloseForegroundMirror(t.Context()))
	require.NoError(t, s.commitCloseResumableBoundary(t.Context(), true, s.prepareCloseLifecycleTerminal()))

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
