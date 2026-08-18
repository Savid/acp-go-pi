package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// containedConfiguration is the answer a session whose close boundary proves
// whole-tree vacancy gives: updates between prompts, no activity kind, and the
// process-containment proof class.
func containedConfiguration() Negotiated {
	return Negotiated{
		Versions:                []int{Version},
		UpdatesOutsidePrompt:    true,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        ProofClassProcessContainment,
		ActivityKinds:           []ActivityKind{},
	}
}

// reduceEnvelopes decodes each emitted envelope from a session/update
// notification and reduces it, which is the only measure of wire legality that
// counts.
func reduceEnvelopes(t *testing.T, negotiated Negotiated, envelopes []map[string]any) State {
	t.Helper()

	reducer := NewReducer(Options{Negotiated: negotiated})

	for index, envelope := range envelopes {
		params, err := json.Marshal(map[string]any{
			"sessionId": "sess-1",
			"update":    map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
			metaField:   map[string]any{MetaKey: envelope},
		})
		require.NoError(t, err)
		require.NoError(t, reducer.ReduceSessionUpdate(params), "envelope %d", index)
	}

	return reducer.State()
}

func emitAll(t *testing.T, stream *Stream, events ...Event) []map[string]any {
	t.Helper()

	envelopes := make([]map[string]any, 0, len(events))

	for _, event := range events {
		envelope, err := stream.Emit(event)
		require.NoError(t, err)

		envelopes = append(envelopes, envelope)
	}

	return envelopes
}

// TestEmittedStreamReducesThroughTheSameReducer drives the exact shape one
// ordinary prompt emits and proves the emitted bytes reduce.
func TestEmittedStreamReducesThroughTheSameReducer(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	stream := NewStream("strm-1", negotiated)
	submission := Submission{SubmissionID: "sub-1", ClientNonce: "non-1", RunID: "run-1"}
	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment}),
		AcceptedEvent(submission, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	)

	require.Equal(t, uint64(4), stream.Sequence())

	state := reduceEnvelopes(t, negotiated, envelopes)
	require.Equal(t, "strm-1", state.StreamID)
	require.Equal(t, uint64(4), state.ReducedThrough)
	require.Equal(t, []TurnRecord{{
		TurnID:       "turn-1",
		Origin:       CauseSubmission,
		Terminal:     true,
		Outcome:      OutcomeSuccess,
		SubmissionID: "sub-1",
		ClientNonce:  "non-1",
		RunID:        "run-1",
		CycleID:      "cyc-1",
		StopReason:   StopReasonEndTurn,
	}}, state.Turns)
	require.False(t, state.Quiescence.Certified, "an ordinary prompt proves no boundary")
}

// TestEmittedCloseBoundaryCertifiesTheProofItCompleted proves the close-fenced
// order's wire half: the terminal idle, then the quiescence fact naming the
// containment proof that produced it.
func TestEmittedCloseBoundaryCertifiesTheProofItCompleted(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	stream := NewStream("strm-1", negotiated)
	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment}),
		AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	)
	envelopes = append(envelopes, emitAll(t, stream, QuiescenceEvent(QuiescenceFact{
		Quiescent: true,
		Source:    ProofClassProcessContainment,
		Watermark: stream.State().ReducedThrough,
		Barrier:   "close-fence-1",
	}))...)

	state := reduceEnvelopes(t, negotiated, envelopes)
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, uint64(4), state.Quiescence.Watermark)
	require.Equal(t, "close-fence-1", state.Quiescence.Barrier)
}

// TestEmittedBlockingActionMovesTheForegroundThroughItsOwnTransition proves the
// announced action, the requires_action transition it accompanies, its
// resolution, and the release the last resolution permits all reduce in order.
func TestEmittedBlockingActionMovesTheForegroundThroughItsOwnTransition(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	stream := NewStream("strm-1", negotiated)
	owner := Owner{Type: OwnerTurn, ID: "turn-1"}
	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{}),
		AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		ActionEvent("act-1", ActionPermission, ActionPending, owner, true),
		RequiresActionEvent("cyc-1", "turn-1"),
		ActionResolvedEvent("act-1", ActionAccepted),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	)

	state := reduceEnvelopes(t, negotiated, envelopes)
	require.Equal(t, []ActionRecord{{
		ActionID:         "act-1",
		Kind:             ActionPermission,
		State:            ActionAccepted,
		Owner:            owner,
		BlocksForeground: true,
	}}, state.Actions)
	require.Equal(t, ForegroundIdle, state.Foreground.State)
}

// TestEmittedBackgroundActionLeavesTheForegroundAlone proves an action the turn
// does not block is announced without any foreground transition beside it.
func TestEmittedBackgroundActionLeavesTheForegroundAlone(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	stream := NewStream("strm-1", negotiated)
	envelopes := emitAll(t, stream,
		SnapshotEvent("cyc-0", QuiescenceFact{}),
		AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		ActionEvent("act-1", ActionElicitation, ActionPending, Owner{Type: OwnerTurn, ID: "turn-1"}, false),
		ActionResolvedEvent("act-1", ActionCancelled),
		IdleEvent("cyc-1", "turn-1", StopReasonCancelled, OutcomeCancelled),
	)

	state := reduceEnvelopes(t, negotiated, envelopes)
	require.Equal(t, ForegroundIdle, state.Foreground.State)
	require.Equal(t, ActionCancelled, state.Actions[0].State)
}

// TestEmitClaimsTheSequenceBeforeDelivery proves a refused event consumes its
// sequence: a counter that advanced only on success would make loss invisible,
// which is the exact failure contiguity exists to expose.
func TestEmitClaimsTheSequenceBeforeDelivery(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(RunningEvent("cyc-1", "turn-1"))
	require.ErrorAs(t, err, new(*ViolationError))
	require.Equal(t, uint64(1), stream.Sequence())
}

// TestSnapshotStatesAnUnprovenBoundaryAsNotQuiescent proves a configuration with
// no proof class emits a negative fact rather than a `none` sentinel or a
// present-and-empty source.
func TestSnapshotStatesAnUnprovenBoundaryAsNotQuiescent(t *testing.T) {
	t.Parallel()

	degenerate := Negotiated{Versions: []int{Version}, ActivityKinds: []ActivityKind{}}

	envelope, err := NewStream("strm-1", degenerate).Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{fieldQuiescent: false}, event[fieldQuiescence])
}

// TestAcceptanceOmitsAnAbsentRunID proves an optional handle is omitted rather
// than emitted empty: an empty opaque identifier fails closed on the reader.
func TestAcceptanceOmitsAnAbsentRunID(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	envelope, err := stream.Emit(AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"))
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, event, fieldRunID)
}

// TestResolvedActionRestatesNoImmutableMember proves a later patch carries the
// identity and the new state only: restating an immutable member it does not own
// is how two emitters disagree about what an action is.
func TestResolvedActionRestatesNoImmutableMember(t *testing.T) {
	t.Parallel()

	encoded := encodeAction(*ActionResolvedEvent("act-1", ActionDeclined).Action)
	require.Equal(t, map[string]any{fieldActionID: "act-1", fieldState: string(ActionDeclined)}, encoded)
}
