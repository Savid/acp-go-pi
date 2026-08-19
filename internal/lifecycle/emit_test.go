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

// observingConfiguration is the answer for a source that does prove an activity
// kind. The reducer is also the validator for streams this adapter reads, so the
// encoder is held to the whole shape rather than to the subset this adapter's own
// configuration happens to produce.
func observingConfiguration() Negotiated {
	negotiated := containedConfiguration()
	negotiated.ActivityKinds = []ActivityKind{ActivityTask}

	return negotiated
}

// delivered wraps one envelope in the single carrier the extension permits.
func delivered(t *testing.T, envelope map[string]any) json.RawMessage {
	t.Helper()

	params, err := json.Marshal(map[string]any{
		"sessionId": "sess-1",
		"update":    map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
		metaField:   map[string]any{MetaKey: envelope},
	})
	require.NoError(t, err)

	return params
}

// reduceEnvelopes decodes each emitted envelope from a session/update
// notification and reduces it, which is the only measure of wire legality that
// counts.
func reduceEnvelopes(t *testing.T, negotiated Negotiated, envelopes []map[string]any) State {
	t.Helper()

	reducer := NewReducer(Options{Negotiated: negotiated})

	for index, envelope := range envelopes {
		require.NoError(t, reducer.ReduceSessionUpdate(delivered(t, envelope)), "envelope %d", index)
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

// TestMisshapenSnapshotForegroundIsRefusedAtEmitAndInItsOwnBytes drives every
// shape the foreground rule forbids through both halves of this package. Emit
// refuses each one, because the reducer that gates emission is the same rule the
// decoder applies — and the bytes the encoder renders for the same assertion
// carry the identical defect, so the refusal an emitter states is the refusal its
// consumer would state. An encoder that dropped the turn or its origin would
// render a legal-looking envelope for an assertion this adapter just refused.
func TestMisshapenSnapshotForegroundIsRefusedAtEmitAndInItsOwnBytes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		foreground Foreground
		refusal    string
	}{
		{
			name:       "an idle foreground naming a turn",
			foreground: Foreground{State: ForegroundIdle, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
			refusal:    "an idle foreground reports no turn",
		},
		{
			name:       "a turn without its origin",
			foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1"},
			refusal:    "foreground origin is present exactly while a turn is",
		},
		{
			name:       "an origin without its turn",
			foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", Origin: CauseSubmission},
			refusal:    "foreground origin is present exactly while a turn is",
		},
		{
			name:       "an origin outside the cause vocabulary",
			foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: "bogus"},
			refusal:    "foreground origin bogus",
		},
		{
			name:       "a cause no turn is ever opened by",
			foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSession},
			refusal:    "foreground origin session",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			negotiated := containedConfiguration()
			event := Event{Type: EventSnapshot, Snapshot: &Snapshot{Foreground: test.foreground}}
			refusal := violation(ViolationMalformedEnvelope, "strm-1", 1, test.refusal)

			_, err := NewStream("strm-1", negotiated).Emit(event)
			require.Equal(t, refusal, err)

			_, err = DecodeSessionUpdate(delivered(t, map[string]any{
				fieldVersion:  Version,
				fieldStreamID: "strm-1",
				fieldSequence: 1,
				fieldEvent:    encodeEvent(event),
			}), negotiated)
			require.Equal(t, refusal, err)
		})
	}
}

// TestEmittedMidTurnSnapshotReducesToTheEmitterOwnProjection proves the emitter
// and a consumer agree about a resumed stream. The emitter reduces the assertion
// in memory and the consumer reduces the bytes rendered for it, so any member the
// encoder dropped would leave the two holding different states for one stream:
// the turn the foreground names, its origin, the live activity with its opaque
// progress, and the pending action with its stated blocking claim all have to
// survive the round trip.
func TestEmittedMidTurnSnapshotReducesToTheEmitterOwnProjection(t *testing.T) {
	t.Parallel()

	negotiated := observingConfiguration()
	blocks := true
	stream := NewStream("strm-1", negotiated)

	envelope, err := stream.Emit(Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundRunning, CycleID: "cyc-1", TurnID: "turn-1", Origin: CauseSubmission},
		Activities: []ActivityUpdate{{
			ActivityID:   "acty-1",
			Kind:         ActivityTask,
			State:        ActivityRunning,
			ToolCallID:   "tool-1",
			Cause:        CauseSubmission,
			OriginTurnID: "turn-1",
			RunID:        "run-1",
			Progress:     json.RawMessage(`{"phase":"scanning"}`),
		}},
		Actions: []ActionUpdate{{
			ActionID:         "act-1",
			Kind:             ActionPermission,
			State:            ActionPending,
			Owner:            Owner{Type: OwnerTurn, ID: "turn-1"},
			RunID:            "run-1",
			BlocksForeground: &blocks,
		}},
		Quiescence: QuiescenceFact{},
	}})
	require.NoError(t, err)

	delivery, err := DecodeSessionUpdate(delivered(t, envelope), negotiated)
	require.NoError(t, err)

	consumer := NewReducer(Options{Negotiated: negotiated})
	require.NoError(t, consumer.Reduce(delivery))

	emitted, reduced := stream.State(), consumer.State()
	require.Equal(t, emitted.StreamID, reduced.StreamID)
	require.Equal(t, emitted.ReducedThrough, reduced.ReducedThrough)
	require.Equal(t, emitted.Foreground, reduced.Foreground)
	require.Equal(t, emitted.Turns, reduced.Turns)
	require.Equal(t, emitted.Activities, reduced.Activities)
	require.Equal(t, emitted.Actions, reduced.Actions)
	require.Equal(t, emitted.Quiescence, reduced.Quiescence)
	require.Equal(t, emitted, reduced)
}
