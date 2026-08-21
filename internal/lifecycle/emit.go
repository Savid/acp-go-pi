package lifecycle

import "encoding/json"

// Stream is one incarnation's ordered emitter. It claims a sequence before
// delivery is attempted, so a lost or refused event leaves a detectable gap
// rather than a silently contiguous stream, and it renders every event as the
// notification it will ride and reads it back through DecodeSessionUpdate before
// reducing it, so a stream this adapter could not support fails at the point of
// emission instead of at its consumers.
//
// A Stream is not safe for concurrent use; the session that owns the incarnation
// serializes emission.
type Stream struct {
	id       string
	reducer  *Reducer
	sequence uint64
}

// NewStream opens an incarnation identified by id. The identity names one native
// lifecycle source lifetime: it never rotates while that native process
// survives, and it never outlives it.
func NewStream(id string, negotiated Negotiated) *Stream {
	return &Stream{id: id, reducer: NewReducer(Options{Negotiated: negotiated})}
}

// ID reports the incarnation this stream speaks for.
func (s *Stream) ID() string { return s.id }

// State returns the projection the emitted stream proves.
func (s *Stream) State() State { return s.reducer.State() }

// Close records that the addressed session's close containment completed. It is
// the stronger of the two ends a stream can reach: fencing an incarnation stops
// this stream, while closing the session stops the session, so a later event on
// it — an opening snapshot for a would-be new incarnation included — fails
// closed as stale at the emitter rather than reaching a consumer.
func (s *Stream) Close() { s.reducer.Close() }

// Sequence reports the highest sequence claimed so far.
func (s *Stream) Sequence() uint64 { return s.sequence }

// Emit claims the next sequence, renders the envelope for the notification's
// `_meta`, and validates the rendered bytes before returning them. A refused
// event is never returned and its sequence stays consumed, which is exactly the
// detectable gap the ordering rule wants.
//
// "Emitted envelopes are well formed" is a claim about bytes, so the claim is
// tested on bytes: the notification is rendered, marshalled, decoded, and
// reduced through the exact path a consumer takes. Reducing the in-process
// struct instead would leave every encoder defect — a dropped member, a member
// rendered under the wrong name — invisible to the emitter that produced it,
// because the struct the reducer judged was never the thing that went out.
func (s *Stream) Emit(event Event) (map[string]any, error) {
	// The payload is judged before the sequence claim, so a caller defect
	// neither burns a sequence nor dereferences a payload that is not there.
	// The verdicts mirror the decoder's: an unknown discriminant is the
	// discriminant's violation, a known one without its payload is shape.
	if !event.payloadMatchesType() {
		if !knownEventType(event.Type) {
			return nil, violation(ViolationUnknownEventType, s.id, s.sequence+1,
				"event type "+string(event.Type))
		}

		return nil, violation(ViolationMalformedEnvelope, s.id, s.sequence+1,
			"event payload does not match type "+string(event.Type))
	}

	s.sequence++

	envelope := map[string]any{
		fieldVersion:  Version,
		fieldStreamID: s.id,
		fieldSequence: s.sequence,
		fieldEvent:    encodeEvent(event),
	}

	// A rendered envelope holds only JSON-safe values, and a payload this step
	// could not produce fails the decode below as a malformed envelope rather
	// than escaping as an untyped error.
	params, _ := json.Marshal(map[string]any{
		metaField:   map[string]any{MetaKey: envelope},
		updateField: map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
	})

	delivery, err := DecodeSessionUpdate(params, s.reducer.Negotiated())
	if err != nil {
		return nil, err
	}

	if err := s.reducer.Reduce(delivery); err != nil {
		return nil, err
	}

	return envelope, nil
}

// SnapshotEvent opens a stream from the whole state this adapter can state
// truthfully. A fresh native process holds no turn, no activity, and no pending
// action, so the nonterminal sets are empty and the quiescence fact is whatever
// the configuration's proof class actually established at the previous boundary.
func SnapshotEvent(cycleID string, quiescence QuiescenceFact) Event {
	return Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: cycleID},
		Quiescence: quiescence,
	}}
}

// AcceptedEvent records that the native dispatcher took durable ownership of a
// submitted frame. The submission identity is echoed verbatim from the prompt's
// correlation value.
func AcceptedEvent(submission Submission, turnID string) Event {
	return Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: submission.SubmissionID,
		ClientNonce:  submission.ClientNonce,
		TurnID:       turnID,
		RunID:        submission.RunID,
	}}
}

// RunningEvent opens or resumes the foreground cycle a submission caused.
func RunningEvent(cycleID, turnID string) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:   ForegroundRunning,
		CycleID: cycleID,
		TurnID:  turnID,
		Cause:   CauseSubmission,
	}}
}

// AgentRunningEvent opens the foreground cycle an agent-origin activity caused.
// It is the second of the two events that can open a turn: the native harness
// began work nobody submitted, so there is no acceptance to precede it and no
// submission identity to borrow.
func AgentRunningEvent(cycleID, turnID string) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:   ForegroundRunning,
		CycleID: cycleID,
		TurnID:  turnID,
		Cause:   CauseActivity,
	}}
}

// RequiresActionEvent reports the cycle a blocking action stopped. A blocking
// action never moves the foreground by itself, so this transition always
// accompanies the action that caused it.
func RequiresActionEvent(cycleID, turnID string) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:   ForegroundRequiresAction,
		CycleID: cycleID,
		TurnID:  turnID,
		Cause:   CauseSubmission,
	}}
}

// IdleEvent ends the cycle a submission caused, carrying the turn's truthful stop
// reason and recorded outcome. A failed outcome carries no stop reason: no ACP v1
// stop reason names a failure and the v1 error carries it instead.
func IdleEvent(cycleID, turnID, stopReason string, outcome Outcome) Event {
	return IdleEventFor(CauseSubmission, cycleID, turnID, stopReason, outcome)
}

// IdleEventFor ends a cycle whose cause is stated by the caller. A cycle ends
// for the same reason it opened, so an agent-origin cycle reports the activity
// cause rather than borrowing the submission one from a prompt that never
// existed.
func IdleEventFor(cause Cause, cycleID, turnID, stopReason string, outcome Outcome) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:      ForegroundIdle,
		CycleID:    cycleID,
		TurnID:     turnID,
		Cause:      cause,
		StopReason: stopReason,
		Outcome:    outcome,
	}}
}

// ActionEvent announces one permission or elicitation awaiting an answer. Every
// member that fixes what the action is rides its first sight.
func ActionEvent(actionID string, kind ActionKind, state ActionState, owner Owner, blocks bool) Event {
	return Event{Type: EventActionUpdate, Action: &ActionUpdate{
		ActionID:         actionID,
		Kind:             kind,
		State:            state,
		Owner:            owner,
		BlocksForeground: &blocks,
	}}
}

// ActionResolvedEvent resolves an announced action exactly once.
func ActionResolvedEvent(actionID string, state ActionState) Event {
	return Event{Type: EventActionUpdate, Action: &ActionUpdate{ActionID: actionID, State: state}}
}

// QuiescenceEvent states the authoritative quiescence fact a completed proof
// produced. It carries the proof class and the watermark that proof covers, never
// a guess, a heuristic, or a confidence.
func QuiescenceEvent(fact QuiescenceFact) Event {
	return Event{Type: EventQuiescenceUpdate, Quiescence: &fact}
}

// encodeEvent renders the events this adapter emits. This adapter's own
// configuration proves no activity kind, so it never emits an `activity_update`
// of its own; the encoder still renders all six, because Emit reads the rendered
// bytes back through the decoder and a discriminant the encoder could not render
// would be a hole in that self-check rather than an event nobody produces.
func encodeEvent(event Event) map[string]any {
	switch event.Type {
	case EventSnapshot:
		return encodeSnapshot(*event.Snapshot)
	case EventActivityUpdate:
		return map[string]any{fieldType: string(EventActivityUpdate), fieldActivity: encodeActivity(*event.Activity)}
	case EventPromptAccepted:
		return withOptional(map[string]any{
			fieldType:         string(EventPromptAccepted),
			fieldSubmissionID: event.PromptAccepted.SubmissionID,
			fieldClientNonce:  event.PromptAccepted.ClientNonce,
			fieldTurnID:       event.PromptAccepted.TurnID,
		}, fieldRunID, event.PromptAccepted.RunID)
	case EventActionUpdate:
		return map[string]any{fieldType: string(EventActionUpdate), fieldAction: encodeAction(*event.Action)}
	case EventQuiescenceUpdate:
		fact := encodeQuiescence(*event.Quiescence)
		fact[fieldType] = string(EventQuiescenceUpdate)

		return fact
	default:
		return encodeTransition(*event.State)
	}
}

// encodeSnapshot renders the whole-state assertion. The foreground names its turn
// and that turn's origin exactly while one is open, and the two sets are rendered
// member for member with the same encoding the delta events use: the reducer that
// gates emission reduces the assertion in memory, so an encoder that dropped a
// member would render a different assertion than the one this adapter proved, and
// emitter and consumer would disagree about the state of the same stream.
func encodeSnapshot(snapshot Snapshot) map[string]any {
	foreground := map[string]any{
		fieldState:   string(snapshot.Foreground.State),
		fieldCycleID: snapshot.Foreground.CycleID,
	}
	withOptional(foreground, fieldTurnID, snapshot.Foreground.TurnID)
	withOptional(foreground, fieldOrigin, string(snapshot.Foreground.Origin))

	activities := make([]any, 0, len(snapshot.Activities))
	for index := range snapshot.Activities {
		activities = append(activities, encodeActivity(snapshot.Activities[index]))
	}

	actions := make([]any, 0, len(snapshot.Actions))
	for index := range snapshot.Actions {
		actions = append(actions, encodeAction(snapshot.Actions[index]))
	}

	return map[string]any{
		fieldType:       string(EventSnapshot),
		fieldForeground: foreground,
		fieldActivities: activities,
		fieldActions:    actions,
		fieldQuiescence: encodeQuiescence(snapshot.Quiescence),
	}
}

// encodeActivity renders one activity's members. A snapshot's set is the complete
// nonterminal one, so every member that fixes what an activity is rides it,
// including the opaque progress object this contract renders and never reduces.
func encodeActivity(activity ActivityUpdate) map[string]any {
	encoded := map[string]any{
		fieldActivityID: activity.ActivityID,
		fieldState:      string(activity.State),
	}
	withOptional(encoded, fieldKind, string(activity.Kind))
	withOptional(encoded, fieldParentID, activity.ParentID)
	withOptional(encoded, fieldToolCallID, activity.ToolCallID)
	withOptional(encoded, fieldCause, string(activity.Cause))
	withOptional(encoded, fieldOriginTurnID, activity.OriginTurnID)
	withOptional(encoded, fieldRunID, activity.RunID)

	if activity.Progress != nil {
		encoded[fieldProgress] = activity.Progress
	}

	return encoded
}

func encodeTransition(transition StateTransition) map[string]any {
	encoded := map[string]any{
		fieldType:    string(EventStateUpdate),
		fieldState:   string(transition.State),
		fieldCycleID: transition.CycleID,
		fieldTurnID:  transition.TurnID,
		fieldCause:   string(transition.Cause),
	}
	withOptional(encoded, fieldStopReason, transition.StopReason)
	withOptional(encoded, fieldOutcome, string(transition.Outcome))

	return encoded
}

// encodeAction renders an action's first sight or one later patch. A patch
// restates nothing: an omitted immutable member states nothing about it.
func encodeAction(action ActionUpdate) map[string]any {
	encoded := map[string]any{
		fieldActionID: action.ActionID,
		fieldState:    string(action.State),
	}
	withOptional(encoded, fieldKind, string(action.Kind))
	withOptional(encoded, fieldRunID, action.RunID)

	if action.Owner.ID != "" {
		encoded[fieldOwner] = map[string]any{fieldType: string(action.Owner.Type), fieldID: action.Owner.ID}
	}

	if action.BlocksForeground != nil {
		encoded[fieldBlocksForeground] = *action.BlocksForeground
	}

	return encoded
}

// encodeQuiescence renders a fact's members. A negative fact carries no proof at
// all: `source` is present if and only if the fact is positive, and it is never a
// `none` sentinel.
func encodeQuiescence(fact QuiescenceFact) map[string]any {
	if !fact.Quiescent {
		return map[string]any{fieldQuiescent: false}
	}

	encoded := map[string]any{
		fieldQuiescent: true,
		fieldSource:    string(fact.Source),
		fieldWatermark: fact.Watermark,
	}

	return withOptional(encoded, fieldBarrier, fact.Barrier)
}

// withOptional adds a member only when it has a value. An optional member is
// omitted rather than emitted empty, because an empty opaque identifier fails
// closed on the reading side.
func withOptional(encoded map[string]any, key, value string) map[string]any {
	if value != "" {
		encoded[key] = value
	}

	return encoded
}
