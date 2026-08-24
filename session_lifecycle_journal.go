package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

// lifecycleBoundaryVersion is the version of the adapter-owned boundary record
// written under SessionStoreLifecycleSubpath.
const lifecycleBoundaryVersion = 1

const lifecycleBoundaryFieldNativeRows = "nativeRows"

const lifecycleBoundaryFieldNativeState = "nativeState"

const lifecycleBoundaryFieldRecordedAt = "recordedAt"

// Native-state dispositions a boundary record states. The durable format is a
// raw mirror of pi's own session file, so a cycle whose frames pi never wrote
// cannot be represented there at all: the boundary record states the fact
// instead of the wrapper inventing a transcript row for it.
const (
	// nativeStateCommitted means the store holds every native row this cycle
	// produced.
	nativeStateCommitted = "committed"
	// nativeStateRetained means the store holds the last safe generation and
	// this cycle's native rows are not in it, either because pi never wrote
	// them or because teardown ended the cycle before they could be adopted.
	nativeStateRetained = "retained"
)

// lifecycleBoundaryRecord is one durable foreground or settlement boundary. It
// is adapter-owned state under its own subpath: pi's transcript rows stay
// exactly the bytes pi wrote, and nothing here is ever replayed as
// conversation.
type lifecycleBoundaryRecord struct {
	Version int `json:"version"`
	// StreamID names the incarnation whose ordered stream reached this
	// boundary.
	StreamID string `json:"streamId"`
	TurnID   string `json:"turnId,omitempty"`
	CycleID  string `json:"cycleId,omitempty"`
	// Outcome and StopReason are the ones the terminal transition carries. A
	// failed outcome states no stop reason.
	Outcome    string `json:"outcome,omitempty"`
	StopReason string `json:"stopReason,omitempty"`
	// NativeRows is how many native transcript rows the store holds for this
	// session at this boundary.
	NativeRows int `json:"nativeRows"`
	// NativeState says whether this cycle's native transcript is in the store
	// or was never written; Detail names the loss or fence in words a later
	// reader can act on.
	NativeState string `json:"nativeState"`
	Detail      string `json:"detail,omitempty"`
	// VacancyProven records that the boundary that produced this record proved
	// whole-tree vacancy, which is what lets the next incarnation open on a
	// positive quiescence fact instead of an advertised one.
	VacancyProven       bool  `json:"vacancyProven"`
	RecordedAtUnixMilli int64 `json:"recordedAt"`
}

var errLifecycleBoundaryCommit = errors.New("append lifecycle boundary record")

var lifecycleBoundaryNow = time.Now

// commitLifecycleBoundary durably records one boundary. Prompt settlement
// writes it before terminal idle. Close writes its foreground mirror first,
// publishes the terminal transition, then writes this resumable boundary before
// quiescence; a quiescence fact without that snapshot would be a claim the next
// incarnation cannot honour.
func (s *agentSession) commitLifecycleBoundary(ctx context.Context, record lifecycleBoundaryRecord) error {
	if s.id == "" {
		// The store addresses a session by its native identity, so a session
		// that has none yet has no key a boundary could be recorded under.
		return nil
	}

	record.Version = lifecycleBoundaryVersion
	record.RecordedAtUnixMilli = lifecycleBoundaryNow().UnixMilli()

	// Every field is a scalar, so the encoding cannot fail.
	encoded, _ := json.Marshal(record)

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	if s.persistFenced {
		return nil
	}

	key := SessionKey{SessionID: string(s.id), Subpath: SessionStoreLifecycleSubpath}

	appendCtx, finishAppend := s.agent.observe.StartSessionStore(ctx, "append")
	err := appendMirrorEntries(appendCtx, s.agent.sessionStore(), key, []SessionStoreEntry{encoded})

	finishAppend(err)

	if err != nil {
		return fmt.Errorf("%w: %w", errLifecycleBoundaryCommit, err)
	}

	return nil
}

// lastLifecycleBoundary validates the complete journal and returns the last
// boundary a restored session may resume from. Corrupt adapter-owned state is
// not equivalent to an absent proof: callers fail the restore rather than
// opening from invented state.
func (a *Agent) lastLifecycleBoundary(
	ctx context.Context,
	sessionID string,
) (lifecycleBoundaryRecord, bool, error) {
	entries, err := a.loadStoreEntries(ctx, a.sessionStore(), SessionKey{
		SessionID: sessionID,
		Subpath:   SessionStoreLifecycleSubpath,
	})
	if err != nil {
		return lifecycleBoundaryRecord{}, false, fmt.Errorf("load lifecycle journal: %w", err)
	}

	var last lifecycleBoundaryRecord

	for index, entry := range entries {
		record, decodeErr := decodeLifecycleBoundaryRecord(entry)
		if decodeErr != nil {
			return lifecycleBoundaryRecord{}, false, fmt.Errorf("decode lifecycle journal row %d: %w", index, decodeErr)
		}

		if index > 0 && record.NativeRows < last.NativeRows {
			return lifecycleBoundaryRecord{}, false, fmt.Errorf(
				"decode lifecycle journal row %d: native row count regressed from %d to %d",
				index, last.NativeRows, record.NativeRows,
			)
		}

		last = record
	}

	return last, len(entries) > 0, nil
}

func decodeLifecycleBoundaryRecord(entry SessionStoreEntry) (lifecycleBoundaryRecord, error) {
	decoder := json.NewDecoder(bytes.NewReader(entry))
	decoder.DisallowUnknownFields()

	var record lifecycleBoundaryRecord
	if err := decoder.Decode(&record); err != nil {
		return lifecycleBoundaryRecord{}, err
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return lifecycleBoundaryRecord{}, errors.New("trailing JSON value")
		}

		return lifecycleBoundaryRecord{}, fmt.Errorf("decode trailing data: %w", err)
	}

	var fields map[string]json.RawMessage

	_ = json.Unmarshal(entry, &fields) // the strict full decode above already proved valid JSON

	for _, field := range []string{
		lifecycleFieldVersion,
		lifecycleFieldStreamID,
		lifecycleBoundaryFieldNativeRows,
		lifecycleBoundaryFieldNativeState,
		lifecycleBoundaryFieldRecordedAt,
	} {
		raw, present := fields[field]
		if !present || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return lifecycleBoundaryRecord{}, fmt.Errorf("%s is required", field)
		}
	}

	for field, raw := range fields {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return lifecycleBoundaryRecord{}, fmt.Errorf("%s cannot be null", field)
		}
	}

	if err := validateLifecycleBoundaryRecord(record); err != nil {
		return lifecycleBoundaryRecord{}, err
	}

	return record, nil
}

func validateLifecycleBoundaryRecord(record lifecycleBoundaryRecord) error {
	switch {
	case record.Version != lifecycleBoundaryVersion:
		return fmt.Errorf("unsupported version %d", record.Version)
	case record.NativeRows < 0:
		return fmt.Errorf("nativeRows cannot be negative")
	case record.RecordedAtUnixMilli <= 0:
		return fmt.Errorf("recordedAt must be positive")
	case record.NativeState != nativeStateCommitted && record.NativeState != nativeStateRetained:
		return fmt.Errorf("unsupported nativeState %q", record.NativeState)
	case record.VacancyProven && record.NativeState != nativeStateCommitted:
		return errors.New("vacancyProven requires committed native state")
	// A cycle outlives the turn it carried: the terminal transition clears the
	// turn and leaves the foreground idle, so a boundary recorded there names a
	// cycle and no turn. Turn identity is present only while a turn is open —
	// an idle foreground carrying one is malformed — which makes the turnless
	// cycle the correct shape here rather than a defect to refuse.
	case record.StreamID == "" && (record.TurnID != "" || record.CycleID != ""):
		return errors.New("turnId and cycleId require streamId")
	}

	outcome := lifecycle.Outcome(record.Outcome)
	switch {
	case outcome == "" && record.StopReason != "":
		return errors.New("stopReason requires outcome")
	case outcome != "" && !outcome.Valid():
		return fmt.Errorf("unsupported outcome %q", record.Outcome)
	case outcome == lifecycle.OutcomeFailed && record.StopReason != "":
		return errors.New("failed outcome forbids stopReason")
	case outcome != "" && outcome != lifecycle.OutcomeFailed && !lifecycle.ValidStopReason(record.StopReason):
		return fmt.Errorf("outcome %q requires a valid stopReason", record.Outcome)
	}

	return nil
}

// fencePersistence stops every later durable write for this session and ends
// its lifecycle incarnation with them. It takes the same lock a commit holds, so
// a commit already in flight either completed before the fence or writes nothing
// after it — which is what makes a delete unrecoverable by a settlement that was
// still running when it landed.
//
// The stream is fenced first, and never after: every fact this extension states
// is a claim about durable state a later incarnation can act on, so once no
// write can land, the close boundary that follows must terminalize nothing and
// certify nothing. Its commits silently no-op, and a quiescence fact behind zero
// durable rows would promise a resumable snapshot the delete has already taken
// away.
func (s *agentSession) fencePersistence() {
	s.fenceLifecycleStream()

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.persistFenced = true
}
