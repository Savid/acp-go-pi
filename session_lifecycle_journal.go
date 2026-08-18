package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// lifecycleBoundaryVersion is the version of the adapter-owned boundary record
// written under SessionStoreLifecycleSubpath.
const lifecycleBoundaryVersion = 1

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

// commitLifecycleBoundary durably records one boundary. It is awaited before
// the event that boundary precedes: a terminal idle a store does not stand
// behind, or a quiescence fact with no resumable snapshot behind it, would be a
// claim the next incarnation cannot honour.
func (s *agentSession) commitLifecycleBoundary(ctx context.Context, record lifecycleBoundaryRecord) error {
	if s.id == "" {
		// The store addresses a session by its native identity, so a session
		// that has none yet has no key a boundary could be recorded under.
		return nil
	}

	record.Version = lifecycleBoundaryVersion
	record.RecordedAtUnixMilli = lifecycleBoundaryNow().UnixMilli()

	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode lifecycle boundary: %w", err)
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	if s.persistFenced {
		return nil
	}

	key := SessionKey{SessionID: string(s.id), Subpath: SessionStoreLifecycleSubpath}

	appendCtx, finishAppend := s.agent.observe.StartSessionStore(ctx, "append")
	err = appendMirrorEntries(appendCtx, s.agent.sessionStore(), key, []SessionStoreEntry{encoded})

	finishAppend(err)

	if err != nil {
		return fmt.Errorf("%w: %w", errLifecycleBoundaryCommit, err)
	}

	return nil
}

// lastLifecycleBoundary reads the boundary a restored session resumes from. A
// session with no record resumes from no proven boundary, which is the truthful
// answer for one that never reached one.
func (a *Agent) lastLifecycleBoundary(ctx context.Context, sessionID string) lifecycleBoundaryRecord {
	entries, err := a.sessionStore().Load(ctx, SessionKey{SessionID: sessionID, Subpath: SessionStoreLifecycleSubpath})
	if err != nil || len(entries) == 0 {
		return lifecycleBoundaryRecord{}
	}

	var record lifecycleBoundaryRecord
	if err := json.Unmarshal(entries[len(entries)-1], &record); err != nil || record.Version != lifecycleBoundaryVersion {
		return lifecycleBoundaryRecord{}
	}

	return record
}

// fencePersistence stops every later durable write for this session. It takes
// the same lock a commit holds, so a commit already in flight either completed
// before the fence or writes nothing after it — which is what makes a delete
// unrecoverable by a settlement that was still running when it landed.
func (s *agentSession) fencePersistence() {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.persistFenced = true
}

func (s *agentSession) persistenceFenced() bool {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	return s.persistFenced
}
