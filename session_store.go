package piacp

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	// SessionStoreFormat identifies the durable store format written by this
	// package: raw pi session JSONL plus adapter-owned lifecycle records.
	SessionStoreFormat = "pi-session-jsonl-v1"
	// SessionStoreMainSubpath addresses a session's main entry log.
	SessionStoreMainSubpath = ""
	// SessionStoreLifecycleSubpath addresses a session's adapter-owned
	// lifecycle boundary log. Its rows are wrapper records rather than native
	// transcript, so the main log stays exactly the bytes pi wrote and no
	// boundary or session-configuration fact is replayed as conversation.
	SessionStoreLifecycleSubpath = "lifecycle"
)

// SessionStoreEntry is one JSON row in a main or adapter-owned store log.
type SessionStoreEntry = json.RawMessage

// SessionKey addresses one entry log inside the session store.
type SessionKey struct {
	SessionID string
	Subpath   string
}

// SessionSummary describes one stored session for session/list.
type SessionSummary struct {
	SessionID          string
	UpdatedAtUnixMilli int64
	Cwd                string
	Title              string
	Meta               map[string]any
}

// SessionStoreReplacement is one key's full replacement contents for Replace.
type SessionStoreReplacement struct {
	Key     SessionKey
	Entries []SessionStoreEntry
}

// SessionStore is the host-provided durability boundary for pi sessions.
//
// A tombstone is final. Once Delete returns for a session's main key, no later
// Append or Replace may make that session's rows readable again: an
// implementation writes nothing and returns success. Delete may race an
// in-flight settlement, so the store itself must enforce the tombstone.
type SessionStore interface {
	Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error
	Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error)
	Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error
	Delete(ctx context.Context, key SessionKey) error
	ListSessions(ctx context.Context) ([]SessionSummary, error)
	ListSubkeys(ctx context.Context, key SessionKey) ([]string, error)
}

// InMemorySessionStore is a process-local SessionStore for tests and embedding.
type InMemorySessionStore struct {
	mu        sync.Mutex
	entries   map[SessionKey][]SessionStoreEntry
	updatedAt map[SessionKey]int64
	tombstone map[SessionKey]struct{}
}

var _ SessionStore = (*InMemorySessionStore)(nil)

// NewInMemorySessionStore constructs an empty in-memory session store.
func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{
		entries:   make(map[SessionKey][]SessionStoreEntry),
		updatedAt: make(map[SessionKey]int64),
		tombstone: make(map[SessionKey]struct{}),
	}
}

// Append durably appends entries to the addressed key in input order.
func (s *InMemorySessionStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return fmt.Errorf("nil InMemorySessionStore")
	}

	if len(entries) == 0 {
		return nil
	}

	if key.SessionID == "" {
		return fmt.Errorf("session id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensure()

	if s.isTombstonedLocked(key) {
		return nil
	}

	for _, entry := range entries {
		s.entries[key] = append(s.entries[key], cloneStoreEntry(entry))
	}

	s.updatedAt[key] = time.Now().UnixMilli()

	return nil
}

// storeKeyLabel renders one store key for a refusal message, so a caller is
// told exactly which entry log it addressed wrongly rather than only which
// session id it named.
func storeKeyLabel(key SessionKey) string {
	return fmt.Sprintf("%q subpath %q", key.SessionID, key.Subpath)
}

// Load returns the latest committed entries for the key in append order.
func (s *InMemorySessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isTombstonedLocked(key) {
		return nil, nil
	}

	return cloneStoreEntries(s.entries[key]), nil
}

// Replace atomically installs a full committed generation for a session.
func (s *InMemorySessionStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return fmt.Errorf("nil InMemorySessionStore")
	}

	if main.SessionID == "" {
		return fmt.Errorf("session id is required")
	}

	if main.Subpath != SessionStoreMainSubpath {
		return fmt.Errorf("main key must use the main subpath")
	}

	now := time.Now().UnixMilli()
	next := make(map[SessionKey][]SessionStoreEntry, len(replacements))
	mainCount := 0

	// Every replacement in a call belongs to the one session the main key
	// names, and each key appears at most once. Both refusals run over the
	// whole call before the store lock is taken, so a rejected Replace writes
	// nothing at all rather than committing the prefix it had already accepted.
	for _, replacement := range replacements {
		// A key from another session would let one session's commit rewrite a
		// second session's rows under a single atomic generation, which no
		// caller can undo and no reader can attribute.
		if replacement.Key.SessionID != main.SessionID {
			return fmt.Errorf(
				"replacement key %s does not belong to session %q",
				storeKeyLabel(replacement.Key), main.SessionID,
			)
		}

		// Two replacements naming one key describe two different generations of
		// it, and nothing in the call says which one the caller meant. Resolving
		// that by last-write-wins would silently commit one of them, so the whole
		// call is refused before any key is written.
		if _, duplicate := next[replacement.Key]; duplicate {
			return fmt.Errorf("duplicate replacement key %s", storeKeyLabel(replacement.Key))
		}

		if replacement.Key == main {
			mainCount++
		}

		next[replacement.Key] = cloneStoreEntries(replacement.Entries)
	}

	if mainCount != 1 {
		return fmt.Errorf("replacements must include the main key exactly once")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensure()

	// A tombstone is final: a replacement that landed after a delete would
	// answer for a session the host was told is gone.
	if s.isTombstonedLocked(main) {
		return nil
	}

	for key := range s.entries {
		if key.SessionID == main.SessionID {
			delete(s.entries, key)
			delete(s.updatedAt, key)
			s.tombstone[key] = struct{}{}
		}
	}

	for key, entries := range next {
		s.entries[key] = entries
		s.updatedAt[key] = now
		delete(s.tombstone, key)
	}

	return nil
}

// Delete durably tombstones the key; deleting main cascades to all subpaths.
func (s *InMemorySessionStore) Delete(ctx context.Context, key SessionKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return fmt.Errorf("nil InMemorySessionStore")
	}

	if key.SessionID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensure()

	for candidate := range s.entries {
		if candidate.SessionID != key.SessionID {
			continue
		}

		if key.Subpath != SessionStoreMainSubpath && candidate.Subpath != key.Subpath {
			continue
		}

		delete(s.entries, candidate)
		delete(s.updatedAt, candidate)
		s.tombstone[candidate] = struct{}{}
	}

	s.tombstone[key] = struct{}{}

	return nil
}

// ListSessions lists committed, non-tombstoned main keys, newest first.
func (s *InMemorySessionStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	summaries := make([]SessionSummary, 0)

	for key := range s.entries {
		if key.Subpath != SessionStoreMainSubpath || s.isTombstonedLocked(key) {
			continue
		}

		summaries = append(summaries, SessionSummary{
			SessionID:          key.SessionID,
			UpdatedAtUnixMilli: s.updatedAt[key],
		})
	}

	slices.SortFunc(summaries, func(left, right SessionSummary) int {
		if byTime := cmp.Compare(right.UpdatedAtUnixMilli, left.UpdatedAtUnixMilli); byTime != 0 {
			return byTime
		}

		return strings.Compare(left.SessionID, right.SessionID)
	})

	return summaries, nil
}

// ListSubkeys lists committed, non-tombstoned subpaths sorted bytewise ascending.
func (s *InMemorySessionStore) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, fmt.Errorf("nil InMemorySessionStore")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	subkeys := make([]string, 0)

	for candidate := range s.entries {
		if candidate.SessionID != key.SessionID ||
			candidate.Subpath == SessionStoreMainSubpath ||
			s.isTombstonedLocked(candidate) {
			continue
		}

		subkeys = append(subkeys, candidate.Subpath)
	}

	slices.Sort(subkeys)

	return subkeys, nil
}

func (s *InMemorySessionStore) ensure() {
	if s.entries == nil {
		s.entries = make(map[SessionKey][]SessionStoreEntry)
	}

	if s.updatedAt == nil {
		s.updatedAt = make(map[SessionKey]int64)
	}

	if s.tombstone == nil {
		s.tombstone = make(map[SessionKey]struct{})
	}
}

func (s *InMemorySessionStore) isTombstonedLocked(key SessionKey) bool {
	if _, ok := s.tombstone[key]; ok {
		return true
	}

	_, mainDeleted := s.tombstone[SessionKey{SessionID: key.SessionID, Subpath: SessionStoreMainSubpath}]

	return mainDeleted && key.Subpath != SessionStoreMainSubpath
}

func cloneStoreEntries(entries []SessionStoreEntry) []SessionStoreEntry {
	if entries == nil {
		return nil
	}

	cloned := make([]SessionStoreEntry, len(entries))
	for index, entry := range entries {
		cloned[index] = cloneStoreEntry(entry)
	}

	return cloned
}

func cloneStoreEntry(entry SessionStoreEntry) SessionStoreEntry {
	if entry == nil {
		return nil
	}

	return append(SessionStoreEntry(nil), entry...)
}
