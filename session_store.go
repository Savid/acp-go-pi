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
	// package: raw pi session JSONL rows appended after settled turns.
	SessionStoreFormat = "pi-session-jsonl-v1"
	// SessionStoreMainSubpath addresses a session's main entry log.
	SessionStoreMainSubpath = ""
)

// SessionStoreEntry is one raw native JSON row.
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

	if main.SessionID == "" || main.Subpath != SessionStoreMainSubpath {
		return fmt.Errorf("main key must use a session id and the main subpath")
	}

	now := time.Now().UnixMilli()
	next := make(map[SessionKey][]SessionStoreEntry, len(replacements))
	mainCount := 0

	for _, replacement := range replacements {
		if replacement.Key.SessionID != main.SessionID {
			return fmt.Errorf("replacement session id %q does not match main session id %q", replacement.Key.SessionID, main.SessionID)
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
