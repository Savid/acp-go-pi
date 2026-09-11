package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

// configSubpath holds the adapter's session records. Each mirror commit
// appends one; the last row is current.
const configSubpath = "config"

// sessionRecord is the adapter-owned state a session needs to resume: where
// pi keeps the native file and the configuration the session was established
// with.
type sessionRecord struct {
	SessionID             string            `json:"sessionId"`
	Cwd                   string            `json:"cwd"`
	AdditionalDirectories []string          `json:"additionalDirectories,omitempty"`
	SessionFile           string            `json:"sessionFile"`
	Env                   map[string]string `json:"env,omitempty"`
	ExtraPathDirs         []string          `json:"extraPathDirs,omitempty"`
	Model                 string            `json:"model,omitempty"`
	ThinkingLevel         string            `json:"thinkingLevel,omitempty"`
	Permission            string            `json:"permission,omitempty"`
	AutoRetry             bool              `json:"autoRetry,omitempty"`
	UpdatedAtUnixMilli    int64             `json:"updatedAtUnixMilli"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{
		SessionID:             string(s.id),
		Cwd:                   s.cwd,
		AdditionalDirectories: slices.Clone(s.additionalDirectories),
		SessionFile:           s.sessionFile,
		Env:                   cloneStringMap(s.options.Env),
		ExtraPathDirs:         slices.Clone(s.options.ExtraPathDirs),
		Model:                 s.model,
		ThinkingLevel:         s.thinkingLevel,
		Permission:            s.options.Permission,
		AutoRetry:             s.options.AutoRetry,
		UpdatedAtUnixMilli:    time.Now().UnixMilli(),
	}
}

// commitMirror appends the native session file's new rows to the store, then
// the current session record. pi creates the file lazily, so a session that
// produced nothing yet leaves nothing to mirror.
func (s *session) commitMirror(ctx context.Context) error {
	s.mu.Lock()
	path := s.sessionFile
	mirrored := s.mirrored
	s.mu.Unlock()

	if path == "" {
		return nil
	}

	rows, err := pi.ReadRows(path)
	if err != nil {
		return fmt.Errorf("read native session file: %w", err)
	}

	if len(rows) <= mirrored {
		return nil
	}

	entries := make([]acpcore.SessionStoreEntry, 0, len(rows)-mirrored)
	for _, row := range rows[mirrored:] {
		entries = append(entries, acpcore.SessionStoreEntry(row))
	}

	store := s.agent.store
	key := acpcore.SessionKey{SessionID: string(s.id)}

	appendCtx, finish := s.agent.observe.StartSessionStore(ctx, "append")
	err = store.Append(appendCtx, key, entries)
	finish(err)

	if err != nil {
		return fmt.Errorf("append session rows: %w", err)
	}

	s.mu.Lock()
	if len(rows) > s.mirrored {
		s.mirrored = len(rows)
	}
	s.mu.Unlock()

	// The record cannot fail to marshal: it holds strings, bools, and an integer.
	encoded, _ := json.Marshal(s.record())

	if err := store.Append(ctx, acpcore.SessionKey{SessionID: string(s.id), Subpath: configSubpath}, []acpcore.SessionStoreEntry{encoded}); err != nil {
		return fmt.Errorf("append session record: %w", err)
	}

	return nil
}

// storedSession is what the store holds for one session id.
type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

// loadStored reads a session's rows and latest record. A session with no
// committed rows is unknown.
func (a *Agent) loadStored(ctx context.Context, sessionID acp.SessionId) (storedSession, error) {
	loadCtx, cancel := context.WithTimeout(ctx, a.options.SessionStoreLoadTimeout)
	defer cancel()

	loadCtx, finish := a.observe.StartSessionStore(loadCtx, "load")

	entries, err := a.store.Load(loadCtx, acpcore.SessionKey{SessionID: string(sessionID)})
	if err == nil {
		var records []acpcore.SessionStoreEntry

		records, err = a.store.Load(loadCtx, acpcore.SessionKey{SessionID: string(sessionID), Subpath: configSubpath})
		if err == nil && len(records) > 0 {
			var record sessionRecord
			if decodeErr := json.Unmarshal(records[len(records)-1], &record); decodeErr == nil {
				return storedSession{rows: storeRows(entries), record: record, found: len(entries) > 0}, finishLoad(finish, nil)
			}
		}
	}

	finish(err)

	if err != nil {
		return storedSession{}, fmt.Errorf("load session store: %w", err)
	}

	return storedSession{rows: storeRows(entries), found: len(entries) > 0}, nil
}

func finishLoad(finish func(error), err error) error {
	finish(err)

	return err
}

func storeRows(entries []acpcore.SessionStoreEntry) [][]byte {
	rows := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		if trimmed := bytes.TrimSpace(entry); len(trimmed) > 0 {
			rows = append(rows, trimmed)
		}
	}

	return rows
}

// hydrate reconciles the store with pi's own file before a load or resume. An
// existing native file at least as long as the store wins and its newer rows
// are adopted; a missing or shorter one is materialized from the store. A
// disagreement at a shared position fails the restore. It returns the native
// path and the rows the session now holds.
func (a *Agent) hydrate(ctx context.Context, sessionID acp.SessionId, stored storedSession, cwd string, agentDir string) (string, [][]byte, error) {
	path := stored.record.SessionFile
	if path == "" || !fileExists(path) {
		header, _ := pi.ParseHeader(stored.rows[0])

		stamp, err := time.Parse(time.RFC3339Nano, header.Timestamp)
		if err != nil {
			stamp = time.Now()
		}

		path = pi.SessionFile(agentDir, cwd, string(sessionID), stamp)
	}

	native, err := pi.ReadRows(path)
	if err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	shared := min(len(native), len(stored.rows))
	for index := range shared {
		if !bytes.Equal(native[index], stored.rows[index]) {
			return "", nil, a.restoreRefused(ctx, sessionID, fmt.Errorf("native row %d disagrees with the store", index))
		}
	}

	if len(native) >= len(stored.rows) {
		if len(native) > len(stored.rows) {
			entries := make([]acpcore.SessionStoreEntry, 0, len(native)-len(stored.rows))
			for _, row := range native[len(stored.rows):] {
				entries = append(entries, acpcore.SessionStoreEntry(row))
			}

			if err := a.store.Append(ctx, acpcore.SessionKey{SessionID: string(sessionID)}, entries); err != nil {
				return "", nil, a.restoreRefused(ctx, sessionID, err)
			}
		}

		return path, native, nil
	}

	if err := pi.WriteRows(path, stored.rows); err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	return path, stored.rows, nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)

	return err == nil && !info.IsDir()
}

func (a *Agent) restoreRefused(ctx context.Context, sessionID acp.SessionId, err error) error {
	a.log.ErrorContext(ctx, "pi session restore failed",
		slog.String("session_id", string(sessionID)), slog.String("reason", err.Error()))

	return wire.RestoreFailed(vendor)
}

// storedTitle derives a listing title: the first user message text, else the
// session id.
func storedTitle(sessionID string, rows [][]byte) string {
	for _, row := range rows {
		var entry struct {
			Type    string          `json:"type"`
			Message json.RawMessage `json:"message"`
		}

		if json.Unmarshal(row, &entry) != nil || entry.Type != rowTypeMessage {
			continue
		}

		var message pi.AgentMessage
		if json.Unmarshal(entry.Message, &message) != nil || message.Role != messageRoleUser {
			continue
		}

		blocks, err := message.ContentBlocks()
		if err != nil {
			continue
		}

		for index := range blocks {
			if blocks[index].Type != contentBlockTypeText {
				continue
			}

			if title := normalizeTitle(blocks[index].Text); title != "" {
				return title
			}
		}
	}

	return sessionID
}

func storedCwd(rows [][]byte) string {
	if len(rows) == 0 {
		return ""
	}

	header, ok := pi.ParseHeader(rows[0])
	if !ok {
		return ""
	}

	return header.Cwd
}

func trimSpace(value string) string {
	return strings.TrimSpace(value)
}
