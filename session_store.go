package piacp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

// configSubpath holds the current session configuration.
const configSubpath = sessionlog.ConfigSubpath

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

// commitMirror publishes the native rows and current session configuration
// as one durable generation.
func (s *session) commitMirror(ctx context.Context) error {
	s.mirrorMu.Lock()
	defer s.mirrorMu.Unlock()

	s.mu.Lock()
	path := s.sessionFile
	mirrored := s.mirrored
	s.mu.Unlock()

	if path == "" {
		return nil
	}

	rows, err := pi.ReadRows(path)
	if err != nil {
		return fmt.Errorf("read native session: %w", err)
	}

	if len(rows) < mirrored {
		return fmt.Errorf("native log shrank from %d to %d rows", mirrored, len(rows))
	}

	if len(rows) == 0 {
		return nil
	}

	commitCtx, finish := s.agent.observe.StartSessionStore(ctx, "replace")
	err = sessionlog.Commit(commitCtx, s.agent.store, string(s.id), rows, s.record())
	finish(err)

	if err != nil {
		return fmt.Errorf("commit session mirror: %w", err)
	}

	s.mu.Lock()
	s.mirrored = len(rows)
	s.mu.Unlock()

	return nil
}

// storedSession is what the store holds for one session id.
type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

// loadStored reads the native rows and required current configuration.
func (a *Agent) loadStored(ctx context.Context, sessionID acp.SessionId) (storedSession, error) {
	loadCtx, cancel := context.WithTimeout(ctx, a.options.SessionStoreLoadTimeout)
	defer cancel()

	loadCtx, finish := a.observe.StartSessionStore(loadCtx, "load")

	var record sessionRecord

	rows, err := sessionlog.Load(loadCtx, a.store, string(sessionID), &record)
	if err == nil && len(rows) > 0 {
		err = record.validate(string(sessionID))
	}

	finish(err)

	if err != nil {
		return storedSession{}, a.restoreRefused(ctx, sessionID, err)
	}

	return storedSession{rows: rows, record: record, found: len(rows) > 0}, nil
}

func (r sessionRecord) validate(sessionID string) error {
	if r.SessionID != sessionID || !filepath.IsAbs(r.Cwd) || !filepath.IsAbs(r.SessionFile) || r.UpdatedAtUnixMilli <= 0 {
		return fmt.Errorf("invalid session record identity or location")
	}

	if err := process.ValidateNames(r.Env); err != nil {
		return err
	}

	if err := process.ValidateExtraPathDirs(r.ExtraPathDirs); err != nil {
		return err
	}

	return nil
}

// hydrate reconciles the store with pi's own file before a load or resume. An
// existing native file at least as long as the store wins and its newer rows
// are adopted; a missing or shorter one is materialized from the store. A
// disagreement at a shared position fails the restore. It returns the native
// path and the rows the session now holds.
func (a *Agent) hydrate(ctx context.Context, sessionID acp.SessionId, stored storedSession, cwd string, agentDir string) (string, [][]byte, error) {
	header, ok := pi.ParseHeader(stored.rows[0])
	if !ok || header.ID != string(sessionID) || filepath.Base(header.ID) != header.ID {
		return "", nil, a.restoreRefused(ctx, sessionID, fmt.Errorf("invalid native session identity"))
	}

	stamp, err := time.Parse(time.RFC3339Nano, header.Timestamp)
	if err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	path := stored.record.SessionFile
	if !fileExists(path) {
		path = pi.SessionFile(agentDir, cwd, string(sessionID), stamp)
	}

	native, err := pi.ReadRows(path)
	if err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	if _, err := sessionlog.Reconcile(native, stored.rows); err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	if len(native) >= len(stored.rows) {
		if len(native) > len(stored.rows) {
			if err := sessionlog.Commit(ctx, a.store, string(sessionID), native, stored.record); err != nil {
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

func trimSpace(value string) string {
	return strings.TrimSpace(value)
}
