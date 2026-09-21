package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/coder/acp-go-sdk"

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
	NativeSessionID       string            `json:"nativeSessionId"`
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
		NativeSessionID:       s.nativeID,
		Cwd:                   s.cwd,
		AdditionalDirectories: slices.Clone(s.additionalDirectories),
		SessionFile:           s.sessionFile,
		Env:                   maps.Clone(s.options.Env),
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
	ephemeral := s.ephemeral
	s.mu.Unlock()

	if path == "" || ephemeral {
		return nil
	}

	rows, err := pi.ReadRows(path)
	if err != nil {
		return fmt.Errorf("read native session: %w", err)
	}

	if len(rows) < mirrored {
		return fmt.Errorf("native log shrank from %d to %d rows", mirrored, len(rows))
	}

	commitCtx, finish := s.agent.observe.StartSessionStore(ctx, "replace")
	err = sessionlog.Commit(commitCtx, s.agent.store, string(s.id), rows, s.record())
	finish(err)

	if err == nil {
		s.mu.Lock()
		s.persisted = true
		s.mu.Unlock()
	}

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
	loadCtx, finish := a.observe.StartSessionStore(ctx, "load")

	var record sessionRecord

	rows, found, err := sessionlog.Load(loadCtx, a.store, string(sessionID), &record)
	if err == nil && found {
		err = record.validate(string(sessionID))
	}

	finish(err)

	if err != nil {
		return storedSession{}, a.restoreRefused(ctx, sessionID, err)
	}

	return storedSession{rows: rows, record: record, found: found}, nil
}

func (r sessionRecord) validate(sessionID string) error {
	if r.NativeSessionID == "" || r.SessionID != sessionID || !filepath.IsAbs(r.Cwd) || !filepath.IsAbs(r.SessionFile) || r.UpdatedAtUnixMilli <= 0 {
		return fmt.Errorf("invalid session record identity or location")
	}

	for _, directory := range r.AdditionalDirectories {
		if !filepath.IsAbs(directory) {
			return fmt.Errorf("invalid additional directory")
		}
	}

	if _, err := parseSessionMeta(inheritCarrier(sessionMeta{}, r).Meta()); err != nil {
		return err
	}

	return nil
}

// hydrate reconciles the store with pi's own file before a load or resume. An
// existing native file at least as long as the store wins and its newer rows
// are adopted; a missing or shorter one is materialized from the store. A
// disagreement at a shared position, or a row the adapter cannot decode, fails
// the restore. It returns the native path and the rows the session now holds.
func (a *Agent) hydrate(ctx context.Context, sessionID acp.SessionId, stored storedSession, cwd string, agentDir string) (string, [][]byte, error) {
	path, err := a.nativePath(ctx, sessionID, stored, cwd, agentDir)
	if err != nil {
		return "", nil, err
	}

	native, err := pi.ReadRows(path)
	if err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	rows, nativeWins, err := sessionlog.Reconcile(native, stored.rows)
	if err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	if err := validateRows(rows); err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	if nativeWins {
		if len(native) > len(stored.rows) {
			if err := sessionlog.Commit(ctx, a.store, string(sessionID), rows, stored.record); err != nil {
				return "", nil, a.restoreRefused(ctx, sessionID, err)
			}
		}

		return path, rows, nil
	}

	if err := pi.WriteRows(path, rows); err != nil {
		return "", nil, a.restoreRefused(ctx, sessionID, err)
	}

	return path, rows, nil
}

// nativePath locates the session file a restore continues. The recorded path
// is used while it still names a file under the agent directory; otherwise the
// header row's timestamp reproduces the name pi gives it. A committed
// conversation with no native history carries no header row, so the recorded
// path is the only identity it has.
func (a *Agent) nativePath(ctx context.Context, sessionID acp.SessionId, stored storedSession, cwd string, agentDir string) (string, error) {
	path := stored.record.SessionFile

	if len(stored.rows) == 0 {
		return path, nil
	}

	header, ok := pi.ParseHeader(stored.rows[0])
	if !ok || header.ID != stored.record.NativeSessionID || filepath.Base(header.ID) != header.ID {
		return "", a.restoreRefused(ctx, sessionID, errors.New("invalid native session identity"))
	}

	stamp, err := time.Parse(time.RFC3339Nano, header.Timestamp)
	if err != nil {
		return "", a.restoreRefused(ctx, sessionID, err)
	}

	relative, pathErr := filepath.Rel(agentDir, path)
	if pathErr != nil || !filepath.IsLocal(relative) || !fileExists(path) {
		path = pi.SessionFile(agentDir, cwd, stored.record.NativeSessionID, stamp)
	}

	return path, nil
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
		message, blocks, ok, err := messageRow(row)
		if err != nil || !ok || message.Role != messageRoleUser {
			continue
		}

		for index := range blocks {
			if blocks[index].Type != contentBlockTypeText {
				continue
			}

			if title := wire.NormalizeTitle(blocks[index].Text); title != "" {
				return title
			}
		}
	}

	return sessionID
}
