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

	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

// sessionRecord is the adapter-owned state a session needs to resume and be
// listed: where pi keeps the native file, the configuration the session was
// established with, and its title.
type sessionRecord struct {
	SessionID             string            `json:"sessionId"`
	NativeSessionID       string            `json:"nativeSessionId"`
	Cwd                   string            `json:"cwd"`
	AdditionalDirectories []string          `json:"additionalDirectories,omitempty"`
	NativePath            string            `json:"nativePath"`
	Title                 string            `json:"title,omitempty"`
	Env                   map[string]string `json:"env,omitempty"`
	ExtraPathDirs         []string          `json:"extraPathDirs,omitempty"`
	Model                 string            `json:"model,omitempty"`
	ThinkingLevel         string            `json:"thinkingLevel,omitempty"`
	Permission            string            `json:"permission,omitempty"`
	AutoRetry             bool              `json:"autoRetry,omitempty"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{
		SessionID:             string(s.id),
		NativeSessionID:       s.nativeID,
		Cwd:                   s.cwd,
		AdditionalDirectories: slices.Clone(s.additionalDirectories),
		NativePath:            s.sessionFile,
		Title:                 s.title,
		Env:                   maps.Clone(s.options.Env),
		ExtraPathDirs:         slices.Clone(s.options.ExtraPathDirs),
		Model:                 s.model,
		ThinkingLevel:         s.thinkingLevel,
		Permission:            s.options.Permission,
		AutoRetry:             s.options.AutoRetry,
	}
}

// commitMirror publishes the rows pi appended to its session file since the
// last commit, with the current session configuration, as one durable
// generation. The record is taken under the mirror's lock after the read, so
// it describes the rows it commits; the caller must not hold s.mu.
func (s *session) commitMirror(ctx context.Context) error {
	s.mu.Lock()
	path := s.sessionFile
	mirror := s.mirror
	ephemeral := s.ephemeral
	s.mu.Unlock()

	if path == "" || ephemeral || mirror == nil {
		return nil
	}

	commitCtx, finish := s.agent.observe.StartSessionStore(ctx, observer.SessionStoreCommit)
	report, err := mirror.Commit(commitCtx, sessionlog.File(path), func(sessionlog.Tail) (any, error) { return s.record(), nil })
	s.agent.observe.RecordRepublish(commitCtx, string(report.Republish))
	finish(err)

	s.logRepublish(ctx, report)

	if err != nil {
		return fmt.Errorf("commit session mirror: %w", err)
	}

	s.mu.Lock()
	s.persisted = true
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
	loadCtx, finish := a.observe.StartSessionStore(ctx, observer.SessionStoreLoad)

	var record sessionRecord

	rows, found, err := sessionlog.Load(loadCtx, a.store, string(sessionID), &record)
	if err == nil && found {
		err = record.validate(string(sessionID))
	}

	finish(err)

	if err != nil {
		return storedSession{}, wire.RestoreRefused(ctx, a.log, vendor, sessionID, err)
	}

	return storedSession{rows: rows, record: record, found: found}, nil
}

// loadRecord reads only the configuration record of a stored session, as
// listing needs no native rows.
func (a *Agent) loadRecord(ctx context.Context, sessionID acp.SessionId) (sessionRecord, bool, error) {
	loadCtx, finish := a.observe.StartSessionStore(ctx, observer.SessionStoreLoadRecord)

	var record sessionRecord

	found, err := sessionlog.LoadRecord(loadCtx, a.store, string(sessionID), &record)
	if err == nil && found {
		err = record.validate(string(sessionID))
	}

	finish(err)

	return record, found, err
}

func (r sessionRecord) validate(sessionID string) error {
	if r.NativeSessionID == "" || r.SessionID != sessionID || !filepath.IsAbs(r.Cwd) || !filepath.IsAbs(r.NativePath) {
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

// hydrate reconciles the store with pi's own file before a load or resume
// through the session's mirror. An existing native file at least as long as
// the store wins and its newer rows are adopted; a missing or shorter one is
// materialized from the store. A disagreement at a shared position, or a row
// the adapter cannot decode, fails the restore. It returns the native path and
// the rows the session now holds.
func (a *Agent) hydrate(ctx context.Context, s *session, stored storedSession, cwd string) (string, [][]byte, error) {
	path, err := a.nativePath(ctx, s.id, stored, cwd, s.agentDir)
	if err != nil {
		return "", nil, err
	}

	rows, hydrated, err := s.mirror.Hydrate(ctx, path, stored.rows, stored.record, validateRows)
	if err != nil {
		return "", nil, wire.RestoreRefused(ctx, a.log, vendor, s.id, err)
	}

	if hydrated.Changed() {
		a.log.WarnContext(ctx, "native session file repaired on restore",
			slog.String("session_id", string(s.id)),
			slog.Int64("torn_bytes_cut", hydrated.Tail.Cut),
			slog.Bool("record_terminated", hydrated.Tail.Terminated),
			slog.Int64("invalid_bytes_cut", hydrated.InvalidCut),
			slog.Bool("rewritten_from_store", hydrated.Rewritten))
	}

	s.logRepublish(ctx, hydrated.Commit)

	return path, rows, nil
}

// logRepublish warns once for a commit that republished the whole
// generation for a reason other than its first publication.
func (s *session) logRepublish(ctx context.Context, report sessionlog.CommitReport) {
	if !report.Recovery() {
		return
	}

	s.agent.log.WarnContext(ctx, "session mirror republished",
		slog.String("session_id", string(s.id)),
		slog.String("reason", string(report.Republish)),
		slog.Any("append_error", report.AppendErr))
}

// cutTornTail cuts a final session-file line a dead pi left torn, and warns
// once when it changed the file.
func (s *session) cutTornTail(ctx context.Context, path string) error {
	torn, err := sessionlog.CutTornTail(path)
	if err != nil {
		return err
	}

	if torn.Changed() {
		s.agent.log.WarnContext(ctx, "native session file tail repaired",
			slog.String("session_id", string(s.id)),
			slog.Int64("bytes_cut", torn.Cut),
			slog.Bool("record_terminated", torn.Terminated))
	}

	return nil
}

// nativePath locates the session file a restore continues. The recorded path
// is used while it still names a file under the agent directory; otherwise the
// header row's timestamp reproduces the name pi gives it. A committed
// conversation with no native history carries no header row, so the recorded
// path is the only identity it has, and one outside the agent directory is
// refused.
func (a *Agent) nativePath(ctx context.Context, sessionID acp.SessionId, stored storedSession, cwd string, agentDir string) (string, error) {
	path := stored.record.NativePath

	if len(stored.rows) == 0 {
		if !underDir(agentDir, path) {
			return "", wire.RestoreRefused(ctx, a.log, vendor, sessionID, errors.New("native session path is outside the agent directory"))
		}

		return path, nil
	}

	header, ok := pi.ParseHeader(stored.rows[0])
	if !ok || header.ID != stored.record.NativeSessionID || filepath.Base(header.ID) != header.ID {
		return "", wire.RestoreRefused(ctx, a.log, vendor, sessionID, errors.New("invalid native session identity"))
	}

	stamp, err := time.Parse(time.RFC3339Nano, header.Timestamp)
	if err != nil {
		return "", wire.RestoreRefused(ctx, a.log, vendor, sessionID, err)
	}

	if !underDir(agentDir, path) || !fileExists(path) {
		path = pi.SessionFile(agentDir, cwd, stored.record.NativeSessionID, stamp)
	}

	return path, nil
}

// underDir reports whether path names an entry below dir.
func underDir(dir string, path string) bool {
	relative, err := filepath.Rel(dir, path)

	return err == nil && relative != "." && filepath.IsLocal(relative)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)

	return err == nil && !info.IsDir()
}
