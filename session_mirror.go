package piacp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

const defaultSessionMirrorAppendTimeout = 60 * time.Second

var sessionMirrorAppendTimeout = defaultSessionMirrorAppendTimeout

var errSessionMirrorAppend = errors.New("append session mirror entries")

// commitMirror appends the native session file's new raw JSONL rows to the
// store. It is awaited on the settlement path before the terminal lifecycle
// event and the prompt response; a failed commit fails the prompt. The native
// file is durable at agent_settled, so reading it after the settle fence
// captures the full cycle. A session whose delete fenced persistence writes
// nothing, so no late commit can recreate a row the delete removed.
func (s *agentSession) commitMirror(ctx context.Context) error {
	s.mu.Lock()
	path := s.sessionFilePath
	mirrored := s.mirroredRows
	s.mu.Unlock()

	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		// pi creates the session file lazily; a turn that produced no entries
		// leaves nothing to mirror.
		if errors.Is(err, os.ErrNotExist) {
			s.agent.log.DebugContext(ctx, "no native session file to mirror",
				slog.String(acpFieldSessionID, string(s.id)),
			)

			return nil
		}

		return fmt.Errorf("read native session file: %w", err)
	}

	rows := splitJSONLRows(data)
	if len(rows) <= mirrored {
		return nil
	}

	newRows := make([]SessionStoreEntry, 0, len(rows)-mirrored)
	for _, row := range rows[mirrored:] {
		newRows = append(newRows, SessionStoreEntry(row))
	}

	var appended bool

	appended, err = s.appendMirrorRows(ctx, newRows)
	if err != nil {
		return fmt.Errorf("%w: %w", errSessionMirrorAppend, err)
	}

	if !appended {
		return nil
	}

	s.mu.Lock()
	if len(rows) > s.mirroredRows {
		s.mirroredRows = len(rows)
	}
	s.mu.Unlock()

	return nil
}

func (s *agentSession) appendMirrorRows(ctx context.Context, rows []SessionStoreEntry) (appended bool, err error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	if s.persistFenced {
		return false, nil
	}

	appendCtx, finishAppend := s.agent.observe.StartSessionStore(ctx, "append")

	defer func() {
		if recovered := recover(); recovered != nil {
			finishAppend(errors.New("session store append panicked"))

			panic(recovered)
		}

		finishAppend(err)
	}()

	err = appendMirrorEntries(appendCtx, s.agent.sessionStore(), SessionKey{SessionID: string(s.id)}, rows)

	return err == nil, err
}

// mirrorAppendDelays is the wait before each attempt at one durable append. A
// store that refuses every attempt has refused the commit.
var mirrorAppendDelays = []time.Duration{0, 200 * time.Millisecond, 800 * time.Millisecond}

func appendMirrorEntries(ctx context.Context, store SessionStore, key SessionKey, entries []SessionStoreEntry) error {
	var lastErr error

	for _, delay := range mirrorAppendDelays {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		appendCtx, cancel := context.WithTimeout(ctx, sessionMirrorAppendTimeout)
		err := store.Append(appendCtx, key, entries)

		cancel()

		if err == nil {
			return nil
		}

		lastErr = err

		if appendCtx.Err() == context.DeadlineExceeded {
			break
		}
	}

	return lastErr
}

func splitJSONLRows(data []byte) [][]byte {
	lines := bytes.Split(data, []byte("\n"))
	rows := make([][]byte, 0, len(lines))

	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		rows = append(rows, append([]byte(nil), trimmed...))
	}

	return rows
}
