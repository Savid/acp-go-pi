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
// store. It is awaited on the prompt path before the prompt response returns;
// a failed commit fails the prompt. The native file is durable at
// agent_settled, so reading it after the settle fence captures the full turn.
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
				slog.String("path", path),
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

	appendCtx, finishAppend := s.agent.observe.StartSessionStore(ctx, "append")
	err = appendMirrorEntries(appendCtx, s.agent.sessionStore(), SessionKey{SessionID: string(s.id)}, newRows)

	finishAppend(err)

	if err != nil {
		return fmt.Errorf("%w: %w", errSessionMirrorAppend, err)
	}

	s.mu.Lock()
	if len(rows) > s.mirroredRows {
		s.mirroredRows = len(rows)
	}
	s.mu.Unlock()

	return nil
}

func appendMirrorEntries(ctx context.Context, store SessionStore, key SessionKey, entries []SessionStoreEntry) error {
	var lastErr error

	for _, delay := range []time.Duration{0, 200 * time.Millisecond, 800 * time.Millisecond} {
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
