package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

func (s *agentSession) initializeDurability(ctx context.Context) error {
	initialCtx, cancel := context.WithTimeout(ctx, sessionSettleTimeout)
	defer cancel()

	entries, err := s.client.InitialSession(initialCtx)
	if err != nil {
		return fmt.Errorf("read native session initialization: %w", err)
	}

	var header struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if len(entries) == 0 || json.Unmarshal(entries[0], &header) != nil || header.Type != storeRowTypeSession || header.ID == "" || header.ID != string(s.id) {
		return errors.New("native session initialization does not match session id")
	}

	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancelWrite()

	appended, err := s.appendMirrorRows(writeCtx, entries)
	if err != nil {
		return fmt.Errorf("initialize session durability: %w", err)
	}

	if !appended {
		return errors.New("session initialization persistence fenced")
	}

	s.mu.Lock()
	s.mirroredRows = len(entries)
	s.mu.Unlock()

	return s.commitLifecycleBoundary(writeCtx, lifecycleBoundaryRecord{
		NativeRows: len(entries), NativeState: nativeStateCommitted,
	})
}
