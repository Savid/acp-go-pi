package piacp

import (
	"context"
	"log/slog"
)

// publishSessionOpen emits the establishing snapshot for one session, exactly
// once: the explicit command catalog — including the empty case, which is an
// answer rather than a silence a host has to guess at — and the opening
// lifecycle stream for the native generation the session is running on.
//
// A lifecycle stream this session cannot open is not opened: the failure is
// recorded and the incarnation is fenced, so the next one states the truthful
// state rather than continuing from a first event that never landed.
func (s *agentSession) publishSessionOpen(ctx context.Context) {
	s.mu.Lock()
	if s.opened {
		s.mu.Unlock()

		return
	}

	s.opened = true
	generation := s.pumpGeneration
	s.mu.Unlock()

	if err := s.emitAvailableCommandsUpdate(ctx, true); err != nil {
		s.agent.log.ErrorContext(ctx, "publish initial pi command catalog failed",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String(jsonFieldError, err.Error()),
		)
	}

	if err := s.openLifecycleStream(ctx, generation); err != nil {
		s.fenceLifecycleStream()
		s.agent.log.ErrorContext(ctx, "open pi session lifecycle stream failed",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String(jsonFieldError, err.Error()),
		)
	}
}

// publishSessionOpenInline emits the establishing snapshot for a host whose
// connection has no post-response hook to defer it behind. The stdio connection
// this package builds writes the establishing response first and then runs the
// hook; an embedded host holding the Agent directly has no transport write to
// order against, so its snapshot is published as the establishing call returns.
func (a *Agent) publishSessionOpenInline(ctx context.Context, session *agentSession) {
	if _, deferred := a.connection().(*localAgentConnection); deferred {
		return
	}

	session.publishSessionOpen(ctx)
}
