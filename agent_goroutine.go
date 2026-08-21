package piacp

import (
	"context"
	"log/slog"
)

// recoverAgentGoroutine is deferred at the top of agent-owned goroutines so a
// panic in background work is logged instead of crashing the embedding host.
func recoverAgentGoroutine(ctx context.Context, log *slog.Logger, name string) {
	handleAgentGoroutinePanic(ctx, log, name, nil, recover())
}

func handleAgentGoroutinePanic(
	ctx context.Context,
	log *slog.Logger,
	name string,
	shutdown func(any),
	recovered any,
) {
	if recovered == nil {
		return
	}

	if log == nil {
		log = slog.Default()
	}

	log.ErrorContext(ctx, "agent goroutine panic", slog.String("goroutine", name))

	if shutdown != nil {
		shutdown(recovered)
	}
}

func agentLogger(agent *Agent) *slog.Logger {
	if agent == nil {
		return nil
	}

	return agent.log
}
