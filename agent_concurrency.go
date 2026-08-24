package piacp

import (
	"context"
	"fmt"
	"time"

	"github.com/coder/acp-go-sdk"
)

const (
	defaultMaxActiveSessions        = 32
	defaultMaxConcurrentClientCalls = 16

	// sessionTurnCapacity fixes per-session prompt admission to a single
	// in-flight turn. pi serializes turns within a session and rejects a
	// second prompt while streaming, so a concurrent prompt is refused with
	// session_prompt backpressure; the capacity is not configurable.
	sessionTurnCapacity = 1

	// limitSessionPrompt names the single-foreground admission a prompt was
	// refused by: another prompt, work the agent began on its own, a native
	// queue pi has not drained, or a restore holding the session.
	limitSessionPrompt = "session_prompt"
	// limitSessionRestore names the gate an active session/load or
	// session/resume was refused by, which is the same single foreground read
	// from the other side.
	limitSessionRestore = "session_restore"
)

func validateConcurrencyLimits(limits ConcurrencyLimits) error {
	if limits.MaxActiveSessions < 0 {
		return fmt.Errorf("max active sessions must be non-negative")
	}

	if limits.MaxConcurrentClientCalls < 0 {
		return fmt.Errorf("max concurrent client calls must be non-negative")
	}

	return nil
}

// turnTimeout is the configured per-turn deadline. Zero means no deadline.
func (a *Agent) turnTimeout() time.Duration {
	return a.options.TurnTimeout
}

func (a *Agent) maxActiveSessions() int {
	if a.options.ConcurrencyLimits.MaxActiveSessions > 0 {
		return a.options.ConcurrencyLimits.MaxActiveSessions
	}

	return defaultMaxActiveSessions
}

func (a *Agent) maxConcurrentClientCalls() int {
	if a.options.ConcurrencyLimits.MaxConcurrentClientCalls > 0 {
		return a.options.ConcurrencyLimits.MaxConcurrentClientCalls
	}

	return defaultMaxConcurrentClientCalls
}

func (a *Agent) acquireClientCall(ctx context.Context) (func(), error) {
	a.mu.Lock()
	if a.clientCalls == nil {
		a.clientCalls = make(chan struct{}, a.maxConcurrentClientCalls())
	}

	calls := a.clientCalls
	a.mu.Unlock()

	select {
	case calls <- struct{}{}:
		return func() { <-calls }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("client_calls")
	}
}

func backpressureError(limit string) *acp.RequestError {
	return acp.NewInvalidRequest(map[string]any{
		jsonFieldError: "backpressure",
		"limit":        limit,
	})
}
