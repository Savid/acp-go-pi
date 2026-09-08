package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/savid/acp-go-pi/internal/pi"
)

// The native command owns credentials and HTTP. This exchange carries only
// source observations and is fenced to the exact process generation.
type quotaExchange struct {
	cancel <-chan struct{}
	done   chan struct{}
	answer chan RateLimitsResponse
	outbox *sessionOutbox
}

type quotaBridgeMessage struct {
	ID       string             `json:"id"`
	Kind     string             `json:"kind"`
	Response RateLimitsResponse `json:"response"`
}

func (s *agentSession) readQuota(ctx context.Context, provider, model string) (RateLimitsResponse, error) {
	failed := rateLimitsUnavailable(provider, "read_failed")

	id, err := newAuthToken()
	if err != nil {
		return failed, nil
	}

	s.mu.Lock()

	client, outbox := s.client, s.outbox
	if client == nil || outbox == nil {
		s.mu.Unlock()

		return rateLimitsUnavailable(provider, "session_required"), nil
	}

	exchange := &quotaExchange{cancel: ctx.Done(), done: make(chan struct{}), answer: make(chan RateLimitsResponse, 1), outbox: outbox}
	if s.quotaExchanges == nil {
		s.quotaExchanges = make(map[string]*quotaExchange)
	}

	s.quotaExchanges[id] = exchange
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.quotaExchanges, id)
		s.mu.Unlock()
		close(exchange.done)
	}()

	boundary := pi.CallBoundary{BeforeDispatch: func() (func(), error) {
		if err := outbox.dispatchMu.lock(ctx); err != nil {
			return nil, err
		}

		if err := s.agent.quotaOwnerError(s); err != nil {
			outbox.dispatchMu.Unlock()

			return nil, err
		}

		if !s.quotaSourceCurrent(outbox, model) {
			outbox.dispatchMu.Unlock()

			return nil, errAuthBridge
		}

		return outbox.dispatchMu.Unlock, nil
	}}

	command := pi.EncodeAuthCommand(pi.AuthRequest{ID: id, Op: pi.AuthOpQuota, ProviderID: provider})
	if err := client.PromptWithBoundary(ctx, command, nil, boundary); err != nil {
		if !nativeContainmentComplete(err) || errors.Is(err, ErrNativeTreeBusy) {
			return RateLimitsResponse{}, err
		}

		return failed, nil
	}

	select {
	case response := <-exchange.answer:
		if ctx.Err() != nil || !s.quotaSourceCurrent(outbox, model) || !validQuotaBridgeResponse(provider, response) {
			return failed, nil
		}

		return response, nil
	case <-ctx.Done():
		return failed, nil
	}
}

func (s *agentSession) quotaSourceCurrent(outbox *sessionOutbox, model string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing || s.poisonCause != "" || s.outbox != outbox || s.client != outbox.client || s.model != model {
		return false
	}

	outbox.mu.Lock()
	defer outbox.mu.Unlock()

	return !outbox.closing && !outbox.fenced && !outbox.ended
}

func validQuotaBridgeResponse(provider string, response RateLimitsResponse) bool {
	if response.ProviderID != provider || response.Pools == nil {
		return false
	}

	switch response.Availability {
	case "available":
		return response.Reason == ""
	case rateLimitsAvailabilityUnsupported:
		return response.Reason == "" && len(response.Pools) == 0
	case rateLimitsAvailabilityUnavailable:
		if len(response.Pools) != 0 {
			return false
		}

		switch response.Reason {
		case "not_authenticated", "session_required", "read_failed", "not_observed":
			return true
		}
	}

	return false
}

// Quota dialogs are consumed ahead of raw events and work even when provider
// auth is not configured. A late or unowned marker is cancelled on its source
// client, never exposed as a user dialog.
func (s *agentSession) routeQuotaDialog(ctx context.Context, outbox *sessionOutbox, request pi.UIRequest, release func()) bool {
	payload, ok := strings.CutPrefix(request.Title, pi.AuthTitleMarker)
	if !ok {
		return false
	}

	var message quotaBridgeMessage
	if err := json.Unmarshal([]byte(payload), &message); err != nil ||
		(message.Kind != pi.AuthKindQuota && message.Kind != pi.AuthKindQuotaCancel) {
		return false
	}

	s.mu.Lock()
	exchange := s.quotaExchanges[message.ID]
	s.mu.Unlock()

	go func() {
		defer recoverAgentGoroutine(ctx, agentLogger(s.agent), "quota dialog")
		defer release()

		response := pi.UICancelResponse(request.ID)

		if exchange != nil && exchange.outbox == outbox {
			if message.Kind == pi.AuthKindQuota {
				select {
				case exchange.answer <- message.Response:
				default:
				}
			} else {
				select {
				case <-exchange.cancel:
				case <-exchange.done:
				case <-ctx.Done():
				}
			}

			response = pi.UIValueResponse(request.ID, pi.AuthAck)
		}

		s.respondExactClientUIDialog(ctx, outbox.client, response)
	}()

	return true
}
