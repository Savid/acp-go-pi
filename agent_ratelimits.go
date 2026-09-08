package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"
)

func (a *Agent) handleRateLimits(ctx context.Context, raw json.RawMessage) (RateLimitsResponse, error) {
	request, decodeErr := decodeRateLimitsRequest(raw)
	if decodeErr != nil {
		return RateLimitsResponse{}, decodeErr
	}

	if err := ctx.Err(); err != nil {
		return RateLimitsResponse{}, err
	}

	model := a.options.DefaultModel

	var selected *agentSession

	if request.SessionID != "" {
		session, err := a.session(request.SessionID)
		if err != nil {
			return RateLimitsResponse{}, err
		}

		model, err = rateLimitsSessionModel(session)
		if err != nil {
			return RateLimitsResponse{}, err
		}

		selected = session
	}

	provider := request.ProviderID
	if provider == "" {
		selected, modelID, found := strings.Cut(model, "/")
		if !found || strings.TrimSpace(selected) == "" || strings.TrimSpace(modelID) == "" {
			return RateLimitsResponse{}, acp.NewInvalidParams(map[string]any{rateLimitsFieldError: valMissing, rateLimitsFieldField: rateLimitsFieldProviderID})
		}

		provider = selected
	}

	if err := a.quotaOwnerError(selected); err != nil {
		return RateLimitsResponse{}, err
	}

	if provider != "openrouter" && provider != "opencode-go" {
		return rateLimitsUnsupported(provider), nil
	}

	if !a.options.DirectAPI {
		return rateLimitsUnavailable(provider, "disabled"), nil
	}

	if selected == nil {
		return rateLimitsUnavailable(provider, "session_required"), nil
	}

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	response, readErr := selected.readQuota(readCtx, provider, model)
	readErr = errors.Join(readErr, a.quotaOwnerError(selected))

	if err := ctx.Err(); err != nil {
		return RateLimitsResponse{}, errors.Join(err, readErr)
	}

	if err := a.ensureOpen(); err != nil {
		return RateLimitsResponse{}, errors.Join(err, readErr)
	}

	current, err := a.session(request.SessionID)
	if err != nil {
		return RateLimitsResponse{}, errors.Join(err, readErr)
	}

	currentModel, err := rateLimitsSessionModel(current)
	if err != nil {
		return RateLimitsResponse{}, errors.Join(err, readErr)
	}

	if readErr != nil {
		return RateLimitsResponse{}, readErr
	}

	if current != selected || currentModel != model {
		return rateLimitsUnavailable(provider, "read_failed"), nil
	}

	return response, nil
}

func (a *Agent) quotaOwnerError(session *agentSession) error {
	var sessionErr error
	if session != nil {
		sessionErr = session.nativeContainmentError()
	}

	return errors.Join(a.optionsError(), a.nativeContainmentError(), a.nativeAdmissionError(), sessionErr)
}

func rateLimitsSessionModel(session *agentSession) (string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.closing {
		return "", unknownSessionError()
	}

	if session.poisonCause != "" {
		return "", poisonedSessionError(session.poisonCause)
	}

	return session.model, nil
}
