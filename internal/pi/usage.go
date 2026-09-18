package pi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/savid/acp-go-core/usage/anthropic"
	"github.com/savid/acp-go-core/usage/openaicodex"
	"github.com/savid/acp-go-core/usage/opencodego"
	"github.com/savid/acp-go-core/usage/openrouter"
	"github.com/savid/acp-go-core/wire"
)

const (
	usageAPICodex       = "openai-codex-responses"
	usageAPIAnthropic   = "anthropic-messages"
	usageAPICompletions = "openai-completions"
	EnvUsageURL         = InternalEnvPrefix + "USAGE_URL"
	EnvUsageToken       = InternalEnvPrefix + "USAGE_TOKEN"
)

// UsageEndpoint addresses the session extension's authenticated credential read.
type UsageEndpoint struct{ URL, Token string }

func NewUsageEndpoint() (UsageEndpoint, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return UsageEndpoint{}, err
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return UsageEndpoint{}, err
	}

	return UsageEndpoint{URL: "http://" + address, Token: rand.Text()}, nil
}

// UsageAccess keeps credential material inside the adapter.
type UsageAccess struct {
	APIKey      string
	AccountID   string
	Reason      string
	Fingerprint [32]byte
}

type usageRoute struct {
	API       string            `json:"api"`
	BaseURL   string            `json:"baseUrl"`
	APIKey    string            `json:"apiKey"`
	AccountID string            `json:"accountId"`
	Headers   map[string]string `json:"headers"`
}

func (e UsageEndpoint) Access(ctx context.Context, providerID, modelID string) (UsageAccess, error) {
	query := url.Values{"provider": {providerID}, "model": {modelID}}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, e.URL+"/access?"+query.Encode(), http.NoBody)
	if err != nil {
		return UsageAccess{}, err
	}

	request.Header.Set("Authorization", "Bearer "+e.Token)

	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	response, err := client.Do(request)
	if err != nil {
		return UsageAccess{}, errors.New("native account access failed")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return UsageAccess{}, errors.New("native account access refused")
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return UsageAccess{}, errors.New("native account access invalid")
	}

	var payload struct {
		Configured *bool        `json:"configured"`
		Custom     *bool        `json:"custom"`
		Routes     []usageRoute `json:"routes"`
	}
	if json.Unmarshal(data, &payload) != nil || payload.Configured == nil || payload.Custom == nil || payload.Routes == nil {
		return UsageAccess{}, errors.New("native account access invalid")
	}

	if !*payload.Configured {
		return UsageAccess{Reason: wire.AccountUsageNotAuthenticated}, nil
	}

	if *payload.Custom || len(payload.Routes) == 0 {
		return UsageAccess{Reason: wire.AccountUsageNotReported}, nil
	}

	key := payload.Routes[0].APIKey

	accountID := payload.Routes[0].AccountID
	for _, route := range payload.Routes {
		if route.APIKey != key || route.AccountID != accountID || !route.official(providerID) {
			return UsageAccess{Reason: wire.AccountUsageNotReported}, nil
		}
	}

	if strings.TrimSpace(key) == "" {
		return UsageAccess{Reason: wire.AccountUsageNotAuthenticated}, nil
	}

	return UsageAccess{APIKey: key, AccountID: accountID, Fingerprint: sha256.Sum256(data)}, nil
}

func (r usageRoute) official(providerID string) bool {
	var base string

	switch providerID {
	case openrouter.ProviderID:
		base = strings.TrimSuffix(openrouter.Endpoint, "/key")
	case opencodego.ProviderID:
		base = strings.TrimSuffix(opencodego.Endpoint, "/usage")
	case openaicodex.ProviderID:
		if strings.TrimSpace(r.AccountID) == "" {
			return false
		}

		base = strings.TrimSuffix(openaicodex.Endpoint, "/wham/usage")
	case anthropic.ProviderID:
		if r.API != usageAPIAnthropic {
			return false
		}

		base = strings.TrimSuffix(anthropic.Endpoint, "/api/oauth/usage")
	default:
		return false
	}

	switch r.API {
	case usageAPICodex:
		if providerID != openaicodex.ProviderID {
			return false
		}
	case usageAPIAnthropic:
		base = strings.TrimSuffix(base, "/v1")
	case usageAPICompletions, "openai-responses":
	default:
		return false
	}

	if providerID == openaicodex.ProviderID && r.API != usageAPICodex {
		return false
	}

	if strings.TrimSuffix(r.BaseURL, "/") != base {
		return false
	}

	for name, value := range r.Headers {
		switch strings.ToLower(name) {
		case "authorization":
			if value != "Bearer "+r.APIKey {
				return false
			}
		case "x-api-key":
			if value != r.APIKey {
				return false
			}
		case "chatgpt-account-id":
			if providerID != openaicodex.ProviderID || value != r.AccountID {
				return false
			}
		case "http-referer", "x-title", "x-source":
		case "anthropic-beta", "anthropic-version":
			if providerID != anthropic.ProviderID {
				return false
			}
		default:
			return false
		}
	}

	return true
}
