package pi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/savid/acp-go-core/usage"
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
	EnvUsageFile        = InternalEnvPrefix + "USAGE_FILE"
	EnvUsageToken       = InternalEnvPrefix + "USAGE_TOKEN"
)

// UsageEndpoint addresses the session extension's authenticated credential read.
type UsageEndpoint struct {
	URL, Token, File string
	dir              string
}

func NewUsageEndpoint(dir string) (UsageEndpoint, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return UsageEndpoint{}, err
	}

	return UsageEndpoint{File: filepath.Join(dir, "endpoint"), Token: rand.Text(), dir: dir}, nil
}

// Wait reads the address atomically published by the listening extension.
func (e *UsageEndpoint) Wait(ctx context.Context, exited <-chan struct{}) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		address, err := readUsageAddress(e.File)
		if err == nil {
			e.URL = address

			return nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-exited:
			return errors.New("native account access extension exited before readiness")
		case <-ticker.C:
		}
	}
}

// Close removes only this generation's private endpoint directory.
func (e *UsageEndpoint) Close() error {
	if e.dir == "" {
		return nil
	}

	return os.RemoveAll(e.dir)
}

func readUsageAddress(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, 1025))
	if err != nil {
		return "", err
	}

	address, err := url.Parse(string(data))
	if err != nil || len(data) > 1024 || address.Scheme != "http" || address.Hostname() != "127.0.0.1" || address.User != nil || address.Path != "" || address.RawQuery != "" || address.Fragment != "" {
		return "", errors.New("native account access endpoint invalid")
	}

	port, err := strconv.Atoi(address.Port())
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("native account access endpoint invalid")
	}

	return address.String(), nil
}

type usageRoute struct {
	API       string            `json:"api"`
	BaseURL   string            `json:"baseUrl"`
	APIKey    string            `json:"apiKey"`
	AccountID string            `json:"accountId"`
	Headers   map[string]string `json:"headers"`
}

func (e UsageEndpoint) Access(ctx context.Context, providerID, modelID string) (usage.Access, error) {
	query := url.Values{"provider": {providerID}, "model": {modelID}}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, e.URL+"/access?"+query.Encode(), http.NoBody)
	if err != nil {
		return usage.Access{}, err
	}

	request.Header.Set("Authorization", "Bearer "+e.Token)

	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	response, err := client.Do(request)
	if err != nil {
		return usage.Access{}, errors.New("native account access failed")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return usage.Access{}, errors.New("native account access refused")
	}

	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return usage.Access{}, errors.New("native account access invalid")
	}

	var payload struct {
		Configured *bool        `json:"configured"`
		Custom     *bool        `json:"custom"`
		Routes     []usageRoute `json:"routes"`
	}
	if json.Unmarshal(data, &payload) != nil || payload.Configured == nil || payload.Custom == nil || payload.Routes == nil {
		return usage.Access{}, errors.New("native account access invalid")
	}

	if !*payload.Configured {
		return usage.Access{Reason: wire.AccountUsageNotAuthenticated}, nil
	}

	if *payload.Custom || len(payload.Routes) == 0 {
		return usage.Access{Reason: wire.AccountUsageNotReported}, nil
	}

	key := payload.Routes[0].APIKey

	accountID := payload.Routes[0].AccountID
	for _, route := range payload.Routes {
		if route.APIKey != key || route.AccountID != accountID || !route.official(providerID) {
			return usage.Access{Reason: wire.AccountUsageNotReported}, nil
		}
	}

	if strings.TrimSpace(key) == "" {
		return usage.Access{Reason: wire.AccountUsageNotAuthenticated}, nil
	}

	return usage.Access{APIKey: key, AccountID: accountID, Fingerprint: sha256.Sum256(data)}, nil
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
