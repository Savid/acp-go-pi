package pi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestUsageAccessVerifiesNativeRoutes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, provider, api, endpoint string
		headers                       map[string]string
		custom                        bool
		reason                        string
	}{
		{name: "OpenRouter", provider: "openrouter", api: usageAPICompletions, endpoint: "https://openrouter.ai/api/v1"},
		{name: "OpenRouter Anthropic", provider: "openrouter", api: usageAPIAnthropic, endpoint: "https://openrouter.ai/api"},
		{name: "Go responses", provider: "opencode-go", api: "openai-responses", endpoint: "https://opencode.ai/zen/go/v1"},
		{name: "Go Anthropic", provider: "opencode-go", api: usageAPIAnthropic, endpoint: "https://opencode.ai/zen/go"},
		{name: "proxy", provider: "openrouter", api: usageAPICompletions, endpoint: "https://proxy.invalid/v1", reason: wire.AccountUsageNotReported},
		{name: "custom implementation", provider: "openrouter", api: usageAPICompletions, endpoint: "https://openrouter.ai/api/v1", custom: true, reason: wire.AccountUsageNotReported},
		{name: "matching auth", provider: "openrouter", api: usageAPICompletions, endpoint: "https://openrouter.ai/api/v1", headers: map[string]string{"Authorization": "Bearer native-key"}},
		{name: "different auth", provider: "openrouter", api: usageAPICompletions, endpoint: "https://openrouter.ai/api/v1", headers: map[string]string{"Authorization": "Bearer other"}, reason: wire.AccountUsageNotReported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer local-token" || r.URL.Query().Get("provider") != tc.provider {
					w.WriteHeader(http.StatusForbidden)

					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"configured": true, "custom": tc.custom, "routes": []usageRoute{{API: tc.api, BaseURL: tc.endpoint, APIKey: "native-key", Headers: tc.headers}}})
			}))
			defer server.Close()
			access, err := (UsageEndpoint{URL: server.URL, Token: "local-token"}).Access(t.Context(), tc.provider, "")
			require.NoError(t, err)
			require.Equal(t, tc.reason, access.Reason)
			if tc.reason == "" {
				require.Equal(t, "native-key", access.APIKey)
			} else {
				require.Empty(t, access.APIKey)
			}
		})
	}
}
