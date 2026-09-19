package piacp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

const usageSessionField = "sessionId"

type usageTransportFunc func(*http.Request) (*http.Response, error)

func (f usageTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAccountUsageBindsNativeCredentialsAndHoldsGate(t *testing.T) {
	t.Parallel()
	for _, rotate := range []bool{false, true} {
		t.Run(map[bool]string{false: "stable", true: "rotated"}[rotate], func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "providers.json")
			write := func(key string) {
				t.Helper()
				require.NoError(t, os.WriteFile(path, []byte(`{"configured":true,"custom":false,"routes":[{"apiKey":"`+key+`","api":"openai-completions","baseUrl":"https://openrouter.ai/api/v1"}]}`), 0600))
			}
			write("native-key")
			a := NewAgent(testOptions(t, WithEnv(map[string]string{fakePiEnv: "1", "ACP_GO_PI_TEST_USAGE_ACCESS": path}))...)
			t.Cleanup(func() { require.NoError(t, a.Close()) })
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			a.usageTransport = usageTransportFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") != "Bearer native-key" {
					return nil, io.ErrUnexpectedEOF
				}
				if r.URL.Path == "/api/v1/key" {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}

					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":{"limit":0,"limit_remaining":-0.02,"usage":1.125}}`))}, nil
				}

				return &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: http.NoBody}, nil
			})
			initialized, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
			require.NoError(t, err)
			require.Contains(t, initialized.AgentCapabilities.Meta["pi"], wire.AccountUsageCapabilityKey)
			session, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err)
			params, err := json.Marshal(map[string]any{usageSessionField: session.SessionId, "providerId": "openrouter"})
			require.NoError(t, err)
			type result struct {
				response wire.AccountUsageResponse
				err      error
			}
			done := make(chan result, 1)
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			go func() { response, readErr := a.accountUsage(ctx, params); done <- result{response, readErr} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("provider read not reached")
			}
			_, err = a.accountUsage(ctx, params)
			require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])
			_, err = a.Prompt(ctx, wire.PromptRequest(session.SessionId))
			require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])
			if rotate {
				write("changed-key")
			}
			unblock()
			got := <-done
			if rotate {
				require.Equal(t, "account_usage", requestErrorData(t, got.err)["class"])
				require.False(t, got.response.Available)

				return
			}
			require.NoError(t, got.err)
			require.Len(t, got.response.Balances, 2)
			require.Equal(t, 0.0, got.response.Balances[0].Limit.Amount)
			require.Equal(t, -0.02, got.response.Balances[0].Remaining.Amount)
			require.Equal(t, 1.125, got.response.Balances[1].Used.Amount)
		})
	}
}

// A provider pi holds no native account for is read through the report of a
// gateway an extension registered, keyed to that provider's section.
func TestAccountUsageReadsThroughGatewayRoutes(t *testing.T) {
	t.Parallel()
	access := filepath.Join(t.TempDir(), "access.json")
	require.NoError(t, os.WriteFile(access, []byte(`{"configured":false,"custom":false,"routes":[]}`), 0o600))
	gateways := filepath.Join(t.TempDir(), "gateways.json")
	require.NoError(t, os.WriteFile(gateways, []byte(`{"routes":[{"provider":"proxy","baseUrl":"https://proxy.example/v1","apiKey":"proxy-key"},{"provider":"omp","baseUrl":"https://gateway.example/v1","apiKey":"gateway-key"},{"provider":"bad","baseUrl":"gateway.example","apiKey":"x"}]}`), 0o600))
	a := NewAgent(testOptions(t, WithEnv(map[string]string{fakePiEnv: "1", "ACP_GO_PI_TEST_USAGE_ACCESS": access, fakePiEnvUsageGateways: gateways}))...)
	t.Cleanup(func() { require.NoError(t, a.Close()) })
	var asked []string
	a.usageTransport = usageTransportFunc(func(r *http.Request) (*http.Response, error) {
		asked = append(asked, r.URL.Host+r.URL.Path+" "+r.Header.Get("Authorization"))
		if r.URL.Host == "gateway.example" && r.URL.Path == "/v1/usage" && r.Header.Get("Authorization") == "Bearer gateway-key" {
			body := `{"generatedAt":1,"reports":[{"provider":"anthropic","fetchedAt":1789807237831,"limits":[{"id":"anthropic:5h","label":"Claude 5 Hour","window":{"id":"5h","durationMs":18000000,"resetsAt":1789817399682},"amount":{"usedFraction":0.25,"unit":"percent"},"status":"ok"}],"metadata":{}}]}`

			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		}

		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
	})
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	session, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	params, err := json.Marshal(map[string]any{usageSessionField: session.SessionId, "providerId": "anthropic"})
	require.NoError(t, err)
	response, err := a.accountUsage(t.Context(), params)
	require.NoError(t, err)
	require.True(t, response.Available)
	require.Equal(t, []wire.AccountUsageLimit{{ObservedAt: "2026-09-19T08:40:37Z", ID: "5h", Label: "Claude 5 Hour", WindowSeconds: 18000, UsedPercent: 25, UsageAllowed: new(true), ResetsAt: "2026-09-19T11:29:59Z"}}, response.Limits)
	require.Equal(t, []string{"proxy.example/v1/usage Bearer proxy-key", "gateway.example/v1/usage Bearer gateway-key"}, asked, "each valid route is asked in order until one covers the provider")

	params, err = json.Marshal(map[string]any{usageSessionField: session.SessionId, "providerId": "openrouter"})
	require.NoError(t, err)
	response, err = a.accountUsage(t.Context(), params)
	require.NoError(t, err)
	require.Equal(t, wire.AccountUsageUnavailable(wire.AccountUsageNotAuthenticated), response, "a provider no gateway reports keeps the native answer")
}

func TestAccountUsageRPCRefusals(t *testing.T) {
	h := newHarness(t)
	h.initialize()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		params map[string]any
		field  string
	}{
		{"missing session", map[string]any{"providerId": "openrouter"}, usageSessionField},
		{"missing provider", map[string]any{usageSessionField: session.SessionId}, "providerId"},
		{"unsupported provider", map[string]any{usageSessionField: session.SessionId, "providerId": "other"}, "providerId"},
		{"unknown session", map[string]any{usageSessionField: "missing", "providerId": "openrouter"}, usageSessionField},
		{"unknown field", map[string]any{usageSessionField: session.SessionId, "providerId": "openrouter", "accountId": "x"}, "accountId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, readErr := h.conn.CallExtension(h.ctx(), AccountUsageMethod, tc.params)
			require.Equal(t, tc.field, requestErrorData(t, readErr)["field"])
		})
	}
	raw, err := h.conn.CallExtension(h.ctx(), AccountUsageMethod, map[string]any{usageSessionField: session.SessionId, "providerId": "openrouter"})
	require.NoError(t, err)
	require.JSONEq(t, `{"available":false,"reason":"not_authenticated"}`, string(raw))
}
