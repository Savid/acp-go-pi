package pi

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestUsageEndpointReadiness(t *testing.T) {
	t.Parallel()
	for _, address := range []string{"http://127.0.0.1:43210", "http://127.0.0.1:0", "http://example.com:1234", "http://127.0.0.1:1234/access", "http://127.0.0.1:1234?secret=x"} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()
			endpoint, err := NewUsageEndpoint(filepath.Join(t.TempDir(), "usage"))
			require.NoError(t, err)
			defer endpoint.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			ready := make(chan error, 1)
			go func() { ready <- endpoint.Wait(ctx, nil) }()
			require.NoError(t, os.WriteFile(endpoint.File+".tmp", []byte(address), 0o600))
			select {
			case readyErr := <-ready:
				t.Fatalf("readiness accepted an unpublished address: %v", readyErr)
			default:
			}
			require.NoError(t, os.Rename(endpoint.File+".tmp", endpoint.File))
			err = <-ready
			if address == "http://127.0.0.1:43210" {
				require.NoError(t, err)
				require.Equal(t, address, endpoint.URL)
			} else {
				require.Error(t, err)
				require.Empty(t, endpoint.URL)
			}
		})
	}
}

func TestUsageEndpointWaitEndsWithoutAnnouncement(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"cancelled", "process exited"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			endpoint, err := NewUsageEndpoint(filepath.Join(t.TempDir(), "usage"))
			require.NoError(t, err)
			defer endpoint.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			exited := make(chan struct{})
			if state == "cancelled" {
				cancel()
			} else {
				close(exited)
			}
			err = endpoint.Wait(ctx, exited)
			require.Error(t, err)
			if state == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, ctx.Err(), "process exit must end readiness before the caller expires")
			}
			require.Empty(t, endpoint.URL)
			require.NoError(t, endpoint.Close())
			require.NoDirExists(t, endpoint.dir)
		})
	}
}

func TestUsageAccessVerifiesNativeRoutes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, provider, api, endpoint, account string
		headers                                map[string]string
		custom                                 bool
		reason                                 string
	}{
		{name: "OpenRouter", provider: "openrouter", api: usageAPICompletions, endpoint: "https://openrouter.ai/api/v1"},
		{name: "OpenRouter Anthropic", provider: "openrouter", api: usageAPIAnthropic, endpoint: "https://openrouter.ai/api"},
		{name: "Go responses", provider: "opencode-go", api: "openai-responses", endpoint: "https://opencode.ai/zen/go/v1"},
		{name: "Go Anthropic", provider: "opencode-go", api: usageAPIAnthropic, endpoint: "https://opencode.ai/zen/go"},
		{name: "Codex subscription", provider: "openai-codex", api: "openai-codex-responses", endpoint: "https://chatgpt.com/backend-api", account: "account-1", headers: map[string]string{"ChatGPT-Account-ID": "account-1"}},
		{name: "different Codex account", provider: "openai-codex", api: "openai-codex-responses", endpoint: "https://chatgpt.com/backend-api", account: "account-1", headers: map[string]string{"ChatGPT-Account-ID": "account-2"}, reason: wire.AccountUsageNotReported},
		{name: "Claude subscription", provider: "anthropic", api: usageAPIAnthropic, endpoint: "https://api.anthropic.com", headers: map[string]string{"anthropic-beta": "oauth-2025-04-20"}},
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
				_ = json.NewEncoder(w).Encode(map[string]any{"configured": true, "custom": tc.custom, "routes": []usageRoute{{API: tc.api, BaseURL: tc.endpoint, APIKey: "native-key", AccountID: tc.account, Headers: tc.headers}}})
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

// The installed Node runs the embedded source with a minimal native registry;
// no pi installation or provider request is needed.
func TestUsageExtensionOwnsEndpoint(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node unavailable")
	}
	probe, err := exec.CommandContext(t.Context(), node, "-p", "Boolean(process.features.typescript)").Output()
	if err != nil || strings.TrimSpace(string(probe)) != "true" {
		t.Skip("Node TypeScript support unavailable")
	}
	var previous string
	for range 2 {
		dir := t.TempDir()
		endpoint, err := NewUsageEndpoint(filepath.Join(dir, "usage"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = endpoint.Close() })
		info, err := os.Stat(endpoint.dir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
		require.NoError(t, os.WriteFile(filepath.Join(dir, UsageExtensionFileName), usageExtensionSource, 0o600))
		runner := filepath.Join(dir, "runner.mjs")
		require.NoError(t, os.WriteFile(runner, []byte(usageExtensionRunner), 0o600))
		env, err := (process.Environment{Owned: map[string]string{EnvUsageFile: endpoint.File, EnvUsageToken: endpoint.Token}}).Build()
		require.NoError(t, err)
		proc, err := process.Start(t.Context(), process.Request{Executable: node, Args: []string{runner}, Env: env})
		require.NoError(t, err)
		t.Cleanup(func() { _ = proc.Kill(); _ = proc.Close() })
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		require.NoError(t, endpoint.Wait(ctx, proc.Done()), proc.StderrLastLine())
		require.NotEqual(t, previous, endpoint.URL, "live extensions must own distinct ports")
		previous = endpoint.URL
		listener, err := net.Listen("tcp", strings.TrimPrefix(endpoint.URL, "http://"))
		if listener != nil {
			_ = listener.Close()
		}
		require.Error(t, err, "announced port must already belong to the extension")
		info, err = os.Stat(endpoint.File)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		access, err := endpoint.Access(ctx, "openrouter", "test-model")
		require.NoError(t, err)
		require.Equal(t, "native-key", access.APIKey)
		refused := endpoint
		refused.Token = "invalid"
		_, err = refused.Access(ctx, "openrouter", "test-model")
		require.Error(t, err)
		// Keep both endpoints live until their individual cleanup callbacks run.
		t.Cleanup(func() {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelShutdown()
			_, writeErr := io.WriteString(proc.Stdin(), "shutdown\n")
			require.NoError(t, writeErr)
			_, waitErr := proc.Wait(shutdownCtx)
			require.NoError(t, waitErr, proc.StderrLastLine())
			require.NoFileExists(t, endpoint.File)
			stdout, readErr := io.ReadAll(proc.Stdout())
			require.NoError(t, readErr)
			require.Empty(t, stdout, "endpoint discovery must not write to the native protocol stream")
			require.NoError(t, endpoint.Close())
			require.NoDirExists(t, endpoint.dir)
		})
	}
}

const usageExtensionRunner = `
import extension from "./acp-usage.ts";
const callbacks = new Map();
extension({ on: (name, callback) => callbacks.set(name, callback) });
process.stdin.resume();
await callbacks.get("session_start")({}, { modelRegistry: {
  getProviderAuthStatus: () => ({ configured: true }),
  getAll: () => [{ provider: "openrouter", id: "test-model", api: "openai-completions", baseUrl: "https://openrouter.ai/api/v1" }],
  getRegisteredProviderConfig: () => undefined,
  getRegisteredNativeProvider: () => undefined,
  getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "native-key" }),
  getProvider: () => undefined,
} });
for await (const chunk of process.stdin) {
  await callbacks.get("session_shutdown")();
  break;
}
`
