package piacp

import (
	"context"
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

	"github.com/savid/acp-go-pi/internal/pi"
)

// Method entry types, mirroring pi's provider auth discriminator: a provider
// object carries at most one oauth and at most one api-key login.
const (
	authMethodTypeOAuth = "oauth"
	authMethodTypeAPI   = "api"
)

// Native presentation event types the flow legs read.
const (
	authNativeEventAuthURL    = "auth_url"
	authNativeEventDeviceCode = "device_code"
)

// Display-field bounds. A value violating its bound is dropped, never
// truncated.
const (
	authMaxURLBytes      = 2048
	authMaxMessageBytes  = 2048
	authMaxUserCodeBytes = 64
	authMaxLabelBytes    = 256
)

// Input bounds.
const (
	authMaxTextInputBytes = 1024
	authMaxInputsBytes    = 8192
	authMaxInputsKeys     = 16
)

// authShellCredentialPrefix marks a credential pi executes as a shell command,
// caching its stdout as the key. A brokered value that reached such a field
// would be a remote command channel into the worker, so it is refused.
const authShellCredentialPrefix = "!"

// authUserCodePattern is anchored: a substring match accepts a code with markup
// wrapped around it.
var authUserCodePattern = regexp.MustCompile(`\A[A-Za-z0-9-]+\z`)

// authCatalogMethod is one entry of the current catalog paired with the native
// addressing the adapter needs to drive it.
type authCatalogMethod struct {
	ID    string
	Type  string
	Label string
}

type authMethodEntry struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

type authMethodsResult struct {
	Providers  map[string][]authMethodEntry `json:"providers"`
	Generation string                       `json:"generation"`
}

// methods enumerates the catalog and mints the generation that names this exact
// result. The catalog is exactly what the adapter enumerates: pi's provider
// enumeration is executed rather than read off disk, because the two disagree.
func (p *providerAuth) methods(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	message, err := p.exchange(ctx, session, authBridgeRequest{Op: authOpCatalog})
	if err != nil {
		return nil, authFailed(authCauseTransport, "", "", "")
	}

	methods, entries := buildAuthCatalog(message.Providers)

	generation, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, "", "", "")
	}

	p.mu.Lock()
	p.generation = generation
	p.catalog = methods
	p.mu.Unlock()

	return authMethodsResult{Providers: entries, Generation: generation}, nil
}

// buildAuthCatalog converts the executed native enumeration. A provider whose
// label violates its bound is omitted and the leg still succeeds: one bad entry
// in a large provider catalog must not take the whole surface down.
func buildAuthCatalog(providers []pi.AuthProvider) (map[string][]authCatalogMethod, map[string][]authMethodEntry) {
	methods := make(map[string][]authCatalogMethod, len(providers))
	entries := make(map[string][]authMethodEntry, len(providers))

	ids := make([]string, 0, len(providers))
	byID := make(map[string]pi.AuthProvider, len(providers))

	for _, provider := range providers {
		if provider.ID == "" {
			continue
		}

		if _, duplicate := byID[provider.ID]; duplicate {
			continue
		}

		byID[provider.ID] = provider
		ids = append(ids, provider.ID)
	}

	sort.Strings(ids)

	for _, id := range ids {
		resolved, published := buildProviderMethods(byID[id])
		if len(resolved) == 0 {
			continue
		}

		methods[id] = resolved
		entries[id] = published
	}

	return methods, entries
}

func buildProviderMethods(provider pi.AuthProvider) ([]authCatalogMethod, []authMethodEntry) {
	resolved := make([]authCatalogMethod, 0, 2)
	published := make([]authMethodEntry, 0, 2)

	appendMethod := func(id string, kind string, label string) {
		bounded, ok := authDisplayText(label, authMaxLabelBytes)
		if !ok {
			return
		}

		resolved = append(resolved, authCatalogMethod{ID: id, Type: kind, Label: bounded})
		published = append(published, authMethodEntry{ID: id, Type: kind, Label: bounded})
	}

	if provider.OAuth != nil {
		label := provider.OAuth.LoginLabel
		if label == "" {
			label = provider.OAuth.Name
		}

		appendMethod(authMethodTypeOAuth, authMethodTypeOAuth, label)
	}

	if provider.API != nil {
		appendMethod(authMethodTypeAPI, authMethodTypeAPI, provider.API.Name)
	}

	return resolved, published
}

// authDisplayText normalises a native presentation string to NFC and measures
// its bounds and categories on that normalised form, which is also the form the
// adapter relays, persists, and returns. Normalising after measuring bounds a
// string nobody sends.
func authDisplayText(value string, maxBytes int) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > maxBytes || !utf8.ValidString(normalized) {
		return "", false
	}

	for _, r := range normalized {
		if !authDisplayRune(r) {
			return "", false
		}
	}

	return normalized, true
}

// authDisplayRune restricts free text to Unicode categories L, N, P, S, and Zs.
// Every C* category is rejected, which is also what excludes every
// bidirectional override and embedding character: a label is the provider name
// in the one place a human decides which account to bind.
func authDisplayRune(r rune) bool {
	switch {
	case unicode.IsLetter(r), unicode.IsNumber(r), unicode.IsPunct(r), unicode.IsSymbol(r):
		return true
	case unicode.Is(unicode.Zs, r):
		return true
	default:
		return false
	}
}

// authDisplayURL applies the url bound: at most 2048 bytes, scheme exactly
// https, no userinfo, no fragment.
func authDisplayURL(value string) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > authMaxURLBytes {
		return "", false
	}

	parsed, err := url.Parse(normalized)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Fragment != "" || parsed.Host == "" {
		return "", false
	}

	return normalized, true
}

// authDisplayUserCode applies the userCode bound with an anchored pattern.
func authDisplayUserCode(value string) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > authMaxUserCodeBytes || !authUserCodePattern.MatchString(normalized) {
		return "", false
	}

	return normalized, true
}

// authLoopbackHost reports whether a minted authorization URL completes on a
// loopback listener. Such a method is not brokered: the adapter cannot relay a
// URL whose completion lands on a socket the owner's browser cannot reach.
func authLoopbackHost(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	if isLoopbackHostname(parsed.Hostname()) {
		return true
	}

	redirect := parsed.Query().Get("redirect_uri")
	if redirect == "" {
		return false
	}

	target, err := url.Parse(redirect)
	if err != nil {
		return false
	}

	return isLoopbackHostname(target.Hostname())
}

func isLoopbackHostname(host string) bool {
	switch strings.ToLower(host) {
	case "127.0.0.1", "::1", "localhost":
		return true
	default:
		return strings.HasSuffix(strings.ToLower(host), ".localhost")
	}
}

// authHeadlessOptionID is the native id of the device-code branch of a login
// variant select, normalised: pi's catalog spells it `device_code` on one
// provider and `device-code` on another.
const authHeadlessOptionID = "device-code"

// authHeadlessOption picks the one branch of a native login-variant select this
// adapter can broker. Every other branch of those selects completes on a
// loopback listener the owner's browser cannot reach, which recordAuthURL
// refuses anyway, so this is not a preference the adapter invented: it is the
// only branch that survives the adapter's own veto. An option set with no such
// branch, or with more than one, is answered by nobody and vetoes the flow.
func authHeadlessOption(options []string) (string, bool) {
	chosen := ""

	for _, option := range options {
		if strings.ReplaceAll(strings.ToLower(option), "_", "-") != authHeadlessOptionID {
			continue
		}

		if chosen != "" {
			return "", false
		}

		chosen = option
	}

	return chosen, chosen != ""
}

// validateAuthInputs checks the values of an authorize request, not merely its
// key set. pi's provider enumeration exposes no prompt schema — a native login
// asks for whatever it asks for at flow time — so the catalog declares no
// prompts and an authorize carrying any input names a prompt nobody advertised.
func validateAuthInputs(inputs map[string]string) error {
	if len(inputs) > authMaxInputsKeys {
		return invalidAuthField(authFieldInputs)
	}

	total := 0
	for key, value := range inputs {
		total += len(key) + len(value)
	}

	if total > authMaxInputsBytes {
		return invalidAuthField(authFieldInputs)
	}

	if len(inputs) > 0 {
		return invalidAuthField(authFieldInputs)
	}

	return nil
}

// validateAuthSecret bounds one submitted credential value. A value pi would
// execute as a shell command is refused before it can reach a credential field.
func validateAuthSecret(value string) error {
	if value == "" || len(value) > authMaxTextInputBytes || !utf8.ValidString(value) {
		return invalidAuthField(authFieldInput)
	}

	if strings.HasPrefix(value, authShellCredentialPrefix) {
		return invalidAuthField(authFieldInput)
	}

	for _, r := range value {
		if r == '\n' || r == '\r' || unicode.IsControl(r) {
			return invalidAuthField(authFieldInput)
		}
	}

	return nil
}
