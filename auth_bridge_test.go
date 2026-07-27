package piacp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// TestAuthDialogCancelsUnrecognizedTitles pins that a dialog this adapter did
// not start is dismissed rather than answered: the bridge command is reachable
// as ordinary prompt text, so an unowned exchange must do nothing.
func TestAuthDialogCancelsUnrecognizedTitles(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	for _, title := range []string{
		pi.AuthTitleMarker + "not json",
		pi.AuthTitleMarker + `{"kind":"result"}`,
		pi.AuthTitleMarker + `{"id":"x"}`,
		pi.AuthTitleMarker + `{"id":"unknown-exchange","kind":"result"}`,
	} {
		harness.broker.handleAuthDialog(t.Context(), harness.session, pi.UIRequest{
			ID:     "dialog-x",
			Method: uiMethodSelect,
			Title:  title,
		})

		require.True(t, harness.lastResponse().Cancelled, title)
	}
}

func TestAuthDialogCancelsUnknownKind(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	exchange := harness.broker.registerExchange("ex-1", nil)
	require.NotNil(t, exchange)

	harness.deliver(t.Context(), pi.AuthMessage{ID: "ex-1", Kind: "invented"})
	require.True(t, harness.lastResponse().Cancelled)
}

// TestAuthDialogWithoutFlowAcknowledges pins that a flow-shaped message on a
// plain command exchange is acknowledged rather than left hanging.
func TestAuthDialogWithoutFlowAcknowledges(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	harness.broker.registerExchange("ex-1", nil)

	harness.deliver(t.Context(), pi.AuthMessage{
		ID:    "ex-1",
		Kind:  pi.AuthKindEvent,
		Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: authTestURL},
	})
	require.NotNil(t, harness.lastResponse().Value)

	harness.deliver(t.Context(), pi.AuthMessage{ID: "ex-1", Kind: pi.AuthKindPrompt, Prompt: pi.AuthPromptText})
	require.True(t, harness.lastResponse().Cancelled)
}

// TestAuthEventWithoutPayloadAcknowledges pins that an event carrying nothing is
// acknowledged and folds nothing in.
func TestAuthEventWithoutPayloadAcknowledges(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})
	flow := harness.broker.byID[flowID]

	exchange := harness.broker.lookupExchange(flowID)
	require.NotNil(t, exchange)

	harness.deliver(t.Context(), pi.AuthMessage{ID: flowID, Kind: pi.AuthKindEvent})
	require.NotNil(t, harness.lastResponse().Value)
	require.Empty(t, harness.broker.flowVeto(flow))
}

// TestAuthPromptVetoesUnansweredPrompt pins that pi's enumeration declares no
// prompts, so an unexpected native question fails the flow closed rather than
// being answered with a value nobody authorized.
func TestAuthPromptVetoesUnansweredPrompt(t *testing.T) {
	t.Parallel()

	for _, prompt := range []string{pi.AuthPromptText, pi.AuthPromptSecret, pi.AuthPromptSelect, "invented"} {
		t.Run(prompt, func(t *testing.T) {
			t.Parallel()

			harness := newAuthHarness(t)
			generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

			harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
				harness.deliver(ctx, pi.AuthMessage{
					ID:      request.ID,
					Kind:    pi.AuthKindPrompt,
					Prompt:  prompt,
					Message: "GitHub Enterprise URL/domain",
				})

				return nil
			})

			_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
			requireAuthFailed(t, err, authCauseNativeVeto)
		})
	}
}

// TestAuthSecretAnsweredOnlyOnce pins that the submitted credential answers one
// prompt: a second question is one this adapter cannot answer.
func TestAuthSecretAnsweredOnlyOnce(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	require.NoError(t, err)

	flowID := authorizeResult(t, authorized).FlowID

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		first := harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindPrompt, Prompt: pi.AuthPromptSecret})
		require.NotNil(t, harness.awaitAnswer(first).Value)

		second := harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindPrompt, Prompt: pi.AuthPromptSecret})
		require.True(t, harness.awaitAnswer(second).Cancelled)

		harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})

		return nil
	})

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "openai",
		authFieldMethod:     authMethodTypeAPI,
		authFieldFlowID:     flowID,
		authFieldInput:      "sk-live-value",
	})
	requireAuthFailed(t, err, authCauseNativeVeto)
}

// TestCallbackWithoutParkedPromptIsFlowState pins that a code submitted to a
// flow with no prompt waiting is a flow-state failure.
func TestCallbackWithoutParkedPromptIsFlowState(t *testing.T) {
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
			},
		})

		<-ctx.Done()

		return nil
	})

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     authorizeResult(t, authorized).FlowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseFlowState)
}

// TestCancelDismissesParkedPrompt pins that a cancelled flow does not leave the
// extension blocked for the life of the process.
func TestCancelDismissesParkedPrompt(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	flow := harness.broker.byID[flowID]

	harness.broker.mu.Lock()
	parked := flow.parkedDialog
	harness.broker.mu.Unlock()
	require.NotEmpty(t, parked)

	_, err := harness.call(t.Context(), AuthCancelMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     flowID,
	})
	require.NoError(t, err)
	require.True(t, harness.awaitAnswer(parked).Cancelled)
}

// TestCancelParkedIgnoresVanishedSession pins that a flow whose session already
// went away dismisses nothing rather than panicking.
func TestCancelParkedIgnoresVanishedSession(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	flow := &authFlow{sessionID: "gone", parkedDialog: "dialog-x", ready: make(chan struct{})}
	harness.broker.cancelParked(t.Context(), flow)

	flow.parkedDialog = ""
	harness.broker.cancelParked(t.Context(), flow)
}

func TestAuthExchangeRequiresLiveClient(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	harness.session.mu.Lock()
	harness.session.client = nil
	harness.session.mu.Unlock()

	_, err := harness.broker.exchange(t.Context(), harness.session, authBridgeRequest{Op: authOpCatalog})
	require.ErrorIs(t, err, errAuthBridge)
}

func TestAuthExchangeReportsTokenFailure(t *testing.T) {
	harness := newAuthHarness(t)

	original := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	t.Cleanup(func() { authRandRead = original })

	_, err := harness.broker.exchange(t.Context(), harness.session, authBridgeRequest{Op: authOpCatalog})
	require.Error(t, err)
}

func TestAuthExchangeReportsCommandFailure(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error { return errors.New("child died") })

	_, err := harness.broker.exchange(t.Context(), harness.session, authBridgeRequest{Op: authOpCatalog})
	require.ErrorIs(t, err, errAuthBridge)
}

// TestAuthDeliverDropsSecondAnswer pins that a bridge answering twice cannot
// overwrite the answer a leg already took.
func TestAuthDeliverDropsSecondAnswer(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	exchange := harness.broker.registerExchange("ex-1", nil)

	harness.broker.deliver(exchange, pi.AuthMessage{ID: "ex-1", Kind: pi.AuthKindProbe})
	harness.broker.deliver(exchange, pi.AuthMessage{ID: "ex-1", Kind: pi.AuthKindCatalog})

	require.Equal(t, pi.AuthKindProbe, (<-exchange.answer).Kind)
}

func TestAuthDeliverResultDropsSecondAnswer(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flow := &authFlow{ready: make(chan struct{}), result: make(chan pi.AuthMessage, 1)}
	exchange := harness.broker.registerExchange("ex-1", flow)

	harness.broker.deliverResult(exchange, pi.AuthMessage{Cause: authCauseProcess})
	harness.broker.deliverResult(exchange, pi.AuthMessage{OK: true})

	require.Equal(t, authCauseProcess, (<-flow.result).Cause)
}

// TestAuthPumpRoutesDialogsOutsideAnyTurn pins the routing change flows depend
// on: an auth dialog reaches the broker with no live turn sink.
func TestAuthPumpRoutesDialogsOutsideAnyTurn(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	harness.broker.registerExchange("ex-1", nil)

	request := harness.dialogRequest(pi.AuthMessage{ID: "ex-1", Kind: pi.AuthKindProbe})

	require.Nil(t, harness.session.activeTurnSink())
	harness.session.dispatchUIRequest(t.Context(), request)

	require.Equal(t, pi.AuthAck, *harness.awaitAnswer(request.ID).Value)
}

// TestAuthPumpLeavesForeignDialogsToTheTurn pins that the routing change is
// scoped to the auth marker: an ordinary dialog outside a turn is still
// cancelled by the existing path.
func TestAuthPumpLeavesForeignDialogsToTheTurn(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	harness.session.dispatchUIRequest(t.Context(), pi.UIRequest{
		ID:     "dialog-foreign",
		Method: uiMethodSelect,
		Title:  "an ordinary question",
	})

	require.True(t, harness.lastResponse().Cancelled)
}

// TestAwaitPresentationHonoursCancellation pins that a caller walking away
// times the leg out rather than blocking on a login nobody is driving.
func TestAwaitPresentationHonoursCancellation(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flow := &authFlow{providerID: "anthropic", ready: make(chan struct{}), result: make(chan pi.AuthMessage, 1)}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	requireAuthFailed(t, harness.broker.awaitPresentation(ctx, flow), authCauseTimeout)

	_, err := harness.broker.awaitResult(ctx, flow)
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestAwaitPresentationHonoursDeadline pins the adapter's own bound on a native
// login that reports nothing at all.
func TestAwaitPresentationHonoursDeadline(t *testing.T) {
	originalCall := authNativeCallTimeoutValue
	originalLogin := authLoginTimeoutValue
	authNativeCallTimeoutValue = time.Millisecond
	authLoginTimeoutValue = time.Millisecond

	t.Cleanup(func() {
		authNativeCallTimeoutValue = originalCall
		authLoginTimeoutValue = originalLogin
	})

	harness := newAuthHarness(t)
	flow := &authFlow{providerID: "anthropic", ready: make(chan struct{}), result: make(chan pi.AuthMessage, 1)}

	requireAuthFailed(t, harness.broker.awaitPresentation(t.Context(), flow), authCauseTimeout)

	_, err := harness.broker.awaitResult(t.Context(), flow)
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestParkKeepsNativePresentationText pins that a prompt label never displaces
// the presentation text the harness already supplied.
func TestParkKeepsNativePresentationText(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	flow := &authFlow{ready: make(chan struct{}), presentMessage: "label"}
	harness.broker.park(flow, "dialog-1", "line\nbreak")
	require.Equal(t, "label", flow.presentMessage)

	native := &authFlow{ready: make(chan struct{}), presentMessage: "instructions", presentMessageNative: true}
	harness.broker.park(native, "dialog-2", "paste the code")
	require.Equal(t, "instructions", native.presentMessage)
}

// TestParkAdoptsPromptTextWhenNoneWasSupplied pins the fallback: a device-code
// flow supplies no instructions, so the prompt's own message is the
// presentation.
func TestParkAdoptsPromptTextWhenNoneWasSupplied(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	flow := &authFlow{ready: make(chan struct{}), presentMessage: "label"}
	harness.broker.park(flow, "dialog-1", "Paste the authorization code here")

	require.Equal(t, "Paste the authorization code here", flow.presentMessage)
	require.True(t, flow.presentMessageNative)
	require.Equal(t, authInteractionCallback, flow.presentInteraction)
}
