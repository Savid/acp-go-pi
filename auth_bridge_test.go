package piacp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
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

// TestAuthPromptVetoesUnansweredPrompt pins the prompts this adapter still has
// no answer for. A secret prompt with no submitted value and a prompt type this
// build cannot classify fail the flow closed rather than being answered with a
// value nobody authorized.
func TestAuthPromptVetoesUnansweredPrompt(t *testing.T) {
	t.Parallel()

	for _, prompt := range []string{pi.AuthPromptSecret, "invented"} {
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

// TestAuthPromptVetoesASelectWithNoHeadlessBranch pins the select this adapter
// still has no answer for: every branch of it completes on a socket the owner's
// browser cannot reach, so there is nothing to choose.
func TestAuthPromptVetoesASelectWithNoHeadlessBranch(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"no headless branch", "two headless branches"} {
		options := []string{"browser", "qr-code"}
		if name == "two headless branches" {
			options = []string{"device-code", "device_code"}
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newAuthHarness(t)
			generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

			harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
				harness.deliver(ctx, pi.AuthMessage{
					ID:      request.ID,
					Kind:    pi.AuthKindPrompt,
					Prompt:  pi.AuthPromptSelect,
					Message: "Select login method",
					Options: options,
				})

				return nil
			})

			_, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
			requireAuthFailed(t, err, authCauseNativeVeto)
		})
	}
}

// TestAuthPromptAnswersTheHeadlessBranchOfALoginVariantSelect pins the choice
// that reaches pi's device flows at all. openai-codex and radius each open with
// a select whose only brokerable branch is the device-code one; refusing the
// question took both providers off the surface entirely.
func TestAuthPromptAnswersTheHeadlessBranchOfALoginVariantSelect(t *testing.T) {
	t.Parallel()

	for _, option := range []string{"device_code", "device-code"} {
		t.Run(option, func(t *testing.T) {
			t.Parallel()

			harness := newAuthHarness(t)
			generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

			chosen := make(chan string, 1)

			harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
				if request.Op != pi.AuthOpLogin {
					return nil
				}

				dialog := harness.deliver(ctx, pi.AuthMessage{
					ID:      request.ID,
					Kind:    pi.AuthKindPrompt,
					Prompt:  pi.AuthPromptSelect,
					Message: "Select login method",
					Options: []string{"browser", option},
				})

				answer := harness.awaitAnswer(dialog)
				require.NotNil(t, answer.Value)
				chosen <- *answer.Value

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
			require.Equal(t, option, <-chosen)
			require.Equal(t, authInteractionWait, authorizeResult(t, authorized).Interaction)
		})
	}
}

// TestAuthPromptAnswersATextPromptWithNoValue pins the other branch pi's
// catalog cannot declare. github-copilot opens with an optional enterprise-host
// question whose blank answer selects the vendor's own host; refusing it took
// the provider off the surface, and answering it with anything else would send
// a customer-chosen host this adapter has no allowlist entry for.
func TestAuthPromptAnswersATextPromptWithNoValue(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	answered := make(chan string, 1)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		if request.Op != pi.AuthOpLogin {
			return nil
		}

		dialog := harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindPrompt,
			Prompt:  pi.AuthPromptText,
			Message: "GitHub Enterprise URL/domain (blank for github.com)",
		})

		answer := harness.awaitAnswer(dialog)
		require.NotNil(t, answer.Value)
		answered <- *answer.Value

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
	require.Empty(t, <-answered)
	require.Equal(t, authInteractionWait, authorizeResult(t, authorized).Interaction)
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

// TestReleaseNativeLoginIgnoresVanishedSession pins that a flow whose session
// already went away releases nothing rather than panicking.
func TestReleaseNativeLoginIgnoresVanishedSession(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	flow := &authFlow{sessionID: "gone", parkedDialog: "dialog-x", abortDialog: "dialog-y", decidable: make(chan struct{})}
	harness.broker.releaseNativeLogin(t.Context(), flow)

	flow.parkedDialog = ""
	flow.abortDialog = ""
	harness.broker.releaseNativeLogin(t.Context(), flow)
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

// TestAuthExchangeWaitsForADelayedAnswer pins that the answer is read under the
// call's own bound rather than sampled the instant the command is
// acknowledged: the extension answers on the pump, not on the leg's goroutine.
func TestAuthExchangeWaitsForADelayedAnswer(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	delivered := make(chan struct{})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		go func() {
			defer close(delivered)

			time.Sleep(20 * time.Millisecond)
			harness.deliver(context.WithoutCancel(ctx), pi.AuthMessage{
				ID:      request.ID,
				Kind:    pi.AuthKindProbe,
				Entries: map[string]string{"anthropic": "oauth"},
			})
		}()

		return nil
	})

	message, err := harness.broker.exchange(t.Context(), harness.session, authBridgeRequest{
		Op:          authOpProbe,
		ProviderIDs: []string{"anthropic"},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"anthropic": "oauth"}, message.Entries)

	<-delivered
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
	flow := &authFlow{decidable: make(chan struct{}), result: make(chan pi.AuthMessage, 1)}
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

	require.Nil(t, harness.session.activeTurnDelivery())
	harness.session.routeUIRequest(t.Context(), harness.session.outbox, request)

	require.Equal(t, pi.AuthAck, *harness.awaitAnswer(request.ID).Value)
}

// TestAuthPumpLeavesForeignDialogsToTheTurn pins that the routing change is
// scoped to the auth marker: an ordinary dialog outside a turn is still
// cancelled by the existing path.
func TestAuthPumpLeavesForeignDialogsToTheTurn(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	harness.session.routeUIRequest(t.Context(), harness.session.outbox, pi.UIRequest{
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
	flow := &authFlow{providerID: "anthropic", decidable: make(chan struct{}), result: make(chan pi.AuthMessage, 1)}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.Equal(t, authCauseTimeout, harness.broker.awaitPresentation(ctx, flow))

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
	flow := &authFlow{providerID: "anthropic", decidable: make(chan struct{}), result: make(chan pi.AuthMessage, 1)}

	require.Equal(t, authCauseTimeout, harness.broker.awaitPresentation(t.Context(), flow))

	_, err := harness.broker.awaitResult(t.Context(), flow)
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestParkKeepsNativePresentationText pins that a prompt label never displaces
// the presentation text the harness already supplied.
func TestParkKeepsNativePresentationText(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	flow := &authFlow{state: authStatePending, decidable: make(chan struct{}), presentMessage: "label"}
	harness.broker.park(flow, "dialog-1", "line\nbreak")
	require.Equal(t, "label", flow.presentMessage)

	native := &authFlow{state: authStatePending, decidable: make(chan struct{}), presentMessage: "instructions", presentMessageNative: true}
	harness.broker.park(native, "dialog-2", "paste the code")
	require.Equal(t, "instructions", native.presentMessage)
}

// TestParkAdoptsPromptTextWhenNoneWasSupplied pins the fallback: a device-code
// flow supplies no instructions, so the prompt's own message is the
// presentation.
func TestParkAdoptsPromptTextWhenNoneWasSupplied(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	flow := &authFlow{state: authStatePending, decidable: make(chan struct{}), presentMessage: "label"}
	harness.broker.park(flow, "dialog-1", "Paste the authorization code here")

	require.Equal(t, "Paste the authorization code here", flow.presentMessage)
	require.True(t, flow.presentMessageNative)
	require.Equal(t, authInteractionCallback, flow.presentInteraction)
}

// armPendingAuthFlow installs one pending provider-auth flow whose native login
// is still parked on a pi dialog, with the abort dialog the bridge holds open
// for the life of that login.
func armPendingAuthFlow(h *authHarness) *authFlow {
	flow := &authFlow{
		id:           "flow-1",
		session:      h.session,
		sessionID:    h.session.id,
		providerID:   "prov",
		state:        authStatePending,
		parkedDialog: "dialog-parked",
		abortDialog:  "dialog-abort",
		disarm:       make(chan struct{}),
		decidable:    make(chan struct{}),
		ready:        make(chan struct{}),
		result:       make(chan pi.AuthMessage, 1),
	}

	h.broker.mu.Lock()
	h.broker.flows[authFlowKey{sessionID: h.session.id, providerID: "prov"}] = flow
	h.broker.byID[flow.id] = flow
	h.broker.mu.Unlock()

	return flow
}

// dialogAnswers reports every native dialog the wrapper answered.
func dialogAnswers(h *authHarness) []string {
	h.client.mu.Lock()
	defer h.client.mu.Unlock()

	ids := make([]string, 0, len(h.client.responses))
	for _, response := range h.client.responses {
		ids = append(ids, response.ID)
	}

	return ids
}

// TestShutdownLadderStepFourReachesTheNativeCancelOnEveryTeardown pins step 4's
// "invoke native cancel where one exists" on all three boundaries the ladder
// applies to. The native cancel answers dialogs through the flow's own session
// rather than one resolved from the agent's active map: close, delete, and
// Agent.Close all take the id out of that map before step 4 runs, so a lookup
// would leave every parked login unanswered on exactly the paths that owe an
// answer.
func TestShutdownLadderStepFourReachesTheNativeCancelOnEveryTeardown(t *testing.T) {
	for _, test := range []struct {
		name     string
		boundary func(*authHarness) error
	}{
		{
			name: "the broker called directly",
			boundary: func(h *authHarness) error {
				h.broker.closeSession(context.Background(), h.session)

				return nil
			},
		},
		{
			name: "session/close",
			boundary: func(h *authHarness) error {
				_, err := h.agent.CloseSession(context.Background(),
					acp.CloseSessionRequest{SessionId: h.session.id})

				return err
			},
		},
		{
			name: "session/delete",
			boundary: func(h *authHarness) error {
				_, err := h.agent.UnstableDeleteSession(context.Background(),
					acp.UnstableDeleteSessionRequest{SessionId: h.session.id})

				return err
			},
		},
		{
			name:     "Agent.Close",
			boundary: func(h *authHarness) error { return h.agent.Close() },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newAuthHarness(t)
			flow := armPendingAuthFlow(h)

			require.NoError(t, test.boundary(h))

			h.broker.mu.Lock()
			state := flow.state
			reason := flow.reason
			h.broker.mu.Unlock()

			require.Equal(t, authStateCancelled, state, "the record terminalizes")
			require.Equal(t, authReasonSessionClosed, reason)
			require.Subset(t, dialogAnswers(h), []string{"dialog-parked", "dialog-abort"},
				"the parked native login and its abort dialog are both answered")
		})
	}
}

// TestShutdownLadderStepFourPrecedesTheNativeInterrupt pins step 4's fixed
// position. It runs after pending interactions are resolved and before the
// native interrupt on every boundary, so a flow is never abandoned to a process
// already being torn down. The abort observed here is the interrupt itself, and
// the session's auth admission is already closed when it arrives.
//
// Agent.Close is absent because it sends no native interrupt at all: embedded
// shutdown takes the graceful process exit directly, so there is no interrupt
// for step 4 to precede there. That it still reaches the native cancel is
// pinned above.
func TestShutdownLadderStepFourPrecedesTheNativeInterrupt(t *testing.T) {
	for _, test := range []struct {
		name     string
		boundary func(*authHarness) error
	}{
		{
			name: "session/close",
			boundary: func(h *authHarness) error {
				_, err := h.agent.CloseSession(context.Background(),
					acp.CloseSessionRequest{SessionId: h.session.id})

				return err
			},
		},
		{
			name: "session/delete",
			boundary: func(h *authHarness) error {
				_, err := h.agent.UnstableDeleteSession(context.Background(),
					acp.UnstableDeleteSessionRequest{SessionId: h.session.id})

				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newAuthHarness(t)
			armPendingAuthFlow(h)

			var (
				aborted       bool
				closedAtAbort bool
			)

			h.client.abortFunc = func(context.Context) error {
				aborted = true

				h.broker.mu.Lock()
				closedAtAbort = h.session.authClosed
				h.broker.mu.Unlock()

				return nil
			}

			require.NoError(t, test.boundary(h))
			require.True(t, aborted, "the boundary reaches the native interrupt")
			require.True(t, closedAtAbort,
				"provider-auth flows are cancelled before the native interrupt")
		})
	}
}
