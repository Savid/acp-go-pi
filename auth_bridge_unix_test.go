//go:build !windows

// These cases drive the native auth bridge through an oauth mint, which is
// refused where no browser shim can shadow the launchers a native login opens.
// Windows refuses it by design, so the bridge traffic below never happens
// there; TestAuthorizeRefusesOAuthWithoutABrowserShim pins what does.

package piacp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

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
