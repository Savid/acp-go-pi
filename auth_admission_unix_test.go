//go:build !windows

// Connection admission is reached here through an oauth mint, which is refused
// where no browser shim can shadow the launchers a native login opens. Windows
// refuses it by design, so these admissions never occur there;
// TestAuthorizeRefusesOAuthWithoutABrowserShim pins what does.

package piacp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// TestRetiredAuthorizeRequestIDCannotCancelItsSuccessor pins the tombstone a
// supersede leaves. Only the newest record is replayable, so a delayed
// transport retry of the request that was replaced names nothing this broker
// can answer — and reading it as a fresh request would cancel the flow whose
// code the owner is looking at and start a third login.
func TestRetiredAuthorizeRequestIDCannotCancelItsSuccessor(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	var (
		mu     sync.Mutex
		logins int
	)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		if request.Op != pi.AuthOpLogin {
			return nil
		}

		mu.Lock()
		logins++
		mu.Unlock()

		harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindEvent,
			Event: &pi.AuthNativeEvent{Type: authNativeEventAuthURL, URL: authTestURL},
		})
		harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindPrompt,
			Prompt:  pi.AuthPromptManualCode,
			Message: "Paste the authorization code here",
		})

		<-ctx.Done()

		return nil
	})

	retried := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)

	_, err := harness.call(t.Context(), AuthAuthorizeMethod, retried)
	require.NoError(t, err)

	successor := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
	successor[authFieldAuthorizeRequestID] = "req-2"

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, successor)
	require.NoError(t, err)

	current := authorizeResult(t, authorized).FlowID

	_, err = harness.call(t.Context(), AuthAuthorizeMethod, retried)
	requireInvalidAuthField(t, err, authFieldAuthorizeRequestID)

	require.Equal(t, authStatePending, harness.broker.flowState(harness.broker.byID[current]))

	mu.Lock()
	require.Equal(t, 2, logins, "a retired request starts no login of its own")
	mu.Unlock()

	// A retired key is answerable for exactly as long as its session is.
	harness.broker.closeSession(t.Context(), harness.session)

	harness.broker.mu.Lock()
	defer harness.broker.mu.Unlock()
	require.Empty(t, harness.broker.retired)
}

// TestWaitCompletionReportsAnUnreachableSlot pins the answer a device login
// gets when the provider's slot never frees: pi wrote the credential, this
// wrapper could not record what it is bound to, and acceptance_unknown is the
// only honest report.
func TestWaitCompletionReportsAnUnreachableSlot(t *testing.T) {
	harness := newAuthHarness(t)

	settled := make(chan pi.AuthMessage, 1)
	presentation := startDeviceCodeFlow(t, harness, settled)

	release, err := harness.broker.admitSlot(t.Context(), "anthropic", "", "")
	require.NoError(t, err)

	shortenAuthNativeCallTimeout(t)

	settled <- pi.AuthMessage{OK: true}

	require.Eventually(t, func() bool {
		return harness.statusOf(presentation.FlowID).State == authStateFailed
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, authReasonAcceptanceUnknown, harness.statusOf(presentation.FlowID).Reason)

	release()
}

// TestOAuthCompletionAdmissionEndsWithTheCallersContext pins the same for the
// paste-back arm, whose answer to the parked prompt is what lets pi finish the
// token exchange and write the slot.
func TestOAuthCompletionAdmissionEndsWithTheCallersContext(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	release, err := harness.broker.admitSlot(t.Context(), "anthropic", "", "")
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = harness.call(ctx, AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestOAuthCallbackRefusesToInstallOverADisconnectedSlot pins the paste-back
// arm of the same lineage check. The code is what lets pi finish the token
// exchange and write the slot, so a flow whose generation the owner has already
// disconnected never gets to hand it over: the parked prompt is left exactly as
// it was and the native login is told nothing.
func TestOAuthCallbackRefusesToInstallOverADisconnectedSlot(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpRemove:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindProbe, Entries: map[string]string{}})
		}

		return nil
	})

	_, err := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	require.NoError(t, err)

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	requireAuthFailed(t, err, authCauseBindingConflict)

	harness.broker.mu.Lock()
	defer harness.broker.mu.Unlock()
	require.NotEmpty(t, harness.broker.byID[flowID].parkedDialog, "the code was never handed to the native login")
}
