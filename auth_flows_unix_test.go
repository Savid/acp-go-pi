//go:build !windows

// The provider-auth login flows below all reach pi through an oauth mint, and
// that mint is refused outright where no browser shim can shadow the launchers
// a native login would open on the operator's desktop. Windows is such a
// platform by design, so none of these flows exists there to assert anything
// about; what it does instead is pinned by
// TestAuthorizeRefusesOAuthWithoutABrowserShim, which runs everywhere.

package piacp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// TestWaitFlowSettlesFromTheNativeLogin pins the completion path a wait flow
// has at all. pi exposes no poll route, so status reads nothing of its own: the
// login's own terminal answer is the only completion signal, and without
// something reading it the flow could only ever expire while the credential it
// earned sat resident in pi's durable agent directory.
func TestWaitFlowSettlesFromTheNativeLogin(t *testing.T) {
	harness := newAuthHarness(t)

	settled := make(chan pi.AuthMessage, 1)
	presentation := startDeviceCodeFlow(t, harness, settled)

	require.Equal(t, authInteractionWait, presentation.Interaction)
	require.Equal(t, authTestUserCode, presentation.UserCode)
	require.Equal(t, authTestVerify, presentation.URL)
	require.Equal(t, int64(5000), presentation.PollIntervalMs)
	require.Empty(t, presentation.CallbackInput)
	require.Equal(t, authStatePending, harness.statusOf(presentation.FlowID).State)

	settled <- pi.AuthMessage{OK: true, Expires: 1783945909169}

	require.Eventually(t, func() bool {
		return harness.statusOf(presentation.FlowID).State == authStateAuthenticated
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, authStatusResult{
		FlowID:    presentation.FlowID,
		State:     authStateAuthenticated,
		ExpiresAt: 1783945909169,
	}, harness.statusOf(presentation.FlowID))

	record, ok, err := harness.broker.ledger.read("anthropic")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, authLedgerConfirmed, record.State)
}

// TestWaitFlowReportsANativeRefusal pins the other half: a login that ends
// badly terminalizes the flow rather than leaving it pending until the safety
// deadline.
func TestWaitFlowReportsANativeRefusal(t *testing.T) {
	harness := newAuthHarness(t)

	settled := make(chan pi.AuthMessage, 1)
	presentation := startDeviceCodeFlow(t, harness, settled)

	settled <- pi.AuthMessage{Cause: authCauseProviderRefused}

	require.Eventually(t, func() bool {
		return harness.statusOf(presentation.FlowID).State == authStateFailed
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, authReasonProviderRefused, harness.statusOf(presentation.FlowID).Reason)
}

// TestTerminalFlowAbortsTheNativeLogin pins the one thing that stops a device
// poll. pi's device logins park no prompt, so dismissing a prompt aborts
// nothing: a cancelled flow left its login polling a live user code, and an
// approval landing after the flow ended would write a credential into pi's
// durable agent directory under a ledger entry no leg will ever confirm.
func TestTerminalFlowAbortsTheNativeLogin(t *testing.T) {
	harness := newAuthHarness(t)

	settled := make(chan pi.AuthMessage)
	presentation := startDeviceCodeFlow(t, harness, settled)

	flow := harness.broker.byID[presentation.FlowID]

	harness.broker.mu.Lock()
	watch := flow.abortDialog
	harness.broker.mu.Unlock()
	require.NotEmpty(t, watch)

	_, err := harness.call(t.Context(), AuthCancelMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     presentation.FlowID,
	})
	require.NoError(t, err)

	answer := harness.awaitAnswer(watch)
	require.NotNil(t, answer.Value)
	require.Equal(t, pi.AuthAck, *answer.Value)
	require.False(t, answer.Cancelled)

	status := harness.statusOf(presentation.FlowID)
	require.Equal(t, authStateCancelled, status.State)
	require.Equal(t, authReasonOwnerCancel, status.Reason)
}

// TestCancelStopsWaitCompletionBeforeTheNativeResult pins the completion
// waiter's terminal-flow exit. The native abort and its terminal result are
// separate messages: cancellation must release the waiter from the flow's own
// terminal signal without waiting for that result, and a result arriving later
// must not replace the outcome the owner already chose.
func TestCancelStopsWaitCompletionBeforeTheNativeResult(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	aborted := make(chan struct{})
	releaseResult := make(chan struct{}, 1)
	resultDelivered := make(chan struct{})

	t.Cleanup(func() {
		select {
		case releaseResult <- struct{}{}:
		default:
		}
	})

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

		<-harness.answered(watch)
		close(aborted)
		<-releaseResult

		harness.deliver(ctx, pi.AuthMessage{
			ID:    request.ID,
			Kind:  pi.AuthKindResult,
			Cause: authCauseProviderRefused,
		})
		close(resultDelivered)

		return nil
	})

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	presentation := authorizeResult(t, authorized)
	require.Equal(t, authInteractionWait, presentation.Interaction)

	flow := harness.broker.byID[presentation.FlowID]
	claimed := func() bool {
		harness.broker.mu.Lock()
		defer harness.broker.mu.Unlock()

		return flow.claimed
	}
	require.Eventually(t, claimed, 5*time.Second, 5*time.Millisecond)

	_, err = harness.call(t.Context(), AuthCancelMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldFlowID:     presentation.FlowID,
	})
	require.NoError(t, err)

	select {
	case <-aborted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "native login was not aborted")
	}

	require.Eventually(t, func() bool { return !claimed() }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, authStatusResult{
		FlowID: presentation.FlowID,
		State:  authStateCancelled,
		Reason: authReasonOwnerCancel,
	}, harness.statusOf(presentation.FlowID))

	releaseResult <- struct{}{}

	select {
	case <-resultDelivered:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "native result was not delivered")
	}

	require.Equal(t, authStatusResult{
		FlowID: presentation.FlowID,
		State:  authStateCancelled,
		Reason: authReasonOwnerCancel,
	}, harness.statusOf(presentation.FlowID))
}

// TestAbortWatchArrivingAfterTheFlowEndedIsAnsweredAtOnce pins the race the
// abort handle has with a mint that already failed: a watch registered against
// a flow nobody can still terminalize would leave the login running with
// nothing left to stop it.
func TestAbortWatchArrivingAfterTheFlowEndedIsAnsweredAtOnce(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})
	flow := harness.broker.byID[flowID]

	harness.broker.terminalize(flow, authStateCancelled, authReasonOwnerCancel, 0)

	exchange := harness.broker.registerExchange("ex-abort", flow)
	require.NotNil(t, exchange)
	bindTestAuthExchange(t, harness, exchange)

	dialog := harness.deliver(t.Context(), pi.AuthMessage{ID: "ex-abort", Kind: pi.AuthKindCancel})
	answer := harness.awaitAnswer(dialog)
	require.NotNil(t, answer.Value)
	require.Equal(t, pi.AuthAck, *answer.Value)
}

// TestTerminalizeKeepsTheFirstTerminalTransition pins the record itself: a flow
// has one terminal transition, and a later one is dropped rather than
// overwriting the owner's.
func TestTerminalizeKeepsTheFirstTerminalTransition(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})
	flow := harness.broker.byID[flowID]

	harness.broker.terminalize(flow, authStateCancelled, authReasonOwnerCancel, 0)
	harness.broker.terminalize(flow, authStateFailed, authReasonTransport, 0)

	status := harness.statusOf(flowID)
	require.Equal(t, authStateCancelled, status.State)
	require.Equal(t, authReasonOwnerCancel, status.Reason)
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

	select {
	case <-minting:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the native mint never started")
	}

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

	harness.broker.closeSession(t.Context(), harness.session)
	require.Equal(t, authStateCancelled, harness.broker.flowState(flow))
	require.Equal(t, authReasonSessionClosed, flow.reason)

	// A second pass finds nothing and a foreign session id touches nothing.
	harness.broker.closeSession(t.Context(), harness.session)
	harness.broker.closeSession(t.Context(), &agentSession{id: "other"})
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

	harness.broker.closeSession(t.Context(), harness.session)

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

	harness.broker.closeSession(t.Context(), harness.session)
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

	harness.broker.closeSession(t.Context(), &agentSession{id: "someone-else"})
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
func TestConnectionIDIsRefusedAtEverySurfaceEntry(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())
	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	_, err := harness.call(t.Context(), AuthAuthorizeMethod,
		authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth))
	require.NoError(t, err)

	for name, connectionID := range adversarialConnectionIDs() {
		authorize := authorizeParams(harness, "anthropic", generation, authMethodTypeOAuth)
		authorize[authFieldConnectionID] = connectionID
		authorize[authFieldAuthorizeRequestID] = "req-" + name

		_, authorizeErr := harness.call(t.Context(), AuthAuthorizeMethod, authorize)
		requireInvalidAuthField(t, authorizeErr, authFieldConnectionID, name)

		_, disconnectErr := harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
			authFieldSessionID:         string(harness.session.id),
			authFieldProviderID:        "anthropic",
			authFieldConnectionID:      connectionID,
			authFieldBindingGeneration: 1,
		})
		requireInvalidAuthField(t, disconnectErr, authFieldConnectionID, name)
	}

	// Every refusal landed before the leg superseded the live flow or read the
	// entry the live binding names, so nothing recorded a value the bound
	// rejects.
	live, ok, readErr := harness.broker.ledger.read("anthropic")
	require.NoError(t, readErr)
	require.True(t, ok)
	require.Equal(t, "conn-1", live.ConnectionID)
	require.Equal(t, int64(1), live.BindingGeneration)
}
