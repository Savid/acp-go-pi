package piacp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// authAdmissionQuiescence bounds the wait a test spends letting a leg the
// discipline holds back prove that it is held back. An admitted leg reaches its
// signal in microseconds, so this only ever elapses when the gate did its job.
const authAdmissionQuiescence = 200 * time.Millisecond

// authNativeSlots is a scripted stand-in for pi's credential store: a login
// installs, a removal clears, and a probe answers whatever is resident now. It
// is what makes "the ledger says removed while the credential is live" an
// assertion rather than an argument.
type authNativeSlots struct {
	mu        sync.Mutex
	resident  map[string]bool
	logins    int
	installed chan struct{}
}

func newAuthNativeSlots() *authNativeSlots {
	return &authNativeSlots{resident: make(map[string]bool), installed: make(chan struct{}, 4)}
}

func (s *authNativeSlots) install(providerID string) {
	s.mu.Lock()
	s.resident[providerID] = true
	s.mu.Unlock()

	s.installed <- struct{}{}
}

func (s *authNativeSlots) clear(providerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.resident, providerID)
}

func (s *authNativeSlots) isResident(providerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.resident[providerID]
}

func (s *authNativeSlots) entries() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make(map[string]string, len(s.resident))

	for providerID, present := range s.resident {
		if present {
			entries[providerID] = "api_key"
		}
	}

	return entries
}

func (s *authNativeSlots) loginCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.logins
}

// scriptSecretStore installs the whole native side of an api-key provider: the
// login that writes the slot, the removal that clears it, and the probe
// disconnect verifies absence with. hold, when non-nil, is what the login waits
// on before it writes, which is where a test parks one leg.
func scriptSecretStore(harness *authHarness, slots *authNativeSlots, hold func(int)) {
	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpLogin:
			slots.mu.Lock()
			slots.logins++
			attempt := slots.logins
			slots.mu.Unlock()

			if hold != nil {
				hold(attempt)
			}

			prompt := harness.deliver(ctx, pi.AuthMessage{
				ID:      request.ID,
				Kind:    pi.AuthKindPrompt,
				Prompt:  pi.AuthPromptSecret,
				Message: "OpenAI API key",
			})
			harness.awaitAnswer(prompt)
			slots.install(request.ProviderID)

			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true, CredType: "api_key"})
		case pi.AuthOpRemove:
			slots.clear(request.ProviderID)
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindProbe, Entries: slots.entries()})
		}

		return nil
	})
}

func startSecretFlow(t *testing.T, harness *authHarness) string {
	t.Helper()

	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	require.NoError(t, err)

	return authorizeResult(t, authorized).FlowID
}

func secretCallbackParams(harness *authHarness, flowID string) map[string]any {
	return map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "openai",
		authFieldMethod:     authMethodTypeAPI,
		authFieldFlowID:     flowID,
		authFieldInput:      "sk-live-value",
	}
}

func disconnectParams(harness *authHarness, generation int64) map[string]any {
	return map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "openai",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: generation,
	}
}

// awaitLeg takes the answer of a leg running off the test goroutine, failing
// rather than hanging when the leg never answers at all.
func awaitLeg(t *testing.T, legs <-chan error) error {
	t.Helper()

	select {
	case err := <-legs:
		return err
	case <-time.After(10 * time.Second):
		require.FailNow(t, "a provider-auth leg never answered")

		return nil
	}
}

func callLeg(harness *authHarness, method string, params map[string]any, legs chan<- error) {
	_, err := harness.call(context.WithoutCancel(harness.t.Context()), method, params)
	legs <- err
}

// TestConcurrentCallbacksClaimTheFlowOnce runs the two callbacks a host can
// have in flight for one flow — a retried submit is the ordinary shape — and
// pins that exactly one is admitted. The pending check and the native login it
// authorises used to sit either side of an unlocked window, so both legs read
// pending and both drove a login. Nothing there is a data race for -race to
// find: every field access is locked. It is a lost update, and in pi it
// corrupts the bridge, because both logins register their exchange under the
// same flow id and either one's release deletes the other's.
func TestConcurrentCallbacksClaimTheFlowOnce(t *testing.T) {
	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	slots := newAuthNativeSlots()
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	// Only the first login is held open. A second one is let run to completion
	// so the defect reports itself as two logins rather than as a hang.
	scriptSecretStore(harness, slots, func(attempt int) {
		started <- struct{}{}

		if attempt == 1 {
			<-release
		}
	})

	legs := make(chan error, 2)
	for range 2 {
		go callLeg(harness, AuthCallbackMethod, secretCallbackParams(harness, flowID), legs)
	}

	<-started

	// The refused leg answers at once; the claimant cannot answer before the
	// native login it holds does.
	requireAuthFailed(t, awaitLeg(t, legs), authCauseFlowState)

	// Neither leg's release deleted the other's registration: the exchange the
	// dialog router resolves is still the one the live login opened.
	require.NotNil(t, harness.broker.lookupExchange(flowID))

	close(release)
	require.NoError(t, awaitLeg(t, legs))
	require.Equal(t, 1, slots.loginCount())
}

// TestCallbackRefusesToInstallOverADisconnectedSlot pins the lineage check that
// has to happen before the native write rather than only after it. confirm's
// compare-and-set refuses the stale confirmation correctly, but a completion
// that drove the login first has already made pi write the credential: the
// ledger entry reads removed, inventory skips removed, and the credential is
// live and invisible on every host surface with nothing left to fence it.
func TestCallbackRefusesToInstallOverADisconnectedSlot(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	slots := newAuthNativeSlots()
	scriptSecretStore(harness, slots, nil)

	_, err := harness.call(t.Context(), AuthDisconnectMethod, disconnectParams(harness, 1))
	require.NoError(t, err)

	_, err = harness.call(t.Context(), AuthCallbackMethod, secretCallbackParams(harness, flowID))
	requireAuthFailed(t, err, authCauseBindingConflict)

	require.Zero(t, slots.loginCount(), "a completion whose lineage is gone drives no native login")
	require.False(t, slots.isResident("openai"))

	record, ok, err := harness.broker.ledger.read("openai")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, authLedgerRemoved, record.State)
	require.Equal(t, int64(2), record.BindingGeneration)
}

// TestDisconnectAndCompletionSerialiseOnTheProviderSlot runs the completion
// against the disconnect it is racing. Both rewrite the same native slot, and
// interleaved they lose each other's update: the disconnect verifies absence,
// the completion installs behind it, and the removed record is written over a
// credential that is now resident. The disconnect is parked immediately before
// that final record, which is the exact window the defect lives in.
func TestDisconnectAndCompletionSerialiseOnTheProviderSlot(t *testing.T) {
	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	slots := newAuthNativeSlots()
	scriptSecretStore(harness, slots, nil)

	recording := make(chan struct{}, 1)
	release := make(chan struct{})

	original := ledgerMarshal
	ledgerMarshal = func(value any) ([]byte, error) {
		if record, ok := value.(authLedgerRecord); ok && record.State == authLedgerRemoved {
			recording <- struct{}{}
			<-release
		}

		return original(value)
	}

	t.Cleanup(func() { ledgerMarshal = original })

	legs := make(chan error, 2)

	go callLeg(harness, AuthDisconnectMethod, disconnectParams(harness, 1), legs)

	<-recording

	go callLeg(harness, AuthCallbackMethod, secretCallbackParams(harness, flowID), legs)

	select {
	case <-slots.installed:
	case <-time.After(authAdmissionQuiescence):
	}

	close(release)

	require.NoError(t, awaitLeg(t, legs))
	requireAuthFailed(t, awaitLeg(t, legs), authCauseBindingConflict)

	record, ok, err := harness.broker.ledger.read("openai")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, authLedgerRemoved, record.State)
	require.False(t, slots.isResident("openai"), "a removed entry never names a resident credential")
}

// TestConcurrentIdenticalAuthorizesMintOneFlow pins what the idempotency key is
// for. Both requests resolve the retained record before either publishes its
// own, so both miss each other, both mint a flow, and both write a ledger
// intent — and in pi the loser's native login can start after the supersede
// that was meant to end it. The first request is parked inside its ledger
// intent write, which is the window the second used to walk through.
func TestConcurrentIdenticalAuthorizesMintOneFlow(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	minting := make(chan struct{}, 1)
	repeated := make(chan struct{}, 1)
	release := make(chan struct{})

	var (
		mu      sync.Mutex
		intents int
	)

	original := ledgerMarshal
	ledgerMarshal = func(value any) ([]byte, error) {
		record, ok := value.(authLedgerRecord)
		if ok && record.State == authLedgerIntent {
			mu.Lock()
			intents++
			first := intents == 1
			mu.Unlock()

			if first {
				minting <- struct{}{}
				<-release
			} else {
				repeated <- struct{}{}
			}
		}

		return original(value)
	}

	t.Cleanup(func() { ledgerMarshal = original })

	params := authorizeParams(harness, "openai", generation, authMethodTypeAPI)
	results := make(chan authorizeCall, 2)

	for range 2 {
		go func() { results <- callAuthorize(harness, params) }()
	}

	<-minting

	// A second mint can only happen while the first is parked here, so its
	// absence is the discipline holding and not a leg that was merely slow.
	select {
	case <-repeated:
	case <-time.After(authAdmissionQuiescence):
	}

	close(release)

	first := <-results
	second := <-results

	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, authorizeResult(t, first.result).FlowID, authorizeResult(t, second.result).FlowID)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, intents, "one authorize admission writes one intent")
}

// TestAuthorizeThatPublishesAfterCloseIsRefused pins the invariant session
// shutdown depends on: no flow escapes close's cleanup set. The authorize is
// parked between its session lookup and its publication — the window close's
// scan used to run through — and publication is what refuses it, because making
// close wait for a leg mid native call would block shutdown for as long as the
// harness cares to take.
func TestAuthorizeThatPublishesAfterCloseIsRefused(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	publishing := make(chan struct{}, 1)
	release := make(chan struct{})

	original := ledgerMarshal
	ledgerMarshal = func(value any) ([]byte, error) {
		if record, ok := value.(authLedgerRecord); ok && record.State == authLedgerIntent {
			publishing <- struct{}{}
			<-release
		}

		return original(value)
	}

	t.Cleanup(func() { ledgerMarshal = original })

	params := authorizeParams(harness, "openai", generation, authMethodTypeAPI)
	results := make(chan authorizeCall, 1)

	go func() { results <- callAuthorize(harness, params) }()

	<-publishing
	harness.broker.closeSession(t.Context(), harness.session)
	close(release)

	requireUnknownSession(t, (<-results).err)

	harness.broker.mu.Lock()
	require.Empty(t, harness.broker.flows)
	require.Empty(t, harness.broker.byID)
	require.Empty(t, harness.broker.retained)
	harness.broker.mu.Unlock()

	// An ordinary late leg is refused at the door rather than at publication.
	_, err := harness.call(t.Context(), AuthAuthorizeMethod, params)
	requireUnknownSession(t, err)
}

func requireUnknownSession(t *testing.T, err error) {
	t.Helper()

	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)

	data, ok := requestError.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "unknown session", data[jsonFieldError])
}

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

// TestAuthorizeAdmissionEndsWithTheCallersContext pins that a request waiting
// its turn on a key answers its own caller rather than waiting out somebody
// else's native call.
func TestAuthorizeAdmissionEndsWithTheCallersContext(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	key := authFlowKey{sessionID: harness.session.id, providerID: "openai"}

	release, err := harness.broker.admitAuthorize(t.Context(), key, authMethodTypeAPI)
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = harness.call(ctx, AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestDisconnectAdmissionEndsWithTheCallersContext pins the same for the
// provider's credential slot.
func TestDisconnectAdmissionEndsWithTheCallersContext(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	startSecretFlow(t, harness)

	release, err := harness.broker.admitSlot(t.Context(), "openai", "", "")
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = harness.call(ctx, AuthDisconnectMethod, disconnectParams(harness, 1))
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestWaitCompletionRefusesAFlowAnotherLegHolds pins that the harness-driven
// completion claims the flow on the same terms every other completion does: it
// settles the same record a callback would, off its own goroutine.
func TestWaitCompletionRefusesAFlowAnotherLegHolds(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flow := &authFlow{
		id:         "flow-claimed",
		sessionID:  harness.session.id,
		providerID: "anthropic",
		state:      authStatePending,
		claimed:    true,
		method:     authCatalogMethod{ID: authMethodTypeOAuth, Type: authMethodTypeOAuth},
		decidable:  make(chan struct{}),
		ready:      make(chan struct{}),
		result:     make(chan pi.AuthMessage, 1),
		disarm:     make(chan struct{}),
	}
	flow.result <- pi.AuthMessage{Kind: pi.AuthKindResult, OK: true}

	harness.broker.watchNativeCompletion(flow)

	require.Never(t, func() bool {
		return len(flow.result) == 0
	}, authAdmissionQuiescence, 10*time.Millisecond)
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

// TestTerminalFlowTakesNoParkedPrompt pins the other half of a flow that closed
// while its login ran: the release that dismisses the flow's prompts has
// already happened, so a prompt parked now would stay open for the life of the
// process with nothing left to answer it.
func TestTerminalFlowTakesNoParkedPrompt(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flow := &authFlow{
		id:        "flow-terminal",
		sessionID: harness.session.id,
		state:     authStateCancelled,
		decidable: make(chan struct{}),
	}

	harness.broker.registerExchange("ex-park", flow)

	dialog := harness.deliver(t.Context(), pi.AuthMessage{
		ID:      "ex-park",
		Kind:    pi.AuthKindPrompt,
		Prompt:  pi.AuthPromptManualCode,
		Message: "Paste the authorization code here",
	})

	require.True(t, harness.awaitAnswer(dialog).Cancelled)

	harness.broker.mu.Lock()
	defer harness.broker.mu.Unlock()
	require.Empty(t, flow.parkedDialog)
}

// TestCompletionReportsAnUnreadableLineage pins that a completion which cannot
// read what its provider's entry names refuses rather than driving a native
// write it could never account for.
func TestCompletionReportsAnUnreadableLineage(t *testing.T) {
	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	slots := newAuthNativeSlots()
	scriptSecretStore(harness, slots, nil)

	original := ledgerReadFile
	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("unreadable") }

	t.Cleanup(func() { ledgerReadFile = original })

	_, err := harness.call(t.Context(), AuthCallbackMethod, secretCallbackParams(harness, flowID))
	requireAuthFailed(t, err, authCauseProcess)

	require.Zero(t, slots.loginCount())
}

// TestCompletionAdmissionEndsWithTheCallersContext pins that a completion
// queued behind a disconnect on the same provider answers its own caller rather
// than waiting out somebody else's native call.
func TestCompletionAdmissionEndsWithTheCallersContext(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	release, err := harness.broker.admitSlot(t.Context(), "openai", "", "")
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = harness.call(ctx, AuthCallbackMethod, secretCallbackParams(harness, flowID))
	requireAuthFailed(t, err, authCauseTimeout)
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

// TestOAuthCompletionWithoutAParkedPromptIsFlowState pins the answer a code
// submitted into a flow the harness never asked one for gets: the leg holds the
// provider's slot and its lineage is current, and there is still nothing this
// value can be handed to.
func TestOAuthCompletionWithoutAParkedPromptIsFlowState(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flow := &authFlow{
		id:           "flow-unparked",
		sessionID:    harness.session.id,
		providerID:   "anthropic",
		connectionID: "conn-1",
		state:        authStatePending,
		method:       authCatalogMethod{ID: authMethodTypeOAuth, Type: authMethodTypeOAuth},
		decidable:    make(chan struct{}),
		ready:        make(chan struct{}),
		result:       make(chan pi.AuthMessage, 1),
		disarm:       make(chan struct{}),
	}

	_, err := harness.broker.completeOAuth(t.Context(), harness.session, flow, "code-1")
	requireAuthFailed(t, err, authCauseFlowState)
}

// TestAuthorizeIntentSurvivesAConcurrentDisconnect pins the last read-then-write
// over the provider's entry that was still ungated: authorize carries the
// provider's revision and binding generation forward, which is a read and a
// write it decides. A disconnect's sequence is a read, a bump, two native calls
// and a second write, so the two interleave and one loses the other's update.
// The disconnect is parked at its native removal, which is the middle of its own
// sequence and the one point in it that is not inside the ledger's own lock.
func TestAuthorizeIntentSurvivesAConcurrentDisconnect(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	require.NoError(t, err)

	replaced := authorizeResult(t, authorized).FlowID

	removing := make(chan struct{}, 1)
	release := make(chan struct{})

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpRemove:
			removing <- struct{}{}
			<-release

			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindProbe, Entries: map[string]string{}})
		}

		return nil
	})

	legs := make(chan error, 1)

	go callLeg(harness, AuthDisconnectMethod, disconnectParams(harness, 1), legs)

	// The generation bump is recorded and the removal is under way; the removed
	// record is still to come.
	<-removing

	successor := authorizeParams(harness, "openai", generation, authMethodTypeAPI)
	successor[authFieldAuthorizeRequestID] = "req-2"

	minted := make(chan authorizeCall, 1)

	go func() { minted <- callAuthorize(harness, successor) }()

	// An authorize that walked into the middle of the disconnect answers here;
	// one waiting for the provider's slot cannot.
	select {
	case call := <-minted:
		minted <- call
	case <-time.After(authAdmissionQuiescence):
	}

	close(release)

	require.NoError(t, awaitLeg(t, legs))

	call := <-minted
	require.NoError(t, call.err)

	current := authorizeResult(t, call.result).FlowID
	require.NotEqual(t, replaced, current)

	record, ok, err := harness.broker.ledger.read("openai")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, current, record.FlowID, "the entry names the flow authorize returned")
	require.Equal(t, int64(2), record.Revision)
	require.Equal(t, int64(2), record.BindingGeneration)
	require.Equal(t, authLedgerIntent, record.State)
}

// TestAuthorizeIntentAdmissionEndsWithTheCallersContext pins that an authorize
// queued behind a disconnect for the same provider answers its own caller.
func TestAuthorizeIntentAdmissionEndsWithTheCallersContext(t *testing.T) {
	harness := newAuthHarness(t)
	generation := harness.seedCatalog(t.Context(), defaultAuthProviders())

	release, err := harness.broker.admitSlot(t.Context(), "openai", "", "")
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// The leg's caller walks away after its key admission and before its intent
	// write, which is the only point this leg waits for the slot at.
	originalRand := authRandRead
	authRandRead = func(value []byte) (int, error) {
		cancel()

		return originalRand(value)
	}

	t.Cleanup(func() { authRandRead = originalRand })

	_, err = harness.call(ctx, AuthAuthorizeMethod, authorizeParams(harness, "openai", generation, authMethodTypeAPI))
	requireAuthFailed(t, err, authCauseTimeout)
}

// TestReloadedSessionIsAdmittedAfresh pins why the closed marker lives on the
// session instance rather than in a table keyed by session id. A load of a
// session id that is already live registers the new instance and then closes
// the one it replaced, so an id-keyed marker would be set on an id that is
// live at the moment it is written, and every provider-auth leg addressing that
// session would be refused for the life of the agent.
func TestReloadedSessionIsAdmittedAfresh(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startSecretFlow(t, harness)

	replaced := harness.session

	reloaded, err := harness.agent.startAndStoreSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.NoError(t, err)
	require.Equal(t, replaced.id, reloaded.id, "the stub reinstates the same native session id")
	require.NotSame(t, replaced, reloaded)

	t.Cleanup(func() { _ = reloaded.Close(context.WithoutCancel(t.Context())) })

	// The replaced instance took the close, and its flows went with it: the
	// flowId it minted now addresses nothing.
	_, err = harness.call(t.Context(), AuthCallbackMethod, secretCallbackParams(harness, flowID))
	requireInvalidAuthField(t, err, authFieldFlowID)

	// The reinstated id is admitted afresh, at leg entry and at publication.
	successor := authorizeParams(harness, "openai", harness.seedCatalog(t.Context(), defaultAuthProviders()), authMethodTypeAPI)
	successor[authFieldAuthorizeRequestID] = "req-2"

	authorized, err := harness.call(t.Context(), AuthAuthorizeMethod, successor)
	require.NoError(t, err)
	require.Equal(t, authInteractionSecret, authorizeResult(t, authorized).Interaction)
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
