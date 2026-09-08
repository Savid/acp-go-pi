package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

const quotaTestCurrencyUSD = "USD"

func newQuotaHarness(t *testing.T, opts ...Option) *authHarness {
	t.Helper()

	harness := newAuthHarness(t, opts...)
	harness.agent.providerAuth = nil
	harness.session.model = "openrouter/model"

	return harness
}

func quotaTestSnapshot(provider string) RateLimitsResponse {
	return RateLimitsResponse{ProviderID: provider, Availability: "available", Pools: []RateLimitPool{{
		ID: "openrouter-account", Windows: []RateLimitWindow{}, Balances: []RateLimitBalance{{
			ID: "credits", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Used: &RateLimitMoney{Amount: 40, Currency: quotaTestCurrencyUSD}, Remaining: &RateLimitMoney{Amount: 10, Currency: quotaTestCurrencyUSD},
		}},
	}}}
}

func deliverQuotaDialog(t *testing.T, harness *authHarness, outbox *sessionOutbox, message quotaBridgeMessage) string {
	t.Helper()

	payload, err := json.Marshal(message)
	require.NoError(t, err)
	harness.mu.Lock()
	harness.dialogs++
	id := fmt.Sprintf("quota-dialog-%d", harness.dialogs)
	harness.answers[id] = &authAnswer{done: make(chan struct{})}
	harness.mu.Unlock()
	harness.session.routeUIRequestEstablished(t.Context(), outbox, pi.UIRequest{
		ID: id, Method: uiMethodSelect, Title: pi.AuthTitleMarker + string(payload),
	})

	return id
}

func callQuota(t *testing.T, ctx context.Context, harness *authHarness, provider string) (any, error) {
	t.Helper()

	payload, err := json.Marshal(RateLimitsRequest{ProviderID: provider, SessionID: harness.session.id})
	require.NoError(t, err)

	return harness.agent.HandleExtensionMethod(ctx, RateLimitsMethod, payload)
}

func TestQuotaBridgeWorksWithoutProviderAuthAndKeepsIndependentPools(t *testing.T) {
	t.Parallel()

	harness := newQuotaHarness(t)
	want := quotaTestSnapshot("openrouter")
	releaseTurn, err := harness.session.acquireTurn(t.Context())
	require.NoError(t, err)
	defer releaseTurn()

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		deadline, bounded := ctx.Deadline()
		require.True(t, bounded)
		require.LessOrEqual(t, time.Until(deadline), 30*time.Second)
		require.Equal(t, pi.AuthOpQuota, request.Op)
		require.Equal(t, "openrouter", request.ProviderID)
		id := deliverQuotaDialog(t, harness, harness.session.outbox, quotaBridgeMessage{ID: request.ID, Kind: pi.AuthKindQuota, Response: want})
		require.Equal(t, pi.AuthAck, *harness.awaitAnswer(id).Value)

		return nil
	})

	actual, err := callQuota(t, t.Context(), harness, "openrouter")
	require.NoError(t, err)
	require.Equal(t, want, actual)
	require.Empty(t, harness.session.quotaExchanges)
}

func TestQuotaBridgeSourceChangeDiscardsObservations(t *testing.T) {
	t.Parallel()

	for _, change := range []string{"model", "generation", "closed", "deleted", "poisoned", "wrong_provider", "malformed", "native_error"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()

			harness := newQuotaHarness(t)
			original := harness.session.outbox
			want := quotaTestSnapshot("openrouter")
			harness.scriptBridge(func(_ context.Context, request pi.AuthRequest) error {
				if change == "native_error" {
					return errors.New("native failure must not cross")
				}

				harness.session.mu.Lock()
				switch change {
				case "model":
					harness.session.model = "openrouter/different"
				case "generation":
					harness.session.outbox = newTestSessionOutbox(original.generation + 1)
				case "closed":
					harness.session.closing = true
				case "poisoned":
					harness.session.poisonCause = "native_invariant"
				case "wrong_provider":
					want.ProviderID = "other"
				case "malformed":
					want.Pools = nil
				}
				harness.session.mu.Unlock()

				if change == "deleted" {
					harness.agent.mu.Lock()
					harness.agent.deleted[harness.session.id] = struct{}{}
					harness.agent.mu.Unlock()
				}

				// Route to the source exchange before the wrapper revalidates its
				// session. Generation replacement is exercised without binding the
				// old reply to the successor's pump.
				message := quotaBridgeMessage{ID: request.ID, Kind: pi.AuthKindQuota, Response: want}
				payload, err := json.Marshal(message)
				require.NoError(t, err)
				released := make(chan struct{})
				require.True(t, harness.session.routeQuotaDialog(t.Context(), original,
					pi.UIRequest{ID: request.ID, Method: uiMethodSelect, Title: pi.AuthTitleMarker + string(payload)}, func() { close(released) }))
				<-released

				return nil
			})

			actual, err := callQuota(t, t.Context(), harness, "openrouter")
			switch change {
			case "closed", "deleted":
				requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
			case "poisoned":
				var requestErr *acp.RequestError
				require.ErrorAs(t, err, &requestErr)
				require.Equal(t, -32603, requestErr.Code)
			default:
				require.NoError(t, err)
				require.Equal(t, rateLimitsUnavailable("openrouter", "read_failed"), actual)
			}

			harness.session.mu.Lock()
			harness.session.outbox = original
			harness.session.closing = false
			harness.session.poisonCause = ""
			harness.session.mu.Unlock()
		})
	}
}

func TestQuotaBridgeCancellationAnswersNativeAbortWatch(t *testing.T) {
	t.Parallel()

	harness := newQuotaHarness(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	watching := make(chan string, 1)
	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		watching <- deliverQuotaDialog(t, harness, harness.session.outbox, quotaBridgeMessage{ID: request.ID, Kind: pi.AuthKindQuotaCancel})
		<-ctx.Done()

		return ctx.Err()
	})

	done := make(chan error, 1)
	go func() {
		_, err := callQuota(t, ctx, harness, "openrouter")
		done <- err
	}()
	dialogID := <-watching
	select {
	case <-harness.answered(dialogID):
		t.Fatal("quota abort watch answered before cancellation")
	default:
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, pi.AuthAck, *harness.awaitAnswer(dialogID).Value)
	require.Empty(t, harness.session.quotaExchanges)
}

func TestQuotaBridgeRejectsGenerationChangeWhileWaitingToWrite(t *testing.T) {
	t.Parallel()

	harness := newQuotaHarness(t)
	original := harness.session.outbox
	require.NoError(t, original.dispatchMu.lock(t.Context()))
	var writes atomic.Int64
	harness.client.promptWriteFunc = func(context.Context, string) error {
		writes.Add(1)

		return nil
	}
	observed := &observedDoneContext{Context: t.Context(), entered: make(chan struct{})}
	done := make(chan RateLimitsResponse, 1)
	readError := make(chan error, 1)
	go func() {
		response, err := harness.session.readQuota(observed, "openrouter", "openrouter/model")
		done <- response
		readError <- err
	}()
	<-observed.entered
	harness.session.mu.Lock()
	harness.session.outbox = newTestSessionOutbox(original.generation + 1)
	harness.session.mu.Unlock()
	original.dispatchMu.Unlock()
	require.Equal(t, rateLimitsUnavailable("openrouter", "read_failed"), <-done)
	require.NoError(t, <-readError)
	require.Zero(t, writes.Load())
	harness.session.mu.Lock()
	harness.session.outbox = original
	harness.session.mu.Unlock()
}

func TestQuotaBridgePreservesOwnerFailures(t *testing.T) {
	t.Parallel()

	for _, stage := range []string{"before_agent", "before_session", "during_agent", "during_session", "native_containment", "native_authority"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()

			harness := newQuotaHarness(t)
			ownerErr := ErrContainmentIncomplete
			if stage == "native_authority" {
				ownerErr = ErrHostAuthorityUnavailable
			}

			if stage == "before_agent" {
				harness.agent.nativeContainmentErr = ownerErr
			}

			if stage == "before_session" {
				harness.session.nativeContainmentErr = ownerErr
			}

			var calls atomic.Int64
			harness.scriptBridge(func(_ context.Context, request pi.AuthRequest) error {
				calls.Add(1)

				switch stage {
				case "during_agent":
					harness.agent.mu.Lock()
					harness.agent.nativeContainmentErr = ownerErr
					harness.agent.mu.Unlock()
				case "during_session":
					harness.session.mu.Lock()
					harness.session.nativeContainmentErr = ownerErr
					harness.session.mu.Unlock()
				case "native_containment", "native_authority":
					return fmt.Errorf("native bridge: %w", ownerErr)
				}

				id := deliverQuotaDialog(t, harness, harness.session.outbox, quotaBridgeMessage{
					ID: request.ID, Kind: pi.AuthKindQuota, Response: quotaTestSnapshot("openrouter"),
				})
				harness.awaitAnswer(id)

				return nil
			})

			_, err := callQuota(t, t.Context(), harness, "openrouter")
			require.ErrorIs(t, err, ownerErr)
			if stage == "before_agent" || stage == "before_session" {
				require.Zero(t, calls.Load())
			} else {
				require.Equal(t, int64(1), calls.Load())
			}

			harness.agent.mu.Lock()
			harness.agent.nativeContainmentErr = nil
			harness.agent.mu.Unlock()
			harness.session.mu.Lock()
			harness.session.nativeContainmentErr = nil
			harness.session.mu.Unlock()
		})
	}
}

func TestRateLimitsDirectOptOutAndMissingScopeDoNoNativeWork(t *testing.T) {
	t.Parallel()

	harness := newQuotaHarness(t, WithPiDirectAPI(false))
	harness.scriptBridge(func(context.Context, pi.AuthRequest) error {
		t.Fatal("disabled quota read reached native bridge")

		return nil
	})
	actual, err := callQuota(t, t.Context(), harness, "openrouter")
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnavailable("openrouter", "disabled"), actual)
	actual, err = callQuota(t, t.Context(), harness, "unimplemented")
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnsupported("unimplemented"), actual)

	agent := newRateLimitsFixtureAgent(t)
	actual, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"opencode-go"}`))
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnavailable("opencode-go", "session_required"), actual)
}
