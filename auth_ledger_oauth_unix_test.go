//go:build !windows

// The inventory and residence proofs below open a real login flow first, and
// that flow is an oauth mint, which is refused where no browser shim can
// shadow the launchers a native login opens. Windows refuses it by design.

package piacp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestInventoryReportsLedgerAndProbe(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	// Write-ahead intent only: the probe sees a slot but nothing binds it to
	// this connection generation.
	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		require.Equal(t, pi.AuthOpProbe, request.Op)
		require.Equal(t, []string{"anthropic"}, request.ProviderIDs)
		harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindProbe,
			Entries: map[string]string{"anthropic": "oauth"},
		})

		return nil
	})

	result, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	require.NoError(t, err)
	require.Equal(t, authInventoryResult{Entries: []authInventoryEntry{{
		ProviderID:        "anthropic",
		ConnectionID:      "conn-1",
		Revision:          1,
		BindingGeneration: 1,
		ProofSource:       authProofNotConfirmed,
	}}}, result)

	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	_, err = harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	require.NoError(t, err)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindProbe,
			Entries: map[string]string{"anthropic": "oauth"},
		})

		return nil
	})

	confirmed, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	require.NoError(t, err)
	require.Equal(t, authProofConfirmedPresent, inventoryResult(t, confirmed).Entries[0].ProofSource)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindProbe, Entries: map[string]string{}})

		return nil
	})

	absent, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	require.NoError(t, err)
	require.Equal(t, authProofConfirmedAbsent, inventoryResult(t, absent).Entries[0].ProofSource)
}

// TestInventoryReportsProbeFailure pins that an unanswerable probe fails the leg
// rather than reporting an absence nobody established.
func TestInventoryReportsProbeFailure(t *testing.T) {
	harness := newAuthHarness(t)
	startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	shortenAuthNativeCallTimeout(t)

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error { return nil })

	_, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	requireAuthFailed(t, err, authCauseHarvestFailed)
}

// TestResidenceIsTheProbeEntryValueNotItsKey pins which half of the bridge's
// probe answer carries residence. The bridge answers every provider the request
// named, holding the empty string where nothing is stored, so a reader that
// tested key presence would report every provider it asked about as resident —
// telling the host a credential is present after the removal it just performed,
// and refusing that removal as harvest_failed on the way.
func TestResidenceIsTheProbeEntryValueNotItsKey(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	flowID := startManualCodeFlow(t, harness, "code-1", pi.AuthMessage{OK: true})

	scriptManualCodeLogin(harness, "code-1", pi.AuthMessage{OK: true})

	_, err := harness.call(t.Context(), AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(harness.session.id),
		authFieldProviderID: "anthropic",
		authFieldMethod:     authMethodTypeOAuth,
		authFieldFlowID:     flowID,
		authFieldInput:      "code-1",
	})
	require.NoError(t, err)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		harness.deliver(ctx, pi.AuthMessage{
			ID:      request.ID,
			Kind:    pi.AuthKindProbe,
			Entries: map[string]string{"anthropic": ""},
		})

		return nil
	})

	absent, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	require.NoError(t, err)
	require.Equal(t, authProofConfirmedAbsent, inventoryResult(t, absent).Entries[0].ProofSource)

	harness.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		switch request.Op {
		case pi.AuthOpRemove:
			harness.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindResult, OK: true})
		case pi.AuthOpProbe:
			harness.deliver(ctx, pi.AuthMessage{
				ID:      request.ID,
				Kind:    pi.AuthKindProbe,
				Entries: map[string]string{"anthropic": ""},
			})
		}

		return nil
	})

	_, err = harness.call(t.Context(), AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(harness.session.id),
		authFieldProviderID:        "anthropic",
		authFieldConnectionID:      "conn-1",
		authFieldBindingGeneration: 1,
	})
	require.NoError(t, err)

	record, ok, readErr := harness.broker.ledger.read("anthropic")
	require.NoError(t, readErr)
	require.True(t, ok)
	require.Equal(t, authLedgerRemoved, record.State)
}
