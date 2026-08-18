package piacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

func TestLifecycleNegotiationPrecisionAndReservedRouting(t *testing.T) {
	agent := NewAgent(testContainmentOption())
	require.Equal(t, lifecycle.Negotiated{}, (*Agent)(nil).lifecycleNegotiated())

	answer, err := agent.negotiateLifecycle(map[string]any{lifecycleMetaKey: map[string]any{"versions": []any{json.Number("1")}}})
	require.NoError(t, err)
	require.Contains(t, answer, lifecycleMetaKey)

	// Values that float64 would round onto one are not equal to protocol 1.
	_, err = agent.negotiateLifecycle(map[string]any{lifecycleMetaKey: map[string]any{"versions": []any{json.Number("1.0000000000000000001")}}})
	require.Error(t, err)

	require.NoError(t, refuseLifecycleMeta(nil))
	require.Error(t, refuseLifecycleMeta(map[string]any{lifecycleMetaKey: map[string]any{}}))
	require.NoError(t, refuseLifecycleRawMeta(json.RawMessage(`{`)))
	require.NoError(t, refuseLifecycleRawMeta(json.RawMessage(`{"_meta":{}}`)))
	require.Error(t, refuseLifecycleRawMeta(json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))
	require.NoError(t, refuseLifecycleExtensionMeta("unknown/method", json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))
	require.Error(t, refuseLifecycleExtensionMeta(ForkSessionMethod, json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))

	_, err = agent.lifecyclePromptCorrelation(map[string]any{lifecycleMetaKey: map[string]any{"version": 1.0}})
	require.Error(t, err)
	require.Equal(t, acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}, lifecycleCarrier())
	require.Equal(t, map[string]any{lifecycleMetaKey: map[string]any{"x": true}}, lifecycleNotificationMeta(map[string]any{"x": true}))
}

// TestReservedLifecycleMetaRefusedOnEverySurface pins the family-literal rule:
// a surface that never carries the extension refuses the key rather than
// ignoring it, whatever else the request named.
func TestReservedLifecycleMetaRefusedOnEverySurface(t *testing.T) {
	reserved := map[string]any{lifecycleMetaKey: map[string]any{}}
	agent := NewAgent(testContainmentOption())

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{
		Meta: map[string]any{lifecycleMetaKey: map[string]any{"versions": []any{json.Number("1.0000000000000000001")}}},
	})
	require.Error(t, err, "a malformed offer fails initialize")

	_, err = agent.Authenticate(t.Context(), acp.AuthenticateRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.Logout(t.Context(), acp.LogoutRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`))
	require.Error(t, err)
	_, err = agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{Meta: reserved},
	})
	require.Error(t, err)
	_, err = agent.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = piOptionsFromMeta(reserved)
	require.Error(t, err)
	require.Error(t, (&agentSession{agent: agent}).cancelRouted(t.Context(), reserved))
}

// TestProvenLifecycleFactsFollowTheContainmentBoundary pins that the
// authoritative quiescence advertisement is resolved from the active
// containment configuration, never asserted where the boundary cannot prove
// whole-tree vacancy.
func TestProvenLifecycleFactsFollowTheContainmentBoundary(t *testing.T) {
	shared := NewAgent(testContainmentOption()).provenLifecycleFacts()
	require.True(t, shared.UpdatesOutsidePrompt)
	require.False(t, shared.AuthoritativeQuiescence)
	require.Empty(t, shared.QuiescenceSource)
	require.Empty(t, shared.ActivityKinds)

	previous := agentRuntimePlatform
	agentRuntimePlatform = linuxPlatform
	t.Cleanup(func() { agentRuntimePlatform = previous })

	authoritative := NewAgent(WithProcessIsolation(*policyForContainmentModeTest())).provenLifecycleFacts()
	require.True(t, authoritative.AuthoritativeQuiescence)
	require.Equal(t, lifecycle.ProofClassProcessContainment, authoritative.QuiescenceSource)
}
