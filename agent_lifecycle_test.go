package piacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

func TestLifecycleCapabilityStrictScalar(t *testing.T) {
	for _, raw := range []string{`"1"`, `1.0`, `1.5`, `null`, `true`, `{}`, `[]`, `2`} {
		var negotiated lifecycle.Negotiated
		err := json.Unmarshal([]byte(`{"version":`+raw+`}`), &negotiated)
		require.Error(t, err, raw)
	}
}

func TestLifecycleNegotiationPrecisionAndReservedRouting(t *testing.T) {
	agent := NewAgent(testContainmentOption())
	require.Equal(t, lifecycle.Negotiated{}, (*Agent)(nil).lifecycleNegotiated())

	answer, err := agent.negotiateLifecycle(map[string]any{lifecycleMetaKey: map[string]any{"version": json.Number("1")}})
	require.NoError(t, err)
	require.Contains(t, answer, lifecycleMetaKey)

	// Values that float64 would round onto one are not equal to protocol 1.
	_, err = agent.negotiateLifecycle(map[string]any{lifecycleMetaKey: map[string]any{"version": json.Number("1.0000000000000000001")}})
	require.Error(t, err)

	require.NoError(t, refuseLifecycleMeta(nil))
	require.Error(t, refuseLifecycleMeta(map[string]any{lifecycleMetaKey: map[string]any{}}))
	require.NoError(t, refuseLifecycleRawMeta(json.RawMessage(`{`)))
	require.NoError(t, refuseLifecycleRawMeta(json.RawMessage(`{"_meta":{}}`)))
	require.Error(t, refuseLifecycleRawMeta(json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)))

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
		Meta: map[string]any{lifecycleMetaKey: map[string]any{"version": json.Number("1.0000000000000000001")}},
	})
	require.Error(t, err, "a malformed offer fails initialize")

	_, err = agent.Authenticate(t.Context(), acp.AuthenticateRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.Logout(t.Context(), acp.LogoutRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`))
	require.Error(t, err)
	requireRefusedField(t, lifecycle.MetaPath, err)
	_, err = agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{Meta: reserved},
	})
	require.Error(t, err)
	_, err = agent.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: reserved})
	require.Error(t, err)
	// Unreachable from the wire, reachable from an embedding Go host: the
	// literal is refused before the method-not-found verdict.
	_, err = agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{Meta: reserved})
	require.Error(t, err)
	requireRefusedField(t, lifecycle.MetaPath, err)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{Meta: reserved})
	require.Error(t, err)
	_, err = piOptionsFromMeta(reserved)
	require.Error(t, err)
	// The cancel surface authorizes the route nonce before it reads anything
	// else, so the reserved literal is refused on a cancel the current turn
	// would otherwise have authorized.
	routedReserved := turnRouteMeta("active-turn")
	routedReserved[lifecycleMetaKey] = map[string]any{}
	cancelSession := &agentSession{agent: agent, cancel: func() {}, turnNonce: "active-turn"}
	require.Error(t, cancelSession.cancelRouted(t.Context(), routedReserved))
}

// TestReservedLifecycleMetaPrecedesMethodResolution pins the order on the
// extension surface: a closed agent is refused first, then the reserved family
// literal, then the unknown method. A method this adapter defines nowhere is no
// licence to ignore the key — the literal is the family's, not the method's, so
// the request is answered about the key it misplaced. Without the key the same
// unknown method keeps its method-not-found verdict.
func TestReservedLifecycleMetaPrecedesMethodResolution(t *testing.T) {
	agent := NewAgent(testContainmentOption())
	reserved := json.RawMessage(`{"_meta":{"acp-go.dev/lifecycle":{}}}`)

	for _, method := range []string{"_x/nonexistent", "_pi/unknown", ForkSessionMethod} {
		_, err := agent.HandleExtensionMethod(t.Context(), method, reserved)

		var requestError *acp.RequestError

		require.ErrorAs(t, err, &requestError, method)
		require.Equal(t, -32602, requestError.Code, method)
		requireRefusedField(t, lifecycle.MetaPath, err)
	}

	_, err := agent.HandleExtensionMethod(t.Context(), "_x/nonexistent", json.RawMessage(`{}`))

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code, "an unknown method carrying no reserved key is still method-not-found")

	// The closed agent's refusal outranks both.
	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), "_x/nonexistent", reserved)
	require.ErrorIs(t, err, errAgentClosed)
}

// TestRouteValidationPrecedesTheReservedLifecycleRefusal pins the precedence on
// every inbound surface that carries both the route envelope and the lifecycle
// key: the authenticator runs before the placement rule, so a request that is
// wrong in both ways reports the route verdict. One verdict, never an
// implementation-defined choice between two.
func TestRouteValidationPrecedesTheReservedLifecycleRefusal(t *testing.T) {
	agent := NewAgent(testContainmentOption())
	session := &agentSession{agent: agent, id: "route-precedence", cancel: func() {}, turnNonce: "active-turn"}

	for name, meta := range map[string]map[string]any{
		"absent route": {lifecycleMetaKey: map[string]any{}},
		"malformed route": {
			routeMetaKey:     map[string]any{routeFieldVer: 2, routeFieldTurn: "active-turn"},
			lifecycleMetaKey: map[string]any{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireRefusedField(t, routeMetaKey, session.cancelRouted(t.Context(), meta))

			_, promptErr := session.Prompt(t.Context(), acp.PromptRequest{Meta: meta})
			requireRefusedField(t, routeMetaKey, promptErr)
		})
	}

	// A cancel additionally authenticates the nonce against the turn it names,
	// and that authentication is still the route's verdict.
	requireRefusedField(t, routeMetaKey,
		session.cancelRouted(t.Context(), mergeRouteMeta(turnRouteMeta("stale-turn"), lifecycleMetaKey)))

	// With the route valid the same request reports the lifecycle verdict, so
	// the ordering above is precedence rather than the route swallowing the
	// second defect.
	requireRefusedField(t, lifecycle.MetaPath,
		session.cancelRouted(t.Context(), mergeRouteMeta(turnRouteMeta("active-turn"), lifecycleMetaKey)))
}

// mergeRouteMeta adds the reserved lifecycle literal to a route envelope, which
// is the combined case the precedence rule is about.
func mergeRouteMeta(meta map[string]any, key string) map[string]any {
	meta[key] = map[string]any{}

	return meta
}

// requireRefusedField asserts which member a refusal named, which is the only
// way a wire-silent surface can state which of two rules produced its verdict.
func requireRefusedField(t *testing.T, field string, err error) {
	t.Helper()

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)

	data, ok := requestError.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, field, data[jsonFieldField])
}

func TestProvenLifecycleFactsFollowTheContainmentBoundary(t *testing.T) {
	shared := NewAgent(testContainmentOption()).provenLifecycleFacts()
	require.True(t, shared.UpdatesOutsidePrompt)
	require.False(t, shared.AuthoritativeQuiescence)
	require.Empty(t, shared.QuiescenceSource)
	require.Empty(t, shared.ActivityKinds)

	authoritative := (&Agent{options: Options{hostAuthoritySupplied: true}}).provenLifecycleFacts()
	require.True(t, authoritative.AuthoritativeQuiescence)
	require.Equal(t, lifecycle.ProofClassProcessContainment, authoritative.QuiescenceSource)
}
