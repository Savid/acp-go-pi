package piacp

import (
	"encoding/json"
	"strings"
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

	for name, tc := range map[string]struct {
		meta    map[string]any
		verdict string
		field   string
	}{
		"absent route": {
			meta:    map[string]any{lifecycleMetaKey: map[string]any{}},
			verdict: valMissing,
			field:   routeMetaPath,
		},
		"malformed route": {
			meta: map[string]any{
				routeMetaKey:     map[string]any{routeFieldVer: 2, routeFieldTurn: "active-turn"},
				lifecycleMetaKey: map[string]any{},
			},
			verdict: valUnsupported,
			field:   routeMetaPath + "." + routeFieldVer,
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireRefusal(t, tc.verdict, tc.field, session.cancelRouted(t.Context(), tc.meta))

			_, promptErr := session.Prompt(t.Context(), acp.PromptRequest{Meta: tc.meta})
			requireRefusal(t, tc.verdict, tc.field, promptErr)
		})
	}

	// A cancel additionally authenticates the nonce against the turn it names,
	// and that authentication is still the route's verdict.
	requireRefusal(t, valUnsupported, routeMetaPath+"."+routeFieldTurn,
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

// requireRefusal asserts both halves of a uniform -32602 refusal: the verdict
// token and the field path. The two are never collapsed, so a test that pins
// only the path would pass while the adapter reported the wrong verdict.
func requireRefusal(t *testing.T, verdict string, field string, err error) {
	t.Helper()

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)
	require.Equal(t, map[string]any{jsonFieldError: verdict, jsonFieldField: field}, requestError.Data)
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

// TestReservedLifecycleCorrelationRefusals is the conformance table for the
// prompt correlation value while the lifecycle capability is enabled. A host
// reads three distinct facts from one field path: `missing` on the bare path
// (it forgot a required key), `unsupported` on a member path (it sent a
// malformed value), and `unsupported` on the bare path (it sent the key where
// the key has no meaning). The three are never collapsed.
func TestReservedLifecycleCorrelationRefusals(t *testing.T) {
	t.Parallel()

	validRoute := map[string]any{routeFieldVer: 1, routeFieldTurn: "turn-1"}
	value := func(submission any) map[string]any {
		return map[string]any{"version": 1, "submission": submission}
	}

	tests := []struct {
		name    string
		value   any
		present bool
		verdict string
		field   string
	}{
		{
			name:    "absent",
			verdict: valMissing,
			field:   lifecycle.MetaPath,
		},
		{
			name:    "not an object",
			value:   "sub-1",
			present: true,
			verdict: valUnsupported,
			field:   lifecycle.MetaPath,
		},
		{
			name:    "wrong version",
			value:   map[string]any{"version": 2, "submission": testSubmissionValue()},
			present: true,
			verdict: valUnsupported,
			field:   lifecycle.MetaPath + ".version",
		},
		{
			name:    "empty identifier",
			value:   value(map[string]any{"submissionId": "", "clientNonce": "nonce-1"}),
			present: true,
			verdict: valUnsupported,
			field:   lifecycle.MetaPath + ".submission.submissionId",
		},
		{
			name: "over-bound identifier",
			value: value(map[string]any{
				"submissionId": "sub-1",
				"clientNonce":  strings.Repeat("c", lifecycle.IdentifierBound+1),
			}),
			present: true,
			verdict: valUnsupported,
			field:   lifecycle.MetaPath + ".submission.clientNonce",
		},
		{
			name:    "unknown member",
			value:   map[string]any{"version": 1, "submission": testSubmissionValue(), "extra": true},
			present: true,
			verdict: valUnsupported,
			field:   lifecycle.MetaPath + ".extra",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			session := negotiatedLifecycleSession(t)

			meta := map[string]any{routeMetaKey: validRoute}
			if test.present {
				meta[lifecycleMetaKey] = test.value
			}

			_, err := session.Prompt(t.Context(), acp.PromptRequest{Meta: meta})
			requireRefusal(t, test.verdict, test.field, err)
		})
	}
}

// TestPromptFailingBothReservedKeysReportsTheRouteAlone pins the ordering rule
// on the wire shapes themselves: route validation runs before the lifecycle
// correlation value is read, so a prompt wrong in both ways never reports two
// rejections and the order of two failures is never implementation-defined.
func TestPromptFailingBothReservedKeysReportsTheRouteAlone(t *testing.T) {
	t.Parallel()

	session := negotiatedLifecycleSession(t)

	_, err := session.Prompt(t.Context(), acp.PromptRequest{Meta: map[string]any{
		lifecycleMetaKey: map[string]any{"version": 2},
	}})
	requireRefusal(t, valMissing, routeMetaPath, err)

	_, err = session.Prompt(t.Context(), acp.PromptRequest{Meta: map[string]any{
		routeMetaKey:     map[string]any{routeFieldVer: 1, routeFieldTurn: ""},
		lifecycleMetaKey: map[string]any{"version": 2},
	}})
	requireRefusal(t, valUnsupported, routeMetaPath+"."+routeFieldTurn, err)
}

// TestReservedLifecycleKeyOnANonCarrierSurfaceIsUnsupported pins the third
// fact: on a surface that carries no correlation value the same key is present
// where it has no meaning, so the verdict is `unsupported` on the bare path
// rather than `missing`.
func TestReservedLifecycleKeyOnANonCarrierSurfaceIsUnsupported(t *testing.T) {
	t.Parallel()

	agent := NewAgent(testContainmentOption())
	present := map[string]any{lifecycleMetaKey: map[string]any{}}

	_, err := agent.NewSession(t.Context(), acp.NewSessionRequest{Cwd: t.TempDir(), Meta: present})
	requireRefusal(t, valUnsupported, lifecycle.MetaPath, err)

	_, err = agent.LoadSession(t.Context(), acp.LoadSessionRequest{
		SessionId: acp.SessionId(validSessionUUID), Cwd: t.TempDir(), Meta: present,
	})
	requireRefusal(t, valUnsupported, lifecycle.MetaPath, err)
}
