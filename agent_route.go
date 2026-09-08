package piacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	routeMetaKey = "acp-go.dev/route"
	// routeMetaPath is the request path a refusal names. The reserved literal
	// is read out of `_meta`, so a host is told where in its own request the
	// value belongs rather than being handed the bare key name.
	routeMetaPath          = `_meta["` + routeMetaKey + `"]`
	routeVersion           = 1
	routeTurnNonceMaxBytes = 4 * 1024
	routeFieldVer          = "version"
	routeFieldID           = "sessionId"
	routeFieldTurn         = "turnNonce"
)

type inboundTurnRoute struct{ turnNonce string }

type turnRouteContextKey struct{}

var routeRandRead = rand.Read

// parseInboundTurnRoute reads the reserved turn route a prompt or an active-turn
// cancel carries. The two verdicts are distinct and never collapsed: an absent
// key is `missing` on the bare path, because the host forgot a key the contract
// requires; a present value that cannot be accepted is `unsupported` naming the
// offending member — `version`, `turnNonce`, or the unknown key — and the bare
// path only when the value as a whole is not an object.
func parseInboundTurnRoute(meta map[string]any) (inboundTurnRoute, error) {
	value, ok := meta[routeMetaKey]
	if !ok {
		return inboundTurnRoute{}, missingField(routeMetaPath)
	}

	object, ok := value.(map[string]any)
	if !ok {
		return inboundTurnRoute{}, routeMemberInvalid()
	}

	for key := range object {
		if key != routeFieldVer && key != routeFieldTurn {
			return inboundTurnRoute{}, routeMemberInvalid(key)
		}
	}

	if !routeVersionIsOne(object[routeFieldVer]) {
		return inboundTurnRoute{}, routeMemberInvalid(routeFieldVer)
	}

	nonce, ok := object[routeFieldTurn].(string)
	if !ok || strings.TrimSpace(nonce) == "" || len(nonce) > routeTurnNonceMaxBytes {
		return inboundTurnRoute{}, routeMemberInvalid(routeFieldTurn)
	}

	return inboundTurnRoute{turnNonce: nonce}, nil
}

func routeVersionIsOne(value any) bool {
	switch version := value.(type) {
	case int:
		return version == routeVersion
	case float64:
		return version == routeVersion
	case json.Number:
		number, ok := wireIntegerValue(version)

		return ok && number == routeVersion
	default:
		return false
	}
}

// routeMemberInvalid refuses a route value that is present and unacceptable. It
// names the member at fault, and the bare reserved path when the value as a
// whole is not an object.
func routeMemberInvalid(members ...string) error {
	var field strings.Builder
	field.WriteString(routeMetaPath)

	for _, member := range members {
		field.WriteString("." + member)
	}

	return unsupportedField(field.String())
}

func stampRouteMeta(meta map[string]any, scope elicitationScope) (map[string]any, error) {
	if scope.SessionID == "" || strings.TrimSpace(scope.TurnNonce) == "" {
		return nil, fmt.Errorf("route metadata requires sessionId and turnNonce")
	}

	if len(scope.TurnNonce) > routeTurnNonceMaxBytes {
		return nil, fmt.Errorf("route turnNonce exceeds the maximum size")
	}

	if _, exists := meta[routeMetaKey]; exists {
		return nil, fmt.Errorf("reserved route metadata collision")
	}

	route := map[string]any{routeFieldVer: routeVersion, routeFieldID: scope.SessionID, routeFieldTurn: scope.TurnNonce}

	correlations := 0
	if scope.ToolCallID != "" {
		correlations++
		route["toolCallId"] = scope.ToolCallID
	}

	if scope.RequestID != "" {
		correlations++
		route["requestId"] = scope.RequestID
	}

	if correlations == 0 {
		requestID, err := newRouteRequestID()
		if err != nil {
			return nil, err
		}

		correlations++
		route["requestId"] = requestID
	}

	if correlations != 1 {
		return nil, fmt.Errorf("route metadata requires exactly one callback correlation")
	}

	out := cloneAnyMap(meta)
	if out == nil {
		out = map[string]any{}
	}

	out[routeMetaKey] = route

	return out, nil
}

func newRouteRequestID() (string, error) {
	var data [16]byte
	if _, err := routeRandRead(data[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(data[:]), nil
}

func turnRouteMeta(turnNonce string) map[string]any {
	return map[string]any{routeMetaKey: map[string]any{routeFieldVer: routeVersion, routeFieldTurn: turnNonce}}
}

func requestTurnRouteMeta(turnNonce string) map[string]any {
	if !validRouteTurnNonce(turnNonce) {
		return nil
	}

	return turnRouteMeta(turnNonce)
}

func validRouteTurnNonce(turnNonce string) bool {
	return strings.TrimSpace(turnNonce) != "" && len(turnNonce) <= routeTurnNonceMaxBytes
}

func withTurnRoute(ctx context.Context, turnNonce string) context.Context {
	return context.WithValue(ctx, turnRouteContextKey{}, turnNonce)
}

func turnRouteMetaFromContext(ctx context.Context) map[string]any {
	turnNonce := turnNonceFromContext(ctx)
	if turnNonce == "" {
		return nil
	}

	return turnRouteMeta(turnNonce)
}

func turnNonceFromContext(ctx context.Context) string {
	turnNonce, _ := ctx.Value(turnRouteContextKey{}).(string)

	return turnNonce
}
