package piacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	routeMetaKey           = "acp-go.dev/route"
	routeVersion           = 1
	routeTurnNonceMaxBytes = 4 * 1024
	routeFieldVer          = "version"
	routeFieldID           = "sessionId"
	routeFieldTurn         = "turnNonce"
)

type inboundTurnRoute struct{ turnNonce string }

type turnRouteContextKey struct{}

var routeRandRead = rand.Read

func parseInboundTurnRoute(meta map[string]any) (inboundTurnRoute, error) {
	value, ok := meta[routeMetaKey]
	if !ok {
		return inboundTurnRoute{}, routeInvalid()
	}

	object, ok := value.(map[string]any)
	if !ok || len(object) != 2 {
		return inboundTurnRoute{}, routeInvalid()
	}

	if !routeVersionIsOne(object[routeFieldVer]) {
		return inboundTurnRoute{}, routeInvalid()
	}

	nonce, ok := object[routeFieldTurn].(string)
	if !ok || strings.TrimSpace(nonce) == "" {
		return inboundTurnRoute{}, routeInvalid()
	}

	if len(nonce) > routeTurnNonceMaxBytes {
		return inboundTurnRoute{}, routeInvalid()
	}

	return inboundTurnRoute{turnNonce: nonce}, nil
}

func routeVersionIsOne(value any) bool {
	switch version := value.(type) {
	case int:
		return version == routeVersion
	case float64:
		return version == routeVersion
	default:
		return false
	}
}

func routeInvalid() error {
	return unsupportedField(routeMetaKey)
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
