package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/coder/acp-go-sdk"
)

// Session-scoped provider-auth extension methods. pi installs a completed
// credential into its own durable per-instance agent directory and refreshes it
// there under its own cross-process lock, so it brokers no credential out and
// accepts no injection: there is no credential leg and no injection key.
const (
	AuthMethodsMethod    = "_pi/auth/methods"
	AuthAuthorizeMethod  = "_pi/auth/authorize"
	AuthCallbackMethod   = "_pi/auth/callback"
	AuthStatusMethod     = "_pi/auth/status"
	AuthCancelMethod     = "_pi/auth/cancel"
	AuthInventoryMethod  = "_pi/auth/inventory"
	AuthDisconnectMethod = "_pi/auth/disconnect"
)

const (
	providerAuthCapabilityKey = "providerAuth"
	providerAuthMethodsField  = "methods"

	authFailedErrorTag = "pi_auth_failed"

	authFieldSessionID          = "sessionId"
	authFieldProviderID         = "providerId"
	authFieldConnectionID       = "connectionId"
	authFieldMethodsGeneration  = "methodsGeneration"
	authFieldMethod             = "method"
	authFieldAuthorizeRequestID = "authorizeRequestId"
	authFieldInputs             = "inputs"
	authFieldFlowID             = "flowId"
	authFieldInput              = "input"
	authFieldBindingGeneration  = "bindingGeneration"
	authFieldParams             = "params"

	authValueInvalid = "invalid"
)

// Closed cause enum returned by a provider-auth leg.
const (
	authCauseNativeVeto         = "native_veto"
	authCauseProviderRefused    = "provider_refused"
	authCauseTransport          = "transport"
	authCauseProcess            = "process"
	authCauseTimeout            = "timeout"
	authCauseHarvestFailed      = "harvest_failed"
	authCauseUnsupportedVariant = "unsupported_variant"
	authCauseFlowExpired        = "flow_expired"
	authCauseFlowState          = "flow_state"
	authCauseFlowCancelled      = "flow_cancelled"
	authCausePolicy             = "policy"
	authCauseBindingConflict    = "binding_conflict"
)

// authMethodNames lists every advertised leg, in the order the capability
// reports them.
func authMethodNames() []string {
	return []string{
		AuthMethodsMethod,
		AuthAuthorizeMethod,
		AuthCallbackMethod,
		AuthStatusMethod,
		AuthCancelMethod,
		AuthInventoryMethod,
		AuthDisconnectMethod,
	}
}

// providerAuth is the agent-scoped broker behind the provider-auth legs. It
// owns the current method catalog, the per-session flow records, the durable
// values-free ledger, and the in-flight exchanges with the bridge extension.
//
// Every inbound ACP request runs on its own goroutine and only notifications
// are processed in sequence, so two legs addressing the same flow, the same
// session, or the same provider's credential slot run at the same time: an
// authorize and its repeat, two callbacks, a disconnect and the login
// completion it is racing, a session close and the authorize that has not
// published yet. Each of those sequences is a read of broker or ledger state, a
// native call of unbounded length, and a write decided by what the read saw, so
// the window of every check-then-set here is the whole native call. The
// admission gates in auth_admission.go are what make those sequences atomic
// against each other; nothing about them is visible to -race, because each
// individual field access is already locked.
type providerAuth struct {
	agent  *Agent
	ledger *authLedger

	// authorizeGate serialises authorize per (sessionId, providerId); slotGate
	// serialises everything that rewrites one provider's native credential
	// slot. Neither is taken while mu is held.
	authorizeGate *authGate[authFlowKey]
	slotGate      *authGate[string]

	mu         sync.Mutex
	generation string
	catalog    map[string][]authCatalogMethod
	flows      map[authFlowKey]*authFlow
	byID       map[string]*authFlow
	// retained holds the most recent flow per key whatever its state, so the
	// idempotency key stays answerable after the flow has terminalized. Only a
	// session close drops an entry.
	retained map[authFlowKey]*authFlow
	// retired holds the authorizeRequestIds a supersede made unanswerable, so a
	// delayed retry of one fails on its own key instead of cancelling the flow
	// that replaced it.
	retired   map[authFlowKey]map[string]struct{}
	exchanges map[string]*authExchange
}

type authFlowKey struct {
	sessionID  acp.SessionId
	providerID string
}

// newProviderAuth requires a durable agent directory and is unavailable when
// process isolation is configured.
func newProviderAuth(agent *Agent) *providerAuth {
	if !authLedgerRootConfigured(agent.options) || agent.options.Home == "" || agent.options.ProcessIsolation != nil {
		return nil
	}

	ledger, err := newAuthLedger(agent.options)
	if err != nil {
		agent.log.WarnContext(context.Background(), "provider auth surface is unavailable",
			slog.String(jsonFieldError, err.Error()))

		return nil
	}

	return &providerAuth{
		agent:         agent,
		ledger:        ledger,
		authorizeGate: newAuthGate[authFlowKey](),
		slotGate:      newAuthGate[string](),
		flows:         make(map[authFlowKey]*authFlow),
		byID:          make(map[string]*authFlow),
		retained:      make(map[authFlowKey]*authFlow),
		retired:       make(map[authFlowKey]map[string]struct{}),
		exchanges:     make(map[string]*authExchange),
	}
}

// capability reports the enabled leg names. The array is the host's only
// discovery surface for which legs exist, so an absent leg is omitted rather
// than reported false.
func (p *providerAuth) capability() map[string]any {
	return map[string]any{providerAuthMethodsField: authMethodNames()}
}

func (a *Agent) handleAuthExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, bool, error) {
	broker := a.providerAuth
	if broker == nil {
		return nil, false, nil
	}

	switch method {
	case AuthMethodsMethod:
		result, err := broker.methods(ctx, params)

		return result, true, err
	case AuthAuthorizeMethod:
		result, err := broker.authorize(ctx, params)

		return result, true, err
	case AuthCallbackMethod:
		result, err := broker.callback(ctx, params)

		return result, true, err
	case AuthStatusMethod:
		result, err := broker.status(ctx, params)

		return result, true, err
	case AuthCancelMethod:
		result, err := broker.cancel(ctx, params)

		return result, true, err
	case AuthInventoryMethod:
		result, err := broker.inventory(ctx, params)

		return result, true, err
	case AuthDisconnectMethod:
		result, err := broker.disconnect(ctx, params)

		return result, true, err
	default:
		return nil, false, nil
	}
}

// authFailedError is the uniform provider-auth leg failure. Native message
// text, native response bodies, and child stderr never reach it: every failure
// becomes this closed shape.
type authFailedError struct {
	cause      string
	providerID string
	method     string
	flowID     string
}

func (f *authFailedError) Error() string {
	return authFailedErrorTag + ": " + f.cause
}

func (f *authFailedError) requestError() *acp.RequestError {
	data := map[string]any{
		jsonFieldError: authFailedErrorTag,
		"cause":        f.cause,
		"retryable":    authCauseRetryable(f.cause),
	}
	if f.providerID != "" {
		data[authFieldProviderID] = f.providerID
	}

	if f.method != "" {
		data[authFieldMethod] = f.method
	}

	if f.flowID != "" {
		data[authFieldFlowID] = f.flowID
	}

	return acp.NewAuthRequired(data)
}

// authCauseRetryable reports whether the same call could succeed unchanged. The
// three transport-shaped causes can; a refusal, a veto, and every flow-state
// answer cannot, because repeating them changes nothing.
func authCauseRetryable(cause string) bool {
	switch cause {
	case authCauseTransport, authCauseProcess, authCauseTimeout:
		return true
	default:
		return false
	}
}

func authFailed(cause string, providerID string, method string, flowID string) error {
	failure := &authFailedError{cause: cause, providerID: providerID, method: method, flowID: flowID}

	return failure.requestError()
}

// authFlowTransition maps a leg cause to the flow transition it must also
// perform. An empty state means the cause carries no transition: a refusal the
// adapter made itself never consumes the owner's authorization.
func authFlowTransition(cause string, materialInFlight bool) (string, string) {
	switch cause {
	case authCauseNativeVeto, authCauseUnsupportedVariant:
		return authStateFailed, authReasonNativeVeto
	case authCauseProviderRefused:
		return authStateFailed, authReasonProviderRefused
	case authCauseTransport:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonTransport
	case authCauseProcess:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonProcess
	case authCauseTimeout:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonTransport
	case authCauseHarvestFailed:
		return authStateFailed, authReasonHarvestFailed
	case authCauseFlowExpired:
		return authStateExpired, authReasonDeadline
	default:
		return "", ""
	}
}

// authSession resolves the session a leg addresses. An unknown, unloaded, or
// tombstoned session gets the uniform unknown-session rejection, and so does a
// session whose provider-auth admission this broker has already closed: an
// ordinary late leg is refused here cheaply, before it costs a native call.
// This is the fast path, not the authoritative one — publishFlow is what
// decides a leg that was already past this point when the close landed.
func (p *providerAuth) authSession(id string) (*agentSession, error) {
	session, err := p.agent.session(acp.SessionId(id))
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if session.authClosed {
		return nil, unknownSessionError()
	}

	return session, nil
}

// authParamFields walks a leg's params object once, rejecting an unknown field,
// a duplicate field, and a non-object body with the offending field path. Every
// request object on this surface is closed, and encoding/json alone would let a
// duplicate key silently win.
func authParamFields(raw json.RawMessage, allowed ...string) (map[string]json.RawMessage, error) {
	permitted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		permitted[name] = struct{}{}
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalidAuthField(authFieldParams)
	}

	fields := make(map[string]json.RawMessage, len(allowed))

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, invalidAuthField(authFieldParams)
		}

		key, _ := keyToken.(string)
		if _, ok := permitted[key]; !ok {
			return nil, unsupportedField(key)
		}

		if _, duplicate := fields[key]; duplicate {
			return nil, unsupportedField(key)
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, invalidAuthField(key)
		}

		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, invalidAuthField(authFieldParams)
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, invalidAuthField(authFieldParams)
	}

	return fields, nil
}

// authRequiredString decodes a non-empty string field.
func authRequiredString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", invalidAuthField(name)
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", invalidAuthField(name)
	}

	return value, nil
}

// authConnectionIDMaxBytes bounds the caller-minted connection id. The value is
// durable — it lands in a ledger entry a later leg equality-checks against what
// the caller sent — and the bound leaves room for the opaque token a consumer
// mints, of which a prefixed UUID is forty bytes.
const authConnectionIDMaxBytes = 128

// authRequiredConnectionID decodes and validates the connection id a leg
// addresses. It runs where the value enters, ahead of every comparison and
// every write, so no leg ever fences against or records an id this bound
// refuses. The value is never normalised: a later leg compares it byte for byte
// with what the caller sent, so rewriting it would break that comparison.
func authRequiredConnectionID(fields map[string]json.RawMessage) (string, error) {
	value, err := authRequiredString(fields, authFieldConnectionID)
	if err != nil {
		return "", err
	}

	if !authValidConnectionID(value) {
		return "", invalidAuthField(authFieldConnectionID)
	}

	return value, nil
}

// authValidConnectionID reports whether id is an opaque bounded ASCII token.
// The alphabet keeps the id safe in every position it reaches — a path segment,
// a native label, and a log line — and admits no non-ASCII spelling, so two
// wire encodings can never decode to one Go string and alias one connection
// onto another's entry.
func authValidConnectionID(id string) bool {
	if id == "" || len(id) > authConnectionIDMaxBytes {
		return false
	}

	for index := range len(id) {
		if !authConnectionIDByte(id[index]) {
			return false
		}
	}

	return true
}

func authConnectionIDByte(char byte) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9') ||
		char == '-' || char == '_'
}

// authString decodes a string field that may be empty but must be present.
func authString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", invalidAuthField(name)
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", invalidAuthField(name)
	}

	return value, nil
}

func authRequiredInt64(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, invalidAuthField(name)
	}

	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, invalidAuthField(name)
	}

	return value, nil
}

func (p *providerAuth) goSafe(name string, fn func()) {
	go func() {
		defer recoverAgentGoroutine(context.Background(), p.agent.log, name)

		fn()
	}()
}

func invalidAuthField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: authValueInvalid,
		jsonFieldField: path,
	})
}
