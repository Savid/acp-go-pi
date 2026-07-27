package piacp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

// Closed flow states.
const (
	authStatePending       = "pending"
	authStateAuthenticated = "authenticated"
	authStateSaved         = "saved"
	authStateFailed        = "failed"
	authStateCancelled     = "cancelled"
	authStateExpired       = "expired"
)

// Closed flow reasons, legal only against the state each pairs with.
const (
	authReasonProviderRefused   = "provider_refused"
	authReasonNativeVeto        = "native_veto"
	authReasonTransport         = "transport"
	authReasonProcess           = "process"
	authReasonAcceptanceUnknown = "acceptance_unknown"
	authReasonHarvestFailed     = "harvest_failed"
	authReasonOwnerCancel       = "owner_cancel"
	authReasonSuperseded        = "superseded"
	authReasonSessionClosed     = "session_closed"
	authReasonDeadline          = "deadline"
)

// Closed interaction discriminator.
const (
	authInteractionWait     = "wait"
	authInteractionCallback = "callback"
	authInteractionSecret   = "secret"
)

const authCallbackInputCode = "code"

const (
	// authSafetyDeadline bounds a flow independently of the harness.
	authSafetyDeadline = 15 * time.Minute
)

var (
	authRandRead = rand.Read
	authNow      = time.Now

	// authNativeCallTimeoutValue bounds one non-blocking bridge exchange.
	authNativeCallTimeoutValue = 30 * time.Second
	// authLoginTimeoutValue bounds one whole native login, which stays open
	// across the authorize and callback legs.
	authLoginTimeoutValue = 20 * time.Minute
)

// authFlow is the session-scoped record of one login. The presentation it can
// replay lives here and nowhere else: it carries url, message, and userCode,
// which are code-bearing for the flow's life.
type authFlow struct {
	id                 string
	sessionID          acp.SessionId
	providerID         string
	connectionID       string
	revision           int64
	bindingGeneration  int64
	method             authCatalogMethod
	authorizeRequestID string
	presentation       authAuthorizeResult

	createdAt           int64
	state               string
	reason              string
	expiresAt           time.Time
	credentialExpiresAt int64

	// Presentation facts folded in from native events, guarded by
	// providerAuth.mu until authorize publishes them.
	presentInteraction string
	presentURL         string
	presentMessage     string
	presentUserCode    string
	// presentMessageNative records that the harness supplied its own
	// presentation text, which a later prompt label must not displace.
	presentMessageNative bool
	presentPollMs        int64
	nativeCause          string

	parkedDialog  string
	pendingSecret string

	ready     chan struct{}
	readyOnce sync.Once
	result    chan pi.AuthMessage

	disarm chan struct{}
}

type authAuthorizeResult struct {
	Interaction    string `json:"interaction"`
	URL            string `json:"url,omitempty"`
	Message        string `json:"message"`
	UserCode       string `json:"userCode,omitempty"`
	CallbackInput  string `json:"callbackInput,omitempty"`
	FlowID         string `json:"flowId"`
	FlowExpiresAt  int64  `json:"flowExpiresAt"`
	PollIntervalMs int64  `json:"pollIntervalMs,omitempty"`
}

type authFlowIDResult struct {
	FlowID string `json:"flowId"`
}

type authStatusResult struct {
	FlowID    string `json:"flowId"`
	State     string `json:"state"`
	ExpiresAt int64  `json:"expiresAt,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func authTerminal(state string) bool {
	return state != authStatePending
}

// newAuthToken mints an opaque adapter-owned identifier from 16 CSPRNG bytes,
// encoded unpadded base64url. Native flow handles never cross the boundary.
func newAuthToken() (string, error) {
	var value [16]byte
	if _, err := authRandRead(value[:]); err != nil {
		return "", fmt.Errorf("create provider auth token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

// recordAuthURL folds a native authorization URL into the flow. A URL that
// completes on a loopback listener is refused here rather than relayed: the
// owner's browser cannot reach the worker's socket.
func (p *providerAuth) recordAuthURL(flow *authFlow, event pi.AuthNativeEvent) {
	if authLoopbackHost(event.URL) {
		p.veto(flow, authCauseUnsupportedVariant)

		return
	}

	value, ok := authDisplayURL(event.URL)
	if !ok {
		p.veto(flow, authCauseNativeVeto)

		return
	}

	p.mu.Lock()
	flow.presentURL = value

	if text, ok := authDisplayText(event.Instructions, authMaxMessageBytes); ok {
		flow.presentMessage = text
		flow.presentMessageNative = true
	}
	p.mu.Unlock()
}

// recordDeviceCode folds a native device-code presentation into the flow. It
// completes the presentation on its own: pi's device flows poll internally and
// ask the owner for nothing further.
func (p *providerAuth) recordDeviceCode(flow *authFlow, event pi.AuthNativeEvent) {
	value, ok := authDisplayURL(event.VerificationURI)
	if !ok {
		p.veto(flow, authCauseNativeVeto)

		return
	}

	code, ok := authDisplayUserCode(event.UserCode)
	if !ok {
		p.veto(flow, authCauseNativeVeto)

		return
	}

	p.mu.Lock()
	flow.presentInteraction = authInteractionWait
	flow.presentURL = value
	flow.presentUserCode = code

	if event.IntervalSeconds > 0 {
		flow.presentPollMs = event.IntervalSeconds * 1000
	}

	if event.ExpiresIn > 0 {
		native := authNow().Add(time.Duration(event.ExpiresIn) * time.Second)
		if native.Before(flow.expiresAt) {
			flow.expiresAt = native
		}
	}
	p.mu.Unlock()

	p.markReady(flow)
}

// authorize starts exactly one flow per (sessionId, providerId). It records the
// idempotency key before any native mint and has persisted the flow's slot
// binding before it returns.
func (p *providerAuth) authorize(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params,
		authFieldSessionID, authFieldProviderID, authFieldConnectionID,
		authFieldMethodsGeneration, authFieldMethod, authFieldAuthorizeRequestID, authFieldInputs)
	if err != nil {
		return nil, err
	}

	request, err := decodeAuthorizeRequest(fields)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(request.sessionID)
	if err != nil {
		return nil, err
	}

	key := authFlowKey{sessionID: session.id, providerID: request.providerID}

	if replay, ok := p.replayAuthorize(key, request.authorizeRequestID); ok {
		return replay, nil
	}

	method, err := p.resolveMethod(request)
	if err != nil {
		return nil, err
	}

	if inputErr := validateAuthInputs(request.inputs); inputErr != nil {
		return nil, inputErr
	}

	flowID, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	p.supersede(ctx, key, authReasonSuperseded)

	now := authNow()
	record := authLedgerRecord{
		ProviderID:         request.providerID,
		ConnectionID:       request.connectionID,
		Revision:           1,
		BindingGeneration:  1,
		FlowID:             flowID,
		AuthorizeRequestID: request.authorizeRequestID,
		State:              authLedgerIntent,
		CreatedAt:          now.UnixMilli(),
		UpdatedAt:          now.UnixMilli(),
	}

	if prior, ok, readErr := p.ledger.read(request.providerID); readErr == nil && ok {
		record.Revision = prior.Revision + 1
		record.BindingGeneration = prior.BindingGeneration
		record.CreatedAt = prior.CreatedAt
	}

	if writeErr := p.ledger.write(record); writeErr != nil {
		return nil, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	flow := &authFlow{
		id:                 flowID,
		sessionID:          session.id,
		providerID:         request.providerID,
		connectionID:       request.connectionID,
		revision:           record.Revision,
		bindingGeneration:  record.BindingGeneration,
		method:             method,
		authorizeRequestID: request.authorizeRequestID,
		createdAt:          record.CreatedAt,
		state:              authStatePending,
		// The method label stands in until the native flow supplies its own
		// presentation text; pi's device flows supply none.
		presentMessage: method.Label,
		expiresAt:      now.Add(authSafetyDeadline),
		ready:          make(chan struct{}),
		result:         make(chan pi.AuthMessage, 1),
		disarm:         make(chan struct{}),
	}

	p.mu.Lock()
	p.flows[key] = flow
	p.byID[flowID] = flow
	p.mu.Unlock()

	presentation, err := p.mintPresentation(ctx, session, flow)
	if err != nil {
		p.discard(ctx, flow)

		return nil, err
	}

	p.mu.Lock()
	flow.presentation = presentation
	p.mu.Unlock()

	p.armCompleter(flow)

	return presentation, nil
}

type authorizeRequest struct {
	sessionID    string
	providerID   string
	connectionID string
	generation   string
	method       string
	// authorizeRequestID is the caller-minted idempotency key. authorize is the
	// only leg that takes one because it is the most destructive leg here.
	authorizeRequestID string
	inputs             map[string]string
}

func decodeAuthorizeRequest(fields map[string]json.RawMessage) (authorizeRequest, error) {
	request := authorizeRequest{}

	var err error
	if request.sessionID, err = authRequiredString(fields, authFieldSessionID); err != nil {
		return request, err
	}

	if request.providerID, err = authRequiredString(fields, authFieldProviderID); err != nil {
		return request, err
	}

	if request.connectionID, err = authRequiredString(fields, authFieldConnectionID); err != nil {
		return request, err
	}

	if request.generation, err = authRequiredString(fields, authFieldMethodsGeneration); err != nil {
		return request, err
	}

	if request.method, err = authRequiredString(fields, authFieldMethod); err != nil {
		return request, err
	}

	if request.authorizeRequestID, err = authRequiredString(fields, authFieldAuthorizeRequestID); err != nil {
		return request, err
	}

	if raw, ok := fields[authFieldInputs]; ok {
		if err := json.Unmarshal(raw, &request.inputs); err != nil {
			return request, invalidAuthField(authFieldInputs)
		}
	}

	return request, nil
}

// replayAuthorize answers a repeated idempotency key verbatim from memory: no
// supersede, no completer disarm, no destruction of flow state, and no native
// call.
func (p *providerAuth) replayAuthorize(key authFlowKey, requestID string) (authAuthorizeResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow, ok := p.flows[key]
	if !ok || flow.authorizeRequestID != requestID {
		return authAuthorizeResult{}, false
	}

	return flow.presentation, true
}

// resolveMethod fences a method id against the generation that produced it. A
// method id means nothing against a catalog this adapter no longer holds.
func (p *providerAuth) resolveMethod(request authorizeRequest) (authCatalogMethod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.generation == "" || p.generation != request.generation {
		return authCatalogMethod{}, invalidAuthField(authFieldMethodsGeneration)
	}

	for _, method := range p.catalog[request.providerID] {
		if method.ID == request.method {
			return method, nil
		}
	}

	return authCatalogMethod{}, invalidAuthField(authFieldMethod)
}

// mintPresentation starts the native login for an oauth method and builds the
// wire presentation. An api-key method has nothing to mint: its value is
// submitted through callback and applied natively there.
func (p *providerAuth) mintPresentation(ctx context.Context, session *agentSession, flow *authFlow) (authAuthorizeResult, error) {
	if flow.method.Type == authMethodTypeAPI {
		return authAuthorizeResult{
			Interaction:   authInteractionSecret,
			Message:       flow.method.Label,
			FlowID:        flow.id,
			FlowExpiresAt: flow.expiresAt.UnixMilli(),
		}, nil
	}

	p.startLogin(session, flow, pi.AuthMethodOAuth)

	if err := p.awaitPresentation(ctx, flow); err != nil {
		return authAuthorizeResult{}, err
	}

	return p.publishPresentation(flow)
}

// publishPresentation converts the folded native facts into the wire
// presentation, failing closed when the flow never produced a renderable one.
func (p *providerAuth) publishPresentation(flow *authFlow) (authAuthorizeResult, error) {
	p.mu.Lock()

	cause := flow.nativeCause
	interaction := flow.presentInteraction
	result := authAuthorizeResult{
		Interaction:    interaction,
		URL:            flow.presentURL,
		Message:        flow.presentMessage,
		UserCode:       flow.presentUserCode,
		FlowID:         flow.id,
		FlowExpiresAt:  flow.expiresAt.UnixMilli(),
		PollIntervalMs: flow.presentPollMs,
	}
	p.mu.Unlock()

	if cause != "" {
		return authAuthorizeResult{}, authFailed(cause, flow.providerID, flow.method.ID, flow.id)
	}

	if interaction == "" || result.URL == "" || result.Message == "" {
		return authAuthorizeResult{}, authFailed(authCauseNativeVeto, flow.providerID, flow.method.ID, flow.id)
	}

	if interaction == authInteractionCallback {
		result.CallbackInput = authCallbackInputCode
	}

	return result, nil
}

// armCompleter bounds the flow by its effective deadline. It is armed exactly
// once, at authorize, and status never starts, extends, or rearms it.
func (p *providerAuth) armCompleter(flow *authFlow) {
	p.mu.Lock()
	deadline := time.Until(flow.expiresAt)
	p.mu.Unlock()

	disarm := flow.disarm

	p.goSafe("provider auth completer", func() {
		timer := time.NewTimer(deadline)
		defer timer.Stop()

		select {
		case <-disarm:
			return
		case <-timer.C:
			p.expire(flow)
		}
	})
}

func (p *providerAuth) expire(flow *authFlow) {
	p.mu.Lock()

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return
	}

	flow.state = authStateExpired
	flow.reason = authReasonDeadline

	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), authNativeCallTimeoutValue)
	defer cancel()

	p.cancelParked(ctx, flow)
}

// discard drops a flow that never became presentable, leaving nothing a later
// leg could address.
func (p *providerAuth) discard(ctx context.Context, flow *authFlow) {
	p.mu.Lock()
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	delete(p.byID, flow.id)
	flow.state = authStateFailed
	p.mu.Unlock()

	p.cancelParked(ctx, flow)
}

// supersede terminalizes the flow a new authorize replaces, dismissing the
// native prompt it left open so the abandoned login cannot complete later.
func (p *providerAuth) supersede(ctx context.Context, key authFlowKey, reason string) {
	p.mu.Lock()

	flow, ok := p.flows[key]
	if !ok {
		p.mu.Unlock()

		return
	}

	delete(p.flows, key)
	delete(p.byID, flow.id)

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return
	}

	flow.state = authStateCancelled
	flow.reason = reason

	stopAuthCompleter(flow)
	p.mu.Unlock()

	p.cancelParked(ctx, flow)
}

func stopAuthCompleter(flow *authFlow) {
	select {
	case <-flow.disarm:
	default:
		close(flow.disarm)
	}
}

// callback submits the flow's expected value. For an api-key flow it starts the
// native login and answers its credential prompt; for an oauth flow it answers
// the parked manual-code prompt. Either way it returns only after the native
// side reports a terminal outcome.
func (p *providerAuth) callback(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldMethod, authFieldFlowID, authFieldInput)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	method, err := authRequiredString(fields, authFieldMethod)
	if err != nil {
		return nil, err
	}

	flowID, err := authRequiredString(fields, authFieldFlowID)
	if err != nil {
		return nil, err
	}

	input, err := authString(fields, authFieldInput)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	flow, err := p.addressFlow(session.id, providerID, flowID)
	if err != nil {
		return nil, err
	}

	if flow.method.ID != method {
		return nil, invalidAuthField(authFieldMethod)
	}

	if p.flowState(flow) != authStatePending {
		return nil, authFailed(authCauseFlowState, providerID, method, flowID)
	}

	if err := validateAuthSecret(input); err != nil {
		return nil, err
	}

	if flow.method.Type == authMethodTypeAPI {
		return p.applySecret(ctx, session, flow, input)
	}

	return p.completeOAuth(ctx, session, flow, input)
}

// applySecret starts the native api-key login and answers its credential prompt
// with the submitted value. No harness validates a secret at write time, so the
// flow reaches saved rather than authenticated.
func (p *providerAuth) applySecret(ctx context.Context, session *agentSession, flow *authFlow, input string) (any, error) {
	p.mu.Lock()
	flow.pendingSecret = input
	p.mu.Unlock()

	p.startLogin(session, flow, pi.AuthMethodAPI)

	return p.settle(ctx, flow, authStateSaved)
}

func (p *providerAuth) completeOAuth(ctx context.Context, session *agentSession, flow *authFlow, input string) (any, error) {
	if !p.answerParked(ctx, session, flow, input) {
		return nil, authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	return p.settle(ctx, flow, authStateAuthenticated)
}

// settle waits for the native terminal answer, records the post-mutation
// confirmation, and terminalizes the flow.
func (p *providerAuth) settle(ctx context.Context, flow *authFlow, success string) (any, error) {
	message, err := p.awaitResult(ctx, flow)
	if err != nil {
		p.terminalize(flow, authStateFailed, authReasonAcceptanceUnknown, 0)

		return nil, err
	}

	if veto := p.flowVeto(flow); veto != "" {
		return nil, p.fail(flow, veto, true)
	}

	if !message.OK {
		return nil, p.fail(flow, authNativeCause(message.Cause), true)
	}

	if err := p.confirm(flow); err != nil {
		return nil, err
	}

	p.terminalize(flow, success, "", message.Expires)

	return authFlowIDResult{FlowID: flow.id}, nil
}

// confirm records the post-mutation confirmation that binds the resident
// credential to this connection generation.
func (p *providerAuth) confirm(flow *authFlow) error {
	record := authLedgerRecord{
		ProviderID:         flow.providerID,
		ConnectionID:       flow.connectionID,
		Revision:           flow.revision,
		BindingGeneration:  flow.bindingGeneration,
		FlowID:             flow.id,
		AuthorizeRequestID: flow.authorizeRequestID,
		State:              authLedgerConfirmed,
		CreatedAt:          flow.createdAt,
		UpdatedAt:          authNow().UnixMilli(),
	}

	if err := p.ledger.write(record); err != nil {
		return p.fail(flow, authCauseProcess, true)
	}

	return nil
}

// authNativeCause maps the bridge's closed cause tag onto the leg's cause enum
// without forwarding any native text.
func authNativeCause(cause string) string {
	switch cause {
	case authCauseNativeVeto, authCauseHarvestFailed, authCauseProcess:
		return cause
	default:
		return authCauseProviderRefused
	}
}

// fail returns the leg's closed error and performs the transition its cause
// pairs with. A cause with no transition consumes nothing.
func (p *providerAuth) fail(flow *authFlow, cause string, materialInFlight bool) error {
	if state, reason := authFlowTransition(cause, materialInFlight); state != "" {
		p.terminalize(flow, state, reason, 0)
	}

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}

func (p *providerAuth) terminalize(flow *authFlow, state string, reason string, credentialExpiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow.state = state
	flow.reason = reason
	flow.credentialExpiresAt = credentialExpiresAt

	stopAuthCompleter(flow)
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
}

func (p *providerAuth) flowState(flow *authFlow) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return flow.state
}

// addressFlow resolves a flowId a caller supplied. A missing, unknown,
// superseded, or cross-session id is a caller addressing failure and never a
// flow failure.
func (p *providerAuth) addressFlow(sessionID acp.SessionId, providerID string, flowID string) (*authFlow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow, ok := p.byID[flowID]
	if !ok || flow.sessionID != sessionID || flow.providerID != providerID {
		return nil, invalidAuthField(authFieldFlowID)
	}

	return flow, nil
}

// status reports the flow, not the connection. pi exposes no native poll route
// for a login already under way — its own callback loop is the completion
// signal — so the leg serves the cached record behind the family interval floor
// rather than inventing a poll of its own. Its expiresAt is credential expiry
// and never flow expiry.
func (p *providerAuth) status(_ context.Context, params json.RawMessage) (any, error) {
	flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	result := authStatusResult{FlowID: flow.id, State: flow.state, Reason: flow.reason}
	if flow.state == authStateAuthenticated {
		result.ExpiresAt = flow.credentialExpiresAt
	}

	return result, nil
}

// cancel is adapter-owned: pi has no native flow-cancel route, so the leg does
// everything the adapter owns and claims nothing about the provider. An issued
// device code stays valid there until it expires.
func (p *providerAuth) cancel(ctx context.Context, params json.RawMessage) (any, error) {
	flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return authFlowIDResult{FlowID: flow.id}, nil
	}

	flow.state = authStateCancelled
	flow.reason = authReasonOwnerCancel

	stopAuthCompleter(flow)
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	p.cancelParked(ctx, flow)

	return authFlowIDResult{FlowID: flow.id}, nil
}

func (p *providerAuth) addressedFlowLeg(params json.RawMessage) (*authFlow, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldFlowID)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	flowID, err := authRequiredString(fields, authFieldFlowID)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	return p.addressFlow(session.id, providerID, flowID)
}

// disconnect bumps the binding generation before it touches anything else, then
// removes only the exactly-fenced slot and verifies absence. pi's removal is a
// per-provider store call, so it never reaches an entry a different connection
// owns and promises no provider-side revocation.
func (p *providerAuth) disconnect(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldConnectionID, authFieldBindingGeneration)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	connectionID, err := authRequiredString(fields, authFieldConnectionID)
	if err != nil {
		return nil, err
	}

	bindingGeneration, err := authRequiredInt64(fields, authFieldBindingGeneration)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	record, ok, err := p.ledger.read(providerID)
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	if !ok || record.ConnectionID != connectionID || record.BindingGeneration != bindingGeneration {
		return nil, authFailed(authCausePolicy, providerID, "", "")
	}

	record.BindingGeneration++
	record.UpdatedAt = authNow().UnixMilli()
	record.State = authLedgerIntent

	if writeErr := p.ledger.write(record); writeErr != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	removal, err := p.exchange(ctx, session, authBridgeRequest{Op: authOpRemove, ProviderID: providerID})
	if err != nil || !removal.OK {
		return nil, authFailed(authCauseTransport, providerID, "", "")
	}

	resident, err := p.probeSlots(ctx, session, []string{providerID})
	if err != nil {
		return nil, err
	}

	if _, present := resident[providerID]; present {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	record.State = authLedgerRemoved
	record.UpdatedAt = authNow().UnixMilli()

	if writeErr := p.ledger.write(record); writeErr != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	return struct{}{}, nil
}

// closeSession cancels every pending flow the session owns, terminalizing each
// as cancelled/session_closed and dismissing the native prompt it left open. It
// runs before the native interrupt, so a flow is never abandoned to a process
// already being torn down.
func (p *providerAuth) closeSession(ctx context.Context, sessionID acp.SessionId) {
	p.mu.Lock()

	orphaned := make([]*authFlow, 0, len(p.flows))

	for key, flow := range p.flows {
		if key.sessionID != sessionID {
			continue
		}

		delete(p.flows, key)
		delete(p.byID, flow.id)

		if authTerminal(flow.state) {
			continue
		}

		flow.state = authStateCancelled
		flow.reason = authReasonSessionClosed

		stopAuthCompleter(flow)
		orphaned = append(orphaned, flow)
	}

	p.mu.Unlock()

	for _, flow := range orphaned {
		p.cancelParked(ctx, flow)
	}
}
