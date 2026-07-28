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

	parkedDialog string
	// abortDialog is the dialog the bridge leaves open for the life of one
	// native login. Answering it aborts that login.
	abortDialog   string
	pendingSecret string

	// mintErr records why the native mint never produced a presentation, so a
	// repeated idempotency key is answered with the same failure rather than
	// starting a second login.
	mintErr error

	// decidable closes when the native side has said enough to publish or
	// refuse a presentation; ready closes once the mint has settled either way.
	decidable     chan struct{}
	decidableOnce sync.Once
	ready         chan struct{}
	readyOnce     sync.Once
	result        chan pi.AuthMessage

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

	p.markDecidable(flow)
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

	replay, replayed, err := p.replayAuthorize(ctx, key, request.authorizeRequestID)
	if replayed {
		if err != nil {
			return nil, err
		}

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
		decidable:      make(chan struct{}),
		ready:          make(chan struct{}),
		result:         make(chan pi.AuthMessage, 1),
		disarm:         make(chan struct{}),
	}

	// The flow is registered before the mint so the flowId a mint failure
	// reports addresses a real, terminal record, and so a repeat arriving mid
	// mint has something to wait on.
	p.mu.Lock()
	p.flows[key] = flow
	p.byID[flowID] = flow
	p.retained[key] = flow
	p.mu.Unlock()

	presentation, cause := p.mintPresentation(ctx, session, flow)
	if cause != "" {
		return nil, p.failMint(ctx, flow, cause)
	}

	p.mu.Lock()
	flow.presentation = presentation
	p.mu.Unlock()

	p.markReady(flow)
	p.armCompleter(flow)

	if presentation.Interaction == authInteractionWait {
		p.watchNativeCompletion(flow)
	}

	return presentation, nil
}

// watchNativeCompletion settles a wait flow from the native login itself. pi
// exposes no poll route, so status has nothing of its own to read: the login's
// own terminal answer is the only completion signal there is. Without something
// reading it a wait flow could only ever expire, while the credential it earned
// sat resident in pi's agent directory under a ledger entry stuck at intent.
func (p *providerAuth) watchNativeCompletion(flow *authFlow) {
	p.goSafe("provider auth wait completion", func() {
		_, _ = p.settle(context.Background(), flow, authStateAuthenticated)
	})
}

// failMint terminalizes a flow whose native mint never produced a presentation
// and records the failure the idempotency key replays.
func (p *providerAuth) failMint(ctx context.Context, flow *authFlow, cause string) error {
	err := p.fail(flow, cause, false)

	p.mu.Lock()
	flow.mintErr = err
	p.mu.Unlock()

	p.markReady(flow)
	p.releaseNativeLogin(ctx, flow)

	return err
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

	if request.connectionID, err = authRequiredConnectionID(fields); err != nil {
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
// call. It resolves the retained record rather than the pending one, so the
// answer survives the flow's terminalization for as long as the session lives,
// and it waits out a mint still under way rather than replaying a presentation
// nothing has published yet.
func (p *providerAuth) replayAuthorize(
	ctx context.Context,
	key authFlowKey,
	requestID string,
) (authAuthorizeResult, bool, error) {
	p.mu.Lock()
	flow, ok := p.retained[key]
	matched := ok && flow.authorizeRequestID == requestID
	p.mu.Unlock()

	if !matched {
		return authAuthorizeResult{}, false, nil
	}

	select {
	case <-flow.ready:
	case <-ctx.Done():
		return authAuthorizeResult{}, true, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if flow.mintErr != nil {
		return authAuthorizeResult{}, true, flow.mintErr
	}

	return flow.presentation, true, nil
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
// wire presentation, reporting the cause a failed mint fails with. An api-key
// method has nothing to mint: its value is submitted through callback and
// applied natively there.
func (p *providerAuth) mintPresentation(ctx context.Context, session *agentSession, flow *authFlow) (authAuthorizeResult, string) {
	if flow.method.Type == authMethodTypeAPI {
		return authAuthorizeResult{
			Interaction:   authInteractionSecret,
			Message:       flow.method.Label,
			FlowID:        flow.id,
			FlowExpiresAt: flow.expiresAt.UnixMilli(),
		}, ""
	}

	// An oauth mint is the one leg that makes pi reach for a browser. Without a
	// shim shadowing the launchers it would open a tab on the operator's desktop
	// against a URL the worker, not the operator, is authorizing.
	if session.browserShim == nil {
		return authAuthorizeResult{}, authCausePolicy
	}

	p.startLogin(session, flow, pi.AuthMethodOAuth)

	if cause := p.awaitPresentation(ctx, flow); cause != "" {
		return authAuthorizeResult{}, cause
	}

	return p.publishPresentation(flow)
}

// publishPresentation converts the folded native facts into the wire
// presentation, failing closed when the flow never produced a renderable one.
func (p *providerAuth) publishPresentation(flow *authFlow) (authAuthorizeResult, string) {
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
		return authAuthorizeResult{}, cause
	}

	if interaction == "" || result.URL == "" || result.Message == "" {
		return authAuthorizeResult{}, authCauseNativeVeto
	}

	if interaction == authInteractionCallback {
		result.CallbackInput = authCallbackInputCode
	}

	return result, ""
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

	// The completer is what fired, so closing its channel disarms nothing — it
	// is what makes the disarm channel a reliable report that the flow is
	// terminal, which is what every wait on a native answer selects on.
	stopAuthCompleter(flow)
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), authNativeCallTimeoutValue)
	defer cancel()

	p.releaseNativeLogin(ctx, flow)
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

	p.releaseNativeLogin(ctx, flow)
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

	// A secret pi accepted is resident whatever the flow did while the wait ran:
	// the value crossed at the prompt, and pi's write needs no provider exchange
	// a cancel could pre-empt. An oauth acceptance is not resident on those
	// terms, because its write waits on an exchange the owner's cancel aborts.
	resident := message.OK && flow.method.Type == authMethodTypeAPI

	// The wait a native answer costs is unbounded from the owner's side, so the
	// flow can have been cancelled, superseded, or expired while it ran. Such an
	// answer owns no transition and confirms nothing: it arrived into a record
	// somebody else already closed. A resident credential is the exception —
	// answering a no-transition cause over one the agent directory now holds
	// would leave it bound to nothing and hide it from every residence answer.
	// The exception carries the leg as far as confirm and no further: whether
	// its binding is still the one the provider's entry names is confirm's own
	// question, not this one's.
	if cause, abandoned := p.abandonedCause(flow); abandoned && !resident {
		return nil, authFailed(cause, flow.providerID, flow.method.ID, flow.id)
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
// credential to this connection generation. A leg that outlived its own flow
// still gets here, because a credential the agent directory now holds must not
// be left bound to nothing — but the entry may meanwhile have passed to the
// binding that replaced this one, and that binding owns it. The write is a
// compare-and-set on the lineage this flow was minted against, and a leg that
// loses it answers for the record somebody else already closed rather than
// renaming its own over the successor's.
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

	current, err := p.ledger.writeIfCurrent(record)
	if err != nil {
		return p.fail(flow, authCauseProcess, true)
	}

	if current {
		return nil
	}

	if cause, abandoned := p.abandonedCause(flow); abandoned {
		return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
	}

	return p.fail(flow, authCauseBindingConflict, true)
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

// abandonedCause reports the cause a leg answers with when the flow reached a
// terminal state while the native answer this leg waited on was still in
// flight. Neither answer carries a transition: the record is already closed.
func (p *providerAuth) abandonedCause(flow *authFlow) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch {
	case !authTerminal(flow.state):
		return "", false
	case flow.state == authStateCancelled:
		return authCauseFlowCancelled, true
	default:
		return authCauseFlowState, true
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

// terminalize records the flow's one terminal transition and releases the
// native login it still holds. A flow that already reached a terminal state
// keeps it: a native answer still in flight when the owner cancelled arrives
// into a record the owner already closed, and it is no longer the flow's
// outcome.
func (p *providerAuth) terminalize(flow *authFlow, state string, reason string, credentialExpiresAt int64) {
	p.mu.Lock()

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return
	}

	flow.state = state
	flow.reason = reason
	flow.credentialExpiresAt = credentialExpiresAt

	stopAuthCompleter(flow)
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), authNativeCallTimeoutValue)
	defer cancel()

	p.releaseNativeLogin(ctx, flow)
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

	p.releaseNativeLogin(ctx, flow)

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

	connectionID, err := authRequiredConnectionID(fields)
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
		return nil, authFailed(authCauseBindingConflict, providerID, "", "")
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

	if authSlotResident(resident, providerID) {
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
// already being torn down. It is also the only thing that drops a retained
// record: an idempotency key is answerable for as long as its session lives and
// answers nothing after it does not.
func (p *providerAuth) closeSession(ctx context.Context, sessionID acp.SessionId) {
	p.mu.Lock()

	orphaned := make([]*authFlow, 0, len(p.flows))

	for key := range p.retained {
		if key.sessionID == sessionID {
			delete(p.retained, key)
		}
	}

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
		p.releaseNativeLogin(ctx, flow)
	}
}
