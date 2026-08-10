package piacp

import (
	"context"
	"errors"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// Bridge operations, mirroring the extension's request vocabulary.
const (
	authOpCatalog = pi.AuthOpCatalog
	authOpProbe   = pi.AuthOpProbe
	authOpLogin   = pi.AuthOpLogin
	authOpRemove  = pi.AuthOpRemove
)

// errAuthBridge reports that one bridge exchange did not produce its answer.
var errAuthBridge = errors.New("provider auth bridge exchange failed")

// authBridgeRequest is one request the wrapper drives into the bridge
// extension through the pi prompt command.
type authBridgeRequest struct {
	Op          string
	ProviderID  string
	Method      string
	ProviderIDs []string
}

// authExchange is one in-flight bridge command. Every marker dialog the
// extension raises names the exchange it belongs to, so a dialog whose id
// matches nothing this adapter started is cancelled rather than answered.
type authExchange struct {
	id     string
	flow   *authFlow
	answer chan pi.AuthMessage
}

func (p *providerAuth) registerExchange(id string, flow *authFlow) *authExchange {
	exchange := &authExchange{id: id, flow: flow, answer: make(chan pi.AuthMessage, 1)}

	p.mu.Lock()
	p.exchanges[id] = exchange
	p.mu.Unlock()

	return exchange
}

func (p *providerAuth) releaseExchange(id string) {
	p.mu.Lock()
	delete(p.exchanges, id)
	p.mu.Unlock()
}

func (p *providerAuth) lookupExchange(id string) *authExchange {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.exchanges[id]
}

// exchange runs one bridge command that answers with a single message and
// returns it. The command is an extension command, so pi runs it to completion
// and acknowledges without starting a model turn, even mid-stream.
func (p *providerAuth) exchange(ctx context.Context, session *agentSession, request authBridgeRequest) (pi.AuthMessage, error) {
	id, err := newAuthToken()
	if err != nil {
		return pi.AuthMessage{}, err
	}

	exchange := p.registerExchange(id, nil)
	defer p.releaseExchange(id)

	callCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeoutValue)
	defer cancel()

	if err := p.invoke(callCtx, session, id, request); err != nil {
		return pi.AuthMessage{}, err
	}

	// The extension answers on the pump, which runs independently of the leg
	// that sent the command, so the answer is waited for under the call's own
	// bound rather than sampled the instant the command is acknowledged.
	select {
	case message := <-exchange.answer:
		return message, nil
	case <-callCtx.Done():
		return pi.AuthMessage{}, errAuthBridge
	}
}

// invoke sends one /acp-auth command to the session's live pi process.
func (p *providerAuth) invoke(ctx context.Context, session *agentSession, id string, request authBridgeRequest) error {
	client := session.currentClient()
	if client == nil {
		return errAuthBridge
	}

	text := pi.EncodeAuthCommand(pi.AuthRequest{
		ID:          id,
		Op:          request.Op,
		ProviderID:  request.ProviderID,
		Method:      request.Method,
		ProviderIDs: request.ProviderIDs,
	})

	if err := client.Prompt(ctx, text, nil); err != nil {
		return errAuthBridge
	}

	return nil
}

// handleAuthDialog answers one bridge provider-auth dialog. Flows run outside
// any turn, so this runs off the pump directly rather than through the live
// turn sink. Exactly one response is always written back so the extension never
// hangs — except for the parked manual-code prompt, which is the callback leg's
// to answer.
func (p *providerAuth) handleAuthDialog(ctx context.Context, session *agentSession, request pi.UIRequest) {
	message, ok := pi.ParseAuthTitle(request.Title)
	if !ok {
		session.respondUIDialog(ctx, pi.UICancelResponse(request.ID))

		return
	}

	exchange := p.lookupExchange(message.ID)
	if exchange == nil {
		session.respondUIDialog(ctx, pi.UICancelResponse(request.ID))

		return
	}

	switch message.Kind {
	case pi.AuthKindCatalog, pi.AuthKindProbe:
		p.deliver(exchange, message)
		session.respondUIDialog(ctx, pi.UIValueResponse(request.ID, pi.AuthAck))
	case pi.AuthKindResult:
		p.deliverResult(exchange, message)
		session.respondUIDialog(ctx, pi.UIValueResponse(request.ID, pi.AuthAck))
	case pi.AuthKindEvent:
		p.recordEvent(ctx, session, exchange, message, request.ID)
	case pi.AuthKindPrompt:
		p.answerPrompt(ctx, session, exchange, message, request.ID)
	case pi.AuthKindCancel:
		p.armAbort(ctx, session, exchange, request.ID)
	default:
		session.respondUIDialog(ctx, pi.UICancelResponse(request.ID))
	}
}

func (p *providerAuth) deliver(exchange *authExchange, message pi.AuthMessage) {
	select {
	case exchange.answer <- message:
	default:
	}
}

// deliverResult routes a terminal login answer to the flow waiting on it. A
// result for a plain command exchange has no flow and answers that instead.
func (p *providerAuth) deliverResult(exchange *authExchange, message pi.AuthMessage) {
	if exchange.flow == nil {
		p.deliver(exchange, message)

		return
	}

	select {
	case exchange.flow.result <- message:
	default:
	}

	p.markDecidable(exchange.flow)
}

// recordEvent folds one native presentation event into the flow. Only the two
// event types that carry a presentation are read; a progress or info message
// can name a loopback listener or an absolute path and never crosses.
func (p *providerAuth) recordEvent(
	ctx context.Context,
	session *agentSession,
	exchange *authExchange,
	message pi.AuthMessage,
	dialogID string,
) {
	flow := exchange.flow
	if flow == nil || message.Event == nil {
		session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, pi.AuthAck))

		return
	}

	switch message.Event.Type {
	case authNativeEventAuthURL:
		p.recordAuthURL(flow, *message.Event)
	case authNativeEventDeviceCode:
		p.recordDeviceCode(flow, *message.Event)
	}

	// A vetoed presentation cancels the dialog: the native flow aborts rather
	// than continuing toward a completion nothing will accept.
	if p.flowVeto(flow) != "" {
		session.respondUIDialog(ctx, pi.UICancelResponse(dialogID))

		return
	}

	session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, pi.AuthAck))
}

// armAbort records the dialog whose answer aborts the native login. It is the
// one dialog this adapter leaves open: while the flow is pending the login must
// keep running, and answering it later is what stops a device poll a terminal
// flow can no longer report on.
func (p *providerAuth) armAbort(ctx context.Context, session *agentSession, exchange *authExchange, dialogID string) {
	flow := exchange.flow
	if flow == nil {
		session.respondUIDialog(ctx, pi.UICancelResponse(dialogID))

		return
	}

	p.mu.Lock()
	terminal := authTerminal(flow.state)

	if !terminal {
		flow.abortDialog = dialogID
	}
	p.mu.Unlock()

	// A flow that terminalized before its abort handle arrived would keep the
	// login running with nothing left to stop it.
	if terminal {
		session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, pi.AuthAck))
	}
}

// answerPrompt answers one native login prompt. pi's provider enumeration
// exposes no prompt schema, so the catalog declares no prompts, `authorize`
// carries no `inputs`, and this adapter holds no answer a host authorized. What
// it answers with is therefore fixed per prompt type and supplies no value of
// its own:
//
//   - a manual-code prompt is parked for the callback leg;
//   - the flow's own submitted secret answers the single credential prompt of
//     an api-key login;
//   - a login-variant select is answered with its headless branch, the only
//     branch whose completion does not land on a socket the owner's browser
//     cannot reach;
//   - a text prompt is answered with the flow's submitted secret while one is
//     still unspent, and empty otherwise. Every api-key login in pi's catalog
//     that asks a text question asks its credential question first, so by then
//     the secret is spent and the text question declines onto whatever default
//     the login already has — for the one text prompt in pi's oauth catalog,
//     the vendor's own host rather than a customer-chosen one. Nothing here
//     enforces that ordering: a login that asked a text question before its
//     credential question would be answered with the credential;
//   - every other prompt fails the flow closed.
func (p *providerAuth) answerPrompt(
	ctx context.Context,
	session *agentSession,
	exchange *authExchange,
	message pi.AuthMessage,
	dialogID string,
) {
	flow := exchange.flow
	if flow == nil {
		session.respondUIDialog(ctx, pi.UICancelResponse(dialogID))

		return
	}

	switch message.Prompt {
	case pi.AuthPromptManualCode:
		if !p.park(flow, dialogID, message.Message) {
			session.respondUIDialog(ctx, pi.UICancelResponse(dialogID))
		}

		return
	case pi.AuthPromptText:
		answer, _ := p.takeSecret(flow)
		session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, answer))

		return
	case pi.AuthPromptSecret:
		if secret, ok := p.takeSecret(flow); ok {
			session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, secret))

			return
		}
	case pi.AuthPromptSelect:
		if option, ok := authHeadlessOption(message.Options); ok {
			session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, option))

			return
		}
	}

	p.veto(flow, authCauseNativeVeto)
	session.respondUIDialog(ctx, pi.UICancelResponse(dialogID))
}

// park records the dialog the callback leg answers and makes the flow's
// presentation decidable, reporting whether the flow took it. A flow that
// already terminalized takes none: its release has run, so a prompt recorded
// now would stay open for the life of the process with nothing left to answer
// it, and the caller dismisses it instead.
func (p *providerAuth) park(flow *authFlow, dialogID string, message string) bool {
	p.mu.Lock()

	taken := !authTerminal(flow.state)
	if taken {
		flow.parkedDialog = dialogID
		flow.presentInteraction = authInteractionCallback

		if text, ok := authDisplayText(message, authMaxMessageBytes); ok && !flow.presentMessageNative {
			flow.presentMessage = text
			flow.presentMessageNative = true
		}
	}
	p.mu.Unlock()

	p.markDecidable(flow)

	return taken
}

func (p *providerAuth) takeSecret(flow *authFlow) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	secret := flow.pendingSecret
	if secret == "" {
		return "", false
	}

	flow.pendingSecret = ""

	return secret, true
}

func (p *providerAuth) veto(flow *authFlow, cause string) {
	p.mu.Lock()
	if flow.nativeCause == "" {
		flow.nativeCause = cause
	}
	p.mu.Unlock()

	p.markDecidable(flow)
}

func (p *providerAuth) flowVeto(flow *authFlow) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return flow.nativeCause
}

// markDecidable reports that the native side has said enough for the leg to
// publish or refuse a presentation.
func (p *providerAuth) markDecidable(flow *authFlow) {
	flow.decidableOnce.Do(func() { close(flow.decidable) })
}

// markReady reports that the mint has settled either way, which is what an
// idempotent repeat waits on before it replays.
func (p *providerAuth) markReady(flow *authFlow) {
	flow.readyOnce.Do(func() { close(flow.ready) })
}

// startLogin drives one native login through the bridge. It runs off the leg
// that started it because a manual-code flow stays open across two legs: the
// presentation is answered by authorize and the code arrives on callback.
//
// The exchange is keyed by the flow id, which makes this a single-writer
// registration only because every leg that can reach it is serialized before it
// gets here: authorize is the sole caller for an oauth flow and holds that
// key's admission, callback is the sole caller for an api-key flow and holds
// the flow's claim. Two admitted legs would register two exchanges under one
// key, and the release deferred below would then delete whichever one is
// current rather than its own.
func (p *providerAuth) startLogin(session *agentSession, flow *authFlow, method string) {
	exchange := p.registerExchange(flow.id, flow)

	p.goSafe("provider auth login", func() {
		defer p.releaseExchange(flow.id)

		ctx, cancel := context.WithTimeout(context.Background(), authLoginTimeoutValue)
		defer cancel()

		err := p.invoke(ctx, session, flow.id, authBridgeRequest{
			Op:         authOpLogin,
			ProviderID: flow.providerID,
			Method:     method,
		})
		if err != nil {
			p.deliverResult(exchange, pi.AuthMessage{Kind: pi.AuthKindResult, Cause: authCauseProcess})
		}

		// A native flow that ends without reporting anything leaves the leg
		// waiting on a presentation that will never arrive.
		p.markDecidable(flow)
	})
}

// awaitPresentation waits for the flow to become decidable: a parked callback
// prompt, a device-code presentation, a veto, or a terminal result. It reports
// the cause the mint fails with, or the empty string once the flow is
// decidable.
func (p *providerAuth) awaitPresentation(ctx context.Context, flow *authFlow) string {
	select {
	case <-flow.decidable:
		return ""
	case <-ctx.Done():
		return authCauseTimeout
	case <-time.After(authNativeCallTimeoutValue):
		return authCauseTimeout
	}
}

// awaitResult waits for the terminal answer of a login already under way. A
// flow that terminalizes while the wait runs ends it: the owner has already
// closed the record, so there is nothing left for the native answer to decide.
func (p *providerAuth) awaitResult(ctx context.Context, flow *authFlow) (pi.AuthMessage, error) {
	select {
	case message := <-flow.result:
		return message, nil
	case <-flow.disarm:
		return pi.AuthMessage{}, authFailed(authCauseFlowCancelled, flow.providerID, flow.method.ID, flow.id)
	case <-ctx.Done():
		return pi.AuthMessage{}, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	case <-time.After(authLoginTimeoutValue):
		return pi.AuthMessage{}, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}
}

// answerParked hands the callback leg's value to the native prompt waiting on
// it and reports whether a prompt was waiting at all.
func (p *providerAuth) answerParked(ctx context.Context, session *agentSession, flow *authFlow, value string) bool {
	p.mu.Lock()
	dialogID := flow.parkedDialog
	flow.parkedDialog = ""
	p.mu.Unlock()

	if dialogID == "" {
		return false
	}

	session.respondUIDialog(ctx, pi.UIValueResponse(dialogID, value))

	return true
}

// releaseNativeLogin ends every hold a terminal flow still has on its native
// login: the prompt it left parked, and the login itself. Dismissing the prompt
// alone leaves a device flow polling — those flows park nothing — so an issued
// user code would stay approvable, and an approval landing after the flow ended
// would write a credential into pi's durable agent directory under a ledger
// entry no leg will ever confirm.
func (p *providerAuth) releaseNativeLogin(ctx context.Context, flow *authFlow) {
	p.mu.Lock()
	parked := flow.parkedDialog
	abort := flow.abortDialog
	flow.parkedDialog = ""
	flow.abortDialog = ""
	p.mu.Unlock()

	if parked == "" && abort == "" {
		return
	}

	session, err := p.agent.session(flow.sessionID)
	if err != nil {
		return
	}

	if parked != "" {
		session.respondUIDialog(ctx, pi.UICancelResponse(parked))
	}

	if abort != "" {
		session.respondUIDialog(ctx, pi.UIValueResponse(abort, pi.AuthAck))
	}
}
