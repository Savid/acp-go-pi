package piacp

import (
	"context"
	"sync"
)

// authGate serialises the legs that would otherwise interleave inside one
// check-then-set over the same thing. One key admits one holder at a time; the
// rest wait their turn or give up with their caller's context, so a gate never
// outlives the request that is waiting on it.
//
// Entries are reference-counted rather than retained: a key nobody holds or
// waits for is deleted, so the map cannot grow with every session or provider
// this agent ever saw, and no holder can be let past by an entry somebody else
// dropped while it still held one.
type authGate[K comparable] struct {
	mu    sync.Mutex
	slots map[K]*authGateSlot
}

// authGateSlot is one key's turn. pending counts the holder and the waiters
// together, which is what decides when the entry may be dropped.
type authGateSlot struct {
	pending int
	token   chan struct{}
}

func newAuthGate[K comparable]() *authGate[K] {
	return &authGate[K]{slots: make(map[K]*authGateSlot)}
}

// enter takes the key's turn and returns the release its holder defers,
// reporting false when the caller's context ended first.
func (g *authGate[K]) enter(ctx context.Context, key K) (func(), bool) {
	g.mu.Lock()

	slot, ok := g.slots[key]
	if !ok {
		slot = &authGateSlot{token: make(chan struct{}, 1)}
		g.slots[key] = slot
	}

	slot.pending++
	g.mu.Unlock()

	select {
	case slot.token <- struct{}{}:
		return func() {
			<-slot.token

			g.drop(key)
		}, true
	case <-ctx.Done():
		g.drop(key)

		return nil, false
	}
}

func (g *authGate[K]) drop(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()

	slot := g.slots[key]

	slot.pending--
	if slot.pending == 0 {
		delete(g.slots, key)
	}
}

// admitAuthorize holds the (sessionId, providerId) key across the whole
// authorize admission: the replay answer, the retirement check, the supersede,
// the ledger intent, the publication, and the mint. Two identical authorize
// requests arrive on two goroutines and each misses the other's flow, so
// without this both mint one — which is the single thing the idempotency key
// exists to prevent. The hold ends when the mint settles, and the mint's own
// wait is the presentation wait bounded by authNativeCallTimeoutValue, never
// the login bounded by authLoginTimeoutValue, so the longest a second authorize
// waits here is one native call.
func (p *providerAuth) admitAuthorize(ctx context.Context, key authFlowKey, method string) (func(), error) {
	release, ok := p.authorizeGate.enter(ctx, key)
	if !ok {
		return nil, authFailed(authCauseTimeout, key.providerID, method, "")
	}

	return release, nil
}

// admitSlot holds the provider's credential slot across every sequence that
// rewrites it: disconnect's read, generation bump, native removal, absence
// verification and removed-record, and a completion's lineage check, native
// write, and confirmation. Interleaved, those two sequences lose each other's
// update — a disconnect that verified absence leaves the ledger reading removed
// over a credential the completion installed behind it, which inventory skips
// and no later disconnect can fence.
func (p *providerAuth) admitSlot(ctx context.Context, providerID string, method string, flowID string) (func(), error) {
	release, ok := p.slotGate.enter(ctx, providerID)
	if !ok {
		return nil, authFailed(authCauseTimeout, providerID, method, flowID)
	}

	return release, nil
}

// claimFlow admits one leg to drive the flow's native login. The terminal check
// and the claim are one critical section on purpose: a leg that reads pending,
// drops the lock and only then drives the login hands the next leg the same
// pending answer, and both drive one. Nothing about that is a data race — every
// field access is itself locked, so -race reports nothing — and in pi the
// second leg registers its bridge exchange under the flow id the first already
// used, overwriting it, after which either leg's release deletes an exchange it
// never opened and the dialog router can no longer find the exchange the
// extension is answering. Under the claim, startLogin's registration is
// single-writer by construction.
func (p *providerAuth) claimFlow(flow *authFlow) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) || flow.claimed {
		return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	flow.claimed = true

	return nil
}

// releaseFlow ends the claim. A flow that terminalized while the claimant held
// it refuses the next claim anyway, so releasing after success is harmless and
// keeps every claiming path the same shape.
func (p *providerAuth) releaseFlow(flow *authFlow) {
	p.mu.Lock()
	flow.claimed = false
	p.mu.Unlock()
}

// publishFlow makes the flow addressable and is the authoritative admission
// check against a session that closed while this authorize ran. closeSession
// marks the session in the same critical section in which it takes the flows it
// is about to cancel, so a flow published before that runs is in its cleanup
// set and a flow published after it is refused outright.
//
// Deliberately no drain: making session/close wait for the legs already in
// flight would block it for the length of a native call it cannot bound, and
// refusing publication yields the same invariant — no flow escapes close's
// cleanup set — without that. authorize publishes before it mints, so a refusal
// here abandons no native login.
func (p *providerAuth) publishFlow(session *agentSession, key authFlowKey, flow *authFlow) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if session.authClosed {
		return unknownSessionError()
	}

	p.flows[key] = flow
	p.byID[flow.id] = flow
	p.retained[key] = flow

	return nil
}

// retire records an authorizeRequestId the broker can no longer answer. The
// caller holds the mutex.
func (p *providerAuth) retire(key authFlowKey, requestID string) {
	keys, ok := p.retired[key]
	if !ok {
		keys = make(map[string]struct{})
		p.retired[key] = keys
	}

	keys[requestID] = struct{}{}
}

// requestRetired reports whether the key names a request a later authorize
// already replaced. Only the newest record is replayable, so an older key is
// unanswerable — and minting in its place would cancel the live flow it never
// named and show the owner a second code, which is the one thing an idempotency
// key exists to prevent.
func (p *providerAuth) requestRetired(key authFlowKey, requestID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, retired := p.retired[key][requestID]

	return retired
}

// lineageCurrent re-reads the provider's ledger entry before a completion
// drives the native write it will go on to confirm. confirm's compare-and-set
// afterwards refuses a stale confirmation correctly, but by then pi holds the
// credential: the entry says removed, inventory skips removed, and the
// credential is live and invisible on every host surface with no later
// disconnect able to fence it. The caller holds the provider's slot, so what
// this reads cannot move under it before the write it guards.
func (p *providerAuth) lineageCurrent(flow *authFlow) error {
	record, ok, err := p.ledger.read(flow.providerID)
	if err != nil {
		return p.fail(flow, authCauseProcess, false)
	}

	if ok && (record.ConnectionID != flow.connectionID ||
		record.Revision != flow.revision ||
		record.BindingGeneration != flow.bindingGeneration) {
		return authFailed(authCauseBindingConflict, flow.providerID, flow.method.ID, flow.id)
	}

	return nil
}
