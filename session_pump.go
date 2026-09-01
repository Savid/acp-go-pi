package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

// outboxQueueCapacity bounds the records one generation may hold while a cycle
// is settling. The bound exists so a settlement that never completes cannot
// grow the session without limit; reaching it is a containment failure rather
// than a reason to drop a record, so the generation is contained instead.
const outboxQueueCapacity = 256

// errGenerationRetired reports that the generation a producer was working on
// had already been claimed by its owner — a host close, a fence, or a
// containment that is already running. It is the answer to "who ends this
// generation", not a fault: the claiming owner installs the cause and runs the
// ladder, so a producer that reads this stops without contending for either.
var errGenerationRetired = errors.New("native generation was retired by its owner")

// turnDelivery carries one accepted prompt's foreground work from the session
// outbox to the prompt loop. events is closed by the outbox when the native
// generation ends; done is closed by the prompt loop when it stops reading, so
// the pump never blocks on a finished turn.
type turnDelivery struct {
	events     chan pi.Event
	uiRequests chan *nativeDialog
	done       chan struct{}
	// endOnce closes events exactly once. A reservation outlives the
	// generation it was made on, so more than one router can reach the same
	// delivery and only the first end is the one that speaks.
	endOnce sync.Once
}

func newTurnDelivery() *turnDelivery {
	return &turnDelivery{
		events: make(chan pi.Event),
		// pi dialogs are modal and therefore serialized. One slot keeps the
		// transport draining if a dialog arrives before the prompt RPC ack.
		uiRequests: make(chan *nativeDialog, 1),
		done:       make(chan struct{}),
	}
}

// nativeDialog binds a UI request to the exact native generation that emitted
// it. Its wait-group token is acquired by the pump before enqueue and released
// either by the sole consumer or by abandonment when the prompt ends, so close
// can never race a Wait with a later Add.
type nativeDialog struct {
	request pi.UIRequest
	outbox  *sessionOutbox
	client  piClient

	claimed atomic.Bool
	respond sync.Once
	release sync.Once
	done    func()
}

func (d *nativeDialog) claim() bool {
	return d != nil && d.claimed.CompareAndSwap(false, true)
}

func (d *nativeDialog) complete() {
	if d == nil {
		return
	}

	d.release.Do(func() {
		if d.done != nil {
			d.done()
		}
	})
}

func (d *nativeDialog) abandon(ctx context.Context, session *agentSession) {
	if d != nil && d.claimed.CompareAndSwap(false, true) {
		response := pi.UICancelResponse(d.request.ID)
		if d.request.Method == uiMethodSelect && strings.HasPrefix(d.request.Title, pi.PermissionTitleMarker) {
			response = pi.UIValueResponse(d.request.ID, pi.PermissionOptionDeny)
		}

		d.answer(context.WithoutCancel(ctx), session, response)
		d.complete()
	}
}

func (d *nativeDialog) answer(ctx context.Context, session *agentSession, response pi.UIResponse) {
	if d == nil {
		return
	}

	d.respond.Do(func() {
		session.respondExactUIDialog(ctx, d.outbox, d.client, response)
	})
}

// end tells the prompt loop that no generation will produce another event for
// it. Only a router calls it, and only for a generation that routes nothing
// further, so it never races a send it would panic on.
func (d *turnDelivery) end() {
	d.endOnce.Do(func() { close(d.events) })
}

func (d *turnDelivery) abandonQueuedDialogs(ctx context.Context, session *agentSession) {
	if d == nil {
		return
	}

	for {
		select {
		case dialog := <-d.uiRequests:
			if dialog != nil {
				dialog.abandon(ctx, session)
			}
		default:
			return
		}
	}
}

// outboxState is what one generation's router is doing with the records it
// receives. The states are exhaustive and every record is classified under
// exactly one of them, so a record is never routed by inferring the absence of
// a foreground cycle.
type outboxState int

const (
	// outboxIdle holds no cycle: only an agent_start opens work here, and any
	// other record that bears work is an invariant failure.
	outboxIdle outboxState = iota
	// outboxPromptPending is the session admission that excludes an autonomous
	// opener while process recovery chooses the generation the prompt will use.
	outboxPromptPending
	// outboxReserved holds the live generation a pending prompt selected. Its
	// delivery is attached, but its native frame has not been dispatched.
	outboxReserved
	// outboxForeground is a dispatched prompt streaming its turn.
	outboxForeground
	// outboxSettling is a foreground turn whose native settle marker has been
	// handed to the prompt. Later records queue until the prompt's durable
	// commit and terminal idle complete.
	outboxSettling
	// outboxAgentCycle is an agent-origin cycle the session opened for work no
	// prompt asked for.
	outboxAgentCycle
	// outboxAgentSettling is an agent-origin cycle whose settlement is running.
	outboxAgentSettling
	// outboxRestoring is an active session/load or session/resume holding the
	// generation across its replay. Records are retained: the transcript the
	// restore answers with is the one the gate froze.
	outboxRestoring
	// outboxConfiguring retains autonomous records while a host configuration
	// command is in flight on this generation.
	outboxConfiguring
	// outboxClosing is the close ladder holding the foreground. The records a
	// native shutdown emits are drained deliberately rather than judged, and no
	// new cycle opens behind them.
	outboxClosing
)

// agentCycle is one agent-origin foreground cycle: work the native harness
// began between prompts. It carries the lifecycle identity the cycle opened
// with and the accumulation the cycle's terminal boundary reports.
type agentCycle struct {
	generation uint64
	turnID     string
	cycleID    string
	state      *promptTurnState
	// failure records that this cycle's extension surface threw. It decides the
	// cycle's outcome, never its end: only pi's own settle marker says the work
	// stopped, and the thrown text never leaves the adapter.
	failure bool
}

// outboxDisposition is what the pump must do with one classified record.
type outboxDisposition int

const (
	// outboxDrop names a record whose generation is over. Its stream ended and
	// nothing reconstructs it, so it moves no session state at all.
	outboxDrop outboxDisposition = iota
	// outboxDeliver hands the record to the foreground delivery.
	outboxDeliver
	// outboxOpenCycle reports that this record opened an agent-origin cycle the
	// router already owns.
	outboxOpenCycle
	// outboxSession routes the record through the agent-origin cycle it names.
	outboxSession
	// outboxNoted names a record that bears no work and opens nothing: the
	// generation records it and moves on.
	outboxNoted
	// outboxQueued records that the router retained the record in arrival
	// order for a later drain.
	outboxQueued
	// outboxDeferred records a structural pre-acceptance queue report whose raw
	// diagnostic frame is held until prompt_accepted has been published.
	outboxDeferred
	// outboxOverflow reports that the bounded queue is full.
	outboxOverflow
	// outboxShutdown names a record a claimed close drained on purpose.
	outboxShutdown
	// outboxViolation names a record the generation cannot place. It is refused
	// rather than interpreted, and the generation is contained.
	outboxViolation
)

// outboxAdmission is one classified record.
type outboxAdmission struct {
	disposition outboxDisposition
	delivery    *turnDelivery
	// cycle is the agent-origin cycle this record belongs to, captured under
	// the same lock that classified it so a later state change cannot move the
	// record to a different cycle.
	cycle *agentCycle
	// settledForeground reports that this record is the native settle marker
	// of the foreground turn, which is the fence the mirror stands behind.
	settledForeground bool
	// violation is the invariant this record broke, in the terms the poisoned
	// session reports. It names structure only: no native path, no thrown text.
	violation string
}

// sessionOutbox routes every native event of exactly one pi process
// generation. It is session-owned rather than prompt-owned: pi survives a
// prompt, so the router that speaks for its generation must survive one too,
// and an event that arrives with no foreground cycle open is classified rather
// than discarded.
//
// Ordering is the contract. A record is delivered, routed, or retained in
// arrival order; it is never dropped while the generation lives, never
// reordered around a settling cycle, and never sent to a delivery whose turn
// has ended.
//
// Admission is the other contract. Opening an agent-origin cycle and reserving
// the foreground for a prompt are the same transition under this lock, so no
// interval exists in which pi's opener and a client's prompt both believe they
// hold the session's one foreground.
type sessionOutbox struct {
	generation uint64

	// dispatchMu serializes only exact native command writes. Fencing and
	// containment never wait for it: a blocked writer belongs to this captured
	// generation and is contained with that generation.
	dispatchMu    *dispatchGate
	mu            sync.Mutex
	state         outboxState
	admission     *turnDelivery
	turn          *turnDelivery
	cycle         *agentCycle
	queued        []pi.Event
	preAcceptance []pi.Event
	// established gates every startup record behind the successful session
	// response and opening snapshot. startup is one ordered prefix across native
	// events and UI requests; neither its raw projection nor a handler may escape
	// before openingAccepted is replayed to completion.
	startup           []startupRecord
	openingAccepted   bool
	established       bool
	establishmentDone chan struct{}
	establishmentOnce sync.Once
	// nativeQueueDepth is the only queue information the adapter retains. It is
	// updated by admit under this lock, which makes the report atomic with prompt
	// reservation, restore admission, and settle validation.
	nativeQueueDepth int
	ended            bool
	// fenced records that an invariant this router cannot repair failed. A
	// fenced generation routes nothing further and reconstructs nothing.
	fenced bool
	// closing records that the close ladder claimed this generation. It
	// outlives a settlement that is still finishing, so a cycle completing
	// during teardown returns the router to a closing state and never to one
	// that would admit a new cycle.
	closing bool
	// interactionsClosed is the Add-after-Wait fence for dialog handlers and
	// their action senders.
	interactionsClosed bool
	// drain wakes the pump to replay retained records. It is the pump that
	// replays them, so the single-consumer ordering the router promises holds
	// across a settlement boundary.
	drain chan struct{}

	// These handles belong to this exact generation. Containment never consults
	// the session's replaceable current-process fields after capturing them here.
	proc           piProcess
	client         piClient
	generationDone <-chan struct{}
	pumpCancel     context.CancelFunc
	pumpDone       chan struct{}
	nativeBoundary *nativeBoundaryTracker

	containment *generationContainment
	producers   *generationProducers
	// interactions is the generation-owned, sealable join for every dialog
	// and action handler. Cancellation seals it once and every retry waits on
	// the same channel; no retry creates a WaitGroup waiter goroutine.
	interactions *generationProducers
}

type startupRecord struct {
	event   pi.Event
	request *pi.UIRequest
}

// dispatchGate is the context-aware single-writer gate for one native
// generation. A command waiting behind a hostile writer can be interrupted by
// its own bound, while close retains the non-blocking election used to proceed
// directly to containment.
type dispatchGate struct {
	token chan struct{}
}

func newDispatchGate() *dispatchGate {
	gate := &dispatchGate{token: make(chan struct{}, 1)}
	gate.token <- struct{}{}

	return gate
}

func (g *dispatchGate) lock(ctx context.Context) error {
	if g == nil {
		return pi.ErrTransportClosed
	}

	select {
	case <-g.token:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (g *dispatchGate) TryLock() bool {
	if g == nil {
		return false
	}

	select {
	case <-g.token:
		return true
	default:
		return false
	}
}

func (g *dispatchGate) Unlock() {
	g.token <- struct{}{}
}

// generationProducers is the admission root for every asynchronous operation
// descended from the pump. The pump owns the initial token. Children may only
// be admitted while at least one ancestor token remains, and zero is terminal,
// so a waiter that observes completion can never race a later Add.
type generationProducers struct {
	mu       sync.Mutex
	count    int
	children int
	done     chan struct{}
	idle     chan struct{}
}

func newGenerationProducers() *generationProducers {
	idle := make(chan struct{})
	close(idle)

	return &generationProducers{count: 1, done: make(chan struct{}), idle: idle}
}

func (p *generationProducers) acquire(n int) (func(), bool) {
	if p == nil || n <= 0 {
		return func() {}, false
	}

	p.mu.Lock()
	if p.count == 0 {
		p.mu.Unlock()

		return func() {}, false
	}

	if p.children == 0 {
		p.idle = make(chan struct{})
	}

	p.count += n
	p.children += n
	p.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			p.release(n)
		})
	}, true
}

func (p *generationProducers) releaseRoot() {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.count--

	if p.count == 0 {
		close(p.done)
	}
	p.mu.Unlock()
}

func (p *generationProducers) release(n int) {
	p.mu.Lock()

	p.count -= n
	p.children -= n

	if p.children == 0 {
		close(p.idle)
	}

	if p.count == 0 {
		close(p.done)
	}
	p.mu.Unlock()
}

func (p *generationProducers) waitChildren(ctx context.Context) error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	idle := p.idle
	p.mu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: join generation child producer chain: %v",
			ErrContainmentIncomplete, ctx.Err())
	}
}

func (p *generationProducers) wait(ctx context.Context) error {
	if p == nil {
		return nil
	}

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: join generation producer chain: %v",
			ErrContainmentIncomplete, ctx.Err())
	}
}

type containmentOwner uint8

const (
	containmentOwnerPump containmentOwner = iota + 1
	containmentOwnerClose
	containmentOwnerTurn
)

type generationContainment struct {
	owner       containmentOwner
	done        chan struct{}
	waiting     chan struct{}
	waitingOnce sync.Once
	finishOnce  sync.Once
	err         error
}

func newSessionOutbox(generation uint64, boundary *nativeBoundaryTracker) *sessionOutbox {
	return &sessionOutbox{
		generation:        generation,
		dispatchMu:        newDispatchGate(),
		drain:             make(chan struct{}, 1),
		establishmentDone: make(chan struct{}),
		producers:         newGenerationProducers(),
		interactions:      newGenerationProducers(),
		nativeBoundary:    boundary,
	}
}

func (o *sessionOutbox) bindRuntime(
	proc piProcess,
	client piClient,
	cancel context.CancelFunc,
	done chan struct{},
	boundary *nativeBoundaryTracker,
) error {
	if boundary == nil || o == nil || o.nativeBoundary == nil || o.nativeBoundary != boundary {
		return errors.Join(ErrContainmentIncomplete, errors.New("native generation boundary owner is missing"))
	}

	o.proc = proc
	o.client = client
	o.pumpCancel = cancel
	o.pumpDone = done

	return nil
}

// inheritPromptAdmission carries the session's pending foreground claim onto a
// new generation without attaching its delivery to the generation being
// replaced.
func (o *sessionOutbox) inheritPromptAdmission(delivery *turnDelivery) {
	if delivery == nil {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.state = outboxPromptPending
	o.admission = delivery
}

func (o *sessionOutbox) claimPromptAdmission(delivery *turnDelivery) error {
	if o == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.fenced {
		return pi.ErrTransportClosed
	}

	if o.ended {
		return nil
	}

	if !o.established || o.closing || o.state != outboxIdle || len(o.queued) > 0 || o.nativeQueueDepth > 0 {
		return backpressureError(limitSessionPrompt)
	}

	o.state = outboxPromptPending
	o.admission = delivery

	return nil
}

func (o *sessionOutbox) waitEstablished(ctx context.Context) error {
	if o == nil {
		return nil
	}

	select {
	case <-o.establishmentDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// A cleanly ended generation is allowed through so refreshAndReservePrompt
	// can carry the admission onto its successor. Fenced, closing, or merely
	// unfinished establishment is never adoptable.
	if o.fenced || o.closing || (!o.established && !o.ended) {
		return pi.ErrTransportClosed
	}

	return nil
}

func (o *sessionOutbox) reserveClaimed(delivery *turnDelivery) error {
	if o == nil {
		return pi.ErrTransportClosed
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing {
		return pi.ErrTransportClosed
	}

	if o.state != outboxPromptPending || o.admission != delivery || o.nativeQueueDepth > 0 {
		return backpressureError(limitSessionPrompt)
	}

	o.admission = nil
	o.state = outboxReserved
	o.turn = delivery

	return nil
}

// activate records the successful prompt response at the exact stdout boundary
// supplied by pi.Client. It must run on the client reader before that reader can
// deliver a later record. Any state other than the untouched reservation is a
// causal contradiction and is refused; pre-response work is never replayed into
// this prompt.
func (o *sessionOutbox) activate(delivery *turnDelivery) error {
	if o == nil {
		delivery.end()

		return pi.ErrTransportClosed
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced {
		delivery.end()

		return pi.ErrTransportClosed
	}

	if o.turn != delivery || o.state != outboxReserved || o.nativeQueueDepth > 0 {
		return errors.New("prompt acceptance crossed prior or ambiguous native work")
	}

	o.state = outboxForeground

	return nil
}

func (o *sessionOutbox) takePreAcceptance(delivery *turnDelivery) []pi.Event {
	if o == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.turn != delivery || o.state != outboxForeground {
		return nil
	}

	events := o.preAcceptance
	o.preAcceptance = nil

	return events
}

// release returns the foreground to the session after the prompt's durable
// commit, terminal idle, and shared-state teardown have completed, and wakes
// the pump for whatever the settling turn retained.
func (o *sessionOutbox) release(delivery *turnDelivery) {
	if o == nil {
		return
	}

	o.mu.Lock()

	if o.turn != delivery {
		o.mu.Unlock()

		return
	}

	o.turn = nil
	o.preAcceptance = nil

	if !o.ended && !o.fenced {
		o.state = o.vacantStateLocked()
	}
	o.mu.Unlock()

	o.wake()
}

// releasePromptAdmission returns an admission that never reached acceptance.
// The frames held while it was pending are dropped with it: acceptance is the
// only thing that can release them, and an admission that never published one
// leaves them with no turn to be tagged for. Keeping them would let an
// abandoned admission's diagnostics surface under a later prompt's route and
// count against the retention bound this router kills the generation over.
func (o *sessionOutbox) releasePromptAdmission(delivery *turnDelivery) {
	if o == nil {
		return
	}

	o.mu.Lock()
	if o.admission == delivery {
		o.admission = nil
		o.preAcceptance = nil

		if !o.ended && !o.fenced && o.state == outboxPromptPending {
			o.state = o.vacantStateLocked()
		}
	}
	o.mu.Unlock()

	o.wake()
}

// vacantStateLocked is what the router returns to when the work holding it
// finishes. A close that already claimed the generation keeps it: teardown is
// the last thing this router does, and returning to idle would let a shutdown
// record open a cycle the close ladder has already accounted for.
func (o *sessionOutbox) vacantStateLocked() outboxState {
	if o.closing {
		return outboxClosing
	}

	return outboxIdle
}

// foreground reports the delivery a dispatched prompt is reading. A reservation
// is not a foreground: its turn has no lifecycle identity yet, so a request
// answered under it would be attributed to a turn the native dispatcher has not
// acknowledged.
func (o *sessionOutbox) foreground() *turnDelivery {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.state != outboxForeground {
		return nil
	}

	return o.turn
}

// agentBusy reports whether this generation holds work no client turn may
// interleave with: an agent-origin cycle, its settlement, records retained
// behind one, an active restore, or a claimed close.
func (o *sessionOutbox) agentBusy() bool {
	if o == nil {
		return false
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced {
		return false
	}

	if o.nativeQueueDepth > 0 {
		return true
	}

	switch o.state {
	case outboxAgentCycle, outboxAgentSettling, outboxRestoring, outboxConfiguring, outboxClosing:
		return true
	case outboxIdle, outboxPromptPending, outboxReserved, outboxForeground, outboxSettling:
	}

	return len(o.queued) > 0
}

// beginCycleSettlement moves the open cycle into settlement so every later
// record is retained until the cycle's durable commit and terminal idle are
// done.
func (o *sessionOutbox) beginCycleSettlement(cycle *agentCycle) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.state != outboxAgentCycle || o.cycle != cycle {
		return false
	}

	o.state = outboxAgentSettling

	return true
}

func (o *sessionOutbox) claimForCloseLocked() *agentCycle {
	o.closing = true
	o.finishEstablishmentLocked()

	if o.ended || o.fenced {
		return nil
	}

	if o.state == outboxAgentCycle {
		cycle := o.cycle
		o.cycle = nil
		o.state = outboxClosing

		return cycle
	}

	if o.state == outboxIdle {
		o.state = outboxClosing
	}

	return nil
}

// beginRestore holds the generation for an active session/load or
// session/resume. The gate excludes new agent-origin admission for the whole
// replay, and every record that arrives meanwhile is retained rather than
// dropped, so the answer the restore gives is the transcript the gate froze.
func (o *sessionOutbox) beginRestore() bool {
	// A session with no native generation has no router to gate. The turn
	// admission the caller already holds is the whole of its foreground.
	if o == nil {
		return true
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing || o.state != outboxIdle || len(o.queued) > 0 || o.nativeQueueDepth > 0 {
		return false
	}

	o.state = outboxRestoring

	return true
}

// finishRestore releases the replay gate and wakes the pump for whatever
// arrived behind it.
func (o *sessionOutbox) finishRestore() {
	if o == nil {
		return
	}

	o.mu.Lock()

	if o.state == outboxRestoring {
		o.state = o.vacantStateLocked()
	}
	o.mu.Unlock()

	o.wake()
}

func (o *sessionOutbox) beginConfiguration() bool {
	if o == nil {
		return false
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing || o.state != outboxIdle || len(o.queued) > 0 || o.nativeQueueDepth > 0 {
		return false
	}

	o.state = outboxConfiguring

	return true
}

func (o *sessionOutbox) finishConfiguration() {
	if o == nil {
		return
	}

	o.mu.Lock()
	if o.state == outboxConfiguring {
		o.state = o.vacantStateLocked()
	}
	o.mu.Unlock()

	o.wake()
}

// finishCycle closes the settled cycle and wakes the pump for whatever it
// retained.
func (o *sessionOutbox) finishCycle() {
	o.mu.Lock()

	if o.state == outboxAgentSettling {
		o.state = o.vacantStateLocked()
	}

	o.cycle = nil
	o.mu.Unlock()

	o.wake()
}

func (o *sessionOutbox) wake() {
	select {
	case o.drain <- struct{}{}:
	default:
	}
}

// admit classifies one arriving record under the generation's current state.
// A reservation, a settling cycle, an active restore, and a non-empty retention
// queue all retain, which is what keeps a drained record behind every record
// that arrived before it.
func (o *sessionOutbox) admit(event pi.Event) outboxAdmission {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced {
		return outboxAdmission{disposition: outboxDrop}
	}

	// A claimed close outranks retention. Teardown replays nothing, so a record
	// the native shutdown emits is drained on purpose rather than held for a
	// drain that will never run.
	if o.closing {
		return outboxAdmission{disposition: outboxShutdown}
	}

	if queue, reported := event.(pi.QueueUpdateEvent); reported {
		o.nativeQueueDepth = len(queue.Steering) + len(queue.FollowUp)
	}

	if _, settled := event.(pi.AgentSettledEvent); settled && o.nativeQueueDepth > 0 {
		return outboxAdmission{
			disposition: outboxViolation,
			violation:   "pi settled while its native queue was not empty",
		}
	}

	if !o.established {
		if len(o.startup) >= outboxQueueCapacity {
			return outboxAdmission{disposition: outboxOverflow}
		}

		o.startup = append(o.startup, startupRecord{event: event})

		return outboxAdmission{disposition: outboxDeferred}
	}

	switch o.state {
	case outboxPromptPending, outboxReserved:
		// pi decides whether the transcript must be compacted before it accepts a
		// prompt, so the queue report, the compaction pair, its summarization
		// retries, and whatever an extension writes from the compaction hooks all
		// arrive ahead of the command response. None of it bears work the prompt
		// could own, and the response is the first record that can make later work
		// belong to the prompt, so it is held here and released once acceptance is
		// published as raw frames tagged with the accepted turn's route. They stay
		// diagnostics: nothing is dispatched through the prompt's delivery, and
		// none of it claims the prompt's action authority or lifecycle identity.
		if bearsCycleWork(event) {
			return outboxAdmission{
				disposition: outboxViolation,
				violation:   "pi produced work before prompt acceptance",
			}
		}

		if len(o.preAcceptance) >= outboxQueueCapacity {
			return outboxAdmission{disposition: outboxOverflow}
		}

		o.preAcceptance = append(o.preAcceptance, event)

		return outboxAdmission{disposition: outboxDeferred}
	case outboxSettling, outboxAgentSettling, outboxRestoring, outboxConfiguring:
		return o.retainLocked(event)
	case outboxIdle, outboxForeground, outboxAgentCycle, outboxClosing:
	}

	if len(o.queued) > 0 {
		return o.retainLocked(event)
	}

	return o.classifyLocked(event)
}

func (o *sessionOutbox) retainLocked(event pi.Event) outboxAdmission {
	if len(o.queued) >= outboxQueueCapacity {
		return outboxAdmission{disposition: outboxOverflow}
	}

	o.queued = append(o.queued, event)

	return outboxAdmission{disposition: outboxQueued}
}

// classifyLocked judges one record against the state that owns it. Opening an
// agent-origin cycle happens here, under the same lock a prompt's reservation
// takes, so pi's opener and a client prompt can never both be admitted.
func (o *sessionOutbox) classifyLocked(event pi.Event) outboxAdmission {
	switch o.state {
	case outboxForeground:
		if _, settled := event.(pi.AgentSettledEvent); settled {
			o.state = outboxSettling

			return outboxAdmission{disposition: outboxDeliver, delivery: o.turn, settledForeground: true}
		}

		return outboxAdmission{disposition: outboxDeliver, delivery: o.turn}
	case outboxIdle:
		return o.classifyVacantLocked(event)
	case outboxAgentCycle:
		// pi brackets every continuation of one run with its own opener, so a
		// cycle that compacts, retries, or drains a queued message states several
		// openers inside the single scope its settle marker ends. Each belongs to
		// the cycle already open, exactly as the foreground reads them.
		return outboxAdmission{disposition: outboxSession, cycle: o.cycle}
	case outboxPromptPending, outboxReserved, outboxSettling, outboxAgentSettling, outboxRestoring, outboxConfiguring, outboxClosing:
	}

	return outboxAdmission{disposition: outboxViolation, violation: "pi produced a record its generation cannot place"}
}

// classifyVacantLocked judges a record that arrived with nothing to own it.
// agent_start is the sole native record that honestly says work began, so it is
// the only opener. Between runs pi still reports its queue, echoes a
// configuration change, journals what an extension appended, and names types
// this package does not model; none of that bears work, so it is recorded and
// nothing is opened for it. A record that does bear work fails closed, because a
// fabricated opener would give the host a turn whose beginning this adapter
// never observed and attaching the record to the next prompt would attribute it
// to a submission that did not cause it.
func (o *sessionOutbox) classifyVacantLocked(event pi.Event) outboxAdmission {
	if _, opens := event.(pi.AgentStartEvent); opens {
		cycle := &agentCycle{generation: o.generation, state: &promptTurnState{}}
		o.state = outboxAgentCycle
		o.cycle = cycle

		return outboxAdmission{disposition: outboxOpenCycle, cycle: cycle}
	}

	if bearsCycleWork(event) {
		return outboxAdmission{disposition: outboxViolation, violation: orphanRecordViolation(event)}
	}

	return outboxAdmission{disposition: outboxNoted}
}

// bearsCycleWork reports whether a record says pi's agent loop is running
// something a cycle must own. The rest is session-scoped: the queue report,
// which states what is pending and never what is running; the compaction and
// retry pairs, which pi runs around a turn rather than inside one; the custom
// message and journal entry an extension writes; and every type this package
// does not model, which by definition names no entity ACP or the lifecycle
// extension could carry.
func bearsCycleWork(event pi.Event) bool {
	switch typed := event.(type) {
	case pi.MessageStartEvent:
		return typed.Message.Role != messageRoleCustom
	case pi.MessageEndEvent:
		return typed.Message.Role != messageRoleCustom
	case pi.AgentStartEvent, pi.AgentEndEvent, pi.AgentSettledEvent,
		pi.TurnStartEvent, pi.TurnEndEvent, pi.MessageUpdateEvent,
		pi.ToolExecutionStartEvent, pi.ToolExecutionUpdateEvent, pi.ToolExecutionEndEvent,
		pi.ExtensionErrorEvent:
		return true
	}

	return false
}

// orphanRecordViolation names the invariant an unowned record broke, in
// structural terms only. The extension surface is the permission bridge and the
// MCP client: a failure there is a failure of the session's own trust boundary,
// and its native path and thrown text are adapter-internal.
func orphanRecordViolation(event pi.Event) string {
	switch event.(type) {
	case pi.AgentSettledEvent:
		return "pi settled with no cycle open"
	case pi.ExtensionErrorEvent:
		return "a pi extension failed outside any cycle"
	}

	return "pi produced work with no cycle open"
}

// popRetained takes the next retained record when the generation can act on
// one, and classifies it under the state that will receive it. A cycle that
// started settling again keeps the rest of the queue behind it rather than
// letting a later record overtake its boundary.
func (o *sessionOutbox) popRetained() (pi.Event, outboxAdmission, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing || !o.established || len(o.queued) == 0 {
		return nil, outboxAdmission{}, false
	}

	switch o.state {
	case outboxIdle, outboxAgentCycle, outboxForeground:
	case outboxPromptPending, outboxReserved, outboxSettling, outboxAgentSettling, outboxRestoring, outboxConfiguring, outboxClosing:
		return nil, outboxAdmission{}, false
	}

	event := o.queued[0]
	o.queued = o.queued[1:]

	return event, o.classifyLocked(event), true
}

func (o *sessionOutbox) deferStartupUI(request pi.UIRequest) (bool, bool) {
	if o == nil {
		return false, false
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing || o.established {
		return false, false
	}

	if len(o.startup) >= outboxQueueCapacity {
		return false, true
	}

	copyRequest := request
	o.startup = append(o.startup, startupRecord{request: &copyRequest})

	return true, false
}

func (o *sessionOutbox) popStartup() (startupRecord, outboxAdmission, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing || !o.openingAccepted || o.established {
		return startupRecord{}, outboxAdmission{}, false
	}

	if len(o.startup) == 0 {
		o.established = true
		o.finishEstablishmentLocked()

		return startupRecord{}, outboxAdmission{}, false
	}

	record := o.startup[0]

	o.startup = o.startup[1:]
	if record.event != nil {
		return record, o.classifyLocked(record.event), true
	}

	return record, outboxAdmission{}, true
}

// acceptEstablishment releases the opening gate for the exact generation whose
// establishing snapshot has landed. It answers its two refusals separately,
// because they are different facts about who owns the generation.
//
// A generation the close ladder, a fence, or a containment has already claimed
// is retired: a host may close a session at any point, including while the
// snapshot behind its own establishing response is still being written, and the
// claiming owner is already running the ladder that ends it. Nothing here is
// broken, so nothing here is contained.
//
// A gate this generation already released is the invariant the single-release
// rule exists to catch, and it stays fail-closed.
func (o *sessionOutbox) acceptEstablishment() error {
	if o == nil {
		return errors.Join(ErrContainmentIncomplete, errors.New("native generation outbox is missing"))
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ended || o.fenced || o.closing {
		return errGenerationRetired
	}

	if o.established || o.openingAccepted {
		return pi.ErrTransportClosed
	}

	o.openingAccepted = true

	return nil
}

func (o *sessionOutbox) finishEstablishmentLocked() {
	o.establishmentOnce.Do(func() {
		close(o.establishmentDone)
	})
}

// end fences the generation. The foreground delivery learns the native
// transport is gone from the closed channel, which is the one signal that says
// this generation produces no further event. It reports whether the generation
// died owning work or retained records, which is a loss the stream states
// rather than a cycle it completes.
//
// A merely reserved delivery is not ended here. Its prompt has read nothing and
// carries the reservation to the generation that replaces this one, so the
// delivery belongs to that successor rather than to the router shutting down.
//
// A cycle already settling is not a loss either. Its durable boundary and
// terminal event are running on a context this end does not cancel, and
// teardown waits for them, so fencing here would discard a terminal state the
// session is in the middle of stating truthfully.
func (o *sessionOutbox) end() bool {
	o.mu.Lock()
	delivery := o.turn
	reserved := o.state == outboxReserved
	o.admission = nil
	o.turn = nil
	alreadyEnded := o.ended
	o.ended = true
	lost := (o.state == outboxAgentCycle && o.cycle != nil) || len(o.queued) > 0 || len(o.startup) > 0
	o.cycle = nil
	o.queued = nil
	o.startup = nil
	o.preAcceptance = nil
	o.finishEstablishmentLocked()
	o.mu.Unlock()

	if delivery != nil && !alreadyEnded && !reserved {
		delivery.end()
	}

	return lost && !alreadyEnded
}

// fenceLocked is the non-blocking half of exact-generation containment. The
// caller holds o.mu; closing the delivery happens after the lock is released.
func (o *sessionOutbox) fenceLocked() *turnDelivery {
	o.fenced = true
	delivery := o.turn
	o.admission = nil
	o.turn = nil
	o.ended = true
	o.cycle = nil
	o.queued = nil
	o.startup = nil
	o.preAcceptance = nil
	o.state = outboxIdle
	o.finishEstablishmentLocked()

	return delivery
}

func (o *sessionOutbox) claimContainmentLocked(owner containmentOwner) (*generationContainment, bool) {
	if o.containment != nil {
		return o.containment, false
	}

	o.containment = &generationContainment{
		owner:   owner,
		done:    make(chan struct{}),
		waiting: make(chan struct{}),
	}

	return o.containment, true
}

func (o *sessionOutbox) finishContainment(containment *generationContainment, err error) {
	if o == nil || containment == nil {
		return
	}

	o.mu.Lock()
	exact := o.containment == containment
	o.mu.Unlock()

	if !exact {
		return
	}

	containment.finishOnce.Do(func() {
		containment.err = err
		close(containment.done)
	})
}

func (o *sessionOutbox) awaitContainment() (error, bool) {
	if o == nil {
		return nil, false
	}

	o.mu.Lock()
	containment := o.containment
	o.mu.Unlock()

	if containment == nil {
		return nil, false
	}

	containment.waitingOnce.Do(func() { close(containment.waiting) })
	<-containment.done

	return containment.err, true
}

func (s *agentSession) startPumpContext(
	ctx context.Context,
	cancel context.CancelFunc,
	client piClient,
	boundary *nativeBoundaryTracker,
) (uint64, error) {
	done := make(chan struct{})

	if boundary == nil {
		cancel()

		return 0, errors.Join(ErrContainmentIncomplete, errors.New("native generation boundary owner is missing"))
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		cancel()

		return 0, unknownSessionError()
	}

	if s.nativeBoundary != boundary {
		s.mu.Unlock()
		cancel()

		return 0, errors.Join(ErrContainmentIncomplete, errors.New("native generation boundary owner changed"))
	}

	s.pumpGeneration++
	generation := s.pumpGeneration
	outbox := newSessionOutbox(generation, boundary)
	outbox.generationDone = ctx.Done()
	outbox.inheritPromptAdmission(s.promptAdmission)

	// newSessionOutbox was constructed with this exact non-nil boundary, so the
	// construction-owned binding cannot fail after the checks above.
	_ = outbox.bindRuntime(s.proc, client, cancel, done, boundary)

	s.outbox = outbox
	s.pumpCancel = cancel
	s.pumpDone = done
	s.mu.Unlock()

	go s.pump(ctx, client, outbox, done)

	return generation, nil
}

// publishRuntimeGeneration atomically elects close against publication of all
// handles belonging to one successor. Close therefore snapshots either the
// predecessor or this complete generation, never a new process paired with an
// old outbox.
func (s *agentSession) publishRuntimeGeneration(
	ctx context.Context,
	cancel context.CancelFunc,
	proc piProcess,
	client piClient,
	boundary *nativeBoundaryTracker,
) (uint64, *sessionOutbox, bool) {
	done := make(chan struct{})

	if boundary == nil {
		cancel()

		return 0, nil, false
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		cancel()

		return 0, nil, false
	}

	s.pumpGeneration++
	generation := s.pumpGeneration
	outbox := newSessionOutbox(generation, boundary)
	outbox.generationDone = ctx.Done()
	outbox.inheritPromptAdmission(s.promptAdmission)

	_ = outbox.bindRuntime(proc, client, cancel, done, boundary)

	s.proc = proc
	s.client = client
	s.nativeBoundary = boundary
	s.outbox = outbox
	s.pumpCancel = cancel
	s.pumpDone = done
	s.mu.Unlock()

	go s.pump(ctx, client, outbox, done)

	return generation, outbox, true
}

func (s *agentSession) pump(ctx context.Context, client piClient, outbox *sessionOutbox, done chan struct{}) {
	defer close(done)
	defer outbox.producers.releaseRoot()
	defer func() {
		handleAgentGoroutinePanic(ctx, agentLogger(s.agent), "session event pump", func(any) {
			s.containGeneration(ctx, outbox, "the native event pump panicked")
		}, recover())
	}()

	events := client.Events()
	uiRequests := client.UIRequests()
	boundaries := client.ResponseBoundaries()

	for events != nil || uiRequests != nil || boundaries != nil {
		select {
		case event, ok := <-events:
			if !ok {
				events = nil

				continue
			}

			s.routeNativeEvent(ctx, outbox, event)
		case request, ok := <-uiRequests:
			if !ok {
				uiRequests = nil

				continue
			}

			s.routeUIRequest(ctx, outbox, request)
		case boundary, ok := <-boundaries:
			if !ok {
				boundaries = nil

				continue
			}

			boundary.Resolve(ctx)
		case <-outbox.drain:
			s.drainOutbox(ctx, outbox)
		case <-ctx.Done():
			resolveBufferedResponseBoundaries(ctx, boundaries)

			s.endGeneration(outbox)

			return
		}
	}

	if client.Err() != nil {
		s.containGeneration(ctx, outbox, "the native JSONL stream failed")

		return
	}

	s.endGeneration(outbox)
}

func resolveBufferedResponseBoundaries(ctx context.Context, boundaries <-chan pi.ResponseBoundary) {
	for boundaries != nil {
		select {
		case boundary, ok := <-boundaries:
			if !ok {
				boundaries = nil

				continue
			}

			boundary.Resolve(ctx)
		default:
			boundaries = nil
		}
	}
}

// endGeneration ends this generation's routing. A generation that died owning
// an agent-origin cycle or records it never replayed states the loss by fencing
// its incarnation: an end is not an idle, and nothing fabricates one. The
// identity the fence retires is retained until the relaunch path writes the
// loss down, so the boundary record names the turn that was actually lost.
func (s *agentSession) endGeneration(outbox *sessionOutbox) {
	if outbox.end() {
		s.fenceLifecycleGeneration(outbox.generation)
	}
}

// drainOutbox replays the records a reservation, a settling cycle, or a restore
// retained, in arrival order, on the pump goroutine that would have routed them
// live. The raw-event stream and the queue depth were already moved when each
// record arrived, so the replay only dispatches.
func (s *agentSession) drainOutbox(ctx context.Context, outbox *sessionOutbox) {
	for {
		record, admitted, ok := outbox.popStartup()
		if !ok {
			break
		}

		if record.event != nil {
			s.emitRawPiEvent(ctx, record.event.RawJSON())
			s.dispatchNativeRecord(ctx, outbox, record.event, admitted)

			continue
		}

		if record.request != nil {
			s.routeUIRequestEstablished(ctx, outbox, *record.request)
		}
	}

	for {
		event, admitted, ok := outbox.popRetained()
		if !ok {
			return
		}

		s.dispatchNativeRecord(ctx, outbox, event, admitted)
	}
}

// routeNativeEvent routes one native event under its generation's identity.
// Settlement is recorded at pump receipt, the raw-event stream sees every
// admitted event whether or not a prompt is in flight, and every event is
// classified rather than discarded for want of a foreground cycle.
//
// A record the router refused — one belonging to an ended generation, one the
// retention bound could not hold, one no state can place — moves nothing. It
// reaches no raw-event subscriber and never updates the queue depth a later
// settle marker is judged against, because a generation that is being contained
// must not keep speaking through the records that contained it.
func (s *agentSession) routeNativeEvent(ctx context.Context, outbox *sessionOutbox, event pi.Event) {
	admitted := outbox.admit(event)

	switch admitted.disposition {
	case outboxDrop:
		s.logUnroutedRecord(ctx, outbox, event, "its generation ended")

		return
	case outboxOverflow:
		s.containGeneration(ctx, outbox, "the session outbox reached its retention bound")

		return
	case outboxViolation:
		s.containGeneration(ctx, outbox, admitted.violation)

		return
	case outboxDeliver, outboxOpenCycle, outboxSession, outboxNoted, outboxQueued, outboxDeferred, outboxShutdown:
	}

	if admitted.disposition == outboxDeferred {
		return
	}

	rawCtx := ctx
	if admitted.disposition == outboxDeliver {
		rawCtx = s.generationRouteContext(ctx, admitted.delivery)
	}

	s.emitRawPiEvent(rawCtx, event.RawJSON())

	s.dispatchNativeRecord(ctx, outbox, event, admitted)
}

// dispatchNativeRecord acts on one classified record. It is the single place a
// record becomes foreground delivery, agent-origin work, or nothing, so a
// replayed record and a live one take exactly the same path.
func (s *agentSession) dispatchNativeRecord(
	ctx context.Context,
	outbox *sessionOutbox,
	event pi.Event,
	admitted outboxAdmission,
) {
	// The native response barrier ends when the client hands this event to the
	// pump, not when the prompt goroutine receives it from the delivery. Record
	// agent_settled before the cancellable send so stopPump cannot erase a
	// durability fence native pi has already crossed.
	if admitted.settledForeground {
		s.recordNativeSettlement(admitted.delivery)
	}

	switch admitted.disposition {
	case outboxDeliver:
		select {
		case admitted.delivery.events <- event:
		case <-admitted.delivery.done:
		case <-ctx.Done():
		}
	case outboxOpenCycle:
		s.openAgentCycle(ctx, outbox, admitted.cycle)
	case outboxSession:
		s.handleAgentOriginEvent(ctx, outbox, admitted.cycle, event)
	case outboxShutdown:
		s.logUnroutedRecord(ctx, outbox, event, "a claimed close drained it")
	case outboxNoted, outboxQueued, outboxDeferred, outboxDrop, outboxOverflow, outboxViolation:
		s.logUnroutedRecord(ctx, outbox, event, "it opened no work")
	}
}

func (s *agentSession) logUnroutedRecord(ctx context.Context, outbox *sessionOutbox, _ pi.Event, reason string) {
	s.agent.log.DebugContext(ctx, "pi event not routed to a cycle",
		slog.String(acpFieldSessionID, string(s.id)),
		slog.String("reason", reason),
		slog.Uint64("generation", outbox.generation),
	)
}

// containGeneration synchronously publishes poison and fences the exact router,
// then stops that router's captured native generation beside the sole reader.
// A later incarnation can replace every session-global process field without
// changing these handles, so stale containment cannot terminate its successor.
//
// The returned channel closes when the observable poison report this call owns
// has finished. It is nil unless this call is the non-owner that installed the
// cause: the owner's own report runs before it finishes containment, so
// awaiting containment already joins it. Callers that only need the admission
// fence discard the join; containGenerationSync waits on it so a synchronous
// return means the observable half has run too.
func (s *agentSession) containGeneration(ctx context.Context, outbox *sessionOutbox, cause string) <-chan struct{} {
	if outbox == nil {
		return nil
	}

	releaseProducer, admitted := outbox.producers.acquire(1)
	if !admitted {
		return nil
	}

	s.mu.Lock()
	outbox.mu.Lock()

	delivery := outbox.fenceLocked()
	containment, owner := outbox.claimContainmentLocked(containmentOwnerPump)

	s.registerContainmentOutboxLocked(outbox)

	effectiveCause, firstPoison, turnCancel := s.installPoisonLocked(cause)
	outbox.mu.Unlock()
	s.mu.Unlock()

	if delivery != nil {
		delivery.abandonQueuedDialogs(context.WithoutCancel(ctx), s)
		delivery.end()
	}

	if turnCancel != nil {
		turnCancel()
	}

	if !owner {
		if firstPoison {
			reported := make(chan struct{})

			go func() {
				defer close(reported)
				defer releaseProducer()

				containmentErr, _ := outbox.awaitContainment()
				if !nativeContainmentComplete(containmentErr) {
					s.recordNativeContainment(containmentErr)

					return
				}

				s.fenceLifecycleGeneration(outbox.generation)
				s.reportPoison(context.WithoutCancel(ctx), effectiveCause)
			}()

			return reported
		}

		releaseProducer()

		return nil
	}

	containCtx, cancelContain := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)

	go func() {
		defer cancelContain()
		defer releaseProducer()

		containmentErr := errors.Join(
			ErrContainmentIncomplete,
			errors.New("native generation containment did not complete"),
		)

		defer func() {
			if recover() != nil {
				containmentErr = generationContainmentPanicError("containment")
				s.recordNativeContainment(containmentErr)
			}

			outbox.finishContainment(containment, containmentErr)
		}()

		containmentErr = runNativeBoundaryStep(containCtx, "containment ladder", func() error {
			return s.stopNativeGeneration(containCtx, outbox)
		})
		if containmentErr != nil {
			s.recordNativeContainment(containmentErr)
		}

		if !nativeContainmentComplete(containmentErr) {
			return
		}

		// Lifecycle delivery can block on the host connection, so it is fenced only
		// after the exact native process has stopped. The sole reader is never held
		// hostage by an observability boundary.
		s.fenceLifecycleGeneration(outbox.generation)

		if firstPoison {
			s.reportPoison(containCtx, effectiveCause)
		}
	}()

	return nil
}

func (s *agentSession) containGenerationSync(ctx context.Context, outbox *sessionOutbox, cause string) error {
	if outbox == nil {
		err := errors.Join(ErrContainmentIncomplete, errors.New("native generation owner is missing"))
		s.recordNativeContainment(err)

		return err
	}

	reported := s.containGeneration(ctx, outbox, cause)

	containmentErr, ok := outbox.awaitContainment()

	// The non-owner reports poison from its own goroutine once the owner has
	// finished, so a synchronous caller joins that report too. Without it the
	// caller returns while an observable half of containment is still running.
	if reported != nil {
		<-reported
	}

	if !ok {
		containmentErr = errors.Join(
			ErrContainmentIncomplete,
			errors.New("native generation producer admission is closed"),
		)
		s.recordNativeContainment(containmentErr)
	}

	return containmentErr
}

// stopNativeGeneration runs the existing native shutdown ladder against only
// the process generation captured by outbox. It does not call stopPump, whose
// replaceable session fields could name a successor and whose cycle wait group
// includes this goroutine.
func (s *agentSession) stopNativeGeneration(ctx context.Context, outbox *sessionOutbox) error {
	var abortErr error

	if outbox.client != nil {
		abortCtx, cancelAbort := context.WithTimeout(context.WithoutCancel(ctx), sessionCancelAbortGrace)
		abortErr = outbox.nativeBoundary.run(abortCtx, "abort", func() error {
			return outbox.client.Abort(abortCtx)
		})

		cancelAbort()
	}

	var shutdownErr error

	if outbox.proc != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
		shutdownErr = outbox.nativeBoundary.run(shutdownCtx, "shutdown", func() error {
			return outbox.proc.Shutdown(shutdownCtx)
		})

		cancelShutdown()
	} else {
		shutdownErr = errors.Join(ErrContainmentIncomplete, errors.New("native generation has no contained process root"))
	}

	if outbox.pumpCancel != nil {
		outbox.pumpCancel()
	}

	var pumpErr error

	if outbox.pumpDone != nil {
		select {
		case <-outbox.pumpDone:
		case <-ctx.Done():
			pumpErr = fmt.Errorf("wait for native generation pump: %w", ctx.Err())
		}
	}

	var closeErr error

	if outbox.proc != nil {
		// Close is mandatory containment. An earlier shutdown/pump phase exhausting
		// the caller's bound cannot suppress it, so it receives its own detached
		// bound while the shared tracker retains every prior incomplete result.
		closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
		closeErr = outbox.nativeBoundary.run(closeCtx, "close", outbox.proc.Close)

		cancelClose()
	}

	if nativeContainmentComplete(closeErr) && pumpErr != nil && outbox.pumpDone != nil {
		joinCtx, cancelJoin := context.WithTimeout(context.Background(), sessionShutdownTimeout)
		select {
		case <-outbox.pumpDone:
			pumpErr = nil
		case <-joinCtx.Done():
		}

		cancelJoin()
	}

	containmentErr := errors.Join(terminalNativeClose(shutdownErr, closeErr), pumpErr)
	if !nativeContainmentComplete(abortErr) && !nativeContainmentComplete(closeErr) {
		containmentErr = errors.Join(containmentErr, abortErr)
	}

	if s.agent != nil && nativeContainmentComplete(containmentErr) {
		s.agent.observe.RecordPiProcessExit(ctx, "contained", containmentErr)
	}

	if abortErr != nil && s.agent != nil {
		s.agent.log.DebugContext(ctx, "native abort failed during generation containment",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.Uint64("generation", outbox.generation),
		)
	}

	return containmentErr
}

func runNativeBoundaryStep(ctx context.Context, stage string, step func() error) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: native generation %s did not start: %w",
			ErrContainmentIncomplete, stage, err)
	}

	done := make(chan error, 1)

	go func() {
		err := errors.Join(
			ErrContainmentIncomplete,
			fmt.Errorf("native generation %s did not complete", stage),
		)

		defer func() {
			if recover() != nil {
				err = generationContainmentPanicError(stage)
			}

			done <- err
		}()

		err = step()
	}()

	select {
	case err := <-done:
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("%w: native generation %s did not complete: %w",
				ErrContainmentIncomplete, stage, contextErr)
		}

		return err
	case <-ctx.Done():
		return fmt.Errorf("%w: native generation %s did not complete: %w",
			ErrContainmentIncomplete, stage, ctx.Err())
	}
}

// nativeBoundaryTracker serializes the potentially blocking process-control
// calls owned by one exact generation. An incomplete phase is retained on that
// owner, but it does not suppress a later mandatory phase after the active call
// has returned.
type nativeBoundaryTracker struct {
	mu         sync.Mutex
	active     *nativeBoundaryCall
	incomplete error
}

func newNativeBoundaryTracker() *nativeBoundaryTracker {
	return &nativeBoundaryTracker{}
}

type nativeBoundaryCall struct {
	stage string
	done  chan struct{}
	err   error
}

func (t *nativeBoundaryTracker) run(ctx context.Context, stage string, step func() error) error {
	if t == nil {
		return errors.Join(ErrContainmentIncomplete, errors.New("native generation boundary owner is missing"))
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: native generation %s did not start: %w",
			ErrContainmentIncomplete, stage, err)
	}

	for {
		t.mu.Lock()
		if t.active != nil {
			active := t.active
			t.mu.Unlock()

			if err := t.awaitPrior(ctx, stage, active); err != nil {
				return err
			}

			continue
		}

		call := &nativeBoundaryCall{stage: stage, done: make(chan struct{})}
		t.active = call
		t.mu.Unlock()

		go func() {
			err := errors.Join(
				ErrContainmentIncomplete,
				fmt.Errorf("native generation %s did not complete", stage),
			)

			defer func() {
				if recover() != nil {
					err = generationContainmentPanicError(stage)
				}

				t.mu.Lock()
				call.err = err

				if t.active == call {
					t.active = nil
				}

				if !nativeContainmentComplete(err) && t.incomplete == nil {
					t.incomplete = err
				}

				close(call.done)
				t.mu.Unlock()
			}()

			err = step()
		}()

		return t.await(ctx, call)
	}
}

func (t *nativeBoundaryTracker) awaitPrior(ctx context.Context, stage string, call *nativeBoundaryCall) error {
	select {
	case <-call.done:
		return nil
	case <-ctx.Done():
		incomplete := fmt.Errorf("%w: native generation %s did not start after %s: %w",
			ErrContainmentIncomplete, stage, call.stage, ctx.Err())
		t.detachIncomplete(call, incomplete)

		return incomplete
	}
}

func (t *nativeBoundaryTracker) await(ctx context.Context, call *nativeBoundaryCall) error {
	select {
	case <-call.done:
		t.mu.Lock()
		err := call.err
		t.mu.Unlock()

		if contextErr := ctx.Err(); contextErr != nil {
			incomplete := fmt.Errorf("%w: native generation %s did not complete: %w",
				ErrContainmentIncomplete, call.stage, contextErr)

			t.retainIncomplete(incomplete)
			err = incomplete
		}

		return err
	case <-ctx.Done():
		incomplete := fmt.Errorf("%w: native generation %s did not complete: %w",
			ErrContainmentIncomplete, call.stage, ctx.Err())

		t.detachIncomplete(call, incomplete)

		return incomplete
	}
}

// detachIncomplete releases only the serialization slot. The timed-out call
// remains owned by its goroutine and its first incomplete result remains
// immutable, while a mandatory containment phase may proceed without waiting
// for a callback that ignored cancellation.
func (t *nativeBoundaryTracker) detachIncomplete(call *nativeBoundaryCall, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.active == call {
		t.active = nil
	}

	if t.incomplete == nil {
		t.incomplete = err
	}
}

func (t *nativeBoundaryTracker) retainIncomplete(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.incomplete == nil {
		t.incomplete = err
	}
}

func (t *nativeBoundaryTracker) retainedIncomplete() error {
	if t == nil {
		return errors.Join(ErrContainmentIncomplete, errors.New("native generation boundary owner is missing"))
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.incomplete
}

func generationContainmentPanicError(stage string) error {
	return &nativeBoundaryPanicError{stage: stage}
}

type nativeBoundaryPanicError struct {
	stage string
}

func (e *nativeBoundaryPanicError) Error() string {
	return fmt.Sprintf("%s: native generation %s panicked", ErrContainmentIncomplete, e.stage)
}

func (*nativeBoundaryPanicError) Unwrap() error {
	return ErrContainmentIncomplete
}

// recordNativeSettlement fences the mirror for the turn that owns the current
// delivery. The generation binding is what stops a settlement observed on one
// native process from being adopted by a turn running on another.
func (s *agentSession) recordNativeSettlement(delivery *turnDelivery) {
	if delivery == nil {
		return
	}

	s.mu.Lock()
	if s.turnEvents == delivery {
		s.turnNativeSettled = true
	}
	s.mu.Unlock()
}

// generationRouteContext stamps the live turn's route on raw events emitted
// while a dispatched prompt is streaming. An event outside one belongs to the
// generation rather than to a submission, so it carries no route.
func (s *agentSession) generationRouteContext(ctx context.Context, delivery *turnDelivery) context.Context {
	if delivery == nil {
		return ctx
	}

	s.mu.Lock()
	nonce := s.turnNonce
	current := s.turnEvents == delivery
	s.mu.Unlock()

	if !current || nonce == "" {
		return ctx
	}

	return withTurnRoute(ctx, nonce)
}

func (s *agentSession) routeUIRequest(ctx context.Context, outbox *sessionOutbox, request pi.UIRequest) {
	if deferred, overflow := outbox.deferStartupUI(request); deferred {
		return
	} else if overflow {
		s.containGeneration(ctx, outbox, "the session startup prefix reached its retention bound")

		return
	}

	s.routeUIRequestEstablished(ctx, outbox, request)
}

func (s *agentSession) routeUIRequestEstablished(ctx context.Context, outbox *sessionOutbox, request pi.UIRequest) {
	releaseDialog, admitted := s.admitDialogHandler(outbox)
	if !admitted {
		response := pi.UICancelResponse(request.ID)
		if request.Method == uiMethodSelect && strings.HasPrefix(request.Title, pi.PermissionTitleMarker) {
			response = pi.UIValueResponse(request.ID, pi.PermissionOptionDeny)
		}

		s.respondExactUIDialog(ctx, outbox, outbox.client, response)

		return
	}

	if broker := s.agent.providerAuth; broker != nil && strings.HasPrefix(request.Title, pi.AuthTitleMarker) {
		go func() {
			defer recoverAgentGoroutine(ctx, agentLogger(s.agent), "provider auth dialog")
			defer releaseDialog()

			broker.handleAuthDialog(ctx, s, outbox, request)
		}()

		return
	}

	delivery := outbox.foreground()

	// Provider-auth dialogs are excluded above rather than redacted here: a
	// login's presentation and its answers are credential material, and key-name
	// redaction cannot sanitize a secret embedded in prose or a URL.
	s.emitRawPiEvent(s.generationRouteContext(ctx, delivery), request.RawJSON())

	if delivery == nil {
		// A dialog with no dispatched prompt to block is unanswerable by this
		// adapter — an agent-origin cycle states no acceptance and a reserved
		// turn has no identity yet — so it is cancelled rather than left holding
		// pi's extension or attributed to a turn that did not cause it.
		// Cancelling is the load-bearing half; dropping it silently is not.
		if request.IsDialog() {
			s.respondExactUIDialog(ctx, outbox, outbox.client, pi.UICancelResponse(request.ID))
		}

		releaseDialog()

		return
	}

	outbox.mu.Lock()

	current := !outbox.fenced && !outbox.ended && outbox.turn == delivery &&
		(outbox.state == outboxForeground || outbox.state == outboxSettling)
	if !current {
		outbox.mu.Unlock()
		s.respondExactUIDialog(ctx, outbox, outbox.client, pi.UICancelResponse(request.ID))
		releaseDialog()

		return
	}

	dialog := &nativeDialog{
		request: request,
		outbox:  outbox,
		client:  outbox.client,
		done:    releaseDialog,
	}

	select {
	case delivery.uiRequests <- dialog:
		outbox.mu.Unlock()

		go func() {
			<-delivery.done
			dialog.abandon(ctx, s)
		}()
	case <-delivery.done:
		outbox.mu.Unlock()
		dialog.abandon(ctx, s)
	case <-ctx.Done():
		outbox.mu.Unlock()
		dialog.abandon(ctx, s)
	}
}

func (s *agentSession) admitDialogHandler(outbox *sessionOutbox) (func(), bool) {
	if outbox == nil {
		return func() {}, false
	}

	outbox.mu.Lock()
	defer outbox.mu.Unlock()

	if outbox.interactionsClosed || outbox.closing || outbox.fenced || outbox.ended {
		return func() {}, false
	}

	releaseProducer, admitted := outbox.producers.acquire(1)
	if !admitted {
		return func() {}, false
	}

	releaseInteraction, interactionAdmitted := outbox.interactions.acquire(1)
	if !interactionAdmitted {
		releaseProducer()

		return func() {}, false
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			releaseInteraction()
			releaseProducer()
		})
	}, true
}

func (s *agentSession) admitActionHandlers(outbox *sessionOutbox) (func(), func(), bool) {
	if outbox == nil {
		return func() {}, func() {}, false
	}

	outbox.mu.Lock()
	defer outbox.mu.Unlock()

	if outbox.interactionsClosed || outbox.closing || outbox.fenced || outbox.ended {
		return func() {}, func() {}, false
	}

	coordinatorRelease, coordinatorAdmitted := outbox.producers.acquire(1)
	senderRelease, senderAdmitted := outbox.producers.acquire(1)
	coordinatorInteractionRelease, coordinatorInteractionAdmitted := outbox.interactions.acquire(1)
	senderInteractionRelease, senderInteractionAdmitted := outbox.interactions.acquire(1)

	if !coordinatorAdmitted || !senderAdmitted || !coordinatorInteractionAdmitted || !senderInteractionAdmitted {
		coordinatorRelease()
		senderRelease()
		coordinatorInteractionRelease()
		senderInteractionRelease()

		return func() {}, func() {}, false
	}

	return func() {
			coordinatorInteractionRelease()
			coordinatorRelease()
		}, func() {
			senderInteractionRelease()
			senderRelease()
		}, true
}

// stopPump stops the pump goroutine and waits for it, any in-flight dialog
// handlers, and any agent-origin settlement or containment to finish.
func (s *agentSession) stopPump() {
	s.mu.Lock()
	cancel := s.pumpCancel
	done := s.pumpDone
	outbox := s.outbox
	s.pumpCancel = nil
	s.pumpDone = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		<-done
	}

	if done != nil && outbox != nil {
		_ = outbox.producers.wait(context.Background())
	}
}

func (s *agentSession) stopPumpBounded(ctx context.Context) error {
	s.mu.Lock()
	cancel := s.pumpCancel
	done := s.pumpDone
	outbox := s.outbox
	s.pumpCancel = nil
	s.pumpDone = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var err error

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("%w: join native generation pump: %w",
				ErrContainmentIncomplete, ctx.Err())
		}

		if outbox != nil {
			if producerErr := outbox.producers.wait(ctx); producerErr != nil {
				return producerErr
			}
		}
	}

	return err
}

func (s *agentSession) claimPromptForeground(ctx context.Context, delivery *turnDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	for {
		s.mu.Lock()
		outbox := s.outbox
		s.mu.Unlock()

		if err := outbox.waitEstablished(ctx); err != nil {
			return err
		}

		s.mu.Lock()

		if s.closing {
			s.mu.Unlock()

			return unknownSessionError()
		}

		if s.poisonCause != "" {
			s.mu.Unlock()

			return s.admissionFenceError(ctx)
		}

		if s.promptAdmission != nil || s.turnEvents != nil {
			s.mu.Unlock()

			return backpressureError(limitSessionPrompt)
		}

		if s.outbox != outbox {
			s.mu.Unlock()

			continue
		}

		claimErr := outbox.claimPromptAdmission(delivery)
		if claimErr != nil {
			s.mu.Unlock()

			return claimErr
		}

		s.promptAdmission = delivery
		s.mu.Unlock()

		return nil
	}
}

func (s *agentSession) reserveClaimedPromptForeground(
	delivery *turnDelivery,
) (*sessionOutbox, piClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poisonCause != "" {
		return nil, nil, poisonedSessionError(s.poisonCause)
	}

	if s.closing {
		return nil, nil, unknownSessionError()
	}

	if s.promptAdmission != delivery || s.turnEvents != nil {
		return nil, nil, backpressureError(limitSessionPrompt)
	}

	outbox := s.outbox
	if outbox == nil {
		return nil, nil, pi.ErrTransportClosed
	}

	if err := outbox.reserveClaimed(delivery); err != nil {
		return nil, nil, err
	}

	s.promptAdmission = nil
	s.turnEvents = delivery
	s.turnNativeSettled = false

	return outbox, s.client, nil
}

func (s *agentSession) refreshAndReservePrompt(
	ctx context.Context,
	delivery *turnDelivery,
) (*sessionOutbox, piClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	if err := s.refreshMCPTools(ctx); err != nil {
		if fenceErr := s.admissionFenceError(ctx); fenceErr != nil {
			return nil, nil, fenceErr
		}

		return nil, nil, err
	}

	// Reservation holds s.mu while it selects and claims the exact outbox, so a
	// transport refusal belongs to that current generation. Retrying here could
	// spin on an ended router or silently move one prompt onto a later generation.
	return s.reserveClaimedPromptForeground(delivery)
}

// beginPromptDispatch linearizes the prompt command write with exact-generation
// containment. The returned release is held through the native write only.
func (s *agentSession) beginPromptDispatch(
	ctx context.Context,
	outbox *sessionOutbox,
	delivery *turnDelivery,
) (func(), error) {
	if outbox == nil {
		return nil, pi.ErrTransportClosed
	}

	if err := outbox.dispatchMu.lock(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		outbox.dispatchMu.Unlock()

		return nil, unknownSessionError()
	}

	if s.poisonCause != "" {
		cause := s.poisonCause
		s.mu.Unlock()
		outbox.dispatchMu.Unlock()

		return nil, poisonedSessionError(cause)
	}

	if s.outbox != outbox || s.turnEvents != delivery {
		s.mu.Unlock()
		outbox.dispatchMu.Unlock()

		return nil, pi.ErrTransportClosed
	}

	outbox.mu.Lock()
	allowed := !outbox.ended && !outbox.fenced && !outbox.closing && outbox.state == outboxReserved &&
		outbox.turn == delivery && outbox.nativeQueueDepth == 0
	outbox.mu.Unlock()
	s.mu.Unlock()

	if !allowed {
		outbox.dispatchMu.Unlock()

		return nil, backpressureError(limitSessionPrompt)
	}

	return outbox.dispatchMu.Unlock, nil
}

// acceptPromptResponse is the accepted-response watermark. pi.Client invokes it
// on its stdout reader before the next record can reach the pump.
func (s *agentSession) acceptPromptResponse(
	ctx context.Context,
	outbox *sessionOutbox,
	delivery *turnDelivery,
	submission lifecycle.Submission,
) error {
	s.mu.Lock()
	current := s.outbox == outbox && s.turnEvents == delivery
	s.mu.Unlock()

	if !current {
		return pi.ErrTransportClosed
	}

	// The successful native response is the acceptance linearization point.
	// Claim the reserved foreground first, then publish acceptance and its
	// running transition before releasing any deferred raw diagnostics. The
	// client reader does not read another stdout record until this hook returns.
	err := outbox.activate(delivery)
	if err == nil {
		err = s.lifecycleAcceptTurn(ctx, submission)
	}

	if err == nil {
		deferred := outbox.takePreAcceptance(delivery)

		rawCtx := s.generationRouteContext(ctx, delivery)
		for _, event := range deferred {
			s.emitRawPiEvent(rawCtx, event.RawJSON())
			s.logUnroutedRecord(ctx, outbox, event, "its diagnostic frame waited for prompt acceptance")
		}
	}

	if err == nil {
		return nil
	}

	if quarantineErr := s.lifecycleGenerationQuarantine(outbox.generation); quarantineErr != nil {
		return errors.Join(err, quarantineErr)
	}

	s.containGeneration(ctx, outbox, "the native prompt acceptance boundary failed")

	return errors.Join(err, s.poisonedError())
}

// outboxRouter reports the router of the session's current native generation,
// or nil before one exists.
func (s *agentSession) outboxRouter() *sessionOutbox {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.outbox
}
