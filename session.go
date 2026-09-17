package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	// sessionShutdownGrace is how long a pi process gets after SIGTERM before
	// its group is killed.
	sessionShutdownGrace = 2 * time.Second
	// sessionShutdownTimeout bounds one shutdown rung.
	sessionShutdownTimeout = 10 * time.Second
	// sessionAbortTimeout bounds the native abort a cancel or close sends.
	sessionAbortTimeout = 5 * time.Second
	// sessionSettleTimeout bounds one turn's settlement after the native run
	// ended: the stats read, the mirror commit, and the terminal lifecycle
	// event.
	sessionSettleTimeout = 60 * time.Second
)

// session is one ACP session: one pi conversation, driven by one live pi
// process at a time.
type session struct {
	callbacks             sync.WaitGroup
	agent                 *Agent
	id                    acp.SessionId
	nativeID              string
	cwd                   string
	additionalDirectories []string
	options               PiOptions
	rawEvents             *wire.RawEvents
	// agentDir is pi's config root for this session's launches.
	agentDir string
	// sessionFile is the native session file pi writes.
	sessionFile string
	// gate admits one foreground operation at a time: a prompt, a config
	// change, or a restore.
	gate chan struct{}

	mu            sync.Mutex
	runtime       *runtime
	mirrored      int
	model         string
	contextWindow int64
	models        []pi.Model
	thinkingLevel string
	commands      []acp.AvailableCommand
	title         string
	updatedAt     string
	closing       bool
	closeDone     chan struct{}
	closeErr      error
	poison        string
	turn          *turn
	cycle         *cycle
	dialogs       map[string]*dialog

	mirrorMu sync.Mutex
	lcMu     sync.Mutex
	lc       lifecycle.Publisher
}

// runtime is one pi process generation.
type runtime struct {
	proc   *process.Process
	client *pi.Client
	cancel context.CancelFunc
	// done is closed when the pump has stopped routing this generation.
	done chan struct{}
}

// cycle is one foreground run: the work of one accepted prompt, or one
// agent-origin run pi started between prompts.
type cycle struct {
	lifecycle.Cycle
	state cycleState
	// failure records a wrapper extension failure observed during the run.
	failure error
}

type turnEnd int

const (
	turnRunning turnEnd = iota
	turnSettled
	turnTransportEnded
)

// turn is one accepted prompt.
type turn struct {
	cycle
	submission lifecycle.Submission
	// cancel ends the turn's own context. The turn outlives the request that
	// created it, so only the session cancels it.
	cancel context.CancelFunc
	// rt is the process generation running this turn. It is nil until the
	// prompt dispatches, so a turn the session can already cancel is still
	// invisible to every pump.
	rt        *runtime
	accepted  bool
	cancelled bool
	// lcSettled records that the turn published its terminal lifecycle event,
	// so whichever of the turn and the pump acts last fences the incarnation.
	lcSettled  bool
	ended      turnEnd
	settled    chan struct{}
	settleOnce sync.Once
	finished   chan struct{}
}

func (t *turn) settle(end turnEnd) {
	t.settleOnce.Do(func() {
		t.ended = end
		close(t.settled)
	})
}

// dialog is one pending extension UI dialog, cancellable by session/cancel
// and the shutdown ladder.
type dialog struct {
	cancel context.CancelCauseFunc
}

var errDialogCancelled = errors.New("dialog cancelled by the session")

// launch starts one pi process for this session and binds it as the live
// runtime. sessionPath names the native session file to continue, or is
// empty for a new conversation.
func (s *session) launch(ctx context.Context, sessionPath string) (*runtime, error) {
	executable, err := s.agent.ensureExecutable(ctx)
	if err != nil {
		return nil, err
	}

	extensions, err := s.agent.ensureExtensions()
	if err != nil {
		return nil, s.startFailure(ctx, err)
	}

	env, err := s.launchEnvironment()
	if err != nil {
		return nil, err
	}

	if seedErr := pi.WriteSeedFiles(s.agentDir, s.agent.options.SeedFiles); seedErr != nil {
		if refusal := wire.SeedFileRefusal(seedErr); refusal != nil {
			return nil, refusal
		}

		return nil, s.startFailure(ctx, seedErr)
	}

	proc, err := process.Start(ctx, process.Request{
		Executable: executable,
		Args:       pi.Launch{ExtensionPaths: extensions.Paths(), SessionPath: sessionPath}.Args(),
		Env:        env,
		Dir:        s.cwd,
	})
	if err != nil {
		return nil, s.startFailure(ctx, err)
	}

	// The read loop outlives the request that launched pi: the shutdown ladder
	// ends it.
	readCtx, cancelRead := context.WithCancel(context.WithoutCancel(ctx))
	client := pi.NewClient(proc.Stdin(), proc.Stdout())

	if startErr := client.Start(readCtx); startErr != nil {
		cancelRead()

		_ = proc.Kill()
		_ = proc.Close()

		return nil, s.startFailure(ctx, startErr)
	}

	rt := &runtime{proc: proc, client: client, cancel: cancelRead, done: make(chan struct{})}

	s.mu.Lock()
	closing := s.closing

	if !closing {
		s.runtime = rt
	}
	s.mu.Unlock()

	// A close that began while this launch was in flight has already sampled
	// the runtime it will stop, so a process bound now would outlive the
	// session: it is reaped here instead.
	if closing {
		rt.cancel()
		s.releaseRuntime(ctx, rt)

		return nil, wire.UnknownSession()
	}

	go s.pump(context.WithoutCancel(ctx), rt)

	return rt, nil
}

// launchEnvironment builds the merged environment for this session's pi
// process: the inherited environment, the agent overlay, the session env,
// the home when configured, then the adapter's own extension markers.
func (s *session) launchEnvironment() ([]string, error) {
	owned := map[string]string{pi.EnvPermissionMode: s.permissionMode()}
	if len(s.options.ExtraPathDirs) > 0 {
		owned[pi.EnvExtraPathDirs] = strings.Join(s.options.ExtraPathDirs, string(os.PathListSeparator))
	}

	environment := s.agent.environment(s.options.Env, owned)
	environment.ExtraPathDirs = s.options.ExtraPathDirs

	env, err := environment.Build()
	if err != nil {
		return nil, wire.Unsupported(wire.MetaOptionPath(vendor, metaEnvKey))
	}

	return env, nil
}

func (s *session) permissionMode() string {
	if s.options.Permission == "" {
		return pi.PermissionModeAsk
	}

	return s.options.Permission
}

// startFailure maps a failed native launch or setup onto the closed
// off-prompt internal-failure shape. The reason goes to the log.
func (s *session) startFailure(ctx context.Context, err error) error {
	s.agent.log.ErrorContext(ctx, "pi session start failed",
		slog.String("session_id", string(s.id)),
		slog.String("reason", err.Error()),
	)

	return wire.InternalFailure(vendor, internalClassNativeStart)
}

// configureRuntime drives the post-spawn command sequence: retry posture,
// state, thinking level, model, catalog, and commands.
func (s *session) configureRuntime(ctx context.Context, rt *runtime, model string, expectID string) error {
	client := rt.client

	if err := client.SetAutoRetry(ctx, s.options.AutoRetry); err != nil {
		return s.startFailure(ctx, err)
	}

	state, err := client.GetState(ctx)
	if err != nil {
		return s.startFailure(ctx, err)
	}

	if expectID != "" && state.SessionID != expectID {
		return s.startFailure(ctx, fmt.Errorf("native session id %q does not match %q", state.SessionID, expectID))
	}

	if state.SessionID == "" || state.SessionFile == "" {
		return s.startFailure(ctx, errors.New("pi reported no session identity"))
	}

	if s.options.ThinkingLevel != "" {
		if levelErr := client.SetThinkingLevel(ctx, s.options.ThinkingLevel); levelErr != nil {
			return s.startFailure(ctx, levelErr)
		}

		// pi acknowledges a level it does not apply, so the level read back is
		// the one the session advertises.
		state, err = client.GetState(ctx)
		if err != nil {
			return s.startFailure(ctx, err)
		}
	}

	selected := stateModelRef(state)
	contextWindow := int64(0)

	if state.Model != nil {
		contextWindow = state.Model.ContextWindow
	}

	if model != "" {
		ref, _ := pi.ParseModelRef(model)

		chosen, setErr := client.SetModel(ctx, ref.Provider, ref.ID)
		if setErr != nil {
			var commandErr *pi.CommandError
			if errors.As(setErr, &commandErr) {
				return wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
			}

			return s.startFailure(ctx, setErr)
		}

		selected = ref.Provider + "/" + chosen.ID
		contextWindow = chosen.ContextWindow
	}

	models, err := client.GetAvailableModels(ctx)
	if err != nil {
		return s.startFailure(ctx, err)
	}

	commands, err := client.GetCommands(ctx)
	if err != nil {
		return s.startFailure(ctx, err)
	}

	s.mu.Lock()

	s.nativeID = state.SessionID
	if expectID == "" {
		s.id = acp.SessionId(state.SessionID)
	}

	s.sessionFile = state.SessionFile
	if !filepath.IsAbs(s.sessionFile) {
		s.sessionFile = filepath.Join(s.cwd, s.sessionFile)
	}

	s.thinkingLevel = state.ThinkingLevel
	s.model = selected
	s.contextWindow = contextWindow
	s.models = models
	s.commands = availableCommands(commands)
	s.mu.Unlock()

	return nil
}

// unknownModel is what pi reports for the provider and id when it has no
// usable model.
const unknownModel = "unknown"

// stateModelRef normalizes pi's reported model. With no usable credentials pi
// reports a model whose id and provider are both unknown; that is absent.
func stateModelRef(state pi.SessionState) string {
	if state.Model == nil || state.Model.Provider == "" || state.Model.ID == "" {
		return ""
	}

	if state.Model.Provider == unknownModel && state.Model.ID == unknownModel {
		return ""
	}

	return state.Model.Ref()
}

// ensureRuntime returns the live runtime, relaunching pi against the native
// session file when the previous generation is gone.
func (s *session) ensureRuntime(ctx context.Context) (*runtime, error) {
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	if rt != nil {
		return rt, nil
	}

	rt, err := s.launch(ctx, s.sessionFile)
	if err != nil {
		return nil, err
	}

	if configureErr := s.configureRuntime(ctx, rt, "", s.nativeID); configureErr != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, configureErr
	}

	if openErr := s.publishOpen(ctx); openErr != nil {
		return nil, openErr
	}

	return rt, nil
}

// pump routes every native record of one process generation. It is the sole
// reader of the client's channels for the life of the process.
func (s *session) pump(ctx context.Context, rt *runtime) {
	defer close(rt.done)

	events := rt.client.Events()
	uiRequests := rt.client.UIRequests()

	for events != nil || uiRequests != nil {
		select {
		case event, ok := <-events:
			if !ok {
				events = nil

				continue
			}

			s.handleEvent(ctx, rt, event)
		case request, ok := <-uiRequests:
			if !ok {
				uiRequests = nil

				continue
			}

			s.handleUIRequest(rt, request)
		}
	}

	s.runtimeEnded(ctx, rt)
}

// dispatchedTurn returns the turn rt is running, or nil when the session has
// no turn on that generation. A turn the session has installed but not yet
// dispatched, and a turn dispatched on a generation that has been replaced,
// both belong to no live pump. The caller holds s.mu.
func (s *session) dispatchedTurn(rt *runtime) *turn {
	if s.turn == nil || s.turn.rt != rt {
		return nil
	}

	return s.turn
}

// handleEvent attributes one native event to the foreground that owns it: the
// in-flight prompt, the open agent-origin cycle, or, for an agent_start with
// neither, a new agent-origin cycle.
func (s *session) handleEvent(ctx context.Context, rt *runtime, event pi.Event) {
	s.emitRawEvent(ctx, event)

	s.mu.Lock()
	t := s.dispatchedTurn(rt)
	c := s.cycle
	closing := s.closing
	s.mu.Unlock()

	switch {
	case t != nil:
		if bearsWork(event) {
			s.acceptTurn(ctx, t)
		}

		settled, err := s.projectEvent(ctx, rt, &t.cycle, event)
		s.recordFailure(&t.cycle, err)

		if settled {
			t.settle(turnSettled)

			s.finishDelivery(ctx, rt, t)
		}
	case c != nil:
		settled, err := s.projectEvent(ctx, rt, c, event)
		s.recordFailure(c, err)

		if settled {
			s.settleAgentCycle(ctx, rt, c)
		}
	default:
		if _, opens := event.(pi.AgentStartEvent); opens && !closing {
			s.openAgentCycle(ctx)
		}
	}
}

// bearsWork reports whether a record says pi's agent loop is running work a
// cycle owns. Queue reports, compaction, retry pairs, and unmodelled types are
// session-scoped and open nothing.
func bearsWork(event pi.Event) bool {
	switch typed := event.(type) {
	case pi.MessageStartEvent:
		return typed.Message.Role != messageRoleCustom
	case pi.MessageEndEvent:
		return typed.Message.Role != messageRoleCustom
	case pi.AgentStartEvent, pi.AgentEndEvent, pi.AgentSettledEvent,
		pi.TurnStartEvent, pi.TurnEndEvent, pi.MessageUpdateEvent,
		pi.ToolExecutionStartEvent, pi.ToolExecutionUpdateEvent, pi.ToolExecutionEndEvent:
		return true
	}

	return false
}

// openAgentCycle opens the foreground for work pi began with no prompt in
// flight, such as a follow-up an operator extension queued.
func (s *session) openAgentCycle(ctx context.Context) {
	c := &cycle{Cycle: lifecycle.Cycle{Origin: lifecycle.CauseActivity}}
	c.state.tools = make(map[string]*toolState)

	if err := s.lc.OpenAgentCycle(ctx, &c.Cycle); err != nil {
		s.agent.log.ErrorContext(ctx, "open agent-origin cycle failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))

		return
	}

	s.mu.Lock()
	s.cycle = c
	s.mu.Unlock()
}

// settleAgentCycle runs the agent-origin settlement on the pump: usage, the
// mirror commit, then the terminal idle.
func (s *session) settleAgentCycle(ctx context.Context, rt *runtime, c *cycle) {
	settleCtx, cancel := context.WithTimeout(ctx, sessionSettleTimeout)
	defer cancel()

	s.emitUsage(settleCtx, &c.state, nil)

	if err := s.commitMirror(settleCtx); err != nil {
		// The incarnation cannot publish what the store does not hold, so it
		// ends with its generation: the next operation relaunches pi and opens
		// a new stream.
		s.lc.Fence()
		s.dropRuntime(rt)
		s.agent.log.ErrorContext(settleCtx, "mirror commit after agent-origin cycle failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
	}

	verdict := s.judgeCycle(c, false)

	if err := s.lc.Idle(settleCtx, c.Cycle, verdict.stopReason, verdict.outcome); err != nil {
		s.agent.log.ErrorContext(settleCtx, "terminal idle for agent-origin cycle failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
	}

	s.mu.Lock()
	if s.cycle == c {
		s.cycle = nil
	}
	s.mu.Unlock()
}

// runtimeEnded records that a process generation stopped producing events. The
// turn this generation was running learns its transport ended; the generation
// the session is still bound to also hands back its open agent-origin cycle,
// its dialogs, and the incarnation fence. A generation the session already
// replaced or dropped owns none of those. The child is always reaped.
func (s *session) runtimeEnded(ctx context.Context, rt *runtime) {
	s.mu.Lock()
	bound := s.runtime == rt
	closing := s.closing
	t := s.dispatchedTurn(rt)

	var c *cycle

	if bound {
		s.runtime = nil

		if !closing {
			// While closing, the shutdown ladder owns the open cycle so exactly
			// one site terminalizes it.
			c = s.cycle
			s.cycle = nil
		}
	}

	// A turn that already published its terminal event leaves the fence to the
	// generation that carried it; one still settling fences itself.
	fence := bound && !closing && (t == nil || t.lcSettled)
	s.mu.Unlock()

	if bound {
		s.cancelDialogs()
		s.callbacks.Wait()
	}

	if c != nil {
		_ = s.lc.Idle(ctx, c.Cycle, "", lifecycle.OutcomeFailed)
	}

	if t != nil {
		t.settle(turnTransportEnded)
		t.cancel()
	}

	if fence {
		s.lc.Fence()
	}

	s.releaseRuntime(ctx, rt)
}

// releaseRuntime reaps and releases a generation the pump has finished with.
// The child is signalled first so a harness that closed its stdout while still
// running never outlives the session that launched it.
func (s *session) releaseRuntime(ctx context.Context, rt *runtime) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
	defer cancel()

	if err := rt.proc.Shutdown(shutdownCtx, sessionShutdownGrace); err != nil {
		_ = rt.proc.Kill()
	}

	_ = rt.proc.Close()
}

// dropRuntime ends the binding to a generation this session can no longer
// publish for and signals its child. The pump reaps it when its stream ends.
func (s *session) dropRuntime(rt *runtime) {
	s.mu.Lock()
	if s.runtime == rt {
		s.runtime = nil
	}
	s.mu.Unlock()

	_ = rt.proc.Kill()
}

// stopRuntime runs the native rungs of the shutdown ladder for one
// generation: signal the process group, wait for the root, join the pump, and
// release the pipes.
func (s *session) stopRuntime(ctx context.Context, rt *runtime) {
	shutdownCtx, cancel := context.WithTimeout(ctx, sessionShutdownTimeout)
	defer cancel()

	if err := rt.proc.Shutdown(shutdownCtx, sessionShutdownGrace); err != nil {
		_ = rt.proc.Kill()
	}

	select {
	case <-rt.done:
	case <-shutdownCtx.Done():
		rt.cancel()
	}

	// Closing the pipes ends the read loop of a pump that outlived the
	// shutdown, and the join guarantees nothing still writes the cycle state
	// the caller settles from.
	_ = rt.proc.Close()
	<-rt.done

	s.mu.Lock()
	if s.runtime == rt {
		s.runtime = nil
	}
	s.mu.Unlock()
}

// abort interrupts the native run under a bounded context detached from the
// caller's cancellation.
func (s *session) abort(ctx context.Context, rt *runtime) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionAbortTimeout)
	defer cancel()

	if err := rt.client.Abort(abortCtx); err != nil {
		s.agent.log.DebugContext(abortCtx, "pi abort failed", slog.String("session_id", string(s.id)))
	}
}

// abortAsync interrupts the native run from the pump. The abort waits on a
// response only the pump can read, so it runs as session-owned work the
// shutdown ladder joins. A session that is closing or no longer bound to rt is
// already joining its callbacks and has nothing left to interrupt.
func (s *session) abortAsync(ctx context.Context, rt *runtime) {
	s.mu.Lock()
	if s.closing || s.runtime != rt {
		s.mu.Unlock()

		return
	}

	s.callbacks.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.callbacks.Done()

		s.abort(ctx, rt)
	}()
}

// cancel implements session/cancel: it cancels the in-flight turn, resolves
// its pending dialogs, and interrupts pi. It is a silent no-op with no turn.
func (s *session) cancel(ctx context.Context) {
	s.mu.Lock()
	t := s.turn
	rt := s.runtime

	if t == nil || t.cancelled {
		s.mu.Unlock()

		return
	}

	t.cancelled = true
	t.cancel()
	s.mu.Unlock()

	s.cancelDialogs()

	if rt != nil {
		s.abort(ctx, rt)
	}
}

func (s *session) registerDialog(id string, cancel context.CancelCauseFunc) func() {
	s.mu.Lock()
	if s.closing || s.runtime == nil || (s.turn != nil && s.turn.cancelled) {
		s.mu.Unlock()
		cancel(errDialogCancelled)

		return func() {}
	}

	if s.dialogs == nil {
		s.dialogs = make(map[string]*dialog)
	}

	s.callbacks.Add(1)

	entry := &dialog{cancel: cancel}
	s.dialogs[id] = entry
	s.mu.Unlock()

	return sync.OnceFunc(func() {
		defer s.callbacks.Done()

		s.mu.Lock()
		if s.dialogs[id] == entry {
			delete(s.dialogs, id)
		}
		s.mu.Unlock()
	})
}

func (s *session) cancelDialogs() {
	s.mu.Lock()

	dialogs := make([]*dialog, 0, len(s.dialogs))
	for _, d := range s.dialogs {
		dialogs = append(dialogs, d)
	}
	s.mu.Unlock()

	for _, d := range dialogs {
		d.cancel(errDialogCancelled)
	}
}

// admissionError reports why a session admits no further work: it is closing
// or poisoned.
func (s *session) admissionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.poison != "":
		return wire.SessionPoisoned(vendor, s.poison)
	case s.closing:
		return wire.UnknownSession()
	default:
		return nil
	}
}

// poisonSession fences every operation but close and delete.
func (s *session) poisonSession(ctx context.Context, cause string) {
	s.mu.Lock()

	first := s.poison == ""
	if first {
		s.poison = cause
	}
	s.mu.Unlock()

	if !first {
		return
	}

	s.agent.log.ErrorContext(ctx, "pi session poisoned",
		slog.String("session_id", string(s.id)), slog.String("cause", cause))
	s.clearCommands(ctx)
}

// acquireGate admits one foreground operation. limit names the backpressure
// token a refusal carries.
func (s *session) acquireGate(limit string) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return nil, wire.UnknownSession()
	}

	return wire.AcquireSessionGate(s.gate, limit)
}

// close runs the shutdown ladder: mark closed, resolve pending dialogs and the
// in-flight turn, stop the process, commit the owed rows, terminalize what the
// stream still owns, and fence it.
func (s *session) close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		<-done

		return s.closeErr
	}

	s.closing = true
	s.closeDone = make(chan struct{})
	t := s.turn
	rt := s.runtime

	if t != nil {
		t.cancelled = true
		t.cancel()
	}
	s.mu.Unlock()

	s.cancelDialogs()
	s.callbacks.Wait()

	if t != nil {
		if rt != nil {
			s.abort(ctx, rt)
		}

		select {
		case <-t.finished:
		case <-time.After(sessionSettleTimeout):
		}
	}

	if rt != nil {
		s.stopRuntime(ctx, rt)
	}

	var errs []error

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	if err := s.commitMirror(commitCtx); err != nil {
		errs = append(errs, err)
	}

	s.mu.Lock()
	c := s.cycle
	s.cycle = nil
	s.mu.Unlock()

	if len(errs) == 0 && c != nil {
		if err := s.lc.Idle(commitCtx, c.Cycle, lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled); err != nil {
			errs = append(errs, err)
		}
	}

	s.lc.Fence()

	s.mu.Lock()
	s.closeErr = errors.Join(errs...)
	close(s.closeDone)
	s.mu.Unlock()

	return s.closeErr
}

// nativeRecord preserves event and dialog order while a completed foreground
// turn commits its mirror and reads its final statistics.
type nativeRecord struct {
	event   pi.Event
	request *pi.UIRequest
}

func (s *session) finishDelivery(ctx context.Context, rt *runtime, t *turn) {
	events := rt.client.Events()
	requests := rt.client.UIRequests()

	var pending []nativeRecord

	for {
		select {
		case <-t.finished:
			for _, record := range pending {
				if record.request != nil {
					s.handleUIRequest(rt, *record.request)
				} else {
					s.handleEvent(ctx, rt, record.event)
				}
			}

			return
		case <-rt.proc.Done():
			return
		case event, ok := <-events:
			if !ok {
				events = nil

				continue
			}

			pending = append(pending, nativeRecord{event: event})
		case request, ok := <-requests:
			if !ok {
				requests = nil

				continue
			}

			pending = append(pending, nativeRecord{request: &request})
		}
	}
}
