package piacp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestStateModelRef(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", stateModelRef(pi.SessionState{}))
	require.Equal(t, "", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "unknown", ID: "unknown"}}))
	require.Equal(t, "a/b", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "a", ID: "b"}}))
}

func TestPermissionModeDefault(t *testing.T) {
	t.Parallel()

	s := &session{}
	require.Equal(t, pi.PermissionModeAsk, s.permissionMode())

	s.options.Permission = pi.PermissionModeAllow
	require.Equal(t, pi.PermissionModeAllow, s.permissionMode())
}

func TestLateDialogAfterCancellationIsRefused(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"cancelled", "closed", "disconnected"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s := &session{runtime: &runtime{}, turn: &turn{}}
			switch state {
			case "cancelled":
				s.turn.cancelled = true
			case "closed":
				s.closing = true
			case "disconnected":
				s.runtime = nil
			}
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			release := s.registerDialog("late-native-request", cancel)
			require.ErrorIs(t, context.Cause(ctx), errDialogCancelled)
			release()
			s.callbacks.Wait()
		})
	}
}

// The extension abort runs as session-owned work the shutdown ladder joins, so
// it is registered only while the session still routes the generation it
// interrupts. A refused registration adds nothing to the callbacks a closing
// session is already waiting on, and never reaches the runtime.
func TestExtensionAbortRegistrationIsRefused(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"closed", "replaced", "disconnected"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s := &session{agent: NewAgent(testOptions(t)...)}
			rt := &runtime{}
			s.runtime = rt
			switch state {
			case "closed":
				s.closing = true
			case "replaced":
				s.runtime = &runtime{}
			case "disconnected":
				s.runtime = nil
			}
			// The generation carries no client, so an abort that is registered
			// anyway fails on it.
			s.abortAsync(t.Context(), rt)
			s.callbacks.Wait()
		})
	}
}

// A generation the session no longer routes ends without settling the turn or
// ending the cycle the live generation owns. Its own child is still reaped.
func TestRuntimeEndedLeavesTheLiveGenerationAlone(t *testing.T) {
	t.Parallel()

	s := &session{agent: NewAgent(testOptions(t)...)}
	turnCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	live := &runtime{}
	running := &turn{rt: live, cancel: cancel, settled: make(chan struct{}), finished: make(chan struct{})}
	s.runtime = live
	s.turn = running
	s.cycle = &cycle{}

	s.runtimeEnded(t.Context(), &runtime{proc: startedProcess(t), done: make(chan struct{})})

	require.NoError(t, turnCtx.Err(), "the replaced generation cancelled a turn it never ran")
	require.Equal(t, turnRunning, running.ended)
	require.Same(t, live, s.runtime)
	require.NotNil(t, s.cycle)
}

// startedProcess is a real short-lived child, for a generation a test hands to
// code that reaps it.
func startedProcess(t *testing.T) *process.Process {
	t.Helper()

	proc, err := process.Start(t.Context(), process.Request{
		Executable: "/usr/bin/true",
	})
	require.NoError(t, err)

	return proc
}

// A close that begins while a launch is in flight has already sampled the
// runtime it will stop, so the launch binds nothing and answers unknown
// session.
func TestLaunchWhileClosingBindsNothing(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.id = "closing"
	s.closing = true

	rt, err := s.launch(t.Context(), "")
	require.Nil(t, rt)
	require.Equal(t, wire.UnknownSession(), err)
	require.Nil(t, s.runtime)
}

// A released generation leaves no child behind: the process is signalled and
// reaped before the release returns.
func TestReleaseRuntimeReapsTheChild(t *testing.T) {
	t.Parallel()

	s := &session{agent: NewAgent(testOptions(t)...)}

	proc, err := process.Start(t.Context(), process.Request{Executable: "/bin/sleep", Args: []string{"30"}})
	require.NoError(t, err)

	s.releaseRuntime(t.Context(), &runtime{proc: proc, cancel: func() {}, done: make(chan struct{})})

	select {
	case <-proc.Done():
	default:
		t.Fatal("the child was released without being reaped")
	}
}

// Without an executable path, the pi name is resolved on the base PATH.
func TestDefaultExecutableResolvesOnBasePath(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	require.NoError(t, os.Symlink(os.Args[0], filepath.Join(base, "pi")))

	h := newHarness(t, WithExecutablePath(""), WithEnv(map[string]string{fakePiEnv: "1", "PATH": base}))
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

func TestCloseJoinsFirstMirrorAndFencesOpening(t *testing.T) {
	t.Parallel()
	for _, agentClose := range []bool{false, true} {
		name := "close_session"
		if agentClose {
			name = "close_agent"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			t.Cleanup(release)
			store.block.Store(true)
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			rec := newRecorder()
			a.attach(rec, nil)
			request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&request)
			_, err := a.Initialize(t.Context(), request)
			require.NoError(t, err)
			cwd := t.TempDir()
			created := make(chan error, 1)
			go func() {
				_, createErr := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
				created <- createErr
			}()
			var key acpcore.SessionKey
			select {
			case key = <-store.entered:
			case <-time.After(testTimeout):
				t.Fatal("creation did not reach its first mirror")
			}
			s, err := a.session(t.Context(), acp.SessionId(key.SessionID))
			require.NoError(t, err)
			s.mu.Lock()
			rt := s.runtime
			s.mu.Unlock()
			closed := make(chan error, 1)
			go func() {
				if agentClose {
					closed <- a.Close()

					return
				}
				_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: acp.SessionId(key.SessionID)})
				closed <- err
			}()
			require.Eventually(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()

				return s.closing
			}, testTimeout, time.Millisecond)
			select {
			case err := <-closed:
				t.Fatalf("close returned while the first mirror was blocked: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			release()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(testTimeout):
				t.Fatal("close did not join creation")
			}
			select {
			case err := <-created:
				require.Error(t, err, "a closing session must refuse its opening publication")
			case <-time.After(testTimeout):
				t.Fatal("creation did not release its gate before cleanup")
			}
			require.False(t, s.lc.Active())
			before := len(rec.snapshot())
			require.Error(t, s.openStream(t.Context(), rt))
			require.Len(t, rec.snapshot(), before, "closed session published commands or a lifecycle snapshot")
			require.False(t, s.lc.Active())
		})
	}
}

func TestOpeningRejectsReplacedNativeGeneration(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	stale := s.runtime
	s.mu.Unlock()
	transport, meta := prepareOpeningResponse(t)
	a.attach(rec, transport)
	require.NoError(t, a.scheduleOpen(transport.RequestContext(t.Context(), meta), s))
	s.stopRuntime(t.Context(), stale)
	require.False(t, s.lc.Active())
	fresh, err := s.ensureRuntime(t.Context())
	require.NoError(t, err)
	require.NotSame(t, stale, fresh)
	require.True(t, s.lc.Active())
	before := len(rec.snapshot())
	require.Error(t, s.openStream(t.Context(), stale))
	require.Len(t, rec.snapshot(), before, "stale deferred opening published on the replacement generation")
	require.True(t, s.lc.Active(), "stale opening fenced the replacement stream")
	finishOpeningResponse(t, transport, s.id)
	require.Len(t, rec.snapshot(), before, "stale hook published on the replacement generation")
	current, err := a.session(t.Context(), s.id)
	require.NoError(t, err)
	require.Same(t, s, current)
	require.True(t, s.lc.Active(), "stale hook closed the replacement stream")
}

// openingCallbackClient exercises a synchronous embedded callback into admission.
type openingCallbackClient struct {
	*recorder
	agent *Agent
}

func (c *openingCallbackClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if err := c.agent.Cancel(ctx, acp.CancelNotification{SessionId: notification.SessionId}); err != nil {
		return err
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestOpeningAllowsSynchronousSessionCallback(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(&openingCallbackClient{recorder: rec, agent: a}, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.NotEmpty(t, rec.snapshot())
}

func prepareOpeningResponse(t *testing.T) (*wire.Transport, map[string]any) {
	t.Helper()
	transport := wire.NewTransport(strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/new\",\"params\":{}}\n"), io.Discard)
	t.Cleanup(transport.Close)
	transport.Start()
	inbound, err := io.ReadAll(transport.Reader())
	require.NoError(t, err)

	var frame struct {
		Params acp.NewSessionRequest `json:"params"`
	}

	require.NoError(t, json.Unmarshal(inbound, &frame))

	return transport, frame.Params.Meta
}

func finishOpeningResponse(t *testing.T, transport *wire.Transport, id acp.SessionId) {
	t.Helper()
	_, err := transport.Writer().Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	require.NoError(t, transport.AwaitSession(ctx, id))
}

func TestDeferredOpeningFailureDetachesSession(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))...)
	t.Cleanup(func() { _ = a.Close() })
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	transport, meta := prepareOpeningResponse(t)
	a.attach(&firstOpenFailureClient{recorder: newRecorder()}, transport)
	newRequest := wire.NewSessionRequest(t.TempDir())
	newRequest.Meta = meta
	created, err := a.NewSession(t.Context(), newRequest)
	require.NoError(t, err)
	a.mu.Lock()
	s := a.sessions[created.SessionId]
	a.mu.Unlock()
	require.NotNil(t, s)
	finishOpeningResponse(t, transport, created.SessionId)
	require.False(t, s.lc.Active())
	a.mu.Lock()
	_, installed := a.sessions[created.SessionId]
	a.mu.Unlock()
	require.False(t, installed, "failed deferred publication retained the active slot")
	s.mu.Lock()
	closed := s.closing
	s.mu.Unlock()
	require.True(t, closed)
}

func TestRuntimeDrainCompletesBeforeReplacement(t *testing.T) {
	t.Parallel()
	for _, finishing := range []bool{false, true} {
		name := "idle"
		if finishing {
			name = "prompt settling"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			proc, err := process.Start(t.Context(), process.Request{Executable: "/usr/bin/true"})
			require.NoError(t, err)
			defer proc.Close()
			old := &runtime{proc: proc, done: make(chan struct{})}

			s := &session{runtime: old}
			var running *turn
			if finishing {
				running = &turn{rt: old, cancel: func() {}, settled: make(chan struct{})}
				running.settle(turnSettled)
				s.turn = running
			}
			var streams []string
			deliver := func(_ context.Context, envelope map[string]any) error {
				streamID, ok := envelope["streamId"].(string)
				require.True(t, ok)
				streams = append(streams, streamID)

				return nil
			}
			negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true}
			require.NoError(t, s.lc.Open(t.Context(), "old", negotiated, deliver))
			callbackCtx, cancelCallback := context.WithCancelCause(t.Context())
			defer cancelCallback(nil)
			release := s.registerDialog("pending", cancelCallback)
			defer release()
			go func() { s.runtimeEnded(t.Context(), old); close(old.done) }()
			select {
			case <-callbackCtx.Done():
			case <-time.After(testTimeout):
				t.Fatal("runtime did not cancel its pending callback")
			}
			s.mu.Lock()
			bound := s.runtime
			s.mu.Unlock()
			if bound != old {
				release()
				<-old.done
				t.Fatal("runtime released its binding before its callback drained")
			}
			if running != nil {
				require.False(t, s.claimFence(running, old), "the draining runtime owns this turn's fence")
			}
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			cancelRequest()
			returned, requestErr := s.ensureRuntime(requestCtx)
			release()
			select {
			case <-old.done:
			case <-time.After(testTimeout):
				t.Fatal("runtime did not finish teardown")
			}
			require.Nil(t, returned, "an operation cannot acquire the runtime being drained")
			require.ErrorIs(t, requestErr, context.Canceled)
			require.NoError(t, s.lc.Open(t.Context(), "replacement", negotiated, deliver))
			require.True(t, s.lc.Active())
			require.Equal(t, []string{"old", "replacement"}, streams)
		})
	}
}

func TestUsageEndpointDirectoryRemovedAfterLaunchFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"start", "exit", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			scratch := t.TempDir()
			marker := filepath.Join(t.TempDir(), "started")
			options := []Option{WithScratchDir(scratch)}
			switch failure {
			case "exit":
				options = append(options, WithExecutablePath("/usr/bin/true"))
			case "cancel":
				options = append(options, WithEnv(map[string]string{fakePiEnv: "1", fakePiEnvUsageHold: marker}))
			}
			a := NewAgent(testOptions(t, options...)...)
			s := a.newSession(sessionStart{cwd: t.TempDir()})
			if failure == "start" {
				s.cwd = filepath.Join(t.TempDir(), "missing")
			}
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.launch(ctx, "")
				done <- err
			}()
			if failure == "cancel" {
				require.Eventually(t, func() bool {
					_, err := os.Stat(marker)

					return err == nil
				}, testTimeout, time.Millisecond)
				cancel()
			}
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(testTimeout):
				t.Fatal("launch did not return")
			}
			paths, err := filepath.Glob(filepath.Join(scratch, "acp-go-pi-usage-*"))
			require.NoError(t, err)
			require.Empty(t, paths, "failed launch retained a private usage directory")
		})
	}
}

func TestLaunchFailureLogsNativeStderr(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	a := NewAgent(testOptions(t,
		WithEnv(map[string]string{fakePiEnv: "1", fakePiEnvStartupDeath: "Error: Failed to load extension"}),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)...)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	_, err := s.launch(t.Context(), "")
	require.Equal(t, "pi_internal_failure", requestErrorData(t, err)["error"])
	require.Contains(t, logs.String(), "exited before readiness: Error: Failed to load extension")
}

func TestUsageEndpointDirectoryRemovedWhenRuntimeStops(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	rt, err := s.launch(t.Context(), "")
	require.NoError(t, err)
	dir := filepath.Dir(rt.usage.File)
	require.DirExists(t, dir)
	require.FileExists(t, rt.usage.File)
	require.NotEmpty(t, rt.usage.URL)
	s.stopRuntime(t.Context(), rt)
	require.NoDirExists(t, dir)
}

func TestUsageEndpointReadinessDrainsStartupDialogs(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithEnv(map[string]string{fakePiEnv: "1", fakePiEnvStartupDialog: "1"}))...)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	rt, err := s.launch(ctx, "")
	require.NoError(t, err, "startup UI must receive its cancellation before usage readiness")
	s.stopRuntime(t.Context(), rt)
	require.NoDirExists(t, filepath.Dir(rt.usage.File))
}

type cancellingBackgroundClient struct {
	*recorder
	agent          *Agent
	terminalOnly   bool
	terminalCalled bool
}

func (c *cancellingBackgroundClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.terminalOnly {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["state"] != "idle" {
			return c.recorder.SessionUpdate(ctx, notification)
		}
		c.terminalCalled = true
	}
	done := make(chan error, 1)
	go func() { done <- c.agent.Cancel(ctx, wire.CancelRequest(notification.SessionId)) }()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		return context.DeadlineExceeded
	}
}

func TestBackgroundPublicationAllowsCancelCallback(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	a.attach(&cancellingBackgroundClient{recorder: rec, agent: a}, nil)
	s.openAgentCycle(t.Context(), rt)
	a.attach(rec, nil)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.NoError(t, s.cycleFailure(c))
	require.True(t, s.cycleCancelled(c))
	s.settleAgentCycle(t.Context(), rt, c)
}

func TestCancelAgentOriginResolvesDialogsAndSettlesCancelled(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	s.openAgentCycle(t.Context(), rt)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	dialogCtx, cancelDialog := context.WithCancelCause(t.Context())
	defer cancelDialog(nil)
	unregister := s.registerDialog("permission", cancelDialog)
	require.NoError(t, s.lc.ActionPending(t.Context(), c.Cycle, "permission", lifecycle.ActionPermission))
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	require.ErrorIs(t, context.Cause(dialogCtx), errDialogCancelled)
	unregister()
	lateCtx, cancelLate := context.WithCancelCause(t.Context())
	defer cancelLate(nil)
	release := s.registerDialog("late", cancelLate)
	release()
	require.ErrorIs(t, context.Cause(lateCtx), errDialogCancelled)
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	prompt := wire.TextPromptRequest(created.SessionId, "HELLO")
	prompt.Meta = promptMeta(1)
	_, err = a.Prompt(t.Context(), prompt)
	require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	s.settleAgentCycle(t.Context(), rt, c)
	s.mu.Lock()
	active := s.cycle
	s.mu.Unlock()
	require.Nil(t, active)
	cancelled := false
	for _, notification := range rec.snapshot() {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "cancelled" {
			cancelled = true
		}
	}
	require.True(t, cancelled)
}

func TestTerminalPublicationCannotInterruptNextCycle(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	terminalClient := &cancellingBackgroundClient{recorder: rec, agent: a, terminalOnly: true}
	a.attach(terminalClient, nil)
	s.openAgentCycle(t.Context(), rt)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.NoError(t, s.cycleFailure(c))
	require.False(t, s.cycleCancelled(c))
	s.settleAgentCycle(t.Context(), rt, c)
	require.False(t, s.cycleCancelled(c))
	require.True(t, terminalClient.terminalCalled)
	s.callbacks.Wait()
	a.attach(rec, nil)
}
