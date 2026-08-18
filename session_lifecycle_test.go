package piacp

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

func withSessionCloseTurnWait(t *testing.T, wait time.Duration) {
	t.Helper()

	original := sessionCloseTurnWait
	sessionCloseTurnWait = wait

	t.Cleanup(func() { sessionCloseTurnWait = original })
}

// TestSessionCloseWaitsForTheInFlightTurn proves close blocks on the session's
// single turn admission rather than tearing the native generation down beneath
// a live turn. Close signals the turn's cancel immediately before it waits, so
// observing that signal with the shutdown ladder still untouched is a
// linearized proof that the wait is real, and the admission is balanced after.
func TestSessionCloseWaitsForTheInFlightTurn(t *testing.T) {
	process := newStubProcess(false)
	session := &agentSession{
		agent:       NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
		id:          "id",
		proc:        process,
		sessionRoot: t.TempDir(),
	}

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)

	waiting := make(chan struct{})
	session.cancel = func() { close(waiting) }

	closed := make(chan error, 1)
	go func() { closed <- session.Close(context.Background()) }()

	<-waiting
	require.Zero(t, process.shutdownCalls, "close reached the shutdown ladder with a turn in flight")

	release()
	require.NoError(t, <-closed)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
	require.Empty(t, session.turn, "close left the turn admission unbalanced")
}

// TestSessionCloseSurrendersAnUnreleasedTurn proves the wait is bounded: a turn
// that never releases costs close its bound and is reported, rather than
// blocking teardown forever.
func TestSessionCloseSurrendersAnUnreleasedTurn(t *testing.T) {
	withSessionCloseTurnWait(t, time.Millisecond)

	session := &agentSession{
		agent:       NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
		id:          "id",
		proc:        newStubProcess(false),
		sessionRoot: t.TempDir(),
	}

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	t.Cleanup(release)

	require.ErrorIs(t, session.Close(t.Context()), context.DeadlineExceeded)
}

// TestSessionCloseIsTerminalAndIdempotent proves close has exactly one owner: a
// second caller runs no teardown of its own, reports the first caller's result,
// and the session admits no prompt, MCP refresh, or relaunch from the moment
// the first caller claims it.
func TestSessionCloseIsTerminalAndIdempotent(t *testing.T) {
	spawns := 0
	agent := newStubClientAgent(t, newStubPiClient())
	realStart := agent.startPiProcess
	agent.startPiProcess = func(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return realStart(ctx, spec)
	}

	tearingDown := make(chan struct{})
	release := make(chan struct{})
	process := newStubProcess(false)
	process.shutdownFunc = func(context.Context) error {
		close(tearingDown)
		<-release

		return errors.New("shutdown")
	}

	session := &agentSession{agent: agent, id: "id", proc: process, sessionRoot: t.TempDir()}
	prepareRelaunchFixture(t, session)

	first := make(chan error, 1)
	go func() { first <- session.Close(context.Background()) }()

	<-tearingDown

	_, err := session.acquireTurn(t.Context())
	requireInvalidParams(t, err)
	requireInvalidParams(t, session.refreshMCPTools(t.Context()))
	requireInvalidParams(t, session.ensureProcessAlive(t.Context()))
	requireInvalidParams(t, session.relaunchProcess(t.Context()))

	second := make(chan error, 1)
	go func() { second <- session.Close(context.Background()) }()

	close(release)

	firstErr := <-first
	require.Error(t, firstErr)
	require.Equal(t, firstErr, <-second)
	require.Equal(t, 1, process.shutdownCalls)
	require.Equal(t, 1, process.closeCalls)
	require.Zero(t, spawns, "a closing session spawned a pi process")
}

// TestClosedSessionOwnsNoSurvivingProcess proves the post-close relaunch door
// is shut: the closed process reached its containment boundary exactly once and
// no path spawns a replacement the session no longer tracks.
func TestClosedSessionOwnsNoSurvivingProcess(t *testing.T) {
	spawns := 0
	agent := newStubClientAgent(t, newStubPiClient())
	realStart := agent.startPiProcess
	agent.startPiProcess = func(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return realStart(ctx, spec)
	}

	process := newStubProcess(true)
	session := &agentSession{agent: agent, id: "id", proc: process, sessionRoot: t.TempDir()}
	prepareRelaunchFixture(t, session)

	require.NoError(t, session.Close(t.Context()))
	require.Equal(t, 1, process.closeCalls)

	_, err := session.acquireTurn(t.Context())
	requireInvalidParams(t, err)
	requireInvalidParams(t, session.ensureProcessAlive(t.Context()))
	requireInvalidParams(t, session.refreshMCPTools(t.Context()))
	requireInvalidParams(t, session.relaunchProcess(t.Context()))
	require.Zero(t, spawns)
	require.Equal(t, 1, process.closeCalls)
}

// TestRefreshMCPToolsRefusesACloseClaimedMidCheck proves the refresh guard sits
// inside the critical section that mutates the session rather than in front of
// it: a close claimed after the liveness check still stops the refresh from
// shutting the native process down under teardown.
func TestRefreshMCPToolsRefusesACloseClaimedMidCheck(t *testing.T) {
	process := newStubProcess(false)
	session := &agentSession{
		agent:             NewAgent(WithLogger(slog.New(slog.DiscardHandler))),
		id:                "id",
		proc:              process,
		mcpRefreshPending: true,
	}
	process.onExited = func() {
		session.mu.Lock()
		session.closing = true
		session.mu.Unlock()
	}

	requireInvalidParams(t, session.refreshMCPTools(t.Context()))
	require.Zero(t, process.shutdownCalls)
	require.True(t, session.mcpRefreshPending)
}

// TestRelaunchRefusesToAdoptAfterCloseBegins proves the two mid-relaunch
// windows are closed inside the critical sections that publish them: a native
// root admitted after close begins is handed straight back, and a process
// spawned before close claimed the session is contained here rather than
// surviving as a pi process nothing owns.
func TestRelaunchRefusesToAdoptAfterCloseBegins(t *testing.T) {
	t.Run("native root", func(t *testing.T) {
		var session *agentSession

		releases := 0
		agent := NewAgent(
			testContainmentOption(),
			WithLogger(slog.New(slog.DiscardHandler)),
			WithRuntimeResourceHooks(RuntimeResourceHooks{
				AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
					session.mu.Lock()
					session.closing = true
					session.mu.Unlock()

					return func() { releases++ }, nil
				},
			}),
		)
		session = &agentSession{agent: agent, id: "id"}
		prepareRelaunchFixture(t, session)

		requireInvalidParams(t, session.relaunchProcess(t.Context()))
		require.Equal(t, 1, releases)
	})

	t.Run("relaunched process", func(t *testing.T) {
		var session *agentSession

		agent := newStubClientAgent(t, newStubPiClient())
		relaunched := newStubProcess(false)
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			session.mu.Lock()
			session.closing = true
			session.mu.Unlock()

			return relaunched, newStubPiClient(), nil
		}
		session = &agentSession{agent: agent, id: "id"}
		prepareRelaunchFixture(t, session)

		requireInvalidParams(t, session.relaunchProcess(t.Context()))
		require.Equal(t, 1, relaunched.killCalls)
		require.Equal(t, 1, relaunched.closeCalls)

		session.mu.Lock()
		defer session.mu.Unlock()
		require.Nil(t, session.proc, "a closing session adopted a relaunched process")
	})
}

func TestRuntimeRelaunchRefusesDurableHomeWithExplicitIsolation(t *testing.T) {
	sessionRoot := t.TempDir()
	agent := NewAgent(testProcessIsolationOption(), WithHome(filepath.Join(t.TempDir(), "home")))
	session := &agentSession{agent: agent, sessionRoot: sessionRoot}

	_, err := session.nextRuntimeLaunch(pi.LaunchSpec{}, "")
	require.ErrorContains(t, err, "durable pi agent directory is unavailable with explicit process isolation")
}

func TestSessionRetainsIncompleteNativeContainment(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	session := &agentSession{agent: agent}
	session.recordNativeContainment(errors.New("ordinary native error"))
	require.NoError(t, session.nativeContainmentError())
	require.NoError(t, agent.nativeContainmentError())

	session.recordNativeContainment(pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, session.nativeContainmentError(), pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.nativeContainmentError(), pi.ErrProcessContainmentIncomplete)
}

func TestSessionTurnLifecycleBranches(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithTurnTimeout(time.Second))
	client := newStubPiClient()
	session := &agentSession{agent: agent, id: "id", client: client, proc: newStubProcess(false)}

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	_, err = session.acquireTurn(t.Context())
	requireInvalidRequest(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = session.acquireTurn(ctx)
	require.ErrorIs(t, err, context.Canceled)
	release()

	require.NoError(t, session.ensureProcessAlive(t.Context()))
	noProcess := &agentSession{}
	require.Error(t, noProcess.ensureProcessAlive(t.Context()))

	require.NoError(t, session.Cancel(t.Context()))
	client.abortErr = errors.New("abort")
	require.Error(t, session.Cancel(t.Context()))
	noClient := &agentSession{}
	require.NoError(t, noClient.Cancel(t.Context()))

	cancelled := 0
	session.cancel = func() { cancelled++ }
	dialogCtx, cancelDialog := context.WithCancel(context.Background())
	session.pendingDialogs = map[string]*dialogCancel{"dialog": {cancel: cancelDialog}}
	session.cancelPendingInteractions()
	require.True(t, session.wasTurnCancelled())
	require.Empty(t, session.pendingDialogs)
	require.ErrorIs(t, dialogCtx.Err(), context.Canceled)

	process := newStubProcess(false)
	process.shutdown = errors.New("shutdown")
	process.close = errors.New("close")
	closing := &agentSession{agent: agent, proc: process, cancel: func() { cancelled++ }, sessionRoot: t.TempDir()}
	require.Error(t, closing.Close(t.Context()))
	require.Equal(t, 1, cancelled)
}

func TestSessionCancelEscalatesUnacknowledgedAbort(t *testing.T) {
	originalGrace := sessionCancelAbortGrace
	sessionCancelAbortGrace = 10 * time.Millisecond
	t.Cleanup(func() { sessionCancelAbortGrace = originalGrace })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	client.abortFunc = func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}

	process := newStubProcess(false)
	turnCtx, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:  agent,
		client: client,
		proc:   process,
		cancel: turnCancel,
	}

	require.NoError(t, session.Cancel(t.Context()))
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
	require.NoError(t, session.nativeContainmentError())
}

func TestSessionCancelContainsAcknowledgedAbortBeforeReturning(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	process := newStubProcess(false)
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	process.closeFunc = func() error {
		close(closeStarted)
		<-releaseClose

		return nil
	}

	turnCtx, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:         agent,
		client:        client,
		proc:          process,
		cancel:        turnCancel,
		turnFenceDone: make(chan struct{}),
	}

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- session.Cancel(t.Context()) }()

	<-closeStarted
	require.NoError(t, turnCtx.Err(), "turn context must remain live until the selected boundary completes")
	select {
	case err := <-cancelDone:
		t.Fatalf("cancel settled before the selected containment boundary: %v", err)
	default:
	}

	close(releaseClose)
	require.NoError(t, <-cancelDone)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
}

// TestSessionCancelRecordsPumpObservedSettlementForTheCommitAfterContainment
// pins the response-barrier race and the one commit point together: the outbox
// records agent_settled at receipt even when cancellation prevents delivery, the
// containment boundary commits nothing itself, and the settlement that follows
// adopts the complete durable aborted generation after that boundary.
func TestSessionCancelRecordsPumpObservedSettlementForTheCommitAfterContainment(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))

	process := newStubProcess(false)
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	process.closeFunc = func() error {
		close(closeStarted)
		<-releaseClose

		return nil
	}

	turnCtx, turnCancel := context.WithCancel(t.Context())
	delivery := newTurnDelivery()
	outbox := newSessionOutbox(1)
	outbox.adopt(delivery)
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          newStubPiClient(),
		proc:            process,
		cancel:          turnCancel,
		outbox:          outbox,
		turnEvents:      delivery,
		turnFenceDone:   make(chan struct{}),
		sessionFilePath: path,
	}

	// Reproduce the response-barrier race: the outbox has accepted
	// agent_settled, but cancellation prevents delivery to the prompt loop.
	routeCtx, stopRouting := context.WithCancel(t.Context())
	stopRouting()
	session.routeNativeEvent(routeCtx, outbox, pi.AgentSettledEvent{})
	require.True(t, session.turnNativeSettled, "settlement is recorded at outbox receipt")

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- session.Cancel(t.Context()) }()

	<-closeStarted
	entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Empty(t, entries, "the mirror must remain behind process-tree containment")

	close(releaseClose)
	require.NoError(t, <-cancelDone)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)

	entries, err = store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Empty(t, entries, "the containment boundary is not a commit point")

	var timedOut atomic.Bool
	response, err := session.settlePrompt(
		t.Context(), TextPromptRequest("id", "turn", "done"), &promptTurnState{}, promptOutcome{transportEnded: true}, &timedOut,
	)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, response.StopReason)

	entries, err = store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Len(t, entries, 2, "the durable cancelled generation lands after the boundary that proved it")
}

func TestSessionCancelWithoutNativeSettlementPreservesMirror(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
	_, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          newStubPiClient(),
		proc:            newStubProcess(false),
		cancel:          turnCancel,
		turnFenceDone:   make(chan struct{}),
		sessionFilePath: path,
	}

	require.NoError(t, session.Cancel(t.Context()))
	entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
	require.NoError(t, err)
	require.Empty(t, entries, "a forced pre-settle cancellation must not adopt partial native bytes")
}

func TestSessionCloseAndTimeoutDoNotCommitSettledCancellation(t *testing.T) {
	for _, test := range []struct {
		name  string
		fence func(*agentSession) error
	}{
		{
			name: "close",
			fence: func(session *agentSession) error {
				return session.cancelForClose(t.Context())
			},
		},
		{
			name: "timeout",
			fence: func(session *agentSession) error {
				var timedOut atomic.Bool

				return session.fenceTimedOutTurn(t.Context(), &timedOut)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			agent := NewAgent(
				WithLogger(slog.New(slog.DiscardHandler)),
				WithSessionStore(store),
			)
			path := filepath.Join(t.TempDir(), "session.jsonl")
			require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
			_, turnCancel := context.WithCancel(t.Context())
			session := &agentSession{
				agent:             agent,
				id:                "id",
				client:            newStubPiClient(),
				proc:              newStubProcess(false),
				cancel:            turnCancel,
				turnNativeSettled: true,
				turnFenceDone:     make(chan struct{}),
				sessionFilePath:   path,
			}

			require.NoError(t, test.fence(session))
			entries, err := store.Load(t.Context(), SessionKey{SessionID: "id"})
			require.NoError(t, err)
			require.Empty(t, entries, "non-user cancellation must preserve the prior mirror")
		})
	}
}

// TestSettledCancelMirrorFailureFailsTheSettlement pins that a durability
// failure on a cancelled cycle fails the prompt rather than reporting a
// cancelled success, and that no terminal idle stands behind a store that does
// not hold the prefix.
func TestSettledCancelMirrorFailureFailsTheSettlement(t *testing.T) {
	store := newFaultySessionStore()
	store.appendErr = errors.New("durability unavailable")
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(store),
	)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"type\":\"session\"}\n{\"type\":\"message\"}\n"), 0o600))
	_, turnCancel := context.WithCancel(t.Context())
	session := &agentSession{
		agent:             agent,
		id:                "id",
		client:            newStubPiClient(),
		proc:              newStubProcess(false),
		cancel:            turnCancel,
		turnNativeSettled: true,
		turnFenceDone:     make(chan struct{}),
		sessionFilePath:   path,
	}

	require.NoError(t, session.Cancel(t.Context()))

	var timedOut atomic.Bool
	_, err := session.settlePrompt(
		t.Context(), TextPromptRequest("id", "turn", "done"), &promptTurnState{}, promptOutcome{transportEnded: true}, &timedOut,
	)
	require.ErrorIs(t, err, errSessionMirrorAppend)
	require.ErrorContains(t, err, "durability unavailable")
}

func TestPromptTimeoutContainsProcessTreeBeforeReturning(t *testing.T) {
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithTurnTimeout(20*time.Millisecond),
	)
	client := newStubPiClient()
	process := newStubProcess(false)
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	process.closeFunc = func() error {
		close(closeStarted)
		<-releaseClose

		return nil
	}
	session := &agentSession{agent: agent, id: "id", client: client, proc: process}
	session.startPump(client)

	promptDone := make(chan error, 1)
	go func() {
		_, err := session.Prompt(t.Context(), TextPromptRequest("id", "timeout-turn", "hang"))
		promptDone <- err
	}()

	<-closeStarted
	select {
	case err := <-promptDone:
		t.Fatalf("timeout settled before the selected containment boundary: %v", err)
	default:
	}

	close(releaseClose)
	err := <-promptDone
	requirePiTurnFailure(t, err, failureCauseTimeout)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
}

func TestTurnFenceInactiveIdempotentAndErrorBranches(t *testing.T) {
	var timedOut atomic.Bool
	inactive := &agentSession{}
	require.NoError(t, inactive.fenceTimedOutTurn(t.Context(), &timedOut))
	require.False(t, timedOut.Load())
	require.NoError(t, inactive.fenceTurnAfterContext(t.Context()))
	require.NoError(t, inactive.fenceTurnAfterFailure(t.Context()))

	turnCtx, turnCancel := context.WithCancel(t.Context())
	process := newStubProcess(false)
	process.close = pi.ErrProcessContainmentIncomplete
	active := &agentSession{
		client:        newStubPiClient(),
		proc:          process,
		cancel:        turnCancel,
		turnFenceDone: make(chan struct{}),
	}
	err := active.fenceTurnAfterContext(t.Context())
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.ErrorIs(t, active.fenceTurnAfterFailure(t.Context()), pi.ErrProcessContainmentIncomplete)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)

	settling := &agentSession{cancel: func() {}, turnSettling: true}
	require.NoError(t, settling.fenceTurnAfterContext(t.Context()))
}

func TestSessionCancelEscalationFailures(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	abortErr := errors.New("abort")

	t.Run("missing process", func(t *testing.T) {
		turnCtx, turnCancel := context.WithCancel(t.Context())
		session := &agentSession{agent: agent}

		require.ErrorIs(t, session.terminateCancelledTurn(t.Context(), nil, turnCancel, abortErr), abortErr)
		require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	})

	t.Run("incomplete process containment", func(t *testing.T) {
		turnCtx, turnCancel := context.WithCancel(t.Context())
		process := newStubProcess(false)
		process.kill = errors.New("kill")
		process.close = pi.ErrProcessContainmentIncomplete
		session := &agentSession{agent: agent}

		err := session.terminateCancelledTurn(t.Context(), process, turnCancel, abortErr)
		require.ErrorIs(t, err, abortErr)
		require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
		require.ErrorIs(t, turnCtx.Err(), context.Canceled)
		require.ErrorIs(t, session.nativeContainmentError(), pi.ErrProcessContainmentIncomplete)
	})
}

func TestRelaunchProcessBranches(t *testing.T) {
	base := func(client *stubPiClient) (*agentSession, *Agent) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		old := newStubProcess(true)
		session := &agentSession{agent: agent, id: "id", proc: old, client: newStubPiClient(), sessionFilePath: filepath.Join(t.TempDir(), "missing"), mirroredRows: 5}
		prepareRelaunchFixture(t, session)
		relaunched := newStubProcess(false)
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return relaunched, client, nil
		}

		return session, agent
	}

	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/fresh"}
	session, _ := base(client)
	require.NoError(t, session.ensureProcessAlive(t.Context()))
	require.Equal(t, "/fresh", session.sessionFilePath)
	require.Zero(t, session.mirroredRows)

	client = newStubPiClient()
	session, agent := base(client)
	starts := 0
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		starts++

		return nil, nil, pi.ErrProcessContainmentIncomplete
	}
	require.Error(t, session.ensureProcessAlive(t.Context()))
	require.ErrorIs(t, session.nativeContainmentError(), pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, session.ensureProcessAlive(t.Context()), pi.ErrProcessContainmentIncomplete)
	require.Equal(t, 1, starts, "incomplete failed relaunch admitted another native root")

	client = newStubPiClient()
	session, _ = base(client)
	oldProcess, ok := session.proc.(*stubProcess)
	require.True(t, ok)
	oldProcess.close = pi.ErrProcessContainmentIncomplete
	require.ErrorIs(t, session.ensureProcessAlive(t.Context()), pi.ErrProcessContainmentIncomplete)

	client = newStubPiClient()
	client.startErr = errors.New("start")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.autoRetryErr = errors.New("retry")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.stateErr = errors.New("state")
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))
	client = newStubPiClient()
	client.state.SessionID = "other"
	session, _ = base(client)
	require.Error(t, session.ensureProcessAlive(t.Context()))

	client = newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/hydrated"}
	session, _ = base(client)
	file := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(file, []byte(`{}`), 0o600))
	session.sessionFilePath = file
	require.NoError(t, session.ensureProcessAlive(t.Context()))
	require.Equal(t, "/hydrated", session.sessionFilePath)
}

// TestSessionResidenceSurvivesRelaunch proves a relaunched generation still
// launches from this session's own residence: a generation-private agent
// directory carries the residence forward and rebases every path onto the new
// generation, a durable home keeps addressing the residence the session already
// owns, and the published files stay byte-identical and read-only either way.
func TestSessionResidenceSurvivesRelaunch(t *testing.T) {
	for _, test := range []struct {
		name       string
		durable    bool
		sameConfig bool
	}{
		{name: "generation private agent directory"},
		{name: "durable home", durable: true, sameConfig: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: "id"}

			options := []Option(nil)
			if test.durable {
				options = append(options, WithHome(filepath.Join(t.TempDir(), "home")))
			}

			agent := newStubClientAgent(t, client, options...)
			processes := []piProcess{newStubProcess(true), newStubProcess(false)}

			var launched pi.LaunchSpec

			agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
				launched = spec
				process := processes[0]
				processes = processes[1:]

				return process, client, nil
			}

			session, err := agent.startSession(t.Context(), sessionStart{
				Cwd:        t.TempDir(),
				McpServers: []acp.McpServer{StdioMCPServer("stdio", "/bin/true", nil, nil)},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

			started := launched
			require.Equal(t, session.residence.Root(), filepath.Dir(started.Env[pi.EnvMCPConfig]))

			config, err := os.ReadFile(started.Env[pi.EnvMCPConfig]) // #nosec G304 -- test temp dir.
			require.NoError(t, err)

			require.NoError(t, session.ensureProcessAlive(t.Context()))

			relaunched := launched
			require.NotEmpty(t, relaunched.Env[pi.EnvMCPConfig])
			require.Equal(t, test.sameConfig, started.Env[pi.EnvMCPConfig] == relaunched.Env[pi.EnvMCPConfig])
			require.Equal(t, filepath.Dir(relaunched.Env[pi.EnvMCPConfig]), filepath.Dir(relaunched.ExtensionPaths[0]))

			relaunchedConfig, err := os.ReadFile(relaunched.Env[pi.EnvMCPConfig]) // #nosec G304 -- test temp dir.
			require.NoError(t, err)
			require.Equal(t, config, relaunchedConfig)

			info, err := os.Stat(relaunched.Env[pi.EnvMCPConfig])
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o400), info.Mode().Perm())

			for _, path := range relaunched.ExtensionPaths {
				require.FileExists(t, path)
			}
		})
	}
}

func TestRefreshMCPToolsRebuildsRegistryOnFirstTurn(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	old := newStubProcess(false)
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: "id", SessionFile: "/refreshed"}
	relaunched := newStubProcess(false)

	sessionFile := filepath.Join(t.TempDir(), "session.jsonl")
	require.NoError(t, os.WriteFile(sessionFile, []byte(`{}`), 0o600))

	var launched pi.LaunchSpec
	starts := 0
	agent.startPiProcess = func(_ context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
		starts++
		launched = spec

		return relaunched, client, nil
	}

	session := &agentSession{
		agent:             agent,
		id:                "id",
		launch:            pi.LaunchSpec{SessionDir: "/sessions"},
		sessionFilePath:   sessionFile,
		mcpRefreshPending: true,
		proc:              old,
		client:            newStubPiClient(),
	}
	prepareRelaunchFixture(t, session)
	t.Cleanup(session.stopPump)

	require.NoError(t, session.refreshMCPTools(t.Context()))
	require.Equal(t, 1, starts)
	require.NotEqual(t, sessionFile, launched.SessionPath)
	require.FileExists(t, launched.SessionPath)
	require.Contains(t, launched.SessionPath, launched.Containment.GenerationRoot)
	require.Empty(t, launched.SessionID)
	require.Same(t, relaunched, session.proc)
	require.Same(t, client, session.client)
	require.Equal(t, "/refreshed", session.sessionFilePath)
	require.False(t, session.mcpRefreshPending)

	require.NoError(t, session.refreshMCPTools(t.Context()))
	require.Equal(t, 1, starts, "the fixed registry is refreshed only once")
}

func TestRefreshMCPToolsKeepsRetryPendingAfterFailedRelaunch(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, errors.New("relaunch")
	}
	session := &agentSession{
		agent:             agent,
		id:                "id",
		mcpRefreshPending: true,
		proc:              newStubProcess(false),
		client:            newStubPiClient(),
	}
	prepareRelaunchFixture(t, session)

	require.ErrorContains(t, session.refreshMCPTools(t.Context()), "relaunch")
	require.True(t, session.mcpRefreshPending)

	incomplete := newStubProcess(false)
	incomplete.close = pi.ErrProcessContainmentIncomplete
	session.proc = incomplete
	session.nativeContainmentErr = nil
	require.ErrorIs(t, session.refreshMCPTools(t.Context()), pi.ErrProcessContainmentIncomplete)
	require.True(t, session.mcpRefreshPending)
}

func prepareRelaunchFixture(t *testing.T, session *agentSession) {
	t.Helper()
	parent := t.TempDir()
	sessionRoot, err := os.MkdirTemp(parent, "acp-go-pi-session-*")
	require.NoError(t, err)
	dirs, err := createSessionGeneration(sessionRoot)
	require.NoError(t, err)
	require.NoError(t, session.agent.applyGenerationAgentDir(&dirs))
	session.sessionRoot = sessionRoot
	session.launch.AgentDir = dirs.AgentDir
	session.launch.SessionDir = dirs.SessionDir
	session.launch.Containment = pi.ContainmentSpec{
		ScratchParent:  parent,
		GenerationRoot: dirs.Root,
		RuntimeID:      strings.Repeat("a", 32),
		LifecycleKind:  string(RuntimeResourceSession),
	}
}

func TestNextRuntimeLaunchFailureBranches(t *testing.T) {
	wantErr := errors.New("injected relaunch materialization failure")

	fixture := func(t *testing.T) (*agentSession, pi.LaunchSpec, string) {
		t.Helper()
		parent := t.TempDir()
		sessionRoot, err := os.MkdirTemp(parent, "acp-go-pi-session-")
		require.NoError(t, err)
		oldRoot, err := os.MkdirTemp(sessionRoot, "acp-go-pi-runtime-")
		require.NoError(t, err)
		oldAgent := filepath.Join(oldRoot, "agent")
		require.NoError(t, os.Mkdir(oldAgent, 0o700))
		agent := NewAgent(testContainmentOption(), WithScratchDir(parent), WithLogger(slog.New(slog.DiscardHandler)))
		session := &agentSession{agent: agent, id: "id", sessionRoot: sessionRoot}
		previous := pi.LaunchSpec{
			AgentDir:    oldAgent,
			Containment: pi.ContainmentSpec{GenerationRoot: oldRoot},
		}

		return session, previous, oldRoot
	}

	t.Run("generation", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		materializeMkdirTemp = func(string, string) (string, error) { return "", wantErr }
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("startup defaults", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		home := filepath.Join(t.TempDir(), "home")
		require.NoError(t, os.MkdirAll(home, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(home, pi.SettingsFileName), []byte(`{"defaultModel":`), 0o600))
		session.agent.options.Home = home
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorContains(t, err, "decode pi settings")
	})

	// A relaunch reads settings.json exactly like a first launch, so the home
	// is reconciled again before the new generation spawns.
	t.Run("reconciles the home", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		home := filepath.Join(t.TempDir(), "home")
		require.NoError(t, os.MkdirAll(home, 0o700))
		session.agent.options.Home = home
		previous.AgentDir = home

		_, err := session.nextRuntimeLaunch(previous, "")
		require.NoError(t, err)

		settings := filepath.Join(home, pi.SettingsFileName)
		require.NoError(t, os.WriteFile(settings, []byte(`{"defaultModel":"gpt-4o"}`), 0o600))

		spec, err := session.nextRuntimeLaunch(previous, "")
		require.NoError(t, err)
		require.Equal(t, home, spec.AgentDir)
		contents, err := os.ReadFile(settings) // #nosec G304 -- the path is this test's own temp dir.
		require.NoError(t, err)
		require.NotContains(t, string(contents), "gpt-4o")
	})

	t.Run("copy agent", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		previous.AgentDir = filepath.Join(t.TempDir(), "missing")
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorContains(t, err, "copy pi agent generation")
	})

	for _, test := range []struct {
		name  string
		apply func(*pi.LaunchSpec, string)
	}{
		{name: "extension", apply: func(spec *pi.LaunchSpec, outside string) { spec.ExtensionPaths = []string{outside} }},
		{name: "skill", apply: func(spec *pi.LaunchSpec, outside string) { spec.SkillPaths = []string{outside} }},
		{name: "prompt template", apply: func(spec *pi.LaunchSpec, outside string) { spec.PromptTemplatePaths = []string{outside} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreMaterializeSeams(t)
			session, previous, _ := fixture(t)
			test.apply(&previous, filepath.Join(t.TempDir(), "outside"))
			_, err := session.nextRuntimeLaunch(previous, "")
			require.ErrorContains(t, err, "outside")
		})
	}

	t.Run("hydrate write", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		last := filepath.Join(t.TempDir(), "session.jsonl")
		require.NoError(t, os.WriteFile(last, []byte("{}\n"), 0o600))
		materializeWriteFile = func(string, []byte, os.FileMode) error { return wantErr }
		_, err := session.nextRuntimeLaunch(previous, last)
		require.ErrorContains(t, err, "hydrate relaunched pi session")
	})

	t.Run("hydrate read", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		materializeReadFile = func(string) ([]byte, error) { return nil, wantErr }
		_, err := session.nextRuntimeLaunch(previous, "/prior/session.jsonl")
		require.ErrorContains(t, err, "read prior pi session")
	})

	t.Run("containment identity", func(t *testing.T) {
		restoreMaterializeSeams(t)
		restoreRuntimeGenerationSeams(t)
		session, previous, _ := fixture(t)
		runtimeGenerationRandRead = func([]byte) (int, error) { return 0, wantErr }
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("remove previous", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, oldRoot := fixture(t)
		materializeRemoveAll = func(path string) error {
			if path == oldRoot {
				return wantErr
			}

			return os.RemoveAll(path)
		}
		_, err := session.nextRuntimeLaunch(previous, "")
		require.ErrorContains(t, err, "remove prior runtime generation")
	})

	t.Run("rebases environment", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, previous, _ := fixture(t)
		previous.Env = map[string]string{"RESOURCE": filepath.Join(previous.AgentDir, "resource"), "OTHER": "/outside"}
		spec, err := session.nextRuntimeLaunch(previous, "")
		require.NoError(t, err)
		require.Equal(t, filepath.Join(spec.AgentDir, "resource"), spec.Env["RESOURCE"])
		require.Equal(t, "/outside", spec.Env["OTHER"])
		require.Equal(t, string(session.id), spec.SessionID)
	})
}

func TestRelaunchAdmissionFailureBranches(t *testing.T) {
	t.Run("agent closed", func(t *testing.T) {
		agent := NewAgent(testContainmentOption())
		agent.mu.Lock()
		agent.closed = true
		agent.mu.Unlock()

		session := &agentSession{agent: agent}
		require.ErrorIs(t, session.relaunchProcess(t.Context()), errAgentClosed)
	})

	t.Run("next launch", func(t *testing.T) {
		session := &agentSession{agent: NewAgent(testContainmentOption()), sessionRoot: filepath.Join(t.TempDir(), "missing")}
		require.Error(t, session.relaunchProcess(t.Context()))
	})

	t.Run("native root", func(t *testing.T) {
		wantErr := errors.New("native root unavailable")
		agent := NewAgent(testContainmentOption(), WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
		}))
		session := &agentSession{agent: agent, id: "id"}
		prepareRelaunchFixture(t, session)
		require.ErrorIs(t, session.relaunchProcess(t.Context()), wantErr)
	})
}

// TestRelaunchPublicationFailureBranches pins the late relaunch failures:
// once the replacement process is live, a catalog fetch, catalog publication,
// or lifecycle stream that cannot complete retires the whole replacement
// generation rather than adopting a session whose published state would
// diverge from the live process.
func TestRelaunchPublicationFailureBranches(t *testing.T) {
	base := func(client *stubPiClient) (*agentSession, *Agent) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		session := &agentSession{agent: agent, id: "id", proc: newStubProcess(true), client: newStubPiClient()}
		prepareRelaunchFixture(t, session)
		relaunched := newStubProcess(false)
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return relaunched, client, nil
		}

		return session, agent
	}

	t.Run("catalog fetch", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id"}
		client.commandsErr = errors.New("commands")
		session, _ := base(client)
		require.ErrorContains(t, session.relaunchProcess(t.Context()), "commands")
	})

	t.Run("catalog publication", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id"}
		session, agent := base(client)
		connection := newDirectAgentClient()
		connection.updateErr = errors.New("catalog delivery")
		agent.setConnection(connection)
		require.ErrorContains(t, session.relaunchProcess(t.Context()), "catalog delivery")
	})

	t.Run("lifecycle stream", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "id"}
		session, agent := base(client)
		agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
		agent.setConnection(&lifecycleFailingClient{directAgentClient: newDirectAgentClient(), err: errors.New("stream delivery")})
		require.ErrorContains(t, session.relaunchProcess(t.Context()), "stream delivery")
	})
}

// TestRelaunchRecordsGenerationLossBeforeLaunch pins the ordering at the head
// of a relaunch: the lost generation's boundary is recorded before the
// replacement launches, and a store that cannot record it stops the relaunch
// before any native root is admitted.
func TestRelaunchRecordsGenerationLossBeforeLaunch(t *testing.T) {
	store := newFaultySessionStore()
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	agent.setConnection(newDirectAgentClient())
	session := &agentSession{agent: agent, id: "id", proc: newStubProcess(true), client: newStubPiClient()}
	prepareRelaunchFixture(t, session)
	require.NoError(t, session.openLifecycleStream(t.Context(), 1))

	spawns := 0
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		spawns++

		return newStubProcess(false), newStubPiClient(), nil
	}
	store.appendErr = errors.New("durability unavailable")

	require.ErrorIs(t, session.relaunchProcess(t.Context()), errLifecycleBoundaryCommit)
	require.True(t, session.lc.fenced, "the lost generation's stream ends even when its record cannot commit")
	require.Zero(t, spawns, "an unrecorded generation loss admits no replacement")
}
