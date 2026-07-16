package piacp

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestSessionCloseTurnWaitFailure(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	session := &agentSession{
		agent:         agent,
		id:            "id",
		proc:          process,
		turn:          make(chan struct{}, sessionTurnCapacity),
		closeTurnWait: time.Millisecond,
	}

	release, err := session.acquireTurn(t.Context())
	require.NoError(t, err)
	t.Cleanup(release)

	require.Error(t, session.Close(t.Context()))
}

func TestSessionRetainsUnprovenNativeQuiescence(t *testing.T) {
	session := &agentSession{}
	session.recordNativeQuiescence(errors.New("ordinary native error"))
	require.NoError(t, session.nativeQuiescenceError())

	session.recordNativeQuiescence(pi.ErrProcessTreeNotQuiescent)
	require.ErrorIs(t, session.nativeQuiescenceError(), pi.ErrProcessTreeNotQuiescent)
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
	closing := &agentSession{agent: agent, proc: process, cancel: func() { cancelled++ }, closeTurnWait: time.Millisecond, sessionRoot: t.TempDir()}
	require.Error(t, closing.Close(t.Context()))
	require.Equal(t, 1, cancelled)
	require.Equal(t, defaultSessionCloseTurnWait, (&agentSession{}).closeTurnTimeout())
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
	require.NoError(t, session.nativeQuiescenceError())
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
	require.NoError(t, turnCtx.Err(), "turn context must remain live until containment is proved")
	select {
	case err := <-cancelDone:
		t.Fatalf("cancel settled before process-tree proof: %v", err)
	default:
	}

	close(releaseClose)
	require.NoError(t, <-cancelDone)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.Equal(t, 1, process.killCalls)
	require.Equal(t, 1, process.closeCalls)
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
		t.Fatalf("timeout settled before process-tree proof: %v", err)
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
	process.close = pi.ErrProcessTreeNotQuiescent
	active := &agentSession{
		client:        newStubPiClient(),
		proc:          process,
		cancel:        turnCancel,
		turnFenceDone: make(chan struct{}),
	}
	err := active.fenceTurnAfterContext(t.Context())
	require.ErrorIs(t, err, pi.ErrProcessTreeNotQuiescent)
	require.ErrorIs(t, turnCtx.Err(), context.Canceled)
	require.ErrorIs(t, active.fenceTurnAfterFailure(t.Context()), pi.ErrProcessTreeNotQuiescent)
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

	t.Run("unproven process tree", func(t *testing.T) {
		turnCtx, turnCancel := context.WithCancel(t.Context())
		process := newStubProcess(false)
		process.kill = errors.New("kill")
		process.close = pi.ErrProcessTreeNotQuiescent
		session := &agentSession{agent: agent}

		err := session.terminateCancelledTurn(t.Context(), process, turnCancel, abortErr)
		require.ErrorIs(t, err, abortErr)
		require.ErrorIs(t, err, pi.ErrProcessTreeNotQuiescent)
		require.ErrorIs(t, turnCtx.Err(), context.Canceled)
		require.ErrorIs(t, session.nativeQuiescenceError(), pi.ErrProcessTreeNotQuiescent)
	})
}

func TestRelaunchProcessBranches(t *testing.T) {
	base := func(client *stubPiClient) (*agentSession, *Agent) {
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
		old := newStubProcess(true)
		session := &agentSession{agent: agent, id: "id", proc: old, client: newStubPiClient(), sessionFilePath: filepath.Join(t.TempDir(), "missing"), mirroredRows: 5}
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

		return nil, nil, pi.ErrProcessTreeNotQuiescent
	}
	require.Error(t, session.ensureProcessAlive(t.Context()))
	require.ErrorIs(t, session.nativeQuiescenceError(), pi.ErrProcessTreeNotQuiescent)
	require.ErrorIs(t, session.ensureProcessAlive(t.Context()), pi.ErrProcessTreeNotQuiescent)
	require.Equal(t, 1, starts, "unproven failed relaunch admitted another native root")

	client = newStubPiClient()
	session, _ = base(client)
	oldProcess, ok := session.proc.(*stubProcess)
	require.True(t, ok)
	oldProcess.close = pi.ErrProcessTreeNotQuiescent
	require.ErrorIs(t, session.ensureProcessAlive(t.Context()), pi.ErrProcessTreeNotQuiescent)

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
	t.Cleanup(session.stopPump)

	require.NoError(t, session.refreshMCPTools(t.Context()))
	require.Equal(t, 1, starts)
	require.Equal(t, sessionFile, launched.SessionPath)
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

	require.ErrorContains(t, session.refreshMCPTools(t.Context()), "relaunch")
	require.True(t, session.mcpRefreshPending)

	unproven := newStubProcess(false)
	unproven.close = pi.ErrProcessTreeNotQuiescent
	session.proc = unproven
	session.nativeQuiescenceErr = nil
	require.ErrorIs(t, session.refreshMCPTools(t.Context()), pi.ErrProcessTreeNotQuiescent)
	require.True(t, session.mcpRefreshPending)
}
