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

func TestSessionCancelCommitsPumpObservedSettlementAfterContainment(t *testing.T) {
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
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          newStubPiClient(),
		proc:            process,
		cancel:          turnCancel,
		turnSink:        newTurnSink(),
		turnFenceDone:   make(chan struct{}),
		sessionFilePath: path,
	}

	// Reproduce the response-barrier race: the pump has accepted
	// agent_settled, but cancellation prevents delivery to the prompt sink.
	dispatchCtx, stopDispatch := context.WithCancel(t.Context())
	stopDispatch()
	session.dispatchEvent(dispatchCtx, pi.AgentSettledEvent{})

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
	require.Len(t, entries, 2)
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

func TestSessionSettledCancelMirrorFailureFailsFence(t *testing.T) {
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

	err := session.Cancel(t.Context())
	require.ErrorContains(t, err, "durability unavailable")
	require.ErrorIs(t, session.awaitTurnFence(), errSessionMirrorAppend)
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
