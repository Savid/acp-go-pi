package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

type finishAttemptOnErrContext struct {
	context.Context //nolint:containedctx // The test wrapper closes the attempt only when the waiter inspects Err.
	once            sync.Once
	finish          func()
}

func (c *finishAttemptOnErrContext) Err() error {
	c.once.Do(c.finish)

	return c.Context.Err()
}

func TestAgentDirectSurfaceAndVersionChecks(t *testing.T) {
	agent := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	resolved, err := agent.lookPath("sh")
	require.NoError(t, err)
	require.NotEmpty(t, resolved)
	_, err = agent.Authenticate(t.Context(), acp.AuthenticateRequest{MethodId: "native"})
	requireInvalidParams(t, err)
	_, err = agent.Logout(t.Context(), acp.LogoutRequest{})
	require.NoError(t, err)
	_, err = agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{})
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)

	agent.lookPath = func(string) (string, error) { return "/fake/pi", nil }
	agent.probeVersion = func(context.Context, string, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.NoError(t, agent.ensureVersion(t.Context()))

	missing := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	missing.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	require.Error(t, missing.ensureVersion(t.Context()))
	probe := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	probe.probeVersion = func(context.Context, string, string) (string, error) {
		return "", errors.New("probe")
	}
	require.Error(t, probe.ensureVersion(t.Context()))
	old := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	old.probeVersion = func(context.Context, string, string) (string, error) { return "0.1.0", nil }
	require.Error(t, old.ensureVersion(t.Context()))
	ordinary := NewAgent(WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	ordinary.probeVersion = func(context.Context, string, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	require.NoError(t, ordinary.ensureVersion(t.Context()))

	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorIs(t, err, errAgentClosed)
	require.NoError(t, agent.Close())
}

func TestEnsureVersionContainsGenerationWhenCloseWinsAfterCreation(t *testing.T) {
	restoreRuntimeGenerationSeams(t)

	agent := NewAgent(
		testContainmentOption(),
		WithExecutablePath("/fake/pi"),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	probeCalls := 0
	agent.probeVersion = func(context.Context, string, string) (string, error) {
		probeCalls++

		return pi.DefaultMinimumVersion, nil
	}

	mkdirTemp := runtimeGenerationMkdirTemp
	var closeAttempt *agentCloseAttempt
	runtimeGenerationMkdirTemp = func(parent, pattern string) (string, error) {
		root, err := mkdirTemp(parent, pattern)
		if err == nil {
			var owner bool
			closeAttempt, owner = agent.beginClose()
			require.True(t, owner)
		}

		return root, err
	}

	err := agent.ensureVersion(t.Context())
	require.ErrorIs(t, err, errAgentClosed)
	require.Zero(t, probeCalls, "a generation created after close was not admitted to the probe")
	require.NotNil(t, closeAttempt)
	agent.finishClose(closeAttempt, nil)
}

func TestStartRealPiProcessRejectsEmptySpec(t *testing.T) {
	agent := NewAgent()
	_, _, err := agent.startRealPiProcess(t.Context(), pi.LaunchSpec{})
	require.Error(t, err)
}

func TestServeContextAndConnectionBranches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, Serve(ctx, strings.NewReader(""), io.Discard), context.Canceled)
	require.NoError(t, Serve(context.Background(), strings.NewReader(""), io.Discard))

	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })

	started := make(chan struct{})
	newServeAgent = func(opts ...Option) *Agent {
		defer close(started)

		agent := NewAgent(append(opts, WithLogger(slog.New(slog.DiscardHandler)))...)
		process := newFailingCloseProcess()
		process.close = ErrContainmentIncomplete
		agent.sessions["serve"] = &agentSession{
			agent: agent,
			id:    "serve",
			proc:  process,
			turn:  make(chan struct{}, sessionTurnCapacity),
		}

		return agent
	}

	ctx, cancel = context.WithCancel(context.Background())
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		require.NoError(t, input.Close())
		require.NoError(t, inputWriter.Close())
	})
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, input, io.Discard) }()
	<-started
	cancel()
	require.ErrorIs(t, <-errCh, ErrContainmentIncomplete)
}

func TestAgentCloseJoinsSessionCloseError(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.sessions["id"] = &agentSession{
		agent: agent,
		id:    "id",
		proc:  newFailingCloseProcess(),
		turn:  make(chan struct{}, sessionTurnCapacity),
	}
	require.Error(t, agent.Close())
}

func TestAgentCloseIsSingleflightAndMemoizesResult(t *testing.T) {
	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })

	wantErr := errors.New("memoized close failure")
	var removeCalls atomic.Int32
	materializeRemoveAll = func(string) error {
		removeCalls.Add(1)

		return wantErr
	}

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.sessions["id"] = &agentSession{
		agent: agent, id: "id", sessionRoot: "/session", turn: make(chan struct{}, sessionTurnCapacity),
	}

	const callers = 16
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- agent.Close()
		}()
	}
	group.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, wantErr)
	}
	require.Equal(t, int32(1), removeCalls.Load())
	require.ErrorIs(t, agent.Close(), wantErr)
	require.Equal(t, int32(1), removeCalls.Load())
}

func TestAgentRetainsContainmentFailureAfterSessionRemoval(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	process.close = ErrContainmentIncomplete
	agent.sessions["id"] = &agentSession{
		agent: agent, id: "id", proc: process, turn: make(chan struct{}, sessionTurnCapacity),
	}

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "id"})
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Contains(t, agent.sessions, acp.SessionId("id"),
		"a close that could not contain the tree dropped the session that still owns it")
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestAgentCloseBoundsNeverReturningConstruction(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return bounded, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	owner, err := agent.beginNativeConstruction()
	require.NoError(t, err)

	closeErr := agent.Close()
	require.ErrorIs(t, closeErr, ErrContainmentIncomplete)
	agent.mu.Lock()
	require.Contains(t, agent.constructions, owner)
	require.ErrorIs(t, owner.err, ErrContainmentIncomplete)
	agent.mu.Unlock()
}

func TestQuarantinedAgentCloseCannotDetachOrReleaseSessions(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	session := &agentSession{agent: agent, id: "retained", proc: process}
	agent.sessions[session.id] = session

	attempt, owner := agent.beginClose()
	require.True(t, owner)
	want := errors.Join(ErrContainmentIncomplete, errors.New("agent waiter quarantine"))
	require.True(t, agent.quarantineClose(attempt, want))
	require.ErrorIs(t, agent.close(attempt), want)
	agent.finishClose(attempt, nil)
	require.ErrorIs(t, agent.awaitClose(attempt), want)
	require.Same(t, session, agent.sessions[session.id])
	require.Zero(t, process.shutdownCalls)
	require.Zero(t, process.closeCalls)
}

func TestAgentOwnershipHelpersPreserveImmutableQuarantine(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))

	// Nil is not a construction owner. These guards are intentionally fail
	// closed for internal callers that have not yet admitted native work.
	agent.finishNativeConstruction(nil, false, nil)
	agent.updateNativeConstruction(nil, func(*nativeConstruction) { t.Fatal("nil construction was updated") })
	require.Nil(t, agent.nativeConstructionError(nil))
	require.NoError(t, agent.cleanupNativeConstruction(t.Context(), nil, nil))
	agent.retainIncompleteSession(nil, errors.New("ignored"))

	owner, err := agent.beginNativeConstruction()
	require.NoError(t, err)
	want := errors.Join(ErrContainmentIncomplete, errors.New("immutable construction"))
	agent.mu.Lock()
	owner.immutable = true
	owner.err = want
	agent.mu.Unlock()

	session := &agentSession{agent: agent, id: "late"}
	closed, transferred := agent.transferConstructionToSession(owner, session)
	require.False(t, closed)
	require.False(t, transferred)
	require.False(t, agent.transferConstructionToRelaunch(owner, &sessionRelaunchAttempt{}))
	agent.finishNativeConstruction(owner, false, nil)
	require.ErrorIs(t, agent.nativeConstructionError(owner), ErrContainmentIncomplete)

	agent.retainIncompleteSession(session, errors.New("retain"))
	require.Contains(t, agent.retainedSessions, session)
	agent.retainIncompleteSession(session, nil)
	require.NotContains(t, agent.retainedSessions, session)

	agent.mu.Lock()
	agent.deleted["deleted"] = struct{}{}
	agent.sessions["deleted"] = &agentSession{fingerprint: sessionStartFingerprint(sessionStart{})}
	agent.mu.Unlock()
	require.Nil(t, agent.activeSessionForStart("deleted", sessionStart{}))
	require.Nil(t, agent.connection())
	require.False(t, agent.clientSupportsFormElicitation())
}

func TestAgentCloseContainsPanicsAndQuarantineIsOneShot(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	// A corrupt retained owner exercises the outer containment guard. Close must
	// publish one immutable failure rather than allow the panic to escape.
	agent.retainedSessions[nil] = struct{}{}
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)

	other := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	session := &agentSession{agent: other, id: "same", proc: process}
	other.sessions[session.id] = session
	other.retainedSessions[session] = struct{}{}
	attempt, owner := other.beginClose()
	require.True(t, owner)
	want := errors.Join(ErrContainmentIncomplete, errors.New("waiter"))
	require.True(t, other.quarantineClose(attempt, want))
	require.False(t, other.quarantineClose(attempt, errors.New("replacement")))
	require.ErrorIs(t, other.awaitClose(attempt), want)
}

func TestAgentCloseSettlementAndRetainedOwnerEdges(t *testing.T) {
	want := errors.Join(ErrContainmentIncomplete, errors.New("linearized waiter"))
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	attempt := &agentCloseAttempt{done: make(chan struct{}), err: want}
	attempt.settlement.Store(closeSettlementFinalizing)
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return &finishAttemptOnErrContext{
			Context: bounded,
			finish: func() {
				attempt.finishOnce.Do(func() { close(attempt.done) })
			},
		}, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })
	require.ErrorIs(t, agent.awaitClose(attempt), want)
	sessionCloseTurnWaitContext = originalWait

	retainedOnly := &agentSession{agent: agent, id: "retained-only"}
	agent.sessions[retainedOnly.id] = retainedOnly
	agent.retainedSessions[retainedOnly] = struct{}{}
	closeAttempt := &agentCloseAttempt{done: make(chan struct{})}
	require.NoError(t, agent.close(closeAttempt))

	quarantined := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	qAttempt := &agentCloseAttempt{done: make(chan struct{}), err: want}
	qAttempt.settlement.Store(closeSettlementQuarantined)
	close(qAttempt.done)
	require.ErrorIs(t, quarantined.close(qAttempt), want)

	qOwner := &agentSession{agent: quarantined, id: "quarantine-retained"}
	qSessionAttempt, qSessionOwner := qOwner.beginClose()
	require.True(t, qSessionOwner)
	quarantined.retainedSessions[qOwner] = struct{}{}
	openAttempt := &agentCloseAttempt{done: make(chan struct{})}
	require.True(t, quarantined.quarantineClose(openAttempt, want))
	require.ErrorIs(t, qOwner.awaitClose(qSessionAttempt), want)
}

func TestEnsureVersionCloseFenceMatrix(t *testing.T) {
	t.Run("closed before admission", func(t *testing.T) {
		agent := NewAgent(WithExecutablePath("/fake/pi"), WithScratchDir(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
		agent.probeVersion = func(context.Context, string, string) (string, error) {
			return pi.DefaultMinimumVersion, nil
		}
		_, _ = agent.beginClose()
		require.Error(t, agent.ensureVersion(t.Context()))
	})

	t.Run("close during native version probe", func(t *testing.T) {
		agent := NewAgent(WithExecutablePath("/fake/pi"), WithScratchDir(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
		agent.probeVersion = func(context.Context, string, string) (string, error) {
			_, _ = agent.beginClose()

			return pi.DefaultMinimumVersion, nil
		}
		require.Error(t, agent.ensureVersion(t.Context()))
	})
}
func TestServeReturnsIncompleteFailedSpawnWithoutInstalledSession(t *testing.T) {
	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })

	newServeAgent = func(opts ...Option) *Agent {
		agent := NewAgent(append(opts,
			testContainmentOption(),
			WithExecutablePath("/fake/pi"),
			WithScratchDir(t.TempDir()),
			WithLogger(slog.New(slog.DiscardHandler)),
		)...)
		agent.probeVersion = func(context.Context, string, string) (string, error) {
			return pi.DefaultMinimumVersion, nil
		}
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return nil, nil, ErrContainmentIncomplete
		}

		_, err := agent.startSession(context.Background(), sessionStart{Cwd: t.TempDir()})
		require.ErrorIs(t, err, ErrContainmentIncomplete)
		require.Empty(t, agent.sessions)

		return agent
	}

	require.ErrorIs(t, Serve(context.Background(), strings.NewReader(""), io.Discard), ErrContainmentIncomplete)
}

func TestCloseAndServeJoinAdmittedIncompleteSessionConstruction(t *testing.T) {
	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })

	agent := NewAgent(
		testContainmentOption(),
		WithExecutablePath("/fake/pi"),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent.probeVersion = func(context.Context, string, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		close(spawnStarted)
		<-releaseSpawn

		return nil, nil, ErrContainmentIncomplete
	}

	newSessionErr := make(chan error, 1)
	go func() {
		_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
		newSessionErr <- err
	}()
	awaitTestSignal(t, spawnStarted, "the admitted session construction reaching the spawn")

	serveCreated := make(chan struct{})
	newServeAgent = func(...Option) *Agent {
		close(serveCreated)

		return agent
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = input.Close()
		_ = inputWriter.Close()
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- Serve(serveCtx, input, io.Discard) }()
	awaitTestSignal(t, serveCreated, "Serve adopting the agent under construction")
	cancelServe()

	closeErr := make(chan error, 1)
	go func() { closeErr <- agent.Close() }()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()

		return agent.closed
	}, time.Second, time.Millisecond)

	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before admitted construction published containment: %v", err)
	default:
	}
	select {
	case err := <-serveErr:
		t.Fatalf("Serve returned before admitted construction published containment: %v", err)
	default:
	}

	close(releaseSpawn)
	require.ErrorIs(t, <-newSessionErr, ErrContainmentIncomplete)
	require.ErrorIs(t, <-closeErr, ErrContainmentIncomplete)
	require.ErrorIs(t, <-serveErr, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestCloseAndServeJoinAdmittedIncompletePromptRelaunch(t *testing.T) {
	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })

	agent := NewAgent(
		testContainmentOption(),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	sessionID := acp.SessionId("relaunch")
	session := &agentSession{
		agent: agent, id: sessionID, client: newStubPiClient(), proc: newStubProcess(true),
		turn: make(chan struct{}, sessionTurnCapacity),
	}
	prepareRelaunchFixture(t, session)
	agent.sessions[sessionID] = session

	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		close(spawnStarted)
		<-releaseSpawn

		return nil, nil, ErrContainmentIncomplete
	}

	serveCreated := make(chan struct{})
	newServeAgent = func(...Option) *Agent {
		close(serveCreated)

		return agent
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		_ = input.Close()
		_ = inputWriter.Close()
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- Serve(serveCtx, input, io.Discard) }()
	awaitTestSignal(t, serveCreated, "Serve adopting the agent holding the relaunching session")

	promptErr := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(context.Background(), TextPromptRequest(sessionID, "relaunch-turn", "retry"))
		promptErr <- err
	}()
	awaitTestSignal(t, spawnStarted, "the prompt relaunch reaching the spawn")

	cancelServe()
	closeErr := make(chan error, 1)
	go func() { closeErr <- agent.Close() }()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()

		return agent.closed
	}, time.Second, time.Millisecond)

	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before prompt relaunch published containment: %v", err)
	default:
	}
	select {
	case err := <-serveErr:
		t.Fatalf("Serve returned before prompt relaunch published containment: %v", err)
	default:
	}

	close(releaseSpawn)
	require.Error(t, <-promptErr)
	require.ErrorIs(t, <-closeErr, ErrContainmentIncomplete)
	require.ErrorIs(t, <-serveErr, ErrContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

// ordinaryPiHarness publishes a fake pi that records every launch it receives,
// and hands back the executable and the record. The recording is the harness's
// own first act rather than a wrapper around it, because a wrapper would have
// to be a script and the adapter launches an executable image.
func ordinaryPiHarness(t *testing.T, scenario unitFakeScenario) (string, string) {
	t.Helper()

	record := filepath.Join(t.TempDir(), "launch.jsonl")
	executable := publishUnitFake(t, unitFakeSidecar{
		Scenario:     scenario,
		LaunchRecord: record,
		CanaryEnv:    "ACP_GO_PI_TEST_ACTUAL_AMBIENT",
	})

	return executable, record
}

func ordinaryLaunches(t *testing.T, record string) []ordinaryLaunch {
	t.Helper()

	data, err := os.ReadFile(record)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	require.NoError(t, err)

	launches := make([]ordinaryLaunch, 0, 4)

	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}

		var launch ordinaryLaunch

		require.NoError(t, json.Unmarshal([]byte(line), &launch), line)

		launches = append(launches, launch)
	}

	return launches
}

// TestAgentSessionDefaultsToOrdinaryExecution proves that omitting host
// authority reaches a real direct launch with a filtered environment.
func TestAgentSessionDefaultsToOrdinaryExecution(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "ambient-provider-key")
	t.Setenv("ACP_GO_PI_TEST_ACTUAL_AMBIENT", "ambient-canary")
	harness, record := ordinaryPiHarness(t, successfulUnitScenario())
	agent := NewAgent(
		WithExecutablePath(harness),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	response, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	prompt, err := agent.Prompt(t.Context(), TextPromptRequest(response.SessionId, "ordinary-turn", "hello"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, prompt.StopReason)

	launches := ordinaryLaunches(t, record)
	require.NotEmpty(t, launches)
	for index, launch := range launches {
		require.Empty(t, launch.Canary, "launch %d inherited ambient credentials", index)
	}
}

func TestAgentCapturesOrdinaryEnvironmentOnce(t *testing.T) {
	t.Setenv("PATH", "/ordinary/first")
	t.Setenv("OPENAI_API_KEY", "must-not-cross")
	agent := NewAgent()
	t.Cleanup(func() { _ = agent.Close() })
	t.Setenv("PATH", "/ordinary/second")
	require.Equal(t, "/ordinary/first", agent.ordinaryEnvironment["PATH"])
	require.NotContains(t, agent.ordinaryEnvironment, "OPENAI_API_KEY")
}

type ordinaryLaunch struct {
	Canary string `json:"canary"`
	Args   string `json:"args"`
}

func TestEnsureVersionCoverageEdges(t *testing.T) {
	newVersionAgent := func() *Agent {
		agent := NewAgent(WithExecutablePath("/fake/pi"), WithScratchDir(t.TempDir()))
		agent.probeVersion = func(context.Context, string, string) (string, error) {
			return pi.DefaultMinimumVersion, nil
		}

		return agent
	}

	t.Run("admission", func(t *testing.T) {
		agent := newVersionAgent()
		agent.nativeBusyRoots["/busy"] = struct{}{}
		require.ErrorIs(t, agent.ensureVersion(t.Context()), ErrNativeTreeBusy)
	})

	t.Run("busy retry", func(t *testing.T) {
		agent := newVersionAgent()
		agent.options.hostAuthoritySupplied = true
		agent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
			return ErrNativeTreeBusy
		}}
		done := make(chan struct{})
		close(done)
		construction := &nativeConstruction{
			done: done, err: ErrNativeTreeBusy, generationRoot: "/busy", generationPrepared: true,
			nativeBoundary: newNativeBoundaryTracker(),
		}
		agent.constructions[construction] = struct{}{}
		require.ErrorIs(t, agent.ensureVersion(t.Context()), ErrNativeTreeBusy)
	})

	t.Run("generation creation", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		agent := newVersionAgent()
		runtimeGenerationEnsureScratchParent = func(string) (string, error) {
			return "", ErrContainmentIncomplete
		}
		require.ErrorIs(t, agent.ensureVersion(t.Context()), ErrContainmentIncomplete)
	})

	t.Run("close after executable resolution", func(t *testing.T) {
		agent := NewAgent(WithScratchDir(t.TempDir()))
		agent.lookPath = func(string) (string, error) {
			_, _ = agent.beginClose()

			return "/fake/pi", nil
		}
		require.ErrorIs(t, agent.ensureVersion(t.Context()), errAgentClosed)
	})

	t.Run("cancel before probe", func(t *testing.T) {
		agent := newVersionAgent()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, agent.ensureVersion(ctx), context.Canceled)
	})

	t.Run("close at final gate", func(t *testing.T) {
		agent := newVersionAgent()
		ctx := &closeAgentOnErrContext{Context: t.Context(), agent: agent}
		require.ErrorIs(t, agent.ensureVersion(ctx), errAgentClosed)
	})
}

func TestAgentCloseAndAuthorityFenceEdges(t *testing.T) {
	agent := NewAgent()
	done := make(chan struct{})
	close(done)
	construction := &nativeConstruction{done: done, nativeBoundary: newNativeBoundaryTracker()}
	agent.constructions[construction] = struct{}{}
	attempt := &agentCloseAttempt{done: make(chan struct{})}
	require.NoError(t, agent.close(attempt))
	_, retained := agent.constructions[construction]
	require.False(t, retained)

	fenced := NewAgent()
	shared := &agentSession{agent: fenced, id: acp.SessionId("shared")}
	retainedOnly := &agentSession{agent: fenced, id: acp.SessionId("retained")}
	fenced.sessions[shared.id] = shared
	fenced.sessions[acp.SessionId("shared-alias")] = shared
	fenced.retainedSessions[shared] = struct{}{}
	fenced.retainedSessions[retainedOnly] = struct{}{}
	fenced.fenceSessionsAfterAuthorityLoss(ErrHostAuthorityUnavailable)
	require.ErrorIs(t, shared.nativeContainmentError(), ErrHostAuthorityUnavailable)
	require.ErrorIs(t, retainedOnly.nativeContainmentError(), ErrHostAuthorityUnavailable)
}

func TestConstructionOwnershipEdges(t *testing.T) {
	process := newStubProcess(false)
	process.close = ErrContainmentIncomplete
	agent := NewAgent()
	construction := &nativeConstruction{proc: process, nativeBoundary: newNativeBoundaryTracker()}
	require.ErrorIs(t, agent.cleanupNativeConstructionOwned(t.Context(), construction, nil), ErrContainmentIncomplete)

	t.Run("pump boundary changes", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "session"}
		agent := newStubClientAgent(t, client)
		agent.versionChecked = true
		client.startFunc = func(context.Context) error {
			agent.mu.Lock()
			for owner := range agent.constructions {
				owner.session.nativeBoundary = newNativeBoundaryTracker()
			}
			agent.mu.Unlock()

			return nil
		}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: testCwd})
		require.ErrorIs(t, err, ErrContainmentIncomplete)
	})

	t.Run("immutable transfer", func(t *testing.T) {
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "session"}
		agent := newStubClientAgent(t, client)
		agent.versionChecked = true
		client.commandsFunc = func() {
			agent.mu.Lock()
			for owner := range agent.constructions {
				owner.immutable = true
				owner.err = ErrContainmentIncomplete
			}
			agent.mu.Unlock()
		}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: testCwd})
		require.ErrorIs(t, err, ErrContainmentIncomplete)
	})

	t.Run("post-transfer close recheck", func(t *testing.T) {
		agent := NewAgent()
		open := attachTestNativeBoundary(&agentSession{
			agent: agent, proc: newStubProcess(false), turn: make(chan struct{}, sessionTurnCapacity),
		})
		kept, err := agent.closeTransferredSessionIfAgentClosed(t.Context(), open, false)
		require.NoError(t, err)
		require.Same(t, open, kept)

		closed := attachTestNativeBoundary(&agentSession{
			agent: agent, proc: newStubProcess(false), turn: make(chan struct{}, sessionTurnCapacity),
		})
		agent.mu.Lock()
		agent.closed = true
		agent.mu.Unlock()
		removed, err := agent.closeTransferredSessionIfAgentClosed(t.Context(), closed, false)
		require.ErrorIs(t, err, errAgentClosed)
		require.Nil(t, removed)
	})
}

// TestConstructionVerdictWithoutAFieldStaysFieldLess pins that a construction
// refusal that named no option is answered as the bare token rather than with
// an invented field. The option verdict names exactly one option or none.
func TestConstructionVerdictWithoutAFieldStaysFieldLess(t *testing.T) {
	t.Parallel()

	require.Empty(t, optionFailureField(acp.NewInvalidParams("not an object")))
	require.Empty(t, optionFailureField(acp.NewInvalidParams(map[string]any{})))
	require.Equal(t, optionFieldHome, optionFailureField(unsupportedRequest(optionFieldHome)))

	agent := NewAgent(testContainmentOption())
	agent.optionErr = acp.NewInvalidParams("not an object")
	requireClosedInternalError(t, agent.optionsError(), invalidOptionsError)
}
