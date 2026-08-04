package piacp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

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
	agent.probeVersion = func(context.Context, string, pi.ContainmentSpec) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.NoError(t, agent.ensureVersion(t.Context()))

	missing := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	missing.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	require.Error(t, missing.ensureVersion(t.Context()))
	probe := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	probe.probeVersion = func(context.Context, string, pi.ContainmentSpec) (string, error) { return "", errors.New("probe") }
	require.Error(t, probe.ensureVersion(t.Context()))
	old := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	old.probeVersion = func(context.Context, string, pi.ContainmentSpec) (string, error) { return "0.1.0", nil }
	require.Error(t, old.ensureVersion(t.Context()))
	withoutIsolation := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	require.Error(t, withoutIsolation.ensureVersion(t.Context()))

	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorIs(t, err, errAgentClosed)
	require.NoError(t, agent.Close())
}

func TestContainmentModePlatformMatrix(t *testing.T) {
	originalPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = originalPlatform })

	require.Equal(t, RuntimeContainmentUnavailable, (*Agent)(nil).ContainmentMode())

	agentRuntimePlatform = "linux"
	require.Equal(t, RuntimeContainmentAuthoritative, containmentMode(Options{}))
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{DarwinBestEffortContainment: true}))
	require.Error(t, validateContainmentOption(Options{DarwinBestEffortContainment: true}))

	agentRuntimePlatform = windowsPlatform
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{}))
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{DarwinBestEffortContainment: true}))
	require.Error(t, validateContainmentOption(Options{DarwinBestEffortContainment: true}))

	agentRuntimePlatform = darwinPlatform
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{}))
	require.Equal(t, RuntimeContainmentBestEffort, containmentMode(Options{DarwinBestEffortContainment: true}))
	require.NoError(t, validateContainmentOption(Options{DarwinBestEffortContainment: true}))
	var logs strings.Builder
	bestEffort := NewAgent(
		WithDarwinBestEffortContainment(),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	require.Equal(t, RuntimeContainmentBestEffort, bestEffort.ContainmentMode())
	require.Contains(t, logs.String(), "escaped descendants may survive")

	agentRuntimePlatform = "freebsd"
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{}))
	unavailable := NewAgent(testProcessIsolationOption(), WithLogger(slog.New(slog.DiscardHandler)))
	require.ErrorIs(t, unavailable.ensureVersion(t.Context()), ErrProcessContainmentIncomplete)

	var configured Options
	WithDarwinBestEffortContainment()(&configured)
	require.True(t, configured.DarwinBestEffortContainment)
}

func TestStartRealPiProcessRejectsEmptySpec(t *testing.T) {
	_, _, err := startRealPiProcess(t.Context(), pi.LaunchSpec{})
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
		process.close = ErrProcessContainmentIncomplete
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
	require.ErrorIs(t, <-errCh, ErrProcessContainmentIncomplete)
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
	process.close = pi.ErrProcessContainmentIncomplete
	agent.sessions["id"] = &agentSession{
		agent: agent, id: "id", proc: process, turn: make(chan struct{}, sessionTurnCapacity),
	}

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "id"})
	require.ErrorIs(t, err, pi.ErrProcessContainmentIncomplete)
	require.NotContains(t, agent.sessions, acp.SessionId("id"))
	require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)
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
		agent.probeVersion = func(context.Context, string, pi.ContainmentSpec) (string, error) {
			return pi.DefaultMinimumVersion, nil
		}
		agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
			return nil, nil, pi.ErrProcessContainmentIncomplete
		}

		_, err := agent.startSession(context.Background(), sessionStart{Cwd: t.TempDir()})
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		require.Empty(t, agent.sessions)

		return agent
	}

	require.ErrorIs(t, Serve(context.Background(), strings.NewReader(""), io.Discard), ErrProcessContainmentIncomplete)
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
	agent.probeVersion = func(context.Context, string, pi.ContainmentSpec) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	spawnStarted := make(chan struct{})
	releaseSpawn := make(chan struct{})
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		close(spawnStarted)
		<-releaseSpawn

		return nil, nil, pi.ErrProcessContainmentIncomplete
	}

	newSessionErr := make(chan error, 1)
	go func() {
		_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
		newSessionErr <- err
	}()
	<-spawnStarted

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
	<-serveCreated
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
	require.ErrorIs(t, <-newSessionErr, pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, <-closeErr, pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, <-serveErr, pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)
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

		return nil, nil, pi.ErrProcessContainmentIncomplete
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
	<-serveCreated

	promptErr := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(context.Background(), TextPromptRequest(sessionID, "relaunch-turn", "retry"))
		promptErr <- err
	}()
	<-spawnStarted

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
	require.ErrorIs(t, <-closeErr, pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, <-serveErr, pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)
}
