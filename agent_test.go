package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
	agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.NoError(t, agent.ensureVersion(t.Context()))

	missing := NewAgent(testContainmentOption(), WithLogger(slog.New(slog.DiscardHandler)))
	missing.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	require.Error(t, missing.ensureVersion(t.Context()))
	probe := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	probe.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
		return "", errors.New("probe")
	}
	require.Error(t, probe.ensureVersion(t.Context()))
	old := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	old.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) { return "0.1.0", nil }
	require.Error(t, old.ensureVersion(t.Context()))
	withoutIsolation := NewAgent(WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	withoutIsolation.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}
	require.NoError(t, withoutIsolation.ensureVersion(t.Context()))

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
	agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
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

func TestContainmentModePlatformMatrix(t *testing.T) {
	originalPlatform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = originalPlatform })

	require.Equal(t, RuntimeContainmentUnavailable, (*Agent)(nil).ContainmentMode())

	agentRuntimePlatform = "linux"
	require.Equal(t, RuntimeContainmentSharedIdentity, containmentMode(Options{}))
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{DarwinBestEffortContainment: true}))
	require.Error(t, validateContainmentOption(Options{DarwinBestEffortContainment: true}))

	agentRuntimePlatform = windowsPlatform
	require.Equal(t, RuntimeContainmentSharedIdentity, containmentMode(Options{}))
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{DarwinBestEffortContainment: true}))
	require.Error(t, validateContainmentOption(Options{DarwinBestEffortContainment: true}))

	agentRuntimePlatform = darwinPlatform
	require.Equal(t, RuntimeContainmentSharedIdentity, containmentMode(Options{}))
	require.Equal(t, RuntimeContainmentBestEffort, containmentMode(Options{DarwinBestEffortContainment: true}))
	require.NoError(t, validateContainmentOption(Options{DarwinBestEffortContainment: true}))
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{
		ProcessIsolation: policyForContainmentModeTest(), DarwinBestEffortContainment: true,
	}))
	require.Error(t, validateContainmentOption(Options{
		ProcessIsolation: policyForContainmentModeTest(), DarwinBestEffortContainment: true,
	}))
	var logs strings.Builder
	bestEffort := NewAgent(
		WithDarwinBestEffortContainment(),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	require.Equal(t, RuntimeContainmentBestEffort, bestEffort.ContainmentMode())
	require.Contains(t, logs.String(), "escaped descendants may survive")

	agentRuntimePlatform = "freebsd"
	require.Equal(t, RuntimeContainmentSharedIdentity, containmentMode(Options{}))
	unavailable := NewAgent(testProcessIsolationOption(), WithLogger(slog.New(slog.DiscardHandler)))
	require.ErrorContains(t, unavailable.ensureVersion(t.Context()), "supported only on linux")

	var configured Options
	WithDarwinBestEffortContainment()(&configured)
	require.True(t, configured.DarwinBestEffortContainment)
}

func policyForContainmentModeTest() *ProcessIsolation {
	return &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}}
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
	require.Contains(t, agent.sessions, acp.SessionId("id"),
		"a close that could not contain the tree dropped the session that still owns it")
	require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)
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
	require.ErrorIs(t, closeErr, pi.ErrProcessContainmentIncomplete)
	agent.mu.Lock()
	require.Contains(t, agent.constructions, owner)
	require.ErrorIs(t, owner.err, pi.ErrProcessContainmentIncomplete)
	agent.mu.Unlock()
}

func TestQuarantinedAgentCloseCannotDetachOrReleaseSessions(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	session := &agentSession{agent: agent, id: "retained", proc: process}
	agent.sessions[session.id] = session

	attempt, owner := agent.beginClose()
	require.True(t, owner)
	want := errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("agent waiter quarantine"))
	require.True(t, agent.quarantineClose(attempt, want))
	require.ErrorIs(t, agent.close(attempt), want)
	agent.finishClose(attempt, nil)
	require.ErrorIs(t, agent.awaitClose(attempt), want)
	require.Same(t, session, agent.sessions[session.id])
	require.Zero(t, process.shutdownCalls)
	require.Zero(t, process.closeCalls)
}

func TestAgentConstructionQuarantineContainsLateSpawnPublication(t *testing.T) {
	originalWait := sessionCloseTurnWaitContext
	sessionCloseTurnWaitContext = func(ctx context.Context) (context.Context, context.CancelFunc) {
		bounded, cancel := context.WithCancel(ctx)
		cancel()

		return bounded, func() {}
	}
	t.Cleanup(func() { sessionCloseTurnWaitContext = originalWait })

	for _, testCase := range []struct {
		name         string
		spawnProcess bool
	}{
		{name: "valid process", spawnProcess: true},
		{name: "nil process"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var nativeReleases, scratchReleases int
			agent := NewAgent(
				testContainmentOption(),
				WithExecutablePath("/fake/pi"),
				WithScratchDir(t.TempDir()),
				WithLogger(slog.New(slog.DiscardHandler)),
				WithRuntimeResourceHooks(RuntimeResourceHooks{
					AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
						return func() { nativeReleases++ }, nil
					},
					ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
						return func() { scratchReleases++ }, nil
					},
				}),
			)
			agent.versionChecked = true

			spawnEntered := make(chan struct{})
			releaseSpawn := make(chan struct{})
			process := newStubProcess(false)
			agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
				close(spawnEntered)
				<-releaseSpawn
				if !testCase.spawnProcess {
					return nil, nil, nil
				}

				return process, newStubPiClient(), nil
			}

			startDone := make(chan error, 1)
			go func() {
				_, err := agent.startSession(context.Background(), sessionStart{Cwd: t.TempDir()})
				startDone <- err
			}()
			<-spawnEntered

			agent.mu.Lock()
			require.Len(t, agent.constructions, 1)
			var owner *nativeConstruction
			for construction := range agent.constructions {
				owner = construction
			}
			require.NotEmpty(t, owner.sessionRoot)
			require.NotNil(t, owner.nativeRelease)
			require.NotNil(t, owner.scratchRelease)
			agent.mu.Unlock()

			closeErr := agent.Close()
			require.ErrorIs(t, closeErr, pi.ErrProcessContainmentIncomplete)

			close(releaseSpawn)
			require.ErrorIs(t, <-startDone, pi.ErrProcessContainmentIncomplete)

			agent.mu.Lock()
			require.Contains(t, agent.constructions, owner)
			require.True(t, owner.immutable)
			require.ErrorIs(t, owner.err, pi.ErrProcessContainmentIncomplete)
			if testCase.spawnProcess {
				require.Same(t, process, owner.proc, "late spawn was not transferred to its exact quarantine")
				require.NotNil(t, owner.client)
				require.NotNil(t, owner.processRoot)
			} else {
				require.Nil(t, owner.proc)
				require.Nil(t, owner.client)
				require.NotNil(t, owner.processRoot, "the failed spawn's exact tracker root was discarded")
			}
			agent.mu.Unlock()

			if testCase.spawnProcess {
				require.Equal(t, 1, process.shutdownCalls)
				require.Equal(t, 1, process.killCalls)
				require.Equal(t, 1, process.closeCalls)
			} else {
				require.Zero(t, process.shutdownCalls)
				require.Zero(t, process.killCalls)
				require.Zero(t, process.closeCalls)
			}
			require.Equal(t, 1, nativeReleases)
			require.Equal(t, 1, scratchReleases)
			require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)
		})
	}
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
	want := errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("immutable construction"))
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
	require.ErrorIs(t, agent.nativeConstructionError(owner), pi.ErrProcessContainmentIncomplete)

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
	require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), pi.ErrProcessContainmentIncomplete)

	other := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	process := newStubProcess(false)
	session := &agentSession{agent: other, id: "same", proc: process}
	other.sessions[session.id] = session
	other.retainedSessions[session] = struct{}{}
	attempt, owner := other.beginClose()
	require.True(t, owner)
	want := errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("waiter"))
	require.True(t, other.quarantineClose(attempt, want))
	require.False(t, other.quarantineClose(attempt, errors.New("replacement")))
	require.ErrorIs(t, other.awaitClose(attempt), want)
}

func TestAgentCloseSettlementAndRetainedOwnerEdges(t *testing.T) {
	want := errors.Join(pi.ErrProcessContainmentIncomplete, errors.New("linearized waiter"))
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
	type setupFunc func(*Agent, *context.Context)
	tests := []struct {
		name  string
		setup setupFunc
	}{
		{
			name:  "closed before admission",
			setup: func(agent *Agent, _ *context.Context) { _, _ = agent.beginClose() },
		},
		{
			name: "close during executable resolution",
			setup: func(agent *Agent, _ *context.Context) {
				agent.options.ExecutablePath = ""
				agent.lookPath = func(string) (string, error) {
					_, _ = agent.beginClose()

					return "/fake/pi", nil
				}
			},
		},
		{
			name: "close during discovery scratch reservation",
			setup: func(agent *Agent, _ *context.Context) {
				agent.options.RuntimeResourceHooks.ReserveScratchRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
					_, _ = agent.beginClose()

					return func() {}, nil
				}
			},
		},
		{
			name: "close during discovery native root acquisition",
			setup: func(agent *Agent, _ *context.Context) {
				agent.options.RuntimeResourceHooks.AcquireNativeRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
					_, _ = agent.beginClose()

					return func() {}, nil
				}
			},
		},
		{
			name: "context cancels at final probe gate",
			setup: func(agent *Agent, ctx *context.Context) {
				armed := false
				agent.options.RuntimeResourceHooks.AcquireNativeRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
					armed = true

					return func() {}, nil
				}
				*ctx = &closeOnErrContext{Context: *ctx, returnErr: func() error {
					if armed {
						return context.Canceled
					}

					return nil
				}}
			},
		},
		{
			name: "close during final context check",
			setup: func(agent *Agent, ctx *context.Context) {
				armed := false
				agent.options.RuntimeResourceHooks.AcquireNativeRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
					armed = true

					return func() {}, nil
				}
				*ctx = &closeOnErrContext{Context: *ctx, onErr: func() {
					if armed {
						_, _ = agent.beginClose()
					}
				}}
			},
		},
		{
			name: "close during native version probe",
			setup: func(agent *Agent, _ *context.Context) {
				agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
					_, _ = agent.beginClose()

					return pi.DefaultMinimumVersion, nil
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := NewAgent(testContainmentOption(), WithExecutablePath("/fake/pi"), WithScratchDir(t.TempDir()), WithLogger(slog.New(slog.DiscardHandler)))
			agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
				return pi.DefaultMinimumVersion, nil
			}
			ctx := t.Context()
			test.setup(agent, &ctx)
			require.Error(t, agent.ensureVersion(ctx))
		})
	}
}

func TestConstructionProcessObserverCanReenterAgentClose(t *testing.T) {
	var agent *Agent
	var closeErr error
	var reenter atomic.Bool
	hooks := RuntimeResourceHooks{ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) {
		if reenter.CompareAndSwap(false, true) {
			closeErr = agent.Close()
		}
	}}
	agent = NewAgent(testContainmentOption(), WithRuntimeResourceHooks(hooks), WithLogger(slog.New(slog.DiscardHandler)))

	construction, err := agent.beginNativeConstruction()
	require.NoError(t, err)
	process := inventoryPiProcess{stubProcess: newStubProcess(false), count: 1}
	root := agent.processes.registerDeferred()
	session := &agentSession{agent: agent, id: "observer-reentry", proc: process, providerProcessRoot: root}
	session.nativeBoundary = construction.nativeBoundary
	construction.proc = process
	construction.processRoot = root
	construction.session = session
	closed, transferred := agent.transferConstructionToSession(construction, session)
	require.False(t, closed)
	require.True(t, transferred)

	done := make(chan struct{})
	go func() {
		root.observe(context.Background(), process)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("construction observer reentry self-waited")
	}
	require.NoError(t, closeErr)
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
		agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
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
	agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
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

func restoreContainmentPlatformSeam(t *testing.T) {
	t.Helper()

	platform := agentRuntimePlatform
	t.Cleanup(func() { agentRuntimePlatform = platform })
}

// ordinaryLaunch is one recorded native launch: who ran it, and whether the
// ambient credential the supervisor holds crossed into it.
type ordinaryLaunch struct {
	UID    string `json:"uid"`
	GID    string `json:"gid"`
	Canary string `json:"canary"`
	Args   string `json:"args"`
}

// ordinaryPiHarness wraps the fake pi harness in a recorder. Every launch
// appends its identity and the canary's fate before exec'ing the real fake, so
// the properties ordinary execution owes are read off actual spawns rather than
// off the options that asked for them.
func ordinaryPiHarness(t *testing.T, scenario unitFakeScenario) (string, string) {
	t.Helper()

	dir := t.TempDir()
	record := filepath.Join(dir, "launch.jsonl")
	inner := unitFakeExecutable(t, scenario)
	wrapper := filepath.Join(dir, "pi")

	script := fmt.Sprintf(
		"#!/bin/sh\n"+
			"printf '{\"uid\":\"%%s\",\"gid\":\"%%s\",\"canary\":\"%%s\",\"args\":\"%%s\"}\\n' "+
			"\"$(id -u)\" \"$(id -g)\" \"${ACP_GO_PI_TEST_ACTUAL_AMBIENT:-}\" \"$*\" >> %q\n"+
			"exec %q \"$@\"\n",
		record, inner,
	)
	require.NoError(t, os.WriteFile(wrapper, []byte(script), 0o700))

	return wrapper, record
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

// TestAgentSessionDefaultsToOrdinaryExecution is the ordinary-default gate.
// Omitting WithProcessIsolation reaches a real native launch as the identity
// that supervises it, and that launch owes three things it does not get: no
// provider-descendant inventory, no authority handed to the child, and no
// whole-tree quiescence claim made on its behalf. Each is read off the launch
// that actually happened rather than off the options that asked for it.
func TestAgentSessionDefaultsToOrdinaryExecution(t *testing.T) {
	restoreContainmentPlatformSeam(t)

	// The posture itself is portable: no platform turns omission into a
	// hardened or best-effort boundary.
	for _, platform := range []string{linuxPlatform, darwinPlatform, windowsPlatform, "freebsd"} {
		agentRuntimePlatform = platform
		require.Equal(t, RuntimeContainmentSharedIdentity, containmentMode(Options{}), platform)
	}

	agentRuntimePlatform = runtime.GOOS

	t.Setenv("ANTHROPIC_API_KEY", "ambient-provider-key")
	t.Setenv("ACP_GO_PI_TEST_ACTUAL_AMBIENT", "ambient-canary")

	harness, record := ordinaryPiHarness(t, successfulUnitScenario())

	var (
		snapshots int
		observed  []RuntimeContainmentMode
	)

	agent := NewAgent(
		WithExecutablePath(harness),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
			ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
				observed = append(observed, mode)
			},
		}),
	)
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())

	require.Nil(t, agent.options.ProcessIsolation, "the default selects no explicit policy")
	require.Equal(t, RuntimeContainmentSharedIdentity, agent.ContainmentMode())

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)

	response, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.NotEmpty(t, response.SessionId)

	prompt, err := agent.Prompt(t.Context(), TextPromptRequest(response.SessionId, "ordinary-turn", "hello"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, prompt.StopReason)

	launches := ordinaryLaunches(t, record)
	require.NotEmpty(t, launches, "the ordinary session reached a real native launch")

	for index, launch := range launches {
		// Identity: ordinary execution never changes who runs the child. On a
		// root runner that means root; on any other runner the same
		// unprivileged identity. Either way it is the supervisor's own.
		require.Equal(t, strconv.Itoa(os.Geteuid()), launch.UID, "launch %d identity", index)
		require.Equal(t, strconv.Itoa(os.Getegid()), launch.GID, "launch %d identity", index)

		// Authority: nothing hands this child the supervisor's ambient
		// environment, which for pi is live provider auth.
		require.Empty(t, launch.Canary, "launch %d inherited the supervisor's ambient environment", index)
	}

	// No inventory: ordinary execution cannot see a whole process tree, so it
	// publishes no descendant counts rather than an unproven number.
	require.Zero(t, snapshots, "ordinary execution published a descendant snapshot")
	require.Equal(t, []RuntimeContainmentMode{RuntimeContainmentSharedIdentity}, observed)

	session, err := agent.session(response.SessionId)
	require.NoError(t, err)

	inventory, enumerable := session.proc.(providerProcessInventory)
	require.True(t, enumerable, "the ordinary process still answers the inventory question")

	count, available := inventory.ProviderDescendantCount()
	require.False(t, available, "ordinary execution enumerates no provider descendants")
	require.Zero(t, count)

	// No whole-tree claim: the advertisement is resolved from the same
	// configuration that enforces containment, so it states nothing this
	// boundary cannot prove.
	proven := agent.provenLifecycleFacts()
	require.False(t, proven.AuthoritativeQuiescence)
	require.Empty(t, proven.QuiescenceSource)
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

// TestExplicitProcessIsolationPreservesPolicy pins that a supplied policy is
// never reinterpreted as ordinary execution or a Darwin fallback: a valid
// Linux policy selects the hardened boundary, and every other platform reports
// the boundary unavailable and refuses the policy rather than degrading it.
// That the refusal also stops the spawn is proven beside it, in
// TestExplicitProcessIsolationRefusesWithoutASecondSpawnAttempt.
func TestExplicitProcessIsolationPreservesPolicy(t *testing.T) {
	restoreContainmentPlatformSeam(t)
	policy := &ProcessIsolation{UID: 1001, GID: 1001, BaseEnvironment: map[string]string{}}

	agentRuntimePlatform = linuxPlatform
	require.Equal(t, RuntimeContainmentAuthoritative, containmentMode(Options{ProcessIsolation: policy}))

	for _, platform := range []string{darwinPlatform, windowsPlatform, "freebsd"} {
		agentRuntimePlatform = platform
		require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{ProcessIsolation: policy}), platform)
		require.Error(t, validateProcessIsolationOption(policy), platform)
	}

	agentRuntimePlatform = darwinPlatform
	require.Equal(t, RuntimeContainmentUnavailable, containmentMode(Options{
		ProcessIsolation: policy, DarwinBestEffortContainment: true,
	}))
}

// TestExplicitProcessIsolationRefusesWithoutASecondSpawnAttempt is the other
// half of the policy rule: an explicit policy this agent cannot honour refuses
// the launch outright. It never falls back to ordinary execution or to
// best-effort containment to make the session start — a downgrade would run the
// child under a boundary the host did not choose — and the proof is that the
// refusal spawned nothing at all, counted at the executable itself.
//
// Both ways a policy goes unhonoured are driven. An unavailable one is a
// platform verdict, so it can only be reached where no hardened boundary
// exists; an invalid one is a verdict on the policy's own contents and is
// reachable everywhere, which keeps the gate lit on Linux too.
func TestExplicitProcessIsolationRefusesWithoutASecondSpawnAttempt(t *testing.T) {
	for name, testCase := range map[string]struct {
		option              Option
		refusedByOnlyLinux  bool
		wantUnavailableMode bool
	}{
		// Every non-Linux platform is a configuration whose explicit policy
		// cannot be honoured, whatever its contents.
		"the platform has no hardened boundary": {
			option:              testProcessIsolationOption(),
			refusedByOnlyLinux:  true,
			wantUnavailableMode: true,
		},
		// A zero UID/GID names no privilege boundary to cross. On Linux the
		// selected mode still reads as the hardened one, which is the point:
		// the refusal comes from validating the policy, not from picking a
		// weaker boundary to run under.
		"the policy names no identity to drop to": {
			option: WithProcessIsolation(ProcessIsolation{UID: 0, GID: 0}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			restoreContainmentPlatformSeam(t)

			if testCase.refusedByOnlyLinux && runtime.GOOS == linuxPlatform {
				t.Skip("the unavailable-policy case needs a platform with no hardened boundary")
			}

			agentRuntimePlatform = runtime.GOOS

			wantMode := containmentMode(Options{ProcessIsolation: policyForContainmentModeTest()})
			if testCase.wantUnavailableMode {
				wantMode = RuntimeContainmentUnavailable
			}

			harness, record := ordinaryPiHarness(t, successfulUnitScenario())

			// The harness is drained once here so the recorder is armed and the
			// scenario binary is already built: anything the refusal below
			// records is the refusal's own doing.
			require.Empty(t, ordinaryLaunches(t, record))

			agent := NewAgent(
				testCase.option,
				WithExecutablePath(harness),
				WithScratchDir(t.TempDir()),
				WithLogger(slog.New(slog.DiscardHandler)),
			)
			t.Cleanup(func() { _ = agent.Close() })
			agent.setConnection(newDirectAgentClient())

			require.Equal(t, wantMode, agent.ContainmentMode())

			_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
			require.Error(t, err, "an unhonoured explicit policy refuses the session")

			require.Empty(t, ordinaryLaunches(t, record),
				"the refusal spawned a native process anyway")

			agent.mu.Lock()
			active := len(agent.sessions)
			agent.mu.Unlock()
			require.Zero(t, active, "a refused launch installs no session")

			// And the policy is still the one the host supplied: nothing rewrote
			// it into ordinary execution on the way to the refusal.
			require.NotNil(t, agent.options.ProcessIsolation)
			require.False(t, agent.options.DarwinBestEffortContainment)
		})
	}
}

func TestEnsureVersionRefusesExplicitIsolationBestEffortCombination(t *testing.T) {
	restoreContainmentPlatformSeam(t)
	agentRuntimePlatform = linuxPlatform

	agent := NewAgent(
		WithProcessIsolation(ProcessIsolation{
			UID: 11, GID: 22, BaseEnvironment: map[string]string{},
			StandaloneOwnerID: "explicit-test", StandaloneStateRoot: "/var/lib/explicit-test",
		}),
		WithDarwinBestEffortContainment(),
	)
	require.ErrorIs(t, agent.ensureVersion(t.Context()), ErrProcessContainmentIncomplete)
}
