package piacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

type stagedErrorContext struct{ calls atomic.Int32 }

func (*stagedErrorContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*stagedErrorContext) Done() <-chan struct{}       { return nil }
func (c *stagedErrorContext) Err() error {
	if c.calls.Add(1) > 1 {
		return context.Canceled
	}

	return nil
}
func (*stagedErrorContext) Value(any) any { return nil }

type closeAgentOnErrContext struct {
	context.Context //nolint:containedctx // Test hook closes the agent at the explicit final context gate.
	agent           *Agent
	once            sync.Once
}

type closeAgentOnSpanStart struct {
	agent *Agent
	once  sync.Once
}

func (p *closeAgentOnSpanStart) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	p.once.Do(func() {
		p.agent.mu.Lock()
		p.agent.closed = true
		p.agent.mu.Unlock()
	})
}

func (*closeAgentOnSpanStart) OnEnd(sdktrace.ReadOnlySpan)      {}
func (*closeAgentOnSpanStart) Shutdown(context.Context) error   { return nil }
func (*closeAgentOnSpanStart) ForceFlush(context.Context) error { return nil }

type contextIgnoringStore struct{ SessionStore }

func (s *contextIgnoringStore) Load(_ context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	return s.SessionStore.Load(context.Background(), key)
}

func (c *closeAgentOnErrContext) Err() error {
	c.once.Do(func() { _, _ = c.agent.beginClose() })

	return nil
}

func TestConfigurationAndAdmissionEdges(t *testing.T) {
	_, err := resolveSessionConfiguration(PiOptions{}, sessionConfigurationPresence{}, sessionConfigurationRecord{
		Env: map[string]string{}, ExtraPathDirs: []string{"relative"},
	})
	require.Error(t, err)

	err = validateEnvironmentForPlatform(
		map[string]string{"Token": "one", "TOKEN": "two"},
		"env",
		func(string) bool { return false },
		true,
	)
	require.Error(t, err)
	require.NoError(t, validateEnvironmentForPlatform(
		map[string]string{"TOKEN": "one"},
		"env",
		func(string) bool { return false },
		true,
	))

	agent := NewAgent()
	agent.options.ProviderAuthRoot = "/provider-auth"
	agent.options.hostAuthoritySupplied = true
	require.NoError(t, configureProviderAuth(agent))

	agent.markNativeTreeBusy("")
	require.Empty(t, agent.nativeBusyRoots)
	path, err := agent.resolveExecutablePath()
	require.NoError(t, err)
	require.Equal(t, rawEventSourceValue, path)
}

func TestGenerationRetentionEdges(t *testing.T) {
	wantErr := errors.New("cleanup fault")

	empty := &agentSession{agent: NewAgent()}
	empty.retainGeneration("", false, nil)
	require.NoError(t, empty.releaseRetainedGeneration(t.Context()))
	released, err := empty.cleanupGenerationRoot(t.Context(), "", false)
	require.False(t, released)
	require.NoError(t, err)

	retained := &agentSession{agent: NewAgent()}
	retained.retainGeneration("/first", false, nil)
	retained.retainGeneration("/ignored", true, wantErr)
	require.Equal(t, "/first", retained.retainedRoot)
	require.NoError(t, retained.retainedErr)

	incomplete := &agentSession{agent: NewAgent()}
	incomplete.retainGeneration("/incomplete", true, errors.Join(wantErr, ErrContainmentIncomplete))
	require.ErrorIs(t, incomplete.releaseRetainedGeneration(t.Context()), wantErr)
	busyRetention := &agentSession{agent: NewAgent()}
	busyRetention.retainGeneration("/busy", true, ErrNativeTreeBusy)
	require.NoError(t, busyRetention.retainedErr)

	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
	removed := ""
	materializeRemoveAll = func(path string) error {
		removed = path

		return nil
	}
	released, err = retained.cleanupGenerationRoot(t.Context(), "/ordinary", false)
	require.False(t, released)
	require.NoError(t, err)
	require.Equal(t, "/ordinary", removed)

	managedAgent := edgeManagedAgent(&edgeHostAuthority{})
	managed := &agentSession{agent: managedAgent}
	released, err = managed.cleanupGenerationRoot(t.Context(), "/prepared", true)
	require.False(t, released)
	require.NoError(t, err)
	require.Equal(t, "/prepared", removed)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
	released, err = managed.cleanupGenerationRoot(t.Context(), "/prepared", true)
	require.True(t, released)
	require.ErrorIs(t, err, wantErr)

	retainedErr := &agentSession{agent: managedAgent, retainedRoot: "/retained", retainedErr: wantErr}
	require.ErrorIs(t, retainedErr.releaseRetainedGeneration(t.Context()), wantErr)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	busy := &agentSession{agent: managedAgent, retainedRoot: "/busy", retainedPrepared: true}
	require.ErrorIs(t, busy.releaseRetainedGeneration(t.Context()), ErrNativeTreeBusy)
	_, markedBusy := managedAgent.nativeBusyRoots["/busy"]
	require.True(t, markedBusy)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
	otherFailure := &agentSession{agent: managedAgent, retainedRoot: "/other", retainedPrepared: true}
	require.ErrorIs(t, otherFailure.releaseRetainedGeneration(t.Context()), wantErr)

	managedAgent.options.HostAuthority = &edgeHostAuthority{}
	materializeRemoveAll = func(string) error { return wantErr }
	removeFailure := &agentSession{agent: managedAgent, retainedRoot: "/remove", retainedPrepared: true}
	require.ErrorIs(t, removeFailure.releaseRetainedGeneration(t.Context()), wantErr)

	materializeRemoveAll = func(string) error { return nil }
	success := &agentSession{agent: managedAgent, retainedRoot: "/success", retainedPrepared: true}
	managedAgent.markNativeTreeBusy("/success")
	require.NoError(t, success.releaseRetainedGeneration(t.Context()))
	require.Empty(t, success.retainedRoot)
	_, stillBusy := managedAgent.nativeBusyRoots["/success"]
	require.False(t, stillBusy)

	changed := &agentSession{agent: managedAgent, retainedRoot: "/changed"}
	materializeRemoveAll = func(string) error {
		changed.mu.Lock()
		changed.retainedRoot = "/replacement"
		changed.mu.Unlock()

		return nil
	}
	require.NoError(t, changed.releaseRetainedGeneration(t.Context()))
	require.Equal(t, "/replacement", changed.retainedRoot)
}

func TestManagedGenerationRetirementEdges(t *testing.T) {
	wantErr := errors.New("retirement fault")
	require.ErrorIs(t, finalizeSessionNativeResources(nil, ErrContainmentIncomplete, "", false, "", nil, nil), ErrContainmentIncomplete)

	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
	materializeRemoveAll = func(string) error { return wantErr }
	require.ErrorIs(t, finalizeSessionNativeResources(NewAgent(), nil, "/generation", false, "", nil, nil), wantErr)
	require.ErrorIs(t, finalizeSessionNativeResources(nil, nil, "", false, "/session", nil, nil), wantErr)
	materializeRemoveAll = func(string) error { return nil }
	require.NoError(t, finalizeSessionNativeResources(nil, nil, "", false, "/session", nil, nil))
	disposeAgent := edgeManagedAgent(&edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }})
	require.ErrorIs(t, finalizeSessionNativeResources(disposeAgent, nil, "/prepared", true, "", nil, nil), wantErr)
	disposeAgent.options.HostAuthority = &edgeHostAuthority{}
	require.NoError(t, finalizeSessionNativeResources(disposeAgent, nil, "/prepared", true, "", nil, nil))

	ordinary := &agentSession{agent: NewAgent()}
	require.NoError(t, ordinary.retireManagedGeneration(t.Context(), nil))
	require.NoError(t, (&agentSession{}).retireManagedGeneration(t.Context(), nil))

	managedAgent := edgeManagedAgent(&edgeHostAuthority{})
	managed := &agentSession{agent: managedAgent}
	require.ErrorIs(t, managed.retireManagedGeneration(t.Context(), nil), ErrContainmentIncomplete)
	require.True(t, managed.managedCommitPending)

	missing := newTestSessionOutbox(1)
	require.ErrorIs(t, managed.stopSettledManagedGeneration(t.Context(), missing), ErrContainmentIncomplete)
	require.ErrorIs(t, managed.retireManagedGeneration(t.Context(), missing), ErrContainmentIncomplete)

	closedPump := make(chan struct{})
	close(closedPump)
	cancelled := false
	process := newStubProcess(true)
	process.shutdown = wantErr
	complete := newTestSessionOutbox(2)
	complete.proc = process
	complete.pumpDone = closedPump
	complete.pumpCancel = func() { cancelled = true }
	require.NoError(t, managed.stopSettledManagedGeneration(t.Context(), complete))
	require.True(t, cancelled)
	ownerSuccess := newTestSessionOutbox(20)
	ownerSuccess.proc = newStubProcess(true)
	managed.outbox = ownerSuccess
	require.NoError(t, managed.retireManagedGeneration(t.Context(), ownerSuccess))

	process = newStubProcess(true)
	process.shutdown = wantErr
	process.close = wantErr
	failing := newTestSessionOutbox(3)
	failing.proc = process
	require.ErrorIs(t, managed.stopSettledManagedGeneration(t.Context(), failing), wantErr)

	blockedPump := make(chan struct{})
	process = newStubProcess(true)
	blocked := newTestSessionOutbox(4)
	blocked.proc = process
	blocked.pumpDone = blockedPump
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, managed.stopSettledManagedGeneration(ctx, blocked), ErrContainmentIncomplete)

	nonOwner := newTestSessionOutbox(5)
	nonOwner.mu.Lock()
	containment, owner := nonOwner.claimContainmentLocked(containmentOwnerTurn)
	nonOwner.mu.Unlock()
	require.True(t, owner)
	nonOwner.finishContainment(containment, nil)
	managed.outbox = nonOwner
	require.NoError(t, managed.retireManagedGeneration(t.Context(), nonOwner))

	other := newTestSessionOutbox(6)
	managed.outbox = nonOwner
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), ErrContainmentIncomplete)
	managed.outbox = other
	managed.launch.NativeRoot = ""
	managed.generationPrepared = true
	require.NoError(t, managed.reclaimManagedGeneration(t.Context(), other))

	managed.launch.NativeRoot = "/busy"
	managed.generationPrepared = true
	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), ErrNativeTreeBusy)
	_, busy := managedAgent.nativeBusyRoots["/busy"]
	require.True(t, busy)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), wantErr)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		managed.mu.Lock()
		managed.launch.NativeRoot = "/replacement"
		managed.mu.Unlock()

		return nil
	}}
	managed.launch.NativeRoot = "/changed"
	managed.generationPrepared = true
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), ErrContainmentIncomplete)

	managedAgent.options.HostAuthority = &edgeHostAuthority{}
	managed.launch.NativeRoot = "/success"
	managed.generationPrepared = true
	managedAgent.markNativeTreeBusy("/success")
	require.NoError(t, managed.reclaimManagedGeneration(t.Context(), other))
	require.False(t, managed.generationPrepared)
}

func TestSessionCarrierAndLookupEdges(t *testing.T) {
	id := acp.SessionId(validSessionUUID)
	agent := NewAgent()
	agent.sessionCarriers = nil
	cancelledCtx, cancelImmediately := context.WithCancel(t.Context())
	cancelImmediately()
	_, err := agent.acquireSessionCarrier(cancelledCtx, id)
	require.ErrorIs(t, err, context.Canceled)

	release, err := agent.acquireSessionCarrier(t.Context(), id)
	require.NoError(t, err)
	release()
	release()
	require.Empty(t, agent.sessionCarriers)

	agent.sessionCarriers = nil
	_, err = agent.acquireSessionCarrier(&stagedErrorContext{}, id)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, agent.sessionCarriers)

	blocked := &sessionCarrierTransition{token: make(chan struct{}, 1)}
	agent.sessionCarriers = map[acp.SessionId]*sessionCarrierTransition{id: blocked}
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		for {
			agent.mu.Lock()
			registered := blocked.users == 1
			agent.mu.Unlock()
			if registered {
				cancel()

				return
			}

			runtime.Gosched()
		}
	}()
	_, err = agent.acquireSessionCarrier(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, blocked.users)

	session := &agentSession{id: id, configuration: sessionConfigurationRecord{
		Env: map[string]string{"TOKEN": "value"}, ExtraPathDirs: []string{"/bin"},
	}}
	agent.sessions[id] = session
	agent.deleted[id] = struct{}{}
	require.Nil(t, agent.activeSession(id))
	_, ok := agent.activeSessionConfiguration(id)
	require.False(t, ok)

	delete(agent.deleted, id)
	require.Same(t, session, agent.activeSession(id))
	configuration, ok := agent.activeSessionConfiguration(id)
	require.True(t, ok)
	require.Equal(t, session.configuration, configuration)
	delete(agent.sessions, id)
	_, ok = agent.activeSessionConfiguration(id)
	require.False(t, ok)
}

func TestTurnGenerationStopAndFenceEdges(t *testing.T) {
	wantErr := errors.New("turn fault")
	session := &agentSession{agent: NewAgent()}
	require.ErrorIs(t, session.stopTurnGeneration(t.Context(), nil), ErrContainmentIncomplete)

	process := newStubProcess(true)
	client := newStubPiClient()
	complete := newTestSessionOutbox(1)
	complete.proc = process
	complete.client = client
	pumpCancelled := false
	complete.pumpCancel = func() { pumpCancelled = true }
	require.NoError(t, session.stopTurnGeneration(t.Context(), complete))
	require.True(t, pumpCancelled)

	process = newStubProcess(true)
	process.close = ErrContainmentIncomplete
	client = newStubPiClient()
	client.abortErr = errors.Join(wantErr, ErrContainmentIncomplete)
	incomplete := newTestSessionOutbox(2)
	incomplete.proc = process
	incomplete.client = client
	require.ErrorIs(t, session.stopTurnGeneration(t.Context(), incomplete), ErrContainmentIncomplete)
	require.ErrorIs(t, session.stopTurnGeneration(t.Context(), incomplete), wantErr)

	process = newStubProcess(true)
	blockedPump := make(chan struct{})
	blocked := newTestSessionOutbox(3)
	blocked.proc = process
	blocked.pumpDone = blockedPump
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, session.stopTurnGeneration(ctx, blocked), ErrContainmentIncomplete)

	require.NoError(t, session.awaitTurnFence())
	session.turnFenceStarted = true
	require.NoError(t, session.awaitTurnFence())
	done := make(chan struct{})
	close(done)
	session.turnFenceDone = done
	session.turnFenceErr = wantErr
	require.ErrorIs(t, session.awaitTurnFence(), wantErr)
}

func TestPumpNativeBoundaryEdges(t *testing.T) {
	wantErr := errors.New("boundary fault")
	require.ErrorIs(t, runNativeBoundaryStep(t.Context(), "error", func() error { return wantErr }), wantErr)
	require.ErrorIs(t, runNativeBoundaryStep(t.Context(), "panic", func() error { panic("boundary") }), ErrContainmentIncomplete)
	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, runNativeBoundaryStep(cancelledCtx, "cancelled", func() error { return nil }), context.Canceled)

	tracker := newNativeBoundaryTracker()
	require.ErrorIs(t, tracker.run(t.Context(), "panic", func() error { panic("tracker") }), ErrContainmentIncomplete)

	session := &agentSession{agent: NewAgent(), id: acp.SessionId(validSessionUUID)}
	missing := newTestSessionOutbox(1)
	require.NoError(t, session.stopNativeGeneration(t.Context(), missing))

	process := newStubProcess(true)
	client := newStubPiClient()
	client.abortErr = wantErr
	complete := newTestSessionOutbox(2)
	complete.proc = process
	complete.client = client
	pumpCancelled := false
	complete.pumpCancel = func() { pumpCancelled = true }
	require.NoError(t, session.stopNativeGeneration(t.Context(), complete))
	require.True(t, pumpCancelled)

	process = newStubProcess(true)
	process.close = ErrContainmentIncomplete
	client = newStubPiClient()
	client.abortErr = errors.Join(wantErr, ErrContainmentIncomplete)
	incomplete := newTestSessionOutbox(3)
	incomplete.proc = process
	incomplete.client = client
	require.ErrorIs(t, session.stopNativeGeneration(t.Context(), incomplete), wantErr)

	process = newStubProcess(true)
	pumpDone := make(chan struct{})
	process.closeFunc = func() error {
		close(pumpDone)

		return nil
	}
	join := newTestSessionOutbox(4)
	join.proc = process
	ctx, cancelDuringPump := context.WithCancel(t.Context())
	join.pumpCancel = cancelDuringPump
	join.pumpDone = pumpDone
	require.NoError(t, session.stopNativeGeneration(ctx, join))

	containmentOutbox := newTestSessionOutbox(5)
	containmentProcess := newStubProcess(true)
	containmentProcess.close = ErrContainmentIncomplete
	containmentOutbox.proc = containmentProcess
	require.ErrorIs(t, session.containGenerationSync(t.Context(), containmentOutbox, "coverage"), ErrContainmentIncomplete)
}

func TestSettlementAndAutonomousRetirementEdges(t *testing.T) {
	wantErr := errors.New("settlement boundary fault")
	done := make(chan struct{})
	close(done)
	session := &agentSession{
		agent:            NewAgent(),
		turnFenceStarted: true,
		turnFenceDone:    done,
		turnFenceErr:     wantErr,
	}
	timedOut := &atomic.Bool{}
	timedOut.Store(true)
	_, err := session.settlePrompt(
		t.Context(),
		acp.PromptRequest{},
		&promptTurnState{},
		promptOutcome{},
		timedOut,
	)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "exceeded")

	fixture := newAgentCycleFixture(t)
	fixture.session.agent.options.hostAuthoritySupplied = true
	fixture.session.agent.options.HostAuthority = &edgeHostAuthority{}
	fixture.session.completeAgentCycle(t.Context(), nil, &agentCycle{state: &promptTurnState{}})
}

func TestEnsureVersionCoverageEdges(t *testing.T) {
	newVersionAgent := func() *Agent {
		agent := NewAgent(WithExecutablePath("/fake/pi"), WithScratchDir(t.TempDir()))
		agent.probeVersion = func(context.Context, string, string) (string, error) {
			return internalpi.DefaultMinimumVersion, nil
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

func TestStoreStartedSessionRecheckEdges(t *testing.T) {
	id := acp.SessionId("replacement")

	for name, mutate := range map[string]func(*Agent, *agentSession){
		"closed":  func(agent *Agent, _ *agentSession) { agent.closed = true },
		"deleted": func(agent *Agent, _ *agentSession) { agent.deleted[id] = struct{}{} },
		"changed": func(agent *Agent, previous *agentSession) {
			agent.sessions[id] = &agentSession{agent: agent, id: previous.id}
		},
	} {
		t.Run(name, func(t *testing.T) {
			agent := NewAgent()
			process := newStubProcess(false)
			previous := attachTestNativeBoundary(&agentSession{
				agent: agent, id: id, proc: process, sessionRoot: t.TempDir(),
			})
			process.closeFunc = func() error {
				agent.mu.Lock()
				mutate(agent, previous)
				agent.mu.Unlock()

				return nil
			}
			agent.sessions[id] = previous
			replacement := &agentSession{agent: agent, id: id}
			require.Error(t, agent.storeStartedSession(t.Context(), replacement))
		})
	}

	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := NewAgent().restoreSession(cancelledCtx, id, sessionStart{Cwd: "/cwd"}, nil)
	require.ErrorIs(t, err, context.Canceled)

	agent := NewAgent()
	agent.sessions[id] = &agentSession{agent: agent, id: id, configuration: sessionConfigurationRecord{
		ExtraPathDirs: []string{"relative"},
	}}
	_, err = agent.restoreSession(t.Context(), id, sessionStart{Cwd: "/cwd"}, nil)
	require.Error(t, err)
}

func TestSessionConstructionFenceEdges(t *testing.T) {
	wantErr := errors.New("prepare fault")

	t.Run("busy retry", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		done := make(chan struct{})
		close(done)
		construction := &nativeConstruction{
			done: done, err: ErrNativeTreeBusy, generationRoot: "/busy", generationPrepared: true,
			nativeBoundary: newNativeBoundaryTracker(),
		}
		agent.options.hostAuthoritySupplied = true
		agent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
			return ErrNativeTreeBusy
		}}
		agent.constructions[construction] = struct{}{}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, ErrNativeTreeBusy)
	})

	t.Run("closed after version check", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		agent.closed = true
		construction := &nativeConstruction{
			done: make(chan struct{}), nativeBoundary: newNativeBoundaryTracker(),
		}
		_, err := agent.startSessionConstruction(t.Context(), sessionStart{Cwd: "/cwd"}, construction)
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("close after executable resolution", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		agent.options.ExecutablePath = ""
		agent.lookPath = func(string) (string, error) {
			agent.mu.Lock()
			agent.closed = true
			agent.mu.Unlock()

			return "/fake/pi", nil
		}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("close when process observation starts", func(t *testing.T) {
		processor := &closeAgentOnSpanStart{}
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
		t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

		agent := newStubClientAgent(t, newStubPiClient(), WithTracerProvider(provider))
		processor.agent = agent
		agent.versionChecked = true
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("prepare failure", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		agent.options.hostAuthoritySupplied = true
		agent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error {
			return wantErr
		}}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("close after prepare", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		agent.options.hostAuthoritySupplied = true
		agent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error {
			_, _ = agent.beginClose()

			return nil
		}}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("cancel before launch", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := agent.startSession(ctx, sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("close at launch gate", func(t *testing.T) {
		agent := newStubClientAgent(t, newStubPiClient())
		agent.versionChecked = true
		ctx := &closeAgentOnErrContext{Context: t.Context(), agent: agent}
		_, err := agent.startSession(ctx, sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("close after client start", func(t *testing.T) {
		client := newStubPiClient()
		agent := newStubClientAgent(t, client)
		agent.versionChecked = true
		client.startFunc = func(context.Context) error {
			_, _ = agent.beginClose()

			return nil
		}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("close during setup", func(t *testing.T) {
		client := newStubPiClient()
		client.state = internalpi.SessionState{SessionID: "session"}
		agent := newStubClientAgent(t, client)
		agent.versionChecked = true
		client.autoRetryFunc = func(context.Context, bool) error {
			_, _ = agent.beginClose()

			return nil
		}
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, errAgentClosed)
	})

	t.Run("fork retirement", func(t *testing.T) {
		client := newStubPiClient()
		client.state = internalpi.SessionState{SessionID: "session"}
		agent := edgeManagedAgent(&edgeHostAuthority{})
		session := &agentSession{agent: agent, client: client, proc: newStubProcess(true)}
		err := agent.setUpNativeSession(t.Context(), session, sessionStart{ForkSession: true}, internalpi.ModelRef{}, false)
		require.ErrorIs(t, err, ErrContainmentIncomplete)
	})
}

func TestNextRuntimeLaunchCoverageEdges(t *testing.T) {
	wantErr := errors.New("runtime launch fault")
	fixture := func(t *testing.T) (*agentSession, internalpi.LaunchSpec) {
		t.Helper()

		agent := NewAgent(WithScratchDir(t.TempDir()))
		session := &agentSession{agent: agent, id: acp.SessionId("runtime"), sessionRoot: t.TempDir()}
		dirs, err := createSessionGeneration(session.sessionRoot)
		require.NoError(t, err)
		require.NoError(t, agent.applyGenerationAgentDir(&dirs))
		previous := internalpi.LaunchSpec{NativeRoot: dirs.Root, AgentDir: dirs.AgentDir, SessionDir: dirs.SessionDir}
		session.launch = previous

		return session, previous
	}

	t.Run("reclaim", func(t *testing.T) {
		session, previous := fixture(t)
		session.generationPrepared = true
		session.agent.options.hostAuthoritySupplied = true
		session.agent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
		_, err := session.nextRuntimeLaunch(t.Context(), previous)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("pending commit", func(t *testing.T) {
		session, previous := fixture(t)
		session.managedCommitPending = true
		session.sessionFilePath = filepath.Join(t.TempDir(), "session.jsonl")
		require.NoError(t, os.WriteFile(session.sessionFilePath, []byte("{}\n"), 0o600))
		store := newFaultySessionStore()
		store.appendErr = wantErr
		session.agent.options.SessionStore = store
		_, err := session.nextRuntimeLaunch(t.Context(), previous)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("admission", func(t *testing.T) {
		session, _ := fixture(t)
		session.agent.nativeBusyRoots["/busy"] = struct{}{}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.ErrorIs(t, err, ErrNativeTreeBusy)
	})

	t.Run("store load", func(t *testing.T) {
		session, _ := fixture(t)
		session.agent.options.SessionStore = &errorSessionStore{loadErr: wantErr}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("agent directory", func(t *testing.T) {
		session, _ := fixture(t)
		session.agent.options.Home = "relative"
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.Error(t, err)
	})

	t.Run("residence", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, _ := fixture(t)
		mkdirTemp := materializeMkdirTemp
		materializeMkdirTemp = func(parent, pattern string) (string, error) {
			root, err := mkdirTemp(parent, pattern)
			if err != nil {
				return "", err
			}
			agentDir := filepath.Join(root, "agent")
			if err := os.MkdirAll(agentDir, 0o700); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(agentDir, ".acp-session"), nil, 0o600); err != nil {
				return "", err
			}

			return root, nil
		}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.ErrorContains(t, err, "create session residence root")
	})

	t.Run("seed", func(t *testing.T) {
		session, _ := fixture(t)
		session.agent.options.SeedFiles = map[string]string{internalpi.SettingsFileName: "{"}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.Error(t, err)
	})

	t.Run("seed manifest", func(t *testing.T) {
		restoreMaterializeSeams(t)
		session, _ := fixture(t)
		session.agent.options.SeedFiles = map[string]string{"seed.txt": "value"}
		mkdirTemp := materializeMkdirTemp
		materializeMkdirTemp = func(parent, pattern string) (string, error) {
			root, err := mkdirTemp(parent, pattern)
			if err != nil {
				return "", err
			}
			agentDir := filepath.Join(root, "agent")
			if err := os.MkdirAll(agentDir, 0o700); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(agentDir, ".seed-manifest.json"), []byte("{"), 0o600); err != nil {
				return "", err
			}

			return root, nil
		}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.Error(t, err)
		var seedErr *internalpi.SeedFileError
		require.NotErrorAs(t, err, &seedErr)
	})

	t.Run("explicit resources", func(t *testing.T) {
		session, _ := fixture(t)
		original := agentDirExplicitResources
		t.Cleanup(func() { agentDirExplicitResources = original })
		agentDirExplicitResources = func(internalpi.AgentDir) (internalpi.ExplicitResources, error) {
			return internalpi.ExplicitResources{}, wantErr
		}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("prepare", func(t *testing.T) {
		session, _ := fixture(t)
		session.agent.options.hostAuthoritySupplied = true
		session.agent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error { return wantErr }}
		_, err := session.nextRuntimeLaunch(t.Context(), internalpi.LaunchSpec{})
		require.ErrorIs(t, err, wantErr)
	})
}

func TestRelaunchProcessCoverageEdges(t *testing.T) {
	wantErr := errors.New("relaunch fault")
	fixture := func(t *testing.T, client *stubPiClient) (*agentSession, *Agent) {
		t.Helper()

		agent := NewAgent()
		session := &agentSession{
			agent: agent, id: acp.SessionId("relaunch"), proc: newStubProcess(true), client: newStubPiClient(),
		}
		prepareRelaunchFixture(t, session)
		client.state = internalpi.SessionState{SessionID: string(session.id), SessionFile: "/fresh"}
		agent.startPiProcess = func(context.Context, internalpi.LaunchSpec) (piProcess, piClient, error) {
			return newStubProcess(false), client, nil
		}

		return session, agent
	}

	t.Run("retained generation", func(t *testing.T) {
		session, _ := fixture(t, newStubPiClient())
		session.retainedRoot = "/retained"
		session.retainedErr = wantErr
		require.ErrorIs(t, session.relaunchProcess(t.Context()), wantErr)
	})

	t.Run("session closes after materialization", func(t *testing.T) {
		session, agent := fixture(t, newStubPiClient())
		agent.options.hostAuthoritySupplied = true
		agent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error {
			session.mu.Lock()
			session.closing = true
			session.mu.Unlock()

			return nil
		}}
		require.Error(t, session.relaunchProcess(t.Context()))
	})

	t.Run("agent closes after materialization", func(t *testing.T) {
		session, agent := fixture(t, newStubPiClient())
		agent.options.hostAuthoritySupplied = true
		agent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error {
			_, _ = agent.beginClose()

			return nil
		}}
		require.ErrorIs(t, session.relaunchProcess(t.Context()), errAgentClosed)
	})

	t.Run("cancel before spawn", func(t *testing.T) {
		session, agent := fixture(t, newStubPiClient())
		agent.options.SessionStore = &contextIgnoringStore{SessionStore: NewInMemorySessionStore()}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, session.relaunchProcess(ctx), context.Canceled)
	})

	t.Run("agent closes after spawn", func(t *testing.T) {
		session, agent := fixture(t, newStubPiClient())
		agent.startPiProcess = func(context.Context, internalpi.LaunchSpec) (piProcess, piClient, error) {
			_, _ = agent.beginClose()

			return newStubProcess(false), newStubPiClient(), nil
		}
		require.ErrorIs(t, session.relaunchProcess(t.Context()), errAgentClosed)
	})

	t.Run("thinking level", func(t *testing.T) {
		client := newStubPiClient()
		client.thinkingErr = wantErr
		session, _ := fixture(t, client)
		session.thinkingLevel = internalpi.ThinkingLevelMax
		require.ErrorIs(t, session.relaunchProcess(t.Context()), wantErr)
	})

	t.Run("model", func(t *testing.T) {
		client := newStubPiClient()
		client.setModelErr = wantErr
		session, _ := fixture(t, client)
		session.model = "provider/model"
		require.ErrorIs(t, session.relaunchProcess(t.Context()), wantErr)
	})

	t.Run("failed replacement cleanup", func(t *testing.T) {
		restoreMaterializeSeams(t)
		client := newStubPiClient()
		client.startErr = wantErr
		session, agent := fixture(t, client)
		var replacementRoot string
		agent.startPiProcess = func(_ context.Context, spec internalpi.LaunchSpec) (piProcess, piClient, error) {
			replacementRoot = spec.NativeRoot

			return newStubProcess(false), client, nil
		}
		removeAll := materializeRemoveAll
		materializeRemoveAll = func(path string) error {
			if path == replacementRoot && replacementRoot != "" {
				return errors.New("replacement cleanup fault")
			}

			return removeAll(path)
		}
		err := session.relaunchProcess(t.Context())
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "replacement cleanup fault")
	})
}

func TestSessionCloseReleaseEdges(t *testing.T) {
	wantErr := errors.New("close release fault")

	managedAgent := edgeManagedAgent(&edgeHostAuthority{reclaim: func(context.Context, string) error {
		return wantErr
	}})
	managed := &agentSession{
		agent: managedAgent, id: acp.SessionId("managed-close"),
		launch: internalpi.LaunchSpec{NativeRoot: "/managed"}, generationPrepared: true,
	}
	require.ErrorIs(t, managed.Close(t.Context()), wantErr)
	require.ErrorIs(t, managed.nativeContainmentError(), wantErr)

	retained := &agentSession{
		agent: NewAgent(), id: acp.SessionId("retained-close"),
		retainedRoot: "/retained", retainedErr: wantErr,
	}
	require.ErrorIs(t, retained.Close(t.Context()), wantErr)
}

func TestConstructionOwnershipEdges(t *testing.T) {
	process := newStubProcess(false)
	process.close = ErrContainmentIncomplete
	agent := NewAgent()
	construction := &nativeConstruction{proc: process, nativeBoundary: newNativeBoundaryTracker()}
	require.ErrorIs(t, agent.cleanupNativeConstructionOwned(t.Context(), construction, nil), ErrContainmentIncomplete)

	t.Run("pump boundary changes", func(t *testing.T) {
		client := newStubPiClient()
		client.state = internalpi.SessionState{SessionID: "session"}
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
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, ErrContainmentIncomplete)
	})

	t.Run("immutable transfer", func(t *testing.T) {
		client := newStubPiClient()
		client.state = internalpi.SessionState{SessionID: "session"}
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
		_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
		require.ErrorIs(t, err, ErrContainmentIncomplete)
	})

	t.Run("post-transfer close recheck", func(t *testing.T) {
		agent := NewAgent()
		open := attachTestNativeBoundary(&agentSession{
			agent: agent, proc: newStubProcess(false), turn: make(chan struct{}, sessionTurnCapacity),
		})
		require.NoError(t, agent.closeTransferredSessionIfAgentClosed(t.Context(), open, false))

		closed := attachTestNativeBoundary(&agentSession{
			agent: agent, proc: newStubProcess(false), turn: make(chan struct{}, sessionTurnCapacity),
		})
		agent.mu.Lock()
		agent.closed = true
		agent.mu.Unlock()
		require.ErrorIs(t, agent.closeTransferredSessionIfAgentClosed(t.Context(), closed, false), errAgentClosed)
	})
}
